SET QUERY_BAND = '
App_ID=app08649;
DAG_ID=ascp_store_invalid_carton_from_kafka_to_teradata_13569_DAS_SC_OUTBOUND_sc_outbound_insights_frameworkschedule;
Task_Name=main_load_02_kafka;
LoginUser={td_login_user};
Job_Name=ascp_store_invalid_carton_from_kafka_to_teradata;
Data_Plane=Inventory;
Team_Email=TECH_NAP_SUPPLYCHAIN_OUTBOUND@nordstrom.com;
PagerDuty=NAP_Supply_Chain_Outbound;
Conn_Type=JDBC;'
FOR SESSION VOLATILE;

--Source
--Reading Data from  Source Kafka Topic Name=inventory-store-invalid-carton-lifecycle-analytical-avro using SQL API CODE
create temporary view store_invalid_carton_rn AS select 
   cartonNumber, 
   createdSystemTime, 
   lastUpdatedSystemTime, 
   lastTriggeringEventType, 
   scannedDetails,
   row_number() over (partition by cartonNumber order by headers.SystemTime desc) as rn  
from kafka_inventory_store_invalid_carton_object_model_avro;

create temporary view store_invalid_carton AS select 
   cartonNumber, 
   createdSystemTime, 
   lastUpdatedSystemTime, 
   lastTriggeringEventType, 
   scannedDetails
from store_invalid_carton_rn
where rn = 1;

create temporary view store_invalid_carton_extract_scannedDetails as 
select 
   cartonNumber, 
   createdSystemTime, 
   lastUpdatedSystemTime, 
   lastTriggeringEventType, 
   explode(scannedDetails) scannedDetails  
from store_invalid_carton;

create temporary view store_invalid_carton_details as
select 
   cartonNumber as carton_num, 
   createdSystemTime as object_created_sys_tmstp, 
   lastUpdatedSystemTime as last_triggered_event_tmstp, 
   lastTriggeringEventType as last_triggered_event_type, 
   scannedDetails.eventId as event_id,
   scannedDetails.eventTime as event_tmstp,
   scannedDetails.locationId as location_num,
   scannedDetails.cartonStatus as carton_sts,
   scannedDetails.employee.id as user_id,
   scannedDetails.employee.idType as user_type
from store_invalid_carton_extract_scannedDetails;

--Sink

---Writing Kafka Data to S3 in ORC format

-- write empty file to catch correct schema
insert into `ascp-store-invalid-carton-lifecycle-v1`
select 
   cartonNumber, 
   createdSystemTime, 
   lastUpdatedSystemTime, 
   lastTriggeringEventType, 
   scannedDetails
FROM kafka_inventory_store_invalid_carton_object_model_avro
where cartonNumber is NULL;

-- read from S3
create temporary view orc_vw using ORC
OPTIONS (path "s3://{s3_bucket_orc}/data/ascp-store-invalid-carton-lifecycle-v1");

-- sink data to S3
insert overwrite table `ascp-store-invalid-carton-lifecycle-tmp`
select 
   cartonNumber, 
   createdSystemTime, 
   lastUpdatedSystemTime, 
   lastTriggeringEventType, 
   scannedDetails
 FROM ( SELECT u.*
             , Row_Number() Over (PARTITION BY u.cartonNumber ORDER BY u.SystemTime DESC) AS rn
         FROM ( SELECT Cast(0 AS BIGINT) as SystemTime,
                     cartonNumber, 
                     createdSystemTime, 
                     lastUpdatedSystemTime, 
                     lastTriggeringEventType, 
                     scannedDetails
                FROM orc_vw
                UNION ALL
                SELECT Cast(headers.SystemTime AS BIGINT) as SystemTime,
                     cartonNumber, 
                     createdSystemTime, 
                     lastUpdatedSystemTime, 
                     lastTriggeringEventType, 
                     scannedDetails
                FROM kafka_inventory_store_invalid_carton_object_model_avro
              ) AS u
      ) as r
where r.rn = 1;

---Writing Error Data to S3 in csv format
insert overwrite table store_invalid_carton_err
partition(year, month, day, hour)
SELECT /*+ COALESCE(1) */
  carton_num,
  location_num ,
  carton_sts ,
  user_id,
  user_type,
  event_tmstp,
  event_id,
  object_created_sys_tmstp,
  last_triggered_event_tmstp,
  last_triggered_event_type,
  cast(year(current_date) as string) as year, 
  case when month(current_date) > 9 then cast(month(current_date) as string) else concat('0',cast(month(current_date) as string)) end as month, 
  case when dayofmonth(current_date) > 9 then cast(dayofmonth(current_date) as string) else concat('0',cast(dayofmonth(current_date) as string)) end as day,
  case when hour(current_timestamp) > 9 then cast(hour(current_timestamp) as string) else concat('0',cast(hour(current_timestamp) as string)) end as hour
FROM store_invalid_carton_details
where carton_num is null or carton_num = '' or carton_num = '""'
 or event_id is null or event_id = '' or event_id = '""';

---Writing Kafka Data to Teradata staging table
insert overwrite table store_invalid_carton_ldg_table
SELECT
  carton_num,
  location_num ,
  carton_sts ,
  user_id,
  user_type,
  event_tmstp,
  event_id,
  object_created_sys_tmstp,
  last_triggered_event_tmstp,
  last_triggered_event_type
FROM store_invalid_carton_details
where carton_num is not null and carton_num <> '' and carton_num <> '""'
 and event_id is not null and event_id <> '' and event_id <> '""'
 and (last_triggered_event_tmstp is not null and to_timestamp(last_triggered_event_tmstp) is not null)
 and (object_created_sys_tmstp is not null and to_timestamp(object_created_sys_tmstp) is not null)
 and (event_tmstp is not null and to_timestamp(event_tmstp) is not null);
