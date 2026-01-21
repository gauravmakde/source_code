SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfpc_spe_mapping_kafka_to_td;
Task_Name=job_spe_mapping_first_exec_2;'
FOR SESSION VOLATILE;

--Source
--Reading Data from  Source Kafka Topic Name=product-supplier-group-id-assignment-analytical-avro using SQL API CODE
create temporary view temp_spe_mapping_object_model AS select * from kafka_spe_mapping_object_model_avro;
--Transform Logic
--Extracting Required Column from Kafka (Cache in previous SQL)
--create temporary view kafka_spe_mapping_object_model_avro_extract_columns as select productSku.id as sku_id from temp_poitermsdisc_object_model limit 1000;

create temporary view kafka_spe_mapping_object_model_avro_extract_array as select id as supplier_group_id,sellingBrand as selling_brand,departmentNumber as department_number,
supplierGroup as supplier_group,
isActive as is_active,
lastUpdatedTime as event_time,
element_at(headers,'LastUpdatedTime') as last_updated_time_in_millis
from temp_spe_mapping_object_model;

create temporary view kafka_spe_mapping_object_model_avro_extract_columns as
select event_time,
supplier_group_id,
selling_brand, 
department_number,
supplier_group,
is_active,
last_updated_time_in_millis
from kafka_spe_mapping_object_model_avro_extract_array;
--Sink

---Writing Kafka Data to S3 in csv format for TPT load

insert into table spemapping partition (year, month, day )
SELECT
supplier_group_id,
selling_brand,
department_number,
supplier_group,
CAST(IS_ACTIVE AS STRING),
 CAST(EVENT_TIME AS STRING),
last_updated_time_in_millis,
 CAST(DW_SYS_LOAD_TMSTP AS STRING),
year( current_date()) as year,
month( current_date()) as month,
day( current_date()) as day
FROM (SELECT *, current_timestamp as DW_SYS_LOAD_TMSTP, rank() over (PARTITION BY supplier_group_id ORDER BY last_updated_time_in_millis DESC) as 
ranking FROM kafka_spe_mapping_object_model_avro_extract_columns) tmp where ranking == 1;

