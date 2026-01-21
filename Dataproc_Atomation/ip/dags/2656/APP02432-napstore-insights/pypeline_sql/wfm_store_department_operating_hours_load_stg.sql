-- Query Band part without required DAG_ID
SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_scheduling_data_load_2656_napstore_insights;
Task_Name=wfm_store_department_operating_hours_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_store_department_operating_hours_input AS
select *
from kafka_wfm_store_department_operating_hours_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_store_department_operating_hours_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from wfm_store_department_operating_hours_input;

-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_store_department_operating_hours_stg_table
select id                                     as ID,
       storeNumber                            as STORE_NUMBER,
       departmentId                           as DEPARTMENT_ID,
       isDeleted                              as DELETED,
       ifnull(deletedAt, NULL)                as DELETED_AT,
       createdAt                              as CREATED_AT,
       lastUpdatedAt                          as LAST_UPDATED_AT,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       current_timestamp()                    as DW_SYS_LOAD_TMSTP,
       current_timestamp()                    as DW_SYS_UPDT_TMSTP
from wfm_store_department_operating_hours_input;

create
temporary view wfm_store_department_operating_hours_details_input as
select id as storeDepartmentOperatingHoursId, lastUpdatedAt, element_at(headers, 'LastUpdatedTime') as LastUpdatedTime, explode_outer(details) as details
from wfm_store_department_operating_hours_input;

insert
overwrite table wfm_store_department_operating_hours_details_stg_table
select storeDepartmentOperatingHoursId  as STORE_DEPARTMENT_OPERATING_HOURS_ID,
       details.operatingHoursDetailType as OPERATING_HOURS_DETAIL_TYPE,
       details.effectiveDay             as EFFECTIVE_DAY,
       details.startTime                as START_TIME,
       details.endTime                  as END_TIME,
       lastUpdatedAt                    as LAST_UPDATED_AT,
       LastUpdatedTime                  as KAFKA_LAST_UPDATED_AT,
       current_timestamp()              as DW_SYS_LOAD_TMSTP,
       current_timestamp()              as DW_SYS_UPDT_TMSTP
from wfm_store_department_operating_hours_details_input
where storeDepartmentOperatingHoursId is not null;