SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_scheduling_data_load_2656_napstore_insights;
Task_Name=wfm_scheduling_labor_role_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_scheduling_labor_role_input AS
select *
from kafka_wfm_scheduling_labor_role_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_scheduling_labor_role_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from wfm_scheduling_labor_role_input;

-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_scheduling_labor_role_stg_table
select id                      as ID,
       name                    as NAME,
       deleted                 as DELETED,
       ifnull(deletedAt, NULL) as DELETED_AT,
       createdAt               as CREATED_AT,
       lastUpdatedAt           as LAST_UPDATED_AT,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       current_timestamp()     as DW_SYS_LOAD_TMSTP,
       current_timestamp()     as DW_SYS_UPDT_TMSTP
from wfm_scheduling_labor_role_input;
