SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_ideal_hours_load_2656_napstore_insights;
Task_Name=wfm_ideal_hours_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_ideal_hours_input AS
select *
from kafka_wfm_labor_demand_and_ideal_hours_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_ondemand_hours_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from wfm_ideal_hours_input
where idealLaborHours.createdAt IS NOT NULL;


-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_ideal_labor_hours_stg_table
select id                            as ID,
       idealLaborHours.createdAt     as CREATED_AT,
       idealLaborHours.lastUpdatedAt as LAST_UPDATED_AT,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       idealLaborHours.storeNumber   as STORE_NUMBER,
       idealLaborHours.workgroupId   as WORKGROUP_ID,
       idealLaborHours.laborRoleId   as LABOR_ROLE_ID,
       idealLaborHours.effectiveDate as EFFECTIVE_DATE,
       idealLaborHours.localEffectiveTime as LOCAL_EFFECTIVE_TIME,
       idealLaborHours.minutes       as MINUTES,
       current_timestamp()     as DW_SYS_LOAD_TMSTP,
       current_timestamp()     as DW_SYS_UPDT_TMSTP
from wfm_ideal_hours_input
where idealLaborHours.createdAt IS NOT NULL;