SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_labor_demand_load_2656_napstore_insights;
Task_Name=wfm_labor_demand_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_labor_demand_input AS
select *
from kafka_wfm_labor_demand_and_ideal_hours_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_ondemand_hours_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from wfm_labor_demand_input
where laborDemandHours.createdAt IS NOT NULL;


insert
overwrite table wfm_labor_demand_stg_table
select id                             as ID,
date_format(laborDemandHours.createdAt, "yyyy-MM-dd HH:mm:ss.SSS") as CREATED_AT,
date_format(laborDemandHours.lastUpdatedAt, "yyyy-MM-dd HH:mm:ss.SSS") as LAST_UPDATED_AT,
    --    cast(laborDemandHours.createdAt as timestamp)     as CREATED_AT,
    --    cast(laborDemandHours.lastUpdatedAt as timestamp) as LAST_UPDATED_AT,
       cast(element_at(headers, 'LastUpdatedTime') as INTEGER) as KAFKA_LAST_UPDATED_AT,
       laborDemandHours.storeNumber   as STORE_NUMBER,
       laborDemandHours.workgroupId   as WORKGROUP_ID,
       laborDemandHours.laborRoleId   as LABOR_ROLE_ID,
       cast(laborDemandHours.effectiveDate as string) as EFFECTIVE_DATE,
       cast(laborDemandHours.localEffectiveTime as string) as LOCAL_EFFECTIVE_TIME,
       laborDemandHours.minutes       as MINUTES,
       current_timestamp()     as DW_SYS_LOAD_TMSTP,
       current_timestamp()     as DW_SYS_UPDT_TMSTP
from wfm_labor_demand_input
where laborDemandHours.createdAt IS NOT NULL;


