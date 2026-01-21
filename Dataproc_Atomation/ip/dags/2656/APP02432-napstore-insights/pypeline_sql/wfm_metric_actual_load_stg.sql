SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_metric_actual_load_2656_napstore_insights;
Task_Name=wfm_metric_actual_load_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_metric_actual_input AS
select *
from kafka_wfm_metric_actual_input;

-- Writing Kafka Data to S3 Path
insert into table wfm_metric_actual_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from wfm_metric_actual_input;


-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_metric_actual_stg_table
select id as ID,
       createdAt as CREATED_AT,
       lastUpdatedAt as LAST_UPDATED_AT,
       storeNumber as STORE_NUMBER,
       metricId as METRIC_ID,
       coalesce(metricFrequency, 'QUARTER_HOURLY' ) as METRIC_FREQUENCY,
       metricValue as METRIC_VALUE,
       effectiveDate as EFFECTIVE_DATE,
       localEffectiveTime as LOCAL_EFFECTIVE_TIME,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       current_timestamp()     as DW_SYS_LOAD_TMSTP,
       current_timestamp()     as DW_SYS_UPDT_TMSTP
from wfm_metric_actual_input;
