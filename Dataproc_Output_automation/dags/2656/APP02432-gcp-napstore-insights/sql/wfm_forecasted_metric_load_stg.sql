SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_forecasted_metric_load_2656_napstore_insights;
Task_Name=wfm_forecasted_metric_load_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view wfm_forecasted_metric_input AS
select *
from kafka_wfm_forecasted_metric_input;


-- Writing Kafka Data to S3 Path
insert into table wfm_forecasted_metric_orc_output partition(year, month, day)
select *, current_date() as process_date, year(current_date()) as year,month(current_date()) as month,day(current_date()) as day
from wfm_forecasted_metric_input;


-- Writing Kafka to Semantic Layer:
insert
overwrite table wfm_forecasted_metric_stg_table
select id as ID,
       storeNumber as STORE_NUMBER,
       metricId as METRIC_ID,
       forecastGroupId as FORECAST_GROUP_ID,
       effectiveDate as EFFECTIVE_DATE,
       localEffectiveTime as LOCAL_EFFECTIVE_TIME,
       finalForecastValue as FINAL_FORECAST_VALUE,
       coalesce(metricFrequency, 'QUARTER_HOURLY' ) as METRIC_FREQUENCY,
       baselineForecastValue as BASELINE_FORECAST_VALUE,
       createdAt as CREATED_AT,
       lastUpdatedAt as LAST_UPDATED_AT,
       element_at(headers, 'LastUpdatedTime') as KAFKA_LAST_UPDATED_AT,
       current_timestamp()     as DW_SYS_LOAD_TMSTP,
       current_timestamp()     as DW_SYS_UPDT_TMSTP
from wfm_forecasted_metric_input;

