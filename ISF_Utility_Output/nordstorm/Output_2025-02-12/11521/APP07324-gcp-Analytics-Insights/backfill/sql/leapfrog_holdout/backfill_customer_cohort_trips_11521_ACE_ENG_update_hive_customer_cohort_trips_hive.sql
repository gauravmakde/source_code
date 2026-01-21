--SQL script must begin QUERY_BAND SETTINGS
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=customer_cohort_trips_11521_ACE_ENG;
     Task_Name=customer_cohort_trips_hive;'
     FOR SESSION VOLATILE;

CREATE OR REPLACE TEMPORARY VIEW user_trips 
(
    source_platform_code string,
    SOURCE_channel_CODE string,
    business_unit_desc string, 
    tran_type_code string,
    acp_id string,
    engagement_cohort string,
    trips_stores int, 
    trips_online int,
    trips_stores_gross_usd_amt DECIMAL(38,2),
    trips_online_gross_usd_amt DECIMAL(38,2)
    ,dw_sys_load_tmstp  TIMESTAMP 
    ,tran_date DATE 
)
USING CSV 
OPTIONS(path "s3://ace-etl/tpt_export/customer_cohort_trips/customer_cohort_trips.csv",
  sep  ",",
  header "false");



create table if not exists ace_etl.customer_cohort_trips
(
    source_platform_code string,
    SOURCE_channel_CODE string,
    business_unit_desc string, 
    tran_type_code string,
    acp_id string,
    engagement_cohort string,
    trips_stores int, 
    trips_online int,
    trips_stores_gross_usd_amt DECIMAL(38,2),
    trips_online_gross_usd_amt DECIMAL(38,2)
    ,dw_sys_load_tmstp  TIMESTAMP 
    ,tran_date DATE 
)
using PARQUET
location 's3://ace-etl/customer_cohort_trips'
partitioned by (tran_date);




insert overwrite table ace_etl.customer_cohort_trips partition (tran_date)
select *
from user_trips;

