--SQL script must begin QUERY_BAND SETTINGS
SET QUERY_BAND = 'App_ID=app08742;
     DAG_ID=customer_cohort_11521_ACE_ENG;
     Task_Name=customer_cohort_hive;'
     FOR SESSION VOLATILE;

CREATE OR REPLACE TEMPORARY VIEW customer_cohort 
(
  acp_id string ,
      defining_year_ending_qtr int ,
      defining_year_start_dt DATE ,
      defining_year_end_dt DATE ,
      execution_qtr int ,
      execution_qtr_start_dt date,
      execution_qtr_end_dt  date,
      acquired_ind int ,
      nonrestaurant_trips DECIMAL(22,2) ,
      engagement_cohort string ,
      dw_sys_load_tmstp TIMESTAMP,
      execution_qtr_partition int 
)
USING CSV 
OPTIONS(path "s3://{s3_bucket_root_var}/tpt_export/customer_cohort/customer_cohort.csv",
  sep  ",",
  header "false");

create table if not exists {hive_schema}.customer_cohort
(
acp_id string ,
      defining_year_ending_qtr int ,
      defining_year_start_dt DATE ,
      defining_year_end_dt DATE ,
      execution_qtr int ,
      execution_qtr_start_dt date,
      execution_qtr_end_dt  date,
      acquired_ind int ,
      nonrestaurant_trips DECIMAL(22,2) ,
      engagement_cohort string ,
      dw_sys_load_tmstp TIMESTAMP,
      execution_qtr_partition int 
)
using PARQUET
location 's3://{s3_bucket_root_var}/customer_cohort'
partitioned by (execution_qtr_partition);




insert overwrite table {hive_schema}.customer_cohort partition (execution_qtr_partition)
select *
from customer_cohort;

