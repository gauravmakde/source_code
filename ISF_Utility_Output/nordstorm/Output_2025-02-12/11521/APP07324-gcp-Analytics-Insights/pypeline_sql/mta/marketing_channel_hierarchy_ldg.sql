SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=marketing_channel_hierarchy_11521_ACE_ENG;
     Task_Name=marketing_channel_hierarchy_ldg;'
     FOR SESSION VOLATILE;

-- Reading data from S3 and creating the view
create or replace temporary view marketing_channel_hierarchy_view_csv
(
    join_channel string
    , marketing_type string
    , finance_rollup string
    , marketing_channel string
    , finance_detail string
    , marketing_channel_detailed string
    , model_channel string
    , mmm_channel string
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/mta/marketing_channel_hierarchy/marketing_channel_hierarchy.csv",
    sep ",",
    header "false"
)
;
-- Creating the hive table if it doesn't exist
create table if not exists {hive_schema}.marketing_channel_hierarchy (
    join_channel string
    , marketing_type string
    , finance_rollup string
    , marketing_channel string
    , finance_detail string
    , marketing_channel_detailed string
    , model_channel string
    , mmm_channel string

)
using ORC
location 's3://{s3_bucket_root_var}/mta/marketing_channel_hierarchy/'
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table marketing_channel_hierarchy_ldg_output

select
    join_channel
    , marketing_type
    , finance_rollup
    , marketing_channel
    , finance_detail
    , marketing_channel_detailed
    , model_channel
    , mmm_channel

from marketing_channel_hierarchy_view_csv
;

-- Writing output to hive table
insert overwrite table {hive_schema}.marketing_channel_hierarchy
select

    join_channel
    , marketing_type
    , finance_rollup
    , marketing_channel
    , finance_detail
    , marketing_channel_detailed
    , model_channel
    , mmm_channel

from marketing_channel_hierarchy_view_csv
;




