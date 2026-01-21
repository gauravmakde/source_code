SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=utm_channel_lookup_11521_ACE_ENG;
     Task_Name=utm_channel_lookup_ldg;'
     FOR SESSION VOLATILE;

-- Reading data from S3 and creating the view
create or replace temporary view utm_channel_lookup_view_csv
(
    bi_channel string
    , utm_mkt_chnl string
    , strategy string
    , funding_type string
    , utm_prefix string
    , funnel_type string
    , utm_suffix string
    , create_timestamp date
    , update_timestamp date
)
USING CSV
OPTIONS (
    path "s3://analytics-insights-triggers/mta/utm_channel_lookup/",
    sep ",",
    header "true"
)
;

-- Creating the hive table if it doesn't exist
create table if not exists {hive_schema}.utm_channel_lookup (
    bi_channel string
    , utm_mkt_chnl string
    , strategy string
    , funding_type string
    , utm_prefix string
    , funnel_type string
    , utm_suffix string
    , create_timestamp date
    , update_timestamp date

)
using ORC
location 's3://{s3_bucket_root_var}/mta/utm_channel_lookup/'
;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table utm_channel_lookup_ldg_output

select
    bi_channel
    , utm_mkt_chnl
    , strategy
    , funding_type
    , utm_prefix
    , funnel_type
    , utm_suffix
    , create_timestamp
    , update_timestamp

from utm_channel_lookup_view_csv
;

-- Writing output to hive table
insert overwrite table {hive_schema}.utm_channel_lookup
select
    bi_channel
    , utm_mkt_chnl
    , strategy
    , funding_type
    , utm_prefix
    , funnel_type
    , utm_suffix
    , create_timestamp
    , update_timestamp

from utm_channel_lookup_view_csv
;




