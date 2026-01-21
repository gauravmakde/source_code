--SQL script must begin QUERY_BAND SETTINGS
SET QUERY_BAND = 'App_ID=your_app_id;
     DAG_ID=your_dag_name_11521_ACE_ENG;
     Task_Name=your_sql_file_name_without_extension;'
     FOR SESSION VOLATILE;

-- Definine new Hive table for output
create table if not exists {hive_schema}.hive_table_name
(
    event_date_pacific date
    , column_1 string
    , column_2 string
    , column_3 integer
    , column_4 integer
    , column_5 string
    , partition_date date
)
using PARQUET
location 's3://{s3_bucket_root_var}/hive_table_name/'
partitioned by (partition_date);

-- Reading data from upstream Hive table
create or replace temporary view temp_table_name as
select  event_date_pacific
        , column_1
        , column_2
        , column_3
        , column_4
        , column_5
        , event_date_pacific as partition_date
from    acp_event.upstream_source_hive_table
where   event_date_pacific between {start_date} and {end_date}
;

-- Writing output to new Hive table
insert overwrite table {hive_schema}.hive_table_name partition (partition_date)
select * from temp_table_name
;

-- sync partitions on hive table
MSCK REPAIR TABLE {hive_schema}.hive_table_name;

-- Writing output to teradata landing table from hive table to reduce memory load
-- This should match the "sql_table_reference" indicated on the .json file.
insert into table table_name_output
select * from {hive_schema}.hive_table_name
where event_date_pacific between {start_date} and {end_date}
;
