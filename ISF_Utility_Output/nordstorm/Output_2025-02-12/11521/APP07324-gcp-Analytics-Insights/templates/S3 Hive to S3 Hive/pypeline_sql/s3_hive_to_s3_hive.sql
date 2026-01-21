-- Definine new Hive table for output
create table if not exists {hive_schema}.hive_table_name
(
    event_date_pacific date
    , column_1 string
    , column_2 string
    , column_3 integer
    , column_4 integer
    , column_5 string
    , my_date_partition_col date
)
using PARQUET
location 's3://{s3_bucket_root_var}/hive_table_name/'
partitioned by (my_date_partition_col);

-- Can also use a S3 CSV or multiple CSV's - use wildcard (e.g., `s3://my-bucket/csv_dir/*.csv`)
    -- CREATE OR REPLACE TEMPORARY VIEW data_view
    -- (
    -- event_date_pacific date
    -- , column_1 string
    -- , column_2 string
    -- , column_3 integer
    -- , column_4 integer
    -- , column_5 string
    -- , my_date_partition_col date
    -- )
    -- USING CSV
    -- OPTIONS (
    --     path "s3://my-bucket/my.csv",
    --     sep ",",
    --     header "true"
    -- )
    -- ;

    -- create or replace temporary view temp_table_name as 
    -- select * from data_view
    -- where   event_date_pacific between {start_date} and {end_date}
    -- ;

-- Reading data from upstream Hive table
create or replace temporary view temp_table_name as
select  event_date_pacific
        , column_1
        , column_2
        , column_3
        , column_4
        , column_5
        , my_date_partition_col
from    acp_event.upstream_source_hive_table
where   event_date_pacific between {start_date} and {end_date}
;

-- Writing output to new Hive table
insert overwrite table {hive_schema}.hive_table_name partition (my_date_partition_col)
select * from temp_table_name
;

-- sync partitions on hive table
MSCK REPAIR TABLE {hive_schema}.hive_table_name;