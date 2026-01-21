
-- This file uses Spark sql to pull data from S3 or Hive and move it to a new s3 location in a format needed for TPT.

---- Like any SparkSQL file you can use Hive or S3 sources to read/load data to write to CSV for TPT
--- The example below is using Hive - the commented out section below it shows how you can use direct S3 sources too.

-- Example of reading data from Hive:
create or replace temporary view temp_table_name as
select  column_1
        , column_2
        , column_3
        , column_4
        , column_5
from    any_hive_schema.some_table
where   column_1 between {start_date} and {end_date}
;
--- Examples of reading data from S3:
-- create or replace temporary view temp_table_name USING parquet OPTIONS (path "s3://some_bucket/parquet_path/");
-- create or replace temporary view temp_table_name USING orc OPTIONS (path "s3://some_bucket/orc_path/");
-- create or replace temporary view temp_table_name
--         (
--         column_1              date
--         , column_2            string
--         , column_3            int
--         , column_4            decimal(18,2)
--         , column_5            decimal(18,2)
--         )
-- USING CSV
-- OPTIONS (path "s3://some_bucket/data.csv",
--         sep ",",
--         header "true");

-- Writing output to S3 CSV
-- The overwritten table sould match the "sql_table_reference" indicated on the .json file.
-- Column order should be an EXACT match to the Teradata LDG table
insert overwrite table final_table_name_ldg_output
select 
column_1
, column_2
, column_3
, column_4
, column_5
from temp_table_name
;
