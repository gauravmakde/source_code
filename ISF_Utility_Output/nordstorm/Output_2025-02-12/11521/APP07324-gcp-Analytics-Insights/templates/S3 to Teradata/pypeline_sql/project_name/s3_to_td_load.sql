/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=your_app_id;
     DAG_ID=your_dag_name_11521_ACE_ENG;
     Task_Name=your_sql_file_name_without_extension;'
     FOR SESSION VOLATILE;



-- Reading data from S3
create or replace temporary view temp_table_name USING orc OPTIONS (path "s3:/ S3 PATH ");
 
create or replace temporary view temp_table_name_2 as
select  column_1
        , column_2
        , column_3
        , column_4
        , column_5
from    temp_table_name
where   column_1 between {start_date} and {end_date}
;

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert overwrite table table_name_output
select * from temp_table_name_2
;
