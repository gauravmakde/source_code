/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=your_app_id;
     DAG_ID=your_dag_name_11521_ACE_ENG;
     Task_Name=your_sql_file_name_without_extension;'
     FOR SESSION VOLATILE;


/*

T2/Table Name:
Team/Owner:
Date Created/Modified:

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

Teradata SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/
-- collect stats on ldg table
COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (column_name), -- column names used for primary index
                    COLUMN (column_name)  -- column names used for partition
on {t2_schema}.final_table_name_ldg
;

delete
from    {t2_schema}.final_table_name
where   order_date_utc between {start_date} and {end_date}
;

insert into {t2_schema}.final_table_name
select  column_1
        , column_2
        , column_3
        , column_4
        , column_5
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    {t2_schema}.final_table_name_ldg
where   order_date_utc between {start_date} and {end_date}
;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
