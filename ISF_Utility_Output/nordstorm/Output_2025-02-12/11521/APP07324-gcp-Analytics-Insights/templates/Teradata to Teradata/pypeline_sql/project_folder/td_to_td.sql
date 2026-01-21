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

*/



/*
Temp table notes here if applicable
*/
CREATE MULTISET VOLATILE TABLE temp_table_name AS (

[SQL QUERY HERE]

) 
WITH DATA
PRIMARY INDEX(column_name)
ON COMMIT PRESERVE ROWS
;



/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM    {t2_schema}.final_table_name
WHERE   column_1 >= {start_date}
AND     column_1 <= {end_date}
;


INSERT INTO {t2_schema}.final_table_name
SELECT  column_1
        , column_2
        , column_3
        , column_4
        , column_5
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM	temp_table_name
WHERE   column_1 >= {start_date}
AND     column_1 <= {end_date}
;


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
