/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=your_app_id;
     DAG_ID=your_dag_name_11521_ACE_ENG;
     Task_Name=your_sql_file_name_without_extension;'
     FOR SESSION VOLATILE;

/*

T2/View Name:
Team/Owner:
Date Created/Modified:

Note:
-- What is the the purpose of the view

*/

CREATE OR REPLACE VIEW {t2_schema}.view_name_vw
AS
LOCK ROW FOR ACCESS

  [SQL QUERY HERE]
;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
