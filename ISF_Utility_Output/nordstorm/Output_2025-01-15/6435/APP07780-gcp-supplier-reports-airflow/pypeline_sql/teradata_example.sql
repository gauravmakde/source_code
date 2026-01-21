SET QUERY_BAND = '
App_ID=app07780;
DAG_ID=teradata_engine_example_6435_dev_arkansas_arkansas_dags;
Task_Name=run_terdata_sql_stg_0_job_0;'
FOR SESSION VOLATILE;
ET;

-- This table to Test Teradata engine.
CREATE VOLATILE TABLE TPT_TEST_CSV_ETL_FRAMEWORK_TEMP AS (
  SELECT
   *
  FROM  {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK
  )
WITH DATA;
/*create temp table
*/
ET;
-- We are doing commit manually here
--BT; Not needed as with Auto commit off there is implicit transaction ON


DELETE FROM TPT_TEST_CSV_ETL_FRAMEWORK_TEMP;

INSERT INTO TPT_TEST_CSV_ETL_FRAMEWORK_TEMP 
SELECT * FROM  {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK;

select count(*) from TPT_TEST_CSV_ETL_FRAMEWORK_TEMP;

SET QUERY_BAND = NONE FOR SESSION;
ET;
