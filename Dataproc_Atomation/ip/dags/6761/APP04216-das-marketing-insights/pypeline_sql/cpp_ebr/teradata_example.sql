-- This table to Test Teradata engine.
DROP TABLE {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK_CICD;
CREATE MULTISET TABLE {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK_CICD AS (
  SELECT
   *
  FROM  {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK
  )
WITH DATA;
/*
We can use multiline Comment
View results are ACP_ID updates for current date from customer and loyalty
changes could be acp change or loyalty level update.
Created by: Onehop
Supports: Customer Attribute Library
*/


DELETE FROM {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK_CICD ALL;

INSERT INTO {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK_CICD 
SELECT * FROM  {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK;

select count(*) from {database_name}.TPT_TEST_CSV_ETL_FRAMEWORK_CICD;
