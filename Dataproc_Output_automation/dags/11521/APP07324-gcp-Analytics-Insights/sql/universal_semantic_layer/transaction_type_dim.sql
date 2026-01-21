/* SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=device_pricetype_shipmethod_transtype_dim_11521_ACE_ENG;
     Task_Name=transaction_type_dim;'
     FOR SESSION VOLATILE;*/

/*
T2/Table Name: T2DL_DAS_USL.transaction_type_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each transaction_type value, which represents whether a transaction was a retail or service transaction.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/



MERGE INTO {{params.gcp_project_id}}.{{params.usl_t2_schema}}.transaction_type_dim AS tgt
USING (SELECT transaction_type,
  CONCAT(LOWER(transaction_type), ' transaction') AS transaction_type_name
 FROM (SELECT DISTINCT transaction_type
   FROM {{params.gcp_project_id}}.t2dl_das_sales_returns.sales_and_returns_fact_base AS srdb) AS t1) AS src
ON LOWER(tgt.transaction_type) = LOWER(src.transaction_type)
WHEN MATCHED 
THEN UPDATE SET
 transaction_type_name = src.transaction_type_name,
 dw_sys_load_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED 
THEN INSERT (transaction_type, transaction_type_name, dw_sys_load_tmstp) VALUES(src.transaction_type,
 src.transaction_type_name, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));


-- NOTE: SQL treats each NULL as unique value!
DELETE FROM {{params.gcp_project_id}}.{{params.usl_t2_schema}}.transaction_type_dim
WHERE transaction_type IS NULL 
AND transaction_type_id <> (SELECT MIN(transaction_type_id)
   FROM {{params.gcp_project_id}}.{{params.usl_t2_schema}}.transaction_type_dim
   WHERE transaction_type IS NULL);


--COLLECT   STATISTICS COLUMN (transaction_type_id, transaction_type) on T2DL_DAS_USL.transaction_type_dim



--SET QUERY_BAND = NONE FOR SESSION;
