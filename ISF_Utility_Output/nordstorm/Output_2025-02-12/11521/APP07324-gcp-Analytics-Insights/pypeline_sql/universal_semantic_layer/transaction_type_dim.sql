SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=device_pricetype_shipmethod_transtype_dim_11521_ACE_ENG;
     Task_Name=transaction_type_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.transaction_type_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each transaction_type value, which represents whether a transaction was a retail or service transaction.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/

MERGE INTO {usl_t2_schema}.transaction_type_dim as tgt
USING (SELECT transaction_type
             ,concat(lower(transaction_type),' transaction') as transaction_type_name
        FROM (select distinct transaction_type from t2dl_das_sales_returns.sales_and_returns_fact_base as srdb) as src)
 ON tgt.transaction_type = src.transaction_type

WHEN MATCHED THEN
 UPDATE SET transaction_type_name = src.transaction_type_name
           ,dw_sys_load_tmstp = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
 INSERT (transaction_type, transaction_type_name, dw_sys_load_tmstp)
 VALUES (src.transaction_type, src.transaction_type_name, CURRENT_TIMESTAMP);

-- NOTE: SQL treats each NULL as unique value!
DELETE FROM {usl_t2_schema}.transaction_type_dim
 WHERE transaction_type IS NULL
   AND transaction_type_id <> (SELECT MIN(transaction_type_id) FROM {usl_t2_schema}.transaction_type_dim WHERE transaction_type IS NULL);

COLLECT STATISTICS COLUMN (transaction_type_id, transaction_type) on {usl_t2_schema}.transaction_type_dim;


SET QUERY_BAND = NONE FOR SESSION;