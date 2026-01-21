SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=device_pricetype_shipmethod_transtype_dim_11521_ACE_ENG;
     Task_Name=price_type_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.price_type_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: Lookup table creating a unique identifier for each price_type value, which represents whether an item was purchased: at regular price, on clearance, or on promotion.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/

MERGE INTO {usl_t2_schema}.price_type_dim as tgt
USING (SELECT price_type
             ,case when price_type = 'C' then 'closeout price'
                   when price_type = 'P' then 'promo price'
                   when price_type = 'R' then 'regular price'
                   else 'unknown price type'
               end price_type_name
        FROM (select distinct price_type from t2dl_das_sales_returns.sales_and_returns_fact_base as srdb) as src)
 ON tgt.price_type = src.price_type

WHEN MATCHED THEN
 UPDATE SET price_type_name = src.price_type_name
           ,dw_sys_load_tmstp = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
 INSERT (price_type, price_type_name, dw_sys_load_tmstp)
 VALUES (src.price_type, src.price_type_name, CURRENT_TIMESTAMP);

-- NOTE: SQL treats each NULL as unique value!
DELETE FROM {usl_t2_schema}.price_type_dim
 WHERE price_type IS NULL
   AND price_type_id <> (SELECT MIN(price_type_id) FROM {usl_t2_schema}.price_type_dim WHERE price_type IS NULL);

COLLECT STATISTICS COLUMN (price_type_id, price_type) on {usl_t2_schema}.price_type_dim;


SET QUERY_BAND = NONE FOR SESSION;