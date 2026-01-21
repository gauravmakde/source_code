 /*
Table Name: t2dl_das_store_sales_forecasting.receipts_agg
Team/Owner: tech_ffp_analytics/Matthew Bond

Notes:
creates intermediate table aggregating historic and plan weekly receipts units by store for the store sales forecast
https://tableau.nordstrom.com/#/site/AS/views/StoreSalesForecast/StoreVSChannel?:iid=1
*/

SET QUERY_BAND = 'App_ID=APP08569;
     DAG_ID=ddl_ssf_receipts_agg_11521_ACE_ENG;
     Task_Name=ddl_receipts_agg;'
     FOR SESSION VOLATILE;

-- Use drop_if_exists for testing DDL changes in development.  
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{ssf_t2_schema}', 'receipts_agg', OUT_RETURN_MSG);

 CREATE MULTISET TABLE {ssf_t2_schema}.receipts_agg,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
			store_num             SMALLINT,
      wk_idnt               INTEGER,
     	receipts_units       	DECIMAL(10,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
     )
     PRIMARY INDEX (store_num, wk_idnt)
     PARTITION BY RANGE_N(wk_idnt  BETWEEN 201901 AND 202552 EACH 1);
     
-- Table Comment (STANDARD)
COMMENT ON  {ssf_t2_schema}.receipts_agg IS 'past (actual) and future (plan) receipt units by store number';

     
SET QUERY_BAND = NONE FOR SESSION;  