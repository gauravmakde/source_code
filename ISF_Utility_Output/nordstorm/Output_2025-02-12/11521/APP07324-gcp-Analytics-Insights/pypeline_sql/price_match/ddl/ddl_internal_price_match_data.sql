/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09035;
     DAG_ID=ddl_internal_price_match_data_11521_ACE_ENG;
     Task_Name=internal_price_match_data;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_PRICE_MATCHING
Team/Owner: Nicole Miao
Date Created/Modified: Dec 4, 2023

Note:
-- What is the the purpose of the table
-- What is the update cadence/lookback window

*/


-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'INTERNAL_PRICE_MATCHING', OUT_RETURN_MSG);

create multiset table {price_matching_t2_schema}.INTERNAL_PRICE_MATCHING
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      global_tran_id BIGINT,
      sale_order_num BIGINT,
      return_order_num BIGINT,
      business_unit_desc VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      line_item_seq_num SMALLINT,
      sku_num VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
      activity_date DATE FORMAT 'YY/MM/DD',
      shipped_sales DECIMAL(12,2),
      regular_sales DECIMAL(12,2),
      promo_sales DECIMAL(12,2),
      return_sales DECIMAL(38,2),
      shipped_qty DECIMAL(8,0),
      return_qty DECIMAL(8,0),
      price_match_ind BYTEINT,
      promo_name VARCHAR(2000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      acquired_customers VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(global_tran_id,line_item_seq_num)
PARTITION BY RANGE_N(activity_date BETWEEN DATE '2022-01-30' AND DATE '2025-02-01' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {price_matching_t2_schema}.INTERNAL_PRICE_MATCHING IS 'The internal price matching skus provided by brands or buyers';



/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;


