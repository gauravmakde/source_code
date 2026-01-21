SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=ddl_retail_tran_price_type_fact_11521_ACE_ENG;
     Task_Name=ddl_retail_tran_price_type_fact;'
     FOR SESSION VOLATILE;

-- T2DL_DAS_SALES_RETURNS.RETAIL_TRAN_PRICE_TYPE_FACT

CREATE MULTISET TABLE {sales_returns_t2_schema}.RETAIL_TRAN_PRICE_TYPE_FACT ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     ( 
      business_day_date DATE FORMAT 'yyyy-mm-dd',
      global_tran_id BIGINT,
      line_item_seq_num SMALLINT,
      regular_price_amt float,  
      price_type CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('R','P','C'),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
      compare_price_amt float,
      promo_tran_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
      price_type_source CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC 
      COMPRESS ('ND','NP','MD','MP', 'TR','CT','DF','RS','RR','RN','RC'))             
PRIMARY INDEX (business_day_date, global_tran_id ,line_item_seq_num )
PARTITION BY RANGE_N(business_day_date  BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY ,
 NO RANGE);
SET QUERY_BAND = NONE FOR SESSION;