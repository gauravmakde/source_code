SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_sku_dim_11521_ACE_ENG;
     Task_Name=ddl_sku_dim;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.sku_dim
Team/Owner: Customer Analytics/Irene Ma
Date Created/Modified: 05/10/2023

Note:
-- What is the the purpose of the table: SKU-level lookup table containing various item-specific & product hierarchy descriptions.
-- What is the update cadence/lookback window: daily refreshment, run at 8am UTC
*/

-- For testing the table. Comment out before final merge.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'sku_dim', OUT_RETURN_MSG);

CREATE SET TABLE {usl_t2_schema}.sku_dim ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      sku_id VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      epm_style_num BIGINT,
      sku_desc VARCHAR(5000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      style_desc VARCHAR(800) CHARACTER SET UNICODE NOT CASESPECIFIC,
      brand_name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      div_desc VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dept_desc VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_desc VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sbclass_desc VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_division VARCHAR(8000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_department VARCHAR(8000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_class VARCHAR(8000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_brand VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_category_mpg VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_role_nord VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      merch_role_rack VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      merch_color VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_size VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_skutype VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      live_date DATE FORMAT 'YY/MM/DD',
      merch_price_msrp DECIMAL(15,2),
      merch_stylecolor_id VARCHAR(21) CHARACTER SET UNICODE NOT CASESPECIFIC,
      style_group_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_persona VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_occasion VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC,
      merch_type VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC,
      smart_capacity_id VARCHAR(28) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sku_segment_id VARCHAR(24370) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( sku_id );
COMMENT ON {usl_t2_schema}.sku_dim IS 'SKU-level lookup table containing various item-specific & product hierarchy descriptions.';

SET QUERY_BAND = NONE FOR SESSION;