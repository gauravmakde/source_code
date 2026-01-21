SET QUERY_BAND = 'App_ID=APP07324;
    DAG_ID=ddl_fls_assortment_pilot_11521_ACE_ENG;
    Task_Name=ddl_fls_assortment_pilot_trans_base;'
    FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_ccs_categories.fls_assortment_pilot_trans_base
Team/Owner: Merch Insights / Thomas Peterson
Date Created/Modified: 6/19/2024
*/

-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'fls_assortment_pilot_trans_base', OUT_RETURN_MSG);

CREATE MULTISET TABLE {shoe_categories_t2_schema}.fls_assortment_pilot_trans_base
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
     (
      acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      store_num INTEGER,
      ringing_store_num INTEGER,
      fulfilling_store_num INTEGER,
      intent_store_num INTEGER,
      wac_store_num INTEGER,
      sale_date DATE FORMAT 'YY/MM/DD',
      trip_id VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      global_tran_id BIGINT,
      business_day_date DATE FORMAT 'YY/MM/DD',
      order_num VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      positive_sale_flag BYTEINT,
      negative_sale_flag BYTEINT,
      sku_num VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      units DECIMAL(8,0),
      sale_amt DECIMAL(12,2),
      tran_type_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC,
      tran_line_id INTEGER,
      line_item_seq_num SMALLINT,
      line_item_activity_type_desc VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      line_item_fulfillment_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      line_item_order_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC,
      employee_discount_flag CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC,
      promo_flag BYTEINT,
      cc_num VARCHAR(149) CHARACTER SET UNICODE NOT CASESPECIFIC,
      unit_price DECIMAL(38,2),
      dma VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ntn_flag BYTEINT,
      nordy_level VARCHAR(512) CHARACTER SET LATIN NOT CASESPECIFIC,
      customer_segment VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC,
      weighted_average_cost DECIMAL(38,9),
      rp_flag BYTEINT,
	  dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( store_num ,sale_date ,trip_id ,sku_num );

SET QUERY_BAND = NONE FOR SESSION;