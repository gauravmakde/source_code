/*
T2/Table Name: T2DL_DAS_NMN.campaign_attributed_daily_fact
Team/Owner: NMN / Customer Engagement Analytics
Date Created/Modified: 2023-06-15
Note:
-- To support NMN campaign reporting
-- Daily refresh
*/

-- Use drop_if_exists for testing DDL changes in development.  
-- Comment out prior to merging to production.

SET QUERY_BAND = 'App_ID=APP08162;
DAG_ID=ddl_campaign_attributed_daily_fact_11521_ACE_ENG;
Task_Name=ddl_campaign_attributed_daily_fact;' 
FOR SESSION VOLATILE;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{nmn_t2_schema}', 'campaign_attributed_daily_fact', OUT_RETURN_MSG);

CREATE MULTISET TABLE {nmn_t2_schema}.campaign_attributed_daily_fact ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      day_date DATE FORMAT 'YYYY-MM-DD',
      channel_num INTEGER,
      channel_country CHAR(2) CHARACTER SET Unicode NOT CaseSpecific Compress ('US','CA'),
      channel_banner VARCHAR(15) CHARACTER SET Unicode CaseSpecific Compress ('NORDSTROM','NORDSTROM_RACK'),
      mktg_type VARCHAR(20) CHARACTER SET Unicode CaseSpecific Compress ('BASE','PAID','UNPAID'),
      finance_rollup VARCHAR(20) CHARACTER SET Unicode CaseSpecific Compress ('AFFILIATES','BASE','DISPLAY','EMAIL','PAID_OTHER','PAID_SEARCH','SEO','SHOPPING','SOCIAL','UNPAID_OTHER'),
      finance_detail VARCHAR(30) CHARACTER SET Unicode CaseSpecific Compress ('AFFILIATES','APP_PUSH_PLANNED','APP_PUSH_TRANSACTIONAL','BASE','DISPLAY','EMAIL_MARKETING','EMAIL_TRANSACT','EMAIL_TRIGGER','PAID_MISC','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED','SEO_LOCAL','SEO_SEARCH','SEO_SHOPPING','SHOPPING','SOCIAL_ORGANIC','SOCIAL_PAID'),
      utm_source VARCHAR(200) CHARACTER SET Unicode NOT CaseSpecific,
      utm_channel VARCHAR(70) CHARACTER SET Unicode NOT CaseSpecific,
      utm_campaign VARCHAR(90) CHARACTER SET Unicode NOT CaseSpecific,
      utm_term VARCHAR(300) CHARACTER SET Unicode NOT CaseSpecific,
      utm_content VARCHAR(300) CHARACTER SET Unicode NOT CaseSpecific,
      supplier_name VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific,
      brand_name VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific,
      division VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific,
      subdivision VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific,
      department VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific,
      campaign_id VARCHAR(250) CHARACTER SET Unicode NOT CaseSpecific,
	 campaign_brand_id VARCHAR(250) CHARACTER SET Unicode NOT CaseSpecific,
      placementsio_id VARCHAR(250) CHARACTER SET Unicode NOT CaseSpecific,
      ty_product_views INTEGER,
      ty_instock_product_views INTEGER,
      ty_cart_add_units INTEGER,
      ty_order_units INTEGER,
      ty_gross_sales_amt DECIMAL(22,2),
      ty_gross_sales_units DECIMAL(16,0),
      ly_product_views DECIMAL(16,0),
      ly_instock_product_views DECIMAL(16,0),
      ly_cart_add_units DECIMAL(16,0),
      ly_order_units DECIMAL(16,0),
      ly_gross_sales_amt DECIMAL(22,2),
      ly_gross_sales_units DECIMAL(38,0),
	  dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT Current_Timestamp(6) NOT NULL)
     PRIMARY INDEX(day_date, brand_name)
     PARTITION BY Range_N(day_date BETWEEN DATE '2022-01-31' AND DATE '2030-12-31' EACH INTERVAL '1' DAY);

     -- Table Comment (STANDARD)
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_DAILY_FACT IS 'Daily NMN campaign-attributed metrics';

SET QUERY_BAND = NONE FOR SESSION;