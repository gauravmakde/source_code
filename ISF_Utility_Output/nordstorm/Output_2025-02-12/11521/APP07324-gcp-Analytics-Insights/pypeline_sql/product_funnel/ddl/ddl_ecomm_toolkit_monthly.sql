SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=ddl_ecomm_toolkit_monthly_11521_ACE_ENG;
     Task_Name=ddl_ecomm_toolkit_monthly;'
     FOR SESSION VOLATILE;


-- FILENAME: ecomm_toolkit_monthly.sql
-- GOAL: Roll up digital product funnel to monthly level for Ecom Dashboard
-- AUTHOR: Meghan Hickey (meghan.d.hickey@nordstrom.com)

CREATE MULTISET TABLE {product_funnel_t2_schema}.ecomm_toolkit_monthly ,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      mth_idnt INTEGER,
      mth_label VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dept_no INTEGER,
      dept_label VARCHAR(172) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subdiv_label VARCHAR(172) CHARACTER SET UNICODE NOT CASESPECIFIC,
      div_label VARCHAR(172) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_label VARCHAR(82) CHARACTER SET UNICODE NOT CASESPECIFIC,
      supp_no VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      brand VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      class_no INTEGER,
      product_views_ty DECIMAL(38,2),
      items_sold_ty INTEGER,
      instock_views_ty DECIMAL(38,6),
      scored_views_ty DECIMAL(38,6),
      web_demand_ty DECIMAL(38,2),
      product_views_ly DECIMAL(38,2),
      items_sold_ly INTEGER,
      instock_views_ly DECIMAL(38,6),
      scored_views_ly DECIMAL(38,6),
      web_demand_ly DECIMAL(38,2),
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )
PRIMARY INDEX ( mth_idnt ,dept_label ,brand ,class_no );
SET QUERY_BAND = NONE FOR SESSION;