SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=ddl_product_price_funnel_daily_11521_ACE_ENG;
     Task_Name=ddl_product_price_funnel_daily;'
     FOR SESSION VOLATILE;

-- Product Price Funnel Daily
-- Table Definition for T2DL_DAS_PRODUCT_FUNNEL.product_price_funnel_daily

--   -- Creates layer on top of base PFD that includes additional non-hierarchy merch dimension
--   -- Includes price amount and type, RP indicators, and in-stock rates
--

CREATE MULTISET TABLE {product_funnel_t2_schema}.product_price_funnel_daily ,
     FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1,
     LOG
     (
      event_date_pacific DATE FORMAT 'YY/MM/DD',
      channelcountry VARCHAR(10),
      channel VARCHAR(50),
      platform VARCHAR(50),
      site_source VARCHAR(20),
      web_style_num INTEGER,
      rms_sku_num VARCHAR(10),
      rp_ind BYTEINT,
      averagerating FLOAT,
      review_count INTEGER,
      order_quantity INTEGER,
      order_demand DECIMAL(12,2),
      order_sessions INTEGER,
      add_to_bag_quantity INTEGER,
      add_to_bag_sessions INTEGER,
      product_views DECIMAL(12,2),
      product_view_sessions DECIMAL(12,2),
      current_price_type CHAR(1),
      current_price_amt DECIMAL(15,2),
      regular_price_amt DECIMAL(15,2),
      pct_instock DECIMAL(38,4),
      event_type VARCHAR(15),
      event_name VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX ( event_date_pacific ,channel ,rms_sku_num );
SET QUERY_BAND = NONE FOR SESSION;