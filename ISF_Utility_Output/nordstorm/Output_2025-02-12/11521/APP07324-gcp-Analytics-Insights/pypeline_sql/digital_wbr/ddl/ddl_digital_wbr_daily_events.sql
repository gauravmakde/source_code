SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_digital_wbr_daily_events_11521_ACE_ENG;
     Task_Name=ddl_digital_wbr_daily_events;'
     FOR SESSION VOLATILE;

/*
Table Definition for T2DL_DAS_SITE_MERCH.digital_wbr_daily_events
*/
CREATE MULTISET TABLE {site_merch_t2_schema}.digital_wbr_daily_events ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      cust_id_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channel_country VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      platform VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      day_dt_pacific DATE FORMAT 'YY/MM/DD',
      referrer VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      referral_event VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      referral_detail_header_1 VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      referral_detail_header_2 VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      referral_detail VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      url_path VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      next_event VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      next_event_detail_header_1 VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      next_event_detail_header_2 VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      next_event_detail VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC,
      page_interaction_ind BIGINT,
      sort_ind BIGINT,
      filter_ind BIGINT,
      pagination_ind BIGINT,
      last_event_ind BIGINT,
      visitors BIGINT,
      cust_order_demand FLOAT,
      cust_order_quantity BIGINT,
      cust_orders BIGINT,
      ordering_visitors BIGINT,
      page_views BIGINT,
      page_clicks BIGINT, 
      sort_clicks BIGINT,
      filter_clicks BIGINT, 
      pagination_clicks BIGINT,
      pss_clicks BIGINT,
      dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( day_dt_pacific, url_path )
PARTITION BY RANGE_N(day_dt_pacific  BETWEEN DATE '2018-01-01' AND DATE'2026-01-01' EACH INTERVAL '1' DAY );

-- table comment
COMMENT ON  {site_merch_t2_schema}.digital_wbr_daily_events IS 'Category page events and metrics for digital channels'; 


SET QUERY_BAND = NONE FOR SESSION;