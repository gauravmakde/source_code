SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=digital_wbr_daily_events_11521_ACE_ENG;
     Task_Name=ddl_digital_wbr_daily_events_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_SITE_MERCH.digital_wbr_daily_events_ldg
Team/Owner: Sara Scott
Date Created/Modified: 02/14/2023 by AE

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/



CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{site_merch_t2_schema}', 'digital_wbr_daily_events_ldg', OUT_RETURN_MSG);

CREATE MULTISET TABLE {site_merch_t2_schema}.digital_wbr_daily_events_ldg ,FALLBACK ,
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
      pss_clicks BIGINT)
PRIMARY INDEX ( day_dt_pacific, url_path )
PARTITION BY RANGE_N(day_dt_pacific  BETWEEN DATE '2018-01-01' AND DATE'2026-01-01' EACH INTERVAL '1' DAY );

SET QUERY_BAND = NONE FOR SESSION;