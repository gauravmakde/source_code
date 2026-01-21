SET QUERY_BAND = 'App_ID=APP09330;
     DAG_ID=ddl_customer_session_data_11521_ACE_ENG;
     Task_Name=ddl_customer_session_data;'
     FOR SESSION VOLATILE;


/*

T2/Table Name: {sessions_t2_schema}.customer_session_data
Team/Owner: MOA
Date Modified: 2024-04-29

-- To support the SESSION X CUSTOMER Dashboard-Consolidated Version

*/


CREATE MULTISET TABLE {sessions_t2_schema}.customer_session_data ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      acp_id VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  DAY_DT DATE FORMAT 'YY/MM/DD',
      channel VARCHAR(14) CHARACTER SET UNICODE NOT CASESPECIFIC,
      mrkt_type VARCHAR(140) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_rollup VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
      finance_detail VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      engagement_cohort VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LOYALTY_STATUS VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      total_sessions FLOAT,
      bounced_sessions FLOAT,
      product_views FLOAT,
      cart_adds FLOAT,
      web_orders FLOAT,
      web_demands FLOAT,
	  web_ordered_units FLOAT,
	  web_demand_usd FLOAT,
      dw_sys_load_tmstp TIMESTAMP(6) DEFAULT Current_Timestamp(6) NOT NULL
      )
PRIMARY INDEX ( acp_id, DAY_DT  ,channel ,
mrkt_type ,finance_rollup ,finance_detail ,engagement_cohort ,
LOYALTY_STATUS )
PARTITION BY Range_N(day_dt BETWEEN DATE '2021-01-31' AND DATE '2028-12-31' EACH INTERVAL '1' DAY);

COMMENT ON  {sessions_t2_schema}.customer_session_data IS 'SESSION X CUSTOMER Dashboard-Consolidated Version';

COLLECT STATISTICS  COLUMN (DAY_DT, CHANNEL, MRKT_TYPE, FINANCE_ROLLUP, LOYALTY_STATUS, ENGAGEMENT_COHORT), 
                    COLUMN (DAY_DT)  
on {sessions_t2_schema}.customer_session_data;

SET QUERY_BAND = NONE FOR SESSION;
