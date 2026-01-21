SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=ddl_divisional_sessions_daily_11521_ACE_ENG;
     Task_Name=ddl_divisional_sessions_daily;'
     FOR SESSION VOLATILE; 

--Table Definition for T2DL_DAS_PRODUCT_FUNNEL.divisional_sessions_daily
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{product_funnel_t2_schema}', 'divisional_sessions_daily', OUT_RETURN_MSG);

CREATE MULTISET TABLE {product_funnel_t2_schema}.divisional_sessions_daily,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      session_id                                         VARCHAR(255), 
      activity_date_pacific                              DATE FORMAT 'YY/MM/DD',
      channel                                            VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      platform                                           VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      channelcountry                                     VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
      division                                           VARCHAR(172) CHARACTER SET UNICODE NOT CASESPECIFIC,
      subdiv                                             VARCHAR(172) CHARACTER SET UNICODE NOT CASESPECIFIC,
      department                                         VARCHAR(172) CHARACTER SET UNICODE NOT CASESPECIFIC,
      regular_price_amt                                  DECIMAL(15,2),
      current_price_amt                                  DECIMAL(15,2),
      current_price_type                                 VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC,
      product_views                                      DECIMAL(38,2),
      product_viewed_session                             INTEGER,
      add_to_bag_quantity                                INTEGER,
      cart_add_session                                   INTEGER,
      order_demand                                       DECIMAL(38,2),
      order_quantity                                     INTEGER,
      web_order_session                                  INTEGER,
      dw_sys_load_tmstp                                  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
     )
PRIMARY INDEX ( activity_date_pacific, session_id, channelcountry, platform )
PARTITION BY RANGE_N(activity_date_pacific  BETWEEN DATE '2022-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY );

SET QUERY_BAND = NONE FOR SESSION;