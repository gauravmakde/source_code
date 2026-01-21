SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_funnel_events_monitor_11521_ACE_ENG;
     Task_Name=ddl_funnel_events_monitor;'
     FOR SESSION VOLATILE;

-- Table DDL taken from `show table T2DL_DAS_BIE_DEV.funnel_events_monitor`

CREATE MULTISET TABLE T2DL_DAS_BIE_DEV.funnel_events_monitor ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      event_date_pacific DATE FORMAT 'YYYY-MM-DD',
      channel VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      platform VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      channelcountry VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      product_views INTEGER,
      orders INTEGER,
      units INTEGER,
      add_to_bag_qty INTEGER,
      demand DECIMAL(12,2),
      arrived INTEGER,
      authenticated INTEGER,
      cancels INTEGER)
PRIMARY INDEX ( event_date_pacific );
SET QUERY_BAND = NONE FOR SESSION;