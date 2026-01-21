SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=ddl_mkt_utm_agg_11521_ACE_ENG;
     Task_Name=ddl_mkt_utm_agg;'
     FOR SESSION VOLATILE;

/******************************************************************************

Table definition for T2DL_DAS_MTA.mkt_utm_agg

*******************************************************************************/

CREATE MULTISET TABLE {mta_t2_schema}.mkt_utm_agg
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    (
    activity_date_pacific DATE,
    channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('RACK.COM','FULL LINE', 'N.COM','N.CA','RACK CANADA','RACK','FULL LINE CANADA'),
    channelcountry  CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('US','CA'),
    arrived_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('N.COM','R.COM','NULL'),
    device_type VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('ANDROID','MOW','IOS','WEB','UNKNOWN'),
    marketing_type VARCHAR(12) CHARACTER SET UNICODE NOT CASESPECIFIC,
    finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    utm_channel VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC,
    utm_source VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
    utm_medium VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
    funnel_type VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Up', 'Low', 'Mid','BASE','UNATTRIBUTED'),
    utm_campaign VARCHAR(2000) CHARACTER SET UNICODE NOT CASESPECIFIC,
    utm_term VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC,
    utm_content VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
    NMN VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('No', 'Yes'),
    gross FLOAT,
    net_sales FLOAT,
    units FLOAT,
    orders FLOAT,
    session_orders FLOAT,
	session_demand FLOAT,
    sessions FLOAT,
    bounced_sessions FLOAT
    )
PRIMARY INDEX(activity_date_pacific, utm_content, utm_term)
PARTITION BY RANGE_N(activity_date_pacific BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;


-- table comment
COMMENT ON  {mta_t2_schema}.mkt_utm_agg IS 'Daily summary of MTA data and SSA data by UTM parameters for Marketing Performance By UTM dashboard';
--Column comments
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.activity_date_pacific IS 'Order date';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.channel IS 'Channel the order is attributed to';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.channelcountry IS 'Country the order was made from';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.arrived_channel IS 'Channel of the marketing touch';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.device_type IS 'Platform the marketing touch came from';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.marketing_type IS 'Marketing touch type (paid or unpaid)';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.finance_rollup IS 'Finance rollup the order is attributed to';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.finance_detail IS 'Finance detail the order is attributed to';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.utm_channel IS 'UTM channel of the marketing touch';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.utm_source IS 'UTM source of the marketing touch';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.utm_medium IS 'UTM medium of the marketing touch';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.funnel_type IS 'Funnel Type of the marketing touch';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.utm_campaign IS 'UTM campaign of the marketing touch';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.NMN IS 'Was the marketing touch from a Nordstrom Media Network campaign?';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.utm_content IS 'UTM content of the marketing touch';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.utm_term IS 'UTM term of the marketing touch';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.gross IS 'Attributed Demand from 4 box MTA';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.net_sales IS 'Attributed predicted net sales from 4 box MTA';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.units IS 'Attributed units from 4 box MTA';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.orders IS 'Attributed orders from 4 box MTA';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.session_orders IS 'Orders from sessions from SSA';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.session_demand IS 'Demand from sessions from SSA';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.bounced_sessions IS 'Total amount of sessions that were bounced from SSA';
COMMENT ON  {mta_t2_schema}.mkt_utm_agg.sessions IS 'Total amount of sessions from SSA';

SET QUERY_BAND = NONE FOR SESSION;
