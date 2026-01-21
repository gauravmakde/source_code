SET QUERY_BAND = 'App_ID=APP08134;
     DAG_ID=ddl_ssa_mkt_attr_fact_11521_ACE_ENG;
     Task_Name=ddl_ssa_mkt_attr_fact;'
     FOR SESSION VOLATILE;

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'ssa_mkt_attr_fact', OUT_RETURN_MSG);

--T2DL_DAS_MTA.ssa_mkt_attr_fact
CREATE MULTISET TABLE {mta_t2_schema}.SSA_MKT_ATTR_FACT
     , FALLBACK
     , NO BEFORE JOURNAL
     , NO AFTER JOURNAL
     , CHECKSUM = DEFAULT
     , DEFAULT MERGEBLOCKRATIO
(
    activity_date_pacific DATE FORMAT 'YYYY-MM-DD' NOT NULL
    , channelcountry VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA','UNKNOWN')
    , channel VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('UNKNOWN','JWN','NORDSTROM','NORDSTROM_RACK','HAUTELOOK','TRUNK_CLUB')
    , experience VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('UNKNOWN','WEB','MOW','IOS','ANDROID','POS','IN_STORE_DIGITAL','CSR_STORE','CSR_APP','CSR_PHONE','BACKEND_SERVICE','RPOS','THIRD_PARTY_VENDOR')
    , session_id VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , referrer VARCHAR(6000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , siteid VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_source VARCHAR(2000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_medium VARCHAR(2000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sp_source VARCHAR(1000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_channel VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_campaign VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sp_campaign VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_term VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_content VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , gclid VARCHAR(3000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , bounce_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Y', 'N')
    , funnel_type VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Up', 'Low', 'Mid')
    , web_orders INT
    , web_demand_usd DECIMAL(15, 2)
    , session_type VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('ARRIVED','NON_ARRIVED')
    , mrkt_touch CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Y', 'N')
    , mrkt_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dw_batch_date DATE FORMAT 'YYYY-MM-DD' NOT NULL
    , dw_sys_updt_tmstp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
)
PRIMARY INDEX (session_id)
PARTITION BY RANGE_N(activity_date_pacific BETWEEN DATE '2022-01-01' AND DATE '2025-01-01' EACH INTERVAL '1' DAY)
;

--Table Comments
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact IS 'marketing attribution of 1st arrived sessions by mapping to different granularity level (utm_term, utm_channel, utm_campaign, mrkt_type, finance_rollup, finance_detail)';

--Column Comments
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.activity_date_pacific IS 'Date that the session was triggered';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.session_id IS 'Session id';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.channelcountry IS 'Country from where the sessions were triggered, i.e, US or CA';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.channel IS 'Business line/unit to which the sessions were triggered, i.e, TRUNK_CLUB,NORDSTROM_RACK,NORDSTROM,HAUTELOOK';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.experience IS 'Platform through which the sessions were triggered (ex:DESKTOP_WEB, IOS_APP, MOBILE_WEB)';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.siteid IS 'ID for source site';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.utm_term IS 'channel or platform. Contain keywords like, IOS';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.utm_channel IS 'UTM parameters defined by marketing campaigns. utm_channel is in the form funnel_fundingtype_channel (ex:low_nd_affiliates)';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.utm_campaign IS 'Identifies a specific product promotion or strategic campaign';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.utm_source IS 'The individual site within that channel.For example, Facebook would be one of the sources within your Social medium for any unpaid links that you post to Facebook.';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.utm_medium IS 'The individual site within that channel.For example, Social medium for any unpaid links that you post to Facebook.';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.utm_content IS 'This is an optional field. If you have multiple links in the same campaign, like two links in the same email, you can fill in this value so you can differentiate them. For most marketers, this data is more detailed than they really need.';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.sp_campaign IS 'Single-player campaign';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.sp_source IS 'Single-player source';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.gclid IS 'A parameter passed in the URL with ad clicks, to identify the campaign and other attributes of the click associated with the ad for ad tracking and campaign attribution';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.funnel_type IS 'Up, Low, or Mid of the Funnel';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.bounce_ind IS 'A flag to identify if a sessions bounced';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.web_orders IS 'Orders count per session';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.web_demand_usd IS 'Value of the order purchased';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.session_type IS 'Type of sessions, if those sessions have an arrived event or not';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.mrkt_touch IS 'A flag to identify whether the sessions were through marketing or were from base';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.mrkt_type IS 'High level marketing type information. 3 potential values: PAID, UNPAID, BASE';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.finance_rollup IS 'Finance defined granularity one step down from marketing type';
COMMENT ON  {mta_t2_schema}.ssa_mkt_attr_fact.finance_detail IS 'Finance defined granularity one step down from finance rollup';

SET QUERY_BAND = NONE FOR SESSION;
