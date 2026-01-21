SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=ddl_mta_paid_campaign_daily_11521_ACE_ENG;
     Task_Name=ddl_mta_paid_campaign_daily;'
     FOR SESSION VOLATILE;
/*
T2/Table Name: t2dl_das_mta.mta_paid_campaign_daily
Team/Owner: MTA
Date Created/Modified: 05/2023
Note:
-- This table is is the source for the MTA 4 Box and DAD dashboards.
-- Lookback window is xx days to match the upstream funnel_cost_fact lookback window.
*/

--CALL SYS_MGMT.DROP_IF_EXISTS_SP('t2dl_das_bie_dev', 'MTA_PAID_CAMPAIGN_DAILY', OUT_RETURN_MSG);

create multiset table {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     activity_date_pacific      date
     , arrived_channel          varchar(25) CHARACTER SET UNICODE NOT CASESPECIFIC
     , order_channel            varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Unknown','N.COM','RACK.COM','FULL LINE','RACK')
     , finance_detail           varchar(50) CHARACTER SET UNICODE NOT CASESPECIFIC
     , platform                 varchar(25) CHARACTER SET UNICODE NOT CASESPECIFIC
     , sourcename               varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Unknown','Nordstrom Main - 5417253570','Nordstrom','Nordstrom - Product Listing Ads - 5035034225')
     , campaign_name            varchar(2000) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Unknown')
     , campaign_id              varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Unknown')
     , adgroup_name             varchar(255) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Unknown')
     , adgroup_id               varchar(55) CHARACTER SET UNICODE NOT CASESPECIFIC
     , ad_name                  varchar(600) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Unknown')
     , ad_id                    varchar(55) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Unknown')
     , funding                  varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('ND','Unknown','EX','Persistent','Coop')
     , ar_flag                  varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Others','Brand','Acquisition','Retention','Retargeting')
     , country                  varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Unknown','US','CA')
     , funnel_type              varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('LOW','Unknown','MID','UP')
     , nmn_flag                 varchar(8) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Yes','No','Unknown')
     , device_type              varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('COMPUTER','MOBILE','Unknown')
     , cost                     DECIMAL(18, 2)
     , impressions              bigint
     , clicks                   bigint
     , conversions              float
     , conversion_value         float
     , video_views              float
     , video100                 float
     , video75                  float
     , attributed_demand        float
     , attributed_orders        float
     , attributed_units         float
     , attributed_pred_net      float
     , sessions                 float
     , bounced_sessions         float
     , session_orders           float
     ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(activity_date_pacific, platform, arrived_channel, finance_detail, adgroup_id)
partition by RANGE_N(activity_date_pacific BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {mta_t2_schema}.MTA_PAID_CAMPAIGN_DAILY IS 'Multi touch and same session order attribution alongside paid campaign costs';

SET QUERY_BAND = NONE FOR SESSION;
