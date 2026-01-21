SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=ddl_mta_utm_demand_agg_11521_ACE_ENG;
     Task_Name=ddl_mta_utm_demand_agg;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_MTA.mta_utm_demand_agg
Team/Owner: Analytics Engineering
Date Created/Modified: 1/11/23

Note:
-- Daily refresh looking back 5 days

*/

create multiset table {mta_t2_schema}.MTA_UTM_DEMAND_AGG
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     order_date_pacific       	DATE
    , order_channel             VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('N.COM','N.CA','RACK.COM','RACK','RACK CANADA','FULL LINE CANADA','FULL LINE')
    , order_channelcountry      CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    , arrived_channel           VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('N.COM','N.CA','RACK.COM','RACK','RACK CANADA','FULL LINE CANADA','FULL LINE')
    , arrived_platform          VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('ANDROID','MOW','IOS','WEB')
    , arrived_channelcountry    CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    , mktg_type                 VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
    , marketing_type            VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('PAID','UNPAID')
    , finance_rollup            VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
    , finance_detail            VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , is_nmn                    INTEGER
    , utm_channel               VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
    , utm_source                VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
    , utm_campaign              VARCHAR(2000) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
    , utm_term                  VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS
    , utm_content               VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , loyalty_type              VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('NON-LOYALTY','CARDMEMBER','MEMBER')
    , loyalty_level             INTEGER
    , currency                  CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('CAD','USD')
    , attributed_demand         FLOAT
    , attributed_pred_net       FLOAT
    , attributed_units          FLOAT
    , attributed_orders         FLOAT
    , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(order_date_pacific ,finance_detail, utm_content)
partition by RANGE_N(order_date_pacific BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {mta_t2_schema}.MTA_UTM_DEMAND_AGG IS 'multi-touch attribution by day';

SET QUERY_BAND = NONE FOR SESSION;