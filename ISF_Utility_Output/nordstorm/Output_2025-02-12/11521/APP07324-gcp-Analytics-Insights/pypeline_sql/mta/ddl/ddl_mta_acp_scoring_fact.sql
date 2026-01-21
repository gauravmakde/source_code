SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=ddl_mta_acp_scoring_fact_11521_ACE_ENG;
     Task_Name=ddl_mta_acp_scoring_fact;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_MTA.mta_scp_scoring_fact
Team/Owner: Analytics Engineering
Date Created/Modified: 11/17/22

Note:
-- Daily refresh looking back 5 days

*/

create multiset table {mta_t2_schema}.mta_acp_scoring_fact
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    order_date_pacific DATE
    ,order_number VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sku_id VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , acp_id VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    , order_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('N.COM','N.CA','RACK.COM','RACK','RACK CANADA','FULL LINE CANADA','FULL LINE')
    , order_channelcountry  CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    , arrived_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('N.COM','N.CA','RACK.COM','RACK','RACK CANADA','FULL LINE CANADA','FULL LINE')
    , arrived_platform VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('ANDROID','MOW','IOS','WEB')
    , arrived_channelcountry CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    , mktg_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , marketing_type VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('PAID','UNPAID')
    , finance_rollup VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_detail VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_channel VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC -- utm_channel VARCHAR(64) -- max 261
    , utm_source VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC --utm_source VARCHAR(64) -- max 130
    , utm_campaign VARCHAR(2000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_term VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC -- utm_term VARCHAR(64) --max 233
    , utm_content VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC  --utm_content VARCHAR(2400) -- max 3378
    , attributed_demand FLOAT
    , attributed_pred_net FLOAT
    , attributed_units FLOAT
    , attributed_orders FLOAT
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    , is_marketplace  VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , partner_relationship_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , partner_relationship_type VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    )
primary index(order_date_pacific, acp_id, finance_detail, utm_content)
partition by RANGE_N(order_date_pacific between date '2019-01-01' and date '2025-12-31' EACH INTERVAL '1' DAY)
;

comment on {mta_t2_schema}.mta_acp_scoring_fact is 'multi-touch attribution by acp_id';



SET QUERY_BAND = NONE FOR SESSION;