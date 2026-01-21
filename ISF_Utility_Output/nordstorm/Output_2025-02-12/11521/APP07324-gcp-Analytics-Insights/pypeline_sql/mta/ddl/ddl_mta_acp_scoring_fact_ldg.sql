SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=mta_11521_ACE_ENG;
     Task_Name=ddl_mta_acp_scoring_fact_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_MTA.mta_scp_scoring_fact_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 11/17/22

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

The landing table does not match the ddl for the final T2 table as
additional transformations will happen in the mta_acp_scoring_fact.sql file

*/

call SYS_MGMT.DROP_IF_EXISTS_SP ('{mta_t2_schema}', 'mta_acp_scoring_fact_ldg', OUT_RETURN_MSG);

create multiset table {mta_t2_schema}.mta_acp_scoring_fact_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    acp_id VARCHAR(64) CHARACTER SET UNICODE NOT CASESPECIFIC
    , order_number VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , order_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('N.COM','N.CA','RACK.COM','RACK','RACK CANADA','FULL LINE CANADA','FULL LINE')
    , order_channelcountry  CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    , sku_id VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , arrived_event_id VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    , arrived_date_pacific DATE
    , arrived_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('N.COM','N.CA','RACK.COM','RACK','RACK CANADA','FULL LINE CANADA','FULL LINE')
    , arrived_channelcountry CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US','CA')
    , arrived_platform VARCHAR(16) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('ANDROID','MOW','IOS','WEB')
    , mktg_type VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_rollup VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , finance_detail VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC
    , marketing_type VARCHAR(6) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('PAID','UNPAID')
    , attributed_order FLOAT
    , attributed_units FLOAT
    , attributed_demand FLOAT
    , attributed_pred_net FLOAT
    , utm_channel VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC -- utm_channel VARCHAR(64) -- max 261
    , utm_campaign VARCHAR(2000) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sp_campaign VARCHAR(1600) CHARACTER SET UNICODE NOT CASESPECIFIC
    , utm_term VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC -- utm_term VARCHAR(64) --max 233
    , utm_content VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC --utm_content VARCHAR(2400) -- max 3378
    , utm_source VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC --utm_source VARCHAR(64) -- max 130
    , sp_source VARCHAR(140) CHARACTER SET UNICODE NOT CASESPECIFIC
    , gclid VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC
    , is_marketplace VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
    , partner_relationship_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , partner_relationship_type VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
    , order_date_pacific DATE
    )
primary index(order_date_pacific ,finance_detail ,attributed_demand, acp_id)
partition by RANGE_N(order_date_pacific between date '2019-01-01' and date '2025-12-31' EACH INTERVAL '1' DAY)
;

SET QUERY_BAND = NONE FOR SESSION;