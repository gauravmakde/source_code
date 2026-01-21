SET QUERY_BAND = 'App_ID=APP08047;
     DAG_ID=ddl_mta_ssa_cost_agg_11521_ACE_ENG;
     Task_Name=ddl_mta_ssa_cost_agg;'
     FOR SESSION VOLATILE;


/******************************************************************************
Table definition for T2DL_DAS_MTA.MTA_SSA_COST_AGG
*******************************************************************************/
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mta_t2_schema}', 'MTA_SSA_COST_AGG', OUT_RETURN_MSG);
-- drop table {mta_t2_schema}.MTA_SSA_COST_AGG;
CREATE MULTISET TABLE {mta_t2_schema}.MTA_SSA_COST_AGG
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    (
    activity_date_pacific DATE,
    order_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('FLS','FLS CANADA','NCOM','RACK','RACK CANADA','RCOM'),
    channelcountry  CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('US','CA'),
    arrived_channel VARCHAR(32) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('NCOM','RCOM','NULL'),
    marketing_type VARCHAR(12) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
    finance_rollup VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
    finance_detail VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    funnel_type VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Up', 'Low', 'Mid'),
    nmn_flag CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('Yes','No'),
    attributed_demand FLOAT,
    attributed_pred_net FLOAT,
    attributed_units FLOAT,
    attributed_orders FLOAT,
    session_orders FLOAT,
	session_demand FLOAT,
    sessions FLOAT,
    bounced_sessions FLOAT,
    paid_campaign_cost FLOAT,
    affiliates_cost FLOAT,
    cost FLOAT,
    fcst_cost FLOAT,
    fcst_traffic FLOAT,
    fcst_attributed_orders FLOAT,
    fcst_attributed_demand FLOAT,
    fcst_attributed_pred_net FLOAT,
    fcst_sessions FLOAT,
    fcst_session_orders FLOAT,
    dw_sys_load_tmstp 	TIMESTAMP(6) DEFAULT Current_Timestamp(6) NOT NULL
    )
PRIMARY INDEX(activity_date_pacific, finance_detail)
PARTITION BY RANGE_N(activity_date_pacific BETWEEN DATE '2017-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON {mta_t2_schema}.MTA_SSA_COST_AGG IS 'Multi touch and Same Session attribution alongside Campaign Cost at finance detail and marketing type level';

SET QUERY_BAND = NONE FOR SESSION;
