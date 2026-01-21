SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=loyalty_11521_ACE_ENG;
     Task_Name=ddl_loyalty;'
     FOR SESSION VOLATILE;
/*


T2/Table Name: T2DL_DAS_MOA_KPI.loyalty
Team/Owner: Analytics Engineering
Date Created/Modified:22/12/2023


*/
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'loyalty', OUT_RETURN_MSG);

create multiset table {kpi_scorecard_t2_schema}.loyalty
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    Fiscal_Year INTEGER,
    Fiscal_Month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Banner  VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Loyalty_Deferred_Rev FLOAT,
	Gift_Card_Deferred_Rev FLOAT,
	Loyalty_Opex FLOAT,
	Credit_Ebit FLOAT,
	Tender_Expense_Avoidance FLOAT,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(fiscal_year,fiscal_month,Banner)
;

COMMENT ON  {kpi_scorecard_t2_schema}.loyalty IS 'loyalty Data for KPI Scorecard';

SET QUERY_BAND = NONE FOR SESSION;
