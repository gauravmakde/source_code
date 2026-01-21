SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=brand_health_11521_ACE_ENG;
     Task_Name=ddl_brand_health;'
     FOR SESSION VOLATILE;
/*


T2/Table Name: T2DL_DAS_MOA_KPI.brand_health
Team/Owner: Analytics Engineering
Date Created/Modified:04/01/2024


*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.brand_health;

create multiset table {kpi_scorecard_t2_schema}.brand_health
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    Fiscal_Year INTEGER,
    Fiscal_Month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Banner  VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Aware FLOAT,
	Familiar FLOAT,
	Composite FLOAT,
	Shop FLOAT,
	Ly_Aware FLOAT,
	Ly_Familiar FLOAT,
	Ly_Composite FLOAT,
	Ly_Shop FLOAT,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Fiscal_Year,Fiscal_Month,Banner)
;

COMMENT ON  {kpi_scorecard_t2_schema}.brand_health IS 'Brand Health Data for KPI Scorecard';

SET QUERY_BAND = NONE FOR SESSION;
