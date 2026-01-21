SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=gl_sourced_11521_ACE_ENG;
     Task_Name=ddl_gl_sourced;'
     FOR SESSION VOLATILE;
/*


T2/Table Name: T2DL_DAS_MOA_KPI.gl_sourced
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023


*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.gl_sourced;

create multiset table {kpi_scorecard_t2_schema}.gl_sourced
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Fiscal_year INTEGER,
    Box_type VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC ,
	Metric VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	fiscal_month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	TY_Cost FLOAT,
    Plan_Cost FLOAT,
	LY_Cost FLOAT,
	TY_Sales FLOAT,
	Plan_Sales FLOAT,
	LY_Sales FLOAT,   
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Fiscal_year,Box_type,Metric,fiscal_month)
;

COMMENT ON  {kpi_scorecard_t2_schema}.gl_sourced IS 'GL_Sourced Data for KPI Scorecard';

SET QUERY_BAND = NONE FOR SESSION;


