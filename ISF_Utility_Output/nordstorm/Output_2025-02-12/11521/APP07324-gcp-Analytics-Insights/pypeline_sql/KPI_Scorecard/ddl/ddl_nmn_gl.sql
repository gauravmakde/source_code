SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=nmn_gl_11521_ACE_ENG;
     Task_Name=ddl_nmn_gl;'
     FOR SESSION VOLATILE;
/*


T2/Table Name: T2DL_DAS_MOA_KPI.nmn_gl
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023


*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.nmn_gl;

create multiset table {kpi_scorecard_t2_schema}.nmn_gl
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Fiscal_year INTEGER,    
	fiscal_month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,	
	TY_Sales FLOAT,
	LY_Sales FLOAT,
	Plan_Sales FLOAT,
	TY_Earnings FLOAT,
	LY_Earnings FLOAT,
	Plan_Earnings FLOAT,	
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Fiscal_year,fiscal_month)
;

COMMENT ON  {kpi_scorecard_t2_schema}.nmn_gl IS 'NMN_GL Data for KPI Scorecard';

SET QUERY_BAND = NONE FOR SESSION;

