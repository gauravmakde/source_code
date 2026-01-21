SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=nmn_brand_11521_ACE_ENG;
     Task_Name=ddl_nmn_brand;'
     FOR SESSION VOLATILE;
/*


T2/Table Name: T2DL_DAS_MOA_KPI.nmn_brand
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023


*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.nmn_brand;

create multiset table {kpi_scorecard_t2_schema}.nmn_brand
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Fiscal_year INTEGER,    
	fiscal_month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,			
	No_of_Brand FLOAT,
	LY_No_of_Brand FLOAT,
	Plan_No_of_Brand FLOAT,
	Total_Spend FLOAT,	
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Fiscal_year,fiscal_month)
;

COMMENT ON  {kpi_scorecard_t2_schema}.nmn_brand IS 'NMN_BRAND Data for KPI Scorecard';

SET QUERY_BAND = NONE FOR SESSION;
