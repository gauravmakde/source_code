SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_decomp_export_11521_ACE_ENG;
     Task_Name=ddl_mmm_decomp_export;'
     FOR SESSION VOLATILE;
/*


T2/Table Name: T2DL_DAS_MOA_KPI.mmm_decomp_export
Team/Owner: Analytics Engineering
Date Created/Modified:22/12/2023


*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.mmm_decomp_export;

create multiset table {kpi_scorecard_t2_schema}.mmm_decomp_export
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    Fiscal_Month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Channel VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Touchpoint VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Spend FLOAT,
	Events FLOAT,
	US_FLS_Net_Revenue FLOAT,
	US_N_com_New_Customers FLOAT,
	US_N_com_Net_Revenue FLOAT,
	US_FLS_New_Customers FLOAT,
	US_NRHL_New_Customers FLOAT,
	US_NRHL_Net_Revenue FLOAT,
	US_Rack_New_Customers FLOAT,
	US_Rack_Net_Revenue FLOAT,
	Total_US_New_Customers FLOAT,
	Total_US_Net_Revenue FLOAT,
	US_Rack_Web_Traffic FLOAT,
	US_FLS_Web_Traffic FLOAT,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Fiscal_Month,Channel,Touchpoint)
;

COMMENT ON  {kpi_scorecard_t2_schema}.mmm_decomp_export IS 'MMM Decomp Export Data for KPI Scorecard';

SET QUERY_BAND = NONE FOR SESSION;
