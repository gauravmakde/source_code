SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_forecast_11521_ACE_ENG;
     Task_Name=ddl_mmm_forecast;'
     FOR SESSION VOLATILE;
/*


T2/Table Name: T2DL_DAS_MOA_KPI.mmm_forecast
Team/Owner: Analytics Engineering
Date Created/Modified:22/12/2023


*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.mmm_forecast;

create multiset table {kpi_scorecard_t2_schema}.mmm_forecast
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    Brand VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Funnel VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Channel  VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
    Fiscal_Month VARCHAR(256) CHARACTER SET UNICODE NOT CASESPECIFIC,
	Spend FLOAT,
	FLS_Net_Revenue FLOAT,
	FLS_New_Customers FLOAT,
	NCOM_Net_Revenue FLOAT,
	NCOM_New_Customers FLOAT,
	Rack_Net_Revenue FLOAT,
	Rack_New_Customers FLOAT,
	RCOM_Net_Revenue FLOAT,
	RCOM_New_Customers FLOAT,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Brand,Funnel,Channel, Fiscal_Month)
;

COMMENT ON  {kpi_scorecard_t2_schema}.mmm_forecast IS 'MMM Forecast Data for KPI Scorecard';

SET QUERY_BAND = NONE FOR SESSION;
