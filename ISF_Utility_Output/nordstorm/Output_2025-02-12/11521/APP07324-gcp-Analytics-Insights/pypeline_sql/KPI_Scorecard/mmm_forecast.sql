SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_forecast_11521_ACE_ENG;
     Task_Name=mmm_forecast;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.mmm_forecast
Owner: Analytics Engineering
Modified:22/12/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {kpi_scorecard_t2_schema}.mmm_forecast
;
INSERT INTO {kpi_scorecard_t2_schema}.mmm_forecast
SELECT Brand,
    Funnel,
    Channel,
    Fiscal_Month,
	Spend,
	FLS_Net_Revenue,
	FLS_New_Customers,
	NCOM_Net_Revenue,
	NCOM_New_Customers,
	Rack_Net_Revenue,
	Rack_New_Customers,
	RCOM_Net_Revenue,
	RCOM_New_Customers,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.mmm_forecast_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;


