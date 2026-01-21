SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=mmm_decomp_export_11521_ACE_ENG;
     Task_Name=mmm_decomp_export;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.mmm_decomp_export
Owner: Analytics Engineering
Modified:22/12/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {kpi_scorecard_t2_schema}.mmm_decomp_export
;
INSERT INTO {kpi_scorecard_t2_schema}.mmm_decomp_export
SELECT Fiscal_Month,
	Channel,
    Touchpoint,
	Spend,
	Events,
	US_FLS_Net_Revenue,
	US_N_com_New_Customers,
	US_N_com_Net_Revenue,
	US_FLS_New_Customers,
	US_NRHL_New_Customers,
	US_NRHL_Net_Revenue,
	US_Rack_New_Customers,
	US_Rack_Net_Revenue,
	Total_US_New_Customers,
	Total_US_Net_Revenue,
	US_Rack_Web_Traffic,
	US_FLS_Web_Traffic,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.mmm_decomp_export_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;


