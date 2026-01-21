SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=brand_health_11521_ACE_ENG;
     Task_Name=brand_health;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.brand_health
Owner: Analytics Engineering
Modified:04/01/2024

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {kpi_scorecard_t2_schema}.brand_health
;
INSERT INTO {kpi_scorecard_t2_schema}.brand_health
SELECT Fiscal_Year,
    Fiscal_Month,
    Banner,
    Aware,
	Familiar,
	Composite,
	Shop,
	Ly_Aware,
	Ly_Familiar,
	Ly_Composite,
	Ly_Shop,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.brand_health_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;


