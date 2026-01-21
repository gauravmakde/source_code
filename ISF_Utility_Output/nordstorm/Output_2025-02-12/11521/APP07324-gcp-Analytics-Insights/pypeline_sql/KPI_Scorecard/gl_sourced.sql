SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=gl_sourced_11521_ACE_ENG;
     Task_Name=gl_sourced;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.gl_sourced
Owner: Analytics Engineering
Modified: 03/11/2023

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {kpi_scorecard_t2_schema}.gl_sourced
;
INSERT INTO {kpi_scorecard_t2_schema}.gl_sourced
SELECT Fiscal_year,
    Box_type ,
	Metric,
	fiscal_month,
	TY_Cost,
    Plan_Cost,
	LY_Cost,
	TY_Sales,
	Plan_Sales,
	LY_Sales,	
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.gl_sourced_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;

