SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=nmn_gl_11521_ACE_ENG;
     Task_Name=nmn_gl;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.nmn_gl
Owner: Analytics Engineering
Modified: 03/11/2023

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {kpi_scorecard_t2_schema}.nmn_gl
;
INSERT INTO {kpi_scorecard_t2_schema}.nmn_gl
SELECT Fiscal_year,    
	fiscal_month,	
	TY_Sales,
	LY_Sales,
	Plan_Sales,
	TY_Earnings,
	LY_Earnings,
	Plan_Earnings,		
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.nmn_gl_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;

