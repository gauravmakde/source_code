SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=loyalty_11521_ACE_ENG;
     Task_Name=loyalty;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.loyalty
SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/


DELETE
FROM  {kpi_scorecard_t2_schema}.loyalty
;
INSERT INTO {kpi_scorecard_t2_schema}.loyalty
SELECT Fiscal_Year,
    Fiscal_Month ,
    Banner ,
    Loyalty_Deferred_Rev,
	Gift_Card_Deferred_Rev,
	Loyalty_OPEX,
	Credit_Ebit,
	Tender_Expense_Avoidance,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.loyalty_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;
