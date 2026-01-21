SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=customer_plan_11521_ACE_ENG;
     Task_Name=customer_plan;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.customer_plan
Owner: Analytics Engineering
Modified:03/11/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {kpi_scorecard_t2_schema}.customer_plan
;
INSERT INTO {kpi_scorecard_t2_schema}.customer_plan
SELECT fiscal_year,
    fiscal_month ,
    Box_type ,
    Total_Customers,
	Retained_Customers,
	Acquired_Customers,
	Reactivated_Customers,
	Total_Trips,
	Retained_Trips,
	Acquired_Trips,
	Reactivated_Trips,
	Total_Sales,
	Retained_Sales,
	Acquired_Sales,
	Reactivated_Sales,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.customer_plan_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;


