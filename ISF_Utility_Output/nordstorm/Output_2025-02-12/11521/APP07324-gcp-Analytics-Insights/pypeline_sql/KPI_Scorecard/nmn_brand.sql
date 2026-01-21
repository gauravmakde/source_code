SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=nmn_brand_11521_ACE_ENG;
     Task_Name=nmn_brand;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.nmn_brand
Owner: Analytics Engineering
Modified: 03/11/2023

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {kpi_scorecard_t2_schema}.nmn_brand
;
INSERT INTO {kpi_scorecard_t2_schema}.nmn_brand
SELECT Fiscal_year,    
	fiscal_month,	
	No_of_Brand,
	LY_No_of_Brand,
	Plan_No_of_Brand,
	Total_Spend,		
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.nmn_brand_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;

