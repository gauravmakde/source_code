SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=pr_data_11521_ACE_ENG;
     Task_Name=pr_data;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MMM.pr_data
Owner: Analytics Engineering
Modified:03/11/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {mmm_t2_schema}.pr_data
;

INSERT INTO {mmm_t2_schema}.pr_data
SELECT Start_Date,
	End_Date,
	Region,
	City,
	State_name,
	marketing_type,
	Media_Outlet,
	Title_name,
	Total_Readership,
	Link,
	Author,
	Campaign,
	Notes,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {mmm_t2_schema}.pr_data_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;


