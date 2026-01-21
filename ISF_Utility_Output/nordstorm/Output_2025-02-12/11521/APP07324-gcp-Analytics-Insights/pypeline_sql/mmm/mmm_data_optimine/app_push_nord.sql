SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=app_push_nord_11521_ACE_ENG;
     Task_Name=app_push_nord;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MOA_KPI.app_push_nord
Owner: Analytics Engineering
Modified:03/11/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {kpi_scorecard_t2_schema}.app_push_nord
;

INSERT INTO {kpi_scorecard_t2_schema}.app_push_nord
SELECT 
Business_Unit,
Campaign,
Audience_Name,
Event_Date,
Send_Date,
Mobile_App,
Mobile_Device_OS,
Push_Content_Name,
Push_Send_ID,
Push_Send_Name,
Push_Send_Type,
Push_Title,
Push_Bounces,
Push_Not_Sent,
Push_Opens,
Push_Sends,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.app_push_nord_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;






