SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=app_push_11521_ACE_ENG;
     Task_Name=app_push;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_MMM.app_push
Owner: Analytics Engineering
Modified:03/11/2023 

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

DELETE
FROM  {mmm_t2_schema}.app_push
;

INSERT INTO {mmm_t2_schema}.app_push
SELECT Audience_Name, 
Audience_Type,
Campaign_Type,
Send_Date,
Mobile_Device_OS,
Push_Send_Name,
Push_Send_Type,
Push_Content_Name,
Push_Title,
Campaign_Code,
Mobile_App,
Push_Sends,
Push_Deliveries,
Push_Opens,
Push_Delivery_Rate,
Push_Open_Rate,
Push_Bounce_Rate,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {mmm_t2_schema}.app_push_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;






