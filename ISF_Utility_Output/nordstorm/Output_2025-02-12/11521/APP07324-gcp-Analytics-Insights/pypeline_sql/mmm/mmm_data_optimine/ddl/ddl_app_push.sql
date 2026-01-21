SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=app_push_11521_ACE_ENG;
     Task_Name=ddl_app_push;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM_KPI.app_push
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.

--drop table {mmm_t2_schema}.app_push;
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'app_push', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.app_push
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    Audience_Name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Audience_Type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Campaign_Type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Send_Date DATE FORMAT 'YY/MM/DD',
Mobile_Device_OS VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Send_Name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Send_Type VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Content_Name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Title VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Campaign_Code VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Mobile_App VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Sends FLOAT,
Push_Deliveries FLOAT,
Push_Opens FLOAT,
Push_Delivery_Rate FLOAT,
Push_Open_Rate FLOAT,
Push_Bounce_Rate FLOAT,	    
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Send_Date,Audience_Name,Audience_Type)
;

COMMENT ON  {mmm_t2_schema}.app_push IS 'app_push Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;











































