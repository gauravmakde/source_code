SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=app_push_nord_11521_ACE_ENG;
     Task_Name=ddl_app_push_nord;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.app_push_nord
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

*/
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{mmm_t2_schema}', 'app_push_nord', OUT_RETURN_MSG);

create multiset table {mmm_t2_schema}.app_push_nord
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Business_Unit VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Campaign VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Audience_Name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Event_Date DATE FORMAT 'YY/MM/DD',
Send_Date DATE FORMAT 'YY/MM/DD',
Mobile_App VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Mobile_Device_OS VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Content_Name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Send_ID FLOAT,
Push_Send_Name VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Send_Type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Title VARCHAR(400) CHARACTER SET UNICODE NOT CASESPECIFIC,
Push_Bounces FLOAT,
Push_Not_Sent FLOAT,
Push_Opens FLOAT,
Push_Sends FLOAT, 
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Business_Unit,Campaign)
;

COMMENT ON  {mmm_t2_schema}.app_push_nord IS 'app_push Nordstrom Data for MMM Data Consolidation';

SET QUERY_BAND = NONE FOR SESSION;















