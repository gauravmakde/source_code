SET QUERY_BAND = 'App_ID=APP09211;
     DAG_ID=app_push_11521_ACE_ENG;
     Task_Name=ddl_app_push_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MMM.app_push
Team/Owner: Analytics Engineering
Date Created/Modified:03/11/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_MMM', 'app_push_ldg', OUT_RETURN_MSG);

create multiset table T2DL_DAS_MMM.app_push_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
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
Push_Bounce_Rate FLOAT
     )
PRIMARY INDEX (Send_Date,Audience_Name,Audience_Type)
;


SET QUERY_BAND = NONE FOR SESSION;

