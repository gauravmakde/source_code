SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=media_pacing_report_11521_ACE_ENG;
     Task_Name=ddl_media_pacing_report;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.media_pacing_report
Team/Owner: Analytics Engineering
Date Created/Modified:08/29/2024

*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.media_pacing_report;

create multiset table {kpi_scorecard_t2_schema}.media_pacing_report
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (OrgId VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Date_Start DATE FORMAT 'YY/MM/DD',
Date_End DATE FORMAT 'YY/MM/DD',
Comp_Date_Start DATE FORMAT 'YY/MM/DD',
Comp_Date_End DATE FORMAT 'YY/MM/DD',
Comp_Status VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Analysis_Setup_Instance_Name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Analysis_Category_Name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
NMNFlag VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
PlatformType VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
AcqRetType VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
AdFormat VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
LoyaltyStatus VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Banner VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Level9 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
Executed_Audience float,
Comp_Audience float,
Executed_Spend float,
Comp_Spend float,
Expected_Count1_Min float,
Estimated_Count1 float,
Expected_Count1_Max float,
Expected_Count2_Min float,
Estimated_Count2 float,
Expected_Count2_Max float,
Expected_Count3_Min float,
Estimated_Count3 float,
Expected_Count3_Max float,
Expected_Count4_Min float,
Estimated_Count4 float,
Expected_Count4_Max float,
Expected_Count5_Min float,
Estimated_Count5 float,
Expected_Count5_Max float,
Expected_Count6_Min float,
Estimated_Count6 float,
Expected_Count6_Max float,
Expected_Count7_Min float,
Estimated_Count7 float,
Expected_Count7_Max float,
Expected_Count8_Min float,
Estimated_Count8 float,
Expected_Count8_Max float,
Expected_Count9_Min float,
Estimated_Count9 float,
Expected_Count9_Max float,
Expected_Count10_Min float,
Estimated_Count10 float,
Expected_Count10_Max float,
Expected_Value1_Min float,
Estimated_Value1 float,
Expected_Value1_Max float,
Expected_Value2_Min float,
Estimated_Value2 float,
Expected_Value2_Max float,
Expected_Value3_Min float,
Estimated_Value3 float,
Expected_Value3_Max float,
Expected_Value4_Min float,
Estimated_Value4 float,
Expected_Value4_Max float,
Expected_Value5_Min float,
Estimated_Value5 float,
Expected_Value5_Max float,
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Analysis_Setup_Instance_Name,Analysis_Category_Name,NMNFlag,PlatformType,AcqRetType,AdFormat,LoyaltyStatus,Category,Banner,Level9,Date_Start,Date_End)
;

COMMENT ON  {kpi_scorecard_t2_schema}.media_pacing_report IS 'media_pacing_report Data for MMM Insights';

SET QUERY_BAND = NONE FOR SESSION;











































