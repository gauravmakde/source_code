SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=media_pacing_report_11521_ACE_ENG;
     Task_Name=media_pacing_report;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.media_pacing_report
Team/Owner: Analytics Engineering
Date Created/Modified:07/29/2024

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

DELETE
FROM  {kpi_scorecard_t2_schema}.media_pacing_report where Date_Start between (select week_start_day_date from prd_nap_usr_vws.DAY_CAL_454_DIM where day_date = {start_date}) and (select week_end_day_date from prd_nap_usr_vws.DAY_CAL_454_DIM where day_date = {end_date})
;

INSERT INTO {kpi_scorecard_t2_schema}.media_pacing_report
SELECT OrgId ,
Date_Start,
Date_End ,
Comp_Date_Start ,
Comp_Date_End ,
Comp_Status ,
Analysis_Setup_Instance_Name ,
Analysis_Category_Name ,
NMNFlag ,
PlatformType ,
AcqRetType ,
AdFormat ,
LoyaltyStatus ,
Category ,
Banner ,
Level9 ,
Executed_Audience ,
Comp_Audience ,
Executed_Spend ,
Comp_Spend ,
Expected_Count1_Min ,
Estimated_Count1 ,
Expected_Count1_Max ,
Expected_Count2_Min ,
Estimated_Count2 ,
Expected_Count2_Max ,
Expected_Count3_Min ,
Estimated_Count3 ,
Expected_Count3_Max ,
Expected_Count4_Min ,
Estimated_Count4 ,
Expected_Count4_Max ,
Expected_Count5_Min ,
Estimated_Count5 ,
Expected_Count5_Max ,
Expected_Count6_Min ,
Estimated_Count6 ,
Expected_Count6_Max ,
Expected_Count7_Min ,
Estimated_Count7 ,
Expected_Count7_Max ,
Expected_Count8_Min ,
Estimated_Count8 ,
Expected_Count8_Max ,
Expected_Count9_Min ,
Estimated_Count9 ,
Expected_Count9_Max ,
Expected_Count10_Min ,
Estimated_Count10 ,
Expected_Count10_Max ,
Expected_Value1_Min ,
Estimated_Value1 ,
Expected_Value1_Max ,
Expected_Value2_Min ,
Estimated_Value2 ,
Expected_Value2_Max ,
Expected_Value3_Min ,
Estimated_Value3 ,
Expected_Value3_Max ,
Expected_Value4_Min ,
Estimated_Value4 ,
Expected_Value4_Max ,
Expected_Value5_Min ,
Estimated_Value5 ,
Expected_Value5_Max ,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.media_pacing_report_ldg where Date_Start between (select week_start_day_date from prd_nap_usr_vws.DAY_CAL_454_DIM where day_date = {start_date}) and (select week_end_day_date from prd_nap_usr_vws.DAY_CAL_454_DIM where day_date = {end_date})
;

SET QUERY_BAND = NONE FOR SESSION;
































