SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=optimine_channel_11521_ACE_ENG;
     Task_Name=optimine_channel;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.optimine_channel
Team/Owner: Analytics Engineering
Date Created/Modified:07/11/2024

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

DELETE
FROM  {kpi_scorecard_t2_schema}.optimine_channel
;

INSERT INTO {kpi_scorecard_t2_schema}.optimine_channel
SELECT Influencing_Level0 ,
      Influencing_Level1 ,
      Influencing_Level2 ,
      Influencing_Level3 ,
      Influencing_Level4 ,
      Influencing_Level5 ,
      Influencing_Level6 ,
      Influencing_Level7 ,
      Influencing_Level8 ,
      Influencing_Level9 ,
      Converting_Level0 ,
      Converting_Level1 ,
      Converting_Level2 ,
      Converting_Level3 ,
      Converting_Level4 ,
      Converting_Level5 ,
      Converting_Level6 ,
      Converting_Level7 ,
      Converting_Level8 ,
      Converting_Level9 ,
      TimePeriod ,
      XChannelValue1  ,
      XChannelValue2 ,
      XChannelValue3  ,
      XChannelValue4  ,
      XChannelValue5  ,
      XChannelCount1 ,
      XChannelCount2  ,
      XChannelCount3  ,
      XChannelCount4  ,
      XChannelCount5  ,
      XChannelCount6  ,
      XChannelCount7  ,
      XChannelCount8  ,
      XChannelCount9  ,
      XChannelCount10  ,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.optimine_channel_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;
































