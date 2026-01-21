SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=optimine_summary_11521_ACE_ENG;
     Task_Name=optimine_summary;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.optimine_summary
Team/Owner: Analytics Engineering
Date Created/Modified:07/11/2024

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

DELETE
FROM  {kpi_scorecard_t2_schema}.optimine_summary
;

INSERT INTO {kpi_scorecard_t2_schema}.optimine_summary
SELECT Level0,
      Level1,
      Level2,
      Level3,
      Level4,
      Level5,
      Level6,
      Level7,
      Level8,
      Level9,
      TimePeriod,
      ReportedAudience,
      ReportedCost,
      ModeledValue1,
      ModeledValue2,
      ModeledValue3,
      ModeledValue4,
      ModeledValue5,
      ModeledCount1,
      ModeledCount2,
      ModeledCount3,
      ModeledCount4,
      ModeledCount5,
      ModeledCount6,
      ModeledCount7,
      ModeledCount8,
      ModeledCount9,
      ModeledCount10,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM  {kpi_scorecard_t2_schema}.optimine_summary_ldg
WHERE 1=1
;

SET QUERY_BAND = NONE FOR SESSION;
































