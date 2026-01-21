SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=optimine_summary_11521_ACE_ENG;
     Task_Name=ddl_optimine_summary;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.optimine_summary
Team/Owner: Analytics Engineering
Date Created/Modified:07/11/2024

*/
-- Comment out prior to merging to production.

--drop table {kpi_scorecard_t2_schema}.optimine_summary;

create multiset table {kpi_scorecard_t2_schema}.optimine_summary
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
	Level0 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level1 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level2 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level3 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level4 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level5 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level6 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level7 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level8 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Level9 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TimePeriod DATE FORMAT 'YY/MM/DD',
      ReportedAudience FLOAT,
      ReportedCost FLOAT,
      ModeledValue1 FLOAT,
      ModeledValue2 FLOAT,
      ModeledValue3 FLOAT,
      ModeledValue4 FLOAT,
      ModeledValue5 FLOAT,
      ModeledCount1 FLOAT,
      ModeledCount2 FLOAT,
      ModeledCount3 FLOAT,
      ModeledCount4 FLOAT,
      ModeledCount5 FLOAT,
      ModeledCount6 FLOAT,
      ModeledCount7 FLOAT,
      ModeledCount8 FLOAT,
      ModeledCount9 FLOAT,
      ModeledCount10 FLOAT,  
    dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(Level0,Level1,Level2,Level3,Level4,Level5,Level6,Level7,Level8,Level9,TimePeriod)
;

COMMENT ON  {kpi_scorecard_t2_schema}.optimine_summary IS 'optimine_summary Data for RMI Automation';

SET QUERY_BAND = NONE FOR SESSION;











































