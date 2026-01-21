SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=optimine_channel_11521_ACE_ENG;
     Task_Name=ddl_optimine_channel_ldg;'
     FOR SESSION VOLATILE;
     
/*

T2/Table Name: T2DL_DAS_MOA_KPI.optimine_channel
Team/Owner: Analytics Engineering
Date Created/Modified:07/11/2024

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/


-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'optimine_channel_ldg', OUT_RETURN_MSG);

create multiset table {kpi_scorecard_t2_schema}.optimine_channel_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
	, MAP = TD_MAP1
    (
	Influencing_Level0 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level1 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level2 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level3 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level4 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level5 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level6 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level7 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level8 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Influencing_Level9 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level0 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level1 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level2 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level3 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level4 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level5 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level6 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level7 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level8 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Converting_Level9 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TimePeriod DATE FORMAT 'YY/MM/DD',
      XChannelValue1 FLOAT,
      XChannelValue2 FLOAT,
      XChannelValue3 FLOAT,
      XChannelValue4 FLOAT,
      XChannelValue5 FLOAT,
      XChannelCount1 FLOAT,
      XChannelCount2 FLOAT,
      XChannelCount3 FLOAT,
      XChannelCount4 FLOAT,
      XChannelCount5 FLOAT,
      XChannelCount6 FLOAT,
      XChannelCount7 FLOAT,
      XChannelCount8 FLOAT,
      XChannelCount9 FLOAT,
      XChannelCount10 FLOAT 
    )
primary index(Influencing_Level0,Influencing_Level1,Influencing_Level2,Influencing_Level3,Influencing_Level4,Influencing_Level5,Influencing_Level6,Influencing_Level7,Influencing_Level8,Influencing_Level9,
Converting_Level0,Converting_Level1,Converting_Level2,Converting_Level3,Converting_Level4,Converting_Level5,Converting_Level6,Converting_Level7,
Converting_Level8,Converting_Level9,TimePeriod)
;


SET QUERY_BAND = NONE FOR SESSION;











































