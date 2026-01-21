SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=ddl_fp_tiktok_reach_and_freq_11521_ACE_ENG;
     Task_Name = ddl_fp_tiktok_reach_and_freq_ldg;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.nmn_fp_tiktok_reach_and_freq_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 2024-04-02

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{funnel_io_t2_schema}', 'nmn_fp_tiktok_reach_and_freq_ldg', OUT_RETURN_MSG);


CREATE MULTISET TABLE {funnel_io_t2_schema}.nmn_fp_tiktok_reach_and_freq_ldg,
     FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      day_date DATE FORMAT 'YY/MM/DD',
      data_source_type VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      data_source_name VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_id VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_objective VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      daily_reach FLOAT,
      daily_frequency FLOAT)
PRIMARY INDEX ( day_date ,data_source_name,campaign_id );

SET QUERY_BAND = NONE FOR SESSION;
