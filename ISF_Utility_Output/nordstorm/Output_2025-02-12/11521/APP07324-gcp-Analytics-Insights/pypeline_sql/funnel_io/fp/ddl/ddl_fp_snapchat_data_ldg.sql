SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_funnel_cost_fact_11521_ACE_ENG;
     Task_Name=ddl_fp_snapchat_data_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.fp_snapchat_data_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 2022-12-08

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/



CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{funnel_io_t2_schema}', 'fp_snapchat_data_ldg', OUT_RETURN_MSG);
create multiset table {funnel_io_t2_schema}.fp_snapchat_data_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
  stats_date date
    , sourcetype VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , currency CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sourcename VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , media_type VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , ad_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , ad_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , adgroup_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , adgroup_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , campaign_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , campaign_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , video100 FLOAT
    , cost FLOAT
    , impressions FLOAT
    , clicks FLOAT
    , video_views FLOAT
    , video75 FLOAT
    , conversions FLOAT
    , conversion_value FLOAT
    , video_views_15s FLOAT
      )

PRIMARY INDEX (stats_date)
;


SET QUERY_BAND = NONE FOR SESSION;