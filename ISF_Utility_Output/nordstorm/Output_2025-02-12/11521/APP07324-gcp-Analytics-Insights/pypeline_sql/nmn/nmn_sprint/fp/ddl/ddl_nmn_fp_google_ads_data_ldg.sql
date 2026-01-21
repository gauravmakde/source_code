SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_nmn_cost_data_11521_ACE_ENG;
     Task_Name=ddl_nmn_fp_google_ads_data_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.fp_adwords_data_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 2022-12-07

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/



CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{nmn_t2_schema}', 'nmn_fp_google_ads_data_ldg', OUT_RETURN_MSG);

create multiset table {nmn_t2_schema}.nmn_fp_google_ads_data_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
   stats_date date
    , sourcename VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , sourcetype VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , currency CHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , media_type VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC
    , campaign_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , campaign_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , campaign_type VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , adgroup_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , adgroup_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , ad_account_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC
    , cost FLOAT
    , impressions FLOAT
    , clicks FLOAT
    , engagements FLOAT
    , conversions FLOAT
    , conversion_value FLOAT
    , video_views FLOAT
      )

PRIMARY INDEX (stats_date)
;

SET QUERY_BAND = NONE FOR SESSION;
