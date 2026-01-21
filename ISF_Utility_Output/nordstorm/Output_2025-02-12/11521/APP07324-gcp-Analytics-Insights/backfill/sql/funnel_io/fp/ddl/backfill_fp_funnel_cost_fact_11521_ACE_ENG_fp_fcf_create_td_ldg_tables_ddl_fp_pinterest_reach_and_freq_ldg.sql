SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_funnel_cost_fact_11521_ACE_ENG;
     Task_Name=ddl_nmn_fp_pinterest_data_ldg;'
     FOR SESSION VOLATILE;
/*
T2/Table Name: T2DL_DAS_FUNNEL_IO.nmn_fp_pinterest_data_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 2024-04-02

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_FUNNEL_IO', 'nmn_fp_pinterest_data_ldg', OUT_RETURN_MSG);

create multiset table T2DL_DAS_FUNNEL_IO.nmn_fp_pinterest_data_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
      day_date DATE FORMAT 'YY/MM/DD',
      data_source_type VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      data_source_name VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_id VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_name VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_status VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_objective VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      spend FLOAT,
      total_impressions FLOAT,
      clicks FLOAT,
      adds_to_cart FLOAT,
      web_purchases FLOAT,
      offline_purchases FLOAT,
      web_purchase_conversion_value FLOAT,
      offline_purchase_conversion_value FLOAT,
      conversions FLOAT,
      total_conversions_value FLOAT,
      engagements FLOAT,
      video_views FLOAT,
      total_video_played_at_100 FLOAT,
      total_video_played_at_75 FLOAT,
      total_video_played_at_50 FLOAT,
      total_video_played_at_25 FLOAT,
      daily_reach FLOAT,
      daily_frequency FLOAT)
PRIMARY INDEX (day_date ,data_source_name,campaign_id);

SET QUERY_BAND = NONE FOR SESSION;
