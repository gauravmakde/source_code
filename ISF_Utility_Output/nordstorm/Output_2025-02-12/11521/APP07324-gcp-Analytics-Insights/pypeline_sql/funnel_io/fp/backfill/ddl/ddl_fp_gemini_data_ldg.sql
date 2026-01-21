/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.fp_gemini_data_ldg
Team/Owner: Analytics Engineering
Date Created/Modified: 2022-12-07

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{funnel_io_t2_schema}', 'fp_gemini_data_ldg', OUT_RETURN_MSG);

create multiset table {funnel_io_t2_schema}.fp_gemini_data_ldg
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
    stats_date date
    , currency CHAR(3)
    , media_type VARCHAR(255)
    , sourcename VARCHAR(255)
    , sourcetype VARCHAR(255)
    , adgroup_id VARCHAR(255)
    , adgroup_name VARCHAR(255)
    , campaign_id VARCHAR(255)
    , campaign_name VARCHAR(255)
    , clicks FLOAT
    , conversion_value FLOAT
    , conversions FLOAT
    , device_type  VARCHAR(255)
    , impressions FLOAT
    , cost FLOAT
    , video100 FLOAT
    , video75 FLOAT
    , video_views FLOAT
      )

PRIMARY INDEX (stats_date)
;

