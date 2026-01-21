/*

T2/Table Name: T2DL_DAS_FUNNEL_IO.rack_funnel_cost_fact
Team/Owner: Analytics Engineering
Date Created/Modified: 2022-12-23


*/


create multiset table {funnel_io_t2_schema}.rack_funnel_cost_fact
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , MAP = TD_MAP1
     (
      stats_date DATE,
      file_name varchar(25) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
      sourcetype VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC ,
      currency CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD', 'CAD'),
      sourcename VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
      media_type VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC ,
      device_type VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC ,
      campaign_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
      campaign_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      campaign_type VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
      adgroup_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
      adgroup_id VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ad_name VARCHAR(600) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
      ad_id VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC,
      account_name VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC,
      advertising_channel VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC ,
      order_number VARCHAR(55) CHARACTER SET UNICODE NOT CASESPECIFIC ,
      platform VARCHAR(155) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS,
      platform_id VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC,
      estimated_net_total_cost DECIMAL(18, 2),
      gross_commissions DECIMAL(18, 2),
      gross_sales DECIMAL(18, 2),
      sales DECIMAL(18, 2),
      cost DECIMAL(18, 2),
      impressions BIGINT,
      clicks BIGINT,
      conversions FLOAT,
      conversion_value FLOAT,
      video100 FLOAT,
      video75 FLOAT,
      video_views FLOAT,
      video_views_15s FLOAT,
      likes INT,
      dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
      )

PRIMARY INDEX (stats_date, sourcetype, device_type, platform_id, media_type, campaign_id, adgroup_id, order_number)
PARTITION BY RANGE_N(stats_date between date '2020-01-01' and date '2025-12-31' EACH INTERVAL '1' DAY)
;

COMMENT ON  {funnel_io_t2_schema}.rack_funnel_cost_fact IS 'Funnel IO platform loads for Rack';


