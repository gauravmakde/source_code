SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=rack_funnel_cost_fact_11521_ACE_ENG;
     Task_Name=rack_funnel_io_statistics;'
     FOR SESSION VOLATILE;

COLLECT STATISTICS COLUMN (stats_date, sourcetype, device_type, platform_id, media_type, campaign_id, adgroup_id, order_number),
                   COLUMN (stats_date, file_name),
                   COLUMN (file_name),
                   COLUMN (stats_date),
                   COLUMN (PARTITION)
ON {funnel_io_t2_schema}.rack_funnel_cost_fact
;

-- drop staging tables
DROP TABLE {funnel_io_t2_schema}.rack_adwords_data_ldg;
DROP TABLE {funnel_io_t2_schema}.rack_bing_data_ldg;
DROP TABLE {funnel_io_t2_schema}.rack_facegram_data_ldg;
DROP TABLE {funnel_io_t2_schema}.rack_verizon_data_ldg;
DROP TABLE {funnel_io_t2_schema}.rack_pinterest_data_ldg;
DROP TABLE {funnel_io_t2_schema}.rack_rakuten_data_ldg;
DROP TABLE {funnel_io_t2_schema}.rack_snapchat_data_ldg;
DROP TABLE {funnel_io_t2_schema}.rack_tiktok_data_ldg;
DROP TABLE {funnel_io_t2_schema}.rack_criteo_data_ldg;

SET QUERY_BAND = NONE FOR SESSION;
