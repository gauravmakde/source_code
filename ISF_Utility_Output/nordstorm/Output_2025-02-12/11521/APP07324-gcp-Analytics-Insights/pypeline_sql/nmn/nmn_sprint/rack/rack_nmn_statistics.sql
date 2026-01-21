SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=rack_nmn_cost_data_11521_ACE_ENG;
     Task_Name=rack_nmn_statistics;'
     FOR SESSION VOLATILE;



COLLECT STATISTICS COLUMN (stats_date, sourcetype, device_type, platform_id, media_type, campaign_id, adgroup_id, order_number),
                   COLUMN (stats_date, file_name),
                   COLUMN (file_name),
                   COLUMN (stats_date),
                   COLUMN (PARTITION)
ON {nmn_t2_schema}.rack_nmn_cost_data
;


-- drop staging tables
DROP TABLE {nmn_t2_schema}.nmn_rack_meta_data_ldg;
DROP TABLE {nmn_t2_schema}.nmn_rack_dv_three_sixty_data_ldg;
DROP TABLE {nmn_t2_schema}.nmn_rack_tiktok_data_ldg;
DROP TABLE {nmn_t2_schema}.nmn_rack_criteo_data_ldg;
DROP TABLE {nmn_t2_schema}.nmn_rack_pinterest_data_ldg;


SET QUERY_BAND = NONE FOR SESSION;


