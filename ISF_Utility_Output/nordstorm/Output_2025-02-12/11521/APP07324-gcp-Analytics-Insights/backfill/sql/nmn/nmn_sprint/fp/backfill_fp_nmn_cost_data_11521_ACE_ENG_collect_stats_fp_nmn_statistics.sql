SET QUERY_BAND = 'App_ID=APP08133;
     DAG_ID=fp_nmn_cost_data_11521_ACE_ENG;
     Task_Name=fp_nmn_statistics;'
     FOR SESSION VOLATILE;



COLLECT STATISTICS COLUMN (stats_date, sourcetype, device_type, platform_id, media_type, campaign_id, adgroup_id, order_number),
                   COLUMN (stats_date, file_name),
                   COLUMN (file_name),
                   COLUMN (stats_date),
                   COLUMN (PARTITION)
ON T2DL_DAS_NMN.fp_nmn_cost_data
;


-- drop staging tables
DROP TABLE T2DL_DAS_NMN.nmn_fp_google_ads_data_ldg;
DROP TABLE T2DL_DAS_NMN.nmn_fp_meta_data_ldg;
DROP TABLE T2DL_DAS_NMN.nmn_fp_dv_three_sixty_data_ldg;
DROP TABLE T2DL_DAS_NMN.nmn_fp_tiktok_data_ldg;
DROP TABLE T2DL_DAS_NMN.nmn_fp_criteo_data_ldg;
DROP TABLE T2DL_DAS_NMN.nmn_fp_pinterest_data_ldg;


SET QUERY_BAND = NONE FOR SESSION;

