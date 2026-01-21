SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=wifi_config_change_correction_11521_ACE_ENG;
     Task_Name=ddl_wifi_config_change_correction_ldg;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.wifi_config_change_correction
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Created/Modified: 03/03/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_FLS_TRAFFIC_MODEL', 'wifi_config_change_correction_ldg', OUT_RETURN_MSG);

create multiset table T2DL_DAS_FLS_TRAFFIC_MODEL.wifi_config_change_correction_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     store_number              INTEGER NOT NULL
     , correction_start_date   DATE
     , level_correction        DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    )
UNIQUE PRIMARY INDEX (store_number, correction_start_date)
;

SET QUERY_BAND = NONE FOR SESSION;