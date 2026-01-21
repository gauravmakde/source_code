SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_wifi_config_change_correction_11521_ACE_ENG;
     Task_Name=ddl_wifi_config_change_correction;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.wifi_config_change_correction
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Created/Modified: 03/03/2023

Note:
-- The NSG team (the team that manages wifi & other network-related stuff in stores) is making changes to how they
-- capture logs. Because of this change, we expect to see a level shift in wifi numbers for FLS stores. For now, we
-- are engaging with NSG to come up with a transition plan but as we work through that there might be a few cases where
-- these changes might happen earlier than planned due to connectivity issues faced by the customers.


*/

create multiset table {fls_traffic_model_t2_schema}.wifi_config_change_correction
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     store_number              INTEGER NOT NULL
     , correction_start_date   DATE
     , level_correction        DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
     , dw_sys_load_tmstp       TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (store_number, correction_start_date)
;

-- Table Comment (STANDARD)
COMMENT ON {fls_traffic_model_t2_schema}.wifi_config_change_correction IS 'correction for NSG wifi config change';
SET QUERY_BAND = NONE FOR SESSION;