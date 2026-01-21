SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_fls_rn_nsg_wifi_coef_11521_ACE_ENG;
     Task_Name=ddl_fls_rn_nsg_wifi_coef;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.fls_rn_nsg_wifi_coef
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Created/Modified: 03/03/2023

Note:
-- This tables has details on model coefficient for adjusting RetailNext wifi to align with new source of wifi data(NSG)

*/

create multiset table {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_coef
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    store_number          INTEGER,
    intercept             DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    rn_wifi_slope         DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    holiday_dt            DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    weekend_dt            DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    holiday_rn_wifi_slope DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    weekend_rn_wifi_slope DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    r_squared             DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    dw_sys_load_tmstp     TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (store_number)
;

-- Table Comment (STANDARD)
COMMENT ON {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_coef IS 'details on model coefficient for adjusting RetailNext wifi to align with new source of wifi data(NSG)';
SET QUERY_BAND = NONE FOR SESSION;