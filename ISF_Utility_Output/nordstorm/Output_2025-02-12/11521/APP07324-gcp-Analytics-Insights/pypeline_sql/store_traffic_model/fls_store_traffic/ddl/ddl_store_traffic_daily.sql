SET QUERY_BAND = 'App_ID=APP08150; 
     DAG_ID=ddl_store_traffic_daily_11521_ACE_ENG;
     Task_Name=ddl_store_traffic_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_fls_traffic_model.store_traffic_daily
Team/Owner: tech_ffp_analytics/Selina Song/Matthew Bond

Notes: 
-- This view combines traffic for all the different store types - Nordstrom Stores, Nordstrom Rack Stores,
-- Nordstrom Locals & Last Chance Stores, in a single view. 

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'store_traffic_daily', OUT_RETURN_MSG);

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.store_traffic_daily
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    day_date                    DATE NOT NULL
    , store_num                 INTEGER NOT NULL
    , channel                   VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('RACK', 'FULL LINE')
    , traffic_source            VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('retailnext_cam','placer_channel_scaled_to_retailnext', 'placer_complex_scaled_to_retailnext', 'placer_complex_scaled_to_est_retailnext', 'placer_store_scaled_to_retailnext','placer_complex_scaled_to_lp', 'placer_store_scaled_to_lp','placer_complex_raw', 'placer_store_raw','legacy_wifi_lp')
    , purchase_trips            DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , traffic                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(store_num, day_date)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2021-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY);

SET QUERY_BAND = NONE FOR SESSION;