SET QUERY_BAND = 'App_ID=APP08150; 
     DAG_ID=ddl_rn_camera_and_trips_daily_11521_ACE_ENG;
     Task_Name=ddl_rn_camera_and_trips_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_fls_traffic_model.rn_camera_and_trips_daily
Team/Owner: tech_ffp_analytics/Selina Song/Matthew Bond

Notes: 
-- This job loads in RetailNext camera data from prd_nap_usr_vws.store_traffic_vw and adds in store purchase trips calculation.
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'rn_camera_and_trips_daily', OUT_RETURN_MSG);

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.rn_camera_and_trips_daily
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    store_num                   INTEGER NOT NULL
    , day_date                  DATE NOT NULL
    , channel                   VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('RACK', 'FULL LINE')
    , store_type_code           VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('RK', 'FL', 'NL', 'CC')
    , camera_flag               INTEGER
    , camera_traffic            DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , purchase_trips            DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(store_num, day_date)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2021-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY);

SET QUERY_BAND = NONE FOR SESSION;