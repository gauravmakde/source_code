SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_fls_traffic_daily_11521_ACE_ENG;
     Task_Name=ddl_fls_traffic_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_fls_traffic_model.fls_traffic_daily
Team/Owner: TECH_FFP_ANALYTICS/Agnes Bao
Date Created/Modified: 02/02/2023

Note:
-- This job updates the FLS Store Traffic production table
-- Update cadence: daily
-- Lookback window: 15 days

*/


CREATE MULTISET TABLE T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    store_number                INTEGER NOT NULL
    , store_name                VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , region                    VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dma                       VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
    , store_closure_flag        INTEGER
    , day_date                  DATE NOT NULL
    , reopen_date               DATE
    , covid_store_closure_flag  INTEGER
    , wifi_users                DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , purchase_trips            DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , traffic                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , traffic_source            VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Camera', 'Wifi')
    , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(store_number, day_date)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2019-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY);


-- Table Comment (STANDARD)
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily IS 'FLS Store Traffic Data';
-- Column comments (OPTIONAL)
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.store_number IS 'A unique number that was assigned to a store';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.store_name IS 'Store number and store name';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.region IS 'The Region where the store is located';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.dma IS 'The DMA where the store is located';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.store_closure_flag IS 'Flag for stores that are permanently closed';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.day_date IS 'Date';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.reopen_date IS 'Date the store opened post closure in March 2020';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.covid_store_closure_flag IS 'Flag for the time period the stores were closed due COVID starting March 2020';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.wifi_users IS 'Number of unique devices connected to store WiFi daily';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.purchase_trips IS 'Count of customers with positive merchandise transactions';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.traffic IS 'Count of people entering the store. For camera store, it is actual. For non-camera store, it is modeled';
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_daily.traffic_source IS 'Information on the source of traffic data for that given store-day';
SET QUERY_BAND = NONE FOR SESSION;