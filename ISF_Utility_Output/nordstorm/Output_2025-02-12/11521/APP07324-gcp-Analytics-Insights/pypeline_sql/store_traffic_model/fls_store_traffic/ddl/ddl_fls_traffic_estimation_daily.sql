SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_fls_store_traffic_model_11521_ACE_ENG;
     Task_Name=ddl_fls_traffic_estimation_daily;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_fls_traffic_model.fls_traffic_estimation_daily
Team/Owner: TECH_FFP_ANALYTICS/Agnes Bao
Date Created/Modified: 02/02/2023

Supports validation & reporting for FLS store traffic
Description:
This job supports the daily reporting for FLS store traffic - with data coming FROM two sources
    - Actual traffic FROM RetailNext (vendor)
    - Estimated traffic FROM wifi-based regression model
Update cadence: daily
Lookback window: 15 days

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'fls_traffic_estimation_daily', OUT_RETURN_MSG);

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.fls_traffic_estimation_daily
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    store_number                    INTEGER NOT NULL
    , store_name                    VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , day_date                      DATE NOT NULL
    , fiscal_week                   INTEGER
    , fiscal_month                  INTEGER
    , fiscal_year                   INTEGER
    , day_num                       INTEGER
    , day_454_num                   INTEGER
    , store_open_date               DATE
    , store_close_date              DATE
    , region                        VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dma                           VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
    , reopen_date                   DATE
    , covid_store_closure_flag      INTEGER
    , thanksgiving                  INTEGER
    , christmas                     INTEGER
    , easter                        INTEGER
    , unplanned_closure             INTEGER
    , wifi_users                    DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , purchase_trips                DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , net_sales                     DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , gross_sales                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , positive_merch_custs_bopus    DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , camera_flag                   INTEGER
    , camera_traffic                DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , estimated_traffic             DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , dw_sys_load_tmstp             TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
UNIQUE PRIMARY INDEX (store_number, day_date)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2019-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY);


-- Table Comment (STANDARD)
COMMENT ON  {fls_traffic_model_t2_schema}.fls_traffic_estimation_daily IS 'FLS Store Traffic Intermediate Table';
SET QUERY_BAND = NONE FOR SESSION;