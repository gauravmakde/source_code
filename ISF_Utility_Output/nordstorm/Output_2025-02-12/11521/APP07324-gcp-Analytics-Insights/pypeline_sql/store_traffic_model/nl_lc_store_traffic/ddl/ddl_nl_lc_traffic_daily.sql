SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_nl_lc_traffic_daily_11521_ACE_ENG;
     Task_Name=ddl_nl_lc_traffic_daily;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.nl_lc_traffic_daily
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/17/2023

Notes: 
-- Supports reporting for Nordstrom Local & Last Chance camera store traffic
-- The code should be set to truncate & update data for last 2 weeks

-- Update cadence: daily

*/


CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.nl_lc_traffic_daily
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
    , purchase_trips            DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , traffic                   DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , traffic_source            VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('Camera', 'Wifi')
    , dw_sys_load_tmstp         TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(store_number, day_date)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2019-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY);


-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.nl_lc_traffic_daily IS 'Nordstrom Local & Last Chance camera store traffic';



SET QUERY_BAND = NONE FOR SESSION;