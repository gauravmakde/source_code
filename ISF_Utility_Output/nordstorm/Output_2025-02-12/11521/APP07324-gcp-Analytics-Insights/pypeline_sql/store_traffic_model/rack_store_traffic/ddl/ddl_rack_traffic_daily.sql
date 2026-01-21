SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_rack_traffic_daily_11521_ACE_ENG;
     Task_Name=ddl_rack_traffic_daily;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_traffic_daily
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 01/23/2023

Notes: 
-- Purpose: Applies business rules & cleans up final traffic number. This table supports the view that's used by the end-users.
   This job updates the Rack Store Traffic production table.

-- Update cadence: daily

*/ 


CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.rack_traffic_daily
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



-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily IS 'Rack Store Daily Foot Traffic ';
--Column Comments
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.store_number IS 'the store number where the traffic is being measured';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.store_name IS 'the name of the store where the traffic is being measured';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.region IS 'the region the store is located in';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.dma IS 'the DMA the store is located in';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.store_closure_flag IS 'a flag associated with permanantly closed stores';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.day_date IS 'the date for traffic measurement';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.reopen_date IS 'the date when the store re-opened post March 2020 store closure';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.covid_store_closure_flag IS 'a flag to tag 2020 Q1, Q2 COVID store closure';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.wifi_users IS 'the count of visitors who connected to store wifi';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.purchase_trips IS 'the count of customers who did a positive merch transaction at the store';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.traffic IS 'the count of people who visited the store';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_daily.traffic_source IS 'the source of traffic data - camera versus modeled using wifi';


SET QUERY_BAND = NONE FOR SESSION;