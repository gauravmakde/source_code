SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_rack_traffic_estimation_daily_11521_ACE_ENG;
     Task_Name=ddl_rack_traffic_estimation_daily;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_traffic_estimation_daily
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 01/18/2023

Notes: 
-- Purpose: Data Pre-processing, collation + base model traffic estimation for the rack store traffic model
-- Update cadence: daily

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'rack_traffic_estimation_daily', OUT_RETURN_MSG);

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    store_number                    INTEGER NOT NULL
    , store_name                    VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , day_date                      DATE NOT NULL
    , fiscal_week                    INTEGER
    , fiscal_month                   INTEGER
    , fiscal_year                    INTEGER
    , day_454_num                   INTEGER
    , store_open_date               DATE
    , store_close_date              DATE
    , region                        VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dma                           VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC
    , reopen_date                   DATE
    , covid_store_closure_flag      INTEGER
    , holiday_flag                  INTEGER
    , unplanned_closure             INTEGER
    , wifi_users                     DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , purchase_trips                DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , net_sales                      DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , gross_sales                    DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , camera_flag                   INTEGER
    , camera_traffic                DECIMAL(12,0) DEFAULT 0. COMPRESS (0. ,1. ,2. ,3. ,4. ,5. ,-1. )
    , estimated_traffic             DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , dw_sys_load_tmstp             TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
UNIQUE PRIMARY INDEX (store_number, day_date)
PARTITION BY RANGE_N(day_date BETWEEN DATE '2019-01-01' AND DATE '2040-01-01' EACH INTERVAL '1' DAY)
;



-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily IS 'Rack Store Estimated Foot Traffic Using Wifi-Based Model ';
-- Column Comments
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.store_number IS 'the store number where the traffic is being measured';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.store_name IS 'the name of the store where the traffic is being measured';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.region IS 'the region the store is located in';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.dma IS 'the DMA the store is located in';
--COMMENT ON  t2dl_das_fls_traffic_model.rack_traffic_estimation_daily.store_closure_flag IS 'a flag associated with permanantly closed stores';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.day_date IS 'the date for traffic measurement';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.reopen_date IS 'the date when the store re-opened post March 2020 store closure';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.covid_store_closure_flag IS 'a flag to tag 2020 Q1, Q2 COVID store closure';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.wifi_users IS 'the count of visitors who connected to store wifi';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.purchase_trips IS 'the count of customers who did a positive merch transaction at the store';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.camera_flag IS 'the source of traffic data - camera versus modeled using wifi';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.fiscal_week IS 'the week number based on the 454 calendar';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.fiscal_month IS 'the month number based on the 454 calendar';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.fiscal_year IS 'the year number based on 454 calendar';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.day_454_num IS 'the day identifer based on 454 calendar ';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.store_open_date IS 'the date when the store opened';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.store_close_date IS 'the date when the store closed';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.unplanned_closure IS 'the flag to tag any unplanned closures';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.net_sales IS 'the total sales & returns at the store';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.gross_sales IS 'the total sales at the store';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.camera_traffic IS 'the traffic camera measurement for stores with cameras';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.estimated_traffic IS 'the wifi based traffic estimates';
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_estimation_daily.dw_sys_load_tmstp IS 'the timestamp when the record was updated';
 
SET QUERY_BAND = NONE FOR SESSION;