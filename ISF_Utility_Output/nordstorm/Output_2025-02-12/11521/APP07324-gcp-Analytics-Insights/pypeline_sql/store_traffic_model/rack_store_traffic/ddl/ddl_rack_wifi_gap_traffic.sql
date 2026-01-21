SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_rack_wifi_gap_traffic_11521_ACE_ENG;
     Task_Name=ddl_rack_wifi_gap_traffic;'
     FOR SESSION VOLATILE;

/*
Table Name: t2dl_das_fls_traffic_model.rack_wifi_gap_traffic
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/01/2023

Notes:
--This table has estimated traffic for fiscal Feb & Mar 2019 and estimated wifi for Apr to Jun 2019. 
--For traffic due to missing wifi data from Feb to Mar 2019, a more complex technique was used to estimate traffic which can't be implemented using SQL
--For wifi data from Apr to Jun 2019, due to missing fields used for cleaning wifi, an estimated wifi number for calculated for this time period using raw wifi data
--The data for this table can be copied from T3DL_ACE_CORP.rack_wifi_gap_traffic

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'rack_wifi_gap_traffic', OUT_RETURN_MSG);

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
 (
    store_number INTEGER,
    day_dt date,
    wifi_count DECIMAL(20,5) DEFAULT 0.00 COMPRESS 0.00,
    traffic DECIMAL(20,5) DEFAULT 0.00 COMPRESS 0.00,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) UNIQUE PRIMARY INDEX (store_number, day_dt);

-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic IS 'estimated traffic for fiscal Feb & Mar 2019 and estimated wifi for Apr to Jun 2019'; 
SET QUERY_BAND = NONE FOR SESSION;