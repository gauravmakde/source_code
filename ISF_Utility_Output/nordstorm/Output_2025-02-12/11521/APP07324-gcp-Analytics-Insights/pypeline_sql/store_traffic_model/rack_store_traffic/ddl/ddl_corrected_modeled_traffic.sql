SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_corrected_modeled_traffic_11521_ACE_ENG;
     Task_Name=ddl_corrected_modeled_traffic;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.corrected_modeled_traffic
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Notes: 
--This table has details on corrected traffic & wifi numbers for days when there are issues with the data that goes into FLS traffic model.
--More details about the known/ongoing issues can be found here - https://confluence.nordstrom.com/display/AS/Known+FLS+Traffic+Data+Issues

*/

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.corrected_modeled_traffic
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(
    store_number              INTEGER NOT NULL
    , day_date                DATE
    , corrected_wifi           INTEGER
    , corrected_traffic         DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00  
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) UNIQUE PRIMARY INDEX(store_number, day_date);

-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.corrected_modeled_traffic IS 'corrected traffic & wifi numbers for days when there are issues with the data that goes into FLS traffic model'; 
SET QUERY_BAND = NONE FOR SESSION;