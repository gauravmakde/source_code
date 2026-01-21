SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_fls_unplanned_closures_11521_ACE_ENG;
     Task_Name=ddl_fls_unplanned_closures;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.fls_unplanned_closures
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Notes: 
--This tables has details on unplanned store closures
--updates when new csv is uploaded to S3

*/

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.fls_unplanned_closures
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(
    closure_date DATE,
    all_store_flag INTEGER,
    store_number INTEGER,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) UNIQUE PRIMARY INDEX (store_number, closure_date);

-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.fls_unplanned_closures IS 'unplanned store closures'; 
SET QUERY_BAND = NONE FOR SESSION;