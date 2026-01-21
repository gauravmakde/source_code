SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_store_covid_reopen_dt_11521_ACE_ENG;
     Task_Name=ddl_store_covid_reopen_dt;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.store_covid_reopen_dt
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Notes: 
--This table has post-COVID store reopening dates

*/

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.store_covid_reopen_dt
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(
    store_number INTEGER,
    reopen_dt DATE,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) UNIQUE PRIMARY INDEX (store_number);

-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.store_covid_reopen_dt IS 'post-COVID store reopening dates'; 
SET QUERY_BAND = NONE FOR SESSION;