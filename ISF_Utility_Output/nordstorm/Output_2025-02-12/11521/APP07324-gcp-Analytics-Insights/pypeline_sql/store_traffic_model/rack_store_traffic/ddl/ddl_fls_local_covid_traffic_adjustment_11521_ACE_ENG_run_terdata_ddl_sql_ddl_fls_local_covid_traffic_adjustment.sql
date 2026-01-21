SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_fls_local_covid_traffic_adjustment_11521_ACE_ENG;
     Task_Name=ddl_fls_local_covid_traffic_adjustment;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.fls_local_covid_traffic_adjustment
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Notes:
--This table has details on the location based COVID adjustment applied to traffic
--The data for this table can be copied from T3DL_ACE_CORP.fls_local_covid_traffic_adjustment

*/

CREATE MULTISET TABLE T2DL_DAS_FLS_TRAFFIC_MODEL.fls_local_covid_traffic_adjustment
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(
    store_number INTEGER,
    covid_mltplr DECIMAL(20,15) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
    start_date DATE,
    end_date DATE,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) UNIQUE PRIMARY INDEX (store_number, start_date);

-- Table Comment
COMMENT ON  T2DL_DAS_FLS_TRAFFIC_MODEL.fls_local_covid_traffic_adjustment IS 'location based COVID adjustment applied to traffic';
SET QUERY_BAND = NONE FOR SESSION;