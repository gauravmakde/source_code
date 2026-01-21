SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=ddl_fls_camera_store_details_11521_ACE_ENG;
     Task_Name=ddl_fls_camera_store_details;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.fls_camera_store_details
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Created/Modified: 03/03/2023

Note:
-- Details on functioning vs cameras with imputed traffic

*/

create multiset table {fls_traffic_model_t2_schema}.fls_camera_store_details
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    store_number INTEGER
    ,imputation_flag INTEGER
    ,intercept DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00
    ,slope DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00
    ,start_date DATE
    ,end_date DATE
    ,dw_sys_load_tmstp      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
UNIQUE PRIMARY INDEX (store_number, start_date, end_date)
;

-- Table Comment (STANDARD)
COMMENT ON {fls_traffic_model_t2_schema}.fls_camera_store_details IS 'Details on functioning vs cameras with imputed traffic';
SET QUERY_BAND = NONE FOR SESSION;