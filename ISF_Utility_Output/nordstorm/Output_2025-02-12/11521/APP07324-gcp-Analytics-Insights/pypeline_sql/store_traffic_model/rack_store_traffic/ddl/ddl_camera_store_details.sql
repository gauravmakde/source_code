SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_camera_store_details_11521_ACE_ENG;
     Task_Name=ddl_camera_store_details;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.camera_store_details
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/01/2023

Notes: 
--This tables has details on on functioning vs cameras with imputed traffic
--the values for this table can be read from t3dl_ace_corp.camera_store_details

*/

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.camera_store_details
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
(
    store_number INTEGER,
    imputation_flag INTEGER,
    intercept DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    slope DECIMAL(20,15) DEFAULT 0.00 COMPRESS 0.00,
    start_date DATE DEFAULT DATE'1900-01-01',
    end_date DATE DEFAULT DATE'2099-12-31',
    traffic_source VARCHAR(15),
    store_type VARCHAR(2),
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) UNIQUE PRIMARY INDEX (store_number,start_date, traffic_source);


-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.camera_store_details IS 'details on on functioning vs cameras with imputed traffic';
 
SET QUERY_BAND = NONE FOR SESSION;