SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=camera_store_details_11521_ACE_ENG;
     Task_Name=ddl_camera_store_details_ldg;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.camera_store_details
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/01/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'camera_store_details_ldg', OUT_RETURN_MSG);

create multiset table {fls_traffic_model_t2_schema}.camera_store_details_ldg
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
    store_type VARCHAR(2)
    )
UNIQUE PRIMARY INDEX (store_number,start_date, traffic_source);
 

SET QUERY_BAND = NONE FOR SESSION;