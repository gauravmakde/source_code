SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_traffic_model_coeff_11521_ACE_ENG;
     Task_Name=ddl_rack_traffic_model_coeff_ldg;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_traffic_model_coeff
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Note:
This landing table is created as part of the s3_to_td job that loads
data from S3 to teradata.  The landing table is dropped when the job completes.

*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{fls_traffic_model_t2_schema}', 'rack_traffic_model_coeff_ldg', OUT_RETURN_MSG);

create multiset table {fls_traffic_model_t2_schema}.rack_traffic_model_coeff_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    coefficient_type varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
    start_date DATE,
    end_date DATE,
    store_number INTEGER,
    store_intercept DECIMAL(20,5)  DEFAULT 0.00 COMPRESS 0.00,
    store_slope DECIMAL(20,5)  DEFAULT 0.00 COMPRESS 0.00,
    store_intercept_pred DECIMAL(20,5) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
    store_slope_pred DECIMAL(20,5) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
    saturday_intercept DECIMAL(20,5) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
    saturday_slope DECIMAL(20,5) NOT NULL DEFAULT 0.00 COMPRESS 0.00,
    region  varchar(10) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
    store_data_type varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    )
PRIMARY INDEX (store_number, start_date, end_date);

 
SET QUERY_BAND = NONE FOR SESSION;