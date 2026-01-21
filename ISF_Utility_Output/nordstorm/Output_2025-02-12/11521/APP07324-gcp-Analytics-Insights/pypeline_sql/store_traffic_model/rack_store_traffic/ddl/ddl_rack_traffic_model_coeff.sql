SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_rack_traffic_model_coeff_11521_ACE_ENG;
     Task_Name=ddl_rack_traffic_model_coeff;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_traffic_model_coeff
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Notes: 
--This tables has details on model coefficients for traffic estimation using wifi
--the values for this table can be read from t3dl_ace_corp.rack_traffic_model_coeff

*/

CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.rack_traffic_model_coeff
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
    store_data_type varchar(15) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) PRIMARY INDEX (store_number, start_date, end_date);

-- Table Comment
COMMENT ON  {fls_traffic_model_t2_schema}.rack_traffic_model_coeff IS 'model coefficients for traffic estimation using wifi';
 
SET QUERY_BAND = NONE FOR SESSION;