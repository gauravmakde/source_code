SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_fls_store_traffic_model_11521_ACE_ENG;
     Task_Name=ddl_fls_traffic_model_calibration_coeff;'
     FOR SESSION VOLATILE;

/*
t2dl_das_fls_traffic_model.fls_traffic_model_calibration_coeff
This job supports fls model calibration based on store's region
*/

CREATE MULTISET TABLE T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_model_calibration_coeff
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
(
    region_group                     VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
    , week_start_date                  DATE NOT NULL
    , week_end_date                    DATE NOT NULL
    , last_3wk_ratio                   DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , pre_cal_1wk_ratio                DECIMAL(20,2) DEFAULT 0.00 COMPRESS 0.00
    , dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
UNIQUE PRIMARY INDEX (region_group, week_start_date);

COMMENT ON T2DL_DAS_FLS_TRAFFIC_MODEL.fls_traffic_model_calibration_coeff IS 'FLS model calibration based on store region';
SET QUERY_BAND = NONE FOR SESSION;