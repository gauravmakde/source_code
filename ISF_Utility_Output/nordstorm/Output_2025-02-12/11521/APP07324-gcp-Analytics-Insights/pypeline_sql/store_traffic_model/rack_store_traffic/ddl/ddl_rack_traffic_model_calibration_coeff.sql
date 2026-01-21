SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=ddl_rack_traffic_model_calibration_coeff_11521_ACE_ENG;
     Task_Name=ddl_rack_traffic_model_calibration_coeff;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_traffic_model_calibration_coeff
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 01/23/2023

Notes: 
-- Purpose: Calculates calibration factor for the last complete week based on estimated to actual traffic ratio for the last 3 weeks; 
   supports rack model calibration based on store's region

-- Update cadence: daily

*/


CREATE MULTISET TABLE {fls_traffic_model_t2_schema}.rack_traffic_model_calibration_coeff
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
    , dw_sys_load_tmstp             TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)

UNIQUE PRIMARY INDEX (region_group, week_start_date)
;



-- Table Comment
COMMENT ON {fls_traffic_model_t2_schema}.rack_traffic_model_calibration_coeff IS 'rack model calibration based on stores region';
-- Column Comments
COMMENT ON {fls_traffic_model_t2_schema}.rack_traffic_model_calibration_coeff.region_group IS 'the region level aggregation for checking model accuracy and applying correction';
COMMENT ON {fls_traffic_model_t2_schema}.rack_traffic_model_calibration_coeff.week_start_date IS 'the start of week date for applying correction';
COMMENT ON {fls_traffic_model_t2_schema}.rack_traffic_model_calibration_coeff.week_end_date IS 'the end of week date for applying correction';
COMMENT ON {fls_traffic_model_t2_schema}.rack_traffic_model_calibration_coeff.last_3wk_ratio IS 'the model accuracy for the last 3 weeks';
COMMENT ON {fls_traffic_model_t2_schema}.rack_traffic_model_calibration_coeff.pre_cal_1wk_ratio IS 'the model accuracy for the last 1 week';
 
SET QUERY_BAND = NONE FOR SESSION;