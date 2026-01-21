SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_traffic_model_coeff_11521_ACE_ENG;
     Task_Name=rack_traffic_model_coeff;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_traffic_model_coeff
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Notes: 
--This tables has details on model coefficients for traffic estimation using wifi
--the values for this table can be read from t3dl_ace_corp.rack_traffic_model_coeff

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.rack_traffic_model_coeff
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.rack_traffic_model_coeff
 
SELECT 	coefficient_type
        , start_date
        , end_date
        , store_number
        , store_intercept
        , store_slope
        , store_intercept_pred
        , store_slope_pred
        , saturday_intercept
        , saturday_slope
        , region
        , store_data_type
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.rack_traffic_model_coeff_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.rack_traffic_model_coeff_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.rack_traffic_model_coeff COLUMN (store_number, start_date, end_date)
;

SET QUERY_BAND = NONE FOR SESSION;