SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_camera_store_details_11521_ACE_ENG;
     Task_Name=fls_camera_store_details;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.fls_camera_store_details
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Modified: 03/03/2023

Notes:
-- Details on functioning vs cameras with imputed traffic


SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.fls_camera_store_details
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.fls_camera_store_details

SELECT store_number
     , imputation_flag
     , intercept
     , slope
     , start_date
     , end_date
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {fls_traffic_model_t2_schema}.fls_camera_store_details_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.fls_camera_store_details_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT
STATISTICS ON {fls_traffic_model_t2_schema}.fls_camera_store_details COLUMN (store_number)
;

SET QUERY_BAND = NONE FOR SESSION;