SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=camera_store_details_11521_ACE_ENG;
     Task_Name=camera_store_details;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.camera_store_details
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/10/2023

Notes: 
--This tables has details on on functioning vs cameras with imputed traffic
--the values for this table can be read from t3dl_ace_corp.camera_store_details

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.camera_store_details
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.camera_store_details

SELECT 	store_number
        , imputation_flag
        , intercept
        , slope
        , start_date
        , end_date
        , traffic_source
        , store_type
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.camera_store_details_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.camera_store_details_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.camera_store_details COLUMN (store_number,start_date, traffic_source)
; 

SET QUERY_BAND = NONE FOR SESSION;