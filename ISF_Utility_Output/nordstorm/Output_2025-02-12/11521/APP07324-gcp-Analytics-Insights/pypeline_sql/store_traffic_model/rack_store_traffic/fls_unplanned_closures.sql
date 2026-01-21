SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_unplanned_closures_11521_ACE_ENG;
     Task_Name=fls_unplanned_closures;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.fls_unplanned_closures
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/10/2023

Notes: 
--This tables has details on unplanned store closures
--updates when new csv is uploaded to S3

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.fls_unplanned_closures
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.fls_unplanned_closures

SELECT 	closure_date
        , all_store_flag
        , store_number
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.fls_unplanned_closures_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.fls_unplanned_closures_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.fls_unplanned_closures COLUMN (store_number, closure_date)
; 

SET QUERY_BAND = NONE FOR SESSION;