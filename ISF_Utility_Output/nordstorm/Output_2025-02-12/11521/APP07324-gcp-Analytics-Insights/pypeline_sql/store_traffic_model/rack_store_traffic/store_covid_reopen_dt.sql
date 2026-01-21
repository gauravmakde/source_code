SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=store_covid_reopen_dt_11521_ACE_ENG;
     Task_Name=store_covid_reopen_dt;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.store_covid_reopen_dt
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/07/2023

Notes: 
--This table has post-COVID store reopening dates

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.store_covid_reopen_dt
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.store_covid_reopen_dt

SELECT 	store_number
        , reopen_dt
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.store_covid_reopen_dt_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.store_covid_reopen_dt_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.store_covid_reopen_dt COLUMN (store_number)
;
 
SET QUERY_BAND = NONE FOR SESSION;