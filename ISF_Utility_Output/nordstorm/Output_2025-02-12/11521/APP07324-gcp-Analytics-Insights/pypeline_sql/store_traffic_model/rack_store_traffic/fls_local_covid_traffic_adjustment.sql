SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_local_covid_traffic_adjustment_11521_ACE_ENG;
     Task_Name=fls_local_covid_traffic_adjustment;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.fls_local_covid_traffic_adjustment
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/10/2023

Notes: 
--This table has details on the location based COVID adjustment applied to traffic
--The data for this table can be copied from T3DL_ACE_CORP.fls_local_covid_traffic_adjustment

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.fls_local_covid_traffic_adjustment
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.fls_local_covid_traffic_adjustment

SELECT 	store_number
        , covid_mltplr
        , start_date
        , end_date
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.fls_local_covid_traffic_adjustment_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.fls_local_covid_traffic_adjustment_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.fls_local_covid_traffic_adjustment COLUMN (store_number, start_date)
; 

SET QUERY_BAND = NONE FOR SESSION;