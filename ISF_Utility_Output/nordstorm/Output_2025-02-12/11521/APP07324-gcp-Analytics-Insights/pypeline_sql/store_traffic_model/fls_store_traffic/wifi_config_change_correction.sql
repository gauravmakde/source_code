SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=wifi_config_change_correction_11521_ACE_ENG;
     Task_Name=wifi_config_change_correction;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.wifi_config_change_correction
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Modified: 03/03/2023

Notes:
-- Given a 3 day delay in assigning customer identity to transaction, this tables has details on coefficents for
-- estimating purchase trip counts from transaction counts


SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.wifi_config_change_correction
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.wifi_config_change_correction

SELECT store_number
     , correction_start_date
     , level_correction
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {fls_traffic_model_t2_schema}.wifi_config_change_correction_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.wifi_config_change_correction_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT
STATISTICS ON {fls_traffic_model_t2_schema}.wifi_config_change_correction COLUMN (store_number, correction_start_date)
;

SET QUERY_BAND = NONE FOR SESSION;