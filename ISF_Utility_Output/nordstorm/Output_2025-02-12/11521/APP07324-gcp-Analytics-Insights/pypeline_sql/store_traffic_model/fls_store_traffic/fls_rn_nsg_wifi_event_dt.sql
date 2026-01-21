SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_rn_nsg_wifi_event_dt_11521_ACE_ENG;
     Task_Name=fls_rn_nsg_wifi_event_dt;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.fls_rn_nsg_wifi_event_dt
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Modified: 03/03/2023

Notes:
-- Details on event based adjustment applied to RetailNext wifi adjustment model


SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_event_dt
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_event_dt

SELECT wifi_event_date
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_event_dt_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_event_dt_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT
STATISTICS ON {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_event_dt COLUMN (wifi_event_date)
;

SET QUERY_BAND = NONE FOR SESSION;