SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_rn_nsg_wifi_coef_11521_ACE_ENG;
     Task_Name=fls_rn_nsg_wifi_coef;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.fls_rn_nsg_wifi_coef
Team/Owner: tech_ffp_analytics/Agnes Bao
Date Modified: 03/03/2023

Notes:
-- This tables has details on model coefficient for adjusting RetailNext wifi to align with new source of wifi data(NSG)


SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_coef
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_coef

SELECT store_number
    , intercept
    , rn_wifi_slope
    , holiday_dt
    , weekend_dt
    , holiday_rn_wifi_slope
    , weekend_rn_wifi_slope
    , r_squared
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_coef_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_coef_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT
STATISTICS ON {fls_traffic_model_t2_schema}.fls_rn_nsg_wifi_coef COLUMN (store_number)
;

SET QUERY_BAND = NONE FOR SESSION;