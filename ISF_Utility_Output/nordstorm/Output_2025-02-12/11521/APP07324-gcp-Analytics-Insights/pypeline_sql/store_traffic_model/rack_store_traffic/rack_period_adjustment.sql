SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_period_adjustment_11521_ACE_ENG;
     Task_Name=rack_period_adjustment;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_period_adjustment
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/10/2023

Notes: 
-- Purpose: This table has details on additional event based adjustments applied to rack traffic estimation model

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.rack_period_adjustment
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.rack_period_adjustment

SELECT
    period_description
    , period_type
    , event_type
    , region
    , intercept
    , slope
    , start_date
    , end_date
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.rack_period_adjustment_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.rack_period_adjustment_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.rack_period_adjustment COLUMN (period_description)
; 

SET QUERY_BAND = NONE FOR SESSION;