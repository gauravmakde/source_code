SET QUERY_BAND = 'App_ID=APP08176;
     DAG_ID=mothership_bot_traffic_fix_11521_ACE_ENG;
     Task_Name=bot_traffic_fix;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_mothership.bot_traffic_fix
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Modified: 09/18/2023

Notes:
creates table with estimated bot traffic during known bot attacks. 
This table then subtracted from visitor_funnel_fact traffic numbers


SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {mothership_t2_schema}.bot_traffic_fix
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {mothership_t2_schema}.bot_traffic_fix

SELECT day_date
     , box
     , device_type
     , suspicious_visitors
     , suspicious_viewing_visitors
     , suspicious_adding_visitors
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {mothership_t2_schema}.bot_traffic_fix_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {mothership_t2_schema}.bot_traffic_fix_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT
STATISTICS ON {mothership_t2_schema}.bot_traffic_fix COLUMN (day_date)
;

SET QUERY_BAND = NONE FOR SESSION;