SET QUERY_BAND = 'App_ID=APP09037; 
     DAG_ID=mothership_ets_manual_11521_ACE_ENG;
     Task_Name=ets_manual;'
     FOR SESSION VOLATILE;

/*
Table Name: t2dl_das_mothership.ets_manual
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Modified: 07/19/2023

Notes:
ETS = Executive Telemetry Scorecard tableau dashboard: https://tableau.nordstrom.com/#/site/AS/workbooks/32064/views
creates table of data from stakeholders with complex manual (non-automatable in NAP) calculations

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.
*/

--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {mothership_t2_schema}.ets_manual
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {mothership_t2_schema}.ets_manual
 
SELECT box
     , metric               
     , fiscal_year                 
     , time_period          
     , actuals
     , plan
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {mothership_t2_schema}.ets_manual_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {mothership_t2_schema}.ets_manual_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT
STATISTICS ON {mothership_t2_schema}.ets_manual COLUMN (box, metric, time_period)
;

SET QUERY_BAND = NONE FOR SESSION;