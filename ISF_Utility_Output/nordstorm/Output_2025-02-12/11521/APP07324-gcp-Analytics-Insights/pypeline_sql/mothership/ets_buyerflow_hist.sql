SET QUERY_BAND = 'App_ID=APP09037;
     DAG_ID=ets_buyerflow_hist_11521_ACE_ENG;
     Task_Name=ets_buyerflow_hist;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_mothership.ets_buyerflow_hist
Team/Owner: tech_ffp_analytics/Matthew Bond
Date Modified: 07/19/2023

Notes:
ETS = Executive Telemetry Scorecard tableau dashboard: https://tableau.nordstrom.com/#/site/AS/workbooks/32064/views
creates table for rack strategic brands (RSB) to use in sales calculations


SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {mothership_t2_schema}.ets_buyerflow_hist
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {mothership_t2_schema}.ets_buyerflow_hist

SELECT
      fiscal_year
      , channel
      , total_customers_ly
      , total_trips_ly
      , acquired_ly
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {mothership_t2_schema}.ets_buyerflow_hist_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {mothership_t2_schema}.ets_buyerflow_hist_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT
STATISTICS ON {mothership_t2_schema}.ets_buyerflow_hist COLUMN (fiscal_year, channel)
;

SET QUERY_BAND = NONE FOR SESSION;