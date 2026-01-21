SET QUERY_BAND = 'App_ID=APP09037;
     DAG_ID=ets_rsb_lookup_11521_ACE_ENG;
     Task_Name=ets_rsb_lookup;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_mothership.ets_rsb_lookup
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
FROM {mothership_t2_schema}.ets_rsb_lookup
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {mothership_t2_schema}.ets_rsb_lookup

SELECT Brand
     , Strategic_Brand
     , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM {mothership_t2_schema}.ets_rsb_lookup_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {mothership_t2_schema}.ets_rsb_lookup_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT
STATISTICS ON {mothership_t2_schema}.ets_rsb_lookup COLUMN (Brand)
;

SET QUERY_BAND = NONE FOR SESSION;