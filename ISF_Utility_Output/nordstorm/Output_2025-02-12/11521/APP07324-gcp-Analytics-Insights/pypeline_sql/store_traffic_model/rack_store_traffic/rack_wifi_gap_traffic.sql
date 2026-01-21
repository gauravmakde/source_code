SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_wifi_gap_traffic_11521_ACE_ENG;
     Task_Name=rack_wifi_gap_traffic;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.rack_wifi_gap_traffic
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 09/01/2023

Notes: 
--This table has estimated traffic for fiscal Feb & Mar 2019 and estimated wifi for Apr to Jun 2019. 
--For traffic due to missing wifi data from Feb to Mar 2019, a more complex technique was used to estimate traffic which can't be implemented using SQL
--For wifi data from Apr to Jun 2019, due to missing fields used for cleaning wifi, an estimated wifi number for calculated for this time period using raw wifi data

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/
-- collect stats on ldg table
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic_ldg COLUMN (store_number, day_dt)
;

--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic
WHERE day_dt BETWEEN {start_date} and {end_date}
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic

SELECT 	store_number
	    , day_dt
	    , wifi_count
	    , traffic
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic_ldg
WHERE day_dt BETWEEN {start_date} and {end_date}
;


-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.rack_wifi_gap_traffic COLUMN (store_number, day_dt)
; 

SET QUERY_BAND = NONE FOR SESSION;