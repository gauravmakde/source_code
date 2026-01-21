SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=corrected_modeled_traffic_11521_ACE_ENG;
     Task_Name=corrected_modeled_traffic;'
     FOR SESSION VOLATILE;

/*

Table Name: t2dl_das_fls_traffic_model.corrected_modeled_traffic
Team/Owner: tech_ffp_analytics/Selina Song
Date Modified: 02/10/2023

Notes: 
--This table has details on corrected traffic & wifi numbers for days when there are issues with the data that goes into FLS traffic model.
--More details about the known/ongoing issues can be found here - https://confluence.nordstrom.com/display/AS/Known+FLS+Traffic+Data+Issues


SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.corrected_modeled_traffic
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.corrected_modeled_traffic

SELECT 	store_number
        , day_date
        , corrected_wifi
        , corrected_traffic
		, CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.corrected_modeled_traffic_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.corrected_modeled_traffic_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.corrected_modeled_traffic COLUMN (store_number, day_date)
; 

SET QUERY_BAND = NONE FOR SESSION;