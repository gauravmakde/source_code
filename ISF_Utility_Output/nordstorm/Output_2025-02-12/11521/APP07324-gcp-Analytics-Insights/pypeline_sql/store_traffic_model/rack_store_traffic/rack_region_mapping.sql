SET QUERY_BAND = 'App_ID=APP08151;
     DAG_ID=rack_region_mapping_11521_ACE_ENG;
     Task_Name=rack_region_mapping;'
     FOR SESSION VOLATILE;

/*
Table Name: t2dl_das_fls_traffic_model.rack_region_mapping
Team/Owner: tech_ffp_analytics/Selina Song
Date Created: 02/10/2023

Notes: 
-- Purpose: This table has details on region mapping used for apply adjustments to the model to changes in wifi usage behavior

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE TABLE GETS LOADED WITH DATA FROM STAGING TABLE */
DELETE
FROM {fls_traffic_model_t2_schema}.rack_region_mapping
;

-- LOADING PROD TABLE USING STAGING TABLE
INSERT INTO {fls_traffic_model_t2_schema}.rack_region_mapping

SELECT
    region_group
    , region
    , clbr_start_date
    , clbr_end_date
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp

FROM {fls_traffic_model_t2_schema}.rack_region_mapping_ldg
;

-- DROPPING STAGING TABLE BEFORE NEXT JOB RUN
DROP TABLE {fls_traffic_model_t2_schema}.rack_region_mapping_ldg
;

-- COLLECTING STATS ON PROD TABLE
COLLECT STATISTICS ON {fls_traffic_model_t2_schema}.rack_region_mapping COLUMN (region)
; 

SET QUERY_BAND = NONE FOR SESSION;