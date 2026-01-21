SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=closest_store_banner_lkp_11521_ACE_ENG;
     Task_Name=closest_store_banner_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.closest_store_banner_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/13/2023

Notes:
-- Closest Store Banner Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.closest_store_banner_lkp
;

INSERT INTO {str_t2_schema}.closest_store_banner_lkp
    SELECT DISTINCT closest_store_banner_desc
		,ROW_NUMBER() OVER (ORDER BY closest_store_banner_desc  ASC ) AS closest_store_banner_num
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
	from (SELECT DISTINCT coalesce(closest_store_banner, 'missing') as closest_store_banner_desc  
                FROM {str_t2_schema}.customer_store_distance_buckets a)a ;
                

SET QUERY_BAND = NONE FOR SESSION;