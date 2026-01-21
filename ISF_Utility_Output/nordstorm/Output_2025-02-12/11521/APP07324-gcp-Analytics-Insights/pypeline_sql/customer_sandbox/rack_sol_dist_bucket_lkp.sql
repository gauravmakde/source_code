SET QUERY_BAND = 'App_ID=APP08737;
     DAG_ID=rack_sol_dist_bucket_lkp_11521_ACE_ENG;
     Task_Name=rack_sol_dist_bucket_lkp;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_strategy.rack_sol_dist_bucket_lkp
Team/Owner: Customer Analytics/Niharika Srivastava
Date Created/Modified: 04/13/2023

Notes:
-- Closest Nordstrom Rack Store Distance Look-up table for microstrategy customer sandbox
*/


--DELETING ALL ROWS FROM PROD TABLE BEFORE REBUILD */
DELETE
FROM {str_t2_schema}.rack_sol_dist_bucket_lkp
;

INSERT INTO {str_t2_schema}.rack_sol_dist_bucket_lkp
    SELECT DISTINCT rack_sol_dist_bucket_desc
		,ROW_NUMBER() OVER (ORDER BY rack_sol_dist_bucket_desc  ASC ) AS rack_sol_dist_bucket_num
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
	from (SELECT DISTINCT coalesce(rack_sol_dist_bucket, 'missing') as rack_sol_dist_bucket_desc  
                FROM {str_t2_schema}.customer_store_distance_buckets a)a ;
                

SET QUERY_BAND = NONE FOR SESSION;