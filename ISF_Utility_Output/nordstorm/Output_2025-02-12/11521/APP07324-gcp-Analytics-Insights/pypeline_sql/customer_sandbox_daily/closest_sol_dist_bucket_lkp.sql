SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=closest_sol_dist_bucket_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Closest JWN Store Distance Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM {str_t2_schema}.closest_sol_dist_bucket_lkp
;

INSERT INTO {str_t2_schema}.closest_sol_dist_bucket_lkp
SELECT DISTINCT closest_sol_dist_bucket_desc
     , ROW_NUMBER() OVER (ORDER BY closest_sol_dist_bucket_desc ASC) AS closest_sol_dist_bucket_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(closest_store_dist_bucket, 'missing') AS closest_sol_dist_bucket_desc
      FROM {str_t2_schema}.customer_store_distance_buckets) a
;


SET QUERY_BAND = NONE FOR SESSION;
