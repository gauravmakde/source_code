SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=closest_store_banner_lkp_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Closest Store Banner Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild
DELETE
FROM {str_t2_schema}.closest_store_banner_lkp
;

INSERT INTO {str_t2_schema}.closest_store_banner_lkp
SELECT DISTINCT closest_store_banner_desc
     , ROW_NUMBER() OVER (ORDER BY closest_store_banner_desc ASC) AS closest_store_banner_num
     , CURRENT_TIMESTAMP(6) AS dw_sys_load_tmstp
FROM (SELECT DISTINCT COALESCE(closest_store_banner, 'missing') AS closest_store_banner_desc
      FROM {str_t2_schema}.customer_store_distance_buckets) a ;


SET QUERY_BAND = NONE FOR SESSION;
