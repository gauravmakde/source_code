-- SET QUERY_BAND = 'App_ID=APP08737;
-- DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
-- Task_Name=cust_jwn_trip_bucket_lkp_build;'
-- FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Trip Bucket Look-up table for microstrategy customer sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/


-- deleting all rows from prod table before rebuild

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_jwn_trip_bucket_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_jwn_trip_bucket_lkp
(SELECT cust_jwn_trip_bucket_desc,
  ROW_NUMBER() OVER (ORDER BY cust_jwn_trip_bucket_desc) AS cust_jwn_trip_bucket_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(CASE
      WHEN cust_jwn_trips < 10
      THEN '0' || SUBSTR(CAST(cust_jwn_trips AS STRING), 1, 1) || ' trips'
      WHEN cust_jwn_trips < 30
      THEN SUBSTR(CAST(cust_jwn_trips AS STRING), 1, 2) || ' trips'
      ELSE '30+ trips'
      END, 'Unknown') AS cust_jwn_trip_bucket_desc
   FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_cust_chan_yr_attributes) AS a);

