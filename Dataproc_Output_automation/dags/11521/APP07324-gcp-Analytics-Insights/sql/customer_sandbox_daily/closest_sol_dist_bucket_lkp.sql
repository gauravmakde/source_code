

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
---Task_Name=closest_sol_dist_bucket_lkp_build;'*/


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.closest_sol_dist_bucket_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.closest_sol_dist_bucket_lkp
(SELECT closest_sol_dist_bucket_desc,
  ROW_NUMBER() OVER (ORDER BY closest_sol_dist_bucket_desc) AS closest_sol_dist_bucket_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(closest_store_dist_bucket, 'missing') AS closest_sol_dist_bucket_desc
   FROM `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_store_distance_buckets) AS a);


