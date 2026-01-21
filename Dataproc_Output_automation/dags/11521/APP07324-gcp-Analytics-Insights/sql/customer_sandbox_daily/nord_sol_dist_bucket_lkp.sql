
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.nord_sol_dist_bucket_lkp;

INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.nord_sol_dist_bucket_lkp
(SELECT nord_sol_dist_bucket_desc,
  ROW_NUMBER() OVER (ORDER BY nord_sol_dist_bucket_desc) AS nord_sol_dist_bucket_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(nord_sol_dist_bucket, 'missing') AS nord_sol_dist_bucket_desc
   FROM `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_store_distance_buckets) AS a);

