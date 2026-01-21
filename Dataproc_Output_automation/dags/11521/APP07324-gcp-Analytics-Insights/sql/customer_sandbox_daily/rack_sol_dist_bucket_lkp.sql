TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.rack_sol_dist_bucket_lkp;


INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.rack_sol_dist_bucket_lkp (
    rack_sol_dist_bucket_desc,
    rack_sol_dist_bucket_num,
    dw_sys_load_tmstp
)
SELECT DISTINCT 
    rack_sol_dist_bucket_desc,
    ROW_NUMBER() OVER (ORDER BY rack_sol_dist_bucket_desc ASC) AS rack_sol_dist_bucket_num,
    CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
FROM (
    SELECT DISTINCT 
        COALESCE(rack_sol_dist_bucket, 'missing') AS rack_sol_dist_bucket_desc
    FROM `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.customer_store_distance_buckets
) a;