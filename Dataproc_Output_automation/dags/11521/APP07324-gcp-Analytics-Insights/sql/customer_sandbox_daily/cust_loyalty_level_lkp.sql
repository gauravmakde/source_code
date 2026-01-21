
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_loyalty_level_lkp;

INSERT INTO `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cust_loyalty_level_lkp
(SELECT cust_loyalty_level_desc,
  ROW_NUMBER() OVER (ORDER BY cust_loyalty_level_desc) AS cust_loyalty_level_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT COALESCE(cust_loyalty_level, 'Unknown') AS cust_loyalty_level_desc
   FROM `{{params.gcp_project_id}}`.{{params.str_t2_schema}}.cco_cust_chan_yr_attributes) AS a);

