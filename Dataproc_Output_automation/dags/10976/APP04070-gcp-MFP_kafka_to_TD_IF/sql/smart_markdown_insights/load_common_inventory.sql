
-- this is the initial load into CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT
-- targeted metrics will be loaded in subsequent loads
-- all subsequent load will simply update these initial rows

-- store param so only 1 reference



CREATE TEMPORARY TABLE IF NOT EXISTS my_params AS
SELECT dw_batch_dt AS last_sat
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
WHERE LOWER(interface_code) = LOWER('SMD_INSIGHTS_WKLY');


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact (snapshot_date, rms_style_num, color_num,
 channel_country, channel_brand, selling_channel, total_inv_qty, total_inv_dollars, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz
 )
(SELECT (SELECT *
   FROM my_params) AS snapshot_date,
  rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  selling_channel,
  total_inv_qty,
  total_inv_dollars,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_stock_quantity_end_of_wk_vw);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact (snapshot_date, rms_style_num, color_num,
 channel_country, channel_brand, selling_channel, total_inv_qty, total_inv_dollars, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz
 )
(SELECT DISTINCT cwibwf.snapshot_date,
  cwibwf.rms_style_num,
  cwibwf.color_num,
  cwibwf.channel_country,
  cwibwf.channel_brand,
  'ONLINE' AS selling_channel,
  0 AS total_inv_qty,
  0 AS total_inv_dollars,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cwibwf
  LEFT JOIN (SELECT channel_country,
    channel_brand,
    rms_style_num,
    color_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact
   WHERE LOWER(selling_channel) = LOWER('ONLINE')
    AND snapshot_date = (SELECT *
      FROM my_params)) AS online ON LOWER(COALESCE(cwibwf.rms_style_num, 'NA')) = LOWER(COALESCE(online.rms_style_num,
        'NA')) AND LOWER(COALESCE(cwibwf.color_num, 'NA')) = LOWER(COALESCE(online.color_num, 'NA')) AND LOWER(cwibwf.channel_brand
      ) = LOWER(online.channel_brand) AND LOWER(cwibwf.channel_country) = LOWER(online.channel_country)
 WHERE online.channel_country IS NULL
  AND cwibwf.snapshot_date = (SELECT *
    FROM my_params));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact (snapshot_date, rms_style_num, color_num,
 channel_country, channel_brand, selling_channel, total_inv_qty, total_inv_dollars, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz
 )
(SELECT DISTINCT cwibwf.snapshot_date,
  cwibwf.rms_style_num,
  cwibwf.color_num,
  cwibwf.channel_country,
  cwibwf.channel_brand,
  'STORE' AS selling_channel,
  0 AS total_inv_qty,
  0 AS total_inv_dollars,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cwibwf
  LEFT JOIN (SELECT channel_country,
    channel_brand,
    rms_style_num,
    color_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact
   WHERE LOWER(selling_channel) = LOWER('STORE')
    AND snapshot_date = (SELECT *
      FROM my_params)) AS store ON LOWER(COALESCE(cwibwf.rms_style_num, 'NA')) = LOWER(COALESCE(store.rms_style_num,
        'NA')) AND LOWER(COALESCE(cwibwf.color_num, 'NA')) = LOWER(COALESCE(store.color_num, 'NA')) AND LOWER(cwibwf.channel_brand
      ) = LOWER(store.channel_brand) AND LOWER(cwibwf.channel_country) = LOWER(store.channel_country)
 WHERE store.channel_country IS NULL
  AND cwibwf.snapshot_date = (SELECT *
    FROM my_params));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact (snapshot_date, rms_style_num, color_num,
 channel_country, channel_brand, selling_channel, total_inv_qty, total_inv_dollars, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz
 )
(SELECT snapshot_date,
  rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  'OMNI' AS selling_channel,
  SUM(total_inv_qty) AS total_inv_qty,
  SUM(total_inv_dollars) AS total_inv_dollars,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact
 WHERE snapshot_date = (SELECT *
    FROM my_params)
  AND (LOWER(channel_country) <> LOWER('CA') OR LOWER(channel_brand) <> LOWER('NORDSTROM_RACK'))
 GROUP BY snapshot_date,
  rms_style_num,
  color_num,
  channel_country,
  channel_brand,
  selling_channel,
  dw_sys_load_tmstp,
  dw_sys_updt_tmstp);