

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.item_intent_lookup{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.item_intent_lookup{{params.env_suffix}}
(SELECT k.plan_week_calc,
  k.plan_month,
  k.plan_year,
  SUBSTR(k.banner, 1, 15) AS banner,
  SUBSTR(k.channel_country, 1, 2) AS channel_country,
  k.channel_num,
  k.channel_desc,
  k.dept_num,
  k.rms_style_num,
  k.sku_nrf_color_num,
  k.vpn,
  k.supplier_color,
  k.intended_plan_type,
  k.intended_season,
  k.intended_exit_month_year,
  k.intended_lifecycle_type,
  k.scaled_event,
  k.holiday_or_celebration,
  k.dw_sys_load_tmstp
 FROM (SELECT CAST(TRUNC(CAST(CONCAT(plan_year, TRIM(CASE
        WHEN LENGTH(SUBSTR(plan_week, STRPOS(LOWER(plan_week), LOWER('_')) + 1)) = 1
        THEN FORMAT('%4d', 0) || SUBSTR(plan_week, (STRPOS(LOWER(plan_week), LOWER('_')) + 1))
        ELSE SUBSTR(plan_week, STRPOS(LOWER(plan_week), LOWER('_')) + 1)
        END)) AS FLOAT64)) AS INTEGER) AS plan_week_calc,
    plan_month,
    plan_year,
    channel_brand AS banner,
    channel_country,
     CASE
     WHEN LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN 250
     WHEN LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN 120
     WHEN LOWER(selling_channel) = LOWER('STORE') AND LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN 210
     WHEN LOWER(selling_channel) = LOWER('STORE') AND LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN 110
     ELSE NULL
     END AS channel_num,
     CASE
     WHEN LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN '250, OFFPRICE ONLINE'
     WHEN LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN '120, N.COM'
     WHEN LOWER(selling_channel) = LOWER('STORE') AND LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
     THEN '210, RACK STORES'
     WHEN LOWER(selling_channel) = LOWER('STORE') AND LOWER(channel_brand) = LOWER('NORDSTROM')
     THEN '110, FULL LINE STORES'
     ELSE NULL
     END AS channel_desc,
    CAST(trunc(cast(CASE
      WHEN dept_num = ''
      THEN '0'
      ELSE dept_num
      END As float64)) AS INTEGER) AS dept_num,
    rms_style_num,
    sku_nrf_color_num,
    vpn,
    supplier_color,
    intended_plan_type,
    intended_season,
    intended_exit_month_year,
    intended_lifecycle_type,
    scaled_event,
    holiday_or_celebration,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw AS item
   QUALIFY (DENSE_RANK() OVER (ORDER BY plan_week DESC)) < 57) AS k
  INNER JOIN (SELECT DISTINCT week_idnt,
    month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
   WHERE week_end_day_date <= CURRENT_DATE('PST8PDT')
   QUALIFY (ROW_NUMBER() OVER (ORDER BY week_idnt DESC)) <= 52 * 7) AS cal ON k.plan_week_calc = cal.week_idnt);
   
