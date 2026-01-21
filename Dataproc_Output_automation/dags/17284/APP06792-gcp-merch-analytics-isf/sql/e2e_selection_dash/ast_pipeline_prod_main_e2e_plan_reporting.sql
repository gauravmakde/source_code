/*
Plan & LY/LLY Data Update
Author: Sara Riker & David Selover
8/15/22: Create Script

Updates Tables with plans:
    T2DL_DAS_SELECTION.e2e_selection_planning 
*/
 

CREATE TEMPORARY TABLE IF NOT EXISTS hierarchy
CLUSTER BY dept_num
AS
SELECT dept_num,
 dept_name,
 division_num,
 division_name,
 subdivision_num,
 subdivision_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim
WHERE LOWER(division_name) NOT LIKE LOWER('INACTIVE%');


--COLLECT STATS      PRIMARY INDEX(dept_num)      ,COLUMN (division_num)      ,COLUMN (subdivision_num)      ON hierarchy


CREATE TEMPORARY TABLE IF NOT EXISTS month_ref
CLUSTER BY month_idnt
AS
SELECT DISTINCT month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 fiscal_year_num,
 fiscal_month_num,
 month_start_day_date,
 month_idnt AS plan_month_idnt,
  month_idnt - 100 AS ly_month_idnt,
  month_idnt - 200 AS lly_month_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE month_idnt BETWEEN (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE) AND (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = DATE_ADD(CURRENT_DATE, INTERVAL 548 DAY));


--COLLECT STATS      PRIMARY INDEX(month_idnt)      ,COLUMN (quarter_idnt)      ,COLUMN (fiscal_year_num)      ON month_ref

CREATE TEMPORARY TABLE IF NOT EXISTS plan_data
CLUSTER BY channel_brand, dept_idnt, category
AS
SELECT CONCAT('20', SUBSTR(plan_fiscal_month, -2), CASE
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('FEB') THEN '01'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('MAR') THEN '02'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('APR') THEN '03'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('MAY') THEN '04'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('JUN') THEN '05'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('JUL') THEN '06'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('AUG') THEN '07'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('SEP') THEN '08'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('OCT') THEN '09'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('NOV') THEN '10'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('DEC') THEN '11'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 1, 3)) = LOWER('JAN') THEN '12'
   ELSE NULL
   END) AS month_idnt,
  CASE
  WHEN LOWER(cluster_label) LIKE LOWER('N.COM') THEN 'NORDSTROM'
  WHEN LOWER(cluster_label) LIKE LOWER('RACK.COM') THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
 SUBSTR(dept_category, 1, STRPOS(dept_category, '_') - 1) AS dept_idnt,
 SUBSTR(dept_category, STRPOS(dept_category, '_') + 1) AS category,
 plan_dmd_r,
 plan_dmd_u,
 plan_net_sls_r,
 plan_net_sls_u,
 plan_rcpt_r,
 plan_rcpt_u,
 model_los_ccs,
 plan_los_ccs,
 model_new_ccs,
 plan_new_ccs,
 model_cf_ccs,
 plan_cf_ccs,
 model_sell_offs_ccs,
 plan_sell_off_ccs,
 ly_sell_off_ccs,
 lly_sell_off_ccs,
 rcd_load_date,
 ROW_NUMBER() OVER (PARTITION BY plan_fiscal_month, cluster_label, dept_category ORDER BY rcd_load_date DESC) AS date_rank
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.t3dl_nap_planning.sel_dept_category_plan
WHERE LOWER(dept_category) NOT LIKE LOWER('%TOTAL%');


--COLLECT STATS      PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)      ,COLUMN (month_idnt)      ON plan_data


CREATE TEMPORARY TABLE IF NOT EXISTS plan_insert
CLUSTER BY channel_brand, dept_idnt, category
AS
SELECT p.month_idnt,
 m.month_start_day_date,
 m.fiscal_month_num,
 m.fiscal_year_num,
 m.plan_month_idnt,
 m.ly_month_idnt,
 m.lly_month_idnt,
 m.month_label,
 m.quarter_idnt,
 m.quarter_label,
 p.channel_brand,
 h.division_num,
 h.division_name,
 h.subdivision_num,
 h.subdivision_name,
 p.dept_idnt,
 h.dept_name,
 p.category,
 p.plan_dmd_r,
 p.plan_dmd_u,
 p.plan_net_sls_r,
 p.plan_net_sls_u,
 p.plan_rcpt_r,
 p.plan_rcpt_u,
 p.model_los_ccs,
 p.plan_los_ccs,
 p.model_new_ccs,
 p.plan_new_ccs,
 p.model_cf_ccs,
 p.plan_cf_ccs,
 p.model_sell_offs_ccs,
 p.plan_sell_off_ccs,
 p.ly_sell_off_ccs,
 p.lly_sell_off_ccs,
 `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast (NULL as string)) AS ly_lly_update_timestamp_tz,
 `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast (NULL as string)) AS plan_update_timestamp_tz,
 `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(timestamp(current_datetime('PST8PDT')) as string)) AS cm_data_update_timestamp_tz,
 `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(cast(timestamp(current_datetime('PST8PDT')) as string)) AS update_timestamp_tz
FROM plan_data AS p
 INNER JOIN month_ref AS m ON CAST(p.month_idnt AS FLOAT64) = m.plan_month_idnt
 INNER JOIN hierarchy AS h ON CAST(p.dept_idnt AS FLOAT64) = h.dept_num
WHERE h.division_num IN (310, 340, 345, 351, 360, 365, 700)
 AND p.date_rank = 1;


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.t2dl_das_selection.e2e_selection_planning AS a
USING plan_insert AS b
ON a.month_idnt = CAST(b.month_idnt AS FLOAT64) AND a.fiscal_month_num = b.fiscal_month_num AND a.fiscal_year_num = b.fiscal_year_num
       AND LOWER(a.channel_brand) = LOWER(b.channel_brand) AND a.dept_idnt = CAST(b.dept_idnt AS FLOAT64) AND LOWER(a.category
    ) = LOWER(b.category) AND a.month_start_day_date = b.month_start_day_date
WHEN MATCHED THEN UPDATE SET
 plan_month_idnt = cast(b.month_idnt as int64),
 ly_month_idnt = b.ly_month_idnt,
 lly_month_idnt = b.lly_month_idnt,
 month_label = b.month_label,
 quarter_idnt = b.quarter_idnt,
 quarter_label = b.quarter_label,
 division_num = b.division_num,
 division_name = b.division_name,
 subdivision_num = b.subdivision_num,
 subdivision_name = b.subdivision_name,
 dept_name = b.dept_name,
 plan_dmd_r = b.plan_dmd_r,
 plan_dmd_u = b.plan_dmd_u,
 plan_net_sls_r = b.plan_net_sls_r,
 plan_net_sls_u = b.plan_net_sls_u,
 plan_rcpt_r = b.plan_rcpt_r,
 plan_rcpt_u = b.plan_rcpt_u,
 model_los_ccs = b.model_los_ccs,
 plan_los_ccs = b.plan_los_ccs,
 model_new_ccs = b.model_new_ccs,
 plan_new_ccs = b.plan_new_ccs,
 model_cf_ccs = b.model_cf_ccs,
 plan_cf_ccs = b.plan_cf_ccs,
 model_sell_offs_ccs = b.model_sell_offs_ccs,
 plan_sell_off_ccs = b.plan_sell_off_ccs,
 ly_sell_off_ccs = b.ly_sell_off_ccs,
 lly_sell_off_ccs = b.lly_sell_off_ccs,

 plan_update_timestamp = cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) as timestamp),
 update_timestamp =cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)as timestamp)

WHEN NOT MATCHED THEN
 INSERT 
 VALUES(b.fiscal_month_num, b.fiscal_year_num, CAST(b.month_idnt AS INTEGER), b.month_start_day_date
 , b.plan_month_idnt, b.ly_month_idnt, b.lly_month_idnt, b.month_label, b.quarter_idnt, b.quarter_label, b.channel_brand
 , b.division_num, b.division_name, b.subdivision_num, b.subdivision_name, CAST(b.dept_idnt AS INTEGER), b.dept_name, b
 .category, b.plan_dmd_r, b.plan_dmd_u, b.plan_net_sls_r, b.plan_net_sls_u, b.plan_rcpt_r, b.plan_rcpt_u, b.model_los_ccs
 , b.plan_los_ccs, b.model_new_ccs, b.plan_new_ccs, b.model_cf_ccs, b.plan_cf_ccs, b.model_sell_offs_ccs, b.plan_sell_off_ccs
 , b.ly_sell_off_ccs, b.lly_sell_off_ccs,
   CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS INTEGER), CAST(NULL AS TIMESTAMP), 
 b.ly_lly_update_timestamp_tz,
 NULL,
 b.plan_update_timestamp_tz
 , CAST(NULL AS TIMESTAMP),
 b.cm_data_update_timestamp_tz,
  CAST(NULL AS TIMESTAMP),
   b.update_timestamp_tz);


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.t2dl_das_selection.e2e_selection_planning
WHERE month_idnt NOT IN (SELECT DISTINCT month_idnt
  FROM month_ref);