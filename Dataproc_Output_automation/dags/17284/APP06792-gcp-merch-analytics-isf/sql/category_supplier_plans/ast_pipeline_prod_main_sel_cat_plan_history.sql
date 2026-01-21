-- 	, row_number() OVER (PARTITION BY plan_fiscal_month, cluster_label, dept_category ORDER BY rcd_load_date DESC) AS date_rank
BEGIN

CREATE TEMPORARY TABLE IF NOT EXISTS plan_data

AS
SELECT cal.month_idnt AS snapshot_plan_month_idnt,
 cal.month_start_day_date AS snapshot_start_day_date,
 CONCAT('20', SUBSTR(p.plan_fiscal_month, 2 * -1), CASE
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('FEB')
   THEN '01'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('MAR')
   THEN '02'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('APR')
   THEN '03'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('MAY')
   THEN '04'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('JUN')
   THEN '05'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('JUL')
   THEN '06'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('AUG')
   THEN '07'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('SEP')
   THEN '08'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('OCT')
   THEN '09'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('NOV')
   THEN '10'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('DEC')
   THEN '11'
   WHEN LOWER(SUBSTR(p.plan_fiscal_month, 0, 3)) = LOWER('JAN')
   THEN '12'
   ELSE NULL
   END) AS month_idnt,
  CASE
  WHEN LOWER(p.cluster_label) LIKE LOWER('N.COM')
  THEN 'NORDSTROM'
  WHEN LOWER(p.cluster_label) LIKE LOWER('RACK.COM')
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
 LEFT(p.dept_category, STRPOS('_', p.dept_category) ) AS dept_idnt,
 SUBSTR(p.dept_category, CAST(LENGTH(p.dept_category) - STRPOS('_', p.dept_category)  AS INTEGER) * -1)
 AS category,
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
 p.model_sell_offs_ccs AS model_sell_off_ccs,
 p.plan_sell_off_ccs,
 p.ly_sell_off_ccs,
 p.lly_sell_off_ccs,
 p.model_sell_off_ccs_rate,
 p.plan_sell_off_ccs_rate,
 p.ly_sell_off_ccs_rate,
 p.lly_sell_off_ccs_rate,
 p.rcd_load_date
FROM `{{params.gcp_project_id}}`.t3dl_nap_planning.sel_dept_category_plan AS p
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS cal ON p.rcd_load_date = cal.day_date
QUALIFY (ROW_NUMBER() OVER (PARTITION BY p.plan_fiscal_month, p.cluster_label, p.dept_category ORDER BY p.rcd_load_date
     DESC)) = 1;


-- plan


-- plan 


-- update info


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sel_dept_category_plan_history AS a
USING plan_data AS b
ON a.month_idnt = CAST(b.month_idnt AS FLOAT64) AND a.snapshot_plan_month_idnt = b.snapshot_plan_month_idnt AND LOWER(a
      .channel_brand) = LOWER(b.channel_brand) AND a.dept_idnt = CAST(b.dept_idnt AS FLOAT64) AND LOWER(a.category) =
   LOWER(b.category) AND a.snapshot_start_day_date = b.snapshot_start_day_date
WHEN MATCHED THEN UPDATE SET
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
 model_sell_off_ccs = b.model_sell_off_ccs,
 plan_sell_off_ccs = b.plan_sell_off_ccs,
 ly_sell_off_ccs = b.ly_sell_off_ccs,
 lly_sell_off_ccs = b.lly_sell_off_ccs,
 model_sell_off_ccs_rate = b.model_sell_off_ccs_rate,
 plan_sell_off_ccs_rate = b.plan_sell_off_ccs_rate,
 ly_sell_off_ccs_rate = b.ly_sell_off_ccs_rate,
 lly_sell_off_ccs_rate = b.lly_sell_off_ccs_rate,
 rcd_load_date = b.rcd_load_date,
 update_timestamp = timestamp(current_datetime('PST8PDT'))
WHEN NOT MATCHED THEN INSERT VALUES(b.snapshot_plan_month_idnt, b.snapshot_start_day_date, CAST(trunc(CAST(b.month_idnt as FLOAT64)) AS INTEGER)
 , b.channel_brand, CAST(trunc(CAST(b.dept_idnt as FLOAT64)) AS INTEGER), b.category, b.plan_dmd_r, b.plan_dmd_u, b.plan_net_sls_r, b.plan_net_sls_u
 , b.plan_rcpt_r, b.plan_rcpt_u, b.model_los_ccs, b.plan_los_ccs, b.model_new_ccs, b.plan_new_ccs, b.model_cf_ccs, b.plan_cf_ccs
 , b.model_sell_off_ccs, b.plan_sell_off_ccs, b.ly_sell_off_ccs, b.lly_sell_off_ccs, b.model_sell_off_ccs_rate, b.plan_sell_off_ccs_rate
 , b.ly_sell_off_ccs_rate, b.lly_sell_off_ccs_rate, b.rcd_load_date, timestamp(current_datetime('PST8PDT')), `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 );
	
 END