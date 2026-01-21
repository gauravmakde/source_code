/*
Plan & LY/LLY Data Script
Author: Sara Riker & David Selover
8/15/22: Create script
Updates Weekly on Sundays

Updates Tables with historicals:
    T2DL_DAS_SELECTION.e2e_selection_planning 
*/
 



CREATE TEMPORARY TABLE IF NOT EXISTS hierarchy
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


-- DROP TABLE month_ref_plan;


-- edited to reference LLY to 2021 instead of 2019 


CREATE TEMPORARY TABLE IF NOT EXISTS month_ref
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
   WHERE day_date = current_date('PST8PDT')) AND (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = DATE_ADD(current_date('PST8PDT'), INTERVAL 548 DAY));


--COLLECT STATS      PRIMARY INDEX(month_idnt)      ,COLUMN (quarter_idnt)      ,COLUMN (fiscal_year_num)      ON month_ref


CREATE TEMPORARY TABLE IF NOT EXISTS ly_data
AS
SELECT mrly.ly_month_idnt,
 mrly.plan_month_idnt,
 mrly.lly_month_idnt,
 lyd.channel_brand,
 lyd.dept_idnt,
 lyd.category,
 lyd.demand_dollars,
 lyd.demand_units,
 lyd.sales_dollars,
 lyd.sales_units,
 lyd.rcpt_units,
 lyd.rcpt_dollars,
 lyd.ccs_daily_avg_los,
 lyd.ccs_mnth_new,
 lyd.ccs_mnth_cf
FROM `{{params.gcp_project_id}}`.t2dl_das_selection.selection_planning_monthly AS lyd
 INNER JOIN month_ref AS mrly ON lyd.mnth_idnt = mrly.ly_month_idnt
WHERE LOWER(lyd.channel_country) IN (LOWER('US'));


--COLLECT STATS      PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)      ,COLUMN (plan_month_idnt)      ON LY_data


-- TEMP table for LLY DATA


-- DROP TABLE LLY_data;


CREATE TEMPORARY TABLE IF NOT EXISTS lly_data
AS
SELECT mr.ly_month_idnt,
 mr.plan_month_idnt,
 mr.lly_month_idnt,
 llyd.channel_brand,
 llyd.dept_idnt,
 llyd.category,
 llyd.demand_dollars,
 llyd.demand_units,
 llyd.sales_dollars,
 llyd.sales_units,
 llyd.rcpt_units,
 llyd.rcpt_dollars,
 llyd.ccs_daily_avg_los,
 llyd.ccs_mnth_new,
 llyd.ccs_mnth_cf
FROM `{{params.gcp_project_id}}`.t2dl_das_selection.selection_planning_monthly AS llyd
 INNER JOIN month_ref AS mr ON llyd.mnth_idnt = mr.lly_month_idnt
WHERE LOWER(llyd.channel_country) IN (LOWER('US'));


--COLLECT STATS      PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)      ,COLUMN (plan_month_idnt)      ON LLY_data


-- Final Insert 


-- DROP TABLE ly_lly_insert;


CREATE TEMPORARY TABLE IF NOT EXISTS ly_lly_insert
AS
SELECT p.month_idnt,
 p.month_label,
 p.quarter_idnt,
 p.quarter_label,
 p.fiscal_year_num,
 p.fiscal_month_num,
 p.month_start_day_date,
 p.plan_month_idnt,
 p.ly_month_idnt,
 p.lly_month_idnt,
 lyd.channel_brand,
 h.division_num,
 h.division_name,
 h.subdivision_num,
 h.subdivision_name,
 lyd.dept_idnt,
 h.dept_name,
 lyd.category,
 lyd.demand_dollars AS `ly_demand_dollar`,
 lyd.demand_units AS ly_demand_u,
 lyd.sales_dollars AS `ly_sales_dollar`,
 lyd.sales_units AS ly_sales_u,
 lyd.rcpt_units AS ly_rcpt_u,
 lyd.rcpt_dollars AS `ly_rcpt_dollar`,
 lyd.ccs_daily_avg_los AS ly_los_ccs,
 lyd.ccs_mnth_new AS ly_new_ccs,
 lyd.ccs_mnth_cf AS ly_cf_ccs,
 llyd.demand_dollars AS `lly_demand_dollar`,
 llyd.demand_units AS lly_demand_u,
 llyd.sales_dollars AS `lly_sales_dollar`,
 llyd.sales_units AS lly_sales_u,
 llyd.rcpt_dollars AS `lly_rcpt_dollar`,
 llyd.rcpt_units AS lly_rcpt_u,
 llyd.ccs_daily_avg_los AS lly_los_ccs,
 llyd.ccs_mnth_new AS lly_new_ccs,
 llyd.ccs_mnth_cf AS lly_cf_ccs
FROM ly_data AS lyd
 LEFT JOIN lly_data AS llyd ON lyd.dept_idnt = llyd.dept_idnt AND LOWER(lyd.category) = LOWER(llyd.category) AND LOWER(lyd
     .channel_brand) = LOWER(llyd.channel_brand) AND lyd.plan_month_idnt = llyd.plan_month_idnt
 INNER JOIN month_ref AS p ON lyd.plan_month_idnt = p.plan_month_idnt
 INNER JOIN hierarchy AS h ON lyd.dept_idnt = h.dept_num
WHERE h.division_num IN (310, 340, 345, 351, 360, 365, 700);


-- Insert LY and LLY data


--{env_suffix}


-- ly/lly


-- plan 


-- ly/lly


-- cm data


-- update info


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.e2e_selection_planning{{params.env_suffix}} AS a
USING ly_lly_insert AS b
ON a.month_idnt = b.month_idnt AND a.fiscal_month_num = b.fiscal_month_num AND a.fiscal_year_num = b.fiscal_year_num AND
     LOWER(a.channel_brand) = LOWER(b.channel_brand) AND a.dept_idnt = b.dept_idnt AND LOWER(a.category) = LOWER(b.category
    ) AND a.month_start_day_date = b.month_start_day_date
WHEN MATCHED THEN UPDATE SET
 plan_month_idnt = b.month_idnt,
 ly_month_idnt = b.ly_month_idnt,
 lly_month_idnt = b.lly_month_idnt,
 month_label = b.month_label,
 quarter_idnt = b.quarter_idnt,
 quarter_label = b.quarter_label,
 division_num = b.division_num,
 division_name = b.division_name,
 subdivision_num = b.subdivision_num,
 dept_name = b.dept_name,
 `ly_demand_dollar` = b.`ly_demand_dollar`,
 ly_demand_u = b.ly_demand_u,
 `ly_sales_dollar` = b.`ly_sales_dollar`,
 ly_sales_u = b.ly_sales_u,
 ly_rcpt_u = b.ly_rcpt_u,
 `ly_rcpt_dollar` = b.`ly_rcpt_dollar`,
 ly_los_ccs = b.ly_los_ccs,
 ly_new_ccs = b.ly_new_ccs,
 ly_cf_ccs = b.ly_cf_ccs,
 `lly_demand_dollar` = b.`lly_demand_dollar`,
 lly_demand_u = b.lly_demand_u,
 `lly_sales_dollar` = b.`lly_sales_dollar`,
 lly_sales_u = b.lly_sales_u,
 `lly_rcpt_dollar` = b.`lly_rcpt_dollar`,
 lly_rcpt_u = b.lly_rcpt_u,
 lly_los_ccs = b.lly_los_ccs,
 lly_new_ccs = b.lly_new_ccs,
 lly_cf_ccs = b.lly_cf_ccs,
 ly_lly_update_timestamp = cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) as timestamp),
 update_timestamp = cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) as timestamp)
WHEN NOT MATCHED THEN 
INSERT VALUES(b.fiscal_month_num, b.fiscal_year_num, b.month_idnt, b.month_start_day_date, b.plan_month_idnt
 , b.ly_month_idnt, b.lly_month_idnt, b.month_label, b.quarter_idnt, b.quarter_label, b.channel_brand, b.division_num, b
 .division_name, b.subdivision_num, b.subdivision_name, b.dept_idnt, b.dept_name, b.category, CAST(NULL AS BIGNUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS BIGNUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS BIGNUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), b.`ly_demand_dollar`, b.ly_demand_u, b.`ly_sales_dollar`,
 b.ly_sales_u, b.`ly_rcpt_dollar`, b.ly_rcpt_u, b.ly_los_ccs, b.ly_new_ccs, b.ly_cf_ccs, b.`lly_demand_dollar`, b.lly_demand_u, b
 .`lly_sales_dollar`, b.lly_sales_u, b.lly_rcpt_u, b.`lly_rcpt_dollar`, b.lly_los_ccs, b.lly_new_ccs, b.lly_cf_ccs, CAST(NULL AS INTEGER)
 , cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) as timestamp)
 ,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
  ,CAST(NULL AS TIMESTAMP)
 ,CAST(NULL AS STRING)
 , CAST(NULL AS TIMESTAMP)
 ,CAST(NULL AS STRING)
  ,cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) as timestamp)
   ,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 );


-- DROP TABLE month_ref_plan;


CREATE TEMPORARY TABLE IF NOT EXISTS month_18_ref
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
   WHERE day_date = DATE_ADD(current_date('PST8PDT'), INTERVAL 365 DAY)) AND (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = DATE_ADD(current_date('PST8PDT'), INTERVAL 548 DAY));


--COLLECT STATS      PRIMARY INDEX(month_idnt)      ,COLUMN (quarter_idnt)      ,COLUMN (fiscal_year_num)      ON month_18_ref


CREATE TEMPORARY TABLE IF NOT EXISTS plan_data
AS
SELECT CONCAT('20', SUBSTR(plan_fiscal_month, 2 * -1), CASE
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('FEB')
   THEN '01'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('MAR')
   THEN '02'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('APR')
   THEN '03'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('MAY')
   THEN '04'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('JUN')
   THEN '05'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('JUL')
   THEN '06'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('AUG')
   THEN '07'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('SEP')
   THEN '08'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('OCT')
   THEN '09'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('NOV')
   THEN '10'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('DEC')
   THEN '11'
   WHEN LOWER(SUBSTR(plan_fiscal_month, 0, 3)) = LOWER('JAN')
   THEN '12'
   ELSE NULL
   END) AS month_idnt,
  CASE
  WHEN LOWER(cluster_label) LIKE LOWER('N.COM')
  THEN 'NORDSTROM'
  WHEN LOWER(cluster_label) LIKE LOWER('RACK.COM')
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,

  SUBSTR(dept_category, 1, STRPOS(dept_category, '_') - 1) AS dept_idnt,
  SUBSTR(dept_category, STRPOS(dept_category, '_') + 1) AS category,
--  LEFT(dept_category, CAST(LOCATE('_', dept_category) AS FLOAT64) - 1) AS dept_idnt,
--  RIGHT(SUBSTR(dept_category, CAST(LENGTH(dept_category) - CAST(LOCATE('_', dept_category) AS FLOAT64) AS INTEGER)) * -1) AS

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
 ROW_NUMBER() OVER (PARTITION BY plan_fiscal_month, cluster_label, dept_category ORDER BY rcd_load_date DESC) AS
 date_rank
FROM `{{params.gcp_project_id}}`.t3dl_nap_planning.sel_dept_category_plan
WHERE LOWER(dept_category) NOT LIKE LOWER('%TOTAL%');


--COLLECT STATS      PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)      ,COLUMN (month_idnt)      ON plan_data


CREATE TEMPORARY TABLE IF NOT EXISTS ly_18_data
AS
SELECT mrly.ly_month_idnt,
 mrly.plan_month_idnt,
 mrly.lly_month_idnt,
 lyd.channel_brand,
 lyd.dept_idnt,
 lyd.category,
 lyd.plan_dmd_r,
 lyd.plan_dmd_u,
 lyd.plan_net_sls_r,
 lyd.plan_net_sls_u,
 lyd.plan_rcpt_r,
 lyd.plan_rcpt_u,
 lyd.plan_los_ccs,
 lyd.plan_new_ccs,
 lyd.plan_cf_ccs
FROM plan_data AS lyd
 INNER JOIN month_18_ref AS mrly ON CAST(lyd.month_idnt AS FLOAT64) = mrly.ly_month_idnt;


--COLLECT STATS      PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)      ,COLUMN (plan_month_idnt)      ON LY_18_data


-- TEMP table for LLY DATA


-- DROP TABLE LLY_data;


CREATE TEMPORARY TABLE IF NOT EXISTS lly_18_data
AS
SELECT mr.ly_month_idnt,
 mr.plan_month_idnt,
 mr.lly_month_idnt,
 llyd.channel_brand,
 llyd.dept_idnt,
 llyd.category,
 llyd.demand_dollars,
 llyd.demand_units,
 llyd.sales_dollars,
 llyd.sales_units,
 llyd.rcpt_dollars,
 llyd.rcpt_units,
 llyd.ccs_daily_avg_los,
 llyd.ccs_mnth_new,
 llyd.ccs_mnth_cf
FROM `{{params.gcp_project_id}}`.t2dl_das_selection.selection_planning_monthly AS llyd
 INNER JOIN month_18_ref AS mr ON llyd.mnth_idnt = mr.lly_month_idnt
WHERE LOWER(llyd.channel_country) IN (LOWER('US'));


--COLLECT STATS      PRIMARY INDEX(channel_brand, DEPT_IDNT, CATEGORY)      ,COLUMN (plan_month_idnt)      ON lly_18_data


-- Final Insert 


-- DROP TABLE ly_lly_insert;


CREATE TEMPORARY TABLE IF NOT EXISTS ly_lly_18_insert
AS
SELECT p.month_idnt,
 p.month_label,
 p.quarter_idnt,
 p.quarter_label,
 p.fiscal_year_num,
 p.fiscal_month_num,
 p.month_start_day_date,
 p.plan_month_idnt,
 p.ly_month_idnt,
 p.lly_month_idnt,
 lyd.channel_brand,
 h.division_num,
 h.division_name,
 h.subdivision_num,
 h.subdivision_name,
 lyd.dept_idnt,
 h.dept_name,
 lyd.category,
 lyd.plan_dmd_r AS `ly_demand_dollar`,
 lyd.plan_dmd_u AS ly_demand_u,
 lyd.plan_net_sls_r AS `ly_sales_dollar`,
 lyd.plan_net_sls_u AS ly_sales_u,
 lyd.plan_rcpt_r AS `ly_rcpt_dollar`,
 lyd.plan_rcpt_u AS ly_rcpt_u,
 lyd.plan_los_ccs AS ly_los_ccs,
 lyd.plan_new_ccs AS ly_new_ccs,
 lyd.plan_cf_ccs AS ly_cf_ccs,
 llyd.demand_dollars AS `lly_demand_dollar`,
 llyd.demand_units AS lly_demand_u,
 llyd.sales_dollars AS `lly_sales_dollar`,
 llyd.sales_units AS lly_sales_u,
 llyd.rcpt_units AS lly_rcpt_u,
 llyd.rcpt_dollars AS `lly_rcpt_dollar`,
 llyd.ccs_daily_avg_los AS lly_los_ccs,
 llyd.ccs_mnth_new AS lly_new_ccs,
 llyd.ccs_mnth_cf AS lly_cf_ccs
FROM ly_18_data AS lyd
 LEFT JOIN lly_18_data AS llyd ON CAST(lyd.dept_idnt AS FLOAT64) = llyd.dept_idnt AND LOWER(lyd.category) = LOWER(llyd.category
      ) AND LOWER(lyd.channel_brand) = LOWER(llyd.channel_brand) AND lyd.plan_month_idnt = llyd.plan_month_idnt
 INNER JOIN month_18_ref AS p ON lyd.plan_month_idnt = p.plan_month_idnt
 INNER JOIN hierarchy AS h ON CAST(lyd.dept_idnt AS FLOAT64) = h.dept_num
WHERE h.division_num IN (310, 340, 345, 351, 360, 365, 700);


-- plan/ly/lly insert


--{env_suffix}


-- ly/lly


-- plan 


-- ly/lly


-- cm data


-- update info


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.e2e_selection_planning{{params.env_suffix}} AS a
USING ly_lly_18_insert AS b
ON a.month_idnt = b.month_idnt AND a.fiscal_month_num = b.fiscal_month_num AND a.fiscal_year_num = b.fiscal_year_num AND
     LOWER(a.channel_brand) = LOWER(b.channel_brand) AND a.dept_idnt = CAST(b.dept_idnt AS FLOAT64) AND LOWER(a.category
    ) = LOWER(b.category) AND a.month_start_day_date = b.month_start_day_date
WHEN MATCHED THEN UPDATE SET
 plan_month_idnt = b.month_idnt,
 ly_month_idnt = b.ly_month_idnt,
 lly_month_idnt = b.lly_month_idnt,
 month_label = b.month_label,
 quarter_idnt = b.quarter_idnt,
 quarter_label = b.quarter_label,
 division_num = b.division_num,
 division_name = b.division_name,
 subdivision_num = b.subdivision_num,
 dept_name = b.dept_name,
 `ly_demand_dollar` = b.`ly_demand_dollar`,
 ly_demand_u = b.ly_demand_u,
 `ly_sales_dollar` = b.`ly_sales_dollar`,
 ly_sales_u = b.ly_sales_u,
 ly_rcpt_u = b.ly_rcpt_u,
 `ly_rcpt_dollar` = b.`ly_rcpt_dollar`,
 ly_los_ccs = b.ly_los_ccs,
 ly_new_ccs = b.ly_new_ccs,
 ly_cf_ccs = b.ly_cf_ccs,
 `lly_demand_dollar` = b.`lly_demand_dollar`,
 lly_demand_u = b.lly_demand_u,
 `lly_sales_dollar` = b.`lly_sales_dollar`,
 lly_sales_u = b.lly_sales_u,
 `lly_rcpt_dollar` = b.`lly_rcpt_dollar`,
 lly_rcpt_u = b.lly_rcpt_u,
 lly_los_ccs = b.lly_los_ccs,
 lly_new_ccs = b.lly_new_ccs,
 lly_cf_ccs = b.lly_cf_ccs,
 ly_lly_update_timestamp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 update_timestamp =CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP)
WHEN NOT MATCHED THEN 

INSERT VALUES
(b.fiscal_month_num, b.fiscal_year_num, b.month_idnt, b.month_start_day_date, b.plan_month_idnt
 , b.ly_month_idnt, b.lly_month_idnt, b.month_label, b.quarter_idnt, b.quarter_label, b.channel_brand, b.division_num, b
 .division_name, b.subdivision_num, b.subdivision_name, CAST(TRUNC(CAST(b.dept_idnt AS FLOAT64)) AS INTEGER), b.dept_name, b.category, CAST(NULL AS BIGNUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS BIGNUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS BIGNUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), b.`ly_demand_dollar`, b.ly_demand_u
 , b.`ly_sales_dollar`, b.ly_sales_u, b.`ly_rcpt_dollar`, b.ly_rcpt_u, b.ly_los_ccs, b.ly_new_ccs, b.ly_cf_ccs, b.`lly_demand_dollar`,
 b.lly_demand_u, b.`lly_sales_dollar`, b.lly_sales_u, b.lly_rcpt_u, b.`lly_rcpt_dollar`, b.lly_los_ccs, b.lly_new_ccs, b.lly_cf_ccs
 , CAST(NULL AS INTEGER), 
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP)
  ,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
  ,CAST(NULL AS TIMESTAMP)
,CAST(NULL AS STRING)
 , CAST(NULL AS TIMESTAMP)
,CAST(NULL AS STRING)
 ,CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP)
  ,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 );