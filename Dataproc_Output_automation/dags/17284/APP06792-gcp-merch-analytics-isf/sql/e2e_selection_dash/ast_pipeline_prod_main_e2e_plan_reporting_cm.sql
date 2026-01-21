/*
Plan & LY/LLY Data DDL
Author: Sara Riker & David Selover
8/15/22: Create script

Updates Tables with CM:
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
   WHERE day_date = CURRENT_DATE('PST8PDT')) AND (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 548 DAY));


--COLLECT STATS      PRIMARY INDEX(month_idnt)      ,COLUMN (quarter_idnt)      ,COLUMN (fiscal_year_num)      ON month_ref


CREATE TEMPORARY TABLE IF NOT EXISTS cm_data
AS
SELECT CASE
  WHEN c.org_id = 120
  THEN 'NORDSTROM'
  WHEN c.org_id = 250
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
 c.fiscal_month_id AS month_idnt,
 c.dept_id AS dept_idnt,
 COALESCE(map1.category, map2.category, 'OTHER') AS category,
 COUNT(DISTINCT FORMAT('%11d', c.dept_id) || '_' || c.supp_id || '_' || c.vpn || '_' || COALESCE(TRIM(FORMAT('%11d', c.nrf_color_code
      )), 'UNNOWN')) AS new_ccs_cm
FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.ab_cm_orders_current AS c
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS map1 ON c.dept_id = map1.dept_num AND c.class_id = CAST(map1.class_num AS FLOAT64)
     AND c.subclass_id = CAST(map1.sbclass_num AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS map2 ON c.dept_id = map2.dept_num AND c.class_id = CAST(map2.class_num AS FLOAT64)
     AND LOWER(map2.sbclass_num) = LOWER('-1')
WHERE c.org_id IN (120, 250)
 AND LOWER(c.plan_type) = LOWER('FASHION')
GROUP BY channel_brand,
 month_idnt,
 dept_idnt,
 category;


CREATE TEMPORARY TABLE IF NOT EXISTS cm_insert
AS
SELECT c.month_idnt,
 m.month_start_day_date,
 m.fiscal_month_num,
 m.fiscal_year_num,
 m.plan_month_idnt,
 m.ly_month_idnt,
 m.lly_month_idnt,
 m.month_label,
 m.quarter_idnt,
 m.quarter_label,
 c.channel_brand,
 h.division_num,
 h.division_name,
 h.subdivision_num,
 h.subdivision_name,
 c.dept_idnt,
 h.dept_name,
 c.category,
 c.new_ccs_cm
FROM cm_data AS c
 INNER JOIN month_ref AS m ON CAST(c.month_idnt AS FLOAT64) = m.month_idnt
 INNER JOIN hierarchy AS h ON c.dept_idnt = h.dept_num
WHERE h.division_num IN (310, 340, 345, 351, 360, 365, 700);


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.e2e_selection_planning AS a
USING cm_insert AS b
ON a.month_idnt = CAST(b.month_idnt AS FLOAT64) AND a.fiscal_month_num = b.fiscal_month_num AND a.fiscal_year_num = b.fiscal_year_num
       AND LOWER(a.channel_brand) = LOWER(b.channel_brand) AND a.dept_idnt = b.dept_idnt AND LOWER(a.category) = LOWER(b
    .category) AND a.month_start_day_date = b.month_start_day_date
WHEN MATCHED THEN UPDATE SET
 plan_month_idnt = cast(TRUNC(CAST(b.month_idnt AS FLOAT64)) as int64),
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
 new_ccs_cm = b.new_ccs_cm,
 cm_data_update_timestamp = cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S',current_datetime('PST8PDT')) AS DATETIME) as timestamp),
 update_timestamp =cast( CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S',current_datetime('PST8PDT')) AS DATETIME) as timestamp),update_timestamp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN 

INSERT VALUES(b.fiscal_month_num, b.fiscal_year_num, CAST(TRUNC(CAST(b.month_idnt AS FLOAT64)) AS INTEGER), b.month_start_day_date
 , b.plan_month_idnt, b.ly_month_idnt, b.lly_month_idnt, b.month_label, b.quarter_idnt, b.quarter_label, b.channel_brand
 , b.division_num, b.division_name, b.subdivision_num, b.subdivision_name, b.dept_idnt, b.dept_name, b.category, CAST(NULL AS BIGNUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS BIGNUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS BIGNUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC)
 , CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), CAST(NULL AS NUMERIC), b.new_ccs_cm, 
  CAST(NULL AS TIMESTAMP)
,CAST(NULL AS string)
 , CAST(NULL AS TIMESTAMP)
 ,CAST(NULL AS string)
  ,CURRENT_TIMESTAMP
,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 , CURRENT_TIMESTAMP
,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST());
 