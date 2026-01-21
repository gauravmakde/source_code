CREATE TEMPORARY TABLE IF NOT EXISTS rp_spend_inc
AS
SELECT
CAST(TRUNC(cast(REPLACE(week_idnt, '"', '') as float64)) AS INTEGER) AS week_idnt,
 CAST(TRUNC(cast(REPLACE(dept, '"', '') as float64)) AS INTEGER) AS dept_idnt,
 SUBSTR(REPLACE(dept_desc, '"', ''), 1, 40)AS dept_desc,
 CAST(TRUNC(cast(REPLACE(cls_idnt, '"', '')as float64) )AS INTEGER) AS cls_idnt,
 CAST(TRUNC(cast(REPLACE(scls_idnt, '"', '') as float64)) AS INTEGER) AS scls_idnt,
 CAST(TRUNC(cast(REPLACE(supp_idnt, '"', '')as float64)) AS INTEGER) AS supp_idnt,
 SUBSTR(REPLACE(supp_name, '"', ''), 1, 40) AS supp_desc,
 CAST(TRUNC(cast(REPLACE(banner_id, '"', '') as float64)) AS INTEGER) AS banner_id,
 CAST(TRUNC(cast(REPLACE(ft_id, '"', '')as float64)) AS INTEGER) AS ft_id,
 ROUND(CAST(REPLACE(rp_antspnd_u, '"', '') AS NUMERIC), 4) AS rp_antspnd_u,
 ROUND(CAST(REPLACE(rp_antspnd_c, '"', '') AS NUMERIC), 4) AS rp_antspnd_c,
 ROUND(CAST(REPLACE(rp_antspnd_r, '"', '') AS NUMERIC), 4) AS rp_antspnd_r,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) AS update_timestamp
FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_adhoc;


CREATE TEMPORARY TABLE IF NOT EXISTS rp_spend
AS
SELECT rp.week_idnt,
 cal.fiscal_year_num AS year_454,
 cal.month_idnt AS month_454,
 rp.banner_id,
 rp.ft_id,
 rp.dept_idnt,
 rp.dept_desc,
 COALESCE(map1.category, map2.category, 'OTHER') AS category,
 rp.supp_idnt,
 rp.supp_desc,
 SUM(rp.rp_antspnd_u) AS rp_antspnd_u,
 SUM(rp.rp_antspnd_c) AS rp_antspnd_c,
 SUM(rp.rp_antspnd_r) AS rp_antspnd_r,
 CURRENT_DATE AS rcd_load_date,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_timestamp,
 `{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() as update_timestamp_tz
FROM rp_spend_inc AS rp

 INNER JOIN (SELECT DISTINCT week_idnt,
   month_idnt,
   fiscal_year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS cal ON rp.week_idnt = cal.week_idnt
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS map1 ON rp.dept_idnt = map1.dept_num AND rp.cls_idnt = CAST(map1.class_num AS FLOAT64)
     AND rp.scls_idnt = CAST(map1.sbclass_num AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS map2 ON rp.dept_idnt 
 = map2.dept_num AND rp.cls_idnt = CAST(map2.class_num AS FLOAT64)
     AND LOWER(map2.sbclass_num) = LOWER('-1')
GROUP BY rp.week_idnt,
 year_454,
 month_454,
 rp.banner_id,
 rp.ft_id,
 rp.dept_idnt,
 rp.dept_desc,
 category,
 rp.supp_idnt,
 rp.supp_desc;


DELETE FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_current
WHERE week_idnt IN (SELECT DISTINCT week_idnt
  FROM rp_spend);


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_current
(SELECT week_idnt,
  year_454,
  month_454,
  banner_id,
  ft_id,
  dept_idnt,
  dept_desc,
  category,
  supp_idnt,
  supp_desc,
  rp_antspnd_u,
  rp_antspnd_c,
  rp_antspnd_r,
  rcd_load_date,
  CAST(update_timestamp AS TIMESTAMP) AS update_timestamp,
  update_timestamp_tz
 FROM rp_spend);


DELETE FROM `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_hist
WHERE week_idnt IN (SELECT DISTINCT week_idnt
   FROM rp_spend) AND rcd_load_date = CURRENT_DATE('PST8PDT');


INSERT INTO `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_hist
(SELECT week_idnt,
  year_454,
  month_454,
  banner_id,
  ft_id,
  dept_idnt,
  dept_desc,
  category,
  supp_idnt,
  supp_desc,
  rp_antspnd_u,
  rp_antspnd_c,
  rp_antspnd_r,
  rcd_load_date,
  CAST(update_timestamp AS TIMESTAMP) AS update_timestamp,
  update_timestamp_tz

 FROM rp_spend);

