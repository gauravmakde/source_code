CREATE TEMPORARY TABLE IF NOT EXISTS dates AS
SELECT
  a.day_date,
  a.ty_ly_lly_ind,
  a.week_end_day_date,
  a.week_idnt,
  a.week_454_label,
  a.fiscal_week_num,
  a.month_idnt,
  a.month_454_label,
  a.quarter_label,
  a.half_label,
  a.fiscal_year_num,
  b.week_idnt AS week_idnt_true,
  CASE
    WHEN a.week_end_day_date = a.month_end_day_date THEN 1
    ELSE 0
END
  AS last_week_of_month_ind,
  CASE
    WHEN a.month_end_day_date < CURRENT_DATE('PST8PDT') THEN 1
    ELSE 0
END
  AS completed_month_ind,
  CASE
    WHEN (MAX(a.week_idnt) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = a.week_idnt THEN 1
    ELSE 0
END
  AS max_week_ind
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS a
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b
ON
  a.day_date = b.day_date
WHERE
  LOWER(a.ty_ly_lly_ind) IN (LOWER('TY'),
    LOWER('LY'))
  AND a.week_end_day_date < CURRENT_DATE('PST8PDT');
  
CREATE TEMPORARY TABLE IF NOT EXISTS ua
 AS
SELECT
  c.week_end_day_date,
  CAST(a.location_num AS INTEGER) AS location_num,
  b.channel_num,
  a.general_ledger_reference_number AS gl_ref_no,
  e.shrink_category,
  e.reason_description,
  a.department_num,
  a.class_num,
  a.subclass_num,
  UPPER(d.brand_name) AS brand_name,
  UPPER(f.vendor_name) AS supplier,
  SUM(a.total_cost_amount) AS shrink_cost,
  SUM(a.total_retail_amount) AS shrink_retail,
  SUM(a.quantity) AS shrink_units
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_expense_transfer_ledger_fact AS a
INNER JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS b
ON
  a.location_num = b.store_num
  AND LOWER(b.store_country_code) = LOWER('US' )
INNER JOIN
  dates AS c
ON
  a.transaction_date = c.day_date
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d
ON
  LOWER(a.rms_sku_num) = LOWER(d.rms_sku_num)
  AND LOWER(d.channel_country ) = LOWER('US')
INNER JOIN
  `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.shrink_gl_mapping AS e
ON
  CAST(a.general_ledger_reference_number AS FLOAT64) = e.reason_code
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS f
ON
  LOWER(d.prmy_supp_num) = LOWER(f.vendor_num)
WHERE
  (a.location_num <> 808
    OR CAST(a.general_ledger_reference_number AS FLOAT64) <> 376
    OR a.transaction_date < DATE '2024-05-05')
  AND (a.location_num <> 828
    OR CAST(a.general_ledger_reference_number AS FLOAT64) <> 248
    OR a.transaction_date < DATE '2024-05-05')
  AND a.department_num NOT IN (
  SELECT
    DISTINCT dept_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  WHERE
    div_num = 600)
  AND LOWER(b.store_short_name) NOT LIKE LOWER('%CLSD%')
  AND b.channel_num NOT IN (930,
    240)
GROUP BY
  c.week_end_day_date,
  location_num,
  b.channel_num,
  gl_ref_no,
  e.shrink_category,
  e.reason_description,
  a.department_num,
  a.class_num,
  a.subclass_num,
  brand_name,
  supplier;
  
CREATE TEMPORARY TABLE IF NOT EXISTS sl
 AS
SELECT
  a.end_of_week_date AS week_end_day_date,
  CAST(a.location_num AS INTEGER) AS location_num,
  b.channel_num,
  SUBSTR(NULL, 1, 50) AS gl_ref_no,
  CASE
    WHEN (b.channel_num IN (110, 210) OR CAST(a.location_num AS INTEGER) IN (82, 592)) AND CAST(a.location_num AS INTEGER) NOT IN (209, 297, 697) THEN 'PI/Estimated'
    WHEN b.channel_num IN (120,
    220,
    250,
    260,
    310,
    920,
    930)
  OR CAST(a.location_num AS INTEGER) IN (209,
    297,
    697) THEN 'Additional Shrink'
    ELSE NULL
END
  AS shrink_category,
  SUBSTR(NULL, 1, 150) AS reason_description,
  a.product_hierarchy_dept_num AS department_num,
  a.product_hierarchy_class_num AS class_num,
  a.product_hierarchy_subclass_num AS subclass_num,
  SUBSTR(NULL, 1, 150) AS brand_name,
  SUBSTR(NULL, 1, 150) AS supplier,
  SUM(a.total_shrinkage_cost * - 1) AS shrink_cost,
  0 AS shrink_retail,
  0 AS shrink_units
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_stockledger_week_fact AS a
INNER JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS b
ON
  CAST(a.location_num AS FLOAT64) = b.store_num
  AND LOWER(b.store_country_code ) = LOWER('US')
INNER JOIN
  dates AS c
ON
  a.end_of_week_date = c.day_date
  AND c.day_date = c.week_end_day_date
WHERE
  b.channel_num IN (110,
    120,
    210,
    220,
    250,
    260,
    310,
    920,
    930)
  AND a.total_shrinkage_cost <> 0
  AND a.product_hierarchy_dept_num NOT IN (
  SELECT
    DISTINCT dept_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  WHERE
    div_num = 600)
  AND LOWER(b.store_short_name) NOT LIKE LOWER('%CLSD%')
GROUP BY
  week_end_day_date,
  location_num,
  b.channel_num,
  gl_ref_no,
  shrink_category,
  department_num,
  class_num,
  subclass_num,
  reason_description,
  brand_name,
  supplier; 
  
CREATE TEMPORARY TABLE IF NOT EXISTS tc22
 AS
SELECT
  c.week_end_day_date,
  CAST(a.store_num AS INTEGER) AS location_num,
  b.channel_num,
  a.general_reference_number AS gl_ref_no,
  'Additional Shrink' AS shrink_category,
  e.reason_description,
  a.dept_num AS department_num,
  a.class_num,
  a.subclass_num,
  UPPER(d.brand_name) AS brand_name,
  UPPER(f.vendor_name) AS supplier,
  SUM(a.total_cost_amt) AS shrink_cost,
  SUM(a.total_retail_amt) AS shrink_retail,
  SUM(a.total_units) AS shrink_units
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_shrink_tc22_sku_store_day_fact AS a
INNER JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS b
ON
  a.store_num = b.store_num
  AND LOWER(b.store_country_code) = LOWER('US')
INNER JOIN
  dates AS c
ON
  a.posting_date = c.day_date
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS d
ON
  LOWER(a.rms_sku_num) = LOWER(d.rms_sku_num)
  AND LOWER(d.channel_country ) = LOWER('US')
LEFT JOIN
  t2dl_das_in_season_management_reporting.shrink_gl_mapping AS e
ON
  CAST(a.general_reference_number AS FLOAT64) = e.reason_code
LEFT JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS f
ON
  LOWER(d.prmy_supp_num) = LOWER(f.vendor_num)
WHERE
  (b.channel_num IN (120,
      220,
      250,
      260,
      310,
      920,
      930)
    OR a.store_num IN (209,
      297,
      697))
  AND a.dept_num NOT IN (
  SELECT
    DISTINCT dept_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  WHERE
    div_num = 600)
  AND LOWER(b.store_short_name) NOT LIKE LOWER('%CLSD%')
GROUP BY
  c.week_end_day_date,
  location_num,
  b.channel_num,
  gl_ref_no,
  shrink_category,
  e.reason_description,
  department_num,
  a.class_num,
  a.subclass_num,
  brand_name,
  supplier; 
  
CREATE TEMPORARY TABLE IF NOT EXISTS eom_additional
 AS
SELECT
  c.week_end_day_date,
  a.location_num,
  d.channel_num,
  SUBSTR(NULL, 1, 50) AS gl_ref_no,
  'Additional Shrink' AS shrink_category,
  SUBSTR(NULL, 1, 150) AS reason_description,
  a.department_num,
  a.class_num,
  a.subclass_num,
  SUBSTR(NULL, 1, 150) AS brand_name,
  SUBSTR(NULL, 1, 150) AS supplier,
  a.shrink_c - COALESCE(b.shrink_c, 0) AS shrink_cost,
  0 AS shrink_retail,
  0 AS shrink_units
FROM (
  SELECT
    c.month_idnt,
    a.location_num,
    a.department_num,
    a.class_num,
    a.subclass_num,
    SUM(a.shrink_cost) AS shrink_c
  FROM
    sl AS a
  INNER JOIN
    dates AS c
  ON
    a.week_end_day_date = c.day_date
    AND c.day_date = c.week_end_day_date
  WHERE
    LOWER(a.shrink_category) = LOWER('Additional Shrink')
    AND NOT a.shrink_cost BETWEEN - 0.01
    AND 0.01
  GROUP BY
    a.location_num,
    a.department_num,
    a.class_num,
    a.subclass_num,
    c.month_idnt) AS a
LEFT JOIN (
  SELECT
    c0.month_idnt,
    a0.location_num,
    a0.department_num,
    a0.class_num,
    a0.subclass_num,
    SUM(a0.shrink_cost) AS shrink_c
  FROM
    tc22 AS a0
  INNER JOIN
    dates AS c0
  ON
    a0.week_end_day_date = c0.day_date
    AND c0.day_date = c0.week_end_day_date
  WHERE
    LOWER(a0.shrink_category) = LOWER('Additional Shrink')
  GROUP BY
    a0.location_num,
    a0.department_num,
    a0.class_num,
    a0.subclass_num,
    c0.month_idnt) AS b
ON
  a.month_idnt = b.month_idnt
  AND a.location_num = b.location_num
  AND a.department_num = b.department_num
  AND a.class_num = b.class_num
  AND a.subclass_num = b.subclass_num
INNER JOIN (
  SELECT
    DISTINCT week_end_day_date,
    month_idnt,
    fiscal_year_num
  FROM
    dates
  WHERE
    last_week_of_month_ind = 1
    AND completed_month_ind = 1) AS c
ON
  a.month_idnt = c.month_idnt
INNER JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS d
ON
  a.location_num = d.store_num
  AND LOWER(d.store_country_code) = LOWER('US' )
WHERE
  c.fiscal_year_num > 2023
  AND NOT ROUND(CAST(a.shrink_c - COALESCE(b.shrink_c, 0) AS NUMERIC), 2) BETWEEN - 1
  AND 1; 
  
CREATE TEMPORARY TABLE IF NOT EXISTS addtl
 AS
SELECT
  *
FROM
  sl
WHERE
  LOWER(shrink_category) = LOWER('Additional Shrink')
  AND week_end_day_date <= DATE '2024-02-04'
UNION ALL
SELECT
  *
FROM
  eom_additional
UNION ALL
SELECT
  a.week_end_day_date,
  a.location_num,
  a.channel_num,
  a.gl_ref_no,
  a.shrink_category,
  a.reason_description,
  a.department_num,
  a.class_num,
  a.subclass_num,
  a.brand_name,
  a.supplier,
  a.shrink_cost,
  a.shrink_retail,
  a.shrink_units
FROM
  tc22 AS a
INNER JOIN (
  SELECT
    DISTINCT week_end_day_date,
    month_idnt,
    completed_month_ind
  FROM
    dates) AS b
ON
  a.week_end_day_date = b.week_end_day_date
LEFT JOIN (
  SELECT
    c.month_idnt,
    a0.location_num,
    a0.department_num,
    a0.class_num,
    a0.subclass_num
  FROM
    sl AS a0
  INNER JOIN
    dates AS c
  ON
    a0.week_end_day_date = c.day_date
    AND c.day_date = c.week_end_day_date
  WHERE
    LOWER(a0.shrink_category) = LOWER('Additional Shrink')
    AND NOT a0.shrink_cost BETWEEN - 0.01
    AND 0.01
  GROUP BY
    a0.location_num,
    a0.department_num,
    a0.class_num,
    a0.subclass_num,
    c.month_idnt) AS c
ON
  b.month_idnt = c.month_idnt
  AND a.location_num = c.location_num
  AND a.department_num = c.department_num
  AND a.class_num = c.class_num
  AND a.subclass_num = c.subclass_num
WHERE
  a.week_end_day_date > DATE '2024-02-04'
  AND (c.month_idnt IS NOT NULL
    OR b.completed_month_ind = 0)
  AND NOT a.shrink_cost BETWEEN - 0.01  AND 0.01; 
  
CREATE TEMPORARY TABLE IF NOT EXISTS ma
 AS
SELECT
  c.week_end_day_date,
  CAST(a.store_num AS INTEGER) AS location_num,
  b.channel_num,
  SUBSTR(NULL, 1, 50) AS gl_ref_no,
  'Manual Accural/Adjustment' AS shrink_category,
  SUBSTR(NULL, 1, 150) AS reason_description,
  CAST(NULL AS INTEGER) AS department_num,
  CAST(NULL AS INTEGER) AS class_num,
  CAST(NULL AS INTEGER) AS subclass_num,
  SUBSTR(NULL, 1, 150) AS brand_name,
  SUBSTR(NULL, 1, 150) AS supplier,
  SUM(a.shrink_cost_amt) AS shrink_cost,
  SUM(a.shrink_retail_amt) AS shrink_retail,
  SUM(a.shrink_units) AS shrink_units
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_shrink_accrual_store_week_fact AS a
INNER JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS b
ON
  a.store_num = b.store_num
  AND LOWER(b.store_country_code) = LOWER('US')
INNER JOIN (
  SELECT
    DISTINCT week_idnt,
    week_end_day_date
  FROM
    dates) AS c
ON
  a.week_num = c.week_idnt
GROUP BY
  c.week_end_day_date,
  location_num,
  b.channel_num,
  gl_ref_no,
  shrink_category,
  reason_description,
  department_num,
  class_num,
  subclass_num,
  brand_name,
  supplier; 


CREATE TEMPORARY TABLE IF NOT EXISTS sales
 AS
SELECT
  c.week_end_day_date,
  CAST(TRUNC(CAST(a.store_num AS FLOAT64)) AS INTEGER) AS store_num,
  a.channel_num,
  a.department_num,
  a.class_num,
  a.subclass_num,
  SUBSTR(NULL, 1, 150) AS brand_name,
  SUBSTR(NULL, 1, 150) AS supplier,
  SUBSTR(NULL, 1, 150) AS shrink_category,
  SUM(a.net_sales_cost) AS net_sales_cost,
  SUM(a.net_sales_retail) AS net_sales_retail,
  SUM(a.sales_units) AS net_sales_units
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_eow_sl_olap_vw AS a
INNER JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS b
ON
  CAST(a.store_num AS FLOAT64) = b.store_num
  AND LOWER(b.store_country_code ) = LOWER('US')
INNER JOIN (
  SELECT
    DISTINCT week_idnt,
    week_idnt_true,
    week_end_day_date
  FROM
    dates) AS c
ON
  a.week_num = c.week_idnt_true
WHERE
  a.channel_num IN (110,
    120,
    210,
    220,
    250,
    260,
    310,
    920,
    930)
  AND a.sales_units <> 0
  AND LOWER(b.store_short_name) NOT LIKE LOWER('%CLSD%')
  AND a.department_num NOT IN (
  SELECT
    DISTINCT dept_num
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  WHERE
    div_num IN (600,
      800,
      96))
GROUP BY
  c.week_end_day_date,
  store_num,
  a.channel_num,
  a.department_num,
  a.class_num,
  a.subclass_num,
  brand_name,
  supplier,
  shrink_category; 
  

truncate table `{{params.gcp_project_id}}`.{{params.environment_schema}}.shrink_detail_weekly ; 

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.shrink_detail_weekly 
SELECT
  *
FROM
  ua
UNION ALL
SELECT
  *
FROM
  sl
WHERE
  shrink_category <> 'Additional Shrink'
UNION ALL
SELECT
  *
FROM
  ma
UNION ALL
SELECT
  *
FROM
  addtl ;


UPDATE
  `{{params.gcp_project_id}}`.{{params.environment_schema}}.shrink_detail_weekly
SET
  channel_num = 120
WHERE
  location_num = 209 ; 


truncate table  `{{params.gcp_project_id}}`.{{params.environment_schema}}.shrink_summary_weekly ;

INSERT INTO  `{{params.gcp_project_id}}`.{{params.environment_schema}}.shrink_summary_weekly
SELECT
  g.store_name,
  g.region_desc,
  CASE
    WHEN location_num = cast(209 as string) THEN '120, N.COM'
    ELSE TRIM(g.channel_num || ', ' || g.channel_desc)
END
  AS channel,
  --district,
  CASE
    WHEN g.channel_num IN (110, 120, 140, 310, 920) THEN 'NORDSTROM'
    WHEN g.channel_num IN (210,
    220,
    240,
    250,
    260) THEN 'NORDSTROM_RACK'
END
  AS banner,
  h.week_454_label,
  h.week_idnt,
  h.fiscal_week_num,
  h.month_454_label,
  h.month_idnt,
  h.quarter_label,
  h.half_label,
  h.fiscal_year_num,
  h.ty_ly_lly_ind,
  i.region AS store_region,
  i.shrink_cup_group,
  f.*
FROM (
  SELECT
    COALESCE(a.week_end_day_date, b.week_end_day_date) AS week_end_day_date,
    CAST(COALESCE(a.location_num, b.store_num) AS STRING) AS location_num,
    COALESCE(a.channel_num, b.channel_num) AS channel_num,
    COALESCE(a.shrink_category, b.shrink_category) AS shrink_category,
    COALESCE(a.department_num, b.department_num) AS department_num,
    COALESCE(a.class_num, b.class_num) AS class_num,
    COALESCE(a.subclass_num, b.subclass_num) AS subclass_num,
    COALESCE(a.brand_name, b.brand_name) AS brand_name,
    COALESCE(a.supplier, b.supplier) AS supplier,
    COALESCE(shrink_c, 0) AS shrink_c,
    COALESCE(shrink_u, 0) AS shrink_u,
    COALESCE(shrink_r, 0) AS shrink_r,
    COALESCE(net_sales_cost, 0) AS sales_c,
    COALESCE(net_sales_units, 0) AS sales_u,
    COALESCE(net_sales_retail, 0) AS sales_r
  FROM (
    SELECT
      a.week_end_day_date,
      location_num,
      CASE
        WHEN location_num = 209 THEN 120
        ELSE channel_num
    END
      AS channel_num,
      shrink_category,
      department_num,
      class_num,
      subclass_num,
      brand_name,
      supplier,
      SUM(shrink_cost) AS shrink_c,
      SUM(shrink_units) AS shrink_u,
      SUM(shrink_retail) AS shrink_r
    FROM
      `{{params.gcp_project_id}}`.{{params.environment_schema}}.shrink_detail_weekly a
    INNER JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw c
    ON
      a.week_end_day_date = c.day_date
      AND c.day_date = c.week_end_day_date
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9 ) a
  FULL OUTER JOIN
    sales b
  ON
    a.week_end_day_date = b.week_end_day_date
    AND a.location_num = b.store_num
    AND a.channel_num = b.channel_num
    AND a.shrink_category = b.shrink_category
    AND a.department_num = b.department_num
    AND a.class_num = b.class_num
    AND a.subclass_num = b.subclass_num
    AND a.brand_name = b.brand_name
    AND a.supplier = b.supplier ) f
INNER JOIN
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim g
ON
  f.location_num = cast(g.store_num as string)
INNER JOIN (
  SELECT
    DISTINCT week_end_day_date,
    week_454_label,
    week_idnt,
    fiscal_week_num,
    month_454_label,
    month_idnt,
    quarter_label,
    half_label,
    fiscal_year_num,
    ty_ly_lly_ind
  FROM
    dates ) h
ON
  f.week_end_day_date = h.week_end_day_date
LEFT JOIN
  `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.shrink_cup_stores i
ON
  f.location_num = cast(i.store  as string)