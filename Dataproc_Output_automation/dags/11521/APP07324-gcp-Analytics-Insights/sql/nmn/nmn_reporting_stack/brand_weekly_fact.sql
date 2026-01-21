
CREATE TEMPORARY TABLE IF NOT EXISTS date_variables AS
SELECT
  a.week_idnt AS start_week_idnt,
  b.week_idnt AS end_week_idnt,
  a.day_date AS start_date,
  b.day_date AS end_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim a
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim b
ON a.day_date = {{params.start_date}} AND b.day_date  = {{params.end_date}};



CREATE TEMPORARY TABLE IF NOT EXISTS ly_week_mapping
AS
SELECT DISTINCT TRIM(FORMAT('%11d', a.week_idnt)) AS week_idnt,
 TRIM(FORMAT('%11d', b.week_idnt)) AS last_year_wk_idnt,
 TRIM(FORMAT('%11d', a.month_idnt)) AS month_idnt,
 TRIM(FORMAT('%11d', a.quarter_idnt)) AS quarter_idnt,
 TRIM(FORMAT('%11d', a.fiscal_year_num)) AS fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
 LEFT JOIN (SELECT DISTINCT week_idnt,
   day_idnt,
   day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS b ON a.day_idnt_last_year_realigned = b.day_idnt
WHERE a.week_idnt BETWEEN 201901 AND 202552;



CREATE TEMPORARY TABLE IF NOT EXISTS sku_id_map
AS
SELECT DISTINCT TRIM(sku.rms_sku_num) AS rms_sku_num,
 sku.channel_country,
--  TRIM(FORMAT('%11d', CONCAT(sku.div_num, ', ', sku.div_desc))) AS div_label,
TRIM(CONCAT(LPAD(CAST(sku.div_num AS STRING), 11, ' '), ', ', sku.div_desc)) AS div_label,
sku.div_num,
TRIM(CONCAT(LPAD(CAST(sku.grp_num AS STRING), 11, ' '), ', ', sku.grp_desc)) AS grp_label,
sku.grp_num,
TRIM(CONCAT(LPAD(CAST(sku.dept_num AS STRING), 11, ' '), ', ', sku.dept_desc)) AS dept_label,
sku.dept_num,
TRIM(CONCAT(LPAD(CAST(sku.class_num AS STRING), 11, ' '), ', ', sku.class_desc)) AS class_label,

 sku.class_num,
 sku.brand_name,
 v.npg_flag,
 v.vendor_name AS supplier_name
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS v ON LOWER(sku.prmy_supp_num) = LOWER(v.vendor_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim AS vr ON LOWER(sku.prmy_supp_num) = LOWER(vr.order_from_vendor_num
   )
WHERE LOWER(v.vendor_status_code) = LOWER('A')
 AND LOWER(vr.payto_vendor_status_code) = LOWER('A')
 AND LOWER(sku.channel_country) = LOWER('US');




CREATE TEMPORARY TABLE IF NOT EXISTS sales_units_week_level
AS
SELECT a.chnl_idnt AS channel_num,
 TRIM(FORMAT('%11d', a.week_idnt)) AS week_idnt,
 TRIM(lw.last_year_wk_idnt) AS last_year_wk_idnt,
 TRIM(lw.month_idnt) AS month_idnt,
 TRIM(lw.quarter_idnt) AS quarter_idnt,
 TRIM(lw.fiscal_year_num) AS fiscal_year_num,
 b.channel_country,
 b.div_label,
 b.div_num,
 b.grp_label,
 b.grp_num,
 b.dept_label,
 b.dept_num,
 b.class_label,
 b.class_num,
 b.brand_name,
 b.npg_flag,
 b.supplier_name,
 SUM(a.sales_dollars) AS sales_dollars,
 SUM(a.return_dollars) AS return_dollars,
 SUM(a.sales_units) AS sales_units,
 SUM(a.return_units) AS return_units,
 SUM(a.eoh_units) AS eoh_units
FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.wbr_merch_staging AS a
 LEFT JOIN ly_week_mapping AS lw ON a.week_idnt = CAST(lw.week_idnt AS FLOAT64)
 LEFT JOIN sku_id_map AS b ON LOWER(a.sku_idnt) = LOWER(b.rms_sku_num) AND LOWER(a.country) = LOWER(b.channel_country)
WHERE a.week_idnt BETWEEN (SELECT start_week_idnt
   FROM date_variables) AND (SELECT end_week_idnt
   FROM date_variables)
 AND LOWER(a.country) = LOWER('US')
GROUP BY channel_num,
 week_idnt,
 last_year_wk_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 b.channel_country,
 b.div_label,
 b.div_num,
 b.grp_label,
 b.grp_num,
 b.dept_label,
 b.dept_num,
 b.class_label,
 b.class_num,
 b.brand_name,
 b.npg_flag,
 b.supplier_name;




CREATE TEMPORARY TABLE IF NOT EXISTS receipts_week_level
AS
SELECT CAST(TRUNC(CAST(a.channel_num AS FLOAT64)) AS INTEGER) AS channel_num,
 TRIM(FORMAT('%11d', a.week_num)) AS week_idnt,
 TRIM(lw.last_year_wk_idnt) AS last_year_wk_idnt,
 TRIM(FORMAT('%11d', a.month_num)) AS month_idnt,
 TRIM(lw.quarter_idnt) AS quarter_idnt,
 TRIM(FORMAT('%11d', a.year_num)) AS fiscal_year_num,
 c.store_country_code AS channel_country,
 b.div_label,
 b.div_num,
 b.brand_name,
 b.npg_flag,
 b.grp_label,
 b.grp_num,
 b.dept_label,
 b.dept_num,
 b.class_label,
 b.class_num,
 b.supplier_name,
 COALESCE(SUM(a.receipts_regular_units + a.receipts_crossdock_regular_units), 0) AS receipt_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS a
 LEFT JOIN ly_week_mapping AS lw ON a.week_num = CAST(lw.week_idnt AS FLOAT64)
 INNER JOIN sku_id_map AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND LOWER(b.channel_country) = LOWER('US')
 INNER JOIN (SELECT DISTINCT channel_num,
   store_country_code
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim) AS c ON a.channel_num = c.channel_num AND LOWER(c.store_country_code) = LOWER('US')
WHERE a.week_num BETWEEN (SELECT start_week_idnt
   FROM date_variables) AND (SELECT end_week_idnt
   FROM date_variables)
 AND LOWER(a.smart_sample_ind) <> LOWER('Y')
 AND LOWER(a.gift_with_purchase_ind) <> LOWER('Y')
GROUP BY channel_num,
 week_idnt,
 last_year_wk_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 channel_country,
 b.div_label,
 b.div_num,
 b.brand_name,
 b.npg_flag,
 b.grp_label,
 b.grp_num,
 b.dept_label,
 b.dept_num,
 b.class_label,
 b.class_num,
 b.supplier_name;



CREATE TEMPORARY TABLE IF NOT EXISTS funnel_week_sku_level
AS
SELECT CAST(TRUNC(CAST(CASE
   WHEN CASE
     WHEN LOWER(f.channel) = LOWER('FULL_LINE')
     THEN '120'
     WHEN LOWER(f.channel) = LOWER('RACK')
     THEN '250'
     ELSE NULL
     END = ''
   THEN '0'
   ELSE CASE
    WHEN LOWER(f.channel) = LOWER('FULL_LINE')
    THEN '120'
    WHEN LOWER(f.channel) = LOWER('RACK')
    THEN '250'
    ELSE NULL
    END
   END AS FLOAT64)) AS INTEGER) AS channel_num,
 d.week_idnt,
 f.rms_sku_num,
 SUM(f.product_views) AS product_views,
 SUM(f.product_views * f.pct_instock) AS instock_product_views,
 SUM(f.order_quantity) AS order_quantity
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS f
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS d ON f.event_date_pacific = d.day_date
WHERE f.event_date_pacific BETWEEN (SELECT start_date
   FROM date_variables) AND (SELECT end_date
   FROM date_variables)
 AND LOWER(f.channelcountry) = LOWER('US')
 AND LOWER(f.channel) <> LOWER('UNKNOWN')
GROUP BY channel_num,
 d.week_idnt,
 f.rms_sku_num;


CREATE TEMPORARY TABLE IF NOT EXISTS funnel_week
AS
SELECT f.channel_num,
 f.week_idnt,
 lw.last_year_wk_idnt,
 lw.month_idnt,
 lw.quarter_idnt,
 lw.fiscal_year_num,
 sku.channel_country,
 sku.div_label,
 sku.div_num,
 sku.grp_label,
 sku.grp_num,
 sku.dept_label,
 sku.dept_num,
 sku.class_label,
 sku.class_num,
 sku.brand_name,
 sku.npg_flag,
 sku.supplier_name,
 SUM(f.product_views) AS product_views,
 SUM(f.instock_product_views) AS instock_product_views,
 SUM(f.order_quantity) AS order_quantity
FROM funnel_week_sku_level AS f
 LEFT JOIN sku_id_map AS sku ON LOWER(sku.rms_sku_num) = LOWER(f.rms_sku_num) AND LOWER(sku.channel_country) = LOWER('US'
    )
 LEFT JOIN ly_week_mapping AS lw ON f.week_idnt = CAST(lw.week_idnt AS FLOAT64)
GROUP BY f.channel_num,
 f.week_idnt,
 lw.last_year_wk_idnt,
 lw.month_idnt,
 lw.quarter_idnt,
 lw.fiscal_year_num,
 sku.channel_country,
 sku.div_label,
 sku.div_num,
 sku.grp_label,
 sku.grp_num,
 sku.dept_label,
 sku.dept_num,
 sku.class_label,
 sku.class_num,
 sku.brand_name,
 sku.npg_flag,
 sku.supplier_name;


CREATE TEMPORARY TABLE IF NOT EXISTS twist_day
AS
SELECT base.rms_sku_num,
 base.day_date,
 dd.week_idnt,
 base.store_num,
 st.channel_num,
 SUM(base.allocated_traffic) AS allocated_traffic,
 SUM(CASE
   WHEN base.mc_instock_ind = 1
   THEN base.allocated_traffic
   ELSE 0
   END) AS instock_allocated_traffic
FROM `{{params.gcp_project_id}}`.t2dl_das_twist.twist_daily AS base
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CAST(base.store_num AS FLOAT64) = st.store_num AND st.channel_num IN (110
    , 120, 140, 210, 240, 250)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dd ON base.day_date = dd.day_date
WHERE base.day_date BETWEEN (SELECT start_date
   FROM date_variables) AND (SELECT end_date
   FROM date_variables)
GROUP BY base.rms_sku_num,
 base.day_date,
 dd.week_idnt,
 base.store_num,
 st.channel_num;

CREATE TEMPORARY TABLE IF NOT EXISTS twist_week
AS
SELECT f.channel_num,
 f.week_idnt,
 lw.last_year_wk_idnt,
 lw.month_idnt,
 lw.quarter_idnt,
 lw.fiscal_year_num,
 sku.channel_country,
 sku.div_label,
 sku.div_num,
 sku.grp_label,
 sku.grp_num,
 sku.dept_label,
 sku.dept_num,
 sku.class_label,
 sku.class_num,
 sku.brand_name,
 sku.npg_flag,
 sku.supplier_name,
 SUM(f.allocated_traffic) AS allocated_traffic,
 SUM(f.instock_allocated_traffic) AS instock_allocated_traffic
FROM twist_day AS f
 LEFT JOIN ly_week_mapping AS lw ON f.week_idnt = CAST(lw.week_idnt AS FLOAT64)
 LEFT JOIN sku_id_map AS sku ON LOWER(sku.rms_sku_num) = LOWER(f.rms_sku_num) AND LOWER(sku.channel_country) = LOWER('US'
    )
GROUP BY f.channel_num,
 f.week_idnt,
 lw.last_year_wk_idnt,
 lw.month_idnt,
 lw.quarter_idnt,
 lw.fiscal_year_num,
 sku.channel_country,
 sku.div_label,
 sku.div_num,
 sku.grp_label,
 sku.grp_num,
 sku.dept_label,
 sku.dept_num,
 sku.class_label,
 sku.class_num,
 sku.brand_name,
 sku.npg_flag,
 sku.supplier_name;

CREATE TEMPORARY TABLE IF NOT EXISTS sales_receipts_pv_twist
AS
SELECT channel_num,
 week_idnt,
 last_year_wk_idnt AS last_year_week_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 channel_country,
 div_label,
 div_num,
 grp_label,
 grp_num,
 dept_label,
 dept_num,
 class_label,
 class_num,
 brand_name,
 npg_flag,
 supplier_name,
 COALESCE(SUM(sales_dollars), 0) AS net_sales_dollars_r,
 COALESCE(SUM(return_dollars), 0) AS return_dollars_r,
 COALESCE(SUM(sales_units), 0) AS net_sales_units,
 COALESCE(SUM(return_units), 0) AS return_units,
 COALESCE(SUM(eoh_units), 0) AS eoh_units,
 SUM(receipt_units) AS receipt_units,
 SUM(product_views) AS product_views,
 SUM(order_unit_qty) AS order_units,
 SUM(allocated_traffic) AS allocated_traffic,
 SUM(instock_allocated_traffic) AS instock_allocated_traffic
FROM (SELECT CAST(TRUNC(CAST(CASE
      WHEN channel_num = ''
      THEN '0'
      ELSE channel_num
      END AS FLOAT64)) AS INTEGER) AS channel_num,
    TRIM(week_idnt) AS week_idnt,
    TRIM(last_year_wk_idnt) AS last_year_wk_idnt,
    TRIM(month_idnt) AS month_idnt,
    TRIM(quarter_idnt) AS quarter_idnt,
    TRIM(fiscal_year_num) AS fiscal_year_num,
    channel_country,
    div_label,
    div_num,
    grp_label,
    grp_num,
    dept_label,
    dept_num,
    class_label,
    class_num,
    brand_name,
    npg_flag,
    supplier_name,
    SUM(sales_dollars) AS sales_dollars,
    SUM(return_dollars) AS return_dollars,
    SUM(sales_units) AS sales_units,
    SUM(return_units) AS return_units,
    SUM(eoh_units) AS eoh_units,
    0 AS receipt_units,
    0 AS product_views,
    0 AS order_unit_qty,
    0 AS allocated_traffic,
    0 AS instock_allocated_traffic
   FROM sales_units_week_level
   GROUP BY channel_num,
    week_idnt,
    last_year_wk_idnt,
    month_idnt,
    quarter_idnt,
    fiscal_year_num,
    channel_country,
    div_label,
    div_num,
    grp_label,
    grp_num,
    dept_label,
    dept_num,
    class_label,
    class_num,
    brand_name,
    npg_flag,
    supplier_name
   UNION ALL
   SELECT channel_num,
    TRIM(week_idnt) AS week_idnt,
    TRIM(last_year_wk_idnt) AS last_year_wk_idnt,
    TRIM(month_idnt) AS month_idnt,
    TRIM(quarter_idnt) AS quarter_idnt,
    TRIM(fiscal_year_num) AS fiscal_year_num,
    channel_country,
    div_label,
    div_num,
    grp_label,
    grp_num,
    dept_label,
    dept_num,
    class_label,
    class_num,
    brand_name,
    npg_flag,
    supplier_name,
    0 AS sales_dollars,
    0 AS return_dollars,
    0 AS sales_units,
    0 AS return_units,
    0 AS eoh_units,
    SUM(receipt_units) AS receipt_units,
    0 AS product_views,
    0 AS order_unit_qty,
    0 AS allocated_traffic,
    0 AS instock_allocated_traffic
   FROM receipts_week_level
   GROUP BY channel_num,
    week_idnt,
    last_year_wk_idnt,
    month_idnt,
    quarter_idnt,
    fiscal_year_num,
    channel_country,
    div_label,
    div_num,
    grp_label,
    grp_num,
    dept_label,
    dept_num,
    class_label,
    class_num,
    brand_name,
    npg_flag,
    supplier_name
   UNION ALL
   SELECT CAST(TRUNC(CAST(channel_num AS FLOAT64)) AS INTEGER) AS channel_num,
    TRIM(FORMAT('%11d', week_idnt)) AS week_idnt,
    TRIM(last_year_wk_idnt) AS last_year_wk_idnt,
    TRIM(month_idnt) AS month_idnt,
    TRIM(quarter_idnt) AS quarter_idnt,
    TRIM(fiscal_year_num) AS fiscal_year_num,
    channel_country,
    div_label,
    div_num,
    grp_label,
    grp_num,
    dept_label,
    dept_num,
    class_label,
    class_num,
    brand_name,
    npg_flag,
    supplier_name,
    0 AS sales_dollars,
    0 AS return_dollars,
    0 AS sales_units,
    0 AS return_units,
    0 AS eoh_units,
    0 AS receipt_units,
    0 AS product_views,
    0 AS order_unit_qty,
    SUM(allocated_traffic) AS allocated_traffic,
    SUM(instock_allocated_traffic) AS instock_allocated_traffic
   FROM twist_week
   GROUP BY channel_num,
    week_idnt,
    last_year_wk_idnt,
    month_idnt,
    quarter_idnt,
    fiscal_year_num,
    channel_country,
    div_label,
    div_num,
    grp_label,
    grp_num,
    dept_label,
    dept_num,
    class_label,
    class_num,
    brand_name,
    npg_flag,
    supplier_name
   UNION ALL
   SELECT channel_num,
    TRIM(FORMAT('%11d', week_idnt)) AS week_idnt,
    TRIM(last_year_wk_idnt) AS last_year_wk_idnt,
    TRIM(month_idnt) AS month_idnt,
    TRIM(quarter_idnt) AS quarter_idnt,
    TRIM(fiscal_year_num) AS fiscal_year_num,
    channel_country,
    div_label,
    div_num,
    grp_label,
    grp_num,
    dept_label,
    dept_num,
    class_label,
    class_num,
    brand_name,
    npg_flag,
    supplier_name,
    0 AS sales_dollars,
    0 AS return_dollars,
    0 AS sales_units,
    0 AS return_units,
    0 AS eoh_units,
    0 AS receipt_units,
    SUM(product_views) AS product_views,
    SUM(order_quantity) AS order_unit_qty,
    0 AS allocated_traffic,
    0 AS instock_allocated_traffic
   FROM funnel_week
   GROUP BY channel_num,
    week_idnt,
    last_year_wk_idnt,
    month_idnt,
    quarter_idnt,
    fiscal_year_num,
    channel_country,
    div_label,
    div_num,
    grp_label,
    grp_num,
    dept_label,
    dept_num,
    class_label,
    class_num,
    brand_name,
    npg_flag,
    supplier_name) AS x
GROUP BY channel_num,
 week_idnt,
 last_year_wk_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num,
 channel_country,
 div_label,
 div_num,
 grp_label,
 grp_num,
 dept_label,
 dept_num,
 class_label,
 class_num,
 brand_name,
 npg_flag,
 supplier_name;

 
DELETE FROM `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.brand_weekly_fact
WHERE week_idnt BETWEEN (SELECT start_week_idnt
        FROM date_variables) AND (SELECT end_week_idnt
        FROM date_variables);

INSERT INTO `{{params.gcp_project_id}}`.{{params.nmn_t2_schema}}.brand_weekly_fact
(SELECT channel_num,
  SUBSTR(channel_country, 1, 2) AS channel_country,
  CAST(TRUNC(CAST(week_idnt AS FLOAT64)) AS INTEGER) AS week_idnt,
  CAST(TRUNC(CAST(last_year_week_idnt AS FLOAT64)) AS INTEGER) AS last_year_week_idnt,
  CAST(TRUNC(CAST(month_idnt AS FLOAT64)) AS INTEGER) AS month_idnt,
  CAST(TRUNC(CAST(quarter_idnt AS FLOAT64)) AS INTEGER) AS quarter_idnt,
  CAST(TRUNC(CAST(fiscal_year_num AS FLOAT64)) AS INTEGER) AS fiscal_year_num,
  div_label,
  div_num,
  grp_label,
  grp_num,
  dept_label,
  dept_num,
  class_label,
  class_num,
  brand_name,
  npg_flag,
  supplier_name,
  net_sales_dollars_r,
  return_dollars_r,
  net_sales_units,
  return_units,
  CAST(TRUNC(CAST(eoh_units AS FLOAT64)) AS INTEGER) AS eoh_units,
  receipt_units,
  CAST(TRUNC(CAST(product_views AS FLOAT64)) AS INTEGER) AS product_views,
  order_units,
  allocated_traffic,
  CAST(instock_allocated_traffic AS NUMERIC) AS instock_allocated_traffic,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM sales_receipts_pv_twist);



