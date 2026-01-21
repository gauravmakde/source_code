
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.divisional_summary_weekly;

CREATE TEMPORARY TABLE IF NOT EXISTS dts

AS
SELECT dt.day_dt,
 dt.wk_end_dt AS week_end_date,
 dt.wk_num_realigned AS fiscal_week,
 dt.mth_desc_realigned AS fiscal_month,
 dt.yr_idnt_realigned AS fiscal_year,
 dt.qtr_desc_realigned AS fiscal_quarter,
 ty_ly.ty_ly_ind
FROM (SELECT day_date AS day_dt,
   week_end_day_date AS wk_end_dt,
   fiscal_year_num AS fiscal_year,
   fiscal_year_num AS yr_idnt_realigned,
   fiscal_quarter_num AS qtr_454_realigned,
   quarter_desc AS qtr_desc_realigned,
   fiscal_month_num AS mth_454_realigned,
   month_desc AS mth_desc_realigned,
   fiscal_week_num AS wk_num_realigned,
   week_desc AS wk_desc_realigned,
   fiscal_day_num AS day_num_realigned
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
  WHERE week_end_day_date <= (SELECT MAX(event_date_pacific)
     FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_funnel_daily)) AS dt
 INNER JOIN (SELECT fiscal_year,
    CASE
    WHEN CAST(TRUNC(CAST(yr_rank AS FLOAT64)) AS INTEGER) = 1
    THEN 'TY'
    WHEN CAST(TRUNC(CAST(yr_rank AS FLOAT64)) AS INTEGER) = 2
    THEN 'LY'
    ELSE 'OTHER'
    END AS ty_ly_ind,
   latest_wk,
   MIN(latest_wk) OVER (ORDER BY fiscal_year DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fiscal_week
  FROM (SELECT t5.fiscal_year,
     t5.latest_wk,
     DENSE_RANK() OVER (ORDER BY t5.fiscal_year DESC) AS yr_rank
    FROM (SELECT dt.fiscal_year_num AS fiscal_year,
       dt.fiscal_week_num,
       MAX(dt.fiscal_week_num) AS latest_wk
      FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS pf
       INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS dt ON pf.event_date_pacific = dt.day_date AND LOWER(dt.ty_ly_lly_ind
          ) IN (LOWER('TY'), LOWER('LY'))
      WHERE dt.week_end_day_date <= (SELECT MAX(event_date_pacific)
         FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_funnel_daily)
      GROUP BY fiscal_year,
       dt.fiscal_week_num) AS t5) AS ty_base) AS ty_ly ON dt.fiscal_year = ty_ly.fiscal_year AND dt.wk_num_realigned <=
   ty_ly.fiscal_week
WHERE LOWER(ty_ly.ty_ly_ind) <> LOWER('OTHER');


CREATE TEMPORARY TABLE IF NOT EXISTS sku_base
AS
SELECT sku.rms_sku_num,
 sku.channel_country,
 sku.web_style_num,
 sty.style_group_num,
 sku.dept_num,
 sku.prmy_supp_num AS supplier_idnt,
 sku.class_num,
 sku.sbclass_num,
   SUBSTR(CAST(sku.div_num AS STRING), 1, 20) || ', ' || sku.div_desc AS division,
   SUBSTR(CAST(sku.grp_num AS STRING), 1, 20) || ', ' || sku.grp_desc AS subdiv,
   SUBSTR(CAST(sku.dept_num AS STRING), 1, 20) || ', ' || sku.dept_desc AS department,
   SUBSTR(CAST(sku.class_num AS STRING), 1, 20) || ', ' || sku.class_desc AS class,
   SUBSTR(CAST(sku.sbclass_num AS STRING), 1, 20) || ', ' || sku.sbclass_desc AS subclass,
 UPPER(sku.brand_name) AS brand_name,
 UPPER(supp.vendor_name) AS supplier,
 UPPER(sty.type_level_1_desc) AS product_type_1,
 UPPER(sty.style_desc) AS style_desc,
 sku.partner_relationship_type_code,
 sku.npg_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS sku
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim AS sty ON sku.epm_style_num = sty.epm_style_num AND LOWER(sku.channel_country
    ) = LOWER(sty.channel_country)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp ON LOWER(sku.prmy_supp_num) = LOWER(supp.vendor_num)
WHERE (sku.div_num <> 340 OR sku.class_num <> 90)
 AND sku.dept_num NOT IN (584, 585, 523);

CREATE TEMPORARY TABLE IF NOT EXISTS pfd

AS
SELECT pf.rms_sku_num,
 pf.channelcountry,
 pf.channel,
 pf.platform,
 pf.web_style_num AS web_style_id,
 pf.regular_price_amt,
 pf.current_price_amt,
 pf.current_price_type,
 pf.rp_ind,
 pf.event_type,
 ty_ly.week_end_date,
 ty_ly.fiscal_week,
 ty_ly.fiscal_month,
 ty_ly.fiscal_year,
 ty_ly.fiscal_quarter,
 ty_ly.ty_ly_ind,
 SUM(pf.product_views) AS product_views,
 SUM(pf.product_view_sessions) AS product_view_sessions,
 SUM(pf.add_to_bag_quantity) AS add_to_bag_quantity,
 SUM(pf.add_to_bag_sessions) AS add_to_bag_sessions,
 SUM(pf.order_demand) AS order_demand,
 SUM(pf.order_quantity) AS order_quantity,
 SUM(pf.order_sessions) AS order_sessions,
 SUM(pf.product_views * pf.pct_instock) AS instock_views,
 SUM(CASE
   WHEN pf.pct_instock IS NOT NULL
   THEN pf.product_views
   ELSE NULL
   END) AS scored_views
FROM `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.product_price_funnel_daily AS pf
 INNER JOIN dts AS ty_ly ON pf.event_date_pacific = ty_ly.day_dt
WHERE pf.event_date_pacific >= (SELECT MIN(day_dt)
   FROM dts)
 AND pf.event_date_pacific <= (SELECT MAX(day_dt)
   FROM dts)
GROUP BY pf.rms_sku_num,
 pf.channelcountry,
 pf.channel,
 pf.platform,
 web_style_id,
 pf.regular_price_amt,
 pf.current_price_amt,
 pf.current_price_type,
 pf.rp_ind,
 pf.event_type,
 ty_ly.week_end_date,
 ty_ly.fiscal_week,
 ty_ly.fiscal_month,
 ty_ly.fiscal_year,
 ty_ly.fiscal_quarter,
 ty_ly.ty_ly_ind;


CREATE TEMPORARY TABLE IF NOT EXISTS pfd_hier

AS
SELECT pf.week_end_date,
 pf.fiscal_week,
 pf.fiscal_month,
 pf.fiscal_year,
 pf.fiscal_quarter,
 pf.ty_ly_ind,
 pf.channelcountry,
 pf.channel,
 pf.platform,
 sku_base.dept_num,
 sku_base.supplier_idnt,
 sku_base.class_num,
 sku_base.sbclass_num,
 COALESCE(sku_base.division, 'UNKNOWN') AS division,
 COALESCE(sku_base.subdiv, 'UNKNOWN') AS subdiv,
 COALESCE(sku_base.department, 'UNKNOWN') AS department,
 COALESCE(sku_base.class, 'UNKNOWN') AS class,
 COALESCE(sku_base.subclass, 'UNKNOWN') AS subclass,
 COALESCE(sku_base.brand_name, 'UNKNOWN') AS brand_name,
 COALESCE(sku_base.supplier, 'UNKNOWN') AS supplier,
 COALESCE(sku_base.product_type_1, 'UNKNOWN') AS product_type_1,
 pf.event_type,
  CASE
  WHEN pf.rp_ind = 1
  THEN 'RP'
  ELSE 'NRP'
  END AS rp,
  CASE
  WHEN LOWER(sku_base.partner_relationship_type_code) = LOWER('ECONCESSION')
  THEN 'Y'
  ELSE 'N'
  END AS mp_ind,
 sku_base.npg_ind,
 COALESCE(pf.current_price_type, 'UNKNOWN') AS price_type,
  CASE
  WHEN pf.current_price_amt IS NULL OR pf.current_price_amt = 0
  THEN 'UNKNOWN'
  WHEN pf.current_price_amt <= 10.00
  THEN '< $10'
  WHEN pf.current_price_amt <= 25.00
  THEN '$10 - $25'
  WHEN pf.current_price_amt <= 50.00
  THEN '$25 - $50'
  WHEN pf.current_price_amt <= 100.00
  THEN '$50 - $100'
  WHEN pf.current_price_amt <= 150.00
  THEN '$100 - $150'
  WHEN pf.current_price_amt <= 200.00
  THEN '$150 - $200'
  WHEN pf.current_price_amt <= 300.00
  THEN '$200 - $300'
  WHEN pf.current_price_amt <= 500.00
  THEN '$300 - $500'
  WHEN pf.current_price_amt <= 1000.00
  THEN '$500 - $1000'
  WHEN pf.current_price_amt > 1000.00
  THEN '> $1000'
  ELSE NULL
  END AS price_band_one,
  CASE
  WHEN pf.current_price_amt IS NULL OR pf.current_price_amt = 0
  THEN 'UNKNOWN'
  WHEN pf.current_price_amt <= 10.00
  THEN '< $10'
  WHEN pf.current_price_amt <= 15.00
  THEN '$10 - $15'
  WHEN pf.current_price_amt <= 20.00
  THEN '$15 - $20'
  WHEN pf.current_price_amt <= 25.00
  THEN '$20 - $25'
  WHEN pf.current_price_amt <= 30.00
  THEN '$25 - $30'
  WHEN pf.current_price_amt <= 40.00
  THEN '$30 - $40'
  WHEN pf.current_price_amt <= 50.00
  THEN '$40 - $50'
  WHEN pf.current_price_amt <= 60.00
  THEN '$50 - $60'
  WHEN pf.current_price_amt <= 80.00
  THEN '$60 - $80'
  WHEN pf.current_price_amt <= 100.00
  THEN '$80 - $100'
  WHEN pf.current_price_amt <= 125.00
  THEN '$100 - $125'
  WHEN pf.current_price_amt <= 150.00
  THEN '$125 - $150'
  WHEN pf.current_price_amt <= 175.00
  THEN '$150 - $175'
  WHEN pf.current_price_amt <= 200.00
  THEN '$175 - $200'
  WHEN pf.current_price_amt <= 250.00
  THEN '$200 - $250'
  WHEN pf.current_price_amt <= 300.00
  THEN '$250 - $300'
  WHEN pf.current_price_amt <= 400.00
  THEN '$300 - $400'
  WHEN pf.current_price_amt <= 500.00
  THEN '$400 - $500'
  WHEN pf.current_price_amt <= 700.00
  THEN '$500 - $700'
  WHEN pf.current_price_amt <= 900.00
  THEN '$700 - $900'
  WHEN pf.current_price_amt <= 1000.00
  THEN '$900 - $1000'
  WHEN pf.current_price_amt <= 1200.00
  THEN '$1000 - $1200'
  WHEN pf.current_price_amt <= 1500.00
  THEN '$1200 - $1500'
  WHEN pf.current_price_amt <= 1800.00
  THEN '$1500 - $1800'
  WHEN pf.current_price_amt <= 2000.00
  THEN '$1800 - $2000'
  WHEN pf.current_price_amt <= 3000.00
  THEN '$2000 - $3000'
  WHEN pf.current_price_amt <= 4000.00
  THEN '$3000 - $4000'
  WHEN pf.current_price_amt <= 5000.00
  THEN '$4000 - $5000'
  WHEN pf.current_price_amt > 5000.00
  THEN '> $5000'
  ELSE NULL
  END AS price_band_two,
 pf.product_views,
 pf.product_view_sessions,
 pf.add_to_bag_quantity,
 pf.add_to_bag_sessions,
 pf.order_demand,
 pf.order_quantity,
 pf.order_sessions,
 pf.instock_views,
 pf.scored_views
FROM pfd AS pf
 INNER JOIN sku_base ON LOWER(pf.rms_sku_num) = LOWER(sku_base.rms_sku_num) AND LOWER(pf.channelcountry) = LOWER(sku_base
    .channel_country);


CREATE TEMPORARY TABLE IF NOT EXISTS anc

AS
SELECT dept_num,
 supplier_idnt
FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.anchor_brands
WHERE LOWER(anchor_brand_ind) = LOWER('Y')
GROUP BY dept_num,
 supplier_idnt;

 
CREATE TEMPORARY TABLE IF NOT EXISTS rsb

AS
SELECT dept_num,
 supplier_idnt
FROM `{{params.gcp_project_id}}`.t2dl_das_in_season_management_reporting.rack_strategic_brands
WHERE LOWER(rack_strategic_brand_ind) = LOWER('Y')
GROUP BY dept_num,
 supplier_idnt;

CREATE TEMPORARY TABLE IF NOT EXISTS pfd_hier2

AS
SELECT a.week_end_date,
 a.fiscal_week,
 a.fiscal_month,
 a.fiscal_year,
 a.fiscal_quarter,
 a.ty_ly_ind,
 a.channelcountry,
 a.channel,
 a.platform,
 a.dept_num,
 a.supplier_idnt,
 a.class_num,
 a.sbclass_num,
 a.division,
 a.subdiv,
 a.department,
 a.class,
 a.subclass,
 a.brand_name,
 a.supplier,
 a.product_type_1,
 a.event_type,
 a.rp,
 a.mp_ind,
 a.npg_ind,
 a.price_type,
 a.price_band_one,
 a.price_band_two,
 a.product_views,
 a.product_view_sessions,
 a.add_to_bag_quantity,
 a.add_to_bag_sessions,
 a.order_demand,
 a.order_quantity,
 a.order_sessions,
 a.instock_views,
 a.scored_views,
  CASE
  WHEN anc.supplier_idnt IS NOT NULL
  THEN 1
  ELSE 0
  END AS anchor_brand_ind,
  CASE
  WHEN rsb.supplier_idnt IS NOT NULL
  THEN 1
  ELSE 0
  END AS rack_strategic_brand_ind,
 COALESCE(cat1.category, cat2.category, 'OTHER') AS category,
 COALESCE(cat1.category_group, cat2.category_group, 'OTHER') AS category_group
FROM pfd_hier AS a
 LEFT JOIN anc ON a.dept_num = anc.dept_num AND CAST(a.supplier_idnt AS FLOAT64) = anc.supplier_idnt AND LOWER(a.channel
    ) = LOWER('FULL_LINE')
 LEFT JOIN rsb ON a.dept_num = rsb.dept_num AND CAST(a.supplier_idnt AS FLOAT64) = rsb.supplier_idnt AND LOWER(a.channel
    ) = LOWER('RACK')
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat1 ON a.dept_num = cat1.dept_num AND a.class_num = CAST(cat1.class_num AS FLOAT64)
     AND a.sbclass_num = CAST(cat1.sbclass_num AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat2 ON a.dept_num = cat2.dept_num AND a.class_num = CAST(cat2.class_num AS FLOAT64)
     AND CAST(cat2.sbclass_num AS FLOAT64) = - 1;

INSERT INTO `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.divisional_summary_weekly
(SELECT week_end_date,
  fiscal_week,
  fiscal_month,
  fiscal_year,
  fiscal_quarter,
  ty_ly_ind,
  channelcountry,
  channel,
  platform,
  division,
  subdiv,
  department,
  class,
  subclass,
  brand_name,
  supplier,
  product_type_1,
  event_type,
  category,
  category_group,
  rp,
  mp_ind,
  npg_ind,
  anchor_brand_ind,
  rack_strategic_brand_ind,
  price_type,
  price_band_one,
  price_band_two,
  SUM(product_views) AS product_views,
  SUM(product_view_sessions) AS viewing_sessions,
  SUM(add_to_bag_quantity) AS add_to_bag_quantity,
  SUM(add_to_bag_sessions) AS adding_sessions,
  SUM(order_demand) AS order_demand,
  SUM(order_quantity) AS order_quantity,
  SUM(order_sessions) AS buying_sessions,
  SUM(instock_views) AS instock_views,
  SUM(scored_views) AS scored_views,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM pfd_hier2
 WHERE fiscal_week IS NOT NULL
 GROUP BY week_end_date,
  fiscal_week,
  fiscal_month,
  fiscal_year,
  fiscal_quarter,
  ty_ly_ind,
  channelcountry,
  channel,
  platform,
  division,
  subdiv,
  department,
  class,
  subclass,
  brand_name,
  supplier,
  product_type_1,
  event_type,
  rp,
  mp_ind,
  npg_ind,
  price_type,
  price_band_one,
  price_band_two,
  anchor_brand_ind,
  rack_strategic_brand_ind,
  category,
  category_group);

