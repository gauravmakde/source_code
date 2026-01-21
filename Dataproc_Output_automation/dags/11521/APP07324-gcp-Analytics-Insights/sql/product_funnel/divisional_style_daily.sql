BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;


-- SET QUERY_BAND = 'App_ID=APP08227;
--      DAG_ID=product_price_funnel_daily_11521_ACE_ENG;
--      Task_Name=divisional_style_daily;'
--      FOR SESSION VOLATILE;


-- FILENAME: divisional_style_daily.sql
-- GOAL: Roll up in-stock rates for summary dashboard in tableau
-- AUTHOR: Meghan Hickey (meghan.d.hickey@nordstrom.com)

-- full reload to restate product hierarchy

BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.divisional_style_daily;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
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
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim AS sty 
 ON sku.epm_style_num = sty.epm_style_num 
 AND LOWER(sku.channel_country) = LOWER(sty.channel_country)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim AS supp 
 ON LOWER(sku.prmy_supp_num) = LOWER(supp.vendor_num)
WHERE (sku.div_num <> 340 OR sku.class_num <> 90)
 AND sku.dept_num NOT IN (584, 585, 523);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(rms_sku_num, channel_country), column(rms_sku_num, channel_country) on sku_base;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rolling_ty
AS
SELECT day_date AS day_dt,
 week_end_day_date AS week_end_date,
 day_date_last_year_realigned,
 fiscal_year_num AS fiscal_year,
 fiscal_quarter_num,
 quarter_desc AS fiscal_quarter,
 fiscal_month_num,
 month_desc AS fiscal_month,
 fiscal_week_num AS fiscal_week,
 week_desc,
 fiscal_day_num AS fiscal_day
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_end_day_date <= DATE_ADD((SELECT MAX(event_date_pacific)
    FROM t2dl_das_product_funnel.product_funnel_daily), INTERVAL 7 DAY)
QUALIFY (DENSE_RANK() OVER (ORDER BY week_end_day_date DESC)) <= 5;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_ly
AS
SELECT day_dt,
 week_end_date,
 fiscal_day,
 fiscal_week,
 fiscal_month,
 fiscal_year,
 fiscal_quarter,
 'TY' AS ty_ly_ind
FROM rolling_ty
UNION ALL
SELECT b.day_date AS day_dt,
 b.week_end_day_date AS week_end_date,
 a.fiscal_day,
 a.fiscal_week,
 a.fiscal_month,
  a.fiscal_year - 1 AS fiscal_year,
 a.fiscal_quarter,
 'LY' AS ty_ly_ind
FROM rolling_ty AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b 
 ON a.day_date_last_year_realigned = b.day_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pfd AS
SELECT event_date_pacific,
 rms_sku_num,
 channelcountry,
 channel,
 platform,
 web_style_num AS web_style_id,
 regular_price_amt,
 current_price_amt,
 current_price_type,
 rp_ind,
 event_type,
 event_name,
 SUM(product_views) AS product_views,
 SUM(product_view_sessions) AS product_view_sessions,
 SUM(add_to_bag_quantity) AS add_to_bag_quantity,
 SUM(add_to_bag_sessions) AS add_to_bag_sessions,
 SUM(order_demand) AS order_demand,
 SUM(order_quantity) AS order_quantity,
 SUM(order_sessions) AS order_sessions,
 SUM(product_views * pct_instock) AS instock_views,
 SUM(CASE
   WHEN pct_instock IS NOT NULL
   THEN product_views
   ELSE NULL
   END) AS scored_views
FROM `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.product_price_funnel_daily AS pf
WHERE event_date_pacific >= (SELECT MIN(day_dt)
   FROM ty_ly)
GROUP BY event_date_pacific,
 rms_sku_num,
 channelcountry,
 channel,
 platform,
 web_style_id,
 regular_price_amt,
 current_price_amt,
 current_price_type,
 rp_ind,
 event_type,
 event_name;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats column(rms_sku_num, channelcountry), column(event_date_pacific) on pfd;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS filter_dates
AS
SELECT pf.event_date_pacific,
 pf.rms_sku_num,
 pf.channelcountry,
 pf.channel,
 pf.platform,
 pf.web_style_id,
 pf.regular_price_amt,
 pf.current_price_amt,
 pf.current_price_type,
 pf.rp_ind,
 pf.event_type,
 pf.event_name,
 pf.product_views,
 pf.product_view_sessions,
 pf.add_to_bag_quantity,
 pf.add_to_bag_sessions,
 pf.order_demand,
 pf.order_quantity,
 pf.order_sessions,
 pf.instock_views,
 pf.scored_views,
 ty_ly.day_dt AS activity_date,
 ty_ly.week_end_date,
 ty_ly.fiscal_day,
 ty_ly.fiscal_week,
 ty_ly.fiscal_month,
 ty_ly.fiscal_year,
 ty_ly.fiscal_quarter,
 ty_ly.ty_ly_ind
FROM pfd AS pf
 INNER JOIN ty_ly ON pf.event_date_pacific = ty_ly.day_dt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(rms_sku_num, channelcountry), column(rms_sku_num, channelcountry) on filter_dates;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pfd_sku
AS
SELECT pf.event_date_pacific AS activity_date,
 pf.week_end_date,
 pf.fiscal_day,
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
 pf.web_style_id,
 COALESCE(sku_base.style_desc, 'UNKNOWN') AS style_desc,
 COALESCE(sku_base.style_group_num, 'UNKNOWN') AS style_group_num,
 pf.event_type,
 pf.event_name,
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
  CASE
  WHEN pf.regular_price_amt > 0
  THEN 1 - (pf.current_price_amt / pf.regular_price_amt)
  ELSE NULL
  END AS discount,
 pf.product_views,
 pf.product_view_sessions,
 pf.add_to_bag_quantity,
 pf.add_to_bag_sessions,
 pf.order_demand,
 pf.order_quantity,
 pf.order_sessions,
 pf.instock_views,
 pf.scored_views
FROM filter_dates AS pf
 LEFT JOIN sku_base 
 ON LOWER(pf.rms_sku_num) = LOWER(sku_base.rms_sku_num) 
 AND LOWER(pf.channelcountry) = LOWER(sku_base.channel_country);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(activity_date, web_style_id, platform), column(activity_date), column(web_style_id) on pfd_sku;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS anc
AS
SELECT dept_num,
 supplier_idnt
FROM t2dl_das_in_season_management_reporting.anchor_brands
WHERE LOWER(anchor_brand_ind) = LOWER('Y')
GROUP BY dept_num,
 supplier_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS PRIMARY INDEX (dept_num,supplier_idnt) ON anc;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rsb
AS
SELECT dept_num,
 supplier_idnt
FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
WHERE LOWER(rack_strategic_brand_ind) = LOWER('Y')
GROUP BY dept_num,
 supplier_idnt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS PRIMARY INDEX (dept_num,supplier_idnt) ON rsb;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pfd_sku2
AS
SELECT a.activity_date,
 a.week_end_date,
 a.fiscal_day,
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
 a.web_style_id,
 a.style_desc,
 a.style_group_num,
 a.event_type,
 a.event_name,
 a.rp,
 a.mp_ind,
 a.npg_ind,
 a.price_type,
 a.price_band_one,
 a.price_band_two,
 a.discount,
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
FROM pfd_sku AS a
 LEFT JOIN anc ON a.dept_num = anc.dept_num 
 AND CAST(a.supplier_idnt AS FLOAT64) = anc.supplier_idnt 
 AND LOWER(a.channel) = LOWER('FULL_LINE')
 LEFT JOIN rsb ON a.dept_num = rsb.dept_num 
 AND CAST(a.supplier_idnt AS FLOAT64) = rsb.supplier_idnt 
 AND LOWER(a.channel) = LOWER('RACK')
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat1 
 ON a.dept_num = cat1.dept_num AND a.class_num = CAST(cat1.class_num AS FLOAT64) 
 AND a.sbclass_num = CAST(cat1.sbclass_num AS FLOAT64)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim AS cat2 
 ON a.dept_num = cat2.dept_num 
 AND a.class_num = CAST(cat2.class_num AS FLOAT64)
 AND CAST(cat2.sbclass_num AS FLOAT64) = - 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(activity_date, web_style_id, platform), column(activity_date), column(web_style_id) on pfd_sku2;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.product_funnel_t2_schema}}.divisional_style_daily
(SELECT activity_date,
  week_end_date,
  fiscal_day,
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
  web_style_id,
  style_desc,
  style_group_num,
  event_type,
  event_name,
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
  discount,
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
 FROM pfd_sku2
 GROUP BY activity_date,
  week_end_date,
  fiscal_day,
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
  web_style_id,
  style_desc,
  style_group_num,
  event_type,
  event_name,
  rp,
  mp_ind,
  npg_ind,
  price_type,
  price_band_one,
  price_band_two,
  discount,
  anchor_brand_ind,
  rack_strategic_brand_ind,
  category,
  category_group);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(activity_date, web_style_id, platform), column(activity_date) , column(web_style_id) on {{params.product_funnel_t2_schema}}.divisional_style_daily;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
