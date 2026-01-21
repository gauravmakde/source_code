



CREATE TEMPORARY TABLE IF NOT EXISTS los
AS
SELECT day_date,
 channel_country,
 channel_brand,
 sku_id AS sku_idnt,
 MAX(rp_ind) AS rp_ind,
 MAX(ds_ind) AS ds_ind,
 MAX(cl_ind) AS cl_ind,
 MAX(pm_ind) AS pm_ind,
 MAX(reg_ind) AS reg_ind,
 MAX(flash_ind) AS flash_ind
FROM `{{params.gcp_project_id}}`.t2dl_das_site_merch.live_on_site_daily AS los
WHERE day_date BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE))
GROUP BY day_date,
 channel_country,
 channel_brand,
 sku_idnt;


--COLLECT STATS      PRIMARY INDEX(day_date, sku_idnt, channel_country, channel_brand)     ,COLUMN (channel_country, channel_brand, sku_idnt)     ,COLUMN (day_date)     ON los

--DROP TABLE sales_returns;
--ORIGINAL CODE PULLS GROSS SALES AND ADDS NET LATER
--ORIGINAL CODE PULLS GROSS SALES AND ADDS NET LATER
--DATA ONLY AVAILABLE AFTER FY21
--NEED DATA PREVIOUS TO FY21


CREATE TEMPORARY TABLE IF NOT EXISTS sales_returns
AS
SELECT dt.day_date,
  CASE
  WHEN LOWER(st.business_unit_desc) = LOWER('N.CA')
  THEN 'CA'
  ELSE 'US'
  END AS channel_country,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.COM'), LOWER('OMNI.COM'), LOWER('N.CA'))
  THEN 'NORDSTROM'
  ELSE 'NORDSTROM_RACK'
  END AS channel_brand,
 fct.rms_sku_num AS sku_idnt,
 SUM(fct.returns_tot_retl) AS return_dollars,
 SUM(COALESCE(fct.net_sales_tot_retl, 0) + COALESCE(fct.returns_tot_retl, 0)) AS sales_dollars,
 SUM(fct.returns_tot_units) AS return_units,
 SUM(COALESCE(fct.net_sales_tot_units, 0) + COALESCE(fct.returns_tot_units, 0)) AS sales_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_day_columnar_vw AS fct
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dt 
 ON fct.day_num = dt.day_idnt
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st 
 ON fct.store_num = st.store_num
WHERE dt.day_date BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE))
 AND dt.day_date >= CAST('2021-01-31' AS DATE)
 AND LOWER(st.business_unit_desc) IN (LOWER('N.COM'), LOWER('OMNI.COM'), LOWER('N.CA'), LOWER('OFFPRICE ONLINE'))
 AND fct.rms_sku_num IS NOT NULL
GROUP BY dt.day_date,
 channel_country,
 channel_brand,
 sku_idnt
UNION ALL
SELECT dtl.business_day_date AS day_date,
  CASE
  WHEN LOWER(st0.business_unit_desc) = LOWER('N.CA')
  THEN 'CA'
  ELSE 'US'
  END AS channel_country,
  CASE
  WHEN LOWER(st0.business_unit_desc) IN (LOWER('N.COM'), LOWER('OMNI.COM'), LOWER('N.CA'))
  THEN 'NORDSTROM'
  ELSE 'NORDSTROM_RACK'
  END AS channel_brand,
 COALESCE(dtl.sku_num, dtl.hl_sku_num) AS sku_idnt,
  SUM(CASE
    WHEN LOWER(dtl.line_item_activity_type_code) = LOWER('R')
    THEN dtl.line_net_usd_amt
    ELSE 0
    END) * - 1 AS return_dollars,
 SUM(CASE
   WHEN LOWER(dtl.line_item_activity_type_code) = LOWER('S')
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS sales_dollars,
 SUM(CASE
   WHEN LOWER(dtl.line_item_activity_type_code) = LOWER('R')
   THEN dtl.line_item_quantity
   ELSE 0
   END) AS return_units,
 SUM(CASE
   WHEN LOWER(dtl.line_item_activity_type_code) = LOWER('S')
   THEN dtl.line_item_quantity
   ELSE 0
   END) AS sales_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st0 
 ON dtl.intent_store_num = st0.store_num
WHERE dtl.business_day_date BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE))
 AND dtl.business_day_date < CAST('2021-01-31' AS DATE)
 AND LOWER(st0.business_unit_desc) IN (LOWER('N.COM'), LOWER('OMNI.COM'), LOWER('N.CA'), LOWER('OFFPRICE ONLINE'))
 AND dtl.sku_num IS NOT NULL
GROUP BY day_date,
 channel_country,
 channel_brand,
 sku_idnt;


--COLLECT STATS      PRIMARY INDEX (day_date, sku_idnt, channel_country, channel_brand)     ,COLUMN (day_date)     ON sales_returns


-- Demand is what the customer requests. Sales is what is actually shipped to the customer.

--DROP TABLE demand;
-- merch demand does not include BOPUS (digital demand does)
--ncom orders include cancels AND exclude fraud
-- exclude same day cancels
-- filtering so we don't load all the records from prd_nap_usr_vws.product_price_dim


CREATE TEMPORARY TABLE IF NOT EXISTS demand AS 
WITH orders AS (
   SELECT order_date_pacific AS day_date,
   sku_num AS sku_idnt,
    CASE
    WHEN LOWER(source_channel_code) = LOWER('RACK')
    THEN 'NORDSTROM_RACK'
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE')
    THEN 'NORDSTROM'
    ELSE 'UNKNOWN'
    END AS channel_brand,
   source_channel_country_code AS channel_country,
   order_tmstp_pacific,
    CASE
    WHEN COALESCE(order_line_promotion_discount_amount_usd, 0) - COALESCE(order_line_employee_discount_amount_usd, 0) >
     0
    THEN 1
    ELSE 0
    END AS promo_flag,
   SUM(order_line_quantity) AS demand_units,
   SUM(order_line_current_amount_usd) AS demand_dollars
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS o
  WHERE order_date_pacific BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE))
   AND LOWER(delivery_method_code) <> LOWER('PICK')
   AND LOWER(order_type_code) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder'))
   AND LOWER(source_platform_code) <> LOWER('POS')
   AND (LOWER(source_channel_code) IN (LOWER('FULL_LINE')) 
   AND LOWER(COALESCE(fraud_cancel_ind, 'N')) = LOWER('N') 
   OR LOWER(source_channel_code) = LOWER('RACK') 
   AND order_date_pacific >= CAST('2020-11-21' AS DATE))
   AND (canceled_date_pacific IS NULL 
   OR order_date_pacific <> canceled_date_pacific)
  GROUP BY day_date,
   sku_idnt,
   channel_brand,
   channel_country,
   order_tmstp_pacific,
   promo_flag), 

   m_time AS (
    SELECT DATETIME_ADD(MAX(order_tmstp_pacific), INTERVAL 1 YEAR) AS max_timestamp
  FROM (SELECT order_date_pacific AS day_date,
     sku_num AS sku_idnt,
      CASE
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN 'NORDSTROM_RACK'
      WHEN LOWER(source_channel_code) = LOWER('FULL_LINE')
      THEN 'NORDSTROM'
      ELSE 'UNKNOWN'
      END AS channel_brand,
     source_channel_country_code AS channel_country,
     order_tmstp_pacific,
      CASE
      WHEN COALESCE(order_line_promotion_discount_amount_usd, 0) - COALESCE(order_line_employee_discount_amount_usd, 0)
       > 0
      THEN 1
      ELSE 0
      END AS promo_flag,
     SUM(order_line_quantity) AS demand_units,
     SUM(order_line_current_amount_usd) AS demand_dollars
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS o
    WHERE order_date_pacific BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE))
     AND LOWER(delivery_method_code) <> LOWER('PICK')
     AND LOWER(order_type_code) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder'))
     AND LOWER(source_platform_code) <> LOWER('POS')
     AND (LOWER(source_channel_code) IN (LOWER('FULL_LINE')) AND LOWER(COALESCE(fraud_cancel_ind, 'N')) = LOWER('N') OR
         LOWER(source_channel_code) = LOWER('RACK') AND order_date_pacific >= CAST('2020-11-21' AS DATE))
     AND (canceled_date_pacific IS NULL OR order_date_pacific <> canceled_date_pacific)
    GROUP BY day_date,
     sku_idnt,
     channel_brand,
     channel_country,
     order_tmstp_pacific,
     promo_flag) AS t5), 
     
     price_type AS (
    SELECT DISTINCT p.rms_sku_num AS sku_idnt,
   p.channel_country,
   p.channel_brand,
   p.selling_retail_price_type_code AS current_price_type,
   p.eff_begin_tmstp,
   p.eff_end_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS p
   INNER JOIN m_time AS x ON 
   LOWER(p.selling_channel) = LOWER('ONLINE')
   AND (CAST(p.eff_begin_tmstp AS DATETIME) < x.max_timestamp OR CAST(p.eff_end_tmstp AS DATETIME) < x.max_timestamp)
   ) 
   (SELECT
   o.day_date,
   o.sku_idnt,
   o.channel_brand,
   o.channel_country,
   SUM(o.demand_units) AS demand_units,
   SUM(o.demand_dollars) AS demand_dollars,
   SUM(CASE
     WHEN LOWER(p.current_price_type) = LOWER('REGULAR') 
     AND o.promo_flag = 0
     THEN o.demand_units
     ELSE 0
     END) AS demand_units_reg,
   SUM(CASE
     WHEN LOWER(p.current_price_type) = LOWER('REGULAR') 
     AND o.promo_flag = 0
     THEN o.demand_dollars
     ELSE 0
     END) AS demand_dollars_reg
  FROM orders AS o
   LEFT JOIN price_type AS p 
   ON LOWER(o.sku_idnt) = LOWER(p.sku_idnt) 
   AND o.order_tmstp_pacific BETWEEN CAST(p.eff_begin_tmstp AS DATETIME)
       AND (CAST(p.eff_end_tmstp AS DATETIME)) 
       AND LOWER(o.channel_country) = LOWER(p.channel_country) AND LOWER(o.channel_brand) = LOWER(p.channel_brand)
  GROUP BY o.day_date,
   o.sku_idnt,
   o.channel_brand,
   o.channel_country);


--COLLECT STATS      PRIMARY INDEX(day_date, sku_idnt, channel_brand, channel_country)      ON demand


-- Note: Session data is all zero as of 6/15/22


--DROP TABLE digital_funnel;


CREATE TEMPORARY TABLE IF NOT EXISTS digital_funnel
AS
SELECT event_date_pacific AS day_date,
 channelcountry AS channel_country,
  CASE
  WHEN LOWER(channel) = LOWER('RACK')
  THEN 'NORDSTROM_RACK'
  ELSE 'NORDSTROM'
  END AS channel_brand,
 rms_sku_num AS sku_idnt,
 SUM(order_sessions) AS order_sessions,
 SUM(add_to_bag_quantity) AS add_qty,
 SUM(add_to_bag_sessions) AS add_sessions,
 SUM(product_views) AS product_views,
 SUM(product_view_sessions) AS product_view_sessions,
 SUM(product_views * pct_instock) AS instock_views,
 SUM(CASE
   WHEN LOWER(current_price_type) = LOWER('R')
   THEN order_sessions
   ELSE 0
   END) AS order_sessions_reg,
 SUM(CASE
   WHEN LOWER(current_price_type) = LOWER('R')
   THEN add_to_bag_quantity
   ELSE 0
   END) AS add_qty_reg,
 SUM(CASE
   WHEN LOWER(current_price_type) = LOWER('R')
   THEN add_to_bag_sessions
   ELSE 0
   END) AS add_sessions_reg,
 SUM(CASE
   WHEN LOWER(current_price_type) = LOWER('R')
   THEN product_views
   ELSE 0
   END) AS product_views_reg,
 SUM(CASE
   WHEN LOWER(current_price_type) = LOWER('R')
   THEN product_view_sessions
   ELSE 0
   END) AS product_view_sessions_reg,
 SUM(CASE
   WHEN LOWER(current_price_type) = LOWER('R')
   THEN product_views * pct_instock
   ELSE 0
   END) AS instock_views_reg
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS f
WHERE event_date_pacific BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE))
 AND LOWER(rms_sku_num) <> LOWER('UNKNOWN')
 AND LOWER(channel) <> LOWER('UNKNOWN')
GROUP BY day_date,
 channel_country,
 channel_brand,
 sku_idnt;


--COLLECT STATS      PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)     ,COLUMN (day_date)     ON digital_funnel


--DROP TABLE funnel;


CREATE TEMPORARY TABLE IF NOT EXISTS funnel
AS
SELECT COALESCE(f.day_date, d.day_date) AS day_date,
 COALESCE(f.channel_country, d.channel_country) AS channel_country,
 COALESCE(f.channel_brand, d.channel_brand) AS channel_brand,
 COALESCE(f.sku_idnt, d.sku_idnt) AS sku_idnt,
 COALESCE(d.demand_units, 0) AS demand_units,
 COALESCE(d.demand_dollars, 0) AS demand_dollars,
 COALESCE(f.order_sessions, 0) AS order_sessions,
 COALESCE(f.add_qty, 0) AS add_qty,
 COALESCE(f.add_sessions, 0) AS add_sessions,
 COALESCE(f.product_views, 0) AS product_views,
 COALESCE(f.product_view_sessions, 0) AS product_view_sessions,
 COALESCE(f.instock_views, 0) AS instock_views,
 COALESCE(d.demand_units_reg, 0) AS demand_units_reg,
 COALESCE(d.demand_dollars_reg, 0) AS demand_dollars_reg,
 COALESCE(f.order_sessions_reg, 0) AS order_sessions_reg,
 COALESCE(f.add_qty_reg, 0) AS add_qty_reg,
 COALESCE(f.add_sessions_reg, 0) AS add_sessions_reg,
 COALESCE(f.product_views_reg, 0) AS product_views_reg,
 COALESCE(f.product_view_sessions_reg, 0) AS product_view_sessions_reg,
 COALESCE(f.instock_views_reg, 0) AS instock_views_reg
FROM digital_funnel AS f
 FULL JOIN demand AS d 
 ON LOWER(f.sku_idnt) = LOWER(d.sku_idnt) 
 AND f.day_date = d.day_date 
 AND LOWER(f.channel_brand) =LOWER(d.channel_brand) 
 AND LOWER(f.channel_country) = LOWER(d.channel_country);


--COLLECT STATS      PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)     ,COLUMN (day_date)     ON funnel
--DROP TABLE metrics;
-- funnel
-- reg price funnel
-- funnel
-- reg price funnel
-- sales returns


CREATE TEMPORARY TABLE IF NOT EXISTS metrics AS 
WITH los_funnel AS (
  SELECT 
   COALESCE(l.day_date, f.day_date) AS day_date,
   COALESCE(l.channel_country, f.channel_country) AS channel_country,
   COALESCE(l.channel_brand, f.channel_brand) AS channel_brand,
   COALESCE(l.sku_idnt, f.sku_idnt) AS sku_idnt,
    CASE
    WHEN l.sku_idnt IS NOT NULL
    THEN 1
    ELSE 0
    END AS los_flag,
   l.rp_ind,
   l.ds_ind,
   l.cl_ind,
   l.pm_ind,
   l.reg_ind,
   l.flash_ind,
   COALESCE(f.demand_units, 0) AS demand_units,
   COALESCE(f.demand_dollars, 0) AS demand_dollars,
   COALESCE(f.order_sessions, 0) AS order_sessions,
   COALESCE(f.add_qty, 0) AS add_qty,
   COALESCE(f.add_sessions, 0) AS add_sessions,
   COALESCE(f.product_views, 0) AS product_views,
   COALESCE(f.product_view_sessions, 0) AS product_view_sessions,
   COALESCE(f.instock_views, 0) AS instock_views,
   COALESCE(f.demand_units_reg, 0) AS demand_units_reg,
   COALESCE(f.demand_dollars_reg, 0) AS demand_dollars_reg,
   COALESCE(f.order_sessions_reg, 0) AS order_sessions_reg,
   COALESCE(f.add_qty_reg, 0) AS add_qty_reg,
   COALESCE(f.add_sessions_reg, 0) AS add_sessions_reg,
   COALESCE(f.product_views_reg, 0) AS product_views_reg,
   COALESCE(f.product_view_sessions_reg, 0) AS product_view_sessions_reg,
   COALESCE(f.instock_views_reg, 0) AS instock_views_reg
  FROM los AS l
   FULL JOIN funnel AS f 
   ON l.day_date = f.day_date 
   AND LOWER(l.channel_country) = LOWER(f.channel_country) 
   AND LOWER(l.channel_brand) = LOWER(f.channel_brand) 
   AND LOWER(l.sku_idnt) = LOWER(f.sku_idnt)
   ) 
   (SELECT 
   COALESCE(l.day_date,sr.day_date) AS day_date,
   COALESCE(l.channel_country, sr.channel_country) AS channel_country,
   COALESCE(l.channel_brand, sr.channel_brand) AS channel_brand,
   COALESCE(l.sku_idnt, sr.sku_idnt) AS sku_idnt,
   COALESCE(l.los_flag, 0) AS los_flag,
   l.rp_ind,
   l.ds_ind,
   l.cl_ind,
   l.pm_ind,
   l.reg_ind,
   l.flash_ind,
   COALESCE(l.demand_units, 0) AS demand_units,
   COALESCE(l.demand_dollars, 0) AS demand_dollars,
   COALESCE(l.order_sessions, 0) AS order_sessions,
   COALESCE(l.add_qty, 0) AS add_qty,
   COALESCE(l.add_sessions, 0) AS add_sessions,
   COALESCE(l.product_views, 0) AS product_views,
   COALESCE(l.product_view_sessions, 0) AS product_view_sessions,
   COALESCE(l.instock_views, 0) AS instock_views,
   COALESCE(l.demand_units_reg, 0) AS demand_units_reg,
   COALESCE(l.demand_dollars_reg, 0) AS demand_dollars_reg,
   COALESCE(l.order_sessions_reg, 0) AS order_sessions_reg,
   COALESCE(l.add_qty_reg, 0) AS add_qty_reg,
   COALESCE(l.add_sessions_reg, 0) AS add_sessions_reg,
   COALESCE(l.product_views_reg, 0) AS product_views_reg,
   COALESCE(l.product_view_sessions_reg, 0) AS product_view_sessions_reg,
   COALESCE(l.instock_views_reg, 0) AS instock_views_reg,
   COALESCE(sr.return_dollars, 0) AS return_dollars,
   COALESCE(sr.sales_dollars, 0) AS sales_dollars,
   COALESCE(sr.return_units, 0) AS return_units,
   COALESCE(sr.sales_units, 0) AS sales_units,
    COALESCE(sr.sales_dollars, 0) - COALESCE(sr.return_dollars, 0) AS sales_net_dollars,
    COALESCE(sr.sales_units, 0) - COALESCE(sr.return_units, 0) AS sales_net_units
  FROM los_funnel AS l
   FULL JOIN sales_returns AS sr 
   ON l.day_date = sr.day_date 
   AND LOWER(l.channel_country) = LOWER(sr.channel_country)
   AND LOWER(l.channel_brand) = LOWER(sr.channel_brand) 
   AND LOWER(l.sku_idnt) = LOWER(sr.sku_idnt));


--COLLECT STATS      PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)     ,COLUMN (day_date)     ON metrics


--{env_suffix}


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_planning_daily_base{{params.env_suffix}}
WHERE day_date BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE));


--{env_suffix}
--funnel
--reg price funnel
--Sales & Returns
--funnel
--reg price funnel
--Sales & Returns


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_planning_daily_base{{params.env_suffix}}
(
day_date,
sku_idnt,
customer_choice,
wk_idnt,
wk_454,
week_of_fyr,
mnth_454,
mnth_idnt,
qtr_454,
yr_454,
channel_country,
channel_brand,
cc_type,
los_flag,
rp_ind,
ds_ind,
cl_ind,
pm_ind,
reg_ind,
flash_ind,
demand_units,
demand_dollars,
order_sessions,
add_qty,
add_sessions,
product_views,
product_view_sessions,
instock_views,
demand_units_reg,
demand_dollars_reg,
order_sessions_reg,
add_qty_reg,
add_sessions_reg,
product_views_reg,
product_view_sessions_reg,
instock_views_reg,
return_dollars,
sales_dollars,
return_units,
sales_units,
sales_net_dollars,
sales_net_units,
update_timestamp,
update_timestamp_tz
)
WITH metrics_receipt AS (SELECT 
 day_date,
 sku_idnt,
 channel_country,
 channel_brand,
 'DIGITAL' AS channel,
 los_flag,
 rp_ind,
 ds_ind,
 cl_ind,
 pm_ind,
 reg_ind,
 flash_ind,
 demand_units,
 demand_dollars,
 order_sessions,
 add_qty,
 add_sessions,
 product_views,
 product_view_sessions,
 instock_views,
 demand_units_reg,
 demand_dollars_reg,
 order_sessions_reg,
 add_qty_reg,
 add_sessions_reg,
 product_views_reg,
 product_view_sessions_reg,
 instock_views_reg,
 return_dollars,
 sales_dollars,
 cast(trunc(return_units) as int64) as return_units,
 cast(trunc(sales_units) as int64) as sales_units ,
 sales_net_dollars,
 cast(trunc(sales_net_units) as int64) as sales_net_units ,
 timestamp(current_datetime('PST8PDT')) AS update_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz
FROM metrics AS l)
SELECT a.day_date,
 a.sku_idnt,
 cc.customer_choice,
 cal.week_num AS wk_idnt,
 cal.week_454_num AS wk_454,
 cal.week_of_fyr,
 cal.month_454_num AS mnth_454,
 cal.month_num AS mnth_idnt,
 cal.quarter_454_num AS qtr_454,
 cal.year_num AS yr_454,
 a.channel_country,
 a.channel_brand,
 COALESCE(cct.cc_type, 'CF') AS cc_type,
 a.los_flag,
 a.rp_ind,
 a.ds_ind,
 a.cl_ind,
 a.pm_ind,
 a.reg_ind,
 a.flash_ind,
 a.demand_units,
 a.demand_dollars,
 a.order_sessions,
 a.add_qty,
 a.add_sessions,
 a.product_views,
 a.product_view_sessions,
 a.instock_views,
 a.demand_units_reg,
 a.demand_dollars_reg,
 a.order_sessions_reg,
 a.add_qty_reg,
 a.add_sessions_reg,
 a.product_views_reg,
 a.product_view_sessions_reg,
 a.instock_views_reg,
 a.return_dollars,
 a.sales_dollars,
 a.return_units,
 a.sales_units,
 a.sales_net_dollars,
 a.sales_net_units,
 timestamp(current_datetime('PST8PDT')) AS update_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz
FROM metrics_receipt AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal AS cal 
 ON a.day_date = cal.day_date
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS cc 
 ON LOWER(a.sku_idnt) = LOWER(cc.sku_idnt) 
 AND LOWER(a.channel_country) = LOWER(cc.channel_country)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.cc_type_realigned AS cct 
 ON LOWER(cc.customer_choice) = LOWER(cct.customer_choice)
 AND LOWER(a.channel_country) = LOWER(cct.channel_country) 
 AND LOWER(a.channel_brand) = LOWER(cct.channel_brand)
    AND (LOWER(CASE
         WHEN LOWER(a.channel_country) = LOWER('US')
         THEN a.channel
         ELSE NULL
         END) = LOWER(cct.channel) 
         OR LOWER(CASE
         WHEN LOWER(a.channel_country) = LOWER('CA')
         THEN 'STORE'
         ELSE NULL
         END) = LOWER(cct.channel)) 
         AND LOWER(cct.channel) = LOWER('DIGITAL') 
         AND cal.month_num = cct.mnth_idnt
WHERE cal.day_date BETWEEN CAST({{params.start_date}} AS DATE) AND (CAST({{params.end_date}} AS DATE));


--{env_suffix}


--COLLECT STATS      COLUMN (day_date)     ,COLUMN (day_date,sku_idnt,customer_choice,channel_country,channel_brand)     ON T2DL_DAS_SELECTION.selection_planning_daily_base  


-- end


/*
STEP 2: Create Daily Table
    - At day/channel_country/channel_brand/dept/category level
    - daily_hierarchy: join selection_planning_daily_base and hierarchy
*/
-- daily sku level with calendar and hierarchy data
--DROP TABLE daily_hierarchy;
-- cat_idnt created to easily join in python with numbers instead of varchars
--{env_suffix}
-- do not include fanatics skus
-- do not include marketplace skus


CREATE TEMPORARY TABLE IF NOT EXISTS daily_hierarchy AS 
WITH hrchy AS (
  SELECT * ,
   DENSE_RANK() OVER (PARTITION BY dept_idnt ORDER BY COALESCE(quantrix_category, 'OTHER')) AS cat_idnt
  FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS h) 
  (SELECT base.day_date,
   base.sku_idnt,
   base.customer_choice,
   base.wk_idnt,
   base.wk_454,
   base.week_of_fyr,
   base.mnth_454,
   base.mnth_idnt,
   base.qtr_454,
   base.yr_454,
   base.channel_country,
   base.channel_brand,
   base.cc_type,
   base.los_flag,
   base.rp_ind,
   base.ds_ind,
   base.cl_ind,
   base.pm_ind,
   base.reg_ind,
   base.flash_ind,
   base.demand_units,
   base.demand_dollars,
   base.order_sessions,
   base.add_qty,
   base.add_sessions,
   base.product_views,
   base.product_view_sessions,
   base.instock_views,
   base.demand_units_reg,
   base.demand_dollars_reg,
   base.order_sessions_reg,
   base.add_qty_reg,
   base.add_sessions_reg,
   base.product_views_reg,
   base.product_view_sessions_reg,
   base.instock_views_reg,
   base.return_dollars,
   base.sales_dollars,
   base.return_units,
   base.sales_units,
   base.sales_net_dollars,
   base.sales_net_units,
   base.update_timestamp,
   base.update_timestamp_tz,
   h.supplier_idnt,
   h.div_idnt,
   h.div_desc,
   h.grp_idnt,
   h.grp_desc,
   h.dept_idnt,
   h.dept_desc,
   h.class_idnt,
   h.class_desc,
   h.sbclass_desc,
   h.sbclass_idnt,
   h.quantrix_category AS category,
   h.cat_idnt,
   cal.day_idnt,
   cal.month_start_day_idnt,
   cal.month_end_day_idnt,
   cal.month_start_day_date AS mnth_start_day_date
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_planning_daily_base{{params.env_suffix}}
  AS base
   INNER JOIN hrchy AS h 
   ON LOWER(base.sku_idnt) = LOWER(h.sku_idnt) 
   AND LOWER(base.channel_country) = LOWER(h.channel_country
      )
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS cal 
   ON base.day_date = cal.day_date
  WHERE cal.month_idnt >= 201901
   AND cal.month_idnt <= (SELECT DISTINCT month_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE day_date = CURRENT_DATE('PST8PDT'))
   AND h.fanatics_ind = 0
   AND h.marketplace_ind = 0);


--COLLECT STATS     COLUMN (customer_choice, mnth_454, mnth_idnt, qtr_454 ,yr_454 ,channel_country ,channel_brand, div_idnt, grp_idnt, dept_idnt, category, cat_idnt)     ,COLUMN (customer_choice, mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt, category, cat_idnt, mnth_start_day_date)     ,COLUMN (day_date, customer_choice, wk_idnt, wk_454, week_of_fyr, mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt, category, cat_idnt)     ,COLUMN (mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt ,cat_idnt)     ,COLUMN (day_date, wk_idnt, wk_454, week_of_fyr, mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt, category, cat_idnt)     ,COLUMN (mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt, cat_idnt, mnth_start_day_date)     ,COLUMN (mnth_start_day_date)     ,COLUMN (sku_idnt, customer_choice)     ,COLUMN (cat_idnt)     ,COLUMN (category)     ,COLUMN (channel_country)     ,COLUMN (customer_choice)     ,COLUMN (day_date)     ,COLUMN (dept_idnt)     ,COLUMN (div_idnt)     ,COLUMN (grp_idnt)     ,COLUMN (mnth_idnt)     ,COLUMN (mnth_454)     ,COLUMN (qtr_454)     ,COLUMN (wk_454)     ,COLUMN (wk_idnt)     ,COLUMN (week_of_fyr)     ,COLUMN (yr_454)     ON daily_hierarchy


-- daily CC level 
--{env_suffix}


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_planning_daily{{params.env_suffix}};


--{env_suffix}
--choice counts


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_planning_daily{{params.env_suffix}}
(SELECT day_date,
  wk_idnt,
  wk_454,
  week_of_fyr,
  mnth_454,
  mnth_idnt,
  qtr_454,
  yr_454,
  channel_country,
  channel_brand,
  div_idnt,
  grp_idnt,
  dept_idnt,
  cat_idnt,
  category,
  COUNT(DISTINCT customer_choice) AS customer_choices,
  COUNT(DISTINCT CASE
    WHEN los_flag = 1
    THEN customer_choice
    ELSE NULL
    END) AS customer_choices_los,
  COUNT(DISTINCT CASE
    WHEN LOWER(cc_type) = LOWER('NEW')
    THEN customer_choice
    ELSE NULL
    END) AS customer_choices_new,
  COUNT(DISTINCT CASE
    WHEN day_idnt = month_start_day_idnt AND los_flag = 1
    THEN customer_choice
    ELSE NULL
    END) AS bop_est,
  COUNT(DISTINCT CASE
    WHEN day_idnt = month_end_day_idnt AND los_flag = 1
    THEN customer_choice
    ELSE NULL
    END) AS eop_est,
  timestamp(current_datetime('PST8PDT')) AS update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz
 FROM daily_hierarchy
 GROUP BY day_date,
  wk_idnt,
  wk_454,
  week_of_fyr,
  mnth_454,
  mnth_idnt,
  qtr_454,
  yr_454,
  channel_country,
  channel_brand,
  div_idnt,
  grp_idnt,
  dept_idnt,
  cat_idnt,
  category);


--{env_suffix}


--COLLECT STATS      PRIMARY INDEX(day_date, mnth_454, yr_454, dept_idnt, cat_idnt)      ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt)       ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt, cat_idnt)       ,COLUMN (cat_idnt)       ,COLUMN (dept_idnt)      ,COLUMN (yr_454)       ,COLUMN (mnth_454)      ON T2DL_DAS_SELECTION.selection_planning_daily  


/*
STEP 3: Create Monthly Table
    - At month/channel_country/channel_brand/dept/category level
    - bop_cnts: beginning of month cc counts for dept/category
    - eop_cnts: end of month cc counts for dept/category
    - cc_bop_eop_cnts: join bop_cnts and eop_cnts
    - monthly: monthly metric aggregation of daily_hierarchy
    - missing_pvs: fill in missing product views where sku_idnt is missing
*/


--Realigned calendar 


CREATE TEMPORARY TABLE IF NOT EXISTS calendar_realigned
AS
SELECT day_date,
 week_idnt,
 month_idnt,
 week_end_day_date,
 week_start_day_date,
 month_start_day_date,
 month_end_day_date,
 fiscal_month_num AS mnth_454,
 fiscal_quarter_num AS qtr_454,
 fiscal_year_num AS yr_454
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw AS rdlv
WHERE day_date <= current_date('PST8PDT')

UNION ALL

SELECT day_date,
 week_idnt,
 month_idnt,
 week_end_day_date,
 week_start_day_date,
 month_start_day_date,
 month_end_day_date,
 fiscal_month_num AS mnth_454,
 fiscal_quarter_num AS qtr_454,
 fiscal_year_num AS yr_454
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE day_date <= current_date('PST8PDT')
 AND fiscal_year_num NOT IN (SELECT DISTINCT fiscal_year_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw);


CREATE TEMPORARY TABLE IF NOT EXISTS calendar
AS
SELECT cal.day_date,
 cal.week_idnt,
 cal.month_idnt,
 cal.week_end_day_date,
 cal.week_start_day_date,
 cal.month_start_day_date,
 cal.month_end_day_date,
 cal.mnth_454,
 cal.qtr_454,
 cal.yr_454,
 t.day_date AS day_date_true,
 t.week_idnt AS week_idnt_true,
 t.month_idnt AS month_idnt_true,
 t.month_end_day_date AS month_end_day_date_true,
 t.week_end_day_date AS week_end_day_date_true
FROM calendar_realigned AS cal
 FULL JOIN 
 (SELECT day_date,
   day_idnt,
   week_idnt,
   month_idnt,
   month_end_day_date,
   week_end_day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
  WHERE fiscal_year_num >= 2019
   AND day_date <= current_date('PST8PDT')) AS t 
   ON cal.day_date = t.day_date;


--DROP TABLE bop_cnts;
-- get status
-- get calendar dates
-- get customer_choice
-- get dept and category
-- data starts on 2021-05-14
-- do not include fanatics skus
-- do not include marketplace skus


CREATE TEMPORARY TABLE IF NOT EXISTS bop_cnts AS
 WITH bop_status AS 
 (SELECT cc.customer_choice,
   h.dept_idnt,
   h.quantrix_category AS category,
   sku.channel_country,
   sku.channel_brand,
   c.month_idnt AS mnth_idnt,
    CASE
    WHEN LOWER(sku.is_online_purchasable) = LOWER('Y') 
    AND CAST(sku.eff_begin_tmstp AS DATETIME) <= CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(c.day_date AS DATETIME)) AS DATETIME)
    THEN 1
    ELSE 0
    END AS bop_status
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_online_purchasable_item_dim AS sku
   INNER JOIN calendar AS c 
   ON c.day_date BETWEEN CAST(sku.eff_begin_tmstp AS DATE) 
   AND (CAST(sku.eff_end_tmstp AS DATE)
     )
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS cc 
   ON LOWER(sku.rms_sku_num) = LOWER(cc.sku_idnt) 
   AND LOWER(sku.channel_country) = LOWER(cc.channel_country)
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS h ON LOWER(sku.rms_sku_num) = LOWER(h.sku_idnt) 
   AND LOWER(cc.customer_choice) = LOWER(h.customer_choice) 
   AND LOWER(sku.channel_country) = LOWER(h.channel_country)
  WHERE c.month_idnt BETWEEN 202104 AND (SELECT DISTINCT month_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE day_date = current_date('PST8PDT'))
   AND c.day_date = c.month_start_day_date
   AND h.fanatics_ind = 0
   AND h.marketplace_ind = 0
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY sku.rms_sku_num, sku.channel_country, sku.channel_brand, c.day_date ORDER BY
       sku.eff_begin_tmstp)) = 1
       ) 
       (SELECT dept_idnt,
   category,
   channel_country,
   channel_brand,
   mnth_idnt,
   COUNT(DISTINCT CASE
     WHEN a1.bop_status = 1
     THEN customer_choice
     ELSE NULL
     END) AS ccs_mnth_bop
  FROM bop_status a1
  GROUP BY dept_idnt,
   category,
   channel_country,
   channel_brand,
   mnth_idnt);


--COLLECT STATS 	PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt) 	ON bop_cnts
--DROP TABLE eop_cnts;
-- get status
-- get calendar dates
-- get customer_choice
-- get dept and category
-- data starts on 2021-05-14
-- do not include fanatics skus
-- do not include marketplace skus


CREATE TEMPORARY TABLE IF NOT EXISTS eop_cnts AS 
WITH eop_status AS (
  SELECT cc.customer_choice,
   h.dept_idnt,
   h.quantrix_category AS category,
   sku.channel_country,
   sku.channel_brand,
   c.month_idnt AS mnth_idnt,
    CASE
    WHEN LOWER(sku.is_online_purchasable) = LOWER('Y')
    THEN 1
    ELSE 0
    END AS eop_status
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_online_purchasable_item_dim AS sku
   INNER JOIN calendar AS c 
   ON c.day_date BETWEEN CAST(sku.eff_begin_tmstp AS DATE) 
   AND (CAST(sku.eff_end_tmstp AS DATE))
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS cc 
   ON LOWER(sku.rms_sku_num) = LOWER(cc.sku_idnt) 
   AND LOWER(sku.channel_country) = LOWER(cc.channel_country)
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS h ON LOWER(sku.rms_sku_num) = LOWER(h.sku_idnt) 
   AND LOWER(cc.customer_choice) = LOWER(h.customer_choice) 
   AND LOWER(sku.channel_country) = LOWER(h.channel_country)
  WHERE c.month_idnt BETWEEN 202104 AND 
  (SELECT DISTINCT month_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE day_date = current_date('PST8PDT'))
   AND c.day_date = c.month_end_day_date
   AND h.fanatics_ind = 0
   AND h.marketplace_ind = 0
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY sku.rms_sku_num, sku.channel_country, sku.channel_brand, c.day_date ORDER BY
       sku.eff_begin_tmstp DESC)) = 1
       ) 
  (SELECT dept_idnt,
   category,
   channel_country,
   channel_brand,
   mnth_idnt,
   COUNT(DISTINCT CASE
     WHEN b1.eop_status = 1
     THEN customer_choice
     ELSE NULL
     END) AS ccs_mnth_eop
  FROM eop_status b1
  GROUP BY dept_idnt,
   category,
   channel_country,
   channel_brand,
   mnth_idnt);


--COLLECT STATS 	PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt) 	ON eop_cnts

--DROP TABLE cc_bop_eop_cnts;
-- always zero for for first month that appears


CREATE TEMPORARY TABLE IF NOT EXISTS cc_bop_eop_cnts
AS
SELECT e.dept_idnt,
 e.category,
 e.channel_country,
 e.channel_brand,
 e.mnth_idnt,
 COALESCE(b.ccs_mnth_bop, 0) AS ccs_mnth_bop,
 e.ccs_mnth_eop
FROM bop_cnts AS b
 FULL JOIN eop_cnts AS e 
 ON b.dept_idnt = e.dept_idnt 
 AND LOWER(b.category) = LOWER(e.category) 
 AND LOWER(b.channel_country) = LOWER(e.channel_country) 
 AND LOWER(b.channel_brand) = LOWER(e.channel_brand) 
 AND b.mnth_idnt = e.mnth_idnt;


--COLLECT STATS   PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt)   ON cc_bop_eop_cnts
--Break out receipts into groupings, blend MADM with NAP to get full history, and roll up to month level
--NAP receipts from Feb Week 1 2023 onward
-- DIGITAL only
-- removing Reserve Stock (260: RACK RESERVE STOCK WAREHOUSE, 310: RESERVE STOCK), DCs (920: US DC, 921: Canada DC, 922: OP Canada DC), 930: NQC, 940: NPG, 990: FACO
--MADM receipts prior to Feb Week 1 2023 (MADM table only contains dates from Feb Week 1 2019 through Jan Week 4 2023)
-- DIGITAL only
-- removing Reserve Stock (260: RACK RESERVE STOCK WAREHOUSE, 310: RESERVE STOCK), DCs (920: US DC, 921: Canada DC, 922: OP Canada DC), 930: NQC, 940: NPG, 990: FACO
-- cat_idnt created to easily join in python with numbers instead of varchars
--removing fanatics skus from receipts for selection planning
-- do not include marketplace skus


CREATE TEMPORARY TABLE IF NOT EXISTS receipts AS 
WITH dates AS (
  SELECT week_idnt_true,
   month_idnt AS mnth_idnt,
   month_start_day_date AS mnth_start_day_date,
   mnth_454,
   qtr_454,
   yr_454
  FROM calendar
  WHERE month_idnt >= 201901
  GROUP BY week_idnt_true,
   mnth_idnt,
   mnth_start_day_date,
   mnth_454,
   qtr_454,
   yr_454), 
   
   receipts_base AS (
    SELECT 
    d.mnth_idnt,
    d.mnth_start_day_date,
    d.mnth_454,
    d.qtr_454,
    d.yr_454,
    st.channel_country,
    st.channel_brand,
    r.sku_idnt,
    SUM(r.receipt_po_units + r.receipt_ds_units) AS rcpt_units,
    SUM(r.receipt_po_retail + r.receipt_ds_retail) AS rcpt_dollars,
    SUM(CASE
      WHEN r.receipt_po_units > 0
      THEN r.receipt_po_units
      ELSE 0
      END) AS rcpt_bought_units,
    SUM(CASE
      WHEN r.receipt_po_units > 0
      THEN r.receipt_po_retail
      ELSE 0
      END) AS rcpt_bought_dollars,
    SUM(CASE
      WHEN r.receipt_ds_units > 0 AND COALESCE(r.receipt_po_units, 0) = 0
      THEN r.receipt_ds_units
      ELSE 0
      END) AS rcpt_ds_units,
    SUM(CASE
      WHEN r.receipt_ds_units > 0 AND COALESCE(r.receipt_po_units, 0) = 0
      THEN r.receipt_ds_retail
      ELSE 0
      END) AS rcpt_ds_dollars
   FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact AS r
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st 
    ON r.store_num = st.store_num
    INNER JOIN dates AS d 
       ON r.week_num = d.week_idnt_true
   WHERE LOWER(st.selling_channel) = LOWER('ONLINE')
    AND d.mnth_idnt >= 202301
    AND r.channel_num NOT IN (260, 310, 920, 921, 922, 930, 940, 990)
   GROUP BY d.mnth_idnt,
    d.mnth_start_day_date,
    d.mnth_454,
    d.qtr_454,
    d.yr_454,
    st.channel_country,
    st.channel_brand,
    r.sku_idnt

   UNION ALL

   SELECT d.mnth_idnt,
    d.mnth_start_day_date,
    d.mnth_454,
    d.qtr_454,
    d.yr_454,
    st0.channel_country,
    st0.channel_brand,
    r0.sku_idnt,
    SUM(r0.receipt_po_units + r0.receipt_ds_units) AS rcpt_units,
    SUM(r0.receipt_po_retail + r0.receipt_ds_retail) AS rcpt_dollars,
    SUM(CASE
      WHEN r0.receipt_po_units > 0
      THEN r0.receipt_po_units
      ELSE 0
      END) AS rcpt_bought_units,
    SUM(CASE
      WHEN r0.receipt_po_units > 0
      THEN r0.receipt_po_retail
      ELSE 0
      END) AS rcpt_bought_dollars,
    SUM(CASE
      WHEN r0.receipt_ds_units > 0 AND COALESCE(r0.receipt_po_units, 0) = 0
      THEN r0.receipt_ds_units
      ELSE 0
      END) AS rcpt_ds_units,
    SUM(CASE
      WHEN r0.receipt_ds_units > 0 AND COALESCE(r0.receipt_po_units, 0) = 0
      THEN r0.receipt_ds_retail
      ELSE 0
      END) AS rcpt_ds_dollars
   FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact_madm AS r0
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st0 
    ON r0.store_num = st0.store_num
    INNER JOIN dates AS d 
       ON r0.week_num = d.week_idnt_true
   WHERE LOWER(st0.selling_channel) = LOWER('ONLINE')
    AND (d.mnth_idnt >= 201901 
    AND d.mnth_idnt < 202301)
    AND r0.channel_num NOT IN (260, 310, 920, 921, 922, 930, 940, 990)
   GROUP BY d.mnth_idnt,
    d.mnth_start_day_date,
    d.mnth_454,
    d.qtr_454,
    d.yr_454,
    st0.channel_country,
    st0.channel_brand,
    r0.sku_idnt), 
  hrchy AS (
    SELECT * ,
   DENSE_RANK() OVER (PARTITION BY dept_idnt ORDER BY COALESCE(quantrix_category, 'OTHER')) AS cat_idnt
  FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy AS h) (SELECT r.mnth_idnt,
   r.mnth_start_day_date,
   r.mnth_454,
   r.qtr_454,
   r.yr_454,
   r.channel_country,
   r.channel_brand,
   h.div_idnt,
   h.grp_idnt,
   h.dept_idnt,
   h.class_idnt,
   h.cat_idnt,
   COALESCE(h.quantrix_category, 'UNKNOWN') AS category,
   h.customer_choice,
   COALESCE(cc_type.cc_type, 'CF') AS cc_type,
   r.sku_idnt,
   r.rcpt_units,
   r.rcpt_dollars,
   r.rcpt_bought_units,
   r.rcpt_bought_dollars,
   r.rcpt_ds_units,
   r.rcpt_ds_dollars,
    CASE
    WHEN LOWER(COALESCE(cc_type.cc_type, 'CF')) = LOWER('NEW')
    THEN r.rcpt_bought_units
    ELSE 0
    END AS rcpt_new_units,
    CASE
    WHEN LOWER(COALESCE(cc_type.cc_type, 'CF')) = LOWER('NEW')
    THEN r.rcpt_bought_dollars
    ELSE 0
    END AS rcpt_new_dollars,
    CASE
    WHEN LOWER(COALESCE(cc_type.cc_type, 'CF')) = LOWER('CF')
    THEN r.rcpt_bought_units
    ELSE 0
    END AS rcpt_cf_units,
    CASE
    WHEN LOWER(COALESCE(cc_type.cc_type, 'CF')) = LOWER('CF')
    THEN r.rcpt_bought_dollars
    ELSE 0
    END AS rcpt_cf_dollars
  FROM receipts_base AS r
   LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS cc 
   ON LOWER(cc.sku_idnt) = LOWER(r.sku_idnt) 
   AND LOWER(r.channel_country) = LOWER(cc.channel_country)
   LEFT JOIN 
   `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.cc_type_realigned AS cc_type 
   ON LOWER(cc.customer_choice) = LOWER(cc_type.customer_choice
         ) AND LOWER(cc.channel_country) = LOWER(cc_type.channel_country) AND LOWER(r.channel_brand) = LOWER(cc_type.channel_brand
        ) AND LOWER(cc_type.channel) = LOWER('DIGITAL') 
        AND r.mnth_idnt = cc_type.mnth_idnt
   LEFT JOIN hrchy AS h 
   ON LOWER(r.sku_idnt) = LOWER(h.sku_idnt) 
   AND LOWER(r.channel_country) = LOWER(h.channel_country
      )
  WHERE h.fanatics_ind = 0
   AND h.marketplace_ind = 0);


--COLLECT STATS PRIMARY INDEX (mnth_idnt,channel_country,sku_idnt) ON receipts
--monthly CC level metrics
--DROP TABLE monthly;
--funnel
--reg price funnel
--sales & returns
--counts
--funnel
--reg price funnel
--sales & returns


CREATE TEMPORARY TABLE IF NOT EXISTS monthly AS 
WITH monthly_hierarchy AS (
  SELECT cal.mnth_454,
   cal.month_idnt AS mnth_idnt,
   cal.month_start_day_date AS mnth_start_day_date,
   cal.qtr_454,
   cal.yr_454,
   c.channel_country,
   c.channel_brand,
   c.div_idnt,
   c.grp_idnt,
   c.dept_idnt,
   c.cat_idnt,
   c.sku_idnt,
   c.customer_choice,
   c.cc_type,
   COALESCE(c.category, 'UNKNOWN') AS category,
   MAX(c.los_flag) AS los_flag,
   MAX(c.ds_ind) AS ds_ind,
   MAX(c.rp_ind) AS rp_ind,
   SUM(c.order_sessions) AS order_sessions,
   SUM(c.add_qty) AS add_qty,
   SUM(c.add_sessions) AS add_sessions,
   SUM(c.product_views) AS product_views,
   SUM(c.product_view_sessions) AS product_view_sessions,
   SUM(c.instock_views) AS instock_views,
   SUM(c.demand_units) AS demand_units,
   SUM(c.demand_dollars) AS demand_dollars,
   SUM(CASE
     WHEN LOWER(c.cc_type) = LOWER('NEW')
     THEN c.demand_units
     ELSE 0
     END) AS demand_units_new,
   SUM(CASE
     WHEN LOWER(c.cc_type) = LOWER('NEW')
     THEN c.demand_dollars
     ELSE 0
     END) AS demand_dollars_new,
   SUM(c.order_sessions_reg) AS order_sessions_reg,
   SUM(c.add_qty_reg) AS add_qty_reg,
   SUM(c.add_sessions_reg) AS add_sessions_reg,
   SUM(c.product_views_reg) AS product_views_reg,
   SUM(c.product_view_sessions_reg) AS product_view_sessions_reg,
   SUM(c.instock_views_reg) AS instock_views_reg,
   SUM(c.demand_units_reg) AS demand_units_reg,
   SUM(c.demand_dollars_reg) AS demand_dollars_reg,
   SUM(COALESCE(c.return_dollars, 0)) AS return_dollars,
   SUM(COALESCE(c.return_units, 0)) AS return_units,
   SUM(COALESCE(c.sales_dollars, 0)) AS sales_dollars,
   SUM(COALESCE(c.sales_units, 0)) AS sales_units,
   SUM(COALESCE(c.sales_net_dollars, 0)) AS sales_net_dollars,
   SUM(COALESCE(c.sales_net_units, 0)) AS sales_net_units
  FROM daily_hierarchy AS c
   INNER JOIN calendar AS cal 
   ON c.day_date = cal.day_date
  GROUP BY cal.mnth_454,
   mnth_idnt,
   mnth_start_day_date,
   cal.qtr_454,
   cal.yr_454,
   c.channel_country,
   c.channel_brand,
   c.div_idnt,
   c.grp_idnt,
   c.dept_idnt,
   c.cat_idnt,
   c.sku_idnt,
   c.customer_choice,
   c.cc_type,
   category
   ) 
   (SELECT COALESCE(c.mnth_454, r.mnth_454) AS mnth_454,
   COALESCE(c.mnth_idnt, r.mnth_idnt) AS mnth_idnt,
   COALESCE(c.mnth_start_day_date, r.mnth_start_day_date) AS mnth_start_day_date,
   COALESCE(c.qtr_454, r.qtr_454) AS qtr_454,
   COALESCE(c.yr_454, r.yr_454) AS yr_454,
   COALESCE(c.channel_country, r.channel_country) AS channel_country,
   COALESCE(c.channel_brand, r.channel_brand) AS channel_brand,
   COALESCE(c.div_idnt, r.div_idnt) AS div_idnt,
   COALESCE(c.grp_idnt, r.grp_idnt) AS grp_idnt,
   COALESCE(c.dept_idnt, r.dept_idnt) AS dept_idnt,
   COALESCE(c.cat_idnt, r.cat_idnt) AS cat_idnt,
   COALESCE(c.category, r.category) AS category,
   COUNT(DISTINCT COALESCE(c.customer_choice, r.customer_choice)) AS ccs_mnth,
   COUNT(DISTINCT CASE
     WHEN c.los_flag = 1
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_los,
   COUNT(DISTINCT CASE
     WHEN LOWER(COALESCE(c.cc_type, r.cc_type)) = LOWER('NEW')
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_new,
   COUNT(DISTINCT CASE
     WHEN LOWER(COALESCE(c.cc_type, r.cc_type)) = LOWER('CF')
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_cf,
   COUNT(DISTINCT CASE
     WHEN LOWER(COALESCE(c.cc_type, r.cc_type)) = LOWER('CF') AND c.rp_ind = 1
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_cf_rp,
   COUNT(DISTINCT CASE
     WHEN c.rp_ind = 1
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_rp,
   COUNT(DISTINCT CASE
     WHEN c.ds_ind = 1
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_ds,
   COUNT(DISTINCT CASE
     WHEN LOWER(COALESCE(c.cc_type, r.cc_type)) = LOWER('NEW') AND r.rcpt_bought_units > 0
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_rcpt_new,
   COUNT(DISTINCT CASE
     WHEN LOWER(COALESCE(c.cc_type, r.cc_type)) = LOWER('CF') AND r.rcpt_bought_units > 0
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_rcpt_cf,
   COUNT(DISTINCT CASE
     WHEN LOWER(COALESCE(c.cc_type, r.cc_type)) = LOWER('NEW') AND r.rcpt_ds_units > 0
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_rcpt_new_ds,
   COUNT(DISTINCT CASE
     WHEN LOWER(COALESCE(c.cc_type, r.cc_type)) = LOWER('CF') AND r.rcpt_ds_units > 0
     THEN COALESCE(c.customer_choice, r.customer_choice)
     ELSE NULL
     END) AS ccs_mnth_rcpt_cf_ds,
   SUM(c.order_sessions) AS order_sessions,
   SUM(c.add_qty) AS add_qty,
   SUM(c.add_sessions) AS add_sessions,
   SUM(c.product_views) AS product_views,
   SUM(c.product_view_sessions) AS product_view_sessions,
   SUM(c.instock_views) AS instock_views,
   SUM(c.demand_units) AS demand_units,
   SUM(c.demand_dollars) AS demand_dollars,
   SUM(c.demand_units_new) AS demand_units_new,
   SUM(c.demand_dollars_new) AS demand_dollars_new,
   SUM(c.order_sessions_reg) AS order_sessions_reg,
   SUM(c.add_qty_reg) AS add_qty_reg,
   SUM(c.add_sessions_reg) AS add_sessions_reg,
   SUM(c.product_views_reg) AS product_views_reg,
   SUM(c.product_view_sessions_reg) AS product_view_sessions_reg,
   SUM(c.instock_views_reg) AS instock_views_reg,
   SUM(c.demand_units_reg) AS demand_units_reg,
   SUM(c.demand_dollars_reg) AS demand_dollars_reg,
   SUM(c.return_dollars) AS return_dollars,
   SUM(c.return_units) AS return_units,
   SUM(c.sales_dollars) AS sales_dollars,
   SUM(c.sales_units) AS sales_units,
   SUM(c.sales_net_dollars) AS sales_net_dollars,
   SUM(c.sales_net_units) AS sales_net_units,
    SUM(c.demand_dollars) / NULLIF(COUNT(DISTINCT COALESCE(c.customer_choice, r.customer_choice)), 0) AS
   cc_monthly_productivity,
   SUM(r.rcpt_units) AS rcpt_units,
   SUM(r.rcpt_dollars) AS rcpt_dollars,
   SUM(r.rcpt_bought_units) AS rcpt_bought_units,
   SUM(r.rcpt_bought_dollars) AS rcpt_bought_dollars,
   SUM(r.rcpt_ds_units) AS rcpt_ds_units,
   SUM(r.rcpt_ds_dollars) AS rcpt_ds_dollars,
   SUM(r.rcpt_new_units) AS rcpt_new_units,
   SUM(r.rcpt_new_dollars) AS rcpt_new_dollars,
   SUM(r.rcpt_cf_units) AS rcpt_cf_units,
   SUM(r.rcpt_cf_dollars) AS rcpt_cf_dollars
  FROM monthly_hierarchy AS c
   FULL JOIN receipts AS r 
   ON LOWER(c.sku_idnt) = LOWER(r.sku_idnt) 
   AND c.mnth_idnt = r.mnth_idnt 
   AND LOWER(c.channel_country) = LOWER(r.channel_country) 
   AND LOWER(c.channel_brand) = LOWER(r.channel_brand) 
   AND c.dept_idnt = r.dept_idnt
   AND c.cat_idnt = r.cat_idnt 
   AND c.grp_idnt = r.grp_idnt 
   AND LOWER(c.cc_type) = LOWER(r.cc_type)
  WHERE COALESCE(c.mnth_idnt, r.mnth_idnt) <= (SELECT DISTINCT month_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
     WHERE day_date = current_date('PST8PDT'))
  GROUP BY mnth_454,
   mnth_idnt,
   mnth_start_day_date,
   qtr_454,
   yr_454,
   channel_country,
   channel_brand,
   div_idnt,
   grp_idnt,
   dept_idnt,
   cat_idnt,
   category);


--COLLECT STATS      PRIMARY INDEX(mnth_454, yr_454, dept_idnt, cat_idnt, channel_country, channel_brand)      ,COLUMN (channel_country, channel_brand, dept_idnt, cat_idnt)      ,COLUMN (mnth_idnt, channel_country, channel_brand, dept_idnt, category)      ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt)       ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt, cat_idnt)       ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt, category)       ,COLUMN (cat_idnt)       ,COLUMN (dept_idnt)       ,COLUMN (yr_454)       ,COLUMN (mnth_454)      ON monthly


-- fill in missing product views, mainly where sku_idnt is missing 


--DROP TABLE missing_pvs;
-- do not include fanatics skus
-- do not include marketplace skus
-- where sku_idnt is missing


CREATE TEMPORARY TABLE IF NOT EXISTS missing_pvs
AS
SELECT cal.mnth_454,
 cal.yr_454,
 f.channelcountry AS channel_country,
  CASE
  WHEN LOWER(f.channel) = LOWER('RACK')
  THEN 'NORDSTROM_RACK'
  ELSE 'NORDSTROM'
  END AS channel_brand,
 h.dept_idnt,
 COALESCE(h.category, 'UNKNOWN') AS category,
 SUM(f.product_views) AS product_views,
 SUM(f.product_views * f.pct_instock) AS instock_views,
 SUM(CASE
   WHEN LOWER(f.current_price_type) = LOWER('R')
   THEN f.product_views
   ELSE 0
   END) AS product_views_reg,
 SUM(CASE
   WHEN LOWER(f.current_price_type) = LOWER('R')
   THEN f.product_views * f.pct_instock
   ELSE 0
   END) AS instock_views_reg
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS f
 INNER JOIN calendar AS cal 
 ON f.event_date_pacific = cal.day_date
 INNER JOIN (SELECT DISTINCT web_style_num,
   channel_country,
   dept_idnt,
   quantrix_category AS category
  FROM `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.assortment_hierarchy
  WHERE fanatics_ind = 0
   AND marketplace_ind = 0
   AND LOWER(div_desc) NOT LIKE LOWER('INACTIVE%')) AS h 
   ON f.web_style_num = h.web_style_num 
   AND LOWER(f.channelcountry) = LOWER(h.channel_country)
WHERE f.event_date_pacific >= CAST('2019-02-03' AS DATE)
 AND LOWER(f.channel) <> LOWER('UNKNOWN')
 AND (LOWER(f.rms_sku_num) = LOWER('UNKNOWN') 
 OR f.rms_sku_num IS NULL)
GROUP BY cal.mnth_454,
 cal.yr_454,
 channel_country,
 channel_brand,
 h.dept_idnt,
 category;


--COLLECT STATS       COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt, category)       ON missing_pvs


-- Monthly count view 
--{env_suffix}


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_planning_monthly{{params.env_suffix}};


--{env_suffix}
--choice counts
--funnel
--reg price funnel
--sales & returns
-- NOTE: Using estimated bop/eop prior to 202104/05, 24 months of los.bop and los.eop data available starting 2023-07-04
-- forcing bop to match prior month's eop 
--{env_suffix}
-- remove current month (since it is not a complete month)
-- fill in missing product views, mainly from rack where missing sku_idnt


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_planning_monthly{{params.env_suffix}}

(SELECT a.mnth_454,
  a.mnth_idnt,
  a.mnth_start_day_date,
  a.qtr_454,
  a.yr_454,
  a.channel_country,
  a.channel_brand,
  a.div_idnt,
  a.grp_idnt,
  a.dept_idnt,
  a.cat_idnt,
  a.category,
  a.ccs_daily_avg,
  a.ccs_daily_avg_los,
  a.ccs_daily_avg_new,
  a.ccs_mnth_bop,
  a.ccs_mnth_eop,
  a.ccs_mnth,
  a.ccs_mnth_los,
  a.ccs_mnth_new,
  a.ccs_mnth_cf,
  a.ccs_mnth_cf_rp,
  a.ccs_mnth_rp,
  a.ccs_mnth_ds,
  a.ccs_mnth_rcpt_new,
  a.ccs_mnth_rcpt_cf,
  a.ccs_mnth_rcpt_new_ds,
  a.ccs_mnth_rcpt_cf_ds,
  a.order_sessions,
  a.add_qty,
  a.add_sessions,
  ROUND(CAST(a.product_views + COALESCE(b.product_views, 0) AS NUMERIC), 2) AS product_views,
  ROUND(CAST(a.product_view_sessions AS NUMERIC), 2) AS product_view_sessions,
  ROUND(CAST(a.instock_views + COALESCE(b.instock_views, 0) AS NUMERIC), 2) AS instock_views,
  a.demand_units,
  ROUND(CAST(a.demand_dollars AS NUMERIC), 2) AS demand_dollars,
  COALESCE(a.demand_units_new, 0) AS demand_units_new,
  CAST(COALESCE(a.demand_dollars_new, 0) AS NUMERIC) AS demand_dollars_new,
  ROUND(CAST(a.demand_units / IF(a.product_views + COALESCE(b.product_views, 0) = 0, NULL, a.product_views + COALESCE(b.product_views, 0)) AS NUMERIC)
   , 2) AS conversion,
  a.order_sessions_reg,
  a.add_qty_reg,
  a.add_sessions_reg,
  ROUND(CAST(a.product_views_reg + COALESCE(b.product_views_reg, 0) AS NUMERIC), 2) AS product_views_reg,
  ROUND(CAST(a.product_view_sessions_reg AS NUMERIC), 2) AS product_view_sessions_reg,
  ROUND(CAST(a.instock_views_reg + COALESCE(b.instock_views_reg, 0) AS NUMERIC), 2) AS instock_views_reg,
  a.demand_units_reg,
  ROUND(CAST(a.demand_dollars_reg AS NUMERIC), 2) AS demand_dollars_reg,
  ROUND(CAST(a.demand_units_reg / IF(a.product_views_reg + COALESCE(b.product_views_reg, 0) = 0, NULL, a.product_views_reg + COALESCE(b.product_views_reg, 0)) AS NUMERIC)
   , 2) AS conversion_reg,
  ROUND(CAST(a.return_dollars AS NUMERIC), 2) AS return_dollars,
  a.return_units,
  ROUND(CAST(a.sales_dollars AS NUMERIC), 2) AS sales_dollars,
  a.sales_units,
  ROUND(CAST(a.sales_net_dollars AS NUMERIC), 2) AS sales_net_dollars,
  a.sales_net_units,
  ROUND(CAST(a.cc_monthly_productivity AS NUMERIC), 2) AS cc_monthly_productivity,
  COALESCE(a.rcpt_units, 0) AS rcpt_units,
  ROUND(CAST(COALESCE(a.rcpt_dollars, 0) AS NUMERIC), 2) AS rcpt_dollars,
  COALESCE(a.rcpt_bought_units, 0) AS rcpt_bought_units,
  CAST(COALESCE(a.rcpt_bought_dollars, 0) AS NUMERIC) AS rcpt_bought_dollars,
  COALESCE(a.rcpt_ds_units, 0) AS rcpt_ds_units,
  CAST(COALESCE(a.rcpt_ds_dollars, 0) AS NUMERIC) AS rcpt_ds_dollars,
  COALESCE(a.rcpt_new_units, 0) AS rcpt_new_units,
  CAST(COALESCE(a.rcpt_new_dollars, 0) AS NUMERIC) AS rcpt_new_dollars,
  COALESCE(a.rcpt_cf_units, 0) AS rcpt_cf_units,
  CAST(COALESCE(a.rcpt_cf_dollars, 0) AS NUMERIC) AS rcpt_cf_dollars,
  timestamp(current_datetime('PST8PDT')) AS
  update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz
 FROM (SELECT m.* ,
    COALESCE(CASE
      WHEN m.mnth_idnt <= 202105
      THEN 
      -- LAG(d.eop_est, 1, d.ccs_daily_avg_los) OVER (PARTITION BY m.channel_country, m.channel_brand, m.dept_idnt, m.cat_idnt ORDER BY m.yr_454, m.mnth_454)
      coalesce(LAG(d.eop_est, 1) OVER (PARTITION BY m.channel_country, m.channel_brand, m.dept_idnt, m.cat_idnt ORDER BY m.yr_454, m.mnth_454),d.ccs_daily_avg_los)
      ELSE CAST(TRUNC(los.ccs_mnth_bop) AS INTEGER)
      END, 0) AS ccs_mnth_bop,

     COALESCE(CASE
      WHEN m.mnth_idnt <= 202104
      THEN d.eop_est
      ELSE CAST(TRUNC(los.ccs_mnth_eop) AS INTEGER)
      END, 0) AS ccs_mnth_eop,
    d.ccs_daily_avg,
    d.ccs_daily_avg_los,
    d.ccs_daily_avg_new
   FROM monthly AS m
    LEFT JOIN cc_bop_eop_cnts AS los 
    ON m.mnth_idnt = los.mnth_idnt 
    AND LOWER(m.channel_country) = LOWER(los.channel_country) AND LOWER(m.channel_brand) = LOWER(los.channel_brand) 
    AND m.dept_idnt = los.dept_idnt 
    AND LOWER(m.category)= LOWER(los.category)
    LEFT JOIN (
      SELECT cal.mnth_454,
      cal.yr_454,
      d.dept_idnt,
      d.cat_idnt,
      d.channel_country,
      d.channel_brand,
      CAST(TRUNC(AVG(d.customer_choices)) AS INTEGER) AS ccs_daily_avg,
      CAST(TRUNC(AVG(d.customer_choices_los)) AS INTEGER) AS ccs_daily_avg_los,
      CAST(TRUNC(AVG(d.customer_choices_new)) AS INTEGER) AS ccs_daily_avg_new,
      MAX(d.bop_est) AS bop_est,
      MAX(d.eop_est) AS eop_est
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_planning_daily{{params.env_suffix}} AS d
      INNER JOIN calendar AS cal ON d.day_date = cal.day_date
     GROUP BY d.channel_country,
      d.channel_brand,
      d.dept_idnt,
      d.cat_idnt,
      cal.mnth_454,
      cal.yr_454) AS d 
      ON m.mnth_454 = d.mnth_454 
      AND m.yr_454 = d.yr_454 
      AND m.dept_idnt = d.dept_idnt 
      AND (m.cat_idnt IS NULL OR m.cat_idnt = d.cat_idnt) 
      AND LOWER(m.channel_country) = LOWER(d.channel_country) AND LOWER(m.channel_brand ) = LOWER(d.channel_brand)
   WHERE m.mnth_idnt < (SELECT DISTINCT month_num
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal
      WHERE day_date = current_date('PST8PDT'))) AS a
  LEFT JOIN missing_pvs AS b 
  ON a.mnth_454 = b.mnth_454 
  AND a.yr_454 = b.yr_454 
  AND LOWER(a.channel_country) = LOWER(b.channel_country)
  AND LOWER(a.channel_brand) = LOWER(b.channel_brand) 
  AND a.dept_idnt = b.dept_idnt 
  AND LOWER(a.category) =LOWER(b.category));


--{env_suffix}


--COLLECT STATS      PRIMARY INDEX(mnth_start_day_date, dept_idnt, cat_idnt)      ON T2DL_DAS_SELECTION.selection_planning_monthly  