CREATE TEMPORARY TABLE IF NOT EXISTS calendar
AS
SELECT c.day_date,
 c.day_idnt,
 c.week_idnt AS wk_idnt,
 c.month_idnt,
 c.month_abrv,
 c.quarter_idnt,
 c.fiscal_halfyear_num AS half_idnt,
 c.fiscal_year_num AS year_idnt,
  CASE
  WHEN c.day_idnt = c.week_start_day_idnt
  THEN 1
  ELSE 0
  END AS first_day_of_week,
  CASE
  WHEN c.day_idnt = c.week_end_day_idnt
  THEN 1
  ELSE 0
  END AS last_day_of_week,
  CASE
  WHEN c.day_idnt = c.month_start_day_idnt
  THEN 1
  ELSE 0
  END AS first_day_of_month,
  CASE
  WHEN c.day_idnt = c.month_end_day_idnt
  THEN 1
  ELSE 0
  END AS last_day_of_month,
  CASE
  WHEN c.day_idnt = c.quarter_start_day_idnt
  THEN 1
  ELSE 0
  END AS first_day_of_qtr,
  CASE
  WHEN c.day_idnt = c.quarter_end_day_idnt
  THEN 1
  ELSE 0
  END AS last_day_of_qtr,
  CASE
  WHEN c.day_idnt BETWEEN curr.week_start_day_idnt AND curr.week_end_day_idnt
  THEN 1
  ELSE 0
  END AS current_week,
  CASE
  WHEN c.day_idnt BETWEEN curr.month_start_day_idnt AND curr.month_end_day_idnt
  THEN 1
  ELSE 0
  END AS current_month,
  CASE
  WHEN c.day_idnt BETWEEN curr.quarter_start_day_idnt AND curr.quarter_end_day_idnt
  THEN 1
  ELSE 0
  END AS current_quarter,
  CASE
  WHEN CASE
    WHEN c.day_idnt BETWEEN curr.week_start_day_idnt AND curr.week_end_day_idnt
    THEN 1
    ELSE 0
    END = 0
  THEN CAST(trunc(cast(DENSE_RANK() OVER (PARTITION BY CASE
       WHEN c.day_idnt BETWEEN curr.week_start_day_idnt AND curr.week_end_day_idnt
       THEN 1
       ELSE 0
       END ORDER BY c.week_idnt DESC)as float64)) AS INTEGER)
  ELSE 0
  END AS week_rank,
  CASE
  WHEN CASE
    WHEN c.day_idnt BETWEEN curr.month_start_day_idnt AND curr.month_end_day_idnt
    THEN 1
    ELSE 0
    END = 0
  THEN CAST(trunc(cast(DENSE_RANK() OVER (PARTITION BY CASE
       WHEN c.day_idnt BETWEEN curr.month_start_day_idnt AND curr.month_end_day_idnt
       THEN 1
       ELSE 0
       END ORDER BY c.month_idnt DESC)as float64)) AS INTEGER)
  ELSE 0
  END AS month_rank,
  CASE
  WHEN CASE
    WHEN c.day_idnt BETWEEN curr.quarter_start_day_idnt AND curr.quarter_end_day_idnt
    THEN 1
    ELSE 0
    END = 0
  THEN CAST(trunc(cast(DENSE_RANK() OVER (PARTITION BY CASE
       WHEN c.day_idnt BETWEEN curr.quarter_start_day_idnt AND curr.quarter_end_day_idnt
       THEN 1
       ELSE 0
       END ORDER BY c.quarter_idnt DESC)as float64)) AS INTEGER)
  ELSE 0
  END AS quarter_rank
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c
 INNER JOIN (SELECT DISTINCT week_start_day_idnt,
   week_end_day_idnt,
   quarter_start_day_idnt,
   quarter_end_day_idnt,
   month_start_day_idnt,
   month_end_day_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = CURRENT_DATE('PST8PDT')) AS curr 
  --ON CURRENT_DATE
 ON c.day_date BETWEEN {{params.start_date}} AND {{params.end_date}};

--COLLECT STATS PRIMARY INDEX (day_date) ON calendar


CREATE TEMPORARY TABLE IF NOT EXISTS replenishment AS (
	SELECT
    rms_sku_num AS sku_idnt,
    max(st.store_num) AS loc_idnt,
    day_idnt,
    1 AS replenishment_flag
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist AS rp
    INNER JOIN calendar AS d ON d.day_date BETWEEN RANGE_START(rp_period) and RANGE_END(rp_period)
    INNER JOIN (
      SELECT
          str.*,
          CASE
            WHEN str.store_num IN(
              210, 212
            ) THEN 209
            ELSE str.store_num
          END AS store_num_stg
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str
        WHERE rtrim(str.business_unit_desc, ' ') IN(
          'N.COM', 'FULL LINE', 'RACK', 'RACK CANADA', 'OFFPRICE ONLINE', 'N.CA'
        )
    ) AS st ON rp.location_num = st.store_num_stg
  GROUP BY 1, upper(CAST(st.store_num as STRING)), 3, 4
);


CREATE TEMPORARY TABLE IF NOT EXISTS dropship

AS
SELECT ds.snapshot_date AS day_dt,
 ds.rms_sku_id AS sku_idnt,
 1 AS dropship_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact AS ds
 INNER JOIN calendar AS dt ON ds.snapshot_date = dt.day_date
WHERE LOWER(ds.location_type) IN (LOWER('DS'))
GROUP BY day_dt,
 sku_idnt,
 dropship_flag;


--COLLECT STATS      PRIMARY INDEX (day_dt, sku_idnt)      ON dropship

CREATE TEMPORARY TABLE IF NOT EXISTS fanatics

AS
SELECT DISTINCT psd.rms_sku_num AS sku_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS psd
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim AS payto ON LOWER(psd.prmy_supp_num) = LOWER(payto.order_from_vendor_num
   )
WHERE LOWER(payto.payto_vendor_num) = LOWER('5179609');


--COLLECT STATS       PRIMARY INDEX (sku_idnt)     ON fanatics


CREATE TEMPORARY TABLE IF NOT EXISTS anniversary

AS
SELECT a.sku_idnt,
 a.channel_country,
  CASE
  WHEN LOWER(a.selling_channel) = LOWER('ONLINE')
  THEN 'DIGITAL'
  ELSE a.selling_channel
  END AS channel,
 'NORDSTROM' AS channel_brand,
 cal.day_idnt,
 a.day_dt,
 1 AS anniversary_flag
FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.anniversary_sku_chnl_date AS a
 INNER JOIN calendar AS cal ON a.day_idnt = cal.day_idnt
GROUP BY a.sku_idnt,
 a.channel_country,
 channel,
 channel_brand,
 cal.day_idnt,
 a.day_dt;
 
 
 
CREATE TEMPORARY TABLE IF NOT EXISTS last_receipt

AS
SELECT rms_sku_num AS sku_idnt,
 store_num AS loc_idnt,
 po_receipt_last_date AS last_receipt_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_receipt_date_sku_store_fact AS r;


--COLLECT STATS      PRIMARY INDEX (sku_idnt, loc_idnt)     ON last_receipt

 CREATE TEMPORARY TABLE IF NOT EXISTS digital_funnel

AS
SELECT f.event_date_pacific AS day_date,
 f.channelcountry AS channel_country,
  CASE
  WHEN LOWER(f.channel) = LOWER('RACK')
  THEN 'NORDSTROM_RACK'
  ELSE 'NORDSTROM'
  END AS channel_brand,
 f.rms_sku_num AS sku_idnt,
 SUM(f.order_sessions) AS order_sessions,
 SUM(f.add_to_bag_quantity) AS add_qty,
 SUM(f.add_to_bag_sessions) AS add_sessions,
 SUM(f.product_views) AS product_views,
 SUM(f.product_view_sessions) AS product_view_sessions,
 SUM(f.product_views * f.pct_instock) AS instock_views,
 SUM(f.order_demand) AS order_demand,
 SUM(f.order_quantity) AS order_quantity,
 SUM(CASE
   WHEN LOWER(f.current_price_type) = LOWER('R')
   THEN f.order_sessions
   ELSE 0
   END) AS order_sessions_reg,
 SUM(CASE
   WHEN LOWER(f.current_price_type) = LOWER('R')
   THEN f.add_to_bag_quantity
   ELSE 0
   END) AS add_qty_reg,
 SUM(CASE
   WHEN LOWER(f.current_price_type) = LOWER('R')
   THEN f.add_to_bag_sessions
   ELSE 0
   END) AS add_sessions_reg,
 SUM(CASE
   WHEN LOWER(f.current_price_type) = LOWER('R')
   THEN f.product_views
   ELSE 0
   END) AS product_views_reg,
 SUM(CASE
   WHEN LOWER(f.current_price_type) = LOWER('R')
   THEN f.product_view_sessions
   ELSE 0
   END) AS product_view_sessions_reg,
 SUM(CASE
   WHEN LOWER(f.current_price_type) = LOWER('R')
   THEN f.product_views * f.pct_instock
   ELSE 0
   END) AS instock_views_reg
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS f
 INNER JOIN calendar AS cal ON f.event_date_pacific = cal.day_date
WHERE LOWER(f.channel) <> LOWER('UNKNOWN')
GROUP BY day_date,
 channel_country,
 channel_brand,
 sku_idnt;


--COLLECT STATS 	PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country) 	ON digital_funnel


CREATE TEMPORARY TABLE IF NOT EXISTS pricetype_sku
AS
SELECT cal.day_date,
 cal.day_idnt,
 cal.wk_idnt,
 cal.month_idnt,
 cal.month_abrv,
 cal.quarter_idnt,
 cal.half_idnt,
 cal.year_idnt,
 cal.first_day_of_week,
 cal.last_day_of_week,
 cal.first_day_of_month,
 cal.last_day_of_month,
 cal.first_day_of_qtr,
 cal.last_day_of_qtr,
 cal.current_week,
 cal.current_month,
 cal.current_quarter,
 cal.week_rank,
 cal.month_rank,
 cal.quarter_rank,
 sld.sku_idnt,
 sld.price_type,
 st.channel_country,
 st.channel_brand,
  CASE
  WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
  THEN 'DIGITAL'
  ELSE st.selling_channel
  END AS channel,
 MAX(COALESCE(replenishment_flag, 0)) AS rp_flag,
 MAX(COALESCE(ds.dropship_flag, 0)) AS dropship_flag,
 SUM(COALESCE(sld.sales_units, 0)) AS net_sales_units,
 SUM(COALESCE(sld.sales_dollars, 0)) AS net_sales_dollars,
 SUM(COALESCE(sld.return_units, 0)) AS return_units,
 SUM(COALESCE(sld.return_dollars, 0)) AS return_dollars,
 SUM(COALESCE(sld.demand_units, 0)) AS demand_units,
 SUM(COALESCE(sld.demand_dollars, 0)) AS demand_dollars,
 SUM(COALESCE(sld.demand_cancel_units, 0)) AS demand_cancel_units,
 SUM(COALESCE(sld.demand_cancel_dollars, 0)) AS demand_cancel_dollars,
 SUM(COALESCE(sld.demand_dropship_units, 0)) AS demand_dropship_units,
 SUM(COALESCE(sld.demand_dropship_dollars, 0)) AS demand_dropship_dollars,
 SUM(COALESCE(sld.shipped_units, 0)) AS shipped_units,
 SUM(COALESCE(sld.shipped_dollars, 0)) AS shipped_dollars,
 SUM(COALESCE(sld.store_fulfill_units, 0)) AS store_fulfill_units,
 SUM(COALESCE(sld.store_fulfill_dollars, 0)) AS store_fulfill_dollars,
 SUM(COALESCE(sld.eoh_units, 0)) AS eoh_units,
 SUM(COALESCE(sld.eoh_dollars, 0)) AS eoh_dollars,
 SUM(COALESCE(sld.boh_units, 0)) AS boh_units,
 SUM(COALESCE(sld.boh_dollars, 0)) AS boh_dollars,
 SUM(COALESCE(sld.nonsellable_units, 0)) AS nonsellable_units,
 SUM(0) AS receipt_units,
 SUM(0) AS receipt_dollars,
 MAX(lr.last_receipt_date) AS last_receipt_date
FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_loc_pricetype_day AS sld
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st ON sld.loc_idnt = st.store_num
 INNER JOIN calendar AS cal ON sld.day_dt = cal.day_date
  LEFT JOIN replenishment AS r 
  ON sld.sku_idnt = r.sku_idnt
 AND sld.loc_idnt = r.loc_idnt
 AND cal.day_idnt = r.day_idnt
 LEFT JOIN dropship AS ds ON sld.day_dt = ds.day_dt AND LOWER(sld.sku_idnt) = LOWER(ds.sku_idnt)
 LEFT JOIN last_receipt AS lr ON LOWER(lr.sku_idnt) = LOWER(sld.sku_idnt) AND sld.loc_idnt = lr.loc_idnt
WHERE st.business_unit_num NOT IN (8000, 5500)
GROUP BY cal.day_date,
 cal.day_idnt,
 cal.wk_idnt,
 cal.month_idnt,
 cal.month_abrv,
 cal.quarter_idnt,
 cal.half_idnt,
 cal.year_idnt,
 cal.first_day_of_week,
 cal.last_day_of_week,
 cal.first_day_of_month,
 cal.last_day_of_month,
 cal.first_day_of_qtr,
 cal.last_day_of_qtr,
 cal.current_week,
 cal.current_month,
 cal.current_quarter,
 cal.week_rank,
 cal.month_rank,
 cal.quarter_rank,
 sld.sku_idnt,
 sld.price_type,
 st.channel_country,
 st.channel_brand,
 channel;


--COLLECT STATS       PRIMARY INDEX (day_date, sku_idnt)     ON pricetype_sku


CREATE TEMPORARY TABLE IF NOT EXISTS los
AS
SELECT channel_country,
 channel_brand,
 sku_id,
 day_date,
 days_live,
 MIN(day_date) OVER (PARTITION BY channel_country, channel_brand, sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND
  UNBOUNDED FOLLOWING) AS first_day_published
FROM (SELECT channel_country,
   channel_brand,
   sku_id,
   day_date,
   1 AS days_live
  FROM `{{params.gcp_project_id}}`.t2dl_das_site_merch.live_on_site_daily
  GROUP BY channel_country,
   channel_brand,
   sku_id,
   day_date,
   days_live) AS t0;


--COLLECT STATS       PRIMARY INDEX (day_date, sku_id, channel_country, channel_brand)     ON los


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_productivity_base{{params.env_suffix}} 
WHERE day_date BETWEEN {{params.start_date}} and {{params.end_date}};

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.selection_productivity_base{{params.env_suffix}}  
SELECT s.day_date,
 s.wk_idnt,
 s.month_idnt,
 s.month_abrv,
 s.quarter_idnt,
 s.half_idnt,
 s.year_idnt,
 s.first_day_of_week,
 s.last_day_of_week,
 s.first_day_of_month,
 s.last_day_of_month,
 s.first_day_of_qtr,
 s.last_day_of_qtr,
 s.current_week,
 s.current_month,
 s.current_quarter,
 s.week_rank,
 s.month_rank,
 s.quarter_rank,
 s.sku_idnt,
 cc.customer_choice,
 s.channel_country,
 s.channel_brand,
 s.channel,
 MIN(l.first_day_published) AS days_published,
 COALESCE(l.days_live, 0) AS days_live,
  CASE
  WHEN f.sku_idnt IS NULL
  THEN 0
  ELSE 1
  END AS fanatics_flag,
  CASE
  WHEN LOWER(ncf.cc_type) = LOWER('NEW')
  THEN 1
  ELSE 0
  END AS new_flag,
  CASE
  WHEN LOWER(ncf.cc_type) = LOWER('CF')
  THEN 1
  ELSE 0
  END AS cf_flag,
  MAX(rp_flag) AS rp_flag,
 MAX(s.dropship_flag) AS dropship_flag,
 MAX(COALESCE(an.anniversary_flag, 0)) AS anniversary_flag,
 SUM(s.net_sales_units) AS net_sales_units,
 SUM(s.net_sales_dollars) AS net_sales_dollars,
 SUM(s.return_units) AS return_units,
 SUM(s.return_dollars) AS return_dollars,
  SUM(s.net_sales_units) + SUM(s.return_units) AS sales_units,
  SUM(s.net_sales_dollars) + SUM(s.return_dollars) AS sales_dollars,
 SUM(s.demand_units) AS demand_units,
 SUM(s.demand_dollars) AS demand_dollars,
 SUM(s.demand_cancel_units) AS demand_cancel_units,
 SUM(s.demand_cancel_dollars) AS demand_cancel_dollars,
 SUM(s.demand_dropship_units) AS demand_dropship_units,
 SUM(s.demand_dropship_dollars) AS demand_dropship_dollars,
 SUM(s.shipped_units) AS shipped_units,
 SUM(s.shipped_dollars) AS shipped_dollars,
 SUM(s.store_fulfill_units) AS store_fulfill_units,
 SUM(s.store_fulfill_dollars) AS store_fulfill_dollars,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('R')
   THEN s.net_sales_units
   ELSE 0
   END) AS net_sales_reg_units,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('R')
   THEN s.net_sales_dollars
   ELSE 0
   END) AS net_sales_reg_dollars,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('C')
   THEN s.net_sales_units
   ELSE 0
   END) AS net_sales_clr_units,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('C')
   THEN s.net_sales_dollars
   ELSE 0
   END) AS net_sales_clr_dollars,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('P')
   THEN s.net_sales_units
   ELSE 0
   END) AS net_sales_pro_units,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('P')
   THEN s.net_sales_dollars
   ELSE 0
   END) AS net_sales_pro_dollars,
 SUM(s.boh_units) AS boh_units,
 SUM(s.boh_dollars) AS boh_dollars,
 SUM(s.eoh_units) AS eoh_units,
 SUM(s.eoh_dollars) AS eoh_dollars,
 SUM(s.nonsellable_units) AS nonsellable_units,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('R')
   THEN s.eoh_units
   ELSE 0
   END) AS eoh_reg_units,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('R')
   THEN s.eoh_dollars
   ELSE 0
   END) AS eoh_reg_dollars,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('C')
   THEN s.eoh_units
   ELSE 0
   END) AS eoh_clr_units,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('C')
   THEN s.eoh_dollars
   ELSE 0
   END) AS eoh_clr_dollars,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('P')
   THEN s.eoh_units
   ELSE 0
   END) AS eoh_pro_units,
 SUM(CASE
   WHEN LOWER(s.price_type) = LOWER('P')
   THEN s.eoh_dollars
   ELSE 0
   END) AS eoh_pro_dollars,
 MAX(s.last_receipt_date) AS last_receipt_date,
 SUM(df.order_sessions) AS order_sessions,
 SUM(df.add_qty) AS add_qty,
 SUM(df.add_sessions) AS add_sessions,
 SUM(df.product_views) AS product_views,
 SUM(df.product_view_sessions) AS product_view_sessions,
 SUM(df.instock_views) AS instock_views,
 SUM(df.order_sessions_reg) AS order_sessions_reg,
 SUM(df.add_qty_reg) AS add_qty_reg,
 SUM(df.add_sessions_reg) AS add_sessions_reg,
 SUM(df.product_views_reg) AS product_views_reg,
 SUM(df.product_view_sessions_reg) AS product_view_sessions_reg,
 SUM(df.instock_views_reg) AS instock_views_reg,
 SUM(df.order_demand) AS order_demand,
 SUM(df.order_quantity) AS order_quantity
FROM pricetype_sku AS s
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS sku ON LOWER(s.sku_idnt) = LOWER(sku.rms_sku_num) AND LOWER(sku.channel_country
    ) = LOWER('US')
 INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.sku_cc_lkp AS cc ON LOWER(s.sku_idnt) = LOWER(TRIM(cc.sku_idnt)) AND LOWER(s.channel_country
    ) = LOWER(TRIM(cc.channel_country))
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_assortment_dim.cc_type_realigned AS ncf ON s.month_idnt = ncf.mnth_idnt AND LOWER(cc.customer_choice
       ) = LOWER(ncf.customer_choice) AND LOWER(cc.channel_country) = LOWER(ncf.channel_country) AND LOWER(s.channel) =
    LOWER(ncf.channel) AND LOWER(s.channel_brand) = LOWER(ncf.channel_brand)
 LEFT JOIN digital_funnel AS df ON LOWER(s.sku_idnt) = LOWER(df.sku_idnt) AND s.day_date = df.day_date AND LOWER(s.channel_country
      ) = LOWER(df.channel_country) AND LOWER(s.channel_brand) = LOWER(df.channel_brand) AND LOWER(s.channel) = LOWER('DIGITAL'
    )
 LEFT JOIN fanatics AS f ON LOWER(s.sku_idnt) = LOWER(f.sku_idnt)
 LEFT JOIN los AS l ON LOWER(s.sku_idnt) = LOWER(l.sku_id) AND LOWER(s.channel_country) = LOWER(l.channel_country) AND
     LOWER(s.channel_brand) = LOWER(l.channel_brand) AND s.day_date = l.day_date AND LOWER(s.channel) = LOWER('DIGITAL'
    )
 LEFT JOIN anniversary AS an ON LOWER(s.sku_idnt) = LOWER(an.sku_idnt) AND LOWER(s.channel_country) = LOWER(an.channel_country
       ) AND LOWER(s.channel_brand) = LOWER(an.channel_brand) AND LOWER(s.channel) = LOWER(an.channel) AND s.day_date =
   an.day_dt
WHERE LOWER(COALESCE(sku.partner_relationship_type_code, 'UNKNOWN')) <> LOWER('ECONCESSION')
GROUP BY s.day_date,
 s.wk_idnt,
 s.month_idnt,
 s.month_abrv,
 s.quarter_idnt,
 s.half_idnt,
 s.year_idnt,
 s.first_day_of_week,
 s.last_day_of_week,
 s.first_day_of_month,
 s.last_day_of_month,
 s.first_day_of_qtr,
 s.last_day_of_qtr,
 s.current_week,
 s.current_month,
 s.current_quarter,
 s.week_rank,
 s.month_rank,
 s.quarter_rank,
 s.sku_idnt,
 cc.customer_choice,
 s.channel_country,
 s.channel_brand,
 s.channel,
 days_live,
 fanatics_flag,
 new_flag,
 cf_flag;