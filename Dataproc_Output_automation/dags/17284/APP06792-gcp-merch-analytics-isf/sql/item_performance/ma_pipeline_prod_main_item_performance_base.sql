CREATE TEMPORARY TABLE IF NOT EXISTS dates
AS
SELECT day_date,
 day_idnt,
 week_idnt,
 week_desc,
 week_end_day_date,
 month_end_week_idnt,
 month_idnt,
 quarter_idnt,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS tme
WHERE day_date BETWEEN {{params.start_date}} AND {{params.end_date}};


--COLLECT STATS PRIMARY INDEX (week_idnt, month_idnt) ON dates


-- dept || supplier || vpn || color


CREATE TEMPORARY TABLE IF NOT EXISTS sku_cc_lkp
AS
SELECT rms_sku_num AS sku_idnt,
 channel_country,
       FORMAT('%11d', dept_num) || '_' || prmy_supp_num || '_' || supp_part_num || '_' || COALESCE(color_num, 'UNKNOWN'   ) AS customer_choice
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw;


--COLLECT STATS      PRIMARY INDEX (sku_idnt, channel_country)     ,COLUMN (customer_choice)     ,COLUMN (sku_idnt, channel_country)     ,COLUMN (channel_country, customer_choice)      ON sku_cc_lkp


--Location Table: Filters to only the locations we need


CREATE TEMPORARY TABLE IF NOT EXISTS loc
AS
SELECT store_num,
 store_name,
  CASE
  WHEN channel_num IN (110, 120, 130, 140, 210, 220, 250, 260, 310)
  THEN 'US'
  WHEN channel_num IN (111, 121, 211, 221, 261, 311)
  THEN 'CA'
  ELSE NULL
  END AS country,
  CASE
  WHEN channel_num IN (110, 120, 130, 140, 111, 121, 310, 311)
  THEN 'FP'
  WHEN channel_num IN (211, 221, 261, 210, 220, 250, 260)
  THEN 'OP'
  ELSE NULL
  END AS banner,
   SUBSTR(CAST(channel_num AS STRING), 1, 50) || ', ' || SUBSTR(channel_desc, 1, 100) AS channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
WHERE channel_num IN (110, 111, 120, 121, 130, 140, 210, 211, 220, 221, 250, 260, 261, 310, 311)
GROUP BY store_num,
 store_name,
 country,
 banner,
 channel;


--COLLECT STATS      PRIMARY INDEX (store_num)      ON loc


--Live on Site Table: gets sku's with associated live on site start date as well as if an


-- item was live on a specific day


CREATE TEMPORARY TABLE IF NOT EXISTS live_on_site
AS
SELECT CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(channel_country) = LOWER('US')
  THEN '120, N.COM'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(channel_country) = LOWER('CA')
  THEN '121, N.CA'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN '250, OFFPRICE ONLINE'
  ELSE NULL
  END AS channel,
 channel_brand,
 channel_country AS country,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(channel_country) = LOWER('US')
  THEN 808
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(channel_country) = LOWER('CA')
  THEN 857
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 591
  ELSE NULL
  END AS loc_idnt,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'FP'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'OP'
  ELSE NULL
  END AS banner,
 sku_id,
 day_date,
 1 AS days_live,
 day_date AS first_day_published
FROM `{{params.gcp_project_id}}`.t2dl_das_site_merch.live_on_site_daily
GROUP BY channel,
 channel_brand,
 country,
 loc_idnt,
 banner,
 sku_id,
 day_date,
 days_live;


--COLLECT STATS 	PRIMARY INDEX (day_date, sku_id, channel, country) 	ON live_on_site


-- Replenishment Table: gets SKUS that are on RP


--BEGIN(rp_period) and END(rp_period)

CREATE TEMPORARY TABLE IF NOT EXISTS replenishment AS (
	SELECT
      rms_sku_num AS sku_idnt
      , CAST(st.store_num AS STRING) AS loc_idnt
      , day_idnt
      , 1 AS replenishment_flag
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
  INNER JOIN dates d
  ON d.day_date BETWEEN RANGE_START(rp_period) and RANGE_END(rp_period)
  INNER JOIN (
    SELECT
        str.*
        , CASE WHEN store_num IN (210, 212) THEN 209 ELSE store_num END AS store_num_stg
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim str
    WHERE LOWER(business_unit_desc) IN (LOWER('N.COM'), LOWER('FULL LINE'),  LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'), LOWER('N.CA'))
  ) st
     ON rp.location_num = st.store_num_stg
  GROUP BY 1,2,3,4
);


-- Dropship Table: Gets sku's dropship status by day


CREATE TEMPORARY TABLE IF NOT EXISTS dropship
AS
SELECT ds.snapshot_date AS day_dt,
 ds.rms_sku_id AS sku_idnt,
 1 AS dropship_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_fact AS ds
 INNER JOIN dates AS dt ON ds.snapshot_date = dt.day_date
WHERE LOWER(ds.location_type) IN (LOWER('DS'))
GROUP BY day_dt,
 sku_idnt,
 dropship_flag;


--COLLECT STATS      PRIMARY INDEX (day_dt, sku_idnt)      ON dropship


--fanatics indicator


CREATE TEMPORARY TABLE IF NOT EXISTS fanatics
AS
SELECT DISTINCT psd.rms_sku_num AS sku_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS psd
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_payto_relationship_dim AS payto ON LOWER(psd.prmy_supp_num) = LOWER(payto.order_from_vendor_num
   )
WHERE LOWER(payto.payto_vendor_num) = LOWER('5179609');


--COLLECT STATS      PRIMARY INDEX (sku_idnt)     ON fanatics


--gets last receipt date


CREATE TEMPORARY TABLE IF NOT EXISTS last_receipt
AS
SELECT rms_sku_num AS sku_idnt,
 store_num AS loc_idnt,
 po_receipt_last_date AS last_receipt_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_receipt_date_sku_store_fact AS r;


--COLLECT STATS      PRIMARY INDEX (sku_idnt, loc_idnt)     ON last_receipt


--gets on order information by sku


-- NEW


CREATE TEMPORARY TABLE IF NOT EXISTS oo_sku
AS
SELECT cal.day_date,
 cal.week_idnt,
 cal.week_desc,
 cal.week_end_day_date,
 cal.month_end_week_idnt,
 cal.month_idnt,
 cal.quarter_idnt,
 cal.fiscal_year_num,
 oo.sku_idnt,
 oo.price_type,
 oo.country,
 oo.banner,
 oo.channel,
 oo.rp_flag,
 0 AS dropship_flag,
 0 AS net_sales_units,
 0 AS net_sales_dollars,
 0 AS return_units,
 0 AS return_dollars,
 0 AS demand_units,
 0 AS demand_dollars,
 0 AS demand_cancel_units,
 0 AS demand_cancel_dollars,
 0 AS demand_dropship_units,
 0 AS demand_dropship_dollars,
 0 AS shipped_units,
 0 AS shipped_dollars,
 0 AS store_fulfill_units,
 0 AS store_fulfill_dollars,
 0 AS eoh_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS boh_units,
 0 AS boh_dollars,
 0 AS boh_cost,
 0 AS nonsellable_units,
 0 AS cost_of_goods_sold,
 0 AS sales_pm,
 0 AS product_margin,
 0 AS receipt_units,
 0 AS receipt_dollars,
 0 AS reserve_stock_units,
 0 AS reserve_stock_dollars,
 0 AS receipt_dropship_units,
 0 AS receipt_dropship_dollars,
 0 AS backorder_units,
 0 AS backorder_dollars,
 SUM(oo.oo_units) AS oo_units,
 SUM(oo.oo_dollars) AS oo_dollars,
 MAX(oo.last_receipt_date) AS last_receipt_date
FROM (SELECT (SELECT MAX(week_idnt)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE week_end_day_date <= CURRENT_DATE('PST8PDT')) AS week_idnt,
    (SELECT MAX(week_end_day_date)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE week_end_day_date <= CURRENT_DATE('PST8PDT')) AS week_end_day_date,
   oo.rms_sku_num AS sku_idnt,
   st.country,
   st.banner,
   st.channel,
   oo.anticipated_price_type AS price_type,
    CASE
    WHEN LOWER(oo.order_type) = LOWER('AUTOMATIC_REORDER')
    THEN 1
    ELSE 0
    END AS rp_flag,
   SUM(oo.quantity_open) AS oo_units,
   SUM(oo.total_anticipated_retail_amt) AS oo_dollars,
   MAX(lr.last_receipt_date) AS last_receipt_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS oo
   INNER JOIN loc AS st ON oo.store_num = st.store_num
   LEFT JOIN last_receipt AS lr ON LOWER(oo.rms_sku_num) = LOWER(lr.sku_idnt) AND oo.store_num = lr.loc_idnt
  WHERE oo.quantity_open > 0
   AND (LOWER(oo.status) = LOWER('CLOSED') AND oo.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 45 DAY) OR LOWER(oo.status
       ) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
   AND oo.first_approval_date IS NOT NULL
  GROUP BY week_idnt,
   week_end_day_date,
   sku_idnt,
   st.country,
   st.banner,
   st.channel,
   price_type,
   rp_flag) AS oo
 INNER JOIN dates AS cal ON oo.week_end_day_date = cal.day_date
GROUP BY cal.day_date,
 cal.week_idnt,
 cal.week_desc,
 cal.week_end_day_date,
 cal.month_end_week_idnt,
 cal.month_idnt,
 cal.quarter_idnt,
 cal.fiscal_year_num,
 oo.sku_idnt,
 oo.price_type,
 oo.country,
 oo.banner,
 oo.channel,
 oo.rp_flag,
 dropship_flag;


--COLLECT STATS      PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)     ON oo_sku


--Grab metrics from the price type table in NAP and roll up to week - for flags that are at the day level


-- take the max value for the week (i.e. if a sku was on dropship at any day during the week it is flagged


-- as dropship for the entire week)


CREATE TEMPORARY TABLE IF NOT EXISTS pricetype_sku AS 
(
	 SELECT
	 	cal.day_date
	    ,cal.week_idnt
	    ,cal.week_desc
	    ,cal.week_end_day_date
	    ,cal.month_end_week_idnt
	    ,cal.month_idnt
	    ,cal.quarter_idnt
	    ,cal.fiscal_year_num
	    ,sld.sku_idnt
	    ,price_type
	    ,country
	    ,banner
	    ,channel
	    ,MAX(replenishment_flag) AS rp_flag
	    ,MAX(dropship_flag) AS dropship_flag
	    ,SUM(sales_units) AS net_sales_units
	    ,SUM(sales_dollars) AS net_sales_dollars
	    ,SUM(return_units) AS return_units
	    ,SUM(return_dollars) AS return_dollars
	    ,SUM(demand_units) AS demand_units
	    ,SUM(demand_dollars) AS demand_dollars
	    ,SUM(demand_cancel_units) AS demand_cancel_units
	    ,SUM(demand_cancel_dollars) AS demand_cancel_dollars
	    ,SUM(demand_dropship_units) AS demand_dropship_units
	    ,SUM(demand_dropship_dollars) AS demand_dropship_dollars
	    ,SUM(shipped_units) AS shipped_units
	    ,SUM(shipped_dollars) AS shipped_dollars
	    ,SUM(store_fulfill_units) AS store_fulfill_units
	    ,SUM(store_fulfill_dollars) AS store_fulfill_dollars
	    ,SUM(eoh_units) AS eoh_units
	    ,SUM(eoh_dollars) AS eoh_dollars
	    ,SUM(eoh_units * weighted_average_cost) AS eoh_cost
	    ,SUM(boh_units) AS boh_units
	    ,SUM(boh_dollars) AS boh_dollars
	    ,SUM(boh_units * weighted_average_cost) AS boh_cost
	    ,SUM(nonsellable_units) AS nonsellable_units
	    ,SUM(cost_of_goods_sold) as cost_of_goods_sold
		,SUM(CASE WHEN cost_of_goods_sold is not null then sales_dollars else 0 end) as sales_pm
		,SUM(gross_margin) as product_margin
	    ,CAST(0 AS NUMERIC) AS receipt_units
	    ,CAST(0 AS NUMERIC) AS receipt_dollars
	    ,CAST(0 AS NUMERIC) AS reserve_stock_units
	    ,CAST(0 AS NUMERIC) AS reserve_stock_dollars
	    ,CAST(0 AS NUMERIC) AS receipt_dropship_units
	    ,CAST(0 AS NUMERIC) AS receipt_dropship_dollars
	    ,CAST(0 AS NUMERIC) AS backorder_units
	    ,CAST(0 AS NUMERIC) AS backorder_dollars
	    ,CAST(0 AS NUMERIC) AS oo_units
	    ,CAST(0 AS NUMERIC) AS oo_dollars
	    ,MAX(last_receipt_date) as last_receipt_date
	FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_loc_pricetype_day_vw sld
	JOIN loc st
	  ON sld.loc_idnt = st.store_num
	JOIN dates cal
	  ON sld.day_dt = cal.day_date
	LEFT JOIN replenishment r
	  ON sld.sku_idnt = r.sku_idnt
	 AND CAST(sld.loc_idnt AS STRING) = r.loc_idnt
	 AND cal.day_idnt = r.day_idnt
	LEFT JOIN dropship ds
	  ON ds.day_dt = cal.day_date
	 AND sld.sku_idnt = ds.sku_idnt
	LEFT JOIN last_receipt lr
	  ON lr.sku_idnt = sld.sku_idnt
	 AND lr.loc_idnt = sld.loc_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,40,41,42,43,44,45,46,47
);




--COLLECT STATS      PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)     ON pricetype_sku


-- Getting Backorder information

CREATE TEMPORARY TABLE IF NOT EXISTS  backorder_sku AS (
	SELECT
			cal.day_date
	    , cal.week_idnt
	    , cal.week_desc
			, cal.week_end_day_date
			, cal.month_end_week_idnt
			, cal.month_idnt
			, cal.quarter_idnt
		 	, cal.fiscal_year_num
	    , bo.RMS_SKU_NUM as sku_idnt
	    , bo.PRICE_TYPE  AS price_type
	    , country
	    , banner
	    , channel
	    ,MAX(replenishment_flag) AS rp_flag
	    ,MAX(dropship_flag) AS dropship_flag
	    ,CAST(0 AS NUMERIC) AS net_sales_units
	    ,CAST(0 AS NUMERIC) AS net_sales_dollars
	    ,CAST(0 AS NUMERIC) AS return_units
	    ,CAST(0 AS NUMERIC) AS return_dollars
	    ,CAST(0 AS NUMERIC) AS demand_units
	    ,CAST(0 AS NUMERIC) AS demand_dollars
	    ,CAST(0 AS NUMERIC) AS demand_cancel_units
	    ,CAST(0 AS NUMERIC) AS demand_cancel_dollars
	    ,CAST(0 AS NUMERIC) AS demand_dropship_units
	    ,CAST(0 AS NUMERIC) AS demand_dropship_dollars
	    ,CAST(0 AS NUMERIC) AS shipped_units
	    ,CAST(0 AS NUMERIC) AS shipped_dollars
	    ,CAST(0 AS NUMERIC) AS store_fulfill_units
	    ,CAST(0 AS NUMERIC) AS store_fulfill_dollars
	    ,CAST(0 AS NUMERIC) AS eoh_units
	    ,CAST(0 AS NUMERIC) AS eoh_dollars
	    ,CAST(0 AS NUMERIC) AS eoh_cost
	    ,CAST(0 AS NUMERIC) AS boh_units
	    ,CAST(0 AS NUMERIC) AS boh_dollars
	    ,CAST(0 AS NUMERIC) AS boh_cost
	    ,CAST(0 AS NUMERIC) AS nonsellable_units
	    ,CAST(0 AS NUMERIC) AS cost_of_goods_sold
		,CAST(0 AS NUMERIC) AS sales_pm 
	    ,CAST(0 AS NUMERIC) AS product_margin
	    ,CAST(0 AS NUMERIC) AS receipt_units
	    ,CAST(0 AS NUMERIC) AS receipt_dollars
	    ,CAST(0 AS NUMERIC) AS reserve_stock_units
	    ,CAST(0 AS NUMERIC) AS reserve_stock_dollars
	    ,CAST(0 AS NUMERIC) AS receipt_dropship_units
	    ,CAST(0 AS NUMERIC) AS receipt_dropship_dollars
	    ,SUM(backorder_units) AS backorder_units
	    ,SUM(backorder_dollars) AS backorder_dollars
	    ,CAST(0 AS NUMERIC) AS oo_units
	    ,CAST(0 AS NUMERIC) AS oo_dollars
	    ,MAX(last_receipt_date) as last_receipt_date
	FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_bo_history bo
	JOIN loc st
	  ON bo.store_num = st.store_num
	JOIN dates cal
	  ON bo.order_date_pacific = cal.day_date
	LEFT JOIN replenishment r
	  ON bo.rms_sku_num = r.sku_idnt
	 AND CAST(bo.store_num AS STRING) = r.loc_idnt
	 AND cal.day_idnt = r.day_idnt
	LEFT JOIN dropship ds
	  ON ds.day_dt = cal.day_date
	 AND bo.RMS_SKU_NUM  = ds.sku_idnt
	LEFT JOIN last_receipt lr
	  ON lr.sku_idnt = bo.RMS_SKU_NUM
	 AND lr.loc_idnt = bo.STORE_NUM
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44

);


--COLLECT STATS      PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)     ON backorder_sku


--grabbing instock metrics by sku/day


CREATE TEMPORARY TABLE IF NOT EXISTS instock_sku
AS
SELECT tw.day_date,
 tw.country,
 loc.banner,
 loc.channel,
 tw.rms_sku_num AS sku_idnt,
 tw.rp_idnt AS rp_flag,
 SUM(tw.asoh) AS asoh,
 SUM(NULLIF(tw.allocated_traffic, 0)) AS traffic,
 SUM(CASE
   WHEN tw.mc_instock_ind = 1
   THEN tw.allocated_traffic
   ELSE 0
   END) AS instock
FROM `{{params.gcp_project_id}}`.t2dl_das_twist.twist_daily AS tw
 INNER JOIN loc ON CAST(tw.store_num AS FLOAT64) = loc.store_num
 INNER JOIN dates ON tw.day_date = dates.day_date
GROUP BY tw.day_date,
 tw.country,
 loc.banner,
 loc.channel,
 sku_idnt,
 rp_flag;


--COLLECT STATS      PRIMARY INDEX (day_date, sku_idnt, channel, country)     ON instock_sku


--grabbing digital metrics by sku/day


CREATE TEMPORARY TABLE IF NOT EXISTS digital_funnel
AS
SELECT cal.day_date,
  CASE
  WHEN LOWER(f.channel) = LOWER('FULL_LINE') AND LOWER(f.channelcountry) = LOWER('US')
  THEN '120, N.COM'
  WHEN LOWER(f.channel) = LOWER('FULL_LINE') AND LOWER(f.channelcountry) = LOWER('CA')
  THEN '121, N.CA'
  WHEN LOWER(f.channel) = LOWER('RACK')
  THEN '250, OFFPRICE ONLINE'
  ELSE NULL
  END AS channel,
 f.channelcountry AS country,
  CASE
  WHEN LOWER(f.channel) = LOWER('FULL_LINE') AND LOWER(f.channelcountry) = LOWER('US')
  THEN '120'
  WHEN LOWER(f.channel) = LOWER('FULL_LINE') AND LOWER(f.channelcountry) = LOWER('CA')
  THEN '121'
  WHEN LOWER(f.channel) = LOWER('RACK')
  THEN '250'
  ELSE NULL
  END AS channel_idnt,
  CASE
  WHEN LOWER(f.channel) = LOWER('FULL_LINE')
  THEN 'FP'
  WHEN LOWER(f.channel) = LOWER('RACK')
  THEN 'OP'
  ELSE NULL
  END AS banner,
 f.rms_sku_num AS sku_idnt,
 f.rp_ind AS rp_flag,
 f.current_price_type AS price_type,
 SUM(f.order_sessions) AS order_sessions,
 SUM(f.add_to_bag_quantity) AS add_qty,
 SUM(f.product_views) AS product_views,
 SUM(f.product_views * f.pct_instock) AS instock_views,
 SUM(CASE
   WHEN f.pct_instock IS NOT NULL
   THEN f.product_views
   ELSE NULL
   END) AS scored_views,
 SUM(f.order_demand) AS order_demand,
 SUM(f.order_quantity) AS order_quantity
FROM `{{params.gcp_project_id}}`.t2dl_das_product_funnel.product_price_funnel_daily AS f
 INNER JOIN dates AS cal ON f.event_date_pacific = cal.day_date
WHERE LOWER(f.channel) <> LOWER('UNKNOWN')
GROUP BY cal.day_date,
 channel,
 country,
 channel_idnt,
 banner,
 sku_idnt,
 rp_flag,
 price_type;


--COLLECT STATS 	PRIMARY INDEX (day_date, sku_idnt, channel_idnt, country, rp_flag, price_type) 	ON digital_funnel


--udig info to support Holiday metric adds


CREATE TEMPORARY TABLE IF NOT EXISTS udig_sku
AS
SELECT sku_idnt,
 banner,
  CASE
  WHEN LOWER(banner) = LOWER('RACK') AND MIN(CASE
      WHEN LOWER(item_group) = LOWER('1, HOL23_N_CYBER_SPECIAL PURCHASE')
      THEN 1
      WHEN LOWER(item_group) = LOWER('2, CYBER 2023_US')
      THEN 2
      WHEN LOWER(item_group) = LOWER('1, HOL23_NR_CYBER_SPECIAL PURCHASE')
      THEN 3
      ELSE NULL
      END) = 3
  THEN '1, HOL23_NR_CYBER_SPECIAL PURCHASE'
  WHEN LOWER(banner) = LOWER('NORDSTROM') AND MIN(CASE
      WHEN LOWER(item_group) = LOWER('1, HOL23_N_CYBER_SPECIAL PURCHASE')
      THEN 1
      WHEN LOWER(item_group) = LOWER('2, CYBER 2023_US')
      THEN 2
      WHEN LOWER(item_group) = LOWER('1, HOL23_NR_CYBER_SPECIAL PURCHASE')
      THEN 3
      ELSE NULL
      END) = 1
  THEN '1, HOL23_N_CYBER_SPECIAL PURCHASE'
  WHEN LOWER(banner) = LOWER('NORDSTROM') AND MIN(CASE
      WHEN LOWER(item_group) = LOWER('1, HOL23_N_CYBER_SPECIAL PURCHASE')
      THEN 1
      WHEN LOWER(item_group) = LOWER('2, CYBER 2023_US')
      THEN 2
      WHEN LOWER(item_group) = LOWER('1, HOL23_NR_CYBER_SPECIAL PURCHASE')
      THEN 3
      ELSE NULL
      END) = 2
  THEN '2, CYBER 2023_US'
  ELSE NULL
  END AS udig
FROM (SELECT DISTINCT sku_idnt,
    CASE
    WHEN CAST(udig_colctn_idnt AS FLOAT64) = 50009
    THEN 'RACK'
    ELSE 'NORDSTROM'
    END AS banner,
     udig_itm_grp_idnt || ', ' || udig_itm_grp_nm AS item_group
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_usr_vws.udig_itm_grp_sku_lkup
  WHERE LOWER(udig_colctn_idnt) IN (LOWER('50008'), LOWER('50009'), LOWER('25032'))
   AND LOWER(udig_itm_grp_nm) IN (LOWER('HOL23_NR_CYBER_SPECIAL PURCHASE'), LOWER('HOL23_N_CYBER_SPECIAL PURCHASE'),
     LOWER('CYBER 2023_US'))) AS t1
GROUP BY sku_idnt,
 banner;


--COLLECT STATS 	PRIMARY INDEX (sku_idnt) 	ON udig_sku


--combine the receipt/sales/inventory


-- Sales


-- inventory


-- receipts


--cost_metrics


--backorder metrics


--oo metrics


CREATE TEMPORARY TABLE IF NOT EXISTS core_metrics AS (
    SELECT
	     s.day_date
	  	,s.week_idnt
	  	,s.week_desc
	    ,s.month_idnt
	    ,s.quarter_idnt
	    ,s.fiscal_year_num
	    ,s.month_end_week_idnt
      ,s.week_end_day_date
	    ,s.sku_idnt
	    ,s.country
	    ,s.banner
	    ,s.channel
	    ,price_type
	    ,coalesce(rp_flag, 0) as rp_flag
	    ,coalesce(dropship_flag,0) as dropship_flag
	     
	    ,SUM(COALESCE(s.net_sales_units,0)) AS net_sales_units
	    ,SUM(COALESCE(s.net_sales_dollars,0)) AS net_sales_dollars
	    ,SUM(COALESCE(s.return_units,0)) AS return_units
	    ,SUM(COALESCE(s.return_dollars,0)) AS return_dollars
	    ,SUM(COALESCE(demand_units,0)) AS demand_units
	    ,SUM(COALESCE(demand_dollars,0)) AS demand_dollars
	    ,SUM(COALESCE(demand_cancel_units,0)) AS demand_cancel_units
	    ,SUM(COALESCE(demand_cancel_dollars,0)) AS demand_cancel_dollars
	    ,SUM(COALESCE(demand_dropship_units,0)) AS demand_dropship_units
	    ,SUM(COALESCE(demand_dropship_dollars,0)) AS demand_dropship_dollars
	    ,SUM(COALESCE(shipped_units,0)) AS shipped_units
	    ,SUM(COALESCE(shipped_dollars,0)) AS shipped_dollars
	    ,SUM(COALESCE(store_fulfill_units,0)) AS store_fulfill_units
	    ,SUM(COALESCE(store_fulfill_dollars,0)) AS store_fulfill_dollars
	    
	    ,SUM(COALESCE(boh_units,0)) AS boh_units
	    ,SUM(COALESCE(boh_dollars,0)) AS boh_dollars
	    ,SUM(COALESCE(boh_cost,0)) AS boh_cost
	    ,SUM(COALESCE(eoh_units,0)) AS eoh_units
	    ,SUM(COALESCE(eoh_dollars,0)) AS eoh_dollars
	    ,SUM(COALESCE(eoh_cost,0)) AS eoh_cost
	    ,SUM(COALESCE(nonsellable_units,0)) AS nonsellable_units
	    
	    ,MAX(last_receipt_date) AS last_receipt_date
	    	    
	    ,SUM(COALESCE(cost_of_goods_sold,0)) as cost_of_goods_sold
	    ,SUM(COALESCE(product_margin,0)) as product_margin
		,SUM(COALESCE(sales_pm,0)) as sales_pm
	    
	    ,SUM(COALESCE(backorder_units,0)) as backorder_units
	    ,SUM(COALESCE(backorder_dollars,0)) as backorder_dollars
	    
	    ,SUM(COALESCE(oo_units,0)) as oo_units
	    ,SUM(COALESCE(oo_dollars,0)) as oo_dollars
	  FROM  (
    		SELECT * FROM pricetype_sku s
    			UNION ALL
    		SELECT * FROM backorder_sku
    			UNION ALL
    		SELECT * FROM oo_sku

		) s
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
);

--COLLECT STATS      PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country, price_type, rp_flag)     ON core_metrics 


--combine all metrics, add in new / cf flag, fanatics flag, and los flag and insert into base table


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.item_performance_daily_base{{params.env_suffix}}
WHERE day_date BETWEEN {{params.start_date}} AND {{params.end_date}};


-- Sales


-- inventory


-- receipts


-- OO


-- digital metrics


--    ,COALESCE(scored_views,0) AS scored_views


--cost_metrics


--backorder metrics


--instock metrics, 4/8/23 - Nulled out until dupes can be handled


--COALESCE(traffic,0) as traffic


-- COALESCE(instock,0) as instock_traffic




INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.item_performance_daily_base{{params.env_suffix}}
	SELECT
	  	s.day_date
	  	,CAST(trunc(cast(s.week_idnt as FLOAT64)) as INTEGER) as week_idnt
	  	,week_desc
	    ,CAST(trunc(cast(s.month_idnt  as FLOAT64))as INTEGER) as month_idnt
	    ,CAST(trunc(cast(s.quarter_idnt as FLOAT64)) as INTEGER) as quarter_idnt
	    ,CAST(trunc(cast(s.fiscal_year_num as FLOAT64)) as INTEGER) as year_idnt
	    ,CAST(trunc(cast(s.month_end_week_idnt as FLOAT64)) as INTEGER) as month_end_week_idnt
        ,s.week_end_day_date
	    ,s.sku_idnt
	    ,s.country
	    ,s.banner
	    ,s.channel
		,s.price_type
		,CASE WHEN f.sku_idnt IS NULL then 0 else 1 end as fanatics_flag
		,days_live
		,CAST(l.first_day_published as DATE) as days_published
		,s.rp_flag
		,s.dropship_flag
		,udig
	     
	    ,COALESCE(net_sales_units,0) AS net_sales_units
	    ,COALESCE(net_sales_dollars,0) AS net_sales_dollars
	    ,COALESCE(return_units,0) AS return_units
	    ,COALESCE(return_dollars,0) AS return_dollars
	    ,COALESCE(demand_units,0) AS demand_units
	    ,COALESCE(demand_dollars,0) AS demand_dollars
	    ,COALESCE(demand_cancel_units,0) AS demand_cancel_units
	    ,COALESCE(demand_cancel_dollars,0) AS demand_cancel_dollars
	    ,COALESCE(demand_dropship_units,0) AS demand_dropship_units
	    ,COALESCE(demand_dropship_dollars,0) AS demand_dropship_dollars
	    ,COALESCE(shipped_units,0) AS shipped_units
	    ,COALESCE(shipped_dollars,0) AS shipped_dollars
	    ,COALESCE(store_fulfill_units,0) AS store_fulfill_units
	    ,COALESCE(store_fulfill_dollars,0) AS store_fulfill_dollars
	    
	    ,COALESCE(boh_units,0) AS boh_units
	    ,COALESCE(boh_dollars,0) AS boh_dollars
	    ,COALESCE(boh_cost,0) AS boh_cost
	    ,COALESCE(eoh_units,0) AS eoh_units
	    ,COALESCE(eoh_dollars,0) AS eoh_dollars
	    ,COALESCE(eoh_cost,0) AS eoh_cost
	    ,COALESCE(nonsellable_units,0) AS nonsellable_units
		,COALESCE(asoh,0) AS asoh_units
	    
	    ,last_receipt_date AS last_receipt_date
	    
	    ,COALESCE(oo_units,0) as oo_units
	    ,COALESCE(oo_dollars,0) as oo_dollars
	    
	    ,COALESCE(add_qty,0) AS add_qty
	    ,COALESCE(product_views,0) AS product_views
	    ,COALESCE(instock_views,0) AS instock_views
	
	    ,COALESCE(order_demand,0) as order_demand
	    ,COALESCE(order_quantity,0) as order_quantity

	    
	    ,COALESCE(cost_of_goods_sold,0) as cost_of_goods_sold
	    ,COALESCE(product_margin,0) as product_margin
		,COALESCE(sales_pm,0) as sales_pm
	    
	    ,COALESCE(backorder_units,0) as backorder_units
	    ,COALESCE(backorder_dollars,0) as backorder_dollars
	    
		, 0 as traffic 
	    , 0 as instock_traffic 
	   FROM  core_metrics s
		  LEFT JOIN digital_funnel df
		  	ON s.sku_idnt = df.sku_idnt
		  	AND s.day_date = df.day_date
		  	AND s.country = df.country
		  	and s.banner = df.banner
		  	and s.channel = df.channel
				and s.rp_flag = df.rp_flag
				and s.price_type = df.price_type
		  LEFT JOIN fanatics f
				ON s.sku_idnt = f.sku_idnt
		  LEFT JOIN live_on_site l
				ON s.sku_idnt = l.sku_id
			  and s.country = l.country
			  and s.banner = l.banner
			  and s.day_date = l.day_date
			  and s.channel = l.channel
			LEFT JOIN udig_sku u
				ON u.sku_idnt = s.sku_idnt
				AND CASE WHEN u.banner = 'RACK' THEN 'OP' WHEN u.banner = 'NORDSTROM' THEN 'FP' END  = s.banner
			 LEFT JOIN instock_sku td 
		  		ON td.sku_idnt = s.sku_idnt
				AND td.day_date = s.day_date
		  		AND td.channel = s.channel
		  		AND td.banner = s.banner
		  		AND td.country = s.country
				AND td.rp_flag = s.rp_flag
;

--COLLECT STATS      PRIMARY INDEX(day_date, country, channel, sku_idnt)      ON `{{params.gcp_project_id}}`.t2dl_das_selection.item_performance_daily_base


-- end