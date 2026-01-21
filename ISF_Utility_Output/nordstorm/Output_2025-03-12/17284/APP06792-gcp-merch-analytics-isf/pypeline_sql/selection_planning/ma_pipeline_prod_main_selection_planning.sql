/*
Purpose:        Inserts data in {{environment_schema}} tables for Selection Planning
                    selection_planning_daily_base
                    selection_planning_daily
                    selection_planning_monthly
Variable(s):    {{environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
                {{start_date}} date to start for base tables
                {{end_date}} date to end for base tables
Author(s):      Sara Riker & Christine Buckler
Updated:        2024-09-20 by Asiyah Fox
*/

-- begin

/*
STEP 1: Create Base Table
    - At day/channel_country/channel_brand/customer_choice level
    - los: Live on Site CCs daily with indicators
    - sales_returns: sales what is shipped to customer, net sales (what is shipped to customer less returns), and returns in units and dollars
    - demand: what is requested from the customer
    - digital_funnel: session and view counts
    - funnel: demand and digital funnel metrics
    - receipts_daily: receipts units and dollars for digital
    - metrics: joining los, sales_returns and funnel
*/

--DROP TABLE los;
CREATE MULTISET VOLATILE TABLE los AS (
SELECT
     los.day_date
    ,los.channel_country
    ,los.channel_brand
    ,los.sku_id AS sku_idnt
    ,MAX(los.rp_ind) AS rp_ind
    ,MAX(los.ds_ind) AS ds_ind
    ,MAX(los.cl_ind) AS cl_ind
    ,MAX(los.pm_ind) AS pm_ind
    ,MAX(los.reg_ind) AS reg_ind
    ,MAX(los.flash_ind) AS flash_ind
FROM t2dl_das_site_merch.live_on_site_daily los
WHERE day_date BETWEEN {start_date} AND {end_date}
GROUP BY 1,2,3,4
)
WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel_country, channel_brand)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(day_date, sku_idnt, channel_country, channel_brand)
    ,COLUMN (channel_country, channel_brand, sku_idnt)
    ,COLUMN (day_date)
    ON los;

--DROP TABLE sales_returns;
CREATE MULTISET VOLATILE TABLE sales_returns AS (
	SELECT
	     dt.day_date
	    ,CASE WHEN st.business_unit_desc = 'N.CA' THEN 'CA'
	          ELSE 'US' END AS channel_country
	    ,CASE WHEN st.business_unit_desc IN ('N.COM', 'OMNI.COM', 'N.CA') THEN 'NORDSTROM'
	          ELSE 'NORDSTROM_RACK' END AS channel_brand
	    ,fct.rms_sku_num AS sku_idnt
	    ,SUM(fct.returns_tot_retl) AS return_dollars
	     --ORIGINAL CODE PULLS GROSS SALES AND ADDS NET LATER
	    ,SUM(COALESCE(fct.net_sales_tot_retl,0) + COALESCE(fct.returns_tot_retl,0)) AS sales_dollars
	    ,SUM(fct.returns_tot_units) AS return_units
	     --ORIGINAL CODE PULLS GROSS SALES AND ADDS NET LATER
	    ,SUM(COALESCE(fct.net_sales_tot_units,0) + COALESCE(fct.returns_tot_units,0)) AS sales_units
	FROM prd_nap_usr_vws.merch_sale_return_sku_store_day_columnar_vw fct
	JOIN prd_nap_usr_vws.day_cal_454_dim dt
		ON fct.day_num = dt.day_idnt
	JOIN prd_nap_usr_vws.store_dim st
		ON fct.store_num = st.store_num
	WHERE dt.day_date BETWEEN CAST({start_date} AS DATE) AND CAST({end_date} AS DATE)
    --DATA ONLY AVAILABLE AFTER FY21
		AND dt.day_date >= CAST('2021-01-31' AS DATE)
		AND st.business_unit_desc IN ('N.COM', 'OMNI.COM', 'N.CA', 'OFFPRICE ONLINE')
		AND fct.rms_sku_num IS NOT NULL
	GROUP BY 1,2,3,4
	UNION ALL
	SELECT
	     business_day_date AS day_date
	    ,CASE WHEN st.business_unit_desc = 'N.CA' THEN 'CA'
	          ELSE 'US' END AS channel_country
	    ,CASE WHEN st.business_unit_desc IN ('N.COM', 'OMNI.COM', 'N.CA') THEN 'NORDSTROM'
	          ELSE 'NORDSTROM_RACK' END AS channel_brand
	    ,COALESCE(sku_num, hl_sku_num) AS sku_idnt
        ,SUM(CASE WHEN line_item_activity_type_code = 'R' THEN line_net_usd_amt ELSE 0 END) * -1 AS return_dollars
        ,SUM(CASE WHEN line_item_activity_type_code = 'S' THEN line_net_usd_amt ELSE 0 END) AS sales_dollars
        ,SUM(CASE WHEN line_item_activity_type_code = 'R' THEN line_item_quantity ELSE 0 END) AS return_units
        ,SUM(CASE WHEN line_item_activity_type_code = 'S' THEN line_item_quantity ELSE 0 END) AS sales_units
	FROM prd_nap_usr_vws.retail_tran_detail_fact dtl
	JOIN prd_nap_usr_vws.store_dim st
	  ON st.store_num = dtl.intent_store_num
	WHERE dtl.business_day_date BETWEEN {start_date} AND {end_date}
    --NEED DATA PREVIOUS TO FY21
		AND dtl.business_day_date < '2021-01-31'
		AND st.business_unit_desc IN ('N.COM', 'OMNI.COM', 'N.CA', 'OFFPRICE ONLINE')
		AND sku_num IS NOT NULL
	GROUP BY 1,2,3,4
)
WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel_country, channel_brand)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_date, sku_idnt, channel_country, channel_brand)
    ,COLUMN (day_date)
    ON sales_returns;

-- Demand is what the customer requests. Sales is what is actually shipped to the customer.
--DROP TABLE demand;
CREATE MULTISET VOLATILE TABLE demand AS (
WITH orders AS (
    SELECT
      order_date_pacific AS day_date
    	,sku_num AS sku_idnt
    	,CASE WHEN source_channel_code = 'RACK' THEN 'NORDSTROM_RACK'
              WHEN source_channel_code = 'FULL_LINE' THEN 'NORDSTROM'
              ELSE 'UNKNOWN'
              END AS channel_brand
      ,source_channel_country_code AS channel_country
      ,order_tmstp_pacific
      ,CASE WHEN COALESCE(order_line_promotion_discount_amount_usd, 0) - COALESCE(order_line_employee_discount_amount_usd, 0) > 0 THEN 1
            ELSE 0 END AS promo_flag
    	,SUM(order_line_quantity) AS demand_units
    	,SUM(order_line_current_amount_usd) AS demand_dollars
    FROM prd_nap_usr_vws.order_line_detail_fact o
    WHERE order_date_pacific BETWEEN {start_date} AND {end_date}
      AND delivery_method_code <> 'PICK' -- merch demand does not include BOPUS (digital demand does)
      AND order_type_code IN ('CustInitWebOrder', 'CustInitPhoneOrder')
      AND source_platform_code <> 'POS'
      AND ((source_channel_code IN ('FULL_LINE') AND COALESCE(fraud_cancel_ind,'N')='N')  --ncom orders include cancels AND exclude fraud
      OR (source_channel_code = 'RACK' AND order_date_pacific >= '2020-11-21'))
      AND (canceled_date_pacific IS NULL OR order_date_pacific <> canceled_date_pacific) -- exclude same day cancels
    GROUP BY 1,2,3,4,5,6
),
m_time AS (
  SELECT MAX(order_tmstp_pacific) + INTERVAL '1' YEAR AS max_timestamp
  FROM orders
),
price_type AS (
    SELECT DISTINCT
         rms_sku_num AS sku_idnt
        ,channel_country
        ,channel_brand
        ,selling_retail_price_type_code AS current_price_type
        ,eff_begin_tmstp
        ,eff_end_tmstp
    FROM prd_nap_usr_vws.product_price_timeline_dim p, m_time x
    WHERE selling_channel = 'ONLINE'
      AND (eff_begin_tmstp < x.max_timestamp OR eff_end_tmstp < x.max_timestamp) -- filtering so we don't load all the records from prd_nap_usr_vws.product_price_dim
)
SELECT
     o.day_date
    ,o.sku_idnt
    ,o.channel_brand
    ,o.channel_country
    ,SUM(demand_units) AS demand_units
    ,SUM(demand_dollars) AS demand_dollars
    ,SUM(CASE WHEN current_price_type = 'REGULAR' AND promo_flag = 0 THEN demand_units ELSE 0 END) AS demand_units_reg
    ,SUM(CASE WHEN current_price_type = 'REGULAR' AND promo_flag = 0 THEN demand_dollars ELSE 0 END) AS demand_dollars_reg
FROM orders o
LEFT JOIN price_type p
  ON o.sku_idnt = p.sku_idnt
 AND o.order_tmstp_pacific BETWEEN p.eff_begin_tmstp AND p.eff_end_tmstp
 AND o.channel_country = p.channel_country
 AND o.channel_brand = p.channel_brand
GROUP BY 1,2,3,4
) 
WITH DATA
PRIMARY INDEX(day_date, sku_idnt, channel_brand, channel_country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(day_date, sku_idnt, channel_brand, channel_country)
     ON demand;

-- Note: Session data is all zero as of 6/15/22
--DROP TABLE digital_funnel;
CREATE MULTISET VOLATILE TABLE digital_funnel AS (
SELECT
     event_date_pacific AS day_date
    ,channelcountry AS channel_country
    ,CASE WHEN channel = 'RACK' THEN 'NORDSTROM_RACK'
          ELSE 'NORDSTROM' END AS channel_brand
    ,f.rms_sku_num AS sku_idnt
    ,SUM(order_sessions) AS order_sessions
    ,SUM(add_to_bag_quantity) AS add_qty
    ,SUM(add_to_bag_sessions) AS add_sessions
    ,SUM(product_views) AS product_views
    ,SUM(product_view_sessions) AS product_view_sessions
    ,SUM(product_views * pct_instock) AS instock_views
    ,SUM(CASE WHEN current_price_type = 'R' THEN order_sessions ELSE 0 END) AS order_sessions_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN add_to_bag_quantity ELSE 0 END) AS add_qty_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN add_to_bag_sessions ELSE 0 END) AS add_sessions_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN product_views ELSE 0 END) AS product_views_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN product_view_sessions ELSE 0 END) AS product_view_sessions_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN product_views * pct_instock ELSE 0 END) AS instock_views_reg
FROM t2dl_das_product_funnel.product_price_funnel_daily f
WHERE event_date_pacific BETWEEN {start_date} AND {end_date}
  AND rms_sku_num <> 'UNKNOWN'
  AND channel <> 'UNKNOWN'
GROUP BY 1,2,3,4
) 
WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)
    ,COLUMN (day_date)
    ON digital_funnel;

--DROP TABLE funnel;
CREATE MULTISET VOLATILE TABLE funnel AS (
SELECT
     COALESCE(f.day_date, d.day_date) AS day_date
    ,COALESCE(f.channel_country, d.channel_country) AS channel_country
    ,COALESCE(f.channel_brand, d.channel_brand) AS channel_brand
    ,COALESCE(f.sku_idnt, d.sku_idnt) AS sku_idnt
    ,COALESCE(d.demand_units, 0) AS demand_units
    ,COALESCE(d.demand_dollars, 0) AS demand_dollars
    ,COALESCE(f.order_sessions, 0) AS order_sessions
    ,COALESCE(f.add_qty, 0) AS add_qty
    ,COALESCE(f.add_sessions, 0) AS add_sessions
    ,COALESCE(f.product_views, 0) AS product_views
    ,COALESCE(f.product_view_sessions, 0) AS product_view_sessions
    ,COALESCE(f.instock_views, 0) AS instock_views
    ,COALESCE(d.demand_units_reg, 0) AS demand_units_reg
    ,COALESCE(d.demand_dollars_reg, 0) AS demand_dollars_reg
    ,COALESCE(f.order_sessions_reg, 0) AS order_sessions_reg
    ,COALESCE(f.add_qty_reg, 0) AS add_qty_reg
    ,COALESCE(f.add_sessions_reg, 0) AS add_sessions_reg
    ,COALESCE(f.product_views_reg, 0) AS product_views_reg
    ,COALESCE(f.product_view_sessions_reg, 0) AS product_view_sessions_reg
    ,COALESCE(f.instock_views_reg, 0) AS instock_views_reg
FROM digital_funnel f
FULL OUTER JOIN demand d
  ON f.sku_idnt = d.sku_idnt
 AND f.day_date = d.day_date
 AND f.channel_brand = d.channel_brand
 AND f.channel_country = d.channel_country
)
WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)
    ,COLUMN (day_date)
    ON funnel;

--DROP TABLE metrics;
CREATE MULTISET VOLATILE TABLE metrics AS (
WITH los_funnel AS (
    SELECT
         COALESCE(l.day_date, f.day_date) AS day_date
        ,COALESCE(l.channel_country, f.channel_country) AS channel_country
        ,COALESCE(l.channel_brand, f.channel_brand) AS channel_brand
        ,COALESCE(l.sku_idnt, f.sku_idnt) AS sku_idnt
        ,CASE WHEN l.sku_idnt IS NOT NULL THEN 1 ELSE 0 END AS los_flag
        ,l.rp_ind
        ,l.ds_ind
        ,l.cl_ind
        ,l.pm_ind
        ,l.reg_ind
        ,l.flash_ind
        -- funnel
        ,COALESCE(f.demand_units, 0) AS demand_units
        ,COALESCE(f.demand_dollars, 0) AS demand_dollars
        ,COALESCE(f.order_sessions, 0) AS order_sessions
        ,COALESCE(f.add_qty, 0) AS add_qty
        ,COALESCE(f.add_sessions, 0) AS add_sessions
        ,COALESCE(f.product_views, 0) AS product_views
        ,COALESCE(f.product_view_sessions, 0) AS product_view_sessions
        ,COALESCE(f.instock_views, 0) AS instock_views
        -- reg price funnel
        ,COALESCE(f.demand_units_reg, 0) AS demand_units_reg
        ,COALESCE(f.demand_dollars_reg, 0) AS demand_dollars_reg
        ,COALESCE(f.order_sessions_reg, 0) AS order_sessions_reg
        ,COALESCE(f.add_qty_reg, 0) AS add_qty_reg
        ,COALESCE(f.add_sessions_reg, 0) AS add_sessions_reg
        ,COALESCE(f.product_views_reg, 0) AS product_views_reg
        ,COALESCE(f.product_view_sessions_reg, 0) AS product_view_sessions_reg
        ,COALESCE(f.instock_views_reg, 0) AS instock_views_reg
    FROM los l
    FULL OUTER JOIN funnel f
      ON l.day_date = f.day_date
     AND l.channel_country = f.channel_country
     AND l.channel_brand = f.channel_brand
     AND l.sku_idnt = f.sku_idnt
)
SELECT
     COALESCE(l.day_date, sr.day_date) AS day_date
    ,COALESCE(l.channel_country, sr.channel_country) AS channel_country
    ,COALESCE(l.channel_brand, sr.channel_brand) AS channel_brand
    ,COALESCE(l.sku_idnt, sr.sku_idnt) AS sku_idnt
    ,COALESCE(l.los_flag, 0) AS los_flag
    ,l.rp_ind
    ,l.ds_ind
    ,l.cl_ind
    ,l.pm_ind
    ,l.reg_ind
    ,l.flash_ind
    -- funnel
    ,COALESCE(l.demand_units, 0) AS demand_units
    ,COALESCE(l.demand_dollars, 0) AS demand_dollars
    ,COALESCE(l.order_sessions, 0) AS order_sessions
    ,COALESCE(l.add_qty, 0) AS add_qty
    ,COALESCE(l.add_sessions, 0) AS add_sessions
    ,COALESCE(l.product_views, 0) AS product_views
    ,COALESCE(l.product_view_sessions, 0) AS product_view_sessions
    ,COALESCE(l.instock_views, 0) AS instock_views
    -- reg price funnel
    ,COALESCE(l.demand_units_reg, 0) AS demand_units_reg
    ,COALESCE(l.demand_dollars_reg, 0) AS demand_dollars_reg
    ,COALESCE(l.order_sessions_reg, 0) AS order_sessions_reg
    ,COALESCE(l.add_qty_reg, 0) AS add_qty_reg
    ,COALESCE(l.add_sessions_reg, 0) AS add_sessions_reg
    ,COALESCE(l.product_views_reg, 0) AS product_views_reg
    ,COALESCE(l.product_view_sessions_reg, 0) AS product_view_sessions_reg
    ,COALESCE(l.instock_views_reg, 0) AS instock_views_reg
    -- sales returns
    ,COALESCE(sr.return_dollars, 0) AS return_dollars
    ,COALESCE(sr.sales_dollars, 0) AS sales_dollars
    ,COALESCE(sr.return_units, 0) AS return_units
    ,COALESCE(sr.sales_units, 0) AS sales_units
    ,COALESCE(sr.sales_dollars, 0) - COALESCE(sr.return_dollars, 0) AS sales_net_dollars 
    ,COALESCE(sr.sales_units, 0) - COALESCE(sr.return_units, 0) AS sales_net_units
FROM los_funnel l
FULL OUTER JOIN sales_returns sr
  ON l.day_date = sr.day_date
 AND l.channel_country = sr.channel_country
 AND l.channel_brand = sr.channel_brand
 AND l.sku_idnt = sr.sku_idnt
) 
WITH DATA
PRIMARY INDEX(day_date, sku_idnt, channel_brand, channel_country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)
    ,COLUMN (day_date)
    ON metrics;

DELETE FROM {environment_schema}.selection_planning_daily_base{env_suffix} WHERE day_date BETWEEN {start_date} AND {end_date};
INSERT INTO {environment_schema}.selection_planning_daily_base{env_suffix}
WITH metrics_receipt AS (
SELECT
    l.day_date AS day_date
    ,l.sku_idnt AS sku_idnt
    ,l.channel_country AS channel_country
    ,l.channel_brand AS channel_brand
    ,'DIGITAL' AS channel
    ,COALESCE(l.los_flag, 0) AS los_flag
    ,l.rp_ind
    ,l.ds_ind
    ,l.cl_ind
    ,l.pm_ind
    ,l.reg_ind
    ,l.flash_ind
    --funnel
    ,COALESCE(l.demand_units, 0) AS demand_units
    ,COALESCE(l.demand_dollars, 0) AS demand_dollars
    ,COALESCE(l.order_sessions, 0) AS order_sessions
    ,COALESCE(l.add_qty, 0) AS add_qty
    ,COALESCE(l.add_sessions, 0) AS add_sessions
    ,COALESCE(l.product_views, 0) AS product_views
    ,COALESCE(l.product_view_sessions, 0) AS product_view_sessions
    ,COALESCE(l.instock_views, 0) AS instock_views
    --reg price funnel
    ,COALESCE(l.demand_units_reg, 0) AS demand_units_reg
    ,COALESCE(l.demand_dollars_reg, 0) AS demand_dollars_reg
    ,COALESCE(l.order_sessions_reg, 0) AS order_sessions_reg
    ,COALESCE(l.add_qty_reg, 0) AS add_qty_reg
    ,COALESCE(l.add_sessions_reg, 0) AS add_sessions_reg
    ,COALESCE(l.product_views_reg, 0) AS product_views_reg
    ,COALESCE(l.product_view_sessions_reg, 0) AS product_view_sessions_reg
    ,COALESCE(l.instock_views_reg, 0) AS instock_views_reg
    --Sales & Returns
    ,COALESCE(l.return_dollars, 0) AS return_dollars
    ,COALESCE(l.sales_dollars, 0) AS sales_dollars
    ,COALESCE(l.return_units, 0) AS return_units
    ,COALESCE(l.sales_units, 0) AS sales_units
    ,COALESCE(l.sales_net_dollars , 0) AS sales_net_dollars 
    ,COALESCE(l.sales_net_units, 0) AS sales_net_units
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM metrics l
)
SELECT 
     a.day_date
    ,a.sku_idnt
    ,cc.customer_choice
    ,cal.week_num AS wk_idnt
    ,cal.week_454_num AS wk_454
    ,cal.week_of_fyr
    ,cal.month_454_num AS mnth_454
    ,cal.month_num AS mnth_idnt
    ,cal.quarter_454_num AS qtr_454
    ,cal.year_num AS yr_454
    ,a.channel_country
    ,a.channel_brand
    ,COALESCE(cct.cc_type, 'CF') AS cc_type
    ,COALESCE(a.los_flag, 0) AS los_flag
    ,a.rp_ind
    ,a.ds_ind
    ,a.cl_ind
    ,a.pm_ind
    ,a.reg_ind
    ,a.flash_ind
    --funnel
    ,a.demand_units
    ,a.demand_dollars
    ,a.order_sessions
    ,a.add_qty
    ,a.add_sessions
    ,a.product_views
    ,a.product_view_sessions
    ,a.instock_views
    --reg price funnel
    ,a.demand_units_reg
    ,a.demand_dollars_reg
    ,a.order_sessions_reg
    ,a.add_qty_reg
    ,a.add_sessions_reg
    ,a.product_views_reg
    ,a.product_view_sessions_reg
    ,a.instock_views_reg
    --Sales & Returns
    ,a.return_dollars
    ,a.sales_dollars
    ,a.return_units
    ,a.sales_units
    ,a.sales_net_dollars 
    ,a.sales_net_units
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM metrics_receipt a
JOIN prd_nap_usr_vws.day_cal cal
  ON a.day_date = cal.day_date
JOIN t2dl_das_assortment_dim.sku_cc_lkp cc
  ON a.sku_idnt = cc.sku_idnt
 AND a.channel_country = cc.channel_country
LEFT JOIN t2dl_das_assortment_dim.cc_type_realigned cct
  ON cc.customer_choice = cct.customer_choice
 AND a.channel_country = cct.channel_country
 AND a.channel_brand  = cct.channel_brand
 AND (CASE WHEN a.channel_country = 'US' THEN a.channel END = cct.channel OR
      CASE WHEN a.channel_country = 'CA' THEN 'STORE' END = cct.channel)
 AND cct.channel = 'DIGITAL'
 AND cal.month_num = cct.mnth_idnt
WHERE cal.day_date BETWEEN {start_date} AND {end_date};

COLLECT STATS
     COLUMN (day_date)
    ,COLUMN (day_date,sku_idnt,customer_choice,channel_country,channel_brand)
    ON {environment_schema}.selection_planning_daily_base{env_suffix};

-- end

/*
STEP 2: Create Daily Table
    - At day/channel_country/channel_brand/dept/category level
    - daily_hierarchy: join selection_planning_daily_base and hierarchy
*/

-- daily sku level with calendar and hierarchy data
--DROP TABLE daily_hierarchy;
CREATE MULTISET VOLATILE TABLE daily_hierarchy AS (
WITH hrchy AS (
  SELECT
    h.*
    ,dense_rank() OVER (PARTITION BY h.dept_idnt ORDER BY COALESCE(h.quantrix_category, 'OTHER')) AS cat_idnt 
    -- cat_idnt created to easily join in python with numbers instead of varchars
  FROM t2dl_das_assortment_dim.assortment_hierarchy h
)
SELECT 
     base.*
    ,h.supplier_idnt
    ,h.div_idnt
    ,h.div_desc
    ,h.grp_idnt
    ,h.grp_desc
    ,h.dept_idnt
    ,h.dept_desc
    ,h.class_idnt
    ,h.class_desc
    ,h.sbclass_desc
    ,h.sbclass_idnt
    ,h.quantrix_category AS category
    ,h.cat_idnt
    ,cal.day_idnt
    ,cal.month_start_day_idnt
    ,cal.month_end_day_idnt
    ,cal.month_start_day_date AS mnth_start_day_date
FROM {environment_schema}.selection_planning_daily_base{env_suffix} AS base
JOIN hrchy h
  ON base.sku_idnt = h.sku_idnt
 AND base.channel_country = h.channel_country
LEFT JOIN prd_nap_usr_vws.day_cal_454_dim cal
  ON base.day_date = cal.day_date
WHERE cal.month_idnt >= 201901
 AND cal.month_idnt <= (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE)
 AND fanatics_ind = 0 -- do not include fanatics skus
 AND marketplace_ind = 0 -- do not include marketplace skus
) 
WITH DATA
PRIMARY INDEX (channel_country, customer_choice)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
    COLUMN (customer_choice, mnth_454, mnth_idnt, qtr_454 ,yr_454 ,channel_country ,channel_brand, div_idnt, grp_idnt, dept_idnt, category, cat_idnt)
    ,COLUMN (customer_choice, mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt, category, cat_idnt, mnth_start_day_date)
    ,COLUMN (day_date, customer_choice, wk_idnt, wk_454, week_of_fyr, mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt, category, cat_idnt)
    ,COLUMN (mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt ,cat_idnt)
    ,COLUMN (day_date, wk_idnt, wk_454, week_of_fyr, mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt, category, cat_idnt)
    ,COLUMN (mnth_454, mnth_idnt, qtr_454, yr_454, channel_country, channel_brand, div_idnt, grp_idnt, dept_idnt, cat_idnt, mnth_start_day_date)
    ,COLUMN (mnth_start_day_date)
    ,COLUMN (sku_idnt, customer_choice)
    ,COLUMN (cat_idnt)
    ,COLUMN (category)
    ,COLUMN (channel_country)
    ,COLUMN (customer_choice)
    ,COLUMN (day_date)
    ,COLUMN (dept_idnt)
    ,COLUMN (div_idnt)
    ,COLUMN (grp_idnt)
    ,COLUMN (mnth_idnt)
    ,COLUMN (mnth_454)
    ,COLUMN (qtr_454)
    ,COLUMN (wk_454)
    ,COLUMN (wk_idnt)
    ,COLUMN (week_of_fyr)
    ,COLUMN (yr_454)
    ON daily_hierarchy;

-- daily CC level 
DELETE FROM {environment_schema}.selection_planning_daily{env_suffix} ALL;
INSERT INTO {environment_schema}.selection_planning_daily{env_suffix}
SELECT
     day_date
    ,wk_idnt
    ,wk_454
    ,week_of_fyr
    ,mnth_454
    ,mnth_idnt
    ,qtr_454
    ,yr_454
    ,channel_country
    ,channel_brand
    ,div_idnt
    ,grp_idnt
    ,dept_idnt
    ,cat_idnt
    ,category
    --choice counts
    ,COUNT(DISTINCT customer_choice) AS customer_choices
    ,COUNT(DISTINCT CASE WHEN los_flag = 1 THEN customer_choice END) AS customer_choices_los
    ,COUNT(DISTINCT CASE WHEN cc_type = 'NEW' THEN customer_choice END) AS customer_choices_new
    ,COUNT(DISTINCT CASE WHEN day_idnt = month_start_day_idnt AND los_flag = 1 THEN customer_choice END) AS bop_est
    ,COUNT(DISTINCT CASE WHEN day_idnt = month_end_day_idnt AND los_flag = 1 THEN customer_choice END) AS eop_est
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM daily_hierarchy
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;

COLLECT STATS
     PRIMARY INDEX(day_date, mnth_454, yr_454, dept_idnt, cat_idnt)
     ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt) 
     ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt, cat_idnt) 
     ,COLUMN (cat_idnt) 
     ,COLUMN (dept_idnt)
     ,COLUMN (yr_454) 
     ,COLUMN (mnth_454)
     ON {environment_schema}.selection_planning_daily{env_suffix};

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
CREATE MULTISET VOLATILE TABLE calendar_realigned AS (
		SELECT 
			day_date
			, week_idnt
			, month_idnt
			, week_end_day_date
			, week_start_day_date
			, month_start_day_date
			, month_end_day_date
			, fiscal_month_num AS mnth_454
      , fiscal_quarter_num AS qtr_454
			, fiscal_year_num AS yr_454
		FROM prd_nap_vws.realigned_date_lkup_vw AS rdlv 
		WHERE day_date <= current_date
	
		UNION ALL

		SELECT 
			day_date
			, week_idnt
			, month_idnt 
			, week_end_day_date
			, week_start_day_date
			, month_start_day_date
			, month_end_day_date
			, fiscal_month_num AS mnth_454
      		, fiscal_quarter_num AS qtr_454
			, fiscal_year_num AS yr_454
		FROM prd_nap_usr_vws.DAY_CAL_454_DIM AS dcd 
		WHERE fiscal_year_num NOT IN (SELECT DISTINCT fiscal_year_num FROM prd_nap_vws.realigned_date_lkup_vw)
		AND day_date <= current_date
)
WITH DATA
PRIMARY INDEX (day_date)
ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE calendar AS (
  SELECT 
    cal.*
    , t.day_date AS day_date_true
    , t.week_idnt AS week_idnt_true
    , t.month_idnt AS month_idnt_true
    , t.month_end_day_date AS month_end_day_date_true
    , t.week_end_day_date AS week_end_day_date_true
  FROM calendar_realigned AS cal 
  FULL OUTER JOIN
  (
      SELECT 
        dcd.day_date
        , dcd.day_idnt
        , dcd.week_idnt
        , dcd.month_idnt 
        , dcd.month_end_day_date
        , dcd.week_end_day_date
      FROM prd_nap_usr_vws.DAY_CAL_454_DIM AS dcd 
      WHERE dcd.fiscal_year_num >= 2019
        AND dcd.day_date <= current_Date
  ) AS t 
  ON cal.day_date = t.day_date
)
WITH DATA
PRIMARY INDEX (day_date)
ON COMMIT PRESERVE ROWS;

--DROP TABLE bop_cnts;
CREATE MULTISET VOLATILE TABLE bop_cnts AS (
	WITH bop_status AS (
	  SELECT
	      cc.customer_choice
	      ,h.dept_idnt
	      ,h.quantrix_category AS category
	      ,sku.channel_country
	      ,sku.channel_brand
	      ,c.month_idnt AS mnth_idnt
	      ,CASE WHEN (is_online_purchasable = 'Y' AND eff_begin_tmstp <= CAST(day_date AS TIMESTAMP)) THEN 1 ELSE 0 END AS bop_status
	  FROM prd_nap_usr_vws.product_online_purchasable_item_dim sku -- get status
	  JOIN calendar c -- get calendar dates
	    ON day_date BETWEEN CAST(eff_begin_tmstp AS DATE) AND CAST(eff_end_tmstp AS DATE)
	  JOIN t2dl_das_assortment_dim.sku_cc_lkp cc -- get customer_choice
      ON sku.rms_sku_num = cc.sku_idnt
	   AND sku.channel_country = cc.channel_country
    JOIN t2dl_das_assortment_dim.assortment_hierarchy h -- get dept and category
      ON sku.rms_sku_num = h.sku_idnt
 	   AND cc.customer_choice = h.customer_choice
 	   AND sku.channel_country = h.channel_country
	  WHERE c.month_idnt BETWEEN 202104 -- data starts on 2021-05-14
	   AND (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE)
	   AND day_date = month_start_day_date
     AND fanatics_ind = 0 -- do not include fanatics skus
     AND marketplace_ind = 0 -- do not include marketplace skus
	  QUALIFY ROW_NUMBER() OVER(PARTITION BY rms_sku_num, sku.channel_country, sku.channel_brand, day_date ORDER BY eff_begin_tmstp ASC) = 1
	)
	SELECT
	    dept_idnt
	    ,category
	    ,channel_country
	    ,channel_brand
	    ,mnth_idnt
	    ,COUNT(DISTINCT CASE WHEN bop_status = 1 THEN customer_choice END) AS ccs_mnth_bop
	FROM bop_status
	GROUP BY dept_idnt, category, channel_country, channel_brand, mnth_idnt
)
WITH DATA
PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt)
	ON bop_cnts;

--DROP TABLE eop_cnts;
CREATE MULTISET VOLATILE TABLE eop_cnts AS (
	WITH eop_status AS (
	  SELECT
	      cc.customer_choice
	      ,h.dept_idnt
	      ,h.quantrix_category AS category
	      ,sku.channel_country
	      ,sku.channel_brand
	      ,c.month_idnt AS mnth_idnt
	      ,CASE WHEN is_online_purchasable = 'Y' THEN 1 ELSE 0 END AS eop_status
	  FROM prd_nap_usr_vws.product_online_purchasable_item_dim sku -- get status
	  JOIN calendar c -- get calendar dates
	    ON day_date BETWEEN CAST(eff_begin_tmstp AS DATE) AND CAST(eff_end_tmstp AS DATE)
	  JOIN t2dl_das_assortment_dim.sku_cc_lkp cc -- get customer_choice
      ON sku.rms_sku_num = cc.sku_idnt
	   AND sku.channel_country = cc.channel_country
    JOIN t2dl_das_assortment_dim.assortment_hierarchy h -- get dept and category
      ON sku.rms_sku_num = h.sku_idnt
 	   AND cc.customer_choice = h.customer_choice
 	   AND sku.channel_country = h.channel_country
	  WHERE c.month_idnt BETWEEN 202104 -- data starts on 2021-05-14
	   AND (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = CURRENT_DATE)
	   AND day_date = month_end_day_date
     AND fanatics_ind = 0 -- do not include fanatics skus
     AND marketplace_ind = 0 -- do not include marketplace skus
	  QUALIFY ROW_NUMBER() OVER(PARTITION BY rms_sku_num, sku.channel_country, sku.channel_brand, day_date ORDER BY eff_begin_tmstp DESC) = 1
	)
	SELECT
	    dept_idnt
	    ,category
	    ,channel_country
	    ,channel_brand
	    ,mnth_idnt
	    ,COUNT(DISTINCT CASE WHEN eop_status = 1 THEN customer_choice END) AS ccs_mnth_eop
	FROM eop_status
	GROUP BY dept_idnt, category, channel_country, channel_brand, mnth_idnt
)
WITH DATA
PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt)
	ON eop_cnts;

--DROP TABLE cc_bop_eop_cnts;
CREATE MULTISET VOLATILE TABLE cc_bop_eop_cnts AS (
SELECT
    e.dept_idnt
    ,e.category
    ,e.channel_country
    ,e.channel_brand
    ,e.mnth_idnt
    ,COALESCE(ccs_mnth_bop, 0) AS ccs_mnth_bop -- always zero for for first month that appears
    ,ccs_mnth_eop
FROM bop_cnts b
FULL OUTER JOIN eop_cnts e
	ON b.dept_idnt = e.dept_idnt
    AND b.category = e.category
    AND b.channel_country = e.channel_country
    AND b.channel_brand = e.channel_brand
    AND b.mnth_idnt = e.mnth_idnt
)
WITH DATA
PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
  PRIMARY INDEX (dept_idnt, category, channel_country, channel_brand, mnth_idnt)
  ON cc_bop_eop_cnts;

--Break out receipts into groupings, blend MADM with NAP to get full history, and roll up to month level
CREATE MULTISET VOLATILE TABLE receipts AS (
  --NAP receipts from Feb Week 1 2023 onward
	WITH dates AS (
	SELECT 
		week_idnt_true
		  ,month_idnt AS mnth_idnt
		  ,month_start_day_date AS mnth_start_day_date
		  ,mnth_454
   		,qtr_454
    	,yr_454
	FROM calendar
	WHERE mnth_idnt >= 201901
	GROUP BY 1,2,3,4,5,6
	),
  receipts_base AS (
		SELECT
   		    d.mnth_idnt
   		    , d.mnth_start_day_date
   		    , mnth_454
   		    , qtr_454
   		    , yr_454
          ,channel_country
          ,channel_brand
          ,r.sku_idnt
          ,SUM(receipt_po_units + receipt_ds_units) AS rcpt_units
          ,SUM(receipt_po_retail + receipt_ds_retail) AS rcpt_dollars
          ,SUM(CASE WHEN receipt_po_units > 0 THEN receipt_po_units ELSE 0 END) AS rcpt_bought_units
          ,SUM(CASE WHEN receipt_po_units > 0 THEN receipt_po_retail ELSE 0 END) AS rcpt_bought_dollars
          ,SUM(CASE WHEN receipt_ds_units > 0 AND COALESCE(receipt_po_units, 0) = 0 THEN receipt_ds_units ELSE 0 END) AS rcpt_ds_units
          ,SUM(CASE WHEN receipt_ds_units > 0 AND COALESCE(receipt_po_units, 0) = 0 THEN receipt_ds_retail ELSE 0 END) AS rcpt_ds_dollars
		FROM t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact AS r
		JOIN prd_nap_usr_vws.price_store_dim_vw AS st
      ON r.store_num = st.store_num
    JOIN dates d ON r.week_num = d.week_idnt_true
		WHERE selling_channel = 'ONLINE' -- DIGITAL only
      AND r.channel_num NOT IN (260, 310, 920, 921, 922, 930, 940, 990) 
      -- removing Reserve Stock (260: RACK RESERVE STOCK WAREHOUSE, 310: RESERVE STOCK), DCs (920: US DC, 921: Canada DC, 922: OP Canada DC), 930: NQC, 940: NPG, 990: FACO
		  AND d.mnth_idnt >= 202301
		GROUP BY 1,2,3,4,5,6,7,8

		UNION ALL 

    --MADM receipts prior to Feb Week 1 2023 (MADM table only contains dates from Feb Week 1 2019 through Jan Week 4 2023)
    SELECT
      d.mnth_idnt
      , d.mnth_start_day_date
   	  , mnth_454
   	  , qtr_454
   	  , yr_454
      ,channel_country
      ,channel_brand
      ,r.sku_idnt
      ,SUM(receipt_po_units + receipt_ds_units) AS rcpt_units
      ,SUM(receipt_po_retail + receipt_ds_retail) AS rcpt_dollars
      ,SUM(CASE WHEN receipt_po_units > 0 THEN receipt_po_units ELSE 0 END) AS rcpt_bought_units
      ,SUM(CASE WHEN receipt_po_units > 0 THEN receipt_po_retail ELSE 0 END) AS rcpt_bought_dollars
      ,SUM(CASE WHEN receipt_ds_units > 0 AND COALESCE(receipt_po_units, 0) = 0 THEN receipt_ds_units ELSE 0 END) AS rcpt_ds_units
      ,SUM(CASE WHEN receipt_ds_units > 0 AND COALESCE(receipt_po_units, 0) = 0 THEN receipt_ds_retail ELSE 0 END) AS rcpt_ds_dollars
		FROM t2dl_das_assortment_dim.receipt_sku_loc_week_agg_fact_madm AS r
		JOIN prd_nap_usr_vws.price_store_dim_vw AS st
      ON r.store_num = st.store_num
    JOIN dates d ON r.week_num = d.week_idnt_true
		WHERE selling_channel = 'ONLINE' -- DIGITAL only
      AND r.channel_num NOT IN (260, 310, 920, 921, 922, 930, 940, 990) 
      -- removing Reserve Stock (260: RACK RESERVE STOCK WAREHOUSE, 310: RESERVE STOCK), DCs (920: US DC, 921: Canada DC, 922: OP Canada DC), 930: NQC, 940: NPG, 990: FACO
		  AND d.mnth_idnt >= 201901 AND d.mnth_idnt < 202301
		GROUP BY 1,2,3,4,5,6,7,8
	),
  hrchy AS (
    SELECT
        h.*
        ,dense_rank() OVER (PARTITION BY h.dept_idnt ORDER BY COALESCE(h.quantrix_category, 'OTHER')) AS cat_idnt 
        -- cat_idnt created to easily join in python with numbers instead of varchars
    FROM t2dl_das_assortment_dim.assortment_hierarchy h
	)
	SELECT 
		r.mnth_idnt
		,mnth_start_day_date
		,mnth_454
		,qtr_454
		,yr_454
		,r.channel_country
		,r.channel_brand
		,h.div_idnt
		,h.grp_idnt 
		,h.dept_idnt 
		,h.class_idnt
		,h.cat_idnt
		,COALESCE(h.quantrix_category, 'UNKNOWN') AS category
		,h.customer_choice
    ,COALESCE(cc_type , 'CF') AS cc_type
    ,r.sku_idnt
		,rcpt_units
		,rcpt_dollars
		,rcpt_bought_units
		,rcpt_bought_dollars
		,rcpt_ds_units
		,rcpt_ds_dollars
		,CASE WHEN COALESCE(cc_type.cc_type, 'CF') = 'NEW' THEN rcpt_bought_units ELSE 0 END AS rcpt_new_units
    ,CASE WHEN COALESCE(cc_type.cc_type, 'CF') = 'NEW' THEN rcpt_bought_dollars  ELSE 0 END AS rcpt_new_dollars
    ,CASE WHEN COALESCE(cc_type.cc_type, 'CF') = 'CF' THEN rcpt_bought_units  ELSE 0 END AS rcpt_cf_units
    ,CASE WHEN COALESCE(cc_type.cc_type, 'CF') = 'CF' THEN rcpt_bought_dollars   ELSE 0 END AS rcpt_cf_dollars
	FROM receipts_base AS r
	LEFT JOIN t2dl_das_assortment_dim.sku_cc_lkp AS cc
	  ON cc.sku_idnt = r.sku_idnt
	  AND r.channel_country = cc.channel_country
	LEFT JOIN t2dl_das_assortment_dim.cc_type_realigned AS cc_type
	  ON cc.customer_choice = cc_type.customer_choice
	  AND cc.channel_country = cc_type.channel_country
	  AND r.channel_brand = cc_type.channel_brand
    AND cc_type.channel = 'DIGITAL'
	  AND r.mnth_idnt = cc_type.mnth_idnt
	LEFT JOIN hrchy AS h
    ON r.sku_idnt = h.sku_idnt 
	  AND r.channel_country = h.channel_country
  WHERE fanatics_ind = 0 --removing fanatics skus from receipts for selection planning
    AND marketplace_ind = 0 -- do not include marketplace skus
)
WITH DATA PRIMARY INDEX(mnth_idnt, channel_country, sku_idnt) 
ON COMMIT PRESERVE ROWS;
COLLECT STATS PRIMARY INDEX (mnth_idnt,channel_country,sku_idnt) ON receipts;

--monthly CC level metrics
--DROP TABLE monthly;
CREATE MULTISET VOLATILE TABLE monthly AS (
WITH monthly_hierarchy AS (
  SELECT
	   cal.mnth_454
    ,cal.month_idnt AS mnth_idnt
    ,cal.month_start_day_date AS mnth_start_day_date
	  ,cal.qtr_454
	  ,cal.yr_454
	  ,channel_country
	  ,channel_brand
	  ,div_idnt
	  ,grp_idnt
	  ,dept_idnt
	  ,cat_idnt
	  ,sku_idnt
	  ,customer_choice
    ,cc_type
    ,COALESCE(category, 'UNKNOWN') AS category
    ,MAX(los_flag) AS los_flag
    ,MAX(ds_ind) AS ds_ind
    ,MAX(rp_ind) AS rp_ind
    --funnel
    ,SUM(order_sessions) AS order_sessions
    ,SUM(add_qty) AS add_qty
    ,SUM(add_sessions) AS add_sessions
    ,SUM(product_views) AS product_views
    ,SUM(product_view_sessions) AS product_view_sessions
    ,SUM(instock_views) AS instock_views
    ,SUM(demand_units) AS demand_units
    ,SUM(demand_dollars) AS demand_dollars
    ,SUM(CASE WHEN cc_type = 'NEW' THEN demand_units ELSE 0 END) AS demand_units_new
    ,SUM(CASE WHEN cc_type = 'NEW' THEN demand_dollars ELSE 0 END) AS demand_dollars_new
    --reg price funnel
    ,SUM(order_sessions_reg) AS order_sessions_reg
    ,SUM(add_qty_reg) AS add_qty_reg
    ,SUM(add_sessions_reg) AS add_sessions_reg
    ,SUM(product_views_reg) AS product_views_reg
    ,SUM(product_view_sessions_reg) AS product_view_sessions_reg
    ,SUM(instock_views_reg) AS instock_views_reg
    ,SUM(demand_units_reg) AS demand_units_reg
    ,SUM(demand_dollars_reg) AS demand_dollars_reg
    --sales & returns
    ,SUM(COALESCE(return_dollars, 0)) AS return_dollars
    ,SUM(COALESCE(return_units, 0)) AS return_units
    ,SUM(COALESCE(sales_dollars, 0)) AS sales_dollars
    ,SUM(COALESCE(sales_units, 0)) AS sales_units
    ,SUM(COALESCE(sales_net_dollars , 0)) AS sales_net_dollars 
    ,SUM(COALESCE(sales_net_units, 0)) AS sales_net_units
  FROM daily_hierarchy AS c
  JOIN calendar AS cal
    ON c.day_date = cal.day_date 
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
	)
  SELECT 
	   COALESCE(c.mnth_454, r.mnth_454) AS mnth_454
	  ,COALESCE(c.mnth_idnt, r.mnth_idnt) AS mnth_idnt
	  ,COALESCE(c.mnth_start_day_date,r.mnth_start_day_date) AS mnth_start_day_date
	  ,COALESCE(c.qtr_454, r.qtr_454) AS qtr_454
	  ,COALESCE(c.yr_454, r.yr_454) AS yr_454
	  ,COALESCE(c.channel_country, r.channel_country) AS channel_country
	  ,COALESCE(c.channel_brand, r.channel_brand) AS channel_brand
	  ,COALESCE(c.div_idnt, r.div_idnt) AS div_idnt
	  ,COALESCE(c.grp_idnt, r.grp_idnt) AS grp_idnt
	  ,COALESCE(c.dept_idnt, r.dept_idnt) AS dept_idnt
	  ,COALESCE(c.cat_idnt, r.cat_idnt) AS cat_idnt
	  ,COALESCE(c.category, r.category) AS category
	  --counts
	  ,COUNT(DISTINCT COALESCE(c.customer_choice,r.customer_choice)) AS ccs_mnth
	  ,COUNT(DISTINCT CASE WHEN c.los_flag = 1 THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_los
	  ,COUNT(DISTINCT CASE WHEN COALESCE(c.cc_type, r.cc_type) = 'NEW' THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_new
	  ,COUNT(DISTINCT CASE WHEN COALESCE(c.cc_type, r.cc_type) = 'CF' THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_cf
	  ,COUNT(DISTINCT CASE WHEN COALESCE(c.cc_type, r.cc_type) = 'CF' AND c.rp_ind = 1 THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_cf_rp
	  ,COUNT(DISTINCT CASE WHEN c.rp_ind = 1 THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_rp
	  ,COUNT(DISTINCT CASE WHEN c.ds_ind = 1 THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_ds
	  ,COUNT(DISTINCT CASE WHEN COALESCE(c.cc_type, r.cc_type) = 'NEW' AND rcpt_bought_units > 0 THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_rcpt_new
	  ,COUNT(DISTINCT CASE WHEN COALESCE(c.cc_type, r.cc_type) = 'CF' AND rcpt_bought_units > 0 THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_rcpt_cf
	  ,COUNT(DISTINCT CASE WHEN COALESCE(c.cc_type, r.cc_type) = 'NEW' AND rcpt_ds_units > 0 THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_rcpt_new_ds
	  ,COUNT(DISTINCT CASE WHEN COALESCE(c.cc_type, r.cc_type) = 'CF' AND rcpt_ds_units > 0 THEN COALESCE(c.customer_choice,r.customer_choice) END) AS ccs_mnth_rcpt_cf_ds
	  --funnel
	  ,SUM(order_sessions) AS order_sessions
	  ,SUM(add_qty) AS add_qty
	  ,SUM(add_sessions) AS add_sessions
	  ,SUM(product_views) AS product_views
	  ,SUM(product_view_sessions) AS product_view_sessions
	  ,SUM(instock_views) AS instock_views
	  ,SUM(demand_units) AS demand_units
	  ,SUM(demand_dollars) AS demand_dollars
	  ,SUM(demand_units_new) AS demand_units_new
	  ,SUM(demand_dollars_new) AS demand_dollars_new
	  --reg price funnel
	  ,SUM(order_sessions_reg) AS order_sessions_reg
	  ,SUM(add_qty_reg) AS add_qty_reg
	  ,SUM(add_sessions_reg) AS add_sessions_reg
	  ,SUM(product_views_reg) AS product_views_reg
	  ,SUM(product_view_sessions_reg) AS product_view_sessions_reg
	  ,SUM(instock_views_reg) AS instock_views_reg
	  ,SUM(demand_units_reg) AS demand_units_reg
	  ,SUM(demand_dollars_reg) AS demand_dollars_reg
	  --sales & returns
	  ,SUM(return_dollars) AS return_dollars
	  ,SUM(return_units) AS return_units
	  ,SUM(sales_dollars) AS sales_dollars
	  ,SUM(sales_units) AS sales_units
	  ,SUM(sales_net_dollars ) AS sales_net_dollars 
	  ,SUM(sales_net_units) AS sales_net_units
    ,SUM(demand_dollars) / NULLIF(ccs_mnth, 0) AS cc_monthly_productivity
    ,SUM(rcpt_units) AS rcpt_units
    ,SUM(rcpt_dollars) AS rcpt_dollars
    ,SUM(rcpt_bought_units) AS rcpt_bought_units
    ,SUM(rcpt_bought_dollars) AS rcpt_bought_dollars
    ,SUM(rcpt_ds_units) AS rcpt_ds_units
    ,SUM(rcpt_ds_dollars) AS rcpt_ds_dollars
    ,SUM(rcpt_new_units) AS rcpt_new_units
    ,SUM(rcpt_new_dollars) AS rcpt_new_dollars
    ,SUM(rcpt_cf_units) AS rcpt_cf_units
    ,SUM(rcpt_cf_dollars) AS rcpt_cf_dollars
	FROM monthly_hierarchy AS c
	FULL OUTER JOIN receipts AS r
	  ON c.sku_idnt = r.sku_idnt
	  AND c.mnth_idnt = r.mnth_idnt
	  AND c.channel_country = r.channel_country
	  AND c.channel_brand = r.channel_brand
    AND c.dept_idnt = r.dept_idnt
	  AND c.cat_idnt = r.cat_idnt
    and c.grp_idnt = r.grp_idnt
    AND c.cc_type = r.cc_type
  WHERE COALESCE(c.mnth_idnt, r.mnth_idnt) <= (SELECT DISTINCT month_num FROM prd_nap_usr_vws.day_cal WHERE day_date = CURRENT_DATE)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
) 
WITH DATA
PRIMARY INDEX (mnth_454, yr_454, dept_idnt, cat_idnt, channel_country, channel_brand)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(mnth_454, yr_454, dept_idnt, cat_idnt, channel_country, channel_brand)
     ,COLUMN (channel_country, channel_brand, dept_idnt, cat_idnt)
     ,COLUMN (mnth_idnt, channel_country, channel_brand, dept_idnt, category)
     ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt) 
     ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt, cat_idnt) 
     ,COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt, category) 
     ,COLUMN (cat_idnt) 
     ,COLUMN (dept_idnt) 
     ,COLUMN (yr_454) 
     ,COLUMN (mnth_454)
     ON monthly;

-- fill in missing product views, mainly where sku_idnt is missing 
--DROP TABLE missing_pvs;
CREATE MULTISET VOLATILE TABLE missing_pvs AS (
SELECT
     cal.mnth_454
    ,cal.yr_454
    ,channelcountry AS channel_country
    ,CASE WHEN channel = 'RACK' THEN 'NORDSTROM_RACK'
          ELSE 'NORDSTROM' END AS channel_brand
    ,h.dept_idnt
    ,COALESCE(h.category, 'UNKNOWN') AS category
    ,SUM(product_views) AS product_views
    ,SUM(product_views * pct_instock) AS instock_views
    ,SUM(CASE WHEN current_price_type = 'R' THEN product_views ELSE 0 END) AS product_views_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN product_views * pct_instock ELSE 0 END) AS instock_views_reg
FROM t2dl_das_product_funnel.product_price_funnel_daily AS f
JOIN calendar AS cal
  ON f.event_date_pacific = cal.day_date
JOIN (
        SELECT DISTINCT 
             web_style_num
            ,channel_country
            ,dept_idnt
            ,quantrix_category AS category
        FROM t2dl_das_assortment_dim.assortment_hierarchy
        WHERE div_desc NOT LIKE 'INACTIVE%'
          AND fanatics_ind = 0 -- do not include fanatics skus
          AND marketplace_ind = 0 -- do not include marketplace skus
    ) h
  ON f.web_style_num = h.web_style_num
 AND f.channelcountry = h.channel_country
WHERE event_date_pacific >= '2019-02-03'
 AND channel <> 'UNKNOWN'
 AND (rms_sku_num = 'UNKNOWN' OR rms_sku_num IS NULL) -- where sku_idnt is missing
GROUP BY 1,2,3,4,5,6
) 
WITH DATA
PRIMARY INDEX (mnth_454, yr_454, dept_idnt, category, channel_country, channel_brand)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
      COLUMN (mnth_454, yr_454, channel_country, channel_brand, dept_idnt, category)
      ON missing_pvs;

-- Monthly count view 
DELETE FROM {environment_schema}.selection_planning_monthly{env_suffix} ALL;
INSERT INTO {environment_schema}.selection_planning_monthly{env_suffix}
SELECT
     a.mnth_454
    ,a.mnth_idnt
    ,a.mnth_start_day_date
    ,a.qtr_454
    ,a.yr_454
    ,a.channel_country
    ,a.channel_brand
    ,a.div_idnt
    ,a.grp_idnt
    ,a.dept_idnt
    ,a.cat_idnt
    ,a.category
    --choice counts
    ,a.ccs_daily_avg
    ,a.ccs_daily_avg_los
    ,a.ccs_daily_avg_new
    ,a.ccs_mnth_bop
    ,a.ccs_mnth_eop
    ,a.ccs_mnth
    ,a.ccs_mnth_los
    ,a.ccs_mnth_new
    ,a.ccs_mnth_cf
    ,a.ccs_mnth_cf_rp
    ,a.ccs_mnth_rp
    ,a.ccs_mnth_ds
    ,a.ccs_mnth_rcpt_new
    ,a.ccs_mnth_rcpt_cf
    ,a.ccs_mnth_rcpt_new_ds
    ,a.ccs_mnth_rcpt_cf_ds
    --funnel
    ,a.order_sessions
    ,a.add_qty
    ,a.add_sessions
    ,a.product_views + COALESCE(b.product_views, 0) AS product_views
    ,a.product_view_sessions
    ,a.instock_views + COALESCE(b.instock_views, 0) AS instock_views
    ,a.demand_units
    ,a.demand_dollars
    ,COALESCE(a.demand_units_new, 0) AS demand_units_new
    ,COALESCE(a.demand_dollars_new, 0) AS demand_dollars_new
    ,(a.demand_units / NULLIFZERO(a.product_views + COALESCE(b.product_views, 0))) AS conversion
    --reg price funnel
    ,a.order_sessions_reg
    ,a.add_qty_reg
    ,a.add_sessions_reg
    ,a.product_views_reg + COALESCE(b.product_views_reg, 0) AS product_views_reg
    ,a.product_view_sessions_reg
    ,a.instock_views_reg + COALESCE(b.instock_views_reg, 0) AS instock_views_reg
    ,a.demand_units_reg
    ,a.demand_dollars_reg
    ,(a.demand_units_reg / NULLIFZERO(a.product_views_reg + COALESCE(b.product_views_reg, 0))) AS conversion_reg
    --sales & returns
    ,a.return_dollars AS return_dollars
    ,a.return_units AS return_units
    ,a.sales_dollars AS sales_dollars
    ,a.sales_units AS sales_units
    ,a.sales_net_dollars 
    ,a.sales_net_units
    ,a.cc_monthly_productivity
    ,COALESCE(a.rcpt_units,0) AS rcpt_units
    ,COALESCE(a.rcpt_dollars,0) AS rcpt_dollars
    ,COALESCE(a.rcpt_bought_units,0) AS rcpt_bought_units
    ,COALESCE(a.rcpt_bought_dollars,0) AS rcpt_bought_dollars
    ,COALESCE(a.rcpt_ds_units,0) AS rcpt_ds_units
    ,COALESCE(a.rcpt_ds_dollars,0) AS rcpt_ds_dollars
    ,COALESCE(a.rcpt_new_units,0) AS rcpt_new_units
    ,COALESCE(a.rcpt_new_dollars,0) AS rcpt_new_dollars
    ,COALESCE(a.rcpt_cf_units,0) AS rcpt_cf_units
    ,COALESCE(a.rcpt_cf_dollars,0) AS rcpt_cf_dollars
    ,current_timestamp AS update_timestamp
FROM (
      SELECT
           m.*
          -- NOTE: Using estimated bop/eop prior to 202104/05, 24 months of los.bop and los.eop data available starting 2023-07-04
          ,COALESCE(CASE WHEN m.mnth_idnt <= 202105 
                    -- forcing bop to match prior month's eop 
                    THEN LAG(d.eop_est, 1, ccs_daily_avg_los) OVER (PARTITION BY m.channel_country, m.channel_brand, m.dept_idnt, m.cat_idnt ORDER BY m.yr_454, m.mnth_454)
                    ELSE los.ccs_mnth_bop END, 0) AS ccs_mnth_bop
          ,COALESCE(CASE WHEN m.mnth_idnt <= 202104 THEN d.eop_est ELSE los.ccs_mnth_eop END, 0) AS ccs_mnth_eop
          ,d.ccs_daily_avg
          ,d.ccs_daily_avg_los
          ,d.ccs_daily_avg_new
      FROM monthly m
      LEFT JOIN cc_bop_eop_cnts los
        ON m.mnth_idnt = los.mnth_idnt
       AND m.channel_country = los.channel_country
       AND m.channel_brand = los.channel_brand
       AND m.dept_idnt = los.dept_idnt
       AND m.category = los.category
      LEFT JOIN (
                  SELECT
                       cal.mnth_454
                      ,cal.yr_454
                      ,dept_idnt
                      ,cat_idnt
                      ,channel_country
                      ,channel_brand
                      ,CAST(AVG(customer_choices) AS INTEGER) AS ccs_daily_avg
                      ,CAST(AVG(customer_choices_los) AS INTEGER) AS ccs_daily_avg_los
                      ,CAST(AVG(customer_choices_new) AS INTEGER) AS ccs_daily_avg_new
                      ,MAX(bop_est) AS bop_est
                      ,MAX(eop_est) AS eop_est
                  FROM {environment_schema}.selection_planning_daily{env_suffix} AS d
                  JOIN calendar cal on d.day_date = cal.day_date
                  GROUP BY cal.mnth_454, cal.yr_454, dept_idnt, cat_idnt, channel_country, channel_brand
                ) d
          ON m.mnth_454 = d.mnth_454
         AND m.yr_454 = d.yr_454
         AND m.dept_idnt = d.dept_idnt
         AND (m.cat_idnt = d.cat_idnt
          OR m.cat_idnt IS NULL)
         AND m.channel_country = d.channel_country
         AND m.channel_brand = d.channel_brand
      WHERE m.mnth_idnt < (SELECT DISTINCT month_num FROM prd_nap_usr_vws.day_cal WHERE day_date = CURRENT_DATE) 
      -- remove current month (since it is not a complete month)
      ) a
LEFT JOIN missing_pvs b  -- fill in missing product views, mainly from rack where missing sku_idnt
  ON a.mnth_454 = b.mnth_454
 AND a.yr_454 = b.yr_454
 AND a.channel_country = b.channel_country
 AND a.channel_brand = b.channel_brand
 AND a.dept_idnt = b.dept_idnt
 AND a.category = b.category;

COLLECT STATS
     PRIMARY INDEX(mnth_start_day_date, dept_idnt, cat_idnt)
     ON {environment_schema}.selection_planning_monthly{env_suffix};