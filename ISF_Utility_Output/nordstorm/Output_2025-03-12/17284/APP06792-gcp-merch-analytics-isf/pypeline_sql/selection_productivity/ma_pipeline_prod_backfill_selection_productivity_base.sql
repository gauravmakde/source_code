-- Code for Backfilling table
CREATE MULTISET VOLATILE TABLE calendar AS (

SELECT
     c.day_date
    ,c.day_idnt
    ,c.week_idnt AS wk_idnt
    ,c.month_idnt
    ,c.month_abrv
    ,c.quarter_idnt 
    ,c.fiscal_halfyear_num AS half_idnt
    ,c.fiscal_year_num AS year_idnt
    -- First/Last days flag
    ,CASE WHEN c.day_idnt = c.week_start_day_idnt THEN 1 ELSE 0 END AS first_day_of_week
    ,CASE WHEN c.day_idnt = c.week_end_day_idnt THEN 1 ELSE 0 END AS last_day_of_week
    ,CASE WHEN c.day_idnt = c.month_start_day_idnt THEN 1 ELSE 0 END AS first_day_of_month
    ,CASE WHEN c.day_idnt = c.month_end_day_idnt THEN 1 ELSE 0 END AS last_day_of_month
    ,CASE WHEN c.day_idnt = c.quarter_start_day_idnt THEN 1 ELSE 0 END AS first_day_of_qtr
    ,CASE WHEN c.day_idnt = c.quarter_end_day_idnt THEN 1 ELSE 0 END AS last_day_of_qtr
    -- Current timeframe flag
    ,CASE WHEN c.day_idnt BETWEEN curr.week_start_day_idnt AND curr.week_end_day_idnt THEN 1 ELSE 0 END AS current_week
    ,CASE WHEN c.day_idnt BETWEEN curr.month_start_day_idnt AND curr.month_end_day_idnt THEN 1 ELSE 0 END AS current_month
    ,CASE WHEN c.day_idnt BETWEEN curr.quarter_start_day_idnt AND curr.quarter_end_day_idnt THEN 1 ELSE 0 END AS current_quarter
    --- Time Ranking
    ,CASE WHEN current_week = 0 THEN DENSE_RANK() OVER (PARTITION BY current_week ORDER BY week_idnt DESC) ELSE 0 END AS week_rank
    ,CASE WHEN current_month = 0 THEN DENSE_RANK() OVER (PARTITION BY current_month ORDER BY month_idnt DESC) ELSE 0 END AS month_rank
    ,CASE WHEN current_quarter = 0 THEN DENSE_RANK() OVER (PARTITION BY current_quarter ORDER BY quarter_idnt DESC) ELSE 0 END AS quarter_rank
FROM prd_nap_usr_vws.day_cal_454_dim c
CROSS JOIN (
        SELECT DISTINCT
             week_start_day_idnt
            ,week_end_day_idnt
            ,quarter_start_day_idnt
            ,quarter_end_day_idnt
            ,month_start_day_idnt
            ,month_end_day_idnt
        FROM prd_nap_usr_vws.day_cal_454_dim
        WHERE day_date = current_date
    ) curr
WHERE c.month_idnt >= 202101 and c.day_date < current_date

)
WITH DATA
PRIMARY INDEX (day_date) 
ON COMMIT PRESERVE ROWS
;



COLLECT STATS
     PRIMARY INDEX (day_date)
    ON calendar;

-- Customer Choice : SKU lookup table
CREATE MULTISET VOLATILE TABLE sku_cc_lkup AS ( 
SELECT  
     rms_sku_num AS sku_idnt
    ,channel_country
    ,dept_num || '_' || supp_part_num || '_' || COALESCE(color_num, 'UNKNOWN') AS customer_choice
    ,CASE WHEN npg_ind  = 'Y' THEN 1 ELSE 0 END as npg_flag
FROM prd_nap_usr_vws.product_sku_dim_vw sku
)
WITH DATA 
PRIMARY INDEX (sku_idnt)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     PRIMARY INDEX (sku_idnt)
    ,COLUMN (customer_choice) 
    ,COLUMN (sku_idnt, channel_country)
    ON sku_cc_lkup;
    
-- RP
CREATE MULTISET VOLATILE TABLE replenishment AS (
SELECT
     r.sku_idnt
    ,r.loc_idnt
    ,r.wk_idnt
    ,1 AS replenishment_flag
FROM prd_ma_bado.rpl_sku_lw_lkup_vw r
JOIN (SELECT DISTINCT wk_idnt FROM calendar) c
  ON r.wk_idnt = c.wk_idnt
)
WITH DATA
PRIMARY INDEX (sku_idnt, wk_idnt, loc_idnt)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
     PRIMARY INDEX (sku_idnt, wk_idnt, loc_idnt)
    ON replenishment;

-- Dropship
CREATE volatile multiset TABLE dropship AS (
SELECT 
     r.sku_idnt
	,cal.day_date
	,COALESCE(ds.drpshp_elig_ind, MAX(CASE WHEN r.rcpt_dsh_tot_units > 0 THEN 1 END), 0) AS dropship_flag
FROM prd_ma_bado.rcpt_sku_ld_cvw r
LEFT JOIN prd_ma_bado.dshp_sku_lkup_vw ds 
  ON r.sku_idnt = ds.sku_idnt
 AND ds.day_idnt = r.day_idnt 
JOIN calendar cal
  ON r.day_idnt = cal.day_idnt
GROUP BY r.sku_idnt, cal.day_date, ds.drpshp_elig_ind
)
WITH DATA AND STATS
PRIMARY INDEX (sku_idnt, day_date)
ON COMMIT preserve rows;

COLLECT STATS 
     PRIMARY INDEX (sku_idnt, day_date)
    ON dropship;
-- Fanatics
CREATE MULTISET VOLATILE TABLE fanatics AS (
SELECT DISTINCT
    rms_sku_num AS sku_idnt
from PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW psd
JOIN PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM payto
  ON psd.prmy_supp_num = payto.order_from_vendor_num
WHERE payto.payto_vendor_num = '5179609'
) WITH DATA
PRIMARY INDEX(sku_idnt)
ON COMMIT PRESERVE ROWS 
;
COLLECT STATS 
     PRIMARY INDEX (sku_idnt)
    ON fanatics;

-- Anniversary
  create volatile multiset table anniversary as (
	select
		sku_idnt
		, loc_idnt
		, cal.day_idnt
		, 1 as anniversary_flag
	from T2DL_DAS_SCALED_EVENTS.anniversary_sku_store_date a
	join calendar cal
		ON a.day_idnt = cal.day_idnt
	group by 1, 2, 3
)
with data and stats
primary index (sku_idnt, loc_idnt, day_idnt)
on commit preserve rows; 
   
   
-- Last Receipt Date
CREATE MULTISET VOLATILE TABLE last_receipt AS (
SELECT 
	 r.sku_idnt
	,r.loc_idnt
	,d.day_date as last_receipt_date
FROM prd_ma_bado.rcpt_day_sku_loc_lkup r
JOIN prd_nap_usr_vws.day_cal_454_dim d
  ON r.rcpt_po_day_idnt_max = d.day_idnt
)
WITH DATA AND STATS
PRIMARY INDEX (sku_idnt, loc_idnt)
ON COMMIT preserve rows;

COLLECT STATS 
     PRIMARY INDEX (sku_idnt, loc_idnt)
    ON last_receipt;





-- Digital Metrics
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
	,SUM(order_demand) as order_demand
	,SUM(order_quantity) as order_quantity
    ,SUM(CASE WHEN current_price_type = 'R' THEN order_sessions ELSE 0 END) AS order_sessions_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN add_to_bag_quantity ELSE 0 END) AS add_qty_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN add_to_bag_sessions ELSE 0 END) AS add_sessions_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN product_views ELSE 0 END) AS product_views_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN product_view_sessions ELSE 0 END) AS product_view_sessions_reg
    ,SUM(CASE WHEN current_price_type = 'R' THEN product_views * pct_instock ELSE 0 END) AS instock_views_reg
    
FROM t2dl_das_product_funnel.product_price_funnel_daily f
JOIN calendar cal
  ON f.event_date_pacific = cal.day_date
  WHERE channel <> 'UNKNOWN'
GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (day_date, sku_idnt, channel_brand, channel_country)
	ON digital_funnel;



-- Combine everything at ay Level with KPIs

CREATE MULTISET VOLATILE TABLE pricetype_sku AS (
SELECT
     cal.*
    ,sld.sku_idnt
    ,price_type
    ,CASE WHEN st.business_unit_desc IN ('RACK CANADA', 'FULL LINE CANADA', 'N.CA') THEN 'CA'
          ELSE 'US' 
          END AS channel_country
    ,CASE WHEN st.business_unit_desc IN ('RACK', 'OFFPRICE ONLINE', 'RACK CANADA') THEN 'NORDSTROM_RACK'
          ELSE 'NORDSTROM' 
          END AS channel_brand
    ,CASE WHEN st.business_unit_desc IN ('FULL LINE', 'RACK', 'RACK CANADA', 'FULL LINE CANADA') THEN 'STORE'
          ELSE 'DIGITAL' 
          END AS channel
    ,COALESCE(replenishment_flag, 0) AS rp_flag
    ,COALESCE(dropship_flag, 0) AS dropship_flag
    ,COALESCE(anniversary_flag, 0) AS anniversary_flag
    ,COALESCE(sales_units, 0) AS net_sales_units
    ,COALESCE(sales_dollars, 0) AS net_sales_dollars
    ,COALESCE(return_units, 0) AS return_units
    ,COALESCE(return_dollars, 0) AS return_dollars
    ,COALESCE(demand_units, 0) AS demand_units
    ,COALESCE(demand_dollars, 0) AS demand_dollars
    ,COALESCE(demand_cancel_units, 0) AS demand_cancel_units
    ,COALESCE(demand_cancel_dollars, 0) AS demand_cancel_dollars
    ,COALESCE(demand_dropship_units, 0) AS demand_dropship_units
    ,COALESCE(demand_dropship_dollars, 0) AS demand_dropship_dollars
    ,COALESCE(shipped_units, 0) AS shipped_units
    ,COALESCE(shipped_dollars, 0) AS shipped_dollars
    ,COALESCE(store_fulfill_units, 0) AS store_fulfill_units
    ,COALESCE(store_fulfill_dollars, 0) AS store_fulfill_dollars
    ,COALESCE(eoh_units, 0) AS eoh_units
    ,COALESCE(eoh_dollars, 0) AS eoh_dollars
    ,COALESCE(boh_units, 0) AS boh_units
    ,COALESCE(boh_dollars, 0) AS boh_dollars
    ,COALESCE(nonsellable_units, 0) AS nonsellable_units
    ,CAST(0 AS DECIMAL(10,0)) AS receipt_units
    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
    ,last_receipt_date
FROM t2dl_das_ace_mfp.sku_loc_pricetype_day sld
JOIN prd_nap_usr_vws.store_dim st
  ON sld.loc_idnt = st.store_num
JOIN calendar cal
  ON sld.day_dt = cal.day_date
LEFT JOIN replenishment r 
  ON sld.sku_idnt = r.sku_idnt
 AND sld.loc_idnt = r.loc_idnt
 AND cal.wk_idnt = r.wk_idnt
LEFT JOIN dropship ds
  ON ds.day_date = sld.day_dt
 AND sld.sku_idnt = ds.sku_idnt
LEFT JOIN anniversary an
	ON an.day_idnt = sld.DAY_DT 
	AND sld.sku_idnt = an.sku_idnt
	AND an.loc_idnt = sld.LOC_IDNT 
LEFT JOIN last_receipt lr
  ON lr.sku_idnt = sld.sku_idnt
 AND lr.loc_idnt = sld.loc_idnt
WHERE st.business_unit_num NOT IN ('8000', '5500')
)
WITH DATA 
PRIMARY INDEX (day_date, sku_idnt)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (day_date, sku_idnt)
    ON pricetype_sku;


--Getting receipts (removing business unit 8000 to remove distribution channels)


CREATE MULTISET VOLATILE TABLE receipt_sku AS (   
 SELECT
     cal.*
    ,sld.sku_idnt
    ,CASE WHEN sld.clrc_ind = 'Y' THEN 'C' ELSE 'R' END AS price_type
    ,CASE WHEN st.business_unit_desc IN ('RACK CANADA', 'FULL LINE CANADA', 'N.CA') THEN 'CA'
          ELSE 'US' 
          END AS channel_country
    ,CASE WHEN st.business_unit_desc IN ('RACK', 'OFFPRICE ONLINE', 'RACK CANADA') THEN 'NORDSTROM_RACK'
          ELSE 'NORDSTROM' 
          END AS channel_brand
    ,CASE WHEN st.business_unit_desc IN ('FULL LINE', 'RACK', 'RACK CANADA', 'FULL LINE CANADA') THEN 'STORE'
          ELSE 'DIGITAL' 
          END AS channel
    ,COALESCE(replenishment_flag, 0) AS rp_flag
    ,COALESCE(dropship_flag, 0) AS dropship_flag
    ,COALESCE(anniversary_flag, 0) AS anniversary_flag
    ,CAST(0 AS DECIMAL(10,0)) AS net_sales_units
    ,CAST(0 AS DECIMAL(12,2)) AS net_sales_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS return_units
    ,CAST(0 AS DECIMAL(12,2)) AS return_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS demand_units
    ,CAST(0 AS DECIMAL(12,2)) AS demand_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS demand_cancel_units
    ,CAST(0 AS DECIMAL(12,2)) AS demand_cancel_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS demand_dropship_units
    ,CAST(0 AS DECIMAL(12,2)) AS demand_dropship_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS shipped_units
    ,CAST(0 AS DECIMAL(12,2)) AS shipped_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS store_fulfill_units
    ,CAST(0 AS DECIMAL(12,2)) AS store_fulfill_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS eoh_units
    ,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS boh_units
    ,CAST(0 AS DECIMAL(12,2)) AS boh_dollars
    ,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
    ,CASE WHEN rcpt_typ_key IN (1, 2, 11, 7, 8) THEN rcpt_tot_units ELSE 0 END AS receipt_units
    ,CASE WHEN rcpt_typ_key IN (1, 2, 11, 7, 8) THEN rcpt_tot_retl$ ELSE 0 END AS receipt_dollars
    ,last_receipt_date
FROM prd_ma_bado.rcpt_sku_ld_rvw sld
JOIN prd_nap_usr_vws.store_dim st
  ON sld.loc_idnt = st.store_num
JOIN calendar cal
  ON sld.DAY_IDNT = cal.day_idnt
LEFT JOIN replenishment r 
  ON sld.sku_idnt = r.sku_idnt
 AND sld.loc_idnt = r.loc_idnt
 AND cal.wk_idnt = r.wk_idnt
LEFT JOIN dropship ds
  ON ds.day_date = sld.day_idnt
 AND sld.sku_idnt = ds.sku_idnt
LEFT JOIN anniversary an
	ON an.day_idnt = sld.DAY_IDNT 
	AND sld.sku_idnt = an.sku_idnt
	AND an.loc_idnt = sld.LOC_IDNT 
LEFT JOIN last_receipt lr
  ON lr.sku_idnt = sld.sku_idnt
 AND lr.loc_idnt = sld.loc_idnt
 where st.business_unit_num NOT IN ('8000', '5500')

)
WITH DATA 
PRIMARY INDEX (day_date, sku_idnt)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (day_date, sku_idnt)
    ON receipt_sku;

CREATE MULTISET VOLATILE TABLE los as (
   SELECT channel_country
	, channel_brand
	, sku_id
	, day_date
	, 1 as days_live
	, MIN(day_date) over (partition by channel_country, channel_brand, sku_id) first_day_published
	FROM t2dl_das_site_merch.live_on_site_daily
	GROUP BY 1,2,3,4,5
)
WITH DATA PRIMARY INDEX (day_date, sku_id, channel_country, channel_brand)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (day_date, sku_id, channel_country, channel_brand)
    ON los;

DELETE FROM t2dl_das_selection.selection_productivity_base; 
INSERT INTO t2dl_das_selection.selection_productivity_base
SELECT 
     s.day_date
    ,wk_idnt
    ,s.month_idnt
    ,s.month_abrv
    ,quarter_idnt
    ,half_idnt
    ,year_idnt
    ,first_day_of_week
    ,last_day_of_week
    ,first_day_of_month
    ,last_day_of_month
    ,first_day_of_qtr
    ,last_day_of_qtr
    ,current_week
    ,current_month
    ,current_quarter
    ,week_rank
    ,month_rank
    ,quarter_rank
    ,s.sku_idnt
    ,cc.customer_choice
    ,s.channel_country
    ,s.channel_brand
    ,s.channel
    ,MIN(l.first_day_published) as days_published
	  ,COALESCE(days_live,0) as days_live
	  ,CASE WHEN f.sku_idnt IS NULL then 0 else 1 end as fanatics_flag
    ,CASE WHEN cc_type = 'NEW' then 1 ELSE 0 END AS new_flag
	  ,CASE WHEN cc_type = 'CF' then 1 ELSE 0 END AS cf_flag
	  ,MAX(npg_flag) AS npg_flag
    ,MAX(rp_flag) AS rp_flag
    ,MAX(dropship_flag) AS dropship_flag
	  ,MAX(anniversary_flag) AS anniversary_flag


    -- Sales
    ,SUM(s.net_sales_units) AS net_sales_units
    ,SUM(s.net_sales_dollars) AS net_sales_dollars
    ,SUM(s.return_units) AS return_units
    ,SUM(s.return_dollars) AS return_dollars
    ,SUM(s.net_sales_units) + SUM(s.return_units) AS sales_units
    ,SUM(s.net_sales_dollars) + SUM(s.return_dollars) AS sales_dollars
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
    --Sales by price type
    ,SUM(CASE WHEN price_type = 'R' THEN s.net_sales_units ELSE 0 END) AS net_sales_reg_units
    ,SUM(CASE WHEN price_type = 'R' THEN s.net_sales_dollars ELSE 0 END) AS net_sales_reg_dollars
    ,SUM(CASE WHEN price_type = 'C' THEN s.net_sales_units ELSE 0 END) AS net_sales_clr_units
    ,SUM(CASE WHEN price_type = 'C' THEN s.net_sales_dollars ELSE 0 END) AS net_sales_clr_dollars
    ,SUM(CASE WHEN price_type = 'P' THEN s.net_sales_units ELSE 0 END) AS net_sales_pro_units
    ,SUM(CASE WHEN price_type = 'P' THEN s.net_sales_dollars ELSE 0 END) AS net_sales_pro_dollars
    -- inventory 
    ,SUM(boh_units) AS boh_units
    ,SUM(boh_dollars) AS boh_dollars
    ,SUM(eoh_units) AS eoh_units
    ,SUM(eoh_dollars) AS eoh_dollars
    ,SUM(nonsellable_units) AS nonsellable_units
    -- inventory by price type 
    ,SUM(CASE WHEN price_type = 'R' THEN eoh_units ELSE 0 END) AS eoh_reg_units
    ,SUM(CASE WHEN price_type = 'R' THEN eoh_dollars ELSE 0 END) AS eoh_reg_dollars
    ,SUM(CASE WHEN price_type = 'C' THEN eoh_units ELSE 0 END) AS eoh_clr_units
    ,SUM(CASE WHEN price_type = 'C' THEN eoh_dollars ELSE 0 END) AS eoh_clr_dollars
    ,SUM(CASE WHEN price_type = 'P' THEN eoh_units ELSE 0 END) AS eoh_pro_units
    ,SUM(CASE WHEN price_type = 'P' THEN eoh_dollars ELSE 0 END) AS eoh_pro_dollars    
    -- receipts
    ,SUM(receipt_units) AS receipt_units
    ,SUM(receipt_dollars) AS receipt_dollars
    ,MAX(last_receipt_date) AS last_receipt_date
    -- digital metrics
    ,SUM(order_sessions) AS order_sessions
    ,SUM(add_qty) AS add_qty
    ,SUM(add_sessions) AS add_sessions
    ,SUM(product_views) AS product_views
    ,SUM(product_view_sessions) AS product_view_sessions
    ,SUM(instock_views) AS instock_views

    ,SUM(order_sessions_reg) AS order_sessions_reg
    ,SUM(add_qty_reg) AS add_qty_reg
    ,SUM(add_sessions_reg) AS add_sessions_reg
    ,SUM(product_views_reg) AS product_views_reg
    ,SUM(product_view_sessions_reg) AS product_view_sessions_reg
    ,SUM(instock_views_reg) AS instock_views_reg
    ,SUM(order_demand) as order_demand
    ,SUM(order_quantity) as order_quantity
FROM (
SELECT * FROM pricetype_sku s
UNION ALL
SELECT * FROM receipt_sku receipts
) s
JOIN sku_cc_lkup cc
  ON s.sku_idnt = cc.sku_idnt
 AND s.channel_country = cc.channel_country
 LEFT JOIN T2DL_DAS_ASSORTMENT_DIM.cc_type ncf
 	ON s.month_idnt = ncf.mnth_idnt
 	AND cc.customer_choice = ncf.customer_choice
 	and cc.channel_country = ncf.channel_country
  and s.channel = ncf.channel
  AND s.channel_brand = ncf.channel_brand
  LEFT JOIN digital_funnel df 
  	ON s.sku_idnt = df.sku_idnt
  	AND s.day_date = df.day_date
  	AND s.channel_country = df.channel_country
  	and s.channel_brand = df.channel_brand
  	and s.channel = 'DIGITAL'
  LEFT JOIN fanatics f ON s.sku_idnt = f.sku_idnt
  LEFT JOIN los l ON s.sku_idnt = l.sku_id
  and s.channel_country = l.channel_country
  and s.channel_brand = l.channel_brand
  and s.day_date = l.day_date
  and s.channel = 'DIGITAL'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,26,27,28,29;