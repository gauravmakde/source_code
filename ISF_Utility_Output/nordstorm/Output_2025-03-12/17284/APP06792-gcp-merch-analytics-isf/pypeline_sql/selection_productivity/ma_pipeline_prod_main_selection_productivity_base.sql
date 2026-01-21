/*
Purpose:        Creates base table to use in selection productivity full table.
				Inserts data in {{environment_schema}} tables for Selection Productivity
                    selection_productivity_base
Variable(s):     {{environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
              
Author(s):       Sara Scott
*/
-- begin
--DROP TABLE calendar;
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
WHERE c.day_date BETWEEN {start_date} AND {end_date}

)
WITH DATA
PRIMARY INDEX (day_date) 
ON COMMIT PRESERVE ROWS
;



COLLECT STATS
     PRIMARY INDEX (day_date)
    ON calendar;


    
-- RP
--DROP TABLE replenishment;
CREATE MULTISET VOLATILE TABLE replenishment AS (
	SELECT
      rms_sku_num AS sku_idnt
      , CAST(st.store_num AS varchar(5)) AS loc_idnt
      , day_idnt
      , 1 AS replenishment_flag
  FROM PRD_NAP_USR_VWS.MERCH_RP_SKU_LOC_DIM_HIST rp
  INNER JOIN calendar d
  ON d.day_date BETWEEN BEGIN(rp_period) and END(rp_period)
  INNER JOIN (
    SELECT
        str.*
        , CASE WHEN store_num IN (210, 212) THEN 209 ELSE store_num END AS store_num_stg
    FROM prd_nap_usr_vws.store_dim str
    WHERE business_unit_desc IN ('N.COM', 'FULL LINE',  'RACK', 'RACK CANADA', 'OFFPRICE ONLINE', 'N.CA')
  ) st
     ON rp.location_num = st.store_num_stg
  GROUP BY 1,2,3,4
)
WITH DATA
PRIMARY INDEX (sku_idnt, day_idnt, loc_idnt)
ON COMMIT PRESERVE ROWS;


-- Dropship
--DROP TABLE dropship;
CREATE MULTISET VOLATILE TABLE dropship AS (
    SELECT
        snapshot_date AS day_dt
        , rms_sku_id AS sku_idnt
        , 1 as dropship_flag
    FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_FACT ds
    JOIN calendar dt on ds.snapshot_date = dt.day_date
    WHERE location_type IN ('DS')
    GROUP BY 1,2,3
)
WITH DATA
PRIMARY INDEX (day_dt, sku_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_dt, sku_idnt)
     ON dropship;


-- Fanatics
--DROP TABLE fanatics;
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
--DROP TABLE anniversary;
  CREATE MULTISET VOLATILE table anniversary as (
	select
		sku_idnt
		, channel_country 
		, CASE WHEN selling_channel = 'ONLINE' THEN 'DIGITAL' ELSE selling_channel END as channel
		, 'NORDSTROM' AS channel_brand
		, cal.day_idnt
		, day_dt
		, 1 as anniversary_flag
	from T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU_CHNL_DATE a
	join calendar cal
		ON a.day_idnt = cal.day_idnt
	group by 1, 2, 3,4,5,6
)
with data and stats
primary index (sku_idnt, channel, day_idnt)
on commit preserve rows; 
   
   

--last receipt date
--DROP TABLE last_receipt;
CREATE MULTISET VOLATILE TABLE last_receipt AS (
	SELECT
		 r.rms_sku_num as sku_idnt
		,r.store_num as loc_idnt
		,PO_RECEIPT_LAST_DATE as last_receipt_date
	FROM PRD_NAP_USR_VWS.MERCH_RECEIPT_DATE_SKU_STORE_FACT r
)
WITH DATA AND STATS
PRIMARY INDEX (sku_idnt, loc_idnt)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (sku_idnt, loc_idnt)
    ON last_receipt;




-- Digital Metrics
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
--DROP TABLE pricetype_sku;
CREATE MULTISET VOLATILE TABLE pricetype_sku AS (
SELECT
     cal.*
    ,sld.sku_idnt
    ,price_type
    ,channel_country
    ,channel_brand
    ,CASE WHEN selling_channel = 'ONLINE' then 'DIGITAL' ELSE selling_channel END as channel
    ,MAX(COALESCE(replenishment_flag, 0)) AS rp_flag
    ,MAX(COALESCE(dropship_flag, 0)) AS dropship_flag
    ,SUM(COALESCE(sales_units, 0)) AS net_sales_units
    ,SUM(COALESCE(sales_dollars, 0)) AS net_sales_dollars
    ,SUM(COALESCE(return_units, 0)) AS return_units
    ,SUM(COALESCE(return_dollars, 0)) AS return_dollars
    ,SUM(COALESCE(demand_units, 0)) AS demand_units
    ,SUM(COALESCE(demand_dollars, 0)) AS demand_dollars
    ,SUM(COALESCE(demand_cancel_units, 0)) AS demand_cancel_units
    ,SUM(COALESCE(demand_cancel_dollars, 0)) AS demand_cancel_dollars
    ,SUM(COALESCE(demand_dropship_units, 0)) AS demand_dropship_units
    ,SUM(COALESCE(demand_dropship_dollars, 0)) AS demand_dropship_dollars
    ,SUM(COALESCE(shipped_units, 0)) AS shipped_units
    ,SUM(COALESCE(shipped_dollars, 0)) AS shipped_dollars
    ,SUM(COALESCE(store_fulfill_units, 0)) AS store_fulfill_units
    ,SUM(COALESCE(store_fulfill_dollars, 0)) AS store_fulfill_dollars
    ,SUM(COALESCE(eoh_units, 0)) AS eoh_units
    ,SUM(COALESCE(eoh_dollars, 0)) AS eoh_dollars
    ,SUM(COALESCE(boh_units, 0)) AS boh_units
    ,SUM(COALESCE(boh_dollars, 0)) AS boh_dollars
    ,SUM(COALESCE(nonsellable_units, 0)) AS nonsellable_units
    ,SUM(CAST(0 AS DECIMAL(10,0))) AS receipt_units
    ,SUM(CAST(0 AS DECIMAL(12,2))) AS receipt_dollars
    ,MAX(last_receipt_date) as last_receipt_date
FROM t2dl_das_ace_mfp.sku_loc_pricetype_day sld
JOIN prd_nap_usr_vws.price_store_dim_vw st
  ON sld.loc_idnt = st.store_num
JOIN calendar cal
  ON sld.day_dt = cal.day_date
LEFT JOIN replenishment r 
  ON sld.sku_idnt = r.sku_idnt
 AND sld.loc_idnt = r.loc_idnt
 AND cal.day_idnt = r.day_idnt
LEFT JOIN dropship ds
  ON ds.day_dt = sld.day_dt
 AND sld.sku_idnt = ds.sku_idnt
LEFT JOIN last_receipt lr
  ON lr.sku_idnt = sld.sku_idnt
 AND lr.loc_idnt = sld.loc_idnt
WHERE st.business_unit_num NOT IN ('8000', '5500')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)
WITH DATA 
PRIMARY INDEX (day_date, sku_idnt)
ON COMMIT preserve rows;
COLLECT STATS 
     PRIMARY INDEX (day_date, sku_idnt)
    ON pricetype_sku;

--DROP TABLE los;
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


DELETE FROM {environment_schema}.selection_productivity_base{env_suffix}  WHERE day_date BETWEEN {start_date} and {end_date}; 
INSERT INTO {environment_schema}.selection_productivity_base{env_suffix} 
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
    ,s.sku_idnt as sku_idnt
    ,cc.customer_choice as customer_choice
    ,s.channel_country
    ,s.channel_brand
    ,s.channel
    ,MIN(l.first_day_published) as days_published
	,COALESCE(days_live,0) as days_live
	,CASE WHEN f.sku_idnt IS NULL then 0 else 1 end as fanatics_flag
    ,CASE WHEN cc_type = 'NEW' then 1 ELSE 0 END AS new_flag
	,CASE WHEN cc_type = 'CF' then 1 ELSE 0 END AS cf_flag
    ,MAX(rp_flag) AS rp_flag
    ,MAX(dropship_flag) AS dropship_flag
	  ,MAX(COALESCE(anniversary_flag, 0)) AS anniversary_flag
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
FROM pricetype_sku s
LEFT JOIN prd_nap_usr_vws.product_sku_dim sku
		ON s.sku_idnt = sku.rms_sku_num
		AND sku.channel_country = 'US'   --5/14/2024, joining table for removing mp
JOIN T2dl_das_assortment_dim.sku_cc_lkp cc
  ON s.sku_idnt = TRIM(cc.sku_idnt)
 AND s.channel_country = TRIM(cc.channel_country)
 LEFT JOIN T2DL_DAS_ASSORTMENT_DIM.cc_type_realigned ncf
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
  LEFT JOIN anniversary an ON s.sku_idnt = an.sku_idnt
  and s.channel_country = an.channel_country
  and s.channel_brand = an.channel_brand
  and s.channel = an.channel
  and s.day_date = an.day_dt
WHERE COALESCE(sku.partner_relationship_type_code,'UNKNOWN') <> 'ECONCESSION' --5/14/2024, removing mp
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,26,27,28,29;


-- end






