
/*
Purpose:        Creates base table at sku / day level to use for Item performance rollups tables.
				Inserts data in {{environment_schema}} tables for Item Performance Reporting
                    item_performance_daily_base
Variable(s):     {{environment_schema}} T2DL_DAS_SELECTION (prod) or T3DL_ACE_ASSORTMENT
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott
Date Created/Modified: 03/05/2023 -- add asoh units as a field
*/


--Dates table: gets dates
-- begin
CREATE MULTISET VOLATILE TABLE dates AS (

SELECT tme.day_date
				, tme.day_idnt
				, tme.week_idnt
				, tme.week_desc
				, tme.week_end_day_date
				, tme.month_end_week_idnt
				, tme.month_idnt
				, tme.quarter_idnt
				, tme.fiscal_year_num
    FROM prd_nap_usr_vws.day_cal_454_dim tme
   	WHERE tme.day_date BETWEEN {start_date} and {end_date}

  )
WITH DATA PRIMARY INDEX(week_idnt, month_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX (week_idnt, month_idnt) ON dates;

CREATE MULTISET VOLATILE TABLE sku_cc_lkp AS (

SELECT
     rms_sku_num AS sku_idnt
    ,channel_country
    ,dept_num || '_' || prmy_supp_num || '_' || supp_part_num || '_' || COALESCE(color_num, 'UNKNOWN') AS customer_choice -- dept || supplier || vpn || color
FROM prd_nap_usr_vws.product_sku_dim_vw
)
WITH DATA
PRIMARY INDEX (sku_idnt, channel_country)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (sku_idnt, channel_country)
    ,COLUMN (customer_choice)
    ,COLUMN (sku_idnt, channel_country)
    ,COLUMN (channel_country, customer_choice)
     ON sku_cc_lkp;



--Location Table: Filters to only the locations we need
CREATE MULTISET VOLATILE TABLE loc AS(

SELECT
	store_num
	, store_name
	, CASE
    	WHEN channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310') THEN 'US'
    	WHEN channel_num IN ('111', '121', '211', '221', '261', '311') THEN 'CA'
    	ELSE NULL
    	END AS country
	, CASE
	    WHEN channel_num IN ('110', '120', '130', '140', '111', '121', '310', '311') THEN 'FP'
	    WHEN channel_num IN ('211', '221', '261', '210', '220', '250', '260') THEN 'OP'
	    ELSE NULL
	    END AS banner
	,CAST (channel_num as VARCHAR(50)) || ', ' || CAST(channel_desc as VARCHAR(100)) AS channel
FROM  PRD_NAP_USR_VWS.STORE_DIM
WHERE channel_num in('110','111', '120', '121', '130', '140', '210', '211', '220', '221','250', '260', '261',  '310', '311')
GROUP BY 1,2,3,4,5
)
WITH DATA
PRIMARY INDEX (store_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (store_num)
     ON loc;

--Live on Site Table: gets sku's with associated live on site start date as well as if an
-- item was live on a specific day

 CREATE MULTISET VOLATILE TABLE live_on_site AS (

   SELECT

     CASE WHEN channel_brand = 'NORDSTROM' AND channel_country = 'US' THEN '120, N.COM'
    	WHEN channel_brand = 'NORDSTROM' AND channel_country = 'CA' THEN '121, N.CA'
    	WHEN channel_brand = 'NORDSTROM_RACK' THEN '250, OFFPRICE ONLINE'
    	END AS channel
   	 , channel_brand
	 , channel_country AS country
	 , CASE WHEN channel_brand = 'NORDSTROM' AND channel_country = 'US' THEN 808
    	WHEN channel_brand = 'NORDSTROM' AND channel_country = 'CA' THEN 857
    	WHEN channel_brand = 'NORDSTROM_RACK' THEN 591
    	END AS loc_idnt
	 , CASE WHEN channel_brand = 'NORDSTROM' THEN 'FP'
  		WHEN channel_brand = 'NORDSTROM_RACK' THEN 'OP'
   		END AS banner
	 , sku_id
	 , day_date
	 , 1 as days_live
	 , day_date as first_day_published
   FROM t2dl_das_site_merch.live_on_site_daily

   GROUP BY 1,2,3,4,5,6,7,8

)
WITH DATA
PRIMARY INDEX (day_date, sku_id, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
	PRIMARY INDEX (day_date, sku_id, channel, country)
	ON live_on_site;


-- Replenishment Table: gets SKUS that are on RP
CREATE MULTISET VOLATILE TABLE replenishment AS (
	SELECT
      rms_sku_num AS sku_idnt
      , CAST(st.store_num AS varchar(5)) AS loc_idnt
      , day_idnt
      , 1 AS replenishment_flag
  FROM PRD_NAP_USR_VWS.MERCH_RP_SKU_LOC_DIM_HIST rp
  INNER JOIN dates d
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


-- Dropship Table: Gets sku's dropship status by day
CREATE MULTISET VOLATILE TABLE dropship AS (
    SELECT
        snapshot_date AS day_dt
        , rms_sku_id AS sku_idnt
        , 1 as dropship_flag
    FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_FACT ds
    JOIN dates dt on ds.snapshot_date = dt.day_date
    WHERE location_type IN ('DS')
    GROUP BY 1,2,3
)
WITH DATA
PRIMARY INDEX (day_dt, sku_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (day_dt, sku_idnt)
     ON dropship;

--fanatics indicator
CREATE MULTISET VOLATILE TABLE fanatics AS (
	SELECT DISTINCT
	    rms_sku_num AS sku_idnt
	from PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW psd
	JOIN PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM payto
	  ON psd.prmy_supp_num = payto.order_from_vendor_num
	WHERE payto.payto_vendor_num = '5179609'
)
WITH DATA
PRIMARY INDEX(sku_idnt)
ON COMMIT PRESERVE ROWS
;
COLLECT STATS
     PRIMARY INDEX (sku_idnt)
    ON fanatics;



 --gets last receipt date

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


  --gets on order information by sku
CREATE MULTISET VOLATILE TABLE oo_sku AS (
	 SELECT
	 	   cal.day_date
	    ,cal.week_idnt
	    ,cal.week_desc
	    ,cal.week_end_day_date
	    ,cal.month_end_week_idnt
	    ,cal.month_idnt
	    ,cal.quarter_idnt
	    ,cal.fiscal_year_num
	    ,oo.sku_idnt
	    ,price_type
	    ,oo.country
	    ,oo.banner
	    ,oo.channel
	    ,oo.rp_flag
	    ,0 AS dropship_flag
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
	    ,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
	    ,CAST(0 AS DECIMAL(10,0)) AS boh_units
	    ,CAST(0 AS DECIMAL(12,2)) AS boh_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS boh_cost
	    ,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
	    ,CAST(0 AS DECIMAL(12,2)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm -- NEW
	    ,CAST(0 AS DECIMAL(12,2)) AS product_margin
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS reserve_stock_units
	    ,CAST(0 AS DECIMAL(12,2)) AS reserve_stock_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		  ,CAST(0 AS DECIMAL(10,0)) AS backorder_units
	    ,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
	    ,SUM(oo_units) AS oo_units
	    ,SUM(oo_dollars) AS oo_dollars
	    ,MAX(last_receipt_date) as last_receipt_date
	FROM (
     SELECT
        (SELECT max(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date <= current_date) as week_idnt
     		, (SELECT max(week_end_day_date) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date <= current_date) as week_end_day_date
     		, rms_sku_num as sku_idnt
     		, country
  			, banner
  			, channel
  			, anticipated_price_type as price_type
  			, CASE WHEN order_type = 'AUTOMATIC_REORDER' THEN 1 ELSE 0 END AS rp_flag
     		, SUM(quantity_open) oo_units
     		, SUM(total_anticipated_retail_amt) oo_dollars
	      , MAX(last_receipt_date) as last_receipt_date
     FROM PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW oo
     JOIN loc st
     	 ON oo.store_num = st.store_num
  	 LEFT JOIN last_receipt lr
    	 ON oo.rms_sku_num = lr.sku_idnt
    	 AND oo.store_num = lr.loc_idnt
     WHERE quantity_open > 0
       AND first_approval_date IS NOT NULL
       AND ((STATUS = 'CLOSED' AND END_SHIP_DATE >= CURRENT_DATE - 45) OR STATUS IN ('APPROVED','WORKSHEET') )
     GROUP BY 1,2,3,4,5,6,7,8
  ) oo
	JOIN dates cal
	  ON oo.week_end_day_date = cal.day_date
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
WITH DATA
PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
    ON oo_sku;




--Grab metrics from the price type table in NAP and roll up to week - for flags that are at the day level
-- take the max value for the week (i.e. if a sku was on dropship at any day during the week it is flagged
-- as dropship for the entire week)
CREATE MULTISET VOLATILE TABLE pricetype_sku AS (
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
	    ,SUm(demand_cancel_units) AS demand_cancel_units
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
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS reserve_stock_units
	    ,CAST(0 AS DECIMAL(12,2)) AS reserve_stock_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS backorder_units
	    ,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS oo_units
	    ,CAST(0 AS DECIMAL(12,2)) AS oo_dollars
	    ,MAX(last_receipt_date) as last_receipt_date
	FROM t2dl_das_ace_mfp.sku_loc_pricetype_day_vw sld
	JOIN loc st
	  ON sld.loc_idnt = st.store_num
	JOIN dates cal
	  ON sld.day_dt = cal.day_date
	LEFT JOIN replenishment r
	  ON sld.sku_idnt = r.sku_idnt
	 AND sld.loc_idnt = r.loc_idnt
	 AND cal.day_idnt = r.day_idnt
	LEFT JOIN dropship ds
	  ON ds.day_dt = cal.day_date
	 AND sld.sku_idnt = ds.sku_idnt
	LEFT JOIN last_receipt lr
	  ON lr.sku_idnt = sld.sku_idnt
	 AND lr.loc_idnt = sld.loc_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,40,41,42,43,44,45,46,47
)
WITH DATA
PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
    ON pricetype_sku;



-- Getting Backorder information
CREATE MULTISET VOLATILE TABLE backorder_sku AS (
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
	    ,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
	    ,CAST(0 AS DECIMAL(10,0)) AS boh_units
	    ,CAST(0 AS DECIMAL(12,2)) AS boh_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS boh_cost
	    ,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
	    ,CAST(0 AS DECIMAL(12,2)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm -- NEW
	    ,CAST(0 AS DECIMAL(12,2)) AS product_margin
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS reserve_stock_units
	    ,CAST(0 AS DECIMAL(12,2)) AS reserve_stock_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
	    ,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
	    ,SUM(backorder_units) AS backorder_units
	    ,SUM(backorder_dollars) AS backorder_dollars
	    ,CAST(0 AS DECIMAL(10,0)) AS oo_units
	    ,CAST(0 AS DECIMAL(12,2)) AS oo_dollars
	    ,MAX(last_receipt_date) as last_receipt_date
	FROM T2DL_DAS_ACE_MFP.SKU_BO_HISTORY bo
	JOIN loc st
	  ON bo.store_num = st.store_num
	JOIN dates cal
	  ON bo.order_date_pacific = cal.day_date
	LEFT JOIN replenishment r
	  ON bo.rms_sku_num = r.sku_idnt
	 AND bo.store_num = r.loc_idnt
	 AND cal.day_idnt = r.day_idnt
	LEFT JOIN dropship ds
	  ON ds.day_dt = cal.day_date
	 AND bo.RMS_SKU_NUM  = ds.sku_idnt
	LEFT JOIN last_receipt lr
	  ON lr.sku_idnt = bo.RMS_SKU_NUM
	 AND lr.loc_idnt = bo.STORE_NUM
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44

)
WITH DATA
PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country)
    ON backorder_sku;

--grabbing instock metrics by sku/day
CREATE MULTISET VOLATILE TABLE instock_sku AS (
			Select tw.day_date,
			tw.country
			, loc.banner
			, loc.channel
			, tw.rms_sku_num as sku_idnt
			, tw.rp_idnt as rp_flag
			, sum(tw.asoh) as asoh
			, SUM(NULLIF(allocated_traffic,0)) as traffic
			, SUM(case when mc_instock_ind = 1 then allocated_traffic ELSE 0 END) as instock
		FROM T2DL_DAS_TWIST.twist_daily tw
		join loc on tw.store_num = loc.store_num
		join dates on dates.day_date = tw.day_date
		GROUP BY 1,2,3,4,5,6
)
WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel, country)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (day_date, sku_idnt, channel, country)
    ON instock_sku;

--grabbing digital metrics by sku/day
CREATE MULTISET VOLATILE TABLE digital_funnel AS (
	SELECT
		cal.day_date
	    , CASE WHEN channel = 'FULL_LINE' AND channelcountry = 'US' THEN '120, N.COM'
	        WHEN channel = 'FULL_LINE' AND channelcountry = 'CA' THEN '121, N.CA'
	        WHEN channel = 'RACK' THEN '250, OFFPRICE ONLINE'
	        END AS channel
	    , channelcountry AS country
	    , CASE WHEN channel = 'FULL_LINE' AND channelcountry = 'US' THEN '120'
	        WHEN channel = 'FULL_LINE' AND channelcountry = 'CA' THEN '121'
	        WHEN channel = 'RACK' THEN '250'
	        END AS channel_idnt
	    , CASE WHEN channel = 'FULL_LINE' THEN 'FP'
	        WHEN channel = 'RACK' THEN 'OP'
	        END AS banner
		,f.rms_sku_num AS sku_idnt
		,f.rp_ind as rp_flag
		,f.current_price_type as price_type
    ,SUM(order_sessions) AS order_sessions
    ,SUM(add_to_bag_quantity) AS add_qty
    ,SUM(product_views) AS product_views
    ,SUM(product_views * pct_instock) AS instock_views
    ,SUM(CASE WHEN f.pct_instock IS NOT NULL THEN f.product_views END) AS scored_views
		,SUM(order_demand) as order_demand
		,SUM(order_quantity) as order_quantity
	FROM t2dl_das_product_funnel.product_price_funnel_daily f
	JOIN dates cal
	  ON f.event_date_pacific = cal.day_date
	  WHERE channel <> 'UNKNOWN'
	GROUP BY 1,2,3,4,5,6,7,8

)
WITH DATA
PRIMARY INDEX (day_date, sku_idnt, channel_idnt, country, rp_flag, price_type)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (day_date, sku_idnt, channel_idnt, country, rp_flag, price_type)
	ON digital_funnel;


--udig info to support Holiday metric adds
CREATE MULTISET VOLATILE TABLE udig_sku AS (
    SELECT
      rnk.sku_idnt
      , banner
      , CASE
          when banner = 'RACK' and udig_rank = 3 then '1, HOL23_NR_CYBER_SPECIAL PURCHASE'
          when banner = 'NORDSTROM' and udig_rank = 1 then '1, HOL23_N_CYBER_SPECIAL PURCHASE'
          when banner = 'NORDSTROM' and udig_rank = 2 then '2, CYBER 2023_US'
      END AS udig
    FROM
        (SELECT
            sku_idnt
			, banner
            , MIN(case
                when item_group = '1, HOL23_N_CYBER_SPECIAL PURCHASE' then 1
                when item_group = '2, CYBER 2023_US' then 2
                when item_group = '1, HOL23_NR_CYBER_SPECIAL PURCHASE' then 3
                end) as udig_rank
            FROM
                (
                select distinct
                    sku_idnt,
                    case
                      when udig_colctn_idnt = 50009 then 'RACK'
                      else 'NORDSTROM'
                    end as banner,
                    udig_itm_grp_idnt || ', ' || udig_itm_grp_nm as item_group
                from prd_usr_vws.udig_itm_grp_sku_lkup
                where udig_colctn_idnt in ('50008','50009','25032')
                and udig_itm_grp_nm in ('HOL23_NR_CYBER_SPECIAL PURCHASE', 'HOL23_N_CYBER_SPECIAL PURCHASE', 'CYBER 2023_US')
                ) item_group
            GROUP BY 1,2
        ) rnk
)
WITH DATA
PRIMARY INDEX (sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (sku_idnt)
	ON udig_sku;

--combine the receipt/sales/inventory
CREATE MULTISET VOLATILE TABLE core_metrics AS (
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
	     -- Sales
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
	    -- inventory
	    ,SUM(COALESCE(boh_units,0)) AS boh_units
	    ,SUM(COALESCE(boh_dollars,0)) AS boh_dollars
	    ,SUM(COALESCE(boh_cost,0)) AS boh_cost
	    ,SUM(COALESCE(eoh_units,0)) AS eoh_units
	    ,SUM(COALESCE(eoh_dollars,0)) AS eoh_dollars
	    ,SUM(COALESCE(eoh_cost,0)) AS eoh_cost
	    ,SUM(COALESCE(nonsellable_units,0)) AS nonsellable_units
	    -- receipts
	    ,MAX(last_receipt_date) AS last_receipt_date
	    	    --cost_metrics
	    ,SUM(COALESCE(cost_of_goods_sold,0)) as cost_of_goods_sold
	    ,SUM(COALESCE(product_margin,0)) as product_margin
		,SUM(COALESCE(sales_pm,0)) as sales_pm
	    --backorder metrics
	    ,SUM(COALESCE(backorder_units,0)) as backorder_units
	    ,SUM(COALESCE(backorder_dollars,0)) as backorder_dollars
	    --oo metrics
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
)
WITH DATA
PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country, price_type, rp_flag)
ON COMMIT preserve rows;

COLLECT STATS
     PRIMARY INDEX (day_date, week_idnt, sku_idnt, channel, country, price_type, rp_flag)
    ON core_metrics
;

--combine all metrics, add in new / cf flag, fanatics flag, and los flag and insert into base table
DELETE FROM {environment_schema}.item_performance_daily_base{env_suffix} WHERE day_date BETWEEN {start_date} and {end_date};
INSERT INTO {environment_schema}.item_performance_daily_base{env_suffix}
	SELECT
	  	s.day_date
	  	,CAST(s.week_idnt as INTEGER) as week_idnt
	  	,week_desc
	    ,CAST(s.month_idnt as INTEGER) as month_idnt
	    ,CAST(s.quarter_idnt as INTEGER) as quarter_idnt
	    ,CAST(s.fiscal_year_num as INTEGER) as year_idnt
	    ,CAST(s.month_end_week_idnt as INTEGER) as month_end_week_idnt
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
	     -- Sales
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
	    -- inventory
	    ,COALESCE(boh_units,0) AS boh_units
	    ,COALESCE(boh_dollars,0) AS boh_dollars
	    ,COALESCE(boh_cost,0) AS boh_cost
	    ,COALESCE(eoh_units,0) AS eoh_units
	    ,COALESCE(eoh_dollars,0) AS eoh_dollars
	    ,COALESCE(eoh_cost,0) AS eoh_cost
	    ,COALESCE(nonsellable_units,0) AS nonsellable_units
		,COALESCE(asoh,0) AS asoh_units
	    -- receipts
	    ,last_receipt_date AS last_receipt_date
	    -- OO
	    ,COALESCE(oo_units,0) as oo_units
	    ,COALESCE(oo_dollars,0) as oo_dollars
	    -- digital metrics
	    ,COALESCE(add_qty,0) AS add_qty
	    ,COALESCE(product_views,0) AS product_views
	    ,COALESCE(instock_views,0) AS instock_views
	--    ,COALESCE(scored_views,0) AS scored_views
	    ,COALESCE(order_demand,0) as order_demand
	    ,COALESCE(order_quantity,0) as order_quantity

	    --cost_metrics
	    ,COALESCE(cost_of_goods_sold,0) as cost_of_goods_sold
	    ,COALESCE(product_margin,0) as product_margin
		,COALESCE(sales_pm,0) as sales_pm
	    --backorder metrics
	    ,COALESCE(backorder_units,0) as backorder_units
	    ,COALESCE(backorder_dollars,0) as backorder_dollars
	    --instock metrics, 4/8/23 - Nulled out until dupes can be handled
		, 0 as traffic --COALESCE(traffic,0) as traffic
	    , 0 as instock_traffic -- COALESCE(instock,0) as instock_traffic
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

COLLECT STATS
     PRIMARY INDEX(day_date, country, channel, sku_idnt)
     ON {environment_schema}.item_performance_daily_base{env_suffix};
-- end