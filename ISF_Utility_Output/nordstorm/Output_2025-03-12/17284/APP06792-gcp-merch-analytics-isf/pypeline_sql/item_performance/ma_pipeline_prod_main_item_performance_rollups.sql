/*
Purpose:        Inserts data in {{environment_schema}} tables for Item Performance Reporting
                    item_performance_weekly
                    item_performance_monthly
Variable(s):     {{environment_schema}} T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING (prod)
                 {{env_suffix}} '' or '_dev' tablesuffix for prod testing
Author(s):       Sara Scott

Updates (08/16)

    * Add receipt_cost metrics*/



--Images Table: gets url's for images for the performance dashboard.  is_hero_image means that it's the top image for the
--item, we are filtering to only when that is true to hopefully get the best image for the item (there are multiple angles / types of images and
--we want to try to get the best one to represent the item)
CREATE MULTISET VOLATILE TABLE images AS (


SELECT
	rms_style_group_num
	, color_num
	, asset_id
	, image_url
FROM (
	SELECT
		rms_style_group_num
		, color_num
		, asset_id
		, image_url
		, RANK() OVER (PARTITION BY rms_style_group_num, color_num ORDER BY eff_begin_tmstp DESC) image_rank
	FROM PRD_NAP_USR_VWS.PRODUCT_IMAGE_ASSET_DIM
	WHERE is_hero_image = 'Y'
	AND current_date <= eff_end_tmstp
		) a
WHERE image_rank = 1

)
WITH DATA
PRIMARY INDEX(rms_style_group_num, color_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX(rms_style_group_num, color_num) ON images;


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



  CREATE MULTISET VOLATILE TABLE loc AS(

SELECT
	store_num
	, store_name
	, channel_country AS country
	, CASE
	    WHEN channel_num IN ('110', '120', '130', '140', '111', '121', '310', '311') THEN 'FP'
	    WHEN channel_num IN ('211', '221', '261', '210', '220', '250', '260') THEN 'OP'
	    ELSE NULL
	    END AS banner
  , CASE WHEN selling_channel = 'ONLINE' THEN 'DIGITAL' ELSE selling_channel END as selling_channel
	,CAST (channel_num as VARCHAR(50)) || ', ' || CAST(channel_desc as VARCHAR(100)) AS channel
FROM  PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
WHERE channel_num in('110','111', '120', '121', '130', '140', '210', '211', '220', '221','250', '260', '261',  '310', '311')
GROUP BY 1,2,3,4,5,6
)
WITH DATA
PRIMARY INDEX (store_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX (store_num)
     ON loc;

CREATE MULTISET VOLATILE TABLE receipts_base AS (
SELECT 
r.sku_idnt
, mnth_idnt
, week_num 
, week_start_day_date 
, channel
, banner
, country
, selling_channel
, CASE WHEN ds_ind = 'Y' THEN 1 ELSE 0 END as ds_ind
, CASE WHEN rp_ind = 'Y' THEN 1 ELSE 0 END as rp_ind
, price_type
, MAX(last_receipt_date) as last_receipt_date
, SUM(receipt_tot_units + receipt_rsk_units) as rcpt_tot_units 
, SUM(receipt_tot_retail + receipt_rsk_retail) as rcpt_tot_retail
, SUM(receipt_tot_cost + receipt_rsk_cost) as rcpt_tot_cost -- NEW
, SUM(receipt_ds_units) as rcpt_ds_units 
, SUM(receipt_ds_retail) as rcpt_ds_retail 
, SUM(receipt_ds_cost) as rcpt_ds_cost -- NEW
, SUM(receipt_rsk_units) as rcpt_rsk_units 
, SUM(receipt_rsk_retail) as rcpt_rsk_retail
, SUM(receipt_rsk_cost) as rcpt_rsk_cost -- NEW
FROM T2DL_DAS_ASSORTMENT_DIM.receipt_sku_loc_week_agg_fact r 
JOIN loc on r.store_num = loc.store_num
LEFT JOIN last_receipt l
  ON l.sku_idnt = r.sku_idnt
  AND l.loc_idnt = r.store_num
WHERE week_num >= 202301
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
UNION ALL 
SELECT 
r.sku_idnt 
, mnth_idnt
, week_num 
, week_start_day_date 
, channel
, banner
, country
, selling_channel
, CASE WHEN receipt_ds_units >0 THEN 1 ELSE 0 END as ds_ind
, rp_ind
, price_type
, MAX(last_receipt_date) as last_receipt_date
, SUM(receipt_po_units + receipt_ds_units + receipt_rsk_units) as rcpt_tot_units 
, SUM(receipt_po_retail + receipt_ds_retail + receipt_rsk_retail) as rcpt_tot_retail
, 0 as rcpt_tot_cost -- NEW
, SUM(receipt_ds_units) as rcpt_ds_units 
, SUM(receipt_ds_retail) as rcpt_ds_retail 
, 0 as rcpt_ds_cost -- NEW
, SUM(receipt_rsk_units) as rcpt_rsk_units 
, SUM(receipt_rsk_retail) as rcpt_rsk_retail
, 0 as rcpt_rsk_cost -- NEW
FROM T2DL_DAS_ASSORTMENT_DIM.receipt_sku_loc_week_agg_fact_madm r
JOIN loc on r.store_num = loc.store_num
LEFT JOIN last_receipt l
  ON l.sku_idnt = r.sku_idnt
  AND l.loc_idnt = r.store_num
WHERE week_num < 202301
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
WITH DATA
PRIMARY INDEX (sku_idnt, channel)
ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE hierarchy
AS (
	SELECT DISTINCT
	     supp_part_num AS VPN
	    , style_group_num
	    , rms_sku_num AS sku_idnt
	    , rms_style_num as style_num
	    , style_desc
	    , sku.color_num
		  , supp_color
	    , channel_country
	    , prmy_supp_num AS supplier_idnt
	    , supp.vendor_name as supp_name
	    , sku.brand_name
	    , npg.npg_flag
	    , CAST(div_num AS INTEGER) AS div_idnt
	    , sku.div_desc
	    , CAST(grp_num AS INTEGER) AS subdiv_idnt -- subdivision
	    , sku.grp_desc as subdiv_desc
	    , CAST(dept_num  AS INTEGER)AS dept_idnt
	    , sku.dept_desc
	    , CAST(class_num AS INTEGER) AS class_idnt
	    , sku.sbclass_desc
	    , CAST(sbclass_num AS INTEGER) AS sbclass_idnt
	    , sku.class_desc
	    , i.asset_id
	    , i.image_url
	    , ccs.QUANTRIX_CATEGORY
		, ccs.CCS_CATEGORY
		, ccs.CCS_SUBCATEGORY
		, ccs.NORD_ROLE_DESC
		, ccs.RACK_ROLE_DESC
		, ccs.PARENT_GROUP
		, ccs.MERCH_THEMES
		, ccs.HOLIDAY_THEME_TY
		, ccs.GIFT_IND
		, ccs.STOCKING_STUFFER
		, ccs.ASSORTMENT_GROUPING
	FROM prd_nap_usr_vws.product_sku_dim_vw sku
	LEFT JOIN T2DL_DAS_CCS_CATEGORIES.CCS_MERCH_THEMES ccs
	on sku.div_num = ccs.DIV_IDNT
	and sku.dept_num = ccs.DEPT_IDNT
	and sku.class_num = ccs.CLASS_IDNT
	and sku.sbclass_num = ccs.SBCLASS_IDNT
	LEFT JOIN images i
	ON sku.style_group_num = i.rms_style_group_num
	AND sku.color_num = i.color_num
	LEFT JOIN prd_nap_usr_vws.vendor_dim supp
		ON sku.prmy_supp_num = supp.vendor_num
	LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM npg
    ON npg.vendor_num = sku.prmy_supp_num

	WHERE sku.div_desc NOT LIKE 'INACTIVE%'
	QUALIFY dense_rank() OVER (PARTITION BY sku_idnt, channel_country ORDER BY sku.dw_sys_load_tmstp DESC) = 1
)
WITH DATA
PRIMARY INDEX (channel_country, sku_idnt, style_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
     PRIMARY INDEX(channel_country, sku_idnt, style_num)
     ON hierarchy;




--filters to only the last 13 months (last 12 completed and current) for rollup date constraints
CREATE MULTISET VOLATILE TABLE dates_filter as (
SELECT
    day_date
    , week_label
    , week_start_day_date
    , week_end_day_date
		, month_label
    , month_start_day_date
    , month_end_day_date
		, quarter_label
		, week_idnt
		, month_idnt
    , quarter_idnt 
    , fiscal_year_num
		, case when month_end_day_date <= current_date THEN 0 else 1 END as current_month_ind
		, DENSE_RANK() OVER (order by week_idnt desc) as week_rank
FROM prd_nap_usr_vws.day_cal_454_dim tme
WHERE week_end_day_date <= current_date
QUALIFY DENSE_RANK() OVER (ORDER BY month_idnt desc) < 14

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
WITH DATA
PRIMARY INDEX(day_date)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
		PRIMARY INDEX(day_date)
		ON dates_filter;



    CREATE MULTISET VOLATILE TABLE weekly_base AS (
  WITH date_filter AS ( 
    SELECT 
      week_label
		, month_label
		, week_idnt
		, month_idnt
    , quarter_idnt 
    , fiscal_year_num as year_idnt
    , week_rank 
    FROM dates_filter 
    WHERE week_rank < 14
    GROUP BY 1,2,3,4,5,6,7
  )
  , channel_rollup AS (

    SELECT channel
    , selling_channel 
    FROM loc
    GROUP BY 1,2
  )
  SELECT 
    r.week_num as week_idnt
    , d.week_label 
    , r.mnth_idnt as month_idnt
    , d.month_label 
    , d.quarter_idnt 
    , d.year_idnt
    , country
	  , banner
    , r.channel
    , r.selling_channel
    , price_type
	  , r.sku_idnt
    , customer_choice
    , CAST(0 AS INTEGER) as days_live
    , Null as days_published
    , MAX(rp_ind) as rp_ind
    , MAX(ds_ind) as ds_ind
    ,CAST(0 AS DECIMAL(12,0)) AS net_sales_units
    ,CAST(0 AS DECIMAL(20,2)) AS net_sales_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS return_units
    ,CAST(0 AS DECIMAL(20,2)) AS return_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS demand_units
    ,CAST(0 AS DECIMAL(20,2)) AS demand_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS demand_cancel_units
    ,CAST(0 AS DECIMAL(20,2)) AS demand_cancel_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS demand_dropship_units
    ,CAST(0 AS DECIMAL(20,2)) AS demand_dropship_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS shipped_units
    ,CAST(0 AS DECIMAL(20,2)) AS shipped_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS store_fulfill_units
    ,CAST(0 AS DECIMAL(20,2)) AS store_fulfill_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS boh_units
    ,CAST(0 AS DECIMAL(20,2)) AS boh_dollars
    ,CAST(0 AS DECIMAL(20,2)) AS boh_cost
    ,CAST(0 AS DECIMAL(12,0)) AS eoh_units
    ,CAST(0 AS DECIMAL(20,2)) AS eoh_dollars
    ,CAST(0 AS DECIMAL(20,2)) AS eoh_cost

    ,CAST(0 AS DECIMAL(12,0)) AS nonsellable_units
    ,CAST(0 AS DECIMAL(12,0)) AS asoh_units

    , SUM(COALESCE(rcpt_tot_units, 0)) as receipt_units
    , SUM(COALESCE(rcpt_tot_retail, 0)) as receipt_dollars
    , SUM(COALESCE(rcpt_tot_cost,0)) as receipt_cost
    , SUM(COALESCE(rcpt_rsk_units, 0)) as reserve_stock_units
    , SUM(COALESCE(rcpt_rsk_retail, 0)) as reserve_stock_dollars
    , SUM(COALESCE(rcpt_rsk_cost,0)) as reserve_stock_cost
    , SUM(COALESCE(rcpt_ds_units, 0)) as receipt_dropship_units
    , SUM(COALESCE(rcpt_ds_retail, 0)) as receipt_dropship_dollars
    , SUM(COALESCE(rcpt_ds_cost,0)) as receipt_dropship_cost
    , MAX(last_receipt_date) AS last_receipt_date
    ,CAST(0 AS DECIMAL(10,0)) AS oo_units
    ,CAST(0 AS DECIMAL(12,2)) AS oo_dollars	
	,CAST(0 AS DECIMAL(10,0)) AS backorder_units
    ,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
	, CAST(0 AS DECIMAL(12,2)) as instock_traffic
	, CAST(0 AS DECIMAL(12,2)) as traffic
    -- digital metrics
    , CAST(0 AS INTEGER) as add_qty
    , CAST(0 AS DECIMAL(12,2)) as product_views
    , CAST(0 AS DECIMAL(12,2)) as instock_views
    , CAST(0 AS DECIMAL(12,2)) as order_demand
    , CAST(0 AS INTEGER) as order_quantity
    ,CAST(0 AS DECIMAL(12,2)) AS cost_of_goods_sold
    ,CAST(0 AS DECIMAL(12,2)) AS product_margin
    , CAST(0 AS DECIMAL(12,2)) AS sales_pm
  FROM receipts_base r
  JOIN date_filter d
  ON r.week_num = d.week_idnt 
  JOIN t2dl_das_assortment_dim.sku_cc_lkp  s
  ON r.sku_idnt = s.sku_idnt 
  AND r.country = s.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

UNION ALL 

SELECT
	  f.week_idnt
	, week_label
    , f.month_idnt
    , month_label
    , f.quarter_idnt
    , year_idnt
    , f.country
    , f.banner
    , f.channel
    , c.selling_channel
    , price_type
	, f.sku_idnt
    , customer_choice
	, SUM(days_live) days_live
   , MIN(days_published) as days_published
	, MAX(rp_flag) as rp_flag
    , MAX(dropship_flag) as dropship_flag
    -- Sales
    , SUM(COALESCE(net_sales_units, 0)) as net_sales_units
    , SUM(COALESCE(net_sales_dollars, 0)) as net_sales_dollars
    , SUM(COALESCE(return_units, 0)) as return_units
    , SUM(COALESCE(return_dollars, 0)) as return_dollars
    , SUM(COALESCE(demand_units, 0)) as demand_units
    , SUM(COALESCE(demand_dollars, 0)) as demand_dollars
    , SUM(COALESCE(demand_cancel_units, 0)) as demand_cancel_units
    , SUM(COALESCE(demand_cancel_dollars, 0)) as demand_cancel_dollars
    , SUM(COALESCE(demand_dropship_units, 0)) as demand_dropship_units
    , SUM(COALESCE(demand_dropship_dollars, 0)) as demand_dropship_dollars
    , SUM(COALESCE(shipped_units, 0)) as shipped_units
    , SUM(COALESCE(shipped_dollars, 0)) as shipped_dollars
    , SUM(COALESCE(store_fulfill_units, 0)) as store_fulfill_units
    , SUM(COALESCE(store_fulfill_dollars, 0)) as store_fulfill_dollars
    -- inventory
    , SUM(CASE WHEN f.day_date = d.week_start_day_date THEN boh_units ELSE 0 END) as boh_units
    , SUM(CASE WHEN f.day_date = d.week_start_day_date THEN boh_dollars ELSE 0 END) as boh_dollars
    , SUM(CASE WHEN f.day_date = d.week_start_day_date THEN boh_cost ELSE 0 END) as boh_cost
    , SUM(CASE WHEN f.day_date = d.week_end_day_date THEN eoh_units ELSE 0 END) as eoh_units
    , SUM(CASE WHEN f.day_date = d.week_end_day_date THEN eoh_dollars ELSE 0 END) as eoh_dollars
    , SUM(CASE WHEN f.day_date = d.week_end_day_date THEN eoh_cost ELSE 0 END) as eoh_cost
    , SUM(CASE WHEN f.day_date = d.week_end_day_date THEN nonsellable_units ELSE 0 END) as nonsellable_units
    , SUM(CASE WHEN f.day_date = d.week_end_day_date THEN asoh_units ELSE 0 END) as asoh_units
    -- receipts
    , CAST(0 AS DECIMAL(10,0)) AS receipt_units
	, CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	, CAST(0 AS DECIMAL(12,2)) AS receipt_cost
	, CAST(0 AS DECIMAL(10,0)) AS reserve_stock_units
	, CAST(0 AS DECIMAL(12,2)) AS reserve_stock_dollars
	, CAST(0 AS DECIMAL(12,2)) AS reserve_stock_cost
	, CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
	, CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
	, CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
  , MAX(last_receipt_date) AS last_receipt_date 
    -- oo
    , SUM(COALESCE(oo_units, 0)) as oo_units
    , SUM(COALESCE(oo_dollars, 0)) as oo_dollars
    --b ackorders
    , SUM(COALESCE(backorder_units,0)) as backorder_units
    , SUM(COALESCE(backorder_dollars,0)) as backorder_dollars
		-- instock
		, SUM(COALESCE(instock_traffic	,0)) as instock_traffic
		, SUM(COALESCE(traffic, 0)) as traffic
    -- digital metrics
    , SUM(COALESCE(add_qty, 0)) as add_qty
    , SUM(COALESCE(product_views, 0)) as product_views
    , SUM(COALESCE(instock_views, 0)) as instock_views
    , SUM(COALESCE(order_demand, 0)) as order_demand
    , SUM(COALESCE(order_quantity, 0)) as order_quantity
    --cost_metrics
    , SUM(COALESCE(cost_of_goods_sold, 0)) as cost_of_goods_sold
    , SUM(COALESCE(product_margin, 0)) as product_margin
	, SUM(COALESCE(sales_pm,0)) as sales_pm
FROM t2dl_das_selection.item_performance_daily_base{env_suffix} f
JOIN dates_filter d on f.day_date = d.day_date
JOIN t2dl_das_assortment_dim.sku_cc_lkp s
ON f.sku_idnt = s.sku_idnt 
and f.country = s.channel_country
JOIN channel_rollup c
ON f.channel = c.channel
WHERE d.week_rank < 14

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

)
WITH DATA
PRIMARY INDEX(sku_idnt,channel, week_idnt )
ON COMMIT PRESERVE ROWS;


DELETE FROM {environment_schema}.item_performance_weekly{env_suffix} ALL;
INSERT INTO {environment_schema}.item_performance_weekly{env_suffix}
SELECT
	week_idnt
	, week_label
    , month_idnt
    , month_label
    , quarter_idnt
    , year_idnt
    , country
    , f.banner
    , f.channel
    , price_type
    , hierarchy.VPN
    , style_group_num
    , style_num
    , style_desc
    , hierarchy.color_num
    , supp_color
    , asset_id
    , image_url
    , supplier_idnt
    , supp_name as supplier_name
    , brand_name
    , div_idnt
    , div_desc
    , subdiv_idnt -- subdivision
    , subdiv_desc
    , dept_idnt
    , dept_desc
    , class_idnt
    , class_desc
    , sbclass_idnt
    , sbclass_desc
		, quantrix_category
		, ccs_category
		, ccs_subcategory
		, nord_role_desc
		, rack_role_desc
		, PARENT_GROUP
		, MERCH_THEMES
		, HOLIDAY_THEME_TY
		, GIFT_IND
		, STOCKING_STUFFER
		, ASSORTMENT_GROUPING
		, udig
		, CASE WHEN fan.sku_idnt is not null then 1 else 0 end as fanatics_flag
    , CASE WHEN cc.cc_type = 'NEW' THEN 1 ELSE 0 END as new_flag
		, CASE WHEN cc.cc_type = 'CF' THEN 1 ELSE 0 END AS cf_flag
		, MAX(days_live) as days_live
    , MIN(days_published) as days_published
		, MAX(npg_flag)  as npg_flag
		, MAX(rp_ind) as rp_flag
    , MAX(ds_ind) as dropship_flag
    -- Item Intent
    , intended_season
    , intended_exit_month_year
    , intended_lifecycle_type
    , scaled_event
    , holiday_or_celebration
    -- Sales
    , SUM(COALESCE(net_sales_units, 0)) as net_sales_units
    , SUM(COALESCE(net_sales_dollars, 0)) as net_sales_dollars
    , SUM(COALESCE(return_units, 0)) as return_units
    , SUM(COALESCE(return_dollars, 0)) as return_dollars
    , SUM(COALESCE(demand_units, 0)) as demand_units
    , SUM(COALESCE(demand_dollars, 0)) as demand_dollars
    , SUM(COALESCE(demand_cancel_units, 0)) as demand_cancel_units
    , SUM(COALESCE(demand_cancel_dollars, 0)) as demand_cancel_dollars
    , SUM(COALESCE(demand_dropship_units, 0)) as demand_dropship_units
    , SUM(COALESCE(demand_dropship_dollars, 0)) as demand_dropship_dollars
    , SUM(COALESCE(shipped_units, 0)) as shipped_units
    , SUM(COALESCE(shipped_dollars, 0)) as shipped_dollars
    , SUM(COALESCE(store_fulfill_units, 0)) as store_fulfill_units
    , SUM(COALESCE(store_fulfill_dollars, 0)) as store_fulfill_dollars
    -- inventory
    , SUM(COALESCE(boh_units, 0)) as boh_units
    , SUM(COALESCE(boh_dollars, 0)) as boh_dollars
    , SUM(COALESCE(boh_cost, 0)) as boh_cost
    , SUM(COALESCE(eoh_units, 0)) as eoh_units
    , SUM(COALESCE(eoh_dollars, 0)) as eoh_dollars
    , SUM(COALESCE(eoh_cost, 0)) as eoh_cost
    , SUM(COALESCE(nonsellable_units, 0)) as nonsellable_units
    , SUM(COALESCE(asoh_units, 0)) as asoh_units
    -- receipts
    , SUM(COALESCE(receipt_units, 0)) as receipt_units
    , SUM(COALESCE(receipt_dollars, 0)) as receipt_dollars
    , SUM(COALESCE(receipt_cost, 0)) as receipt_cost
    , SUM(COALESCE(reserve_stock_units, 0)) as reserve_stock_units
    , SUM(COALESCE(reserve_stock_dollars, 0)) as reserve_stock_dollars
    , SUM(COALESCE(reserve_stock_cost, 0)) as reserve_stock_cost
    , SUM(COALESCE(receipt_dropship_units, 0)) as receipt_dropship_units
    , SUM(COALESCE(receipt_dropship_dollars, 0)) as receipt_dropship_dollars
    , SUM(COALESCE(receipt_dropship_cost, 0)) as receipt_dropship_cost
    , MAX(last_receipt_date) as last_receipt_date
    -- oo
    , SUM(COALESCE(oo_units, 0)) as oo_units
    , SUM(COALESCE(oo_dollars, 0)) as oo_dollars
    -- backorders
    , SUM(COALESCE(backorder_units,0)) as backorder_units
    , SUM(COALESCE(backorder_dollars,0)) as backorder_dollars
    -- instock
    , SUM(COALESCE(instock_traffic, 0)) as instock_traffic
    , SUM(COALESCE(traffic, 0)) as traffic
    -- digital metrics
    , SUM(COALESCE(add_qty, 0)) as add_qty
    , SUM(COALESCE(product_views, 0)) as product_views
    , SUM(COALESCE(instock_views, 0)) as instock_views
    , SUM(COALESCE(order_demand, 0)) as order_demand
    , SUM(COALESCE(order_quantity, 0)) as order_quantity
    --cost_metrics
    , SUM(COALESCE(cost_of_goods_sold, 0)) as cost_of_goods_sold
    , SUM(COALESCE(product_margin, 0)) as product_margin
		, SUM(COALESCE(sales_pm,0)) as sales_pm
FROM weekly_base f JOIN  hierarchy
ON f.sku_idnt = hierarchy.sku_idnt
and f.country = hierarchy.channel_country
LEFT JOIN udig_sku u
ON u.sku_idnt = f.sku_idnt
AND CASE WHEN u.banner = 'RACK' THEN 'OP' WHEN u.banner = 'NORDSTROM' THEN 'FP' END  = f.banner
LEFT JOIN fanatics fan 
ON f.sku_idnt = fan.sku_idnt 
LEFT JOIN t2dl_das_assortment_dim.cc_type cc
ON cc.customer_choice = f.customer_choice 
AND cc.channel_country = f.country
AND cc.channel = f.selling_channel
AND CASE WHEN cc.channel_brand = 'NORDSTROM' THEN 'FP' WHEN cc.channel_brand = 'NORDSTROM_RACK' THEN 'OP' END = f.banner
and cc.mnth_idnt = f.month_idnt
LEFT JOIN T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.item_intent_lookup{env_suffix} item_stg 
ON item_stg.rms_style_num = hierarchy.style_num 
AND item_stg.sku_nrf_color_num = hierarchy.color_num 
AND item_stg.channel_num = left(f.channel ,3)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,52,53,54,55,56;


CREATE MULTISET VOLATILE TABLE monthly_base as (
  WITH channel_rollup AS (

    SELECT channel
    , selling_channel 
    FROM loc
    GROUP BY 1,2
  )
SELECT
	 f.month_idnt
	  , month_label
    , f.quarter_idnt
    , f.year_idnt
    , f.country
    , f.banner
    , f.channel
    , c.selling_channel
    , price_type
	  , fanatics_flag
		, sku_idnt
		, udig
		, days_live
    , days_published
		, rp_flag
    , dropship_flag
    -- Sales
    , COALESCE(net_sales_units, 0) as net_sales_units
    , COALESCE(net_sales_dollars, 0) as net_sales_dollars
    , COALESCE(return_units, 0) as return_units
    , COALESCE(return_dollars, 0) as return_dollars
    , COALESCE(demand_units, 0) as demand_units
    , COALESCE(demand_dollars, 0) as demand_dollars
    , COALESCE(demand_cancel_units, 0) as demand_cancel_units
    , COALESCE(demand_cancel_dollars, 0) as demand_cancel_dollars
    , COALESCE(demand_dropship_units, 0) as demand_dropship_units
    , COALESCE(demand_dropship_dollars, 0) as demand_dropship_dollars
    , COALESCE(shipped_units, 0) as shipped_units
    , COALESCE(shipped_dollars, 0) as shipped_dollars
    , COALESCE(store_fulfill_units, 0) as store_fulfill_units
    , COALESCE(store_fulfill_dollars, 0) as store_fulfill_dollars
    -- inventory
    , CASE WHEN f.day_date = d.month_start_day_date THEN boh_units ELSE 0 END as boh_units
    , CASE WHEN f.day_date = d.month_start_day_date THEN boh_dollars ELSE 0 END as boh_dollars
    , CASE WHEN f.day_date = d.month_start_day_date THEN boh_cost ELSE 0 END as boh_cost
    , CASE WHEN f.day_date = d.month_end_day_date THEN eoh_units ELSE 0 END as eoh_units
    , CASE WHEN f.day_date = d.month_end_day_date THEN eoh_dollars ELSE 0 END as eoh_dollars
    , CASE WHEN f.day_date = d.month_end_day_date THEN eoh_cost ELSE 0 END as eoh_cost
    , CASE WHEN f.day_date = d.month_end_day_date THEN nonsellable_units ELSE 0 END as nonsellable_units
    , CASE WHEN f.day_date = d.month_end_day_date THEN asoh_units ELSE 0 END as asoh_units
    -- receipts
    , last_receipt_date
    -- oo
    , COALESCE(oo_units, 0) as oo_units
    , COALESCE(oo_dollars, 0) as oo_dollars
    -- instock
    , COALESCE(instock_traffic, 0) as instock_traffic
    , COALESCE(traffic, 0) as traffic
    -- backorders
    , COALESCE(backorder_units,0) as backorder_units
    , COALESCE(backorder_dollars,0) as backorder_dollars
    -- digital metrics
    , COALESCE(add_qty, 0) as add_qty
    , COALESCE(product_views, 0) as product_views
    , COALESCE(instock_views, 0) as instock_views
    , COALESCE(order_demand, 0) as order_demand
    , COALESCE(order_quantity, 0) as order_quantity
    --cost_metrics
    , COALESCE(cost_of_goods_sold, 0) as cost_of_goods_sold
    , COALESCE(product_margin, 0) as product_margin
	  , COALESCE(sales_pm,0) as sales_pm
FROM t2dl_das_selection.item_performance_daily_base{env_suffix} f
JOIN dates_filter d ON f.day_date = d.day_date
JOIN channel_rollup c
ON f.channel = c.channel
AND current_month_ind = 0
)
WITH DATA
PRIMARY INDEX(sku_idnt, channel, country, month_idnt )
ON COMMIT PRESERVE ROWS;

COLLECT STATS PRIMARY INDEX(sku_idnt, channel, country, month_idnt ) ON monthly_base;




    CREATE MULTISET VOLATILE TABLE monthly_base_receipts AS (
  WITH date_filter AS ( 
    SELECT 
    month_label
		, month_idnt
    , quarter_idnt 
    , fiscal_year_num as year_idnt
    FROM dates_filter 
    WHERE current_month_ind = 0
    GROUP BY 1,2,3,4
  )
  SELECT 
   r.mnth_idnt as month_idnt
    , d.month_label 
    , d.quarter_idnt 
    , d.year_idnt
    , country
	  , banner
    , r.channel
    , r.selling_channel
    , price_type
	  , r.sku_idnt
    , customer_choice
    , CAST(0 AS INTEGER) as days_live
    , Null as days_published
    , MAX(rp_ind) as rp_ind
    , MAX(ds_ind) as ds_ind
    ,CAST(0 AS DECIMAL(12,0)) AS net_sales_units
    ,CAST(0 AS DECIMAL(20,2)) AS net_sales_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS return_units
    ,CAST(0 AS DECIMAL(20,2)) AS return_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS demand_units
    ,CAST(0 AS DECIMAL(20,2)) AS demand_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS demand_cancel_units
    ,CAST(0 AS DECIMAL(20,2)) AS demand_cancel_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS demand_dropship_units
    ,CAST(0 AS DECIMAL(20,2)) AS demand_dropship_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS shipped_units
    ,CAST(0 AS DECIMAL(20,2)) AS shipped_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS store_fulfill_units
    ,CAST(0 AS DECIMAL(20,2)) AS store_fulfill_dollars
    ,CAST(0 AS DECIMAL(12,0)) AS boh_units
    ,CAST(0 AS DECIMAL(20,2)) AS boh_dollars
    ,CAST(0 AS DECIMAL(20,2)) AS boh_cost
    ,CAST(0 AS DECIMAL(12,0)) AS eoh_units
    ,CAST(0 AS DECIMAL(20,2)) AS eoh_dollars
    ,CAST(0 AS DECIMAL(20,2)) AS eoh_cost

    ,CAST(0 AS DECIMAL(12,0)) AS nonsellable_units
    ,CAST(0 AS DECIMAL(12,0)) AS asoh_units

    , SUM(COALESCE(rcpt_tot_units, 0)) as receipt_units
    , SUM(COALESCE(rcpt_tot_retail, 0)) as receipt_dollars
    , SUM(COALESCE(rcpt_tot_cost, 0)) as receipt_cost
    , SUM(COALESCE(rcpt_rsk_units, 0)) as reserve_stock_units
    , SUM(COALESCE(rcpt_rsk_retail, 0)) as reserve_stock_dollars
    , SUM(COALESCE(rcpt_rsk_cost, 0)) as reserve_stock_cost
    , SUM(COALESCE(rcpt_ds_units, 0)) as receipt_dropship_units
    , SUM(COALESCE(rcpt_ds_retail, 0)) as receipt_dropship_dollars
    , SUM(COALESCE(rcpt_ds_cost, 0)) as receipt_dropship_cost
    , MAX(last_receipt_date) AS last_receipt_date
    ,CAST(0 AS DECIMAL(10,0)) AS oo_units
    ,CAST(0 AS DECIMAL(12,2)) AS oo_dollars	
	,CAST(0 AS DECIMAL(10,0)) AS backorder_units
    ,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
	, CAST(0 AS DECIMAL(12,2)) as instock_traffic
	, CAST(0 AS DECIMAL(12,2)) as traffic
    -- digital metrics
    , CAST(0 AS INTEGER) as add_qty
    , CAST(0 AS DECIMAL(12,2)) as product_views
    , CAST(0 AS DECIMAL(12,2)) as instock_views
    , CAST(0 AS DECIMAL(12,2)) as order_demand
    , CAST(0 AS INTEGER) as order_quantity
    ,CAST(0 AS DECIMAL(12,2)) AS cost_of_goods_sold
    ,CAST(0 AS DECIMAL(12,2)) AS product_margin
    , CAST(0 AS DECIMAL(12,2)) AS sales_pm
  FROM receipts_base r
  JOIN date_filter d
  ON r.mnth_idnt = d.month_idnt
  JOIN t2dl_das_assortment_dim.sku_cc_lkp  s
  ON r.sku_idnt = s.sku_idnt 
  AND r.country = s.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11

UNION ALL 

SELECT
 f.month_idnt
    , month_label
    , f.quarter_idnt
    , f.year_idnt
    , f.country
    , f.banner
    , f.channel
    , selling_channel
    , price_type
	, f.sku_idnt
    , customer_choice
	, SUM(days_live) days_live
   , MIN(days_published) as days_published
	, MAX(rp_flag) as rp_flag
    , MAX(dropship_flag) as dropship_flag
    -- Sales
    , SUM(COALESCE(net_sales_units, 0)) as net_sales_units
    , SUM(COALESCE(net_sales_dollars, 0)) as net_sales_dollars
    , SUM(COALESCE(return_units, 0)) as return_units
    , SUM(COALESCE(return_dollars, 0)) as return_dollars
    , SUM(COALESCE(demand_units, 0)) as demand_units
    , SUM(COALESCE(demand_dollars, 0)) as demand_dollars
    , SUM(COALESCE(demand_cancel_units, 0)) as demand_cancel_units
    , SUM(COALESCE(demand_cancel_dollars, 0)) as demand_cancel_dollars
    , SUM(COALESCE(demand_dropship_units, 0)) as demand_dropship_units
    , SUM(COALESCE(demand_dropship_dollars, 0)) as demand_dropship_dollars
    , SUM(COALESCE(shipped_units, 0)) as shipped_units
    , SUM(COALESCE(shipped_dollars, 0)) as shipped_dollars
    , SUM(COALESCE(store_fulfill_units, 0)) as store_fulfill_units
    , SUM(COALESCE(store_fulfill_dollars, 0)) as store_fulfill_dollars
    -- inventory
    , SUM(boh_units) as boh_units
    , SUM(boh_dollars) as boh_dollars
    , SUM(boh_cost) as boh_cost
    , SUM(eoh_units) as eoh_units
    , SUM(eoh_dollars) as eoh_dollars
    , SUM(eoh_cost) as eoh_cost
    , SUM(nonsellable_units) as nonsellable_units
    , SUM(asoh_units) as asoh_units
    -- receipts
    , CAST(0 AS DECIMAL(10,0)) AS receipt_units
	, CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	, CAST(0 AS DECIMAL(12,2)) AS receipt_cost
	, CAST(0 AS DECIMAL(10,0)) AS reserve_stock_units
	, CAST(0 AS DECIMAL(12,2)) AS reserve_stock_dollars
	, CAST(0 AS DECIMAL(12,2)) AS reserve_stock_cost
	, CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
	, CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
	, CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
  , MAX(last_receipt_date) AS last_receipt_date 
    -- oo
    , SUM(COALESCE(oo_units, 0)) as oo_units
    , SUM(COALESCE(oo_dollars, 0)) as oo_dollars
    --b ackorders
    , SUM(COALESCE(backorder_units,0)) as backorder_units
    , SUM(COALESCE(backorder_dollars,0)) as backorder_dollars
		-- instock
		, SUM(COALESCE(instock_traffic	,0)) as instock_traffic
		, SUM(COALESCE(traffic, 0)) as traffic
    -- digital metrics
    , SUM(COALESCE(add_qty, 0)) as add_qty
    , SUM(COALESCE(product_views, 0)) as product_views
    , SUM(COALESCE(instock_views, 0)) as instock_views
    , SUM(COALESCE(order_demand, 0)) as order_demand
    , SUM(COALESCE(order_quantity, 0)) as order_quantity
    --cost_metrics
    , SUM(COALESCE(cost_of_goods_sold, 0)) as cost_of_goods_sold
    , SUM(COALESCE(product_margin, 0)) as product_margin
	, SUM(COALESCE(sales_pm,0)) as sales_pm
FROM monthly_base f
JOIN t2dl_das_assortment_dim.sku_cc_lkp s
ON f.sku_idnt = s.sku_idnt 
and f.country = s.channel_country

GROUP BY 1,2,3,4,5,6,7,8,9,10,11

)
WITH DATA
PRIMARY INDEX(sku_idnt,channel, month_idnt )
ON COMMIT PRESERVE ROWS;



DELETE FROM {environment_schema}.item_performance_monthly{env_suffix} ALL;
INSERT INTO {environment_schema}.item_performance_monthly{env_suffix}
SELECT
 month_idnt
    , month_label
    , quarter_idnt
    , year_idnt
    , country
    , f.banner
    , f.channel
    , price_type
    , VPN
    , style_group_num
    , style_num
    , style_desc
    , color_num
    , supp_color
    , asset_id
    , image_url
    , supplier_idnt
    , supp_name as supplier_name
    , brand_name
    , div_idnt
    , div_desc
    , subdiv_idnt -- subdivision
    , subdiv_desc
    , dept_idnt
    , dept_desc
    , class_idnt
    , class_desc
    , sbclass_idnt
    , sbclass_desc
		, quantrix_category
		, ccs_category
		, ccs_subcategory
		, nord_role_desc
		, rack_role_desc
		, PARENT_GROUP
		, MERCH_THEMES
		, HOLIDAY_THEME_TY
		, GIFT_IND
		, STOCKING_STUFFER
		, ASSORTMENT_GROUPING
		, udig
		, CASE WHEN fan.sku_idnt is not null then 1 else 0 end as fanatics_flag
    , CASE WHEN cc.cc_type = 'NEW' THEN 1 ELSE 0 END as new_flag
		, CASE WHEN cc.cc_type = 'CF' THEN 1 ELSE 0 END AS cf_flag
		, MAX(days_live) as days_live
    , MIN(days_published) as days_published
		, MAX(npg_flag)  as npg_flag
		, MAX(rp_ind) as rp_flag
    , MAX(ds_ind) as dropship_flag
    -- Sales
    , SUM(COALESCE(net_sales_units, 0)) as net_sales_units
    , SUM(COALESCE(net_sales_dollars, 0)) as net_sales_dollars
    , SUM(COALESCE(return_units, 0)) as return_units
    , SUM(COALESCE(return_dollars, 0)) as return_dollars
    , SUM(COALESCE(demand_units, 0)) as demand_units
    , SUM(COALESCE(demand_dollars, 0)) as demand_dollars
    , SUM(COALESCE(demand_cancel_units, 0)) as demand_cancel_units
    , SUM(COALESCE(demand_cancel_dollars, 0)) as demand_cancel_dollars
    , SUM(COALESCE(demand_dropship_units, 0)) as demand_dropship_units
    , SUM(COALESCE(demand_dropship_dollars, 0)) as demand_dropship_dollars
    , SUM(COALESCE(shipped_units, 0)) as shipped_units
    , SUM(COALESCE(shipped_dollars, 0)) as shipped_dollars
    , SUM(COALESCE(store_fulfill_units, 0)) as store_fulfill_units
    , SUM(COALESCE(store_fulfill_dollars, 0)) as store_fulfill_dollars
    -- inventory
    , SUM(COALESCE(boh_units, 0)) as boh_units
    , SUM(COALESCE(boh_dollars, 0)) as boh_dollars
    , SUM(COALESCE(boh_cost, 0)) as boh_cost
    , SUM(COALESCE(eoh_units, 0)) as eoh_units
    , SUM(COALESCE(eoh_dollars, 0)) as eoh_dollars
    , SUM(COALESCE(eoh_cost, 0)) as eoh_cost
    , SUM(COALESCE(nonsellable_units, 0)) as nonsellable_units
    , SUM(COALESCE(asoh_units, 0)) as asoh_units
    -- receipts
    , SUM(COALESCE(receipt_units, 0)) as receipt_units
    , SUM(COALESCE(receipt_dollars, 0)) as receipt_dollars
    , SUM(COALESCE(receipt_cost, 0)) as receipt_cost
    , SUM(COALESCE(reserve_stock_units, 0)) as reserve_stock_units
    , SUM(COALESCE(reserve_stock_dollars, 0)) as reserve_stock_dollars
    , SUM(COALESCE(reserve_stock_cost, 0)) as reserve_stock_cost
    , SUM(COALESCE(receipt_dropship_units, 0)) as receipt_dropship_units
    , SUM(COALESCE(receipt_dropship_dollars, 0)) as receipt_dropship_dollars
    , SUM(COALESCE(receipt_dropship_cost, 0)) as receipt_dropship_cost
    , MAX(last_receipt_date) as last_receipt_date
    -- oo
    , SUM(COALESCE(oo_units, 0)) as oo_units
    , SUM(COALESCE(oo_dollars, 0)) as oo_dollars
    -- backorders
    , SUM(COALESCE(backorder_units,0)) as backorder_units
    , SUM(COALESCE(backorder_dollars,0)) as backorder_dollars
    -- instock
    , SUM(COALESCE(instock_traffic, 0)) as instock_traffic
    , SUM(COALESCE(traffic, 0)) as traffic
    -- digital metrics
    , SUM(COALESCE(add_qty, 0)) as add_qty
    , SUM(COALESCE(product_views, 0)) as product_views
    , SUM(COALESCE(instock_views, 0)) as instock_views
    , SUM(COALESCE(order_demand, 0)) as order_demand
    , SUM(COALESCE(order_quantity, 0)) as order_quantity
    --cost_metrics
    , SUM(COALESCE(cost_of_goods_sold, 0)) as cost_of_goods_sold
    , SUM(COALESCE(product_margin, 0)) as product_margin
		, SUM(COALESCE(sales_pm,0)) as sales_pm
FROM monthly_base_receipts f JOIN  hierarchy
ON f.sku_idnt = hierarchy.sku_idnt
and f.country = hierarchy.channel_country
LEFT JOIN udig_sku u
ON f.sku_idnt = u.sku_idnt 
LEFT JOIN fanatics fan 
ON f.sku_idnt = fan.sku_idnt 
LEFT JOIN t2dl_das_assortment_dim.cc_type cc
ON cc.customer_choice = f.customer_choice 
AND cc.channel = f.selling_channel
AND cc.channel_country = f.country
AND CASE WHEN cc.channel_brand = 'NORDSTROM' THEN 'FP' WHEN cc.channel_brand = 'NORDSTROM_RACK' THEN 'OP' END  = f.banner
AND cc.mnth_idnt = f.month_idnt
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44;