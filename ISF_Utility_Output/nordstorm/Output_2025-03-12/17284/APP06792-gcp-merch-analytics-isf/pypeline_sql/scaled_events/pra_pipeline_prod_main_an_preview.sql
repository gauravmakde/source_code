/*
Purpose:         Inserts data in {{environment_schema}} tables for Anniversary Preview
Variable(s):     {{environment_schema}} T2DL_DAS_SCALED_EVENTS
                 This table refreshes daily
Author(s):       Manuela Hurtado Gonzalez
Last Updated:		 2024-06-10
*/

CREATE MULTISET VOLATILE TABLE event_dates
AS (
	SELECT DISTINCT
		fiscal_year_num AS yr_num
		, 'TY' AS ty_ly_ind
		, 'US' AS country
		, day_date
		, day_idnt
		, ROW_NUMBER() OVER(PARTITION BY yr_num, country ORDER BY day_date) AS days_since_launch
	FROM prd_nap_usr_vws.day_cal_454_dim
	WHERE day_date BETWEEN DATE '2024-06-27' AND DATE '2024-08-04'
	UNION ALL
	SELECT 	DISTINCT
		fiscal_year_num AS yr_num
		, 'LY' AS ty_ly_ind
		, 'US' AS country
		, day_date
		, day_idnt
		, ROW_NUMBER() OVER(PARTITION BY yr_num, country ORDER BY day_date) AS days_since_launch
	FROM prd_nap_usr_vws.day_cal_454_dim
	WHERE day_date BETWEEN DATE '2023-07-03' AND DATE '2023-08-06'
) WITH DATA
UNIQUE PRIMARY INDEX (day_date)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( day_date )
    ,COLUMN ( day_date )
        ON event_dates;

CREATE MULTISET VOLATILE TABLE dropship
AS (
    SELECT DISTINCT
        rms_sku_id AS sku_idnt
        , snapshot_date
    FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT a
    INNER JOIN event_dates b
    ON a.snapshot_date = b.day_date
    WHERE location_type IN ('DS')
) WITH DATA
UNIQUE PRIMARY INDEX(sku_idnt, snapshot_date)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( sku_idnt, snapshot_date )
    ,COLUMN ( sku_idnt )
    ,COLUMN ( snapshot_date )
        ON dropship;

CREATE MULTISET VOLATILE TABLE rp
AS (
   SELECT distinct
	 rms_sku_num
	 , day_date
    FROM prd_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
    INNER JOIN PRD_NAP_USR_VWS.store_dim store
		ON rp.location_num = store.store_num
		INNER JOIN event_dates
		ON rp.rp_period CONTAINS event_dates.day_date
		WHERE channel_num = 120

) WITH DATA
UNIQUE PRIMARY INDEX(rms_sku_num, day_date)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS
	PRIMARY INDEX ( rms_sku_num, day_date )
	,COLUMN ( rms_sku_num )
	,COLUMN ( day_date )
    	ON rp;


CREATE MULTISET VOLATILE TABLE anniv_sku
AS (
    SELECT
    	a.sku_idnt as rms_sku_num
    	, substring(cast(a.day_idnt as varchar(7)),1,4) as yr_num
    	, a.reg_price_amt
    	, a.spcl_price_amt
    	, a.channel_country as country
    FROM T2DL_DAS_SCALED_EVENTS.ANNIVERSARY_SKU_CHNL_DATE a
    WHERE selling_channel = 'ONLINE'
    GROUP BY 1,2,3,4,5
) WITH DATA
UNIQUE PRIMARY INDEX(rms_sku_num, yr_num)
ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS
    PRIMARY INDEX ( rms_sku_num, yr_num )
    ,COLUMN ( rms_sku_num )
		,COLUMN ( yr_num )
        ON anniv_sku;

CREATE MULTISET VOLATILE TABLE INV_STG
AS (
	SELECT DISTINCT
		rms_sku_id AS sku_idnt
		, snapshot_date
		, price_store_num
		, location_id AS loc_idnt
		, stock_on_hand_qty AS soh_qty
		, in_transit_qty AS in_transit_qty
	FROM PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT inv
  INNER JOIN anniv_sku sku
       ON inv.rms_sku_id = sku.rms_sku_num
  INNER JOIN event_dates dt
       ON inv.snapshot_date = dt.day_date
	INNER JOIN (
	   SELECT DISTINCT store_num, price_store_num
	   FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
	   WHERE channel_num in (110,120)
	) loc
       ON inv.location_id = loc.store_num
	WHERE sku.yr_num = EXTRACT(YEAR FROM inv.snapshot_date)
)
WITH DATA
PRIMARY INDEX ( sku_idnt, loc_idnt, snapshot_date)
ON COMMIT PRESERVE ROWS;


COLLECT STATS
    PRIMARY INDEX (sku_idnt, loc_idnt, snapshot_date)
    ,COLUMN (sku_idnt)
    ,COLUMN (loc_idnt)
    ,COLUMN (snapshot_date)
        ON INV_STG;

CREATE MULTISET VOLATILE TABLE inventory
AS (
    SELECT
		cost.snapshot_date AS day_date
		, 'US' AS country
		, 'OTHER' AS experience
		, cost.sku_idnt AS rms_sku_num
		, CAST(0 AS INTEGER) AS product_views
		, CAST(0 AS INTEGER) AS cart_adds
		, CAST(0 AS DECIMAL(12,2)) AS com_demand
		, CAST(0 AS INTEGER) AS com_ordered_units
		, CAST(0 AS INTEGER) AS wishlist_adds
		, CAST(0 AS DECIMAL(12,2)) AS fls_gross_sales_r
		, CAST(0 AS DECIMAL(12,2)) AS fls_gross_sales_c
		, CAST(0 AS INTEGER) AS fls_gross_sales_u
    , SUM(cost.eoh_units * prc.ownership_retail_price_amt) AS eoh_dollars
    , SUM(cost.eoh_cost) as eoh_cost
    , SUM(cost.eoh_units) as eoh_units
    , SUM(cost.in_transit_u * prc.ownership_retail_price_amt) as in_transit_r
    , SUM(cost.in_transit_c) as in_transit_c
    , SUM(cost.in_transit_u) as in_transit_u
    , CAST(0 AS DECIMAL(12,2)) AS oo_r
    , CAST(0 AS DECIMAL(12,2)) AS oo_c
    , CAST(0 AS INTEGER) AS oo_u
  FROM (
  	SELECT DISTINCT
  		sku_idnt
  		, snapshot_date
  		, price_store_num
  		, SUM(soh_qty) as eoh_units
  		, SUM(in_transit_qty) as in_transit_u
  		, SUM(soh_qty * weighted_average_cost) as eoh_cost
  		, SUM(in_transit_qty * weighted_average_cost) as in_transit_c
  	FROM INV_STG base
    LEFT JOIN PRD_NAP_USR_VWS.WEIGHTED_AVERAGE_COST_DATE_DIM cst
      		ON base.sku_idnt = cst.sku_num
      		AND base.loc_idnt = cst.location_num
      		AND base.snapshot_date between cst.eff_begin_dt and cst.eff_end_dt - 1
    GROUP BY 1,2,3
  ) cost
  LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM prc
       ON cost.sku_idnt = prc.rms_sku_num
  		 AND cost.price_store_num = prc.store_num
			 AND CAST(cost.snapshot_date AS TIMESTAMP) BETWEEN prc.EFF_BEGIN_TMSTP AND prc.EFF_END_TMSTP - interval '0.001' second
  GROUP BY 1, 2, 3, 4
)
WITH DATA
PRIMARY INDEX (rms_sku_num, day_date)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( rms_sku_num, day_date )
    ,COLUMN ( rms_sku_num )
    ,COLUMN ( day_date )
        ON inventory;

CREATE MULTISET VOLATILE TABLE wishlist_adds
AS (
    SELECT
        b.day_date
        , a.country
        , experience
        , a.rms_sku_num
        , CAST(0 AS INTEGER) AS product_views
        , CAST(0 AS INTEGER) AS cart_adds
        , CAST(0 AS DECIMAL(12,2)) AS com_demand
        , CAST(0 AS INTEGER) AS com_ordered_units
        , coalesce(sum(quantity),0) AS wishlist_adds
        , CAST(0 AS DECIMAL(12,2)) AS fls_gross_sales_r
				, CAST(0 AS DECIMAL(12,2)) AS fls_gross_sales_c
        , CAST(0 AS INTEGER) AS fls_gross_sales_u
        , CAST(0 AS DECIMAL(12,2)) AS eoh_r
        , CAST(0 AS DECIMAL(12,2)) AS eoh_c
        , CAST(0 AS INTEGER) AS eoh_u
        , CAST(0 AS DECIMAL(12,2)) AS in_transit_r
				, CAST(0 AS DECIMAL(12,2)) AS in_transit_c
        , CAST(0 AS INTEGER) AS in_transit_u
        , CAST(0 AS DECIMAL(12,2)) AS oo_r
        , CAST(0 AS DECIMAL(12,2)) AS oo_c
        , CAST(0 AS INTEGER) AS oo_u
    FROM anniv_sku a
    INNER JOIN event_dates b
      ON a.country = b.country
      AND a.yr_num = b.yr_num
    INNER JOIN T2DL_DAS_SCALED_EVENTS.sku_item_added c
      ON a.rms_sku_num = c.rms_sku_num
      AND a.country = c.channelcountry
      AND b.day_date = c.event_date_pacific
    WHERE b.day_date <= current_date - 1
		AND c.channelbrand = 'NORDSTROM'
    GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (day_date, experience, rms_sku_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
		PRIMARY INDEX ( day_date, experience, rms_sku_num )
		,COLUMN ( rms_sku_num )
		,COLUMN ( day_date )
		,COLUMN ( experience )
				ON wishlist_adds;

CREATE MULTISET VOLATILE TABLE digital_funnel
AS (
    SELECT
        b.day_date
        , a.country
        , case when c.platform in ('ANDROID','nord.android') then 'ANDROID_APP'
            when c.platform in ('WEB','nord.com') then 'DESKTOP_WEB'
            when c.platform in ('MOW','nord.mow') then 'MOBILE_WEB'
            when c.platform in ('IOS','nord.ios') then 'IOS_APP'
            else 'OTHER' end as experience
        , a.rms_sku_num
        , coalesce(sum(c.product_views),0) AS product_views
        , coalesce(sum(c.add_to_bag_quantity),0) AS cart_adds
        , coalesce(sum(c.order_demand),0) AS com_demand
        , coalesce(sum(c.order_quantity),0) AS com_ordered_units
        , CAST(0 AS INTEGER) AS wishlist_adds
        , CAST(0 AS DECIMAL(12,2)) AS fls_gross_sales_r
				, CAST(0 AS DECIMAL(12,2)) AS fls_gross_sales_c
        , CAST(0 AS INTEGER) AS fls_gross_sales_u
        , CAST(0 AS DECIMAL(12,2)) AS eoh_r
        , CAST(0 AS DECIMAL(12,2)) AS eoh_c
        , CAST(0 AS INTEGER) AS eoh_u
        , CAST(0 AS DECIMAL(12,2)) AS in_transit_r
				, CAST(0 AS DECIMAL(12,2)) AS in_transit_c
        , CAST(0 AS INTEGER) AS in_transit_u
        , CAST(0 AS DECIMAL(12,2)) AS oo_r
        , CAST(0 AS DECIMAL(12,2)) AS oo_c
        , CAST(0 AS INTEGER) AS oo_u
    FROM anniv_sku a
    INNER JOIN event_dates b
      ON a.country = b.country
      AND a.yr_num = b.yr_num
    INNER JOIN t2dl_das_product_funnel.product_price_funnel_daily c
      ON a.rms_sku_num = c.rms_sku_num
      AND a.country = c.channelcountry
      AND b.day_date = c.event_date_pacific
      AND c.channel = 'FULL_LINE'
    WHERE b.day_date <= current_date - 1
    GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (day_date, experience, rms_sku_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
		PRIMARY INDEX ( day_date, experience, rms_sku_num )
		,COLUMN ( rms_sku_num )
		,COLUMN ( day_date )
		,COLUMN ( experience )
				ON digital_funnel;

CREATE MULTISET VOLATILE TABLE on_order
AS (
    SELECT
      current_date - 1 as day_date
      , sku.country
      , 'OTHER' as experience
      , sku.rms_sku_num
      , CAST(0 AS INTEGER) AS product_views
      , CAST(0 AS INTEGER) AS cart_adds
      , CAST(0 AS DECIMAL(12,2)) AS com_demand
      , CAST(0 AS INTEGER) AS com_ordered_units
      , CAST(0 AS INTEGER) AS wishlist_adds
      , CAST(0 AS DECIMAL(12,2)) AS fls_gross_sales_r
			, CAST(0 AS DECIMAL(12,2)) AS fls_gross_sales_c
      , CAST(0 AS INTEGER) AS fls_gross_sales_u
      , CAST(0 AS DECIMAL(12,2)) AS eoh_r
      , CAST(0 AS DECIMAL(12,2)) AS eoh_c
      , CAST(0 AS INTEGER) AS eoh_u
      , CAST(0 AS DECIMAL(12,2)) AS in_transit_r
			, CAST(0 AS DECIMAL(12,2)) AS in_transit_c
      , CAST(0 AS INTEGER) AS in_transit_u
      , sum(oo.ANTICIPATED_RETAIL_AMT * oo.QUANTITY_OPEN) as oo_r
      , SUM(oo.UNIT_ESTIMATED_LANDING_COST  * oo.QUANTITY_OPEN) AS oo_c
      , sum(oo.quantity_open) as oo_u
	    FROM
	        PRD_NAP_USR_VWS.MERCH_ON_ORDER_FACT_VW oo
	    INNER JOIN PRD_NAP_USR_VWS.STORE_DIM loc
	        ON oo.store_num = loc.store_num
	    INNER JOIN anniv_sku sku
	        ON oo.rms_sku_num = sku.rms_sku_num
					AND sku.yr_num = cast(LEFT(cast(oo.week_num as VARCHAR(6)),4)	as integer)
	    		WHERE ((oo.status = 'CLOSED' AND oo.END_SHIP_DATE >= current_date - 45) OR oo.status = 'APPROVED')
					AND oo.quantity_open > 0
					AND week_num <= (select max(wk_idnt) from T2DL_DAS_SCALED_EVENTS.scaled_event_dates where event_type = 'PE')
	        AND week_num >= (select min(wk_idnt) from T2DL_DAS_SCALED_EVENTS.scaled_event_dates where anniv_ind = 1)
	        AND channel_num in (110, 120)
	    GROUP BY 1, 2, 3, 4
) WITH DATA
PRIMARY INDEX (day_date, experience, rms_sku_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS
    PRIMARY INDEX ( day_date, experience, rms_sku_num )
    ,COLUMN ( day_date )
    ,COLUMN ( rms_sku_num )
		,COLUMN ( experience )
        ON on_order;

CREATE MULTISET VOLATILE TABLE sales
AS (
    SELECT
    	b.day_date
    	, a.country
    	, 'OTHER' as experience
    	, a.rms_sku_num
    	, CAST(0 AS INTEGER) AS product_views
    	, CAST(0 AS INTEGER) AS cart_adds
    	, CAST(0 AS DECIMAL(12,2)) AS com_demand
    	, CAST(0 AS INTEGER) AS com_ordered_units
    	, CAST(0 AS INTEGER) AS wishlist_adds
    	, coalesce(sum(case when channel_num in (110)
        	then coalesce(sales_dollars,0) + coalesce(return_dollars,0) else 0 end),0) AS fls_gross_sales_r
			,	coalesce(sum(case when channel_num in (110)
		     	then coalesce(sales_units,0) * coalesce(weighted_average_cost,0) else 0 end),0) AS fls_gross_sales_c
    	, coalesce(sum(case when channel_num in (110)
        	then coalesce(sales_units,0) + coalesce(return_units,0) else 0 end),0) AS fls_gross_sales_u
			, CAST(0 AS DECIMAL(12,2)) AS eoh_r
		  , CAST(0 AS DECIMAL(12,2)) AS eoh_c
		  , CAST(0 AS INTEGER) AS eoh_u
    	, CAST(0 AS DECIMAL(12,2)) AS in_transit_r
			, CAST(0 AS DECIMAL(12,2)) AS in_transit_c
    	, CAST(0 AS INTEGER) AS in_transit_u
    	, CAST(0 AS DECIMAL(12,2)) AS oo_r
    	, CAST(0 AS DECIMAL(12,2)) AS oo_c
    	, CAST(0 AS INTEGER) AS oo_u
    FROM  anniv_sku a
    INNER JOIN event_dates b
      ON a.country = b.country
      AND a.yr_num = b.yr_num
    INNER JOIN t2dl_das_ace_mfp.sku_loc_pricetype_day_vw  c
      ON a.rms_sku_num = c.sku_idnt
      AND b.day_date = c.day_dt
    INNER JOIN prd_nap_usr_vws.store_dim d
      ON c.loc_idnt = d.store_num
      AND a.country = d.store_country_code
      AND d.channel_num in (110, 120, 310, 920)
    WHERE b.day_date <= current_date - 1
    GROUP BY 1, 2, 3, 4
) WITH DATA
PRIMARY INDEX (day_date, experience, rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS
    PRIMARY INDEX ( day_date, experience, rms_sku_num )
    ,COLUMN ( rms_sku_num )
		,COLUMN ( day_date )
        ON sales;

create multiset volatile table all_kpis as (
    select
        b.day_date
        , b.day_idnt
        , b.ty_ly_ind
        , a.country
        , b.days_since_launch
        , b.yr_num
        , coalesce(experience, 'OTHER') as experience
        , a.rms_sku_num
    		, CASE WHEN ds.sku_idnt IS NOT NULL THEN 'Y' ELSE 'N' END AS dropship_ind
    		, MAX(CASE WHEN rp.rms_sku_num IS NOT NULL THEN 'Y' ELSE 'N' END) AS rp_ind
				, udig.udig
        , sum(coalesce(product_views,0)) AS product_views
        , sum(coalesce(cart_adds,0)) AS cart_adds
        , sum(coalesce(com_demand,0)) AS com_demand
        , sum(coalesce(com_ordered_units,0)) AS com_ordered_units
        , sum(coalesce(wishlist_adds,0)) AS wishlist_adds
        , sum(coalesce(fls_gross_sales_r,0)) AS fls_gross_sales_r
				, sum(coalesce(fls_gross_sales_c,0)) AS fls_gross_sales_c
        , sum(coalesce(fls_gross_sales_u,0)) AS fls_gross_sales_u
        , sum(coalesce(eoh_r,0)) AS eoh_r
        , sum(coalesce(eoh_c,0)) AS eoh_c
        , sum(coalesce(eoh_u,0)) AS eoh_u
        , sum(coalesce(in_transit_r,0)) AS in_transit_r
				, sum(coalesce(in_transit_c,0)) AS in_transit_c
        , sum(coalesce(in_transit_u,0)) AS in_transit_u
        , sum(coalesce(oo_r,0)) as oo_r
        , sum(coalesce(oo_c,0)) as oo_c
        , sum(coalesce(oo_u,0)) as oo_u
    from  anniv_sku a
    inner join event_dates b
      on a.country = b.country
      and a.yr_num = b.yr_num
			LEFT JOIN
	(
	  SELECT DISTINCT udig_item_grp_idnt || ', ' || udig_item_grp_name AS udig,
	                'US' AS country,
	                sku_idnt
	         FROM {environment_schema}.AN_UDIGS_2024
	         WHERE UDIG_COLLECTION_IDNT IN ('50011')
	) udig
	    ON a.rms_sku_num = udig.sku_idnt
	    AND a.country = udig.country

		left join rp
			    ON a.rms_sku_num = rp.rms_sku_num
					AND b.day_date = rp.day_date
		left join dropship ds
		  on a.rms_sku_num = ds.sku_idnt
		  and b.day_date = ds.snapshot_date
    left join (
        select * from digital_funnel
        union all
        select * from wishlist_adds
        union all
        select * from on_order
        union all
        select * from sales
        union all
        select * from inventory
    ) c
      on a.rms_sku_num = c.rms_sku_num
      and a.country = c.country
      and b.day_date = c.day_date
    where b.day_date <= current_date - 1
    group by 1,2,3,4,5,6,7,8,9,11
) with data primary index (day_date, experience, rms_sku_num) on commit preserve rows;

COLLECT STATISTICS
		PRIMARY INDEX ( day_date, experience, rms_sku_num )
		,COLUMN ( rms_sku_num )
		,COLUMN ( day_date )
		,COLUMN ( experience )
				ON all_kpis;

DELETE FROM {environment_schema}.ANNIVERSARY_PREVIEW;
INSERT INTO {environment_schema}.ANNIVERSARY_PREVIEW
    select
        a.*
				, th.QUANTRIX_CATEGORY
				, th.CCS_CATEGORY
				, th.CCS_SUBCATEGORY
				, th.assortment_grouping
				, th.anniversary_theme
				, th.merch_themes as merch_theme
        , b.style_group_num
        , b.style_desc
        , b.supp_color
        , b.div_num
        , b.div_desc
        , trim(b.div_num || ', ' || b.div_desc) as division
        , b.grp_num
        , b.grp_desc
        , trim(b.grp_num || ', ' || b.grp_desc) as subdivision
        , b.dept_num
        , b.dept_desc
        , trim(b.dept_num || ', ' || b.dept_desc) as department
        , b.class_num
        , b.class_desc
        , trim(b.class_num || ', ' || b.class_desc) as "class"
        , b.sbclass_num
        , b.sbclass_desc
        , trim(b.sbclass_num || ', ' || b.sbclass_desc) AS subclass
        , supp.vendor_name as supplier
				, COALESCE(sup.vendor_brand_name, b.brand_name) as brand
        , b.supp_part_num as vpn
        , b.npg_ind as is_npg
				, MAX(CASE WHEN anchor.anchor_brands_ind = 1 THEN 'Y' ELSE 'N' END) AS anchor_brands_ind
				, current_timestamp as process_tmstp

    from all_kpis a
    LEFT JOIN prd_nap_usr_vws.product_sku_dim_vw b
      ON a.rms_sku_num = b.rms_sku_num
      AND a.country = b.channel_country
		LEFT JOIN T2DL_DAS_CCS_CATEGORIES.CCS_MERCH_THEMES th
			ON b.dept_num = th.dept_idnt
			AND b.class_num = th.class_idnt
			AND b.sbclass_num = th.sbclass_idnt
		LEFT JOIN {environment_schema}.AN_STRATEGIC_BRANDS_2024 anchor
	    ON b.dept_num = anchor.dept_num
	    AND b.prmy_supp_num = anchor.supplier_idnt
		LEFT JOIN PRD_NAP_USR_VWS.VENDOR_BRAND_XREF sup
			ON b.prmy_supp_num = sup.vendor_num
			AND b.brand_name = sup.vendor_brand_name
			AND sup.association_status = 'A'
		LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM supp
    	ON supp.vendor_num = b.prmy_supp_num
		WHERE	b.sbclass_desc <> 'GWP'
		AND b.sbclass_desc not like 'TRUNK SHOW%'
		AND b.partner_relationship_type_code <> 'ECONCESSION'
		AND supp.vendor_name not like '%MKTPL'

			GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 58

			;

COLLECT STATISTICS
	PRIMARY INDEX ( day_date, experience, rms_sku_num )
	,COLUMN ( rms_sku_num )
	,COLUMN ( day_date )
	,COLUMN ( experience )
		ON {environment_schema}.ANNIVERSARY_PREVIEW;
