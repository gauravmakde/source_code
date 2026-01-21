
/******************* DATES ********************/

--STEP:1
--DROP TABLE date_filter_main;
CREATE MULTISET VOLATILE TABLE date_filter_main AS (
SELECT
	 COALESCE(main.ss_ind             ,roll.ss_ind            ) AS ss_ind
	,COALESCE(main.day_date           ,roll.day_date          ) AS day_date
	,COALESCE(main.day_idnt           ,roll.day_idnt          ) AS day_idnt
	,COALESCE(main.week_idnt          ,roll.week_idnt         ) AS week_idnt
	,COALESCE(main.week_end_day_date  ,roll.week_end_day_date ) AS week_end_day_date
	,COALESCE(main.year_idnt          ,roll.year_idnt         ) AS year_idnt
FROM
	(
	--Plan full TY
	SELECT
		CAST(1 AS INT) AS ss_ind -- 1 = PL
		,dt.day_date
		,dt.day_idnt
		,dt.week_idnt
		,dt.week_end_day_date
		,dt.fiscal_year_num AS year_idnt
	FROM prd_nap_usr_vws.day_cal_454_dim dt
	WHERE
		(dt.week_idnt > (SELECT MAX(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
		AND dt.fiscal_year_num = (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))
	UNION ALL
	--TY YTD
	SELECT
		CAST(2 AS INT) AS ss_ind -- 2 = SS TY
		,dt.day_date
		,dt.day_idnt
		,dt.week_idnt
		,dt.week_end_day_date
		,dt.fiscal_year_num AS year_idnt
	FROM prd_nap_usr_vws.day_cal_454_dim dt
	WHERE
		(dt.week_idnt <= (SELECT MAX(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
		AND dt.fiscal_year_num = (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))
	UNION ALL
	--LY YTD
	SELECT
		CAST(3 AS INT) AS ss_ind -- 3 = SS LY
		,lydt.day_date
    , dt.day_idnt - 1000 AS day_idnt
    , dt.week_idnt - 100 AS week_idnt
		,lydt.week_end_day_date
		, dt.fiscal_year_num - 1 AS year_idnt
	FROM prd_nap_usr_vws.day_cal_454_dim dt
	INNER JOIN prd_nap_usr_vws.day_cal_454_dim lydt
		ON dt.day_idnt_last_year_realigned = lydt.day_idnt
	WHERE
		(dt.week_idnt <= (SELECT MAX(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
		AND dt.fiscal_year_num = (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))
	) main
	FULL OUTER JOIN
		(
	--6-7 weeks before TY for rolling averages
		SELECT
			CAST(4 AS INT) AS ss_ind -- 4 = DS
			,dt.day_date
			,dt.day_idnt
			,dt.week_idnt
			,dt.week_end_day_date
			,dt.fiscal_year_num AS year_idnt
		FROM prd_nap_usr_vws.day_cal_454_dim dt
		WHERE
			(dt.fiscal_year_num = (SELECT MAX(fiscal_year_num) -1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
			AND dt.fiscal_week_num >= 47)
		) roll
			ON main.day_idnt = roll.day_idnt
--	where main.week_idnt between 202409 and 202413 --testing
--		or main.week_idnt between 202310 and 202314 --testing

) WITH DATA PRIMARY INDEX(day_idnt, ss_ind) ON COMMIT PRESERVE ROWS;
;

COLLECT STATS
	PRIMARY INDEX (day_idnt, ss_ind)
	,COLUMN (day_date, ss_ind             )
	,COLUMN (day_idnt                     )
	,COLUMN (week_idnt, ss_ind            )
	,COLUMN (day_idnt, week_idnt, ss_ind  )
	,COLUMN (year_idnt                    )
		ON date_filter_main
;

--STEP:2
--DROP TABLE date_format;
CREATE MULTISET VOLATILE TABLE date_format AS (
	SELECT
		ty_ly_ind
		,ss_ind
		,week_end_date
		,week_idnt
		,month_idnt
		,quarter_idnt
		,year_idnt
		,week_label
		,month_label
		,quarter_label
		,DENSE_RANK() OVER (ORDER BY week_idnt ASC) AS week_rank
		,week_idnt_aligned
		,month_idnt_aligned
		,quarter_idnt_aligned
		,year_idnt_aligned
		,week_label_aligned
		,month_label_aligned
		,quarter_label_aligned
		,month_454_label
		,true_week_num
	FROM
		(
		SELECT DISTINCT
			'TY'                                                                                                                  AS ty_ly_ind
			,df.ss_ind
			,dt.week_end_day_date                                                                                                 AS week_end_date
			,dt.week_idnt
			,dt.month_idnt
			,dt.quarter_idnt
			,dt.fiscal_year_num                                                                                                   AS year_idnt
			,TRIM(dt.fiscal_year_num) || ', ' || TRIM(dt.fiscal_month_num) || ', Wk ' || TRIM(dt.week_num_of_fiscal_month)        AS week_label
			,INITCAP(dt.month_abrv) || ', ' || TRIM(dt.fiscal_year_num)                                                           AS month_label
			,dt.quarter_label
			,dt.week_idnt                                                                                                         AS week_idnt_aligned
			,dt.month_idnt                                                                                                        AS month_idnt_aligned
			,dt.quarter_idnt                                                                                                      AS quarter_idnt_aligned
			,dt.fiscal_year_num                                                                                                   AS year_idnt_aligned
			,TRIM(dt.fiscal_year_num) || ', ' || TRIM(dt.fiscal_month_num) || ', Wk ' || TRIM(dt.week_num_of_fiscal_month)        AS week_label_aligned
			,INITCAP(dt.month_abrv) || ', ' || TRIM(dt.fiscal_year_num)                                                           AS month_label_aligned
			,dt.quarter_label                                                                                                     AS quarter_label_aligned
			,dt.month_454_label
			,dt.week_idnt as true_week_num
		FROM prd_nap_usr_vws.day_cal_454_dim dt
		INNER JOIN date_filter_main df
			ON dt.day_idnt = df.day_idnt
		WHERE
			dt.fiscal_year_num = (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
		UNION ALL
    SELECT
  		DISTINCT 'LY'
      , CAST(3 AS INT) AS ss_ind -- 3 = SS LY
  		, lydt.week_end_day_date
  		, dt.week_idnt - 100 AS week_idnt
  		, dt.month_idnt - 100 AS month_idnt
  		, CAST(TRIM(TRIM(dt.fiscal_year_num - 1) || TRIM(dt.fiscal_quarter_num)) AS INTEGER) AS quarter_idnt
  		, dt.fiscal_year_num - 1                                                                                                AS year_idnt
   		, TRIM(dt.fiscal_year_num - 1) || ', ' || TRIM(dt.fiscal_month_num) || ', Wk ' || TRIM(dt.week_num_of_fiscal_month)     AS week_label
   		, INITCAP(dt.month_abrv) || ', ' || TRIM(dt.fiscal_year_num - 1)                                                     AS month_label
  		, TRIM(TRIM(dt.fiscal_year_num-1) ||' Q'|| TRIM(dt.fiscal_quarter_num)) AS quarter_label
      , dt.week_idnt AS week_idnt_aligned
      , dt.month_idnt AS month_idnt_aligned
      , dt.quarter_idnt quarter_idnt_aligned
  		, dt.fiscal_year_num AS year_idnt_aligned
  		, TRIM(dt.fiscal_year_num) || ', ' || TRIM(dt.fiscal_month_num) || ', Wk ' || TRIM(dt.week_num_of_fiscal_month)   AS week_label_aligned
      , INITCAP(dt.month_abrv) || ', ' || TRIM(dt.fiscal_year_num) AS month_label_aligned
      , dt.quarter_label as quarter_label_aligned
  	  , CAST(NULL AS VARCHAR(10))                                                                                            AS month_454_label
	    , lydt.week_idnt as true_week_num
  	FROM prd_nap_usr_vws.day_cal_454_dim dt
  	INNER JOIN prd_nap_usr_vws.day_cal_454_dim lydt
  		ON dt.day_idnt_last_year_realigned = lydt.day_idnt
  	WHERE
  		(dt.week_idnt <= (SELECT MAX(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
  		AND dt.fiscal_year_num = (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))
		) sub
) WITH DATA PRIMARY INDEX(week_idnt, ss_ind) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_idnt, ss_ind)
	,COLUMN (week_idnt)
	,COLUMN (ty_ly_ind, week_idnt)
    ,COLUMN (ty_ly_ind ,ss_ind ,week_end_date ,week_idnt,month_idnt ,quarter_idnt ,year_idnt ,week_label ,month_label,quarter_label ,week_rank ,week_idnt_aligned ,month_idnt_aligned,
     		 quarter_idnt_aligned ,year_idnt_aligned ,week_label_aligned,month_label_aligned ,quarter_label_aligned)
		ON date_format
;
	
--STEP:3
--DROP TABLE date_filter_ty;
CREATE MULTISET VOLATILE TABLE date_filter_ty AS (
SELECT dt.*
FROM date_filter_main dt
INNER JOIN date_format df
	ON dt.week_idnt = df.week_idnt
	AND df.ty_ly_ind = 'TY'
) WITH DATA PRIMARY INDEX(day_idnt, ss_ind) ON COMMIT PRESERVE ROWS;
;

COLLECT STATS
	PRIMARY INDEX (day_idnt, ss_ind)
	,COLUMN (day_date, ss_ind             )
	,COLUMN (day_idnt                     )
	,COLUMN (week_idnt, ss_ind            )
	,COLUMN (day_idnt, week_idnt, ss_ind  )
	,COLUMN (year_idnt                    )
		ON date_filter_ty
;

--STEP:4
--DROP TABLE date_filter_ly;
CREATE MULTISET VOLATILE TABLE date_filter_ly AS (
SELECT dt.*
FROM date_filter_main dt
INNER JOIN date_format df
	ON dt.week_idnt = df.week_idnt
	AND df.ty_ly_ind = 'LY'
) WITH DATA PRIMARY INDEX(day_idnt, ss_ind) ON COMMIT PRESERVE ROWS;
;

COLLECT STATS
	PRIMARY INDEX (day_idnt, ss_ind)
	,COLUMN (day_date, ss_ind             )
	,COLUMN (day_idnt                     )
	,COLUMN (week_idnt, ss_ind            )
	,COLUMN (day_idnt, week_idnt, ss_ind  )
	,COLUMN (year_idnt                    )
		ON date_filter_ly
;

/******************* HIERARCHY AND THEMES ********************/
	
--STEP:5
--AOR Lookup
--DROP TABLE aor;
CREATE MULTISET VOLATILE TABLE aor AS (
SELECT DISTINCT
	CASE 
		WHEN channel_brand = 'NORDSTROM' THEN 'N'
		WHEN channel_brand = 'NORDSTROM_RACK' THEN 'NR'
		END AS banner
	,dept_num
	,general_merch_manager_executive_vice_president
	,div_merch_manager_senior_vice_president
	,div_merch_manager_vice_president
	,merch_director
	,buyer
	,merch_planning_executive_vice_president
	,merch_planning_senior_vice_president
	,merch_planning_vice_president
	,merch_planning_director_manager
	,assortment_planner       	
FROM prd_nap_usr_vws.area_of_responsibility_dim
QUALIFY ROW_NUMBER() OVER (PARTITION BY banner, dept_num ORDER BY 3,4,5,6,7,8,9,10,11,12) = 1
) WITH DATA
	PRIMARY INDEX (banner,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (banner,dept_num) 
		ON aor	
;


--STEP:6
--ANCHOR BRANDS Lookup
--DROP TABLE anc;
CREATE MULTISET VOLATILE TABLE anc AS (
SELECT
	dept_num
	,supplier_idnt
FROM t2dl_das_in_season_management_reporting.anchor_brands
WHERE anchor_brand_ind = 'Y'
GROUP BY 1,2
) WITH DATA
	PRIMARY INDEX (dept_num,supplier_idnt) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (dept_num,supplier_idnt) 
		ON anc	
;


--STEP:7
--RSB Lookup
--DROP TABLE rsb;
CREATE MULTISET VOLATILE TABLE rsb AS (
SELECT
	dept_num
	,supplier_idnt
FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
WHERE rack_strategic_brand_ind = 'Y'
GROUP BY 1,2
) WITH DATA
	PRIMARY INDEX (dept_num,supplier_idnt) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (dept_num,supplier_idnt) 
		ON rsb	
;

--STEP:8
--Base hierarchy table, removing GWP and Samples
-- DROP TABLE hier_base;
CREATE MULTISET VOLATILE TABLE hier_base AS (
	SELECT
		 sku.rms_sku_num AS sku_idnt
		,sku.rms_style_num
		,sku.color_num
		,sku.channel_country
		,TRIM(sku.div_num) AS div_idnt
		,TRIM(sku.div_num) || ', ' || sku.div_desc AS div_label
		,TRIM(sku.grp_num) AS sdiv_nmbr
		,TRIM(sku.grp_num) || ', ' || sku.grp_desc AS sdiv_label
		,TRIM(sku.dept_num) AS dept_idnt
		,TRIM(sku.dept_num) || ', ' || sku.dept_desc AS dept_label
		,TRIM(sku.class_num) AS class_idnt
		,TRIM(sku.class_num) || ', ' || sku.class_desc AS class_label
		,TRIM(sku.sbclass_num) AS sbclass_idnt
		,TRIM(sku.sbclass_num) || ', ' || sku.sbclass_desc AS sbclass_label
		,sku.prmy_supp_num AS supp_idnt
		,sku.brand_name
	FROM prd_nap_usr_vws.product_sku_dim_vw sku
	WHERE
		sku.smart_sample_ind <> 'Y'
		AND sku.gwp_ind <> 'Y'
) WITH DATA PRIMARY INDEX(sku_idnt, supp_idnt, brand_name) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (sku_idnt, supp_idnt, brand_name)
	,COLUMN (sku_idnt)
	,COLUMN (supp_idnt)
	,COLUMN (brand_name)
		ON hier_base
;

--STEP:9
--Hierarchy with Supplier Name, NPG, and Diversity Indicator
--DROP TABLE hier_base2;
CREATE MULTISET VOLATILE TABLE hier_base2 AS (
	SELECT
		 sku.sku_idnt
		,sku.rms_style_num
		,sku.color_num
		,sku.channel_country
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sup.vendor_name AS supp_name
		,sku.brand_name
		,COALESCE(CASE WHEN sup.npg_flag = 'Y' THEN 1 ELSE 0 END,0) AS npg_ind
        ,CASE WHEN bi.brand_name IS NOT NULL THEN 1 ELSE 0 END AS diverse_brand_ind
	FROM hier_base sku
	-- supplier
	LEFT JOIN prd_nap_usr_vws.vendor_dim sup
		ON sku.supp_idnt = sup.vendor_num
    -- diverse brand ind
    LEFT JOIN
    	(
		SELECT DISTINCT
			 brand_name
		FROM t2dl_das_in_season_management_reporting.diverse_brands
		) bi
			ON sku.brand_name = bi.brand_name
) WITH DATA PRIMARY INDEX(sku_idnt, dept_idnt, class_idnt, sbclass_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	 PRIMARY INDEX (sku_idnt, dept_idnt, class_idnt, sbclass_idnt)
	,COLUMN (sku_idnt)
		ON hier_base2
;

--STEP:10
--Final hierarchy with Merch Themes and Categories
--DROP TABLE hier;
CREATE MULTISET VOLATILE TABLE hier AS (
	SELECT
		sku.sku_idnt
		,sku.rms_style_num
		,sku.color_num
		,sku.channel_country
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_name
		,sku.supp_idnt
		,sku.brand_name
		,sku.npg_ind
        ,sku.diverse_brand_ind
		,thm.quantrix_category
		,thm.ccs_category
		,thm.ccs_subcategory
		,thm.nord_role_desc
		,thm.rack_role_desc
		,thm.parent_group
		,thm.merch_themes
		,thm.assortment_grouping
		,thm.holiday_theme_ty
		,CASE WHEN thm.gift_ind = 'Y' THEN 'GIFT'
			WHEN thm.gift_ind IN ('N','NULL') THEN 'NON-GIFT'
			ELSE COALESCE(thm.gift_ind,'NON-GIFT') END AS gift_ind
		,CASE WHEN thm.stocking_stuffer = 'Y' THEN 'Y' ELSE 'N' END AS stocking_stuffer
	FROM hier_base2 sku
	--categories
	-- TODO: change to use PRD_NAP_USR_VWS.CATG_SUBCLASS_MAP_DIM? Doesn't have themes or other categories
	LEFT JOIN t2dl_das_ccs_categories.ccs_merch_themes thm
		ON sku.dept_idnt = thm.dept_idnt
		AND sku.class_idnt = thm.class_idnt
		AND sku.sbclass_idnt = thm.sbclass_idnt
) WITH DATA
	PRIMARY INDEX(sku_idnt, channel_country)
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (sku_idnt, channel_country)
	,COLUMN (sku_idnt)
	,COLUMN (rms_style_num, color_num)
	,COLUMN (div_idnt, div_label, sdiv_nmbr, sdiv_label, dept_idnt, dept_label, class_idnt, class_label, sbclass_idnt, sbclass_label, supp_name, supp_idnt, brand_name, npg_ind,
			diverse_brand_ind, quantrix_category, ccs_category, ccs_subcategory, nord_role_desc, rack_role_desc, parent_group,
			merch_themes, assortment_grouping, holiday_theme_ty, gift_ind, stocking_stuffer)
		ON hier
;


/******************* LOCATIONS ********************/

--STEP:11
--temp table filters to all active selling and RS locations
--DROP TABLE locs;
CREATE MULTISET VOLATILE TABLE locs AS (
SELECT DISTINCT
	loc.store_num AS loc_idnt
	,TRIM(loc.channel_num || ', ' ||loc.channel_desc) AS chnl_label
    ,loc.channel_num AS chnl_idnt
    ,loc.channel_country AS country
    ,loc.channel_brand
    ,CASE WHEN loc.channel_brand = 'NORDSTROM' THEN 'N'
    	WHEN loc.channel_brand = 'NORDSTROM_RACK' THEN 'NR'
    	ELSE NULL END AS banner
FROM prd_nap_usr_vws.price_store_dim_vw loc
WHERE loc.channel_num IN (110,111,120,121,130,140,210,211,220,221,250,260,261,310,311) --doesn't include DCs, LC, NQC
	AND loc.store_abbrev_name <> 'CLOSED'
) WITH DATA PRIMARY INDEX(loc_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (loc_idnt)
	,COLUMN (channel_brand)
	,COLUMN (loc_idnt, chnl_idnt)
	,COLUMN (chnl_label, chnl_idnt, country, banner)
		ON locs
;


/******************* DROPSHIP ********************/

--STEP:12
--DROP TABLE dropship;
CREATE MULTISET VOLATILE TABLE dropship AS (
SELECT DISTINCT
	dt.day_date
    ,ds.rms_sku_id AS sku_idnt
FROM prd_nap_usr_vws.inventory_stock_quantity_by_day_fact ds
INNER JOIN date_filter_main dt
	ON ds.snapshot_date = dt.day_date
WHERE ds.location_type IN ('DS', 'DS_OP')
) WITH DATA PRIMARY INDEX(sku_idnt, day_date)
PARTITION BY RANGE_N(day_date BETWEEN '2019-01-01' AND '2035-12-31')
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (sku_idnt, day_date)
	,COLUMN(PARTITION)
		ON dropship
;


/******************* PLAN DIMENSIONS ********************/

--STEP:13
--DROP TABLE dept;
CREATE MULTISET VOLATILE TABLE dept AS (
	SELECT DISTINCT
		dept_idnt
		,dept_label
		,div_idnt
		,div_label
		,sdiv_nmbr
		,sdiv_label
	FROM hier
) WITH DATA PRIMARY INDEX(dept_idnt) ON COMMIT PRESERVE ROWS;
;

COLLECT STATS
	PRIMARY INDEX (dept_idnt)
		ON dept
;

--STEP:14
--DROP TABLE chan;
CREATE MULTISET VOLATILE TABLE chan AS (
	SELECT DISTINCT
		chnl_idnt
		,chnl_label
		,country
		,banner
		,channel_brand
	FROM locs
) WITH DATA PRIMARY INDEX(chnl_idnt) ON COMMIT PRESERVE ROWS;
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt)
		ON chan
;

--STEP:15
--temp table attributing store channels to banner country for inventory channels
--DROP TABLE chan_inv;
CREATE MULTISET VOLATILE TABLE chan_inv AS (
SELECT DISTINCT
	locs.chnl_label
    ,locs.chnl_idnt
    ,locs.country
	,locs.banner
	,CASE
		WHEN locs.banner = 'N'  AND locs.country = 'US' THEN 1
		WHEN locs.banner = 'N'  AND locs.country = 'CA' THEN 2
		WHEN locs.banner = 'NR' AND locs.country = 'US' THEN 3
		WHEN locs.banner = 'NR' AND locs.country = 'CA' THEN 4
		END AS banner_country_num
FROM locs
WHERE locs.chnl_idnt IN ('110', '111', '210', '211')
) WITH DATA PRIMARY INDEX(banner_country_num) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (banner_country_num)
	,COLUMN (banner_country_num)
		ON chan_inv
;

--STEP:16
--DROP TABLE sup_group;
CREATE MULTISET VOLATILE TABLE sup_group AS (
	SELECT DISTINCT
		supplier_num
		,supplier_group
		,dept_num
		,CASE WHEN banner = 'FP' THEN 'N'
			WHEN banner = 'OP' THEN 'NR'
			ELSE banner END AS banner
	FROM prd_nap_usr_vws.supp_dept_map_dim
) WITH DATA
	PRIMARY INDEX(supplier_num, dept_num, banner)
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (supplier_num, dept_num, banner)
	,COLUMN (supplier_group)
		ON sup_group
;

/******************* PFD RP ********************/

--STEP:17
--Channel level RP, only used for pfd
--DROP TABLE pfd_rp;
CREATE MULTISET VOLATILE TABLE pfd_rp
AS (
    SELECT DISTINCT
        base.rms_sku_num
        ,base.rp_period
        ,locs.chnl_idnt
    FROM prd_nap_usr_vws.merch_rp_sku_loc_dim_hist base
    INNER JOIN locs
        ON base.location_num = locs.loc_idnt
        AND locs.chnl_idnt IN (120, 121, 250)
    INNER JOIN (select distinct day_date from date_filter_main where ss_ind <> 1) dt
        ON base.rp_period CONTAINS dt.day_date
)
WITH DATA
	PRIMARY INDEX (rms_sku_num, chnl_idnt)
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (rms_sku_num, chnl_idnt)
	,COLUMN (rms_sku_num, chnl_idnt)
		ON pfd_rp
;

/******************* PFD PRE ********************/

--STEP:18
--DROP TABLE pfd_pre_ty;
CREATE VOLATILE TABLE pfd_pre_ty
AS (
    SELECT
        base.rms_sku_num
        ,base.event_date_pacific
        ,dt.day_idnt
        ,dt.week_idnt
        ,base.current_price_type
        ,CASE WHEN base.channel = 'FULL_LINE' AND base.channelcountry = 'US' THEN '120, N.COM'
            WHEN base.channel = 'FULL_LINE' AND base.channelcountry = 'CA' THEN '121, N.CA'
            WHEN base.channel = 'RACK' THEN '250, OFFPRICE ONLINE'
            END AS chnl_label
        ,CASE WHEN base.channel = 'FULL_LINE' AND base.channelcountry = 'US' THEN 120
            WHEN base.channel = 'FULL_LINE' AND base.channelcountry = 'CA' THEN 121
            WHEN base.channel = 'RACK' THEN 250
            END AS chnl_idnt
        ,base.channelcountry AS country
        ,CASE WHEN base.channel = 'FULL_LINE' THEN 'N'
            WHEN base.channel = 'RACK' THEN 'NR'
            END AS banner
        ,CASE WHEN base.channel = 'FULL_LINE' THEN 'NORDSTROM'
            WHEN base.channel = 'RACK' THEN 'NORDSTROM_RACK'
            END AS channel_brand
        ,product_views
        ,cart_adds
        ,web_order_units
    FROM (
			SELECT
				base.rms_sku_num
				,base.event_date_pacific
				,base.current_price_type
				,base.channel
				,base.channelcountry
				,SUM(base.product_views           ) AS product_views
				,SUM(base.add_to_bag_quantity     ) AS cart_adds
				,SUM(base.order_quantity          ) AS web_order_units
			FROM t2dl_das_product_funnel.product_price_funnel_daily base
			WHERE channel IN ('FULL_LINE','RACK') AND event_date_pacific >= (select min(day_date) from date_filter_ty)
			GROUP BY base.rms_sku_num, base.event_date_pacific, base.current_price_type, base.channel, base.channelcountry
		) base
    --dates
    INNER JOIN date_filter_ty dt
        ON base.event_date_pacific = dt.day_date
    	AND dt.ss_ind <> 1
)
WITH DATA
PRIMARY INDEX (rms_sku_num, day_idnt, chnl_idnt)
PARTITION BY Range_N(event_date_pacific BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY )
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	 PRIMARY INDEX (rms_sku_num, day_idnt, chnl_idnt)
    ,COLUMN (PARTITION)
    ,COLUMN (PARTITION, rms_sku_num)
    ,COLUMN (PARTITION, rms_sku_num, country)
	,COLUMN (rms_sku_num)
	,COLUMN (channel_brand)
	,COLUMN (event_date_pacific)
	,COLUMN (rms_sku_num, week_idnt, chnl_idnt)
	,COLUMN (rms_sku_num, day_idnt)
	,COLUMN (day_idnt)
	,COLUMN (PARTITION, rms_sku_num, day_idnt, chnl_idnt)
		ON pfd_pre_ty
;

--STEP:19
--DROP TABLE pfd_pre_ly;
CREATE VOLATILE TABLE pfd_pre_ly
AS (
    SELECT
        base.rms_sku_num
        ,base.event_date_pacific
        ,dt.day_idnt
        ,dt.week_idnt
        ,base.current_price_type
        ,CASE WHEN base.channel = 'FULL_LINE' AND base.channelcountry = 'US' THEN '120, N.COM'
            WHEN base.channel = 'FULL_LINE' AND base.channelcountry = 'CA' THEN '121, N.CA'
            WHEN base.channel = 'RACK' THEN '250, OFFPRICE ONLINE'
            END AS chnl_label
        ,CASE WHEN base.channel = 'FULL_LINE' AND base.channelcountry = 'US' THEN 120
            WHEN base.channel = 'FULL_LINE' AND base.channelcountry = 'CA' THEN 121
            WHEN base.channel = 'RACK' THEN 250
            END AS chnl_idnt
        ,base.channelcountry AS country
        ,CASE WHEN base.channel = 'FULL_LINE' THEN 'N'
            WHEN base.channel = 'RACK' THEN 'NR'
            END AS banner
        ,CASE WHEN base.channel = 'FULL_LINE' THEN 'NORDSTROM'
            WHEN base.channel = 'RACK' THEN 'NORDSTROM_RACK'
            END AS channel_brand
        ,product_views
        ,cart_adds
        ,web_order_units
    FROM (
			SELECT
				base.rms_sku_num
				,base.event_date_pacific
				,base.current_price_type
				,base.channel
				,base.channelcountry
				,SUM(base.product_views           ) AS product_views
				,SUM(base.add_to_bag_quantity     ) AS cart_adds
				,SUM(base.order_quantity          ) AS web_order_units
			FROM t2dl_das_product_funnel.product_price_funnel_daily base
			WHERE base.channel IN ('FULL_LINE','RACK') AND event_date_pacific >= (select min(day_date) from date_filter_ly)
			GROUP BY base.rms_sku_num, base.event_date_pacific, base.current_price_type, base.channel, base.channelcountry
		) base
    --dates
    INNER JOIN date_filter_ly dt
        ON base.event_date_pacific = dt.day_date
    	AND dt.ss_ind <> 1
)
WITH DATA
PRIMARY INDEX (rms_sku_num, day_idnt, chnl_idnt)
PARTITION BY Range_N(event_date_pacific BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY )
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	 PRIMARY INDEX (rms_sku_num, day_idnt, chnl_idnt)
    ,COLUMN (PARTITION)
    ,COLUMN (PARTITION, rms_sku_num)
    ,COLUMN (PARTITION, rms_sku_num, country)
	,COLUMN (rms_sku_num)
	,COLUMN (channel_brand)
	,COLUMN (event_date_pacific)
	,COLUMN (rms_sku_num, week_idnt, chnl_idnt)
	,COLUMN (rms_sku_num, day_idnt)
	,COLUMN (day_idnt)
	,COLUMN (PARTITION, rms_sku_num, day_idnt, chnl_idnt)
		ON pfd_pre_ly
;

/******************* PFD ********************/

--STEP:20
--DROP TABLE pfd_staging_ty;
CREATE MULTISET VOLATILE TABLE pfd_staging_ty AS (
	SELECT
		 base.week_idnt
		,base.day_idnt
		,base.event_date_pacific
		,base.chnl_label
		,base.chnl_idnt
		,base.country
		,base.banner
		,base.rms_sku_num
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
		,sku.ccs_category
		,sku.ccs_subcategory
		,sku.nord_role_desc
		,sku.rack_role_desc
		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
	    ,COALESCE(base.current_price_type,'NA') AS price_type
		,COALESCE(sku.npg_ind,0) AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
		,CASE WHEN rp.rms_sku_num IS NOT NULL THEN 1 ELSE 0 END AS rp_ind
		,base.product_views
		,base.cart_adds
		,base.web_order_units
	FROM pfd_pre_ty base
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND base.country = sku.channel_country
	--supplier group
	LEFT JOIN sup_group spg
		ON base.banner = spg.banner
		AND sku.supp_idnt = spg.supplier_num
		AND sku.dept_idnt = spg.dept_num
	--RP
	LEFT JOIN pfd_rp rp
		ON base.rms_sku_num = rp.rms_sku_num
		AND base.chnl_idnt = rp.chnl_idnt
        AND rp.rp_period CONTAINS base.event_date_pacific
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND base.channel_brand = COALESCE(iti.channel_brand, 'NONE')
) WITH DATA
	PRIMARY INDEX(rms_sku_num, event_date_pacific)
	PARTITION BY RANGE_N(event_date_pacific BETWEEN DATE '2012-01-01' AND DATE '2035-12-31' EACH INTERVAL '1' DAY )
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (rms_sku_num, event_date_pacific)
    ,COLUMN (PARTITION)
	,COLUMN (day_idnt)
	,COLUMN (week_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
	,COLUMN (week_idnt ,chnl_label ,chnl_idnt, country ,banner ,div_idnt ,div_label ,sdiv_nmbr ,sdiv_label,
             dept_idnt ,dept_label ,class_idnt ,class_label ,sbclass_idnt,sbclass_label ,supp_idnt ,supp_name, brand_name, quantrix_category,
             ccs_category ,ccs_subcategory ,nord_role_desc ,rack_role_desc,parent_group ,merch_themes, supplier_group ,price_type ,npg_ind,
             diverse_brand_ind ,rp_ind)
		ON pfd_staging_ty
;

--STEP:21
--DROP TABLE pfd_staging_ly;
CREATE MULTISET VOLATILE TABLE pfd_staging_ly AS (
	SELECT
		 base.week_idnt
		,base.day_idnt
		,base.event_date_pacific
		,base.chnl_label
		,base.chnl_idnt
		,base.country
		,base.banner
		,base.rms_sku_num
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
		,sku.ccs_category
		,sku.ccs_subcategory
		,sku.nord_role_desc
		,sku.rack_role_desc
		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
	    ,COALESCE(base.current_price_type,'NA') AS price_type
		,COALESCE(sku.npg_ind,0) AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
		,CASE WHEN rp.rms_sku_num IS NOT NULL THEN 1 ELSE 0 END AS rp_ind
		,base.product_views
		,base.cart_adds
		,base.web_order_units
	FROM pfd_pre_ly base
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND base.country = sku.channel_country
	--supplier group
	LEFT JOIN sup_group spg
		ON base.banner = spg.banner
		AND sku.supp_idnt = spg.supplier_num
		AND sku.dept_idnt = spg.dept_num
	--RP
	LEFT JOIN pfd_rp rp
		ON base.rms_sku_num = rp.rms_sku_num
		AND base.chnl_idnt = rp.chnl_idnt
        AND rp.rp_period CONTAINS base.event_date_pacific
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND base.channel_brand = COALESCE(iti.channel_brand, 'NONE')
) WITH DATA
	PRIMARY INDEX(rms_sku_num, event_date_pacific)
	PARTITION BY Range_N(event_date_pacific BETWEEN DATE '2012-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY )
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (rms_sku_num, event_date_pacific)
    ,COLUMN (PARTITION)
	,COLUMN (day_idnt)
	,COLUMN (week_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
	,COLUMN (week_idnt ,chnl_label ,chnl_idnt, country ,banner ,div_idnt ,div_label ,sdiv_nmbr ,sdiv_label,
             dept_idnt ,dept_label ,class_idnt ,class_label ,sbclass_idnt,sbclass_label , supp_idnt ,supp_name ,brand_name, quantrix_category,
             ccs_category ,ccs_subcategory ,nord_role_desc ,rack_role_desc,parent_group ,merch_themes, supplier_group ,price_type ,npg_ind,
             diverse_brand_ind ,rp_ind)
		ON pfd_staging_ly
;

--STEP:22
--DROP TABLE pfd_ty;
CREATE MULTISET VOLATILE TABLE pfd_ty AS (
	SELECT
		 base.week_idnt
		,base.chnl_label
		,CAST(base.chnl_idnt AS INTEGER) AS chnl_idnt
--		,base.country
		,base.banner
		,base.div_idnt
		,base.div_label
		,base.sdiv_nmbr
		,base.sdiv_label
		,base.dept_idnt
		,base.dept_label
		,base.class_idnt
		,base.class_label
		,base.sbclass_idnt
		,base.sbclass_label
		,base.supp_idnt
		,base.supp_name
		,base.brand_name
		,base.quantrix_category
--		,base.ccs_category
--		,base.ccs_subcategory
--		,base.nord_role_desc
--		,base.rack_role_desc
--		,base.parent_group
		,base.merch_themes
		,base.assortment_grouping
		,base.holiday_theme_ty
		,base.gift_ind
		,base.stocking_stuffer
		,base.supplier_group
		,base.intended_lifecycle_type
		,base.intended_season
		,base.intended_exit_month_year
		,base.scaled_event
		,base.holiday_or_celebration
	    ,base.price_type
		,base.npg_ind
		,base.diverse_brand_ind
	    ,CASE WHEN ds.sku_idnt IS NOT NULL THEN 1 ELSE 0 END AS dropship_ind
	    ,base.rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
	    ,SUM(base.product_views     ) AS product_views
	    ,SUM(base.cart_adds         ) AS cart_adds
	    ,SUM(base.web_order_units   ) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM pfd_staging_ty base
	--dropship indicator
	LEFT JOIN dropship ds
		ON base.rms_sku_num = ds.sku_idnt
		AND base.event_date_pacific = ds.day_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
	,COLUMN (week_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
		ON pfd_ty
;

--STEP:23
--DROP TABLE pfd_ly;
CREATE MULTISET VOLATILE TABLE pfd_ly AS (
	SELECT
		 base.week_idnt
		,base.chnl_label
		,CAST(base.chnl_idnt AS INTEGER) AS chnl_idnt
--		,base.country
		,base.banner
		,base.div_idnt
		,base.div_label
		,base.sdiv_nmbr
		,base.sdiv_label
		,base.dept_idnt
		,base.dept_label
		,base.class_idnt
		,base.class_label
		,base.sbclass_idnt
		,base.sbclass_label
		,base.supp_idnt
		,base.supp_name
		,base.brand_name
		,base.quantrix_category
--		,base.ccs_category
--		,base.ccs_subcategory
--		,base.nord_role_desc
--		,base.rack_role_desc
--		,base.parent_group
		,base.merch_themes
		,base.assortment_grouping
		,base.holiday_theme_ty
		,base.gift_ind
		,base.stocking_stuffer
		,base.supplier_group
		,base.intended_lifecycle_type
		,base.intended_season
		,base.intended_exit_month_year
		,base.scaled_event
		,base.holiday_or_celebration
	    ,base.price_type
		,base.npg_ind
		,base.diverse_brand_ind
	    ,CASE WHEN ds.sku_idnt IS NOT NULL THEN 1 ELSE 0 END AS dropship_ind
	    ,base.rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
	    ,SUM(base.product_views     ) AS product_views
	    ,SUM(base.cart_adds         ) AS cart_adds
	    ,SUM(base.web_order_units   ) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM pfd_staging_ly base
	--dropship indicator
	LEFT JOIN dropship ds
		ON base.rms_sku_num = ds.sku_idnt
		AND base.event_date_pacific = ds.day_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
	,COLUMN (week_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
		ON pfd_ly;

/******************* MERCH ********************/

--STEP:24
--DROP TABLE merch;
CREATE MULTISET VOLATILE TABLE merch AS (
SELECT
   dt.week_idnt
	,base.chnl_label
	,CAST(base.chnl_idnt AS INTEGER) AS chnl_idnt
--	,base.country
	,CASE WHEN base.banner = 'FP' THEN 'N'
		WHEN base.banner = 'OP' THEN 'NR'
		ELSE base.banner END AS banner
	,sku.div_idnt
	,sku.div_label
	,sku.sdiv_nmbr
	,sku.sdiv_label
	,sku.dept_idnt
	,sku.dept_label
	,sku.class_idnt
	,sku.class_label
	,sku.sbclass_idnt
	,sku.sbclass_label
	,sku.supp_idnt
	,sku.supp_name
	,sku.brand_name
	,sku.quantrix_category
--	,sku.ccs_category
--	,sku.ccs_subcategory
--	,sku.nord_role_desc
--	,sku.rack_role_desc
--	,sku.parent_group
	,sku.merch_themes
	,sku.assortment_grouping
	,sku.holiday_theme_ty
	,sku.gift_ind
	,sku.stocking_stuffer
	,spg.supplier_group
	,iti.intended_lifecycle_type
	,iti.intended_season
	,iti.intended_exit_month_year
	,iti.scaled_event
	,iti.holiday_or_celebration
	,base.price_type
	,COALESCE(sku.npg_ind,0) AS npg_ind
	,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
	,base.dropship_ind
	,base.rp_ind
	,SUM(base.sales_units                     ) AS sales_units
	,SUM(base.sales_dollars                   ) AS sales_dollars
	,SUM(base.return_units                    ) AS return_units
	,SUM(base.return_dollars                  ) AS return_dollars
	,SUM(base.demand_units                    ) AS demand_units
	,SUM(base.demand_dollars                  ) AS demand_dollars
	,SUM(base.demand_cancel_units             ) AS demand_cancel_units
	,SUM(base.demand_cancel_dollars           ) AS demand_cancel_dollars
	,SUM(base.demand_dropship_units           ) AS demand_dropship_units
	,SUM(base.demand_dropship_dollars         ) AS demand_dropship_dollars
	,SUM(base.shipped_units                   ) AS shipped_units
	,SUM(base.shipped_dollars                 ) AS shipped_dollars
	,SUM(base.store_fulfill_units             ) AS store_fulfill_units
	,SUM(base.store_fulfill_dollars           ) AS store_fulfill_dollars
	,CAST(0 AS DECIMAL(10,0)) AS receipt_units
	,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
	,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
	,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
	,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
	,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
	,CAST(0 AS DECIMAL(10,0)) AS rtv_units
	,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
	,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
	,SUM(base.eoh_units) AS eoh_units
	,SUM(base.eoh_dollars) AS eoh_dollars
	,SUM(base.eoh_cost) AS eoh_cost
	,SUM(base.nonsellable_units) AS nonsellable_units
	,CAST(0 AS DECIMAL(12,2)) AS product_views
	,CAST(0 AS DECIMAL(12,2)) AS cart_adds
	,CAST(0 AS DECIMAL(12,2)) AS web_order_units
	,CAST(0 AS DECIMAL(10,0)) AS on_order_units
	,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
	,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
	,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
	,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
	,CAST(0 AS DECIMAL(10,0)) AS backorder_units
	,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
	,SUM(base.cost_of_goods_sold   ) AS cost_of_goods_sold
	,SUM(base.product_margin       ) AS product_margin
	,SUM(base.sales_pm             ) AS sales_pm
	,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
	,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
	,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
	,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
	,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
	,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
	,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
	,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
	,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
	,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
	,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
	,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
	,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
	,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
	,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
	,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
	,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
	,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
	,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
	,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
	,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
	,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
FROM t2dl_das_in_season_management_reporting.wbr_merch_staging base
JOIN date_format dt
  ON base.week_idnt = dt.true_week_num --week_idnt
  AND dt.ss_ind <> 1
--hierarchy and themes
INNER JOIN hier sku
   ON base.sku_idnt = sku.sku_idnt
   AND base.country = sku.channel_country
--supplier group
LEFT JOIN sup_group spg
	ON CASE WHEN base.banner = 'FP' THEN 'N'
		WHEN base.banner = 'OP' THEN 'NR'
		ELSE base.banner END
			= spg.banner
	AND sku.supp_idnt = spg.supplier_num
	AND sku.dept_idnt = spg.dept_num
--channel brand
LEFT JOIN chan
	ON CAST(base.chnl_idnt AS INTEGER) = chan.chnl_idnt
--ITEM INTENT
LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
	ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
	AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
	AND chan.channel_brand = COALESCE(iti.channel_brand, 'NONE')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
	,COLUMN (week_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
	,COLUMN (chnl_label ,chnl_idnt ,banner ,div_idnt ,div_label ,sdiv_nmbr ,sdiv_label ,dept_idnt,
     		 dept_label ,class_idnt ,class_label ,sbclass_idnt ,sbclass_label,supp_name ,
      	     brand_name ,quantrix_category ,
     	     merch_themes ,price_type ,npg_ind ,diverse_brand_ind,dropship_ind ,rp_ind)
		ON merch
;

/******************* RECEIPTS ********************/

--STEP:25
--DROP TABLE receipts_stg;
CREATE MULTISET VOLATILE TABLE receipts_stg AS (
	SELECT
		dt.week_idnt as week_num
		,base.rms_sku_num
		,base.store_num
		,base.npg_ind
	    ,base.dropship_ind
        ,base.rp_ind
        ,base.supplier_num
        ,base.department_num
        ,locs.country
        ,base.receipts_regular_units
        ,base.receipts_crossdock_regular_units
        ,base.receipts_regular_retail
        ,base.receipts_crossdock_regular_retail
        ,base.receipts_regular_cost
        ,base.receipts_crossdock_regular_cost
        ,base.receipts_clearance_units
        ,base.receipts_crossdock_clearance_units
        ,base.receipts_clearance_retail
        ,base.receipts_crossdock_clearance_retail
        ,base.receipts_clearance_cost
        ,base.receipts_crossdock_clearance_cost
	FROM prd_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw base
	--dates
	INNER JOIN date_format dt
	    ON base.week_num = dt.true_week_num -- week_idnt
	    AND dt.ss_ind <> 1
	--locations
	INNER JOIN locs
		ON base.store_num = locs.loc_idnt
	WHERE base.receipts_regular_units > 0
		OR base.receipts_clearance_units > 0
		OR base.receipts_crossdock_regular_units > 0
		OR base.receipts_crossdock_clearance_units > 0
) WITH DATA PRIMARY INDEX(rms_sku_num, store_num, dropship_ind, supplier_num, department_num, country)
PARTITION BY RANGE_N(week_num BETWEEN 201900 AND 203552)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (rms_sku_num, store_num, dropship_ind, supplier_num, department_num, country)
	,COLUMN (rms_sku_num, country)
	,COLUMN (dropship_ind)
	,COLUMN (supplier_num, department_num)
	,COLUMN (PARTITION)
		ON receipts_stg
;

--STEP:26
--DROP TABLE transfer_stg;
CREATE MULTISET VOLATILE TABLE transfer_stg AS (
	SELECT
		dt.week_idnt as week_num
		,base.rms_sku_num
		,base.store_num
		,base.npg_ind
        ,base.rp_ind
        ,base.supplier_num
        ,base.department_num
        ,locs.country
		,base.reservestock_transfer_in_regular_units
		,base.reservestock_transfer_in_clearance_units
		,base.reservestock_transfer_in_regular_retail
		,base.reservestock_transfer_in_clearance_retail
		,base.reservestock_transfer_in_regular_cost
		,base.reservestock_transfer_in_clearance_cost
	FROM prd_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw base
	--dates
	INNER JOIN date_format dt
	    ON base.week_num = dt.true_week_num -- week_idnt
	    AND dt.ss_ind <> 1
	--locations
	INNER JOIN locs
		ON base.store_num = locs.loc_idnt
	WHERE base.reservestock_transfer_in_regular_units > 0
		OR base.reservestock_transfer_in_clearance_units > 0
) WITH DATA PRIMARY INDEX(rms_sku_num, store_num, supplier_num, department_num, country)
PARTITION BY RANGE_N(week_num BETWEEN 201900 AND 203552)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (rms_sku_num, store_num, supplier_num, department_num, country)
	,COLUMN (rms_sku_num, country)
	,COLUMN (supplier_num, department_num)
	,COLUMN (PARTITION)
		ON transfer_stg
;

--STEP:27
--DROP TABLE receipts;
CREATE MULTISET VOLATILE TABLE receipts AS (
	SELECT
		base.week_num AS week_idnt
		,locs.chnl_label
		,locs.chnl_idnt
--		,locs.country
		,locs.banner
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
--		,sku.ccs_category
--		,sku.ccs_subcategory
--		,sku.nord_role_desc
--		,sku.rack_role_desc
--		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
		,CAST('R' AS VARCHAR(2)) AS price_type
        ,CASE WHEN base.npg_ind = 'Y' THEN 1 ELSE 0 END AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
        ,CASE WHEN base.dropship_ind = 'Y' THEN 1 ELSE 0 END AS dropship_ind
        ,CASE WHEN base.rp_ind = 'Y' THEN 1 ELSE 0 END AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
        ,SUM(receipts_regular_units + receipts_crossdock_regular_units) AS receipt_units
        ,SUM(receipts_regular_retail + receipts_crossdock_regular_retail) AS receipt_dollars
        ,SUM(receipts_regular_cost + receipts_crossdock_regular_cost) AS receipt_cost
        ,SUM(CASE WHEN base.dropship_ind ='Y' THEN receipts_regular_units + receipts_crossdock_regular_units ELSE 0 END) AS receipt_dropship_units
        ,SUM(CASE WHEN base.dropship_ind ='Y' THEN receipts_regular_retail + receipts_crossdock_regular_retail ELSE 0 END) AS receipt_dropship_dollars
        ,SUM(CASE WHEN base.dropship_ind ='Y' THEN receipts_regular_cost + receipts_crossdock_regular_cost ELSE 0 END) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(12,2)) AS product_views
		,CAST(0 AS DECIMAL(12,2)) AS cart_adds
		,CAST(0 AS DECIMAL(12,2)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM receipts_stg base
	--locations
	INNER JOIN locs
		ON base.store_num = locs.loc_idnt
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND base.country = sku.channel_country
	--supplier group
	LEFT JOIN sup_group spg
		ON locs.banner = spg.banner
		AND base.supplier_num = spg.supplier_num
		AND base.department_num = spg.dept_num
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND locs.channel_brand = COALESCE(iti.channel_brand, 'NONE')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
	UNION ALL
	SELECT
		base.week_num AS week_idnt
		,locs.chnl_label
		,locs.chnl_idnt
--		,locs.country
		,locs.banner
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
--		,sku.ccs_category
--		,sku.ccs_subcategory
--		,sku.nord_role_desc
--		,sku.rack_role_desc
--		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
		,CAST('C' AS VARCHAR(2)) AS price_type
        ,CASE WHEN base.npg_ind = 'Y' THEN 1 ELSE 0 END AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
        ,CASE WHEN base.dropship_ind = 'Y' THEN 1 ELSE 0 END AS dropship_ind
        ,CASE WHEN base.rp_ind = 'Y' THEN 1 ELSE 0 END AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
        ,SUM(receipts_clearance_units + receipts_crossdock_clearance_units) AS receipt_units
        ,SUM(receipts_clearance_retail + receipts_crossdock_clearance_retail) AS receipt_dollars
        ,SUM(receipts_clearance_cost + receipts_crossdock_clearance_cost) AS receipt_cost
        ,SUM(CASE WHEN base.dropship_ind ='Y' THEN receipts_clearance_units + receipts_crossdock_clearance_units ELSE 0 END) AS receipt_dropship_units
        ,SUM(CASE WHEN base.dropship_ind ='Y' THEN receipts_clearance_retail + receipts_crossdock_clearance_retail ELSE 0 END) AS receipt_dropship_dollars
        ,SUM(CASE WHEN base.dropship_ind ='Y' THEN receipts_clearance_cost + receipts_crossdock_clearance_cost ELSE 0 END) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(12,2)) AS product_views
		,CAST(0 AS DECIMAL(12,2)) AS cart_adds
		,CAST(0 AS DECIMAL(12,2)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM receipts_stg base
	--locations
	INNER JOIN locs
		ON base.store_num = locs.loc_idnt
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND base.country = sku.channel_country
	--supplier group
	LEFT JOIN sup_group spg
		ON locs.banner = spg.banner
		AND base.supplier_num = spg.supplier_num
		AND base.department_num = spg.dept_num
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND locs.channel_brand = COALESCE(iti.channel_brand, 'NONE')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
	UNION ALL
	SELECT
		base.week_num AS week_idnt
		,locs.chnl_label
		,locs.chnl_idnt
--		,locs.country
		,locs.banner
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
--		,sku.ccs_category
--		,sku.ccs_subcategory
--		,sku.nord_role_desc
--		,sku.rack_role_desc
--		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
		,CAST('R' AS VARCHAR(2)) AS price_type
        ,CASE WHEN base.npg_ind = 'Y' THEN 1 ELSE 0 END AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
        ,0 AS dropship_ind
        ,CASE WHEN base.rp_ind = 'Y' THEN 1 ELSE 0 END AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
        ,SUM(reservestock_transfer_in_regular_units) AS receipt_units
        ,SUM(reservestock_transfer_in_regular_retail) AS receipt_dollars
        ,SUM(reservestock_transfer_in_regular_cost) AS receipt_cost
 		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(12,2)) AS product_views
		,CAST(0 AS DECIMAL(12,2)) AS cart_adds
		,CAST(0 AS DECIMAL(12,2)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM transfer_stg base
	--locations
	INNER JOIN locs
		ON base.store_num = locs.loc_idnt
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND base.country = sku.channel_country
	--supplier group
	LEFT JOIN sup_group spg
		ON locs.banner = spg.banner
		AND base.supplier_num = spg.supplier_num
		AND base.department_num = spg.dept_num
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND locs.channel_brand = COALESCE(iti.channel_brand, 'NONE')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
	UNION ALL
	SELECT
		base.week_num AS week_idnt
		,locs.chnl_label
		,locs.chnl_idnt
--		,locs.country
		,locs.banner
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
--		,sku.ccs_category
--		,sku.ccs_subcategory
--		,sku.nord_role_desc
--		,sku.rack_role_desc
--		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
		,CAST('C' AS VARCHAR(2)) AS price_type
        ,CASE WHEN base.npg_ind = 'Y' THEN 1 ELSE 0 END AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
        ,0 AS dropship_ind
        ,CASE WHEN base.rp_ind = 'Y' THEN 1 ELSE 0 END AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
        ,SUM(reservestock_transfer_in_clearance_units) AS receipt_units
        ,SUM(reservestock_transfer_in_clearance_retail) AS receipt_dollars
        ,SUM(reservestock_transfer_in_clearance_cost) AS receipt_cost
 		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(12,2)) AS product_views
		,CAST(0 AS DECIMAL(12,2)) AS cart_adds
		,CAST(0 AS DECIMAL(12,2)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM transfer_stg base
	--locations
	INNER JOIN locs
		ON base.store_num = locs.loc_idnt
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND base.country = sku.channel_country
	--supplier group
	LEFT JOIN sup_group spg
		ON locs.banner = spg.banner
		AND base.supplier_num = spg.supplier_num
		AND base.department_num = spg.dept_num
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND locs.channel_brand = COALESCE(iti.channel_brand, 'NONE')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA
	PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt)
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
	,COLUMN (week_idnt)
		ON receipts
;

/******************* RTV ********************/

--STEP:28
--DROP TABLE rtv;
CREATE MULTISET VOLATILE TABLE rtv AS (
	SELECT
		dt.week_idnt
		,locs.chnl_label
		,locs.chnl_idnt
--		,locs.country
		,locs.banner
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
--		,sku.ccs_category
--		,sku.ccs_subcategory
--		,sku.nord_role_desc
--		,sku.rack_role_desc
--		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
		,'NA' AS price_type
		,COALESCE(sku.npg_ind,0) AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
	    ,CASE WHEN ds.sku_idnt IS NOT NULL THEN 1 ELSE 0 END AS dropship_ind
        ,CASE WHEN base.rp_ind = 'Y' THEN 1 ELSE 0 END AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
  	  	,SUM(base.rtv_total_units) AS rtv_units
  		,SUM(base.rtv_total_retail) AS rtv_dollars
  		,SUM(base.rtv_total_cost) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(12,2)) AS product_views
		,CAST(0 AS DECIMAL(12,2)) AS cart_adds
		,CAST(0 AS DECIMAL(12,2)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM prd_nap_usr_vws.merch_return_to_vendor_sku_loc_week_agg_fact_vw base
	--dates
	INNER JOIN date_format dt
	    ON base.week_num = dt.true_week_num -- week_idnt
	    AND dt.ss_ind <> 1
	--locations
	INNER JOIN locs
		ON base.store_num = locs.loc_idnt
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND locs.country = sku.channel_country
	--dropship indicator
	LEFT JOIN dropship ds
	    ON base.rms_sku_num = ds.sku_idnt
	    AND dt.week_end_date = ds.day_date
	--supplier group
	LEFT JOIN sup_group spg
		ON locs.banner = spg.banner
		AND sku.supp_idnt = spg.supplier_num
		AND sku.dept_idnt = spg.dept_num
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND locs.channel_brand = COALESCE(iti.channel_brand, 'NONE')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
	,COLUMN (week_idnt)
		ON rtv
;


/******************* ON ORDER ********************/

--STEP:29
-- DROP TABLE oo_stage;
CREATE MULTISET VOLATILE TABLE oo_stage AS (
    SELECT
        base.otb_eow_date
        ,base.store_num
        ,locs.chnl_label
        ,locs.chnl_idnt
        ,locs.country
        ,locs.banner
        ,locs.channel_brand
        ,base.rms_sku_num
        ,base.anticipated_price_type AS price_type
        ,CASE WHEN base.order_type = 'AUTOMATIC_REORDER' THEN 1 ELSE 0 END AS rp_ind --may add 'BUYER_REORDER' if Erica confirms, but very minimal amount
        ,base.quantity_open AS on_order_units
        ,CAST((base.quantity_open * base.anticipated_retail_amt) AS DECIMAL(12,2)) AS on_order_retail_dollars
        ,CAST((base.quantity_open * base.unit_cost_amt) AS DECIMAL(12,2)) AS on_order_cost_dollars
        ,CASE WHEN base.otb_eow_date <= (SELECT MAX(day_date) + 28 FROM date_filter_main WHERE ss_ind = 2)
            THEN base.quantity_open ELSE 0 END AS on_order_4wk_units
        ,CASE WHEN base.otb_eow_date <= (SELECT MAX(day_date) + 28 FROM date_filter_main WHERE ss_ind = 2)
            THEN CAST((base.quantity_open * base.anticipated_retail_amt) AS DECIMAL(12,2)) ELSE 0 END AS on_order_4wk_retail_dollars
        ,CASE WHEN base.otb_eow_date <= (SELECT MAX(day_date) + 28 FROM date_filter_main WHERE ss_ind = 2)
            THEN CAST((base.quantity_open * base.unit_cost_amt) AS DECIMAL(12,2)) ELSE 0 END AS on_order_4wk_cost_dollars
    FROM prd_nap_usr_vws.merch_on_order_fact_vw base
    INNER JOIN locs
        ON base.store_num = locs.loc_idnt
    WHERE base.quantity_open > 0
        AND base.first_approval_date IS NOT NULL
        AND (
            (base.status = 'CLOSED' AND base.end_ship_date >= (SELECT MAX(day_date) FROM date_filter_main WHERE ss_ind = 2) - 45)
                OR base.status IN ('APPROVED','WORKSHEET')
            )
) WITH DATA
    PRIMARY INDEX(rms_sku_num, country, banner)
    PARTITION BY RANGE_N(otb_eow_date BETWEEN CAST('2012-01-01' AS DATE) AND CAST('2050-12-31' AS DATE) EACH INTERVAL '1' DAY )
    ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    PRIMARY INDEX(rms_sku_num, country, banner)
    ,COLUMN (rms_sku_num)
    ,COLUMN (rms_sku_num, country)
    ,COLUMN (rms_sku_num, banner)
    ,COLUMN (country)
    ,COLUMN (banner)
    ,COLUMN (PARTITION)
		ON oo_stage
;

--STEP:30
--DROP TABLE oo;
CREATE MULTISET VOLATILE TABLE oo AS (
	SELECT
		(SELECT MAX(week_idnt) FROM date_filter_main WHERE ss_ind = 2) AS week_idnt
		,base.chnl_label
		,base.chnl_idnt
--		,base.country
		,base.banner
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
--		,sku.ccs_category
--		,sku.ccs_subcategory
--		,sku.nord_role_desc
--		,sku.rack_role_desc
--		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
		,base.price_type
		,COALESCE(sku.npg_ind,0) AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
		,CAST(0 AS INTEGER) AS dropship_ind
		,base.rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
        ,CAST(0 AS DECIMAL(10,0)) AS receipt_units
        ,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
        ,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
        ,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
        ,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
        ,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(12,2)) AS product_views
		,CAST(0 AS DECIMAL(12,2)) AS cart_adds
		,CAST(0 AS DECIMAL(12,2)) AS web_order_units
		,SUM(base.on_order_units) AS on_order_units
		,SUM(base.on_order_retail_dollars) AS on_order_retail_dollars
		,SUM(base.on_order_cost_dollars) AS on_order_cost_dollars
		,SUM(base.on_order_4wk_units) AS on_order_4wk_units
		,SUM(base.on_order_4wk_retail_dollars) AS on_order_4wk_retail_dollars
		,SUM(base.on_order_4wk_cost_dollars) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM oo_stage base
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND base.country = sku.channel_country
	--supplier group
	LEFT JOIN sup_group spg
		ON base.banner = spg.banner
		AND sku.supp_idnt = spg.supplier_num
		AND sku.dept_idnt = spg.dept_num
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND base.channel_brand = COALESCE(iti.channel_brand, 'NONE')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA
    PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
    ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
	,COLUMN (week_idnt)
		ON oo
;


/******************* BACKORDER ********************/

--STEP:31
--DROP TABLE bo;
CREATE MULTISET VOLATILE TABLE bo AS (
	SELECT
		dt.week_idnt
		,locs.chnl_label
		,locs.chnl_idnt
--		,locs.country
		,locs.banner
		,sku.div_idnt
		,sku.div_label
		,sku.sdiv_nmbr
		,sku.sdiv_label
		,sku.dept_idnt
		,sku.dept_label
		,sku.class_idnt
		,sku.class_label
		,sku.sbclass_idnt
		,sku.sbclass_label
		,sku.supp_idnt
		,sku.supp_name
		,sku.brand_name
		,sku.quantrix_category
--		,sku.ccs_category
--		,sku.ccs_subcategory
--		,sku.nord_role_desc
--		,sku.rack_role_desc
--		,sku.parent_group
		,sku.merch_themes
		,sku.assortment_grouping
		,sku.holiday_theme_ty
		,sku.gift_ind
		,sku.stocking_stuffer
		,spg.supplier_group
		,iti.intended_lifecycle_type
		,iti.intended_season
		,iti.intended_exit_month_year
		,iti.scaled_event
		,iti.holiday_or_celebration
		,base.price_type
		,COALESCE(sku.npg_ind,0) AS npg_ind
		,COALESCE(sku.diverse_brand_ind,0) AS diverse_brand_ind
	    ,CASE WHEN ds.sku_idnt IS NOT NULL THEN 1 ELSE 0 END AS dropship_ind
		,CASE WHEN rp.rms_sku_num IS NOT NULL THEN 1 ELSE 0 END AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(10,0)) AS product_views
		,CAST(0 AS DECIMAL(10,0)) AS cart_adds
		,CAST(0 AS DECIMAL(10,0)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,SUM(base.backorder_units      ) AS backorder_units
		,SUM(base.backorder_dollars    ) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
--	FROM prd_ma_bado.co_bo_sku_ld_vw base
	FROM t2dl_das_ace_mfp.sku_bo_history base
	--dates
	INNER JOIN date_filter_main dt
		ON base.order_date_pacific = dt.day_date
    	AND dt.ss_ind <> 1
	--locations
	INNER JOIN locs
		ON base.store_num = locs.loc_idnt
	--hierarchy and themes
	INNER JOIN hier sku
		ON base.rms_sku_num = sku.sku_idnt
		AND base.channel_country = sku.channel_country
	-- RP indicator
    LEFT JOIN prd_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
		ON base.rms_sku_num = rp.rms_sku_num
    	AND base.store_num = rp.location_num
        AND rp.rp_period CONTAINS base.order_date_pacific
	--supplier group
	LEFT JOIN sup_group spg
		ON locs.banner = spg.banner
		AND sku.supp_idnt = spg.supplier_num
		AND sku.dept_idnt = spg.dept_num
	--dropship
	LEFT JOIN dropship ds
		ON base.rms_sku_num = ds.sku_idnt
		AND base.order_date_pacific = ds.day_date
	--ITEM INTENT
	LEFT JOIN prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw iti
		ON sku.rms_style_num = COALESCE(iti.rms_style_num, -1)
		AND sku.color_num = COALESCE(iti.sku_nrf_color_num, iti.nrf_color_num, -1)
		AND locs.channel_brand = COALESCE(iti.channel_brand, 'NONE')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,quantrix_category)
	,COLUMN (week_idnt,dept_idnt,chnl_idnt,supplier_group)
	,COLUMN (week_idnt)
		ON bo
;


/******************* PLANS ********************/

--STEP:32
--DROP TABLE apt_sup_plan;
CREATE MULTISET VOLATILE TABLE apt_sup_plan AS (
	SELECT
		pl.week_idnt
		,chan.chnl_label
		,chan.chnl_idnt
--		,chan.country
		,chan.banner
		,dept.div_idnt
		,dept.div_label
		,dept.sdiv_nmbr
		,dept.sdiv_label
		,dept.dept_idnt
		,dept.dept_label
		,CAST(NULL AS VARCHAR(40)) AS class_idnt
		,CAST(NULL AS VARCHAR(40)) AS class_label
		,CAST(NULL AS VARCHAR(40)) AS sbclass_idnt
		,CAST(NULL AS VARCHAR(40)) AS sbclass_label
		,CAST(NULL AS VARCHAR(40)) AS supp_idnt
		,CAST(NULL AS VARCHAR(40)) AS supp_name
		,CAST(NULL AS VARCHAR(40)) AS brand_name
		,pl.category AS quantrix_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_subcategory
--		,CAST(NULL AS VARCHAR(40)) AS nord_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS rack_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS parent_group
		,CAST(NULL AS VARCHAR(40)) AS merch_themes
		,CAST(NULL AS VARCHAR(40)) AS assortment_grouping
		,CAST(NULL AS VARCHAR(40)) AS holiday_theme_ty
		,CAST(NULL AS VARCHAR(40)) AS gift_ind
		,CAST(NULL AS VARCHAR(40)) AS stocking_stuffer
		,pl.supplier_group
		,CAST(NULL AS VARCHAR(40)) AS intended_lifecycle_type
		,CAST(NULL AS VARCHAR(40)) AS intended_season
		,CAST(NULL AS VARCHAR(40)) AS intended_exit_month_year
		,CAST(NULL AS VARCHAR(40)) AS scaled_event
		,CAST(NULL AS VARCHAR(40)) AS holiday_or_celebration
		,CAST(NULL AS VARCHAR(40)) AS price_type
		,0 AS npg_ind
		,0 AS diverse_brand_ind
		,CASE WHEN fulfill_type_num = 1 THEN 1 ELSE 0 END AS dropship_ind
	    ,0 AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(10,0)) AS product_views
		,CAST(0 AS DECIMAL(10,0)) AS cart_adds
		,CAST(0 AS DECIMAL(10,0)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,SUM(demand_units          ) AS apt_demand_u
		,SUM(demand_r_dollars      ) AS apt_demand_r
		,SUM(net_sls_units         ) AS apt_net_sls_u
		,SUM(net_sls_r_dollars     ) AS apt_net_sls_r
		,SUM(net_sls_c_dollars     ) AS apt_net_sls_c
		,SUM(return_units          ) AS apt_return_u
		,SUM(return_r_dollars      ) AS apt_return_r
		,SUM(plan_eop_c_units      ) AS apt_eop_u
		,SUM(plan_eop_c_dollars    ) AS apt_eop_c
		,SUM(rcpt_need_u           ) AS apt_receipt_u
		,SUM(rcpt_need_c           ) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM t2dl_das_apt_cost_reporting.suppliergroup_channel_cost_plans_weekly pl
	INNER JOIN
		date_format dt
		ON pl.week_idnt = dt.week_idnt
	    AND dt.ss_ind <= 2
	INNER JOIN dept
		ON pl.dept_idnt = dept.dept_idnt
	INNER JOIN chan
		ON pl.chnl_idnt = chan.chnl_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
		ON apt_sup_plan
;

--STEP:33
--DROP TABLE mfp_sls_plan;
CREATE MULTISET VOLATILE TABLE mfp_sls_plan AS (
	SELECT
		pl.week_num AS week_idnt
		,chan.chnl_label
		,chan.chnl_idnt
--		,chan.country
		,chan.banner
		,dept.div_idnt
		,dept.div_label
		,dept.sdiv_nmbr
		,dept.sdiv_label
		,dept.dept_idnt
		,dept.dept_label
		,CAST(NULL AS VARCHAR(40)) AS class_idnt
		,CAST(NULL AS VARCHAR(40)) AS class_label
		,CAST(NULL AS VARCHAR(40)) AS sbclass_idnt
		,CAST(NULL AS VARCHAR(40)) AS sbclass_label
		,CAST(NULL AS VARCHAR(40)) AS supp_idnt
		,CAST(NULL AS VARCHAR(40)) AS supp_name
		,CAST(NULL AS VARCHAR(40)) AS brand_name
		,CAST(NULL AS VARCHAR(40)) AS quantrix_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_subcategory
--		,CAST(NULL AS VARCHAR(40)) AS nord_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS rack_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS parent_group
		,CAST(NULL AS VARCHAR(40)) AS merch_themes
		,CAST(NULL AS VARCHAR(40)) AS assortment_grouping
		,CAST(NULL AS VARCHAR(40)) AS holiday_theme_ty
		,CAST(NULL AS VARCHAR(40)) AS gift_ind
		,CAST(NULL AS VARCHAR(40)) AS stocking_stuffer
		,CAST(NULL AS VARCHAR(40)) AS supplier_group
		,CAST(NULL AS VARCHAR(40)) AS intended_lifecycle_type
		,CAST(NULL AS VARCHAR(40)) AS intended_season
		,CAST(NULL AS VARCHAR(40)) AS intended_exit_month_year
		,CAST(NULL AS VARCHAR(40)) AS scaled_event
		,CAST(NULL AS VARCHAR(40)) AS holiday_or_celebration
		,CAST(NULL AS VARCHAR(40)) AS price_type
		,0 AS npg_ind
		,0 AS diverse_brand_ind
		,CASE WHEN fulfill_type_num = 1 THEN 1 ELSE 0 END AS dropship_ind
	    ,0 AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(10,0)) AS product_views
		,CAST(0 AS DECIMAL(10,0)) AS cart_adds
		,CAST(0 AS DECIMAL(10,0)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,SUM(pl.op_demand_total_qty                                               ) AS mfp_op_demand_u
		,SUM(pl.op_demand_total_retail_amt                                        ) AS mfp_op_demand_r
		,SUM(op_net_sales_qty                                                     ) AS mfp_op_sales_u
		,SUM(op_net_sales_retail_amt                                              ) AS mfp_op_sales_r
		,SUM(op_net_sales_cost_amt                                                ) AS mfp_op_sales_c
		,SUM(op_returns_qty                                                       ) AS mfp_op_returns_u
		,SUM(op_returns_retail_amt                                                ) AS mfp_op_returns_r
		,SUM(pl.sp_demand_total_qty                                               ) AS mfp_sp_demand_u
		,SUM(pl.sp_demand_total_retail_amt                                        ) AS mfp_sp_demand_r
		,SUM(sp_net_sales_qty                                                     ) AS mfp_sp_sales_u
		,SUM(sp_net_sales_retail_amt                                              ) AS mfp_sp_sales_r
		,SUM(sp_net_sales_cost_amt                                                ) AS mfp_sp_sales_c
		,SUM(sp_returns_qty                                                       ) AS mfp_sp_returns_u
		,SUM(sp_returns_retail_amt                                                ) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM prd_nap_usr_vws.mfp_cost_plan_actual_channel_fact pl
	INNER JOIN
		date_format dt
		ON pl.week_num = dt.week_idnt
	    AND dt.ss_ind <= 2
	INNER JOIN dept
		ON pl.dept_num = dept.dept_idnt
	INNER JOIN chan
		ON pl.channel_num = chan.chnl_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
	UNION ALL
	SELECT
		pl.week_num AS week_idnt
		,chan.chnl_label
		,chan.chnl_idnt
--		,chan.country
		,chan.banner
		,dept.div_idnt
		,dept.div_label
		,dept.sdiv_nmbr
		,dept.sdiv_label
		,dept.dept_idnt
		,dept.dept_label
		,CAST(NULL AS VARCHAR(40)) AS class_idnt
		,CAST(NULL AS VARCHAR(40)) AS class_label
		,CAST(NULL AS VARCHAR(40)) AS sbclass_idnt
		,CAST(NULL AS VARCHAR(40)) AS sbclass_label
		,CAST(NULL AS VARCHAR(40)) AS supp_idnt
		,CAST(NULL AS VARCHAR(40)) AS supp_name
		,CAST(NULL AS VARCHAR(40)) AS brand_name
		,CAST(NULL AS VARCHAR(40)) AS quantrix_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_subcategory
--		,CAST(NULL AS VARCHAR(40)) AS nord_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS rack_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS parent_group
		,CAST(NULL AS VARCHAR(40)) AS merch_themes
		,CAST(NULL AS VARCHAR(40)) AS assortment_grouping
		,CAST(NULL AS VARCHAR(40)) AS holiday_theme_ty
		,CAST(NULL AS VARCHAR(40)) AS gift_ind
		,CAST(NULL AS VARCHAR(40)) AS stocking_stuffer
		,CAST(NULL AS VARCHAR(40)) AS supplier_group
		,CAST(NULL AS VARCHAR(40)) AS intended_lifecycle_type
		,CAST(NULL AS VARCHAR(40)) AS intended_season
		,CAST(NULL AS VARCHAR(40)) AS intended_exit_month_year
		,CAST(NULL AS VARCHAR(40)) AS scaled_event
		,CAST(NULL AS VARCHAR(40)) AS holiday_or_celebration
		,CAST(NULL AS VARCHAR(40)) AS price_type
		,0 AS npg_ind
		,0 AS diverse_brand_ind
		,CASE WHEN fulfill_type_num = 1 THEN 1 ELSE 0 END AS dropship_ind
	    ,0 AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(10,0)) AS product_views
		,CAST(0 AS DECIMAL(10,0)) AS cart_adds
		,CAST(0 AS DECIMAL(10,0)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,SUM(pl.qp_demand_total_qty                                          ) AS mfp_qp_demand_u
		,SUM(pl.qp_demand_total_retail_amt                                   ) AS mfp_qp_demand_r
		,SUM(qp_net_sales_qty                                                ) AS mfp_qp_sales_u
		,SUM(qp_net_sales_retail_amt                                         ) AS mfp_qp_sales_r
		,SUM(qp_net_sales_cost_amt                                           ) AS mfp_qp_sales_c
		,SUM(qp_returns_qty                                                  ) AS mfp_qp_returns_u
		,SUM(qp_returns_retail_amt                                           ) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM prd_nap_vws.mfp_cost_plan_qtr_channel_fact pl
	INNER JOIN
		date_format dt
		ON pl.week_num = dt.week_idnt
	    AND dt.ss_ind <= 2
	INNER JOIN dept
		ON pl.dept_num = dept.dept_idnt
	INNER JOIN chan
		ON pl.channel_num = chan.chnl_idnt
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
		ON mfp_sls_plan
;

--STEP:34
--DROP TABLE mfp_inv_plan;
CREATE MULTISET VOLATILE TABLE mfp_inv_plan AS (
	SELECT
		pl.week_num AS week_idnt
		,chan.chnl_label
		,chan.chnl_idnt
--		,chan.country
		,chan.banner
		,dept.div_idnt
		,dept.div_label
		,dept.sdiv_nmbr
		,dept.sdiv_label
		,dept.dept_idnt
		,dept.dept_label
		,CAST(NULL AS VARCHAR(40)) AS class_idnt
		,CAST(NULL AS VARCHAR(40)) AS class_label
		,CAST(NULL AS VARCHAR(40)) AS sbclass_idnt
		,CAST(NULL AS VARCHAR(40)) AS sbclass_label
		,CAST(NULL AS VARCHAR(40)) AS supp_idnt
		,CAST(NULL AS VARCHAR(40)) AS supp_name
		,CAST(NULL AS VARCHAR(40)) AS brand_name
		,CAST(NULL AS VARCHAR(40)) AS quantrix_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_subcategory
--		,CAST(NULL AS VARCHAR(40)) AS nord_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS rack_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS parent_group
		,CAST(NULL AS VARCHAR(40)) AS merch_themes
		,CAST(NULL AS VARCHAR(40)) AS assortment_grouping
		,CAST(NULL AS VARCHAR(40)) AS holiday_theme_ty
		,CAST(NULL AS VARCHAR(40)) AS gift_ind
		,CAST(NULL AS VARCHAR(40)) AS stocking_stuffer
		,CAST(NULL AS VARCHAR(40)) AS supplier_group
		,CAST(NULL AS VARCHAR(40)) AS intended_lifecycle_type
		,CAST(NULL AS VARCHAR(40)) AS intended_season
		,CAST(NULL AS VARCHAR(40)) AS intended_exit_month_year
		,CAST(NULL AS VARCHAR(40)) AS scaled_event
		,CAST(NULL AS VARCHAR(40)) AS holiday_or_celebration
		,CAST(NULL AS VARCHAR(40)) AS price_type
		,0 AS npg_ind
		,0 AS diverse_brand_ind
		,CASE WHEN fulfill_type_num = 1 THEN 1 ELSE 0 END AS dropship_ind
	    ,0 AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(10,0)) AS product_views
		,CAST(0 AS DECIMAL(10,0)) AS cart_adds
		,CAST(0 AS DECIMAL(10,0)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,SUM(pl.op_ending_of_period_active_qty + pl.op_ending_of_period_inactive_qty                                   ) AS mfp_op_eoh_u
		,SUM(pl.op_ending_of_period_active_cost_amt + pl.op_ending_of_period_inactive_cost_amt                         ) AS mfp_op_eoh_c
		,SUM(pl.op_receipts_active_qty + pl.op_receipts_inactive_qty + pl.op_receipts_reserve_qty                      ) AS mfp_op_receipts_u
		,SUM(pl.op_receipts_active_cost_amt + pl.op_receipts_inactive_cost_amt + pl.op_receipts_reserve_cost_amt       ) AS mfp_op_receipts_c
		,SUM(pl.sp_ending_of_period_active_qty + pl.sp_ending_of_period_inactive_qty                                   ) AS mfp_sp_eoh_u
		,SUM(pl.sp_ending_of_period_active_cost_amt + pl.sp_ending_of_period_inactive_cost_amt                         ) AS mfp_sp_eoh_c
		,SUM(pl.sp_receipts_active_qty + pl.sp_receipts_inactive_qty + pl.sp_receipts_reserve_qty                      ) AS mfp_sp_receipts_u
		,SUM(pl.sp_receipts_active_cost_amt + pl.sp_receipts_inactive_cost_amt + pl.sp_receipts_reserve_cost_amt       ) AS mfp_sp_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_receipts_c
	FROM prd_nap_usr_vws.mfp_cost_plan_actual_banner_country_fact pl
	INNER JOIN
		date_format dt
		ON pl.week_num = dt.week_idnt
	    AND dt.ss_ind <= 2
	INNER JOIN dept
		ON pl.dept_num = dept.dept_idnt
	INNER JOIN chan_inv chan
		ON pl.banner_country_num = chan.banner_country_num
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
	UNION ALL
	SELECT
		pl.week_num AS week_idnt
		,chan.chnl_label
		,chan.chnl_idnt
--		,chan.country
		,chan.banner
		,dept.div_idnt
		,dept.div_label
		,dept.sdiv_nmbr
		,dept.sdiv_label
		,dept.dept_idnt
		,dept.dept_label
		,CAST(NULL AS VARCHAR(40)) AS class_idnt
		,CAST(NULL AS VARCHAR(40)) AS class_label
		,CAST(NULL AS VARCHAR(40)) AS sbclass_idnt
		,CAST(NULL AS VARCHAR(40)) AS sbclass_label
		,CAST(NULL AS VARCHAR(40)) AS supp_idnt
		,CAST(NULL AS VARCHAR(40)) AS supp_name
		,CAST(NULL AS VARCHAR(40)) AS brand_name
		,CAST(NULL AS VARCHAR(40)) AS quantrix_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_category
--		,CAST(NULL AS VARCHAR(40)) AS ccs_subcategory
--		,CAST(NULL AS VARCHAR(40)) AS nord_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS rack_role_desc
--		,CAST(NULL AS VARCHAR(40)) AS parent_group
		,CAST(NULL AS VARCHAR(40)) AS merch_themes
		,CAST(NULL AS VARCHAR(40)) AS assortment_grouping
		,CAST(NULL AS VARCHAR(40)) AS holiday_theme_ty
		,CAST(NULL AS VARCHAR(40)) AS gift_ind
		,CAST(NULL AS VARCHAR(40)) AS stocking_stuffer
		,CAST(NULL AS VARCHAR(40)) AS supplier_group
		,CAST(NULL AS VARCHAR(40)) AS intended_lifecycle_type
		,CAST(NULL AS VARCHAR(40)) AS intended_season
		,CAST(NULL AS VARCHAR(40)) AS intended_exit_month_year
		,CAST(NULL AS VARCHAR(40)) AS scaled_event
		,CAST(NULL AS VARCHAR(40)) AS holiday_or_celebration
		,CAST(NULL AS VARCHAR(40)) AS price_type
		,0 AS npg_ind
		,0 AS diverse_brand_ind
		,CASE WHEN fulfill_type_num = 1 THEN 1 ELSE 0 END AS dropship_ind
	    ,0 AS rp_ind
		,CAST(0 AS DECIMAL(10,0)) AS sales_units
		,CAST(0 AS DECIMAL(12,2)) AS sales_dollars
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
		,CAST(0 AS DECIMAL(10,0)) AS receipt_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_cost
		,CAST(0 AS DECIMAL(10,0)) AS receipt_dropship_units
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_dollars
		,CAST(0 AS DECIMAL(12,2)) AS receipt_dropship_cost
		,CAST(0 AS DECIMAL(10,0)) AS rtv_units
		,CAST(0 AS DECIMAL(12,2)) AS rtv_dollars
		,CAST(0 AS DECIMAL(12,2)) AS rtv_cost
		,CAST(0 AS DECIMAL(10,0)) AS eoh_units
		,CAST(0 AS DECIMAL(12,2)) AS eoh_dollars
		,CAST(0 AS DECIMAL(12,2)) AS eoh_cost
		,CAST(0 AS DECIMAL(10,0)) AS nonsellable_units
		,CAST(0 AS DECIMAL(10,0)) AS product_views
		,CAST(0 AS DECIMAL(10,0)) AS cart_adds
		,CAST(0 AS DECIMAL(10,0)) AS web_order_units
		,CAST(0 AS DECIMAL(10,0)) AS on_order_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_retail_dollars
	    ,CAST(0 AS DECIMAL(12,2)) AS on_order_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS on_order_4wk_units
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_retail_dollars
		,CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_cost_dollars
		,CAST(0 AS DECIMAL(10,0)) AS backorder_units
		,CAST(0 AS DECIMAL(12,2)) AS backorder_dollars
		,CAST(0 AS DECIMAL(38,9)) AS cost_of_goods_sold
		,CAST(0 AS DECIMAL(38,9)) AS product_margin
		,CAST(0 AS DECIMAL(12,2)) AS sales_pm
		,CAST(0 AS DECIMAL(10,0)) AS apt_demand_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_net_sls_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_r
		,CAST(0 AS DECIMAL(20,2)) AS apt_net_sls_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_return_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_return_r
		,CAST(0 AS DECIMAL(10,0)) AS apt_eop_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_eop_c
		,CAST(0 AS DECIMAL(10,0)) AS apt_receipt_u
		,CAST(0 AS DECIMAL(20,2)) AS apt_receipt_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_demand_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_demand_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_sales_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_r
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_sales_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_qp_returns_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_qp_returns_r
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_op_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_op_receipts_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_eoh_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_eoh_c
		,CAST(0 AS DECIMAL(10,0)) AS mfp_sp_receipts_u
		,CAST(0 AS DECIMAL(20,0)) AS mfp_sp_receipts_c
		,SUM(pl.qp_ending_of_period_active_qty + pl.qp_ending_of_period_inactive_qty                                ) AS mfp_qp_eoh_u
		,SUM(pl.qp_ending_of_period_active_cost_amt + pl.qp_ending_of_period_inactive_cost_amt                      ) AS mfp_qp_eoh_c
		,SUM(pl.qp_receipts_active_qty + pl.qp_receipts_inactive_qty + pl.qp_receipts_reserve_qty                   ) AS mfp_qp_receipts_u
		,SUM(pl.qp_receipts_active_cost_amt + pl.qp_receipts_inactive_cost_amt + pl.qp_receipts_reserve_cost_amt    ) AS mfp_qp_receipts_c
	FROM prd_nap_usr_vws.mfp_cost_plan_qtr_banner_country_fact pl
	INNER JOIN
		date_format dt
		ON pl.week_num = dt.week_idnt
	    AND dt.ss_ind <= 2
	INNER JOIN dept
		ON pl.dept_num = dept.dept_idnt
	INNER JOIN chan_inv chan
		ON pl.banner_country_num = chan.banner_country_num
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (chnl_idnt, week_idnt, dept_idnt)
		ON mfp_inv_plan
;


/******************* FINAL DELETE & INSERT ********************/

--STEP:35
--DROP TABLE combine;
CREATE MULTISET VOLATILE TABLE combine AS (
SELECT
     dt.ty_ly_ind
    ,dt.ss_ind
  	,dt.week_end_date
  	,dt.month_idnt
  	,dt.quarter_idnt
  	,dt.year_idnt
  	,dt.week_label
  	,dt.month_label
  	,dt.quarter_label
  	,dt.week_rank
  	,dt.week_idnt_aligned
  	,dt.month_idnt_aligned
  	,dt.quarter_idnt_aligned
  	,dt.year_idnt_aligned
  	,dt.week_label_aligned
  	,dt.month_label_aligned
  	,dt.quarter_label_aligned
	,CASE WHEN anc.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS anchor_brand_ind
	,CASE WHEN rsb.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS rack_strategic_brand_ind
	,aor.general_merch_manager_executive_vice_president
	,aor.div_merch_manager_senior_vice_president
	,aor.div_merch_manager_vice_president
	,aor.merch_director
	,aor.buyer
	,aor.merch_planning_executive_vice_president
	,aor.merch_planning_senior_vice_president
	,aor.merch_planning_vice_president
	,aor.merch_planning_director_manager
	,aor.assortment_planner 	
  	,sub.*
FROM (
    SELECT * FROM merch
    UNION ALL
    SELECT * FROM pfd_ty
    UNION ALL
    SELECT * FROM pfd_ly
    UNION ALL
    SELECT * FROM receipts
    UNION ALL
    SELECT * FROM rtv
    UNION ALL
    SELECT * FROM oo
    UNION ALL
    SELECT * FROM bo
    UNION ALL
    SELECT * FROM apt_sup_plan
    UNION ALL
    SELECT * FROM mfp_inv_plan
    UNION ALL
    SELECT * FROM mfp_sls_plan
    ) sub
--date format
FULL OUTER JOIN date_format dt
	ON sub.week_idnt = dt.week_idnt
--ANCHOR BRANDS
LEFT JOIN anc
	ON sub.dept_idnt = anc.dept_num
	AND sub.supp_idnt = anc.supplier_idnt
	AND sub.banner = 'N'
--RSB
LEFT JOIN rsb
	ON sub.dept_idnt = rsb.dept_num
	AND sub.supp_idnt = rsb.supplier_idnt
	AND sub.banner = 'NR'
--AOR
LEFT JOIN aor
	ON sub.dept_idnt = aor.dept_num
	AND sub.banner = aor.banner
) WITH DATA PRIMARY INDEX(chnl_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     COLUMN (chnl_label ,chnl_idnt ,banner ,div_idnt ,div_label ,sdiv_nmbr ,sdiv_label ,dept_idnt,
			 dept_label ,class_idnt ,class_label ,sbclass_idnt ,sbclass_label,
			 supp_idnt, supp_name ,brand_name ,quantrix_category,
			 merch_themes, supplier_group ,price_type ,npg_ind ,diverse_brand_ind,dropship_ind ,rp_ind)
    ,COLUMN (ty_ly_ind ,week_end_date,month_idnt, quarter_idnt ,year_idnt ,week_label ,month_label,
			 quarter_label ,week_rank ,week_idnt_aligned ,month_idnt_aligned,quarter_idnt_aligned ,year_idnt_aligned ,week_label_aligned,
			 month_label_aligned ,quarter_label_aligned ,week_idnt,chnl_label ,chnl_idnt ,banner ,div_idnt , div_label,
			 sdiv_nmbr ,sdiv_label ,dept_idnt ,dept_label ,class_idnt,class_label ,sbclass_idnt ,sbclass_label ,supp_name ,brand_name,
			 quantrix_category ,merch_themes, supplier_group ,price_type ,npg_ind,
			 diverse_brand_ind ,dropship_ind ,rp_ind)
    ,COLUMN (week_idnt)
    ,COLUMN (week_idnt ,chnl_idnt ,dept_idnt)
    ,COLUMN (rp_ind)
    ,COLUMN (dropship_ind)
    ,COLUMN (diverse_brand_ind)
    ,COLUMN (npg_ind)
    ,COLUMN (price_type)
    ,COLUMN (sdiv_nmbr)
    ,COLUMN (year_idnt_aligned)
    ,COLUMN (quarter_idnt_aligned)
    ,COLUMN (month_idnt_aligned)
    ,COLUMN (week_idnt_aligned)
    ,COLUMN (week_rank)
    ,COLUMN (year_idnt)
    ,COLUMN (quarter_idnt)
    ,COLUMN (month_idnt)
    ,COLUMN (week_idnt)
    ,COLUMN (week_end_date)
    ON combine
;


--STEP:36
DELETE FROM {environment_schema}.wbr_supplier{env_suffix};
INSERT INTO {environment_schema}.wbr_supplier{env_suffix}
SELECT
  	 sub.ty_ly_ind
  	,CASE
  		WHEN sub.ss_ind = 1 THEN 'PL'
  		WHEN sub.ss_ind = 2 THEN 'SS'
  		WHEN sub.ss_ind = 3 THEN 'SS'
  		WHEN sub.ss_ind = 4 THEN 'DS'
  		END AS ss_ind
  	,sub.week_end_date
  	,sub.week_idnt
  	,sub.month_idnt
  	,sub.quarter_idnt
  	,sub.year_idnt
  	,sub.week_label
  	,sub.month_label
  	,sub.quarter_label
  	,sub.week_rank
  	,sub.week_idnt_aligned
  	,sub.month_idnt_aligned
  	,sub.quarter_idnt_aligned
  	,sub.year_idnt_aligned
  	,sub.week_label_aligned
  	,sub.month_label_aligned
  	,sub.quarter_label_aligned
  	,sub.chnl_idnt
  	,sub.chnl_label
--  	,sub.country
  	,sub.banner
  	,sub.div_idnt
  	,sub.div_label AS division
  	,sub.sdiv_nmbr AS sdiv_idnt
  	,sub.sdiv_label AS subdivision
  	,sub.dept_idnt
  	,sub.dept_label AS department
  	,sub.class_idnt
  	,sub.class_label AS "class"
  	,sub.sbclass_idnt
  	,sub.sbclass_label AS subclass
	,sub.supp_idnt AS supplier_idnt
  	,sub.supp_name AS supplier
  	,sub.brand_name AS brand
  	,sub.quantrix_category
--  	,sub.ccs_category
--  	,sub.ccs_subcategory
--  	,sub.nord_role_desc
--  	,sub.rack_role_desc
--  	,sub.parent_group
  	,sub.merch_themes
	,sub.assortment_grouping
	,sub.holiday_theme_ty
	,sub.gift_ind
	,sub.stocking_stuffer
  	,sub.supplier_group
	,sub.intended_lifecycle_type
	,sub.intended_season
	,sub.intended_exit_month_year
	,sub.scaled_event
	,sub.holiday_or_celebration
  	,COALESCE(sub.price_type, 'NA') AS price_type
  	,COALESCE(sub.npg_ind,0) AS npg_ind
  	,COALESCE(sub.diverse_brand_ind,0) AS diverse_brand_ind
    ,COALESCE(sub.dropship_ind,0) AS dropship_ind
  	,COALESCE(sub.rp_ind,0) AS rp_ind
	,sub.anchor_brand_ind
	,sub.rack_strategic_brand_ind
	,sub.general_merch_manager_executive_vice_president
	,sub.div_merch_manager_senior_vice_president
	,sub.div_merch_manager_vice_president
	,sub.merch_director
	,sub.buyer
	,sub.merch_planning_executive_vice_president
	,sub.merch_planning_senior_vice_president
	,sub.merch_planning_vice_president
	,sub.merch_planning_director_manager
	,sub.assortment_planner 	
	,COALESCE(SUM(sub.sales_units                          ),0) AS sales_units
	,COALESCE(SUM(sub.sales_dollars                        ),0) AS sales_dollars
	,COALESCE(SUM(sub.cost_of_goods_sold                   ),0) AS cost_of_goods_sold
	,COALESCE(SUM(sub.product_margin                       ),0) AS product_margin
	,COALESCE(SUM(sub.sales_pm                             ),0) AS sales_pm
	,COALESCE(SUM(sub.return_units                         ),0) AS return_units
	,COALESCE(SUM(sub.return_dollars                       ),0) AS return_dollars
	,COALESCE(SUM(sub.demand_units                         ),0) AS demand_units
	,COALESCE(SUM(sub.demand_dollars                       ),0) AS demand_dollars
	,COALESCE(SUM(sub.demand_cancel_units                  ),0) AS demand_cancel_units
	,COALESCE(SUM(sub.demand_cancel_dollars                ),0) AS demand_cancel_dollars
	,COALESCE(SUM(sub.demand_dropship_units                ),0) AS demand_dropship_units
	,COALESCE(SUM(sub.demand_dropship_dollars              ),0) AS demand_dropship_dollars
	,COALESCE(SUM(sub.shipped_units                        ),0) AS shipped_units
	,COALESCE(SUM(sub.shipped_dollars                      ),0) AS shipped_dollars
	,COALESCE(SUM(sub.store_fulfill_units                  ),0) AS store_fulfill_units
	,COALESCE(SUM(sub.store_fulfill_dollars                ),0) AS store_fulfill_dollars
	,COALESCE(SUM(sub.receipt_units                        ),0) AS receipt_units
	,COALESCE(SUM(sub.receipt_dollars                      ),0) AS receipt_dollars
	,COALESCE(SUM(sub.receipt_cost                         ),0) AS receipt_cost
	,COALESCE(SUM(sub.receipt_dropship_units               ),0) AS receipt_dropship_units
	,COALESCE(SUM(sub.receipt_dropship_dollars             ),0) AS receipt_dropship_dollars
	,COALESCE(SUM(sub.receipt_dropship_cost                ),0) AS receipt_dropship_cost
	,COALESCE(SUM(sub.rtv_units                            ),0) AS rtv_units
	,COALESCE(SUM(sub.rtv_dollars                          ),0) AS rtv_dollars
	,COALESCE(SUM(sub.rtv_cost                             ),0) AS rtv_cost
	,COALESCE(SUM(sub.eoh_units                            ),0) AS eoh_units
	,COALESCE(SUM(sub.eoh_dollars                          ),0) AS eoh_dollars
	,COALESCE(SUM(sub.eoh_cost                             ),0) AS eoh_cost
	,COALESCE(SUM(sub.nonsellable_units                    ),0) AS nonsellable_units
	,COALESCE(SUM(sub.product_views                        ),0) AS product_views
	,COALESCE(SUM(sub.cart_adds                            ),0) AS cart_adds
	,COALESCE(SUM(sub.web_order_units                      ),0) AS web_order_units
	,COALESCE(SUM(sub.on_order_units                       ),0) AS on_order_units
	,COALESCE(SUM(sub.on_order_retail_dollars              ),0) AS on_order_retail_dollars
	,COALESCE(SUM(sub.on_order_cost_dollars                ),0) AS on_order_cost_dollars
	,COALESCE(SUM(sub.on_order_4wk_units                   ),0) AS on_order_4wk_units
	,COALESCE(SUM(sub.on_order_4wk_retail_dollars          ),0) AS on_order_4wk_retail_dollars
	,COALESCE(SUM(sub.on_order_4wk_cost_dollars            ),0) AS on_order_4wk_cost_dollars
	,COALESCE(SUM(sub.backorder_units                      ),0) AS backorder_units
	,COALESCE(SUM(sub.backorder_dollars                    ),0) AS backorder_dollars
	,COALESCE(SUM(sub.apt_demand_u                         ),0) AS apt_demand_u
	,COALESCE(SUM(sub.apt_demand_r                         ),0) AS apt_demand_r
	,COALESCE(SUM(sub.apt_net_sls_u                        ),0) AS apt_net_sls_u
	,COALESCE(SUM(sub.apt_net_sls_r                        ),0) AS apt_net_sls_r
	,COALESCE(SUM(sub.apt_net_sls_c                        ),0) AS apt_net_sls_c
	,COALESCE(SUM(sub.apt_return_u                         ),0) AS apt_return_u
	,COALESCE(SUM(sub.apt_return_r                         ),0) AS apt_return_r
	,COALESCE(SUM(sub.apt_eop_u                            ),0) AS apt_eop_u
	,COALESCE(SUM(sub.apt_eop_c                            ),0) AS apt_eop_c
	,COALESCE(SUM(sub.apt_receipt_u                        ),0) AS apt_receipt_u
	,COALESCE(SUM(sub.apt_receipt_c                        ),0) AS apt_receipt_c
	,COALESCE(SUM(sub.mfp_op_demand_u                      ),0) AS mfp_op_demand_u
	,COALESCE(SUM(sub.mfp_op_demand_r                      ),0) AS mfp_op_demand_r
	,COALESCE(SUM(sub.mfp_op_sales_u                       ),0) AS mfp_op_sales_u
	,COALESCE(SUM(sub.mfp_op_sales_r                       ),0) AS mfp_op_sales_r
	,COALESCE(SUM(sub.mfp_op_sales_c                       ),0) AS mfp_op_sales_c
	,COALESCE(SUM(sub.mfp_op_returns_u                     ),0) AS mfp_op_returns_u
	,COALESCE(SUM(sub.mfp_op_returns_r                     ),0) AS mfp_op_returns_r
	,COALESCE(SUM(sub.mfp_sp_demand_u                      ),0) AS mfp_sp_demand_u
	,COALESCE(SUM(sub.mfp_sp_demand_r                      ),0) AS mfp_sp_demand_r
	,COALESCE(SUM(sub.mfp_sp_sales_u                       ),0) AS mfp_sp_sales_u
	,COALESCE(SUM(sub.mfp_sp_sales_r                       ),0) AS mfp_sp_sales_r
	,COALESCE(SUM(sub.mfp_sp_sales_c                       ),0) AS mfp_sp_sales_c
	,COALESCE(SUM(sub.mfp_sp_returns_u                     ),0) AS mfp_sp_returns_u
	,COALESCE(SUM(sub.mfp_sp_returns_r                     ),0) AS mfp_sp_returns_r
	,COALESCE(SUM(sub.mfp_qp_demand_u                      ),0) AS mfp_qp_demand_u
	,COALESCE(SUM(sub.mfp_qp_demand_r                      ),0) AS mfp_qp_demand_r
	,COALESCE(SUM(sub.mfp_qp_sales_u                       ),0) AS mfp_qp_sales_u
	,COALESCE(SUM(sub.mfp_qp_sales_r                       ),0) AS mfp_qp_sales_r
	,COALESCE(SUM(sub.mfp_qp_sales_c                       ),0) AS mfp_qp_sales_c
	,COALESCE(SUM(sub.mfp_qp_returns_u                     ),0) AS mfp_qp_returns_u
	,COALESCE(SUM(sub.mfp_qp_returns_r                     ),0) AS mfp_qp_returns_r
	,COALESCE(SUM(sub.mfp_op_eoh_u                         ),0) AS mfp_op_eoh_u
	,COALESCE(SUM(sub.mfp_op_eoh_c                         ),0) AS mfp_op_eoh_c
	,COALESCE(SUM(sub.mfp_op_receipts_u                    ),0) AS mfp_op_receipts_u
	,COALESCE(SUM(sub.mfp_op_receipts_c                    ),0) AS mfp_op_receipts_c
	,COALESCE(SUM(sub.mfp_sp_eoh_u                         ),0) AS mfp_sp_eoh_u
	,COALESCE(SUM(sub.mfp_sp_eoh_c                         ),0) AS mfp_sp_eoh_c
	,COALESCE(SUM(sub.mfp_sp_receipts_u                    ),0) AS mfp_sp_receipts_u
	,COALESCE(SUM(sub.mfp_sp_receipts_c                    ),0) AS mfp_sp_receipts_c
	,COALESCE(SUM(sub.mfp_qp_eoh_u                         ),0) AS mfp_qp_eoh_u
	,COALESCE(SUM(sub.mfp_qp_eoh_c                         ),0) AS mfp_qp_eoh_c
	,COALESCE(SUM(sub.mfp_qp_receipts_u                    ),0) AS mfp_qp_receipts_u
	,COALESCE(SUM(sub.mfp_qp_receipts_c                    ),0) AS mfp_qp_receipts_c
  	,CURRENT_TIMESTAMP AS update_timestamp
FROM combine sub
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
	,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40
	,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60
	,61,62,63
;

COLLECT STATS
	--PRIMARY INDEX (week_idnt, chnl_idnt, dept_idnt, quantrix_category, supplier)
	ON {environment_schema}.wbr_supplier{env_suffix}
;
