CREATE MULTISET VOLATILE TABLE max_dt_check
AS (
    SELECT 
        t1.month_start_day_date AS max_day_dt
        , t2.day_idnt AS max_day_idnt
        , t1.fiscal_year_num
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM t1
    LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM t2
    ON t1.month_start_day_date  = t2.day_date
    WHERE t1.day_date = CURRENT_DATE
) WITH DATA
PRIMARY INDEX(max_day_dt, max_day_idnt, fiscal_year_num)
ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE current_year
AS (
    SELECT 
        MIN(day_date) AS year_start_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM t1
    WHERE fiscal_year_num = (SELECT fiscal_year_num FROM max_dt_check)
) WITH DATA
PRIMARY INDEX(year_start_date)
ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE skus (sku_idnt)
AS (
    SELECT DISTINCT 
        sku.rms_sku_num AS sku_idnt
    FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku 
    WHERE sku.div_num NOT BETWEEN 90 AND 99
) WITH DATA
PRIMARY INDEX(sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS PRIMARY INDEX (sku_idnt), COLUMN (sku_idnt) ON skus;

CREATE MULTISET VOLATILE TABLE bipoc_sales_eoh
AS (
    SELECT
        fiscal_year_num AS yr_idnt
        , month_idnt AS mth_idnt
        , TRIM(locs.channel_num) || ', ' || TRIM(locs.channel_desc) AS chnl_label
        , CASE WHEN locs.channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310') THEN 'US'
        	 WHEN locs.channel_num IN ('111', '121', '211', '221', '261', '311') THEN 'CA'
        	 ELSE NULL END AS country
        , CASE WHEN locs.channel_num IN ('110', '120', '130', '140', '111', '121', '310', '311') THEN 'FP'
    	     WHEN locs.channel_num IN ('211', '221', '261', '210', '220', '250', '260') THEN 'OP'
    	     ELSE NULL END AS banner
    	, base.sku_idnt 
        , SUM(base.SALES_UNITS) as sales_u
        , SUM(base.SALES_DOLLARS) AS sales_$
        , SUM(base.RETURN_UNITS) as return_u
        , SUM(base.RETURN_DOLLARS) as return_$
        , SUM(base.DEMAND_UNITS) as demand_u
        , SUM(base.DEMAND_DOLLARS) as demand_$
        , SUM(base.EOH_UNITS) as eoh_u
        , SUM(base.EOH_DOLLARS) as eoh_$
        --, CAST(0 AS DECIMAL(12,2)) as eoh_cost
        , CAST(0 AS DECIMAL(12,2)) as ntn
        , CAST(0 AS DECIMAL(12,2)) as receipt_u
        , CAST(0 AS DECIMAL(12,2)) as receipt_$
        , CAST(0 AS DECIMAL(12,2)) AS on_order_u
        , CAST(0 AS DECIMAL(12,2)) AS on_order_$
        , CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_u
        , CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_$
    FROM t2dl_das_ace_mfp.sku_loc_pricetype_day_vw base 
    INNER JOIN PRD_NAP_USR_VWS.STORE_DIM locs
        ON base.loc_idnt = locs.store_num
    INNER JOIN skus
        ON base.sku_idnt = skus.sku_idnt
    INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dates 
        ON base.day_dt = dates.day_date
    WHERE locs.channel_num in ('110', '120', '130', '140', '210', '220', '250', '260', '310', '111', '121', '211', '221', '261', '311')
      AND dates.day_date >= (SELECT year_start_date FROM current_year)
      AND dates.day_date < (SELECT max_day_dt FROM max_dt_check)
    GROUP BY 1, 2, 3, 4, 5, 6
) WITH DATA
PRIMARY INDEX(yr_idnt, mth_idnt, chnl_label, country, banner, sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS PRIMARY INDEX (yr_idnt, mth_idnt, chnl_label, country, banner, sku_idnt), COLUMN (mth_idnt ,yr_idnt), COLUMN (sku_idnt) ON bipoc_sales_eoh;

CREATE MULTISET VOLATILE TABLE bipoc_ntn
AS(
    SELECT
       	fiscal_year_num AS yr_idnt
        , month_idnt AS mth_idnt
        , TRIM(locs.channel_num) || ', ' || TRIM(locs.channel_desc) AS chnl_label
        , CASE WHEN locs.channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310') THEN 'US'
        	 WHEN locs.channel_num IN ('111', '121', '211', '221', '261', '311') THEN 'CA'
        	 ELSE NULL END AS country
        , CASE WHEN locs.channel_num IN ('110', '120', '130', '140', '111', '121', '310', '311') THEN 'FP'
    	     WHEN locs.channel_num IN ('211', '221', '261', '210', '220', '250', '260') THEN 'OP'
    	     ELSE NULL END AS banner
        , base.sku_num AS sku_idnt 
        , CAST(0 AS DECIMAL(12,2)) as sales_u
        , CAST(0 AS DECIMAL(12,2)) AS sales_$
        , CAST(0 AS DECIMAL(12,2)) as return_u
        , CAST(0 AS DECIMAL(12,2)) as return_$
        , CAST(0 AS DECIMAL(12,2)) as demand_u
        , CAST(0 AS DECIMAL(12,2)) as demand_$
        , CAST(0 AS DECIMAL(12,2)) as eoh_u
        , CAST(0 AS DECIMAL(12,2)) as eoh_$
        --, CAST(0 AS DECIMAL(12,2)) AS eoh_cost
       	, COUNT(DISTINCT acp_id) as ntn
        , CAST(0 AS DECIMAL(12,2)) AS receipt_u
        , CAST(0 AS DECIMAL(12,2)) AS receipt_$
        , CAST(0 AS DECIMAL(12,2)) AS on_order_u
        , CAST(0 AS DECIMAL(12,2)) AS on_order_$
        , CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_u
        , CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_$
    FROM PRD_NAP_USR_VWS.CUSTOMER_NTN_FACT base --PRD_NAP_BASE_VWS.CUSTOMER_NTN_FACT base 
    INNER JOIN PRD_NAP_USR_VWS.STORE_DIM locs
        ON base.store_num = locs.store_num
    INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dates
        ON base.business_day_date = dates.day_date
    WHERE locs.channel_num in ('110', '120', '130', '140', '210', '220', '250', '260', '310', '111', '121', '211', '221', '261', '311')
      AND dates.day_date >= (SELECT year_start_date FROM current_year)
      AND dates.day_date < (SELECT max_day_dt FROM max_dt_check)
    GROUP BY 1, 2, 3, 4, 5, 6
) WITH DATA
PRIMARY INDEX(yr_idnt, mth_idnt, chnl_label, country, banner, sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS PRIMARY INDEX (yr_idnt, mth_idnt, chnl_label, country, banner, sku_idnt)
, COLUMN (mth_idnt ,yr_idnt), COLUMN (sku_idnt) ON bipoc_ntn;

CREATE MULTISET VOLATILE TABLE bipoc_receipts
AS(
    SELECT
       	fiscal_year_num AS yr_idnt
        , month_idnt AS mth_idnt
        , TRIM(locs.channel_num) || ', ' || TRIM(locs.channel_desc) AS chnl_label
        , CASE WHEN locs.channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310') THEN 'US'
        	 WHEN locs.channel_num IN ('111', '121', '211', '221', '261', '311') THEN 'CA'
        	 ELSE NULL END AS country
        , CASE WHEN locs.channel_num IN ('110', '120', '130', '140', '111', '121', '310', '311') THEN 'FP'
    	     WHEN locs.channel_num IN ('211', '221', '261', '210', '220', '250', '260') THEN 'OP'
    	     ELSE NULL END AS banner
        , base.sku_idnt
        , CAST(0 AS DECIMAL(12,2)) as sales_u
        , CAST(0 AS DECIMAL(12,2)) AS sales_$
        , CAST(0 AS DECIMAL(12,2)) as return_u
        , CAST(0 AS DECIMAL(12,2)) as return_$
        , CAST(0 AS DECIMAL(12,2)) as demand_u
        , CAST(0 AS DECIMAL(12,2)) as demand_$
        , CAST(0 AS DECIMAL(12,2)) as eoh_u
        , CAST(0 AS DECIMAL(12,2)) as eoh_$
        --, CAST(0 AS DECIMAL(12,2)) AS eoh_cost
        , CAST(0 AS DECIMAL(12,2)) as ntn
        , SUM(CASE WHEN base.rcpt_typ_key IN (1, 2, 11, 7, 8) THEN base.rcpt_tot_units ELSE 0 END) AS receipt_u
        , SUM(CASE WHEN base.rcpt_typ_key IN (1, 2, 11, 7, 8) THEN base.rcpt_tot_retl$ ELSE 0 END) AS receipt_$
        , CAST(0 AS DECIMAL(12,2)) AS on_order_u
        , CAST(0 AS DECIMAL(12,2)) AS on_order_$
        , CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_u
        , CAST(0 AS DECIMAL(12,2)) AS on_order_4wk_$
    FROM prd_ma_bado.rcpt_sku_ld_rvw base
    INNER JOIN PRD_NAP_USR_VWS.STORE_DIM locs
        ON base.loc_idnt = locs.store_num
    INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dates
        ON base.day_idnt = dates.day_idnt
    WHERE locs.channel_num in ('110', '120', '130', '140', '210', '220', '250', '260', '310', '111', '121', '211', '221', '261', '311')
      AND dates.day_date >= (SELECT year_start_date FROM current_year)
      AND dates.day_date < (SELECT max_day_dt FROM max_dt_check)
    GROUP BY 1, 2, 3, 4, 5, 6
) WITH DATA
PRIMARY INDEX(yr_idnt, mth_idnt, chnl_label, country, banner, sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS PRIMARY INDEX (yr_idnt, mth_idnt, chnl_label, country, banner, sku_idnt)
, COLUMN (mth_idnt ,yr_idnt), COLUMN (sku_idnt) ON bipoc_receipts;

CREATE MULTISET VOLATILE TABLE bipoc_oo
AS(
  SELECT
       	fiscal_year_num AS yr_idnt
        , month_idnt AS mth_idnt
        , TRIM(locs.channel_num) || ', ' || TRIM(locs.channel_desc) AS chnl_label
        , CASE WHEN locs.channel_num IN ('110', '120', '130', '140', '210', '220', '250', '260', '310') THEN 'US'
        	 WHEN locs.channel_num IN ('111', '121', '211', '221', '261', '311') THEN 'CA'
        	 ELSE NULL END AS country
        , CASE WHEN locs.channel_num IN ('110', '120', '130', '140', '111', '121', '310', '311') THEN 'FP'
    	     WHEN locs.channel_num IN ('211', '221', '261', '210', '220', '250', '260') THEN 'OP'
    	     ELSE NULL END AS banner
        , base.sku_idnt
        , CAST(0 AS DECIMAL(12,2)) as sales_u
        , CAST(0 AS DECIMAL(12,2)) AS sales_$
        , CAST(0 AS DECIMAL(12,2)) as return_u
        , CAST(0 AS DECIMAL(12,2)) as return_$
        , CAST(0 AS DECIMAL(12,2)) as demand_u
        , CAST(0 AS DECIMAL(12,2)) as demand_$
        , CAST(0 AS DECIMAL(12,2)) as eoh_u
        , CAST(0 AS DECIMAL(12,2)) as eoh_$
        --, CAST(0 AS DECIMAL(12,2)) AS eoh_cost
        , CAST(0 AS DECIMAL(12,2)) as ntn
        , CAST(0 AS DECIMAL(12,2)) as receipt_u
        , CAST(0 AS DECIMAL(12,2)) as receipt_$
        , SUM(inv_oo_tot_open_units) AS on_order_u
        , SUM(inv_oo_tot_open_retl$) AS on_order_$
        , SUM(inv_oo_tot_4wk_lt_open_units) AS on_order_4wk_u
        , SUM(inv_oo_tot_4wk_lt_open_retl$) AS on_order_4wk_$

    FROM prd_ma_bado.inv_oo_sku_lw_dly_cvw base
    INNER JOIN PRD_NAP_USR_VWS.STORE_DIM locs
        ON base.loc_idnt = locs.store_num
    INNER JOIN 
    (
    SELECT week_idnt, fiscal_year_num, month_idnt FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
	WHERE day_date = CURRENT_DATE
    ) dates
        ON base.last_ld_wk_idnt = dates.week_idnt
    WHERE locs.channel_num in ('110', '120', '130', '140', '210', '220', '250', '260', '310', '111', '121', '211', '221', '261', '311')
      --and yr_idnt = 2021
    GROUP BY 1, 2, 3, 4, 5, 6
) WITH DATA
PRIMARY INDEX(yr_idnt, mth_idnt, chnl_label, country, banner, sku_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS PRIMARY INDEX (yr_idnt, mth_idnt, chnl_label, country, banner, sku_idnt)
, COLUMN (mth_idnt ,yr_idnt), COLUMN (sku_idnt) ON bipoc_oo;

CREATE MULTISET VOLATILE TABLE bipoc_hierarchy
AS(
	SELECT 
		sku_idnt
		, cc_idnt
		, div_idnt
		, div_label
		, grp_idnt
		, grp_label
		, dept_idnt
		, dept_label
		, class_idnt
		, class_label
		, sbclass_idnt
		, sbclass_label
		, supp_name
		, supp_idnt
		, brand_name
		, supp_npg_idnt
		, colr_label
		, vpn_label
		, black_idnt
		, latinx_idnt
		, asian_idnt
		, lgtbq_idnt
	FROM 
	(
		SELECT
		    skus.rms_sku_num AS sku_idnt 
		    , TRIM(skus.dept_num) || '-' || TRIM(skus.PRMY_SUPP_NUM) || '-' || TRIM(skus.supp_part_num) || '-' || COALESCE(TRIM(skus.color_num), 'NULL') || '-' || COALESCE(UPPER(TRIM(skus.supp_color)), 'NULL') || '-' || COALESCE(TRIM(skus.class_num), 'NULL') || '-' || COALESCE(TRIM(skus.sbclass_num), 'NULL') AS cc_idnt
		    , skus.div_num AS div_idnt
		    , TRIM(skus.div_num) || ', ' || TRIM(skus.div_desc) AS div_label
		    , skus.grp_num AS grp_idnt
		    , TRIM(skus.grp_num) || ', ' || TRIM(skus.grp_desc) AS grp_label
		    , skus.dept_num AS dept_idnt
		    , TRIM(skus.dept_num) || ', ' || TRIM(skus.dept_desc) AS dept_label
		    , skus.class_num AS class_idnt
		    , TRIM(skus.class_num) || ', ' || TRIM(skus.class_desc) AS class_label
		    , skus.sbclass_num AS sbclass_idnt
		    , TRIM(skus.sbclass_num) || ', ' || TRIM(skus.sbclass_desc)  AS sbclass_label
		    , supp.supp_name
		    , supp.supp_idnt
		    , supp.brand_name
		    , supp_npg_ind AS supp_npg_idnt
		    , skus.color_desc AS colr_label
		    , skus.supp_part_num || ', ' || skus.style_desc AS vpn_label
		    , MAX(black_ind) AS black_idnt
		    , MAX(latinx_ind) AS latinx_idnt
		    , MAX(asian_ind) AS asian_idnt
	        , MAX(lgtbq_ind) AS lgtbq_idnt
		  FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW skus
	      LEFT JOIN PRD_MA_BADO.SUPP_LKUP_VW supp
	      	ON supp.SUPP_IDNT = skus.PRMY_SUPP_NUM
		  INNER JOIN PRD_MA_DIGITALBI.bipoc_brand_lkp bi
		    ON supp.brand_name = bi.brand_name
		  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
	  ) a
	  QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_idnt, cc_idnt ORDER BY vpn_label) = 1
) WITH DATA
PRIMARY INDEX(sku_idnt, cc_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS PRIMARY INDEX (sku_idnt, cc_idnt)
, COLUMN (sku_idnt ,sbclass_label, class_idnt, div_idnt, grp_idnt, dept_idnt) ON bipoc_hierarchy;

CREATE MULTISET VOLATILE TABLE bipoc_all
AS(
		SELECT
		CURRENT_DATE AS run_date
	    , sku_base.yr_idnt
	    , mth_idnt
	    , chnl_label
	    , country
	    , sku_base.banner
	    , hier.div_label
	    , hier.div_idnt
	    , hier.grp_label
	    , hier.grp_idnt
	    , hier.dept_label
	    , hier.dept_idnt
	    , hier.class_label
	    , hier.class_idnt
	    , hier.sbclass_label
	    , hier.sbclass_idnt
	    , supp_name
	    , hier.supp_idnt
	    , brand_name
	    , supp_npg_idnt
	    , cc_idnt
	    , MAX(black_idnt) AS black_idnt
	    , MAX(latinx_idnt) AS latinx_idnt
	    , MAX(asian_idnt) AS asian_idnt
        , MAX(lgtbq_idnt) AS lgtbq_idnt
	    , SUM(sales_u) AS sales_u
	    , SUM(sales_$) AS sales_$
	    --, SUM(sales_cost) AS sales_cost
	    , SUM(return_u) AS return_u
	    , SUM(return_$) AS return_$
	    , SUM(demand_u) AS demand_u
	    , SUM(demand_$) AS demand_$
	    , SUM(eoh_u) AS eoh_u
	    , SUM(eoh_$) AS eoh_$
	    --, SUM(eoh_cost) AS eoh_cost
		, SUM(ntn) as ntn
	    , SUM(receipt_u) AS receipt_u
	    , SUM(receipt_$) AS receipt_$
	    , SUM(on_order_u) AS on_order_u
	    , SUM(on_order_$) AS on_order_$
	    , SUM(on_order_4wk_u) AS on_order_4wk_u
	    , SUM(on_order_4wk_$) AS on_order_4wk_$
	--    , SUM(sales_$) - SUM(sales_cost) AS margin_$
	--    , MAX(sales_target) AS yr_sale_target_$
	FROM 
	(
		SELECT
		    yr_idnt
		    , mth_idnt
		    , chnl_label
		    , country
		    , banner
		    , sku_idnt
		    , SUM(sales_u) AS sales_u
		    , SUM(sales_$) AS sales_$
		    , SUM(return_u) AS return_u
		    , SUM(return_$) AS return_$
		    , SUM(demand_u) AS demand_u
		    , SUM(demand_$) AS demand_$
		    , SUM(eoh_u) AS eoh_u
		    , SUM(eoh_$) AS eoh_$
		    --, SUM(eoh_cost) AS eoh_cost
		    , SUM(ntn) as ntn
		    , SUM(receipt_u) AS receipt_u
		    , SUM(receipt_$) AS receipt_$
		    , SUM(on_order_u) AS on_order_u
		    , SUM(on_order_$) AS on_order_$
		    , SUM(on_order_4wk_u) AS on_order_4wk_u
		    , SUM(on_order_4wk_$) AS on_order_4wk_$
		    --, CASE WHEN SUM(eoh_u) >0 THEN (SUM(eoh_cost)/SUM(eoh_u))*SUM(sales_u) ELSE 0 END AS sales_cost
		FROM 
		(
		    SELECT * FROM bipoc_sales_eoh
		    UNION ALL 
		   	SELECT * FROM bipoc_ntn
		    UNION ALL 
		    SELECT * FROM bipoc_receipts
		    UNION ALL  
		    SELECT * FROM bipoc_oo
		) bipoc_all
		GROUP BY 1, 2, 3, 4, 5, 6
	) sku_base
	LEFT JOIN bipoc_hierarchy hier
	ON sku_base.sku_idnt = hier.sku_idnt
	WHERE cc_idnt IS NOT NULL
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
) WITH DATA
PRIMARY INDEX(yr_idnt, mth_idnt, chnl_label, country, banner, cc_idnt)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS PRIMARY INDEX (yr_idnt, mth_idnt, chnl_label, country, banner, cc_idnt)
, COLUMN (cc_idnt, div_idnt, div_label, grp_idnt, grp_label, dept_idnt, dept_label, sbclass_idnt, sbclass_label, class_idnt, class_label) ON bipoc_all;

DELETE FROM {environment_schema}.diversity_reporting;
INSERT INTO {environment_schema}.diversity_reporting
	SELECT 
		CURRENT_DATE AS run_date
	    , COALESCE(t1.yr_idnt, t2.yr_idnt) AS yr_idnt
		, t1.mth_idnt AS mth_idnt
		, t1.chnl_label
		, t1.country AS country
		, COALESCE(t1.banner, t2.banner) AS banner
		, COALESCE(t1.div_label, t2.div_label) AS div_label
		, COALESCE(t1.div_idnt, LEFT(t2.div_label, POSITION(',' IN t2.div_label)-1)) AS div_idnt
		, COALESCE(t1.grp_label, t2.grp_label) AS grp_label
		, COALESCE(t1.grp_idnt, LEFT(t2.grp_label, POSITION(',' IN t2.grp_label)-1)) AS grp_idnt
		, COALESCE(t1.dept_label, t2.dept_label) AS dept_label
		, COALESCE(t1.dept_idnt, LEFT(t2.dept_label, POSITION(',' IN t2.dept_label)-1)) AS dept_idnt
		, t1.class_label
	    , t1.class_idnt
	    , t1.sbclass_label
	    , t1.sbclass_idnt
		, t1.supp_name AS supp_name
		, t1.supp_idnt
		, brand_name
		, supp_npg_idnt
		, cc_idnt
		, black_idnt
	    , latinx_idnt
	    , asian_idnt
	    , lgtbq_idnt
	    , SUM(COALESCE(sales_u,0)) AS sales_u
	    , SUM(COALESCE(sales_$,0)) AS sales_$
	    --, SUM(COALESCE(sales_cost,0)) AS sales_cost
	    , SUM(COALESCE(return_u,0)) AS return_u
	    , SUM(COALESCE(return_$,0)) AS return_$
	    , SUM(COALESCE(demand_u,0)) AS demand_u
	    , SUM(COALESCE(demand_$,0)) AS demand_$
	    , SUM(COALESCE(eoh_u,0)) AS eoh_u
	    , SUM(COALESCE(eoh_$,0)) AS eoh_$
	    --, SUM(COALESCE(eoh_cost,0)) AS eoh_cost
	    , SUM(COALESCE(ntn,0)) as ntn
	    , SUM(COALESCE(receipt_u,0)) AS receipt_u
	    , SUM(COALESCE(receipt_$,0)) AS receipt_$
	    , SUM(COALESCE(on_order_u,0)) AS on_order_u
	    , SUM(COALESCE(on_order_$,0)) AS on_order_$
	    , SUM(COALESCE(on_order_4wk_u,0)) AS on_order_4wk_u
	    , SUM(COALESCE(on_order_4wk_$,0)) AS on_order_4wk_$
		, MAX(sales_target) AS yr_sale_target_$
		--, MAX(sales_plan) AS mth_sale_target_$
	FROM bipoc_all t1
	FULL OUTER JOIN prd_ma_digitalbi.bipoc_dept_targets t2
	ON t1.yr_idnt = t2.yr_idnt
	AND t1.banner = t2.banner
	AND t1.div_label = t2.div_label
	AND t1.grp_label = t2.grp_label
	AND t1.dept_label = t2.dept_label
	WHERE t2.yr_idnt >= (SELECT fiscal_year_num FROM max_dt_check)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
;

COLLECT STATISTICS PRIMARY INDEX (yr_idnt, mth_idnt, chnl_label, country, banner, cc_idnt)
, COLUMN (cc_idnt, div_idnt, grp_idnt, dept_idnt, sbclass_idnt, class_idnt) 
, COLUMN (yr_idnt, banner, div_label, grp_label, dept_label)
, COLUMN (yr_idnt)ON {environment_schema}.diversity_reporting;
