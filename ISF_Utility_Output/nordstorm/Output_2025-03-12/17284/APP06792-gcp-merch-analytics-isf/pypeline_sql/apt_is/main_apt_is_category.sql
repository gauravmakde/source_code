/*
APT In Season Category Main
Author: Asiyah Fox
Date Created: 1/5/23
Date Last Updated: 6/8/23

Datalab: t2dl_das_apt_cost_reporting
Deletes and Inserts into Table: apt_is_category
*/

/************************************************************************************/
/****************************** 1.DATE LOOKUPS **************************************/
/************************************************************************************/
--purposely includes overlap of LY MTD, LY MTH, and TY for one month--needed for Tableau


--DROP TABLE date_lkup;
CREATE MULTISET VOLATILE TABLE date_lkup AS (
SELECT DISTINCT
	dt.date_ind
	,dt.week_end_day_date
	,dt.week_idnt AS week_idnt_true
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN ly.week_idnt-100 
		ELSE dt.week_idnt 
		END AS week_idnt
	,ty.month_idnt AS month_idnt_true
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN ly.month_idnt-100
		ELSE ty.month_idnt 
		END AS month_idnt
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN ly.month_idnt
		ELSE ty.month_idnt
    	END AS month_idnt_aligned
    ,ty.month_label AS spend_month_true
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN TRIM(ly.fiscal_year_num-1)|| ' ' || TRIM(ly.month_abrv) 
		ELSE ty.month_label 
		END AS spend_month
	,TRIM(ty.fiscal_year_num) || ' ' || TRIM(ty.fiscal_month_num) || ' ' || TRIM(ty.month_abrv) AS month_label_true 	
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN TRIM(ly.fiscal_year_num-1) || ' ' || TRIM(ly.fiscal_month_num) || ' ' || TRIM(ly.month_abrv) 
		ELSE TRIM(ty.fiscal_year_num) || ' ' || TRIM(ty.fiscal_month_num) || ' ' || TRIM(ty.month_abrv)
		END AS month_label	
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN TRIM(ly.fiscal_year_num) || ' ' || TRIM(ly.fiscal_month_num) || ' ' || TRIM(ly.month_abrv) 
		ELSE TRIM(ty.fiscal_year_num) || ' ' || TRIM(ty.fiscal_month_num) || ' ' || TRIM(ty.month_abrv)
    	END AS month_label_aligned
	,ty.month_start_day_date AS month_start_day_date_true
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN MIN(dt.week_start_day_date) OVER (PARTITION BY dt.date_ind, ly.month_idnt)
		ELSE ty.month_start_day_date
		END AS month_start_day_date
    ,ty.month_end_day_date AS month_end_day_date_true
    ,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN MAX(dt.week_end_day_date) OVER (PARTITION BY dt.date_ind, ly.month_idnt)
		ELSE ty.month_end_day_date
		END AS month_end_day_date
	,MAX(ty.week_end_day_date) OVER (PARTITION BY dt.date_ind, ty.month_idnt) AS mtd_end_date_true 
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN MAX(dt.week_end_day_date) OVER (PARTITION BY dt.date_ind, ly.month_idnt) 
		ELSE MAX(ty.week_end_day_date) OVER (PARTITION BY dt.date_ind, ty.month_idnt)
		END AS mtd_end_date
	,MAX(ty.week_idnt) OVER (PARTITION BY dt.date_ind, month_idnt_aligned) AS mtd_week_idnt_true
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN MAX(ly.week_idnt-100) OVER (PARTITION BY dt.date_ind, month_idnt_aligned) 
		ELSE MAX(ty.week_idnt) OVER (PARTITION BY dt.date_ind, month_idnt_aligned) 
		END AS mtd_week_idnt
	,MIN(ty.week_idnt) OVER (PARTITION BY dt.date_ind, month_idnt_aligned) AS mbeg_week_idnt_true
	,CASE WHEN dt.date_ind IN ('LY', 'LY MTH') THEN MIN(ly.week_idnt-100) OVER (PARTITION BY dt.date_ind, month_idnt_aligned) 
		ELSE MIN(ty.week_idnt) OVER (PARTITION BY dt.date_ind, month_idnt_aligned) 
		END AS mbeg_week_idnt
FROM
	(
	SELECT DISTINCT
		'PL MTH' AS date_ind
		,week_idnt
		,week_end_day_date
		,week_start_day_date
	FROM prd_nap_usr_vws.day_cal_454_dim
	WHERE month_idnt
		BETWEEN 
			(SELECT MAX(month_idnt)-100 FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
		AND
			(SELECT DISTINCT month_idnt+100
			FROM prd_nap_usr_vws.day_cal_454_dim 
			WHERE day_date =
				(SELECT MIN(day_date)-1
				FROM prd_nap_usr_vws.day_cal_454_dim 
				WHERE month_idnt = (SELECT MAX(month_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)))
		AND month_idnt >= 202211
	UNION ALL
	SELECT DISTINCT
		'PL MTD' AS date_ind
		,week_idnt
		,week_end_day_date
		,week_start_day_date
	FROM prd_nap_usr_vws.day_cal_454_dim
	WHERE week_idnt 
		BETWEEN
			(SELECT MIN(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE month_idnt =
				(SELECT MAX(month_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))
		AND
			(SELECT MAX(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
	UNION ALL
	SELECT DISTINCT
		'TY' AS date_ind
		,week_idnt
		,week_end_day_date
		,week_start_day_date
	FROM prd_nap_usr_vws.day_cal_454_dim
	WHERE week_idnt
		BETWEEN 
			(SELECT MIN(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE month_idnt = (SELECT MAX(month_idnt)-100 FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))
		AND
			(SELECT MAX(week_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)
	UNION ALL
	SELECT DISTINCT
		'LY' AS date_ind
		,week_idnt
		,week_end_day_date
		,week_start_day_date
	FROM prd_nap_usr_vws.day_cal_454_dim
	WHERE day_date 
		BETWEEN 
			(SELECT MIN(day_date_last_year_realigned) 
			FROM prd_nap_usr_vws.day_cal_454_dim 
			WHERE month_idnt = (SELECT MAX(month_idnt)-100 FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))
		AND
			(SELECT DISTINCT day_date_last_year_realigned
			FROM prd_nap_usr_vws.day_cal_454_dim
			WHERE day_date = (SELECT MAX(week_end_day_date) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))		
	UNION ALL
	SELECT DISTINCT
		'LY MTH' AS date_ind
		,week_idnt
		,week_end_day_date
		,week_start_day_date 
	FROM prd_nap_usr_vws.day_cal_454_dim
	WHERE 
	week_idnt 
		BETWEEN
			(SELECT DISTINCT week_idnt
			FROM prd_nap_usr_vws.day_cal_454_dim
			WHERE day_date = 
				(SELECT DISTINCT day_date_last_year_realigned
				FROM prd_nap_usr_vws.day_cal_454_dim
				WHERE day_date = (SELECT MAX(month_start_day_date) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE)))
		AND 
			(SELECT DISTINCT week_idnt
			FROM prd_nap_usr_vws.day_cal_454_dim
			WHERE day_date = (SELECT MAX(month_end_day_date) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE))				
	) dt
LEFT JOIN prd_nap_usr_vws.day_cal_454_dim ty
	ON dt.week_end_day_date = ty.day_date
LEFT JOIN prd_nap_usr_vws.day_cal_454_dim ly
	ON ty.day_date = ly.day_date_last_year_realigned
--WHERE month_idnt_aligned BETWEEN 202302 AND 202304 --testing
--WHERE ty.month_idnt BETWEEN 202211 AND 202303 --fiscal realigned testing
) WITH DATA
PRIMARY INDEX (date_ind, week_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, week_idnt)
		ON date_lkup;
;

--OPTIMIZING ACTUALS FILTERING
--DROP TABLE date_lkup_acts;
CREATE MULTISET VOLATILE TABLE date_lkup_acts AS (
	SELECT *
	FROM date_lkup
	WHERE date_ind IN ('TY','LY','LY MTH')
) WITH DATA
	PRIMARY INDEX (week_idnt)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (week_idnt)
		ON date_lkup_acts;
;
--OPTIMIZING INVENTORY FILTERING
--DROP TABLE date_lkup_acts_ty;
CREATE MULTISET VOLATILE TABLE date_lkup_acts_ty AS (
	SELECT *
	FROM date_lkup
	WHERE date_ind IN ('TY')
) WITH DATA
	PRIMARY INDEX (week_idnt)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (week_idnt)
		ON date_lkup_acts_ty;
;

--OPTIMIZING INVENTORY FILTERING
--DROP TABLE date_lkup_acts_ly;
CREATE MULTISET VOLATILE TABLE date_lkup_acts_ly AS (
	SELECT *
	FROM date_lkup
	WHERE date_ind IN ('LY','LY MTH')
) WITH DATA
	PRIMARY INDEX (week_idnt)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (week_idnt)
		ON date_lkup_acts_ly;
;

--department lookup
--DROP TABLE dept_lkup;
CREATE MULTISET VOLATILE TABLE dept_lkup AS (
SELECT DISTINCT
    dept_num
	,division_num
	,subdivision_num
	,TRIM (dept_num || ', ' ||dept_name) AS department_desc
	,TRIM (division_num || ', ' ||division_name) AS division_desc
	,TRIM (subdivision_num || ', ' ||subdivision_name) AS subdivision_desc
	,active_store_ind
FROM prd_nap_usr_vws.department_dim
)WITH DATA
	PRIMARY INDEX (dept_num)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (dept_num)
		ON dept_lkup;


--category lookup
--DROP TABLE cat_lkup;
CREATE MULTISET VOLATILE TABLE cat_lkup AS
(SELECT DISTINCT
	psd.dept_num
	,psd.rms_sku_num
	,psd.channel_country
	,COALESCE(cat1.category                  ,cat2.category                  ,'OTHER') AS category
	,COALESCE(cat1.category_planner_1        ,cat2.category_planner_1        ,'OTHER') AS category_planner_1          --ATTR 1
	,COALESCE(cat1.category_planner_2        ,cat2.category_planner_2        ,'OTHER') AS category_planner_2          --ATTR 2
	,COALESCE(cat1.category_group            ,cat2.category_group            ,'OTHER') AS category_group              --ATTR 3
	,COALESCE(cat1.seasonal_designation      ,cat2.seasonal_designation      ,'OTHER') AS seasonal_designation        --ATTR 4
	,COALESCE(cat1.rack_merch_zone           ,cat2.rack_merch_zone           ,'OTHER') AS rack_merch_zone             --ATTR 5
	,COALESCE(cat1.is_activewear             ,cat2.is_activewear             ,'OTHER') AS is_activewear               --ATTR 6
	,COALESCE(cat1.channel_category_roles_1  ,cat2.channel_category_roles_1  ,'OTHER') AS channel_category_roles_1    --ATTR 7
	,COALESCE(cat1.channel_category_roles_2  ,cat2.channel_category_roles_2  ,'OTHER') AS channel_category_roles_2    --ATTR 8
	,COALESCE(cat1.bargainista_dept_map      ,cat2.bargainista_dept_map      ,'OTHER') AS bargainista_dept_map        --ATTR 9
FROM prd_nap_usr_vws.product_sku_dim_vw psd
	LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim cat1
		ON psd.dept_num = cat1.dept_num
		AND psd.class_num = cat1.class_num
		AND psd.sbclass_num = cat1.sbclass_num
	LEFT JOIN prd_nap_usr_vws.catg_subclass_map_dim cat2
		ON psd.dept_num = cat2.dept_num
		AND psd.class_num = cat2.class_num
		AND cat2.sbclass_num = -1
) WITH DATA
	PRIMARY INDEX (dept_num, rms_sku_num)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (dept_num, rms_sku_num)
		ON cat_lkup;

--category attributes
--DROP TABLE cat_attr;
CREATE MULTISET VOLATILE TABLE cat_attr AS
(
SELECT DISTINCT
	category
	,dept_num
	,category_planner_1
	,category_planner_2
	,category_group
	,seasonal_designation
	,rack_merch_zone
	,is_activewear
	,channel_category_roles_1
	,channel_category_roles_2
	,bargainista_dept_map
FROM prd_nap_usr_vws.catg_subclass_map_dim
--FROM cat_lkup
) WITH DATA
	PRIMARY INDEX (category, dept_num)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (category, dept_num)
		ON cat_attr;


/************************************************************************************/
/******************************** 3.STORE LOOKUPS ***********************************/
/************************************************************************************/

--supplier store_channel lookup
--DROP TABLE store_lkup;
CREATE MULTISET VOLATILE TABLE store_lkup AS (
SELECT DISTINCT
    store_num
	,channel_country
	,channel_num
	,TRIM (channel_num || ', ' ||channel_desc) AS channel_label
	,channel_brand AS banner
FROM prd_nap_usr_vws.price_store_dim_vw
WHERE channel_num NOT IN (580)
)WITH DATA
PRIMARY INDEX (store_num, channel_num) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (store_num, channel_num)
		ON store_lkup;
	
--inventory store_channel lookup
--DROP TABLE store_lkup_eop;
CREATE MULTISET VOLATILE TABLE store_lkup_eop AS (
SELECT *
FROM store_lkup
WHERE channel_num IN (110,111,120,121,210,211,250,260,261,310,311)
)WITH DATA
	PRIMARY INDEX (store_num) 
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (store_num) 
		ON store_lkup_eop;	

--N category_store_channel_cluster lookup
--DROP TABLE store_lkup_nord;
CREATE MULTISET VOLATILE TABLE store_lkup_nord AS (
SELECT DISTINCT
    store_num
	,channel_country
	,channel_num
	,channel_label
	,banner
FROM store_lkup
WHERE banner = 'NORDSTROM'
)WITH DATA
PRIMARY INDEX (store_num, channel_num) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (store_num, channel_num)
		ON store_lkup_nord;

--NR category_store_channel_cluster lookup
--DROP TABLE store_lkup_rack;
CREATE MULTISET VOLATILE TABLE store_lkup_rack AS (
SELECT DISTINCT
    store_num
	,channel_country
	,channel_num
	,channel_label
	,banner
FROM store_lkup
WHERE banner = 'NORDSTROM_RACK'
)WITH DATA
PRIMARY INDEX (store_num, channel_num) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (store_num, channel_num)
		ON store_lkup_rack;
		
/************************************************************************************/
/******************************** 4.PRICE BANDS *************************************/
/************************************************************************************/

-- N receipted price band
--DROP TABLE receipt_price_band_nord;
CREATE MULTISET VOLATILE TABLE receipt_price_band_nord AS (
SELECT
	rms_sku_num AS sku_idnt
	,channel_country
	,MAX((RECEIPTS_REGULAR_RETAIL + RECEIPTS_CLEARANCE_RETAIL + RECEIPTS_CROSSDOCK_REGULAR_RETAIL + RECEIPTS_CROSSDOCK_CLEARANCE_RETAIL )
    /
    (RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS)) AS AUR
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW a --UPDATED
	JOIN store_lkup_nord b
	ON a.store_num = b.store_num
	JOIN dept_lkup dept
	ON dept.dept_num = a.department_num
	WHERE (RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS) > 0
	GROUP BY 1,2
)WITH DATA
PRIMARY INDEX (sku_idnt, channel_country) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (sku_idnt, channel_country)
		ON receipt_price_band_nord;

-- NR receipted price band
--DROP TABLE receipt_price_band_rack;
CREATE MULTISET VOLATILE TABLE receipt_price_band_rack AS (
SELECT
	sku_idnt
	,channel_country
	,MAX(AUR) AS AUR
FROM(
	SELECT
		rms_sku_num AS sku_idnt
		,channel_country
		,MAX((RECEIPTS_REGULAR_RETAIL + RECEIPTS_CLEARANCE_RETAIL + RECEIPTS_CROSSDOCK_REGULAR_RETAIL + RECEIPTS_CROSSDOCK_CLEARANCE_RETAIL )
	    /
	    (RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS)) AS AUR
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW a --UPDATED
	JOIN store_lkup_rack b
		ON a.store_num = b.store_num
	JOIN dept_lkup dept
		ON dept.dept_num = a.department_num
	WHERE (RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS) > 0
	GROUP BY 1,2
	UNION ALL
	SELECT TSF.rms_sku_num AS sku_idnt
		,channel_country
		,MAX(PACKANDHOLD_TRANSFER_IN_RETAIL / PACKANDHOLD_TRANSFER_IN_UNITS) AS AUR
	FROM PRD_NAP_USR_VWS.MERCH_TRANSFER_SKU_LOC_WEEK_AGG_FACT_VW TSF --UPDATED
	JOIN store_lkup_rack b
		ON TSF.store_num = b.store_num
	JOIN dept_lkup dept
		ON dept.dept_num = TSF.department_num
	WHERE PACKANDHOLD_TRANSFER_IN_UNITS > 0
	GROUP BY 1, 2
UNION ALL
	SELECT TSF.rms_sku_num AS sku_idnt
		,channel_country
		,MAX(RACKING_TRANSFER_IN_RETAIL / RACKING_TRANSFER_IN_UNITS) AS AUR
	FROM PRD_NAP_USR_VWS.MERCH_TRANSFER_SKU_LOC_WEEK_AGG_FACT_VW TSF --UPDATED
	JOIN store_lkup_rack b
		ON TSF.store_num = b.store_num
	JOIN dept_lkup dept
		ON dept.dept_num = TSF.department_num
	WHERE RACKING_TRANSFER_IN_UNITS > 0
	GROUP BY 1, 2
) TBL
GROUP BY 1,2
)WITH DATA
PRIMARY INDEX (sku_idnt, channel_country) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (sku_idnt, channel_country)
		ON receipt_price_band_rack;

/************************************************************************************/
/************************************************************************************/
/************************************ 5.PLAN ****************************************/
/************************************************************************************/
/************************************************************************************/

--PLANS
--DROP TABLE plan;
CREATE MULTISET VOLATILE TABLE plan AS
(
SELECT
	fct.country AS channel_country
	,fct.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,CAST(fct.dept_idnt AS INTEGER) AS department_num
	,CASE WHEN fct.fulfill_type_num = 1 THEN 'Y' ELSE 'N' END AS dropship_ind
	,fct.category
	,fct.price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,SUM(CASE WHEN dt.week_idnt = dt.mbeg_week_idnt
		THEN fct.plan_bop_c_dollars
		ELSE 0 END) AS plan_bop_c
	,SUM(CASE WHEN dt.week_idnt = dt.mbeg_week_idnt
		THEN fct.plan_bop_c_units
		ELSE 0 END) AS plan_bop_u
	,SUM(CASE WHEN dt.week_end_day_date = dt.mtd_end_date
		THEN fct.plan_eop_c_dollars
		ELSE 0 END) AS plan_eop_c
	,SUM(CASE WHEN dt.week_end_day_date = dt.mtd_end_date
		THEN fct.plan_eop_c_units
		ELSE 0 END) AS plan_eop_u
	,SUM(fct.rcpt_need_c                                                                    ) AS plan_receipts_c
	,SUM(fct.rcpt_need_u                                                                    ) AS plan_receipts_u
	,SUM(fct.rcpt_need_lr_c                                                                 ) AS plan_receipts_lr_c
	,SUM(fct.rcpt_need_lr_u                                                                 ) AS plan_receipts_lr_u
	,SUM(fct.net_sls_c_dollars                                                              ) AS plan_sales_c
	,SUM(fct.net_sls_r_dollars                                                              ) AS plan_sales_r
	,SUM(fct.net_sls_units                                                                  ) AS plan_sales_u
	,SUM(fct.demand_r_dollars                                                               ) AS plan_demand_r
	,SUM(fct.demand_units                                                                   ) AS plan_demand_u
FROM t2dl_das_apt_cost_reporting.category_channel_cost_plans_weekly fct
    JOIN date_lkup dt
    	ON fct.week_idnt = dt.week_idnt
    	AND dt.date_ind IN ('PL MTD', 'PL MTH')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
)WITH DATA
PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON plan;

/************************************************************************************/
/************************************************************************************/
/********************************* 6.ACTUALS ****************************************/
/************************************************************************************/
/************************************************************************************/

/************************************************************************************/
/********************************** 6.A.SALES ***************************************/
/************************************************************************************/


--DROP TABLE sales;
CREATE MULTISET VOLATILE TABLE sales AS
(
--SALES NORDSTROM
SELECT
	st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,fct.dropship_ind
	,cat.category
    ,CASE
	    WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
	    WHEN pb.aur <= 25 THEN '$0-25'
	    WHEN pb.aur <= 50 THEN '$25-50'
	    WHEN pb.aur <= 100 THEN '$50-100'
	    WHEN pb.aur <= 150 THEN '$100-150'
	    WHEN pb.aur <= 200 THEN '$150-200'
	    WHEN pb.aur <= 300 THEN '$200-300'
	    WHEN pb.aur <= 500 THEN '$300-500'
	    WHEN pb.aur <= 700 THEN '$500-700'
	    WHEN pb.aur <= 1000 THEN '$700-1000'
	    WHEN pb.aur <= 1500 THEN '$1000-1500'
	    WHEN pb.aur <= 1000000000 THEN 'Over $1500'
	    ELSE 'NO RECEIPT FOUND'
	    END AS price_band
	,SUM(fct.net_sales_tot_retl) AS net_sls_r
	,SUM(fct.net_sales_tot_cost) AS net_sls_c
	,SUM(fct.net_sales_tot_units) AS net_sls_units
	,SUM(fct.net_sales_tot_regular_retl) AS net_sls_reg_r
	,SUM(fct.net_sales_tot_regular_cost) AS net_sls_reg_c
	,SUM(fct.net_sales_tot_regular_units) AS net_sls_reg_units
	,SUM(COALESCE(fct.gross_sales_tot_retl,0) - COALESCE(fct.net_sales_tot_retl,0)) AS returns_r
	,SUM(COALESCE(fct.gross_sales_tot_cost,0) - COALESCE(fct.net_sales_tot_cost,0)) AS returns_c
	,SUM(COALESCE(fct.gross_sales_tot_units,0) - COALESCE(fct.net_sales_tot_units,0)) AS returns_u
	,SUM(CASE WHEN st.channel_num IN (110,111) THEN fct.gross_sales_tot_retl ELSE 0 END) AS demand_ttl_r
	,SUM(CASE WHEN st.channel_num IN (110,111) THEN fct.gross_sales_tot_units ELSE 0 END) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw fct
    JOIN date_lkup_acts dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_nord st
		ON fct.store_num = st.store_num
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND st.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_nord pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
UNION ALL
--SALES RACK
SELECT
	st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,fct.dropship_ind
	,cat.category
    ,CASE
		WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
		WHEN pb.aur <= 10 THEN '$0-10'
		WHEN pb.aur <= 15 THEN '$10-15'
		WHEN pb.aur <= 20 THEN '$15-20'
		WHEN pb.aur <= 25 THEN '$20-25'
		WHEN pb.aur <= 30 THEN '$25-30'
		WHEN pb.aur <= 40 THEN '$30-40'
		WHEN pb.aur <= 50 THEN '$40-50'
		WHEN pb.aur <= 60 THEN '$50-60'
		WHEN pb.aur <= 80 THEN '$60-80'
		WHEN pb.aur <= 100 THEN '$80-100'
		WHEN pb.aur <= 150 THEN '$100-150'
		WHEN pb.aur <= 200 THEN '$150-200'
		WHEN pb.aur <= 1000000000 THEN 'Over $200'
		ELSE 'NO RECEIPT FOUND'
	    END  AS price_band
	,SUM(fct.net_sales_tot_retl) AS net_sls_r
	,SUM(fct.net_sales_tot_cost) AS net_sls_c
	,SUM(fct.net_sales_tot_units) AS net_sls_units
	,SUM(fct.net_sales_tot_regular_retl) AS net_sls_reg_r
	,SUM(fct.net_sales_tot_regular_cost) AS net_sls_reg_c
	,SUM(fct.net_sales_tot_regular_units) AS net_sls_reg_units
	,SUM(COALESCE(fct.gross_sales_tot_retl,0) - COALESCE(fct.net_sales_tot_retl,0)) AS returns_r
	,SUM(COALESCE(fct.gross_sales_tot_cost,0) - COALESCE(fct.net_sales_tot_cost,0)) AS returns_c
	,SUM(COALESCE(fct.gross_sales_tot_units,0) - COALESCE(fct.net_sales_tot_units,0)) AS returns_u
	,SUM(CASE WHEN st.channel_num IN (210,211) THEN fct.gross_sales_tot_retl ELSE 0 END) AS demand_ttl_r
	,SUM(CASE WHEN st.channel_num IN (210,211) THEN fct.gross_sales_tot_units ELSE 0 END) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw fct
    JOIN date_lkup_acts dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_rack st
		ON fct.store_num = st.store_num
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND st.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_rack pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON sales;

/************************************************************************************/
/******************************* 6.B.DEMAND *****************************************/
/************************************************************************************/

--DROP TABLE demand;
CREATE MULTISET VOLATILE TABLE demand AS
(
--DEMAND NORDSTROM
SELECT
	st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,CASE WHEN fulfill_type_code = 'DS' THEN 'Y' ELSE 'N' END AS dropship_ind
	,cat.category
	,CASE
	    WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
	    WHEN pb.aur <= 25 THEN '$0-25'
	    WHEN pb.aur <= 50 THEN '$25-50'
	    WHEN pb.aur <= 100 THEN '$50-100'
	    WHEN pb.aur <= 150 THEN '$100-150'
	    WHEN pb.aur <= 200 THEN '$150-200'
	    WHEN pb.aur <= 300 THEN '$200-300'
	    WHEN pb.aur <= 500 THEN '$300-500'
	    WHEN pb.aur <= 700 THEN '$500-700'
	    WHEN pb.aur <= 1000 THEN '$700-1000'
	    WHEN pb.aur <= 1500 THEN '$1000-1500'
	    WHEN pb.aur <= 1000000000 THEN 'Over $1500'
	    ELSE 'NO RECEIPT FOUND'
	    END AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,SUM(CASE WHEN st.channel_num NOT IN (110,111,210,211) THEN fct.demand_tot_amt ELSE 0 END) AS demand_ttl_r
	,SUM(CASE WHEN st.channel_num NOT IN (110,111,210,211) THEN fct.demand_tot_qty ELSE 0 END) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM prd_nap_usr_vws.merch_demand_sku_store_week_agg_fact_vw fct
    JOIN date_lkup_acts dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_nord st
		ON fct.store_num = st.store_num
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND st.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_nord pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
UNION ALL
--DEMAND RACK
SELECT
	st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,CASE WHEN fulfill_type_code = 'DS' THEN 'Y' ELSE 'N' END AS dropship_ind
	,cat.category
	,CASE
		WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
		WHEN pb.aur <= 10 THEN '$0-10'
		WHEN pb.aur <= 15 THEN '$10-15'
		WHEN pb.aur <= 20 THEN '$15-20'
		WHEN pb.aur <= 25 THEN '$20-25'
		WHEN pb.aur <= 30 THEN '$25-30'
		WHEN pb.aur <= 40 THEN '$30-40'
		WHEN pb.aur <= 50 THEN '$40-50'
		WHEN pb.aur <= 60 THEN '$50-60'
		WHEN pb.aur <= 80 THEN '$60-80'
		WHEN pb.aur <= 100 THEN '$80-100'
		WHEN pb.aur <= 150 THEN '$100-150'
		WHEN pb.aur <= 200 THEN '$150-200'
		WHEN pb.aur <= 1000000000 THEN 'Over $200'
		ELSE 'NO RECEIPT FOUND'
	    END  AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,SUM(CASE WHEN st.channel_num NOT IN (110,111,210,211) THEN fct.demand_tot_amt ELSE 0 END) AS demand_ttl_r
	,SUM(CASE WHEN st.channel_num NOT IN (110,111,210,211) THEN fct.demand_tot_qty ELSE 0 END) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM prd_nap_usr_vws.merch_demand_sku_store_week_agg_fact_vw fct
    JOIN date_lkup_acts dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_rack st
		ON fct.store_num = st.store_num
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND st.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_rack pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON demand;

/************************************************************************************/
/******************************** 6.C.RECEIPTS **************************************/
/************************************************************************************/

--DROP TABLE receipts;
CREATE MULTISET VOLATILE TABLE receipts AS
(
--RECEIPTS NORDSTROM
SELECT
	st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,fct.dropship_ind
	,cat.category
	,CASE
	    WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
	    WHEN pb.aur <= 25 THEN '$0-25'
	    WHEN pb.aur <= 50 THEN '$25-50'
	    WHEN pb.aur <= 100 THEN '$50-100'
	    WHEN pb.aur <= 150 THEN '$100-150'
	    WHEN pb.aur <= 200 THEN '$150-200'
	    WHEN pb.aur <= 300 THEN '$200-300'
	    WHEN pb.aur <= 500 THEN '$300-500'
	    WHEN pb.aur <= 700 THEN '$500-700'
	    WHEN pb.aur <= 1000 THEN '$700-1000'
	    WHEN pb.aur <= 1500 THEN '$1000-1500'
	    WHEN pb.aur <= 1000000000 THEN 'Over $1500'
	    ELSE 'NO RECEIPT FOUND'
	    END AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,SUM(CASE WHEN st.channel_num NOT IN ('930','220','221') --these are only rack warehouses and NQC
		THEN fct.receipts_regular_cost
			+ fct.receipts_clearance_cost
			+ fct.receipts_crossdock_regular_cost
			+ fct.receipts_crossdock_clearance_cost
			ELSE 0 END) AS ttl_porcpt_c
	,SUM(CASE WHEN st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_units
			+ fct.receipts_clearance_units
			+ fct.receipts_crossdock_regular_units
			+ fct.receipts_crossdock_clearance_units
			ELSE 0 END) AS ttl_porcpt_u
	,SUM(CASE WHEN rp_ind = 'Y' AND st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_cost
			+ fct.receipts_clearance_cost
			+ fct.receipts_crossdock_regular_cost
			+ fct.receipts_crossdock_clearance_cost
			ELSE 0 END) AS ttl_porcpt_rp_c
	,SUM(CASE WHEN rp_ind = 'Y' AND st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_units
			+ fct.receipts_clearance_units
			+ fct.receipts_crossdock_regular_units
			+ fct.receipts_crossdock_clearance_units
			ELSE 0 END) AS ttl_porcpt_rp_u
	,SUM(CASE WHEN rp_ind = 'N' AND st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_cost
			+ fct.receipts_clearance_cost
			+ fct.receipts_crossdock_regular_cost
			+ fct.receipts_crossdock_clearance_cost
			ELSE 0 END) AS ttl_porcpt_nrp_c
	,SUM(CASE WHEN rp_ind = 'N' AND st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_units
			+ fct.receipts_clearance_units
			+ fct.receipts_crossdock_regular_units
			+ fct.receipts_crossdock_clearance_units
			ELSE 0 END) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM prd_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw fct
    JOIN date_lkup_acts dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_nord st
		ON fct.store_num = st.store_num
		AND st.channel_num IN (110,111,120,121,310,311)
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND st.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_nord pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
UNION ALL
--RECEIPTS RACK
SELECT
	st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,fct.dropship_ind
	,cat.category
	,CASE
		WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
		WHEN pb.aur <= 10 THEN '$0-10'
		WHEN pb.aur <= 15 THEN '$10-15'
		WHEN pb.aur <= 20 THEN '$15-20'
		WHEN pb.aur <= 25 THEN '$20-25'
		WHEN pb.aur <= 30 THEN '$25-30'
		WHEN pb.aur <= 40 THEN '$30-40'
		WHEN pb.aur <= 50 THEN '$40-50'
		WHEN pb.aur <= 60 THEN '$50-60'
		WHEN pb.aur <= 80 THEN '$60-80'
		WHEN pb.aur <= 100 THEN '$80-100'
		WHEN pb.aur <= 150 THEN '$100-150'
		WHEN pb.aur <= 200 THEN '$150-200'
		WHEN pb.aur <= 1000000000 THEN 'Over $200'
		ELSE 'NO RECEIPT FOUND'
	    END  AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,SUM(CASE WHEN st.channel_num NOT IN ('930','220','221') --these are only rack warehouses and NQC
		THEN fct.receipts_regular_cost
			+ fct.receipts_clearance_cost
			+ fct.receipts_crossdock_regular_cost
			+ fct.receipts_crossdock_clearance_cost
			ELSE 0 END) AS ttl_porcpt_c
	,SUM(CASE WHEN st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_units
			+ fct.receipts_clearance_units
			+ fct.receipts_crossdock_regular_units
			+ fct.receipts_crossdock_clearance_units
			ELSE 0 END) AS ttl_porcpt_u
	,SUM(CASE WHEN rp_ind = 'Y' AND st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_cost
			+ fct.receipts_clearance_cost
			+ fct.receipts_crossdock_regular_cost
			+ fct.receipts_crossdock_clearance_cost
			ELSE 0 END) AS ttl_porcpt_rp_c
	,SUM(CASE WHEN rp_ind = 'Y' AND st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_units
			+ fct.receipts_clearance_units
			+ fct.receipts_crossdock_regular_units
			+ fct.receipts_crossdock_clearance_units
			ELSE 0 END) AS ttl_porcpt_rp_u
	,SUM(CASE WHEN rp_ind = 'N' AND st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_cost
			+ fct.receipts_clearance_cost
			+ fct.receipts_crossdock_regular_cost
			+ fct.receipts_crossdock_clearance_cost
			ELSE 0 END) AS ttl_porcpt_nrp_c
	,SUM(CASE WHEN rp_ind = 'N' AND st.channel_num NOT IN ('930','220','221')
		THEN fct.receipts_regular_units
			+ fct.receipts_clearance_units
			+ fct.receipts_crossdock_regular_units
			+ fct.receipts_crossdock_clearance_units
			ELSE 0 END) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM prd_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw fct
    JOIN date_lkup_acts dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_rack st
		ON fct.store_num = st.store_num
		AND st.channel_num IN (210,211,250,260,261)
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND st.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_rack pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON receipts;


/************************************************************************************/
/******************************** 6.D.TRANSFERS *************************************/
/************************************************************************************/

--DROP TABLE transfers;
CREATE MULTISET VOLATILE TABLE transfers AS
(
--TRANSFERS NORD
SELECT
	st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,CAST(NULL AS CHAR(1)) AS dropship_ind
	,cat.category
	,CASE
	    WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
	    WHEN pb.aur <= 25 THEN '$0-25'
	    WHEN pb.aur <= 50 THEN '$25-50'
	    WHEN pb.aur <= 100 THEN '$50-100'
	    WHEN pb.aur <= 150 THEN '$100-150'
	    WHEN pb.aur <= 200 THEN '$150-200'
	    WHEN pb.aur <= 300 THEN '$200-300'
	    WHEN pb.aur <= 500 THEN '$300-500'
	    WHEN pb.aur <= 700 THEN '$500-700'
	    WHEN pb.aur <= 1000 THEN '$700-1000'
	    WHEN pb.aur <= 1500 THEN '$1000-1500'
	    WHEN pb.aur <= 1000000000 THEN 'Over $1500'
	    ELSE 'NO RECEIPT FOUND'
	    END AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,SUM(packandhold_transfer_in_cost                                                       ) AS pah_tsfr_in_c
	,SUM(packandhold_transfer_in_units                                                      ) AS pah_tsfr_in_u
	,SUM(packandhold_transfer_out_cost                                                      ) AS pah_tsfr_out_c
	,SUM(packandhold_transfer_out_units                                                     ) AS pah_tsfr_out_u
	,SUM(racking_transfer_in_cost                                                           ) AS rk_tsfr_in_c
	,SUM(racking_transfer_in_units                                                          ) AS rk_tsfr_in_u
	,SUM(racking_transfer_out_cost                                                          ) AS rk_tsfr_out_c
	,SUM(racking_transfer_out_units                                                         ) AS rk_tsfr_out_u
	,SUM(reservestock_transfer_in_cost                                                      ) AS rs_tsfr_in_c
	,SUM(reservestock_transfer_in_units                                                     ) AS rs_tsfr_in_u
	,SUM(reservestock_transfer_out_cost                                                     ) AS rs_tsfr_out_c
	,SUM(reservestock_transfer_out_units                                                    ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM prd_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw fct
    JOIN date_lkup_acts dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_nord st
		ON fct.store_num = st.store_num
		AND st.channel_num IN (110,111,120,121,310,311)
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND st.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_nord pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION ALL
--TRANSFERS RACK
SELECT
	st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,CAST(NULL AS CHAR(1)) AS dropship_ind
	,cat.category
	,CASE
		WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
		WHEN pb.aur <= 10 THEN '$0-10'
		WHEN pb.aur <= 15 THEN '$10-15'
		WHEN pb.aur <= 20 THEN '$15-20'
		WHEN pb.aur <= 25 THEN '$20-25'
		WHEN pb.aur <= 30 THEN '$25-30'
		WHEN pb.aur <= 40 THEN '$30-40'
		WHEN pb.aur <= 50 THEN '$40-50'
		WHEN pb.aur <= 60 THEN '$50-60'
		WHEN pb.aur <= 80 THEN '$60-80'
		WHEN pb.aur <= 100 THEN '$80-100'
		WHEN pb.aur <= 150 THEN '$100-150'
		WHEN pb.aur <= 200 THEN '$150-200'
		WHEN pb.aur <= 1000000000 THEN 'Over $200'
		ELSE 'NO RECEIPT FOUND'
	    END  AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS eop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS eop_ttl_units
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS bop_ttl_c
	,CAST(0 AS INTEGER                                                                      ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,SUM(packandhold_transfer_in_cost                                                       ) AS pah_tsfr_in_c
	,SUM(packandhold_transfer_in_units                                                      ) AS pah_tsfr_in_u
	,SUM(packandhold_transfer_out_cost                                                      ) AS pah_tsfr_out_c
	,SUM(packandhold_transfer_out_units                                                     ) AS pah_tsfr_out_u
	,SUM(racking_transfer_in_cost                                                           ) AS rk_tsfr_in_c
	,SUM(racking_transfer_in_units                                                          ) AS rk_tsfr_in_u
	,SUM(racking_transfer_out_cost                                                          ) AS rk_tsfr_out_c
	,SUM(racking_transfer_out_units                                                         ) AS rk_tsfr_out_u
	,SUM(reservestock_transfer_in_cost                                                      ) AS rs_tsfr_in_c
	,SUM(reservestock_transfer_in_units                                                     ) AS rs_tsfr_in_u
	,SUM(reservestock_transfer_out_cost                                                     ) AS rs_tsfr_out_c
	,SUM(reservestock_transfer_out_units                                                    ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM prd_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw fct
    JOIN date_lkup_acts dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_rack st
		ON fct.store_num = st.store_num
		AND st.channel_num IN (210,211,250,260,261)
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND st.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_rack pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON transfers;

/************************************************************************************/
/****************************** 6.E.EOP STAGING *************************************/
/************************************************************************************/

--DROP TABLE eop_stage_ty;
CREATE MULTISET VOLATILE TABLE eop_stage_ty AS
(
SELECT
	fct.rms_sku_num
	,st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,SUM(CASE WHEN dt.mtd_week_idnt_true = fct.week_num THEN eoh_total_cost + eoh_in_transit_total_cost END) AS eop_ttl_c
	,SUM(CASE WHEN dt.mtd_week_idnt_true = fct.week_num THEN eoh_total_units + eoh_in_transit_total_units END) AS eop_ttl_units
	,SUM(CASE WHEN dt.mbeg_week_idnt_true = fct.week_num THEN boh_total_cost + boh_in_transit_total_cost END) AS bop_ttl_c
	,SUM(CASE WHEN dt.mbeg_week_idnt_true = fct.week_num THEN boh_total_units + boh_in_transit_total_units END) AS bop_ttl_units
FROM prd_nap_usr_vws.merch_inventory_sku_store_week_fact_vw fct
    JOIN date_lkup_acts_ty dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_eop st
		ON fct.store_num = st.store_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA
	PRIMARY INDEX (rms_sku_num, channel_country)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (rms_sku_num, channel_country)
		ON eop_stage_ty;


--DROP TABLE eop_stage_ly;
CREATE MULTISET VOLATILE TABLE eop_stage_ly AS
(
SELECT
	fct.rms_sku_num
	,st.channel_country
	,st.banner
	,dt.date_ind
	,dt.month_idnt
	,dt.month_label
	,dt.month_idnt_aligned
	,dt.month_label_aligned
	,dt.month_start_day_date
    ,dt.month_end_day_date
	,fct.department_num
	,SUM(CASE WHEN dt.mtd_week_idnt_true = fct.week_num THEN eoh_total_cost + eoh_in_transit_total_cost END) AS eop_ttl_c
	,SUM(CASE WHEN dt.mtd_week_idnt_true = fct.week_num THEN eoh_total_units + eoh_in_transit_total_units END) AS eop_ttl_units
	,SUM(CASE WHEN dt.mbeg_week_idnt_true = fct.week_num THEN boh_total_cost + boh_in_transit_total_cost END) AS bop_ttl_c
	,SUM(CASE WHEN dt.mbeg_week_idnt_true = fct.week_num THEN boh_total_units + boh_in_transit_total_units END) AS bop_ttl_units
FROM prd_nap_usr_vws.merch_inventory_sku_store_week_fact_vw fct
    JOIN date_lkup_acts_ly dt
    	ON fct.week_num = dt.week_idnt_true
	JOIN store_lkup_eop st
		ON fct.store_num = st.store_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA
	PRIMARY INDEX (rms_sku_num, channel_country)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (rms_sku_num, channel_country)
		ON eop_stage_ly;

/************************************************************************************/
/*********************************** 6.F.EOP FINAL **********************************/
/************************************************************************************/

--DROP TABLE eop_ty;
CREATE MULTISET VOLATILE TABLE eop_ty AS
(
--EOP NORD
SELECT
	fct.channel_country
	,fct.banner
	,fct.date_ind
	,fct.month_idnt
	,fct.month_label
	,fct.month_idnt_aligned
	,fct.month_label_aligned
	,fct.month_start_day_date
    ,fct.month_end_day_date
	,fct.department_num
	,CAST(NULL AS CHAR(1)) AS dropship_ind
	,cat.category
	,CASE
	    WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
	    WHEN pb.aur <= 25 THEN '$0-25'
	    WHEN pb.aur <= 50 THEN '$25-50'
	    WHEN pb.aur <= 100 THEN '$50-100'
	    WHEN pb.aur <= 150 THEN '$100-150'
	    WHEN pb.aur <= 200 THEN '$150-200'
	    WHEN pb.aur <= 300 THEN '$200-300'
	    WHEN pb.aur <= 500 THEN '$300-500'
	    WHEN pb.aur <= 700 THEN '$500-700'
	    WHEN pb.aur <= 1000 THEN '$700-1000'
	    WHEN pb.aur <= 1500 THEN '$1000-1500'
	    WHEN pb.aur <= 1000000000 THEN 'Over $1500'
	    ELSE 'NO RECEIPT FOUND'
	    END AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,SUM(fct.eop_ttl_c                                                                      ) AS eop_ttl_c
	,SUM(fct.eop_ttl_units                                                                  ) AS eop_ttl_units
	,SUM(fct.bop_ttl_c                                                                      ) AS bop_ttl_c
	,SUM(fct.bop_ttl_units                                                                  ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM eop_stage_ty fct
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND fct.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_nord pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND fct.channel_country = pb.channel_country
WHERE fct.banner = 'NORDSTROM'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
UNION ALL
--EOP RACK
SELECT
	fct.channel_country
	,fct.banner
	,fct.date_ind
	,fct.month_idnt
	,fct.month_label
	,fct.month_idnt_aligned
	,fct.month_label_aligned
	,fct.month_start_day_date
    ,fct.month_end_day_date
	,fct.department_num
	,CAST(NULL AS CHAR(1)) AS dropship_ind
	,cat.category
	,CASE
		WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
		WHEN pb.aur <= 10 THEN '$0-10'
		WHEN pb.aur <= 15 THEN '$10-15'
		WHEN pb.aur <= 20 THEN '$15-20'
		WHEN pb.aur <= 25 THEN '$20-25'
		WHEN pb.aur <= 30 THEN '$25-30'
		WHEN pb.aur <= 40 THEN '$30-40'
		WHEN pb.aur <= 50 THEN '$40-50'
		WHEN pb.aur <= 60 THEN '$50-60'
		WHEN pb.aur <= 80 THEN '$60-80'
		WHEN pb.aur <= 100 THEN '$80-100'
		WHEN pb.aur <= 150 THEN '$100-150'
		WHEN pb.aur <= 200 THEN '$150-200'
		WHEN pb.aur <= 1000000000 THEN 'Over $200'
		ELSE 'NO RECEIPT FOUND'
	    END  AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,SUM(fct.eop_ttl_c                                                                      ) AS eop_ttl_c
	,SUM(fct.eop_ttl_units                                                                  ) AS eop_ttl_units
	,SUM(fct.bop_ttl_c                                                                      ) AS bop_ttl_c
	,SUM(fct.bop_ttl_units                                                                  ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM eop_stage_ty fct
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND fct.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_rack pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND fct.channel_country = pb.channel_country
WHERE fct.banner = 'NORDSTROM_RACK'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON eop_ty;


--DROP TABLE eop_ly;
CREATE MULTISET VOLATILE TABLE eop_ly AS
(
--EOP NORD
SELECT
	fct.channel_country
	,fct.banner
	,fct.date_ind
	,fct.month_idnt
	,fct.month_label
	,fct.month_idnt_aligned
	,fct.month_label_aligned
	,fct.month_start_day_date
    ,fct.month_end_day_date
	,fct.department_num
	,CAST(NULL AS CHAR(1)) AS dropship_ind
	,cat.category
	,CASE
	    WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
	    WHEN pb.aur <= 25 THEN '$0-25'
	    WHEN pb.aur <= 50 THEN '$25-50'
	    WHEN pb.aur <= 100 THEN '$50-100'
	    WHEN pb.aur <= 150 THEN '$100-150'
	    WHEN pb.aur <= 200 THEN '$150-200'
	    WHEN pb.aur <= 300 THEN '$200-300'
	    WHEN pb.aur <= 500 THEN '$300-500'
	    WHEN pb.aur <= 700 THEN '$500-700'
	    WHEN pb.aur <= 1000 THEN '$700-1000'
	    WHEN pb.aur <= 1500 THEN '$1000-1500'
	    WHEN pb.aur <= 1000000000 THEN 'Over $1500'
	    ELSE 'NO RECEIPT FOUND'
	    END AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,SUM(fct.eop_ttl_c                                                                      ) AS eop_ttl_c
	,SUM(fct.eop_ttl_units                                                                  ) AS eop_ttl_units
	,SUM(fct.bop_ttl_c                                                                      ) AS bop_ttl_c
	,SUM(fct.bop_ttl_units                                                                  ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM eop_stage_ly fct
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND fct.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_nord pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND fct.channel_country = pb.channel_country
WHERE fct.banner = 'NORDSTROM'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
UNION ALL
--EOP RACK
SELECT
	fct.channel_country
	,fct.banner
	,fct.date_ind
	,fct.month_idnt
	,fct.month_label
	,fct.month_idnt_aligned
	,fct.month_label_aligned
	,fct.month_start_day_date
    ,fct.month_end_day_date
	,fct.department_num
	,CAST(NULL AS CHAR(1)) AS dropship_ind
	,cat.category
	,CASE
		WHEN pb.aur <= 0.01 THEN 'NO RECEIPT FOUND'
		WHEN pb.aur <= 10 THEN '$0-10'
		WHEN pb.aur <= 15 THEN '$10-15'
		WHEN pb.aur <= 20 THEN '$15-20'
		WHEN pb.aur <= 25 THEN '$20-25'
		WHEN pb.aur <= 30 THEN '$25-30'
		WHEN pb.aur <= 40 THEN '$30-40'
		WHEN pb.aur <= 50 THEN '$40-50'
		WHEN pb.aur <= 60 THEN '$50-60'
		WHEN pb.aur <= 80 THEN '$60-80'
		WHEN pb.aur <= 100 THEN '$80-100'
		WHEN pb.aur <= 150 THEN '$100-150'
		WHEN pb.aur <= 200 THEN '$150-200'
		WHEN pb.aur <= 1000000000 THEN 'Over $200'
		ELSE 'NO RECEIPT FOUND'
	    END  AS price_band
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_r
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS net_sls_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS net_sls_reg_c
	,CAST(0 AS INTEGER                                                                      ) AS net_sls_reg_units
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_r
	,CAST(0 AS DECIMAL(38, 2)                                                               ) AS returns_c
	,CAST(0 AS INTEGER                                                                      ) AS returns_u
	,CAST(0 AS DECIMAL(38, 6)                                                               ) AS demand_ttl_r
	,CAST(0 AS INTEGER                                                                      ) AS demand_ttl_units
	,SUM(fct.eop_ttl_c                                                                      ) AS eop_ttl_c
	,SUM(fct.eop_ttl_units                                                                  ) AS eop_ttl_units
	,SUM(fct.bop_ttl_c                                                                      ) AS bop_ttl_c
	,SUM(fct.bop_ttl_units                                                                  ) AS bop_ttl_units
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_rp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_rp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS ttl_porcpt_nrp_c
	,CAST(0 AS INTEGER                                                                      ) AS ttl_porcpt_nrp_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS pah_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS pah_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rk_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rk_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_in_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_in_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS rs_tsfr_out_c
	,CAST(0 AS INTEGER                                                                      ) AS rs_tsfr_out_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_bop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_bop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_eop_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_eop_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_receipts_lr_c
	,CAST(0 AS INTEGER                                                                      ) AS plan_receipts_lr_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_c
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_sales_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_sales_u
	,CAST(0 AS DECIMAL(38, 4)                                                               ) AS plan_demand_r
	,CAST(0 AS INTEGER                                                                      ) AS plan_demand_u
FROM eop_stage_ly fct
	LEFT JOIN cat_lkup cat
		ON fct.rms_sku_num = cat.rms_sku_num
    	AND fct.department_num = cat.dept_num
    	AND fct.channel_country = cat.channel_country
	LEFT JOIN receipt_price_band_rack pb
		ON fct.rms_sku_num  = pb.sku_idnt
		AND fct.channel_country = pb.channel_country
WHERE fct.banner = 'NORDSTROM_RACK'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON eop_ly;


/************************************************************************************/
/********************************** 7.COMBINE ***************************************/
/************************************************************************************/

--DROP TABLE combine;
CREATE MULTISET VOLATILE TABLE combine AS
(
SELECT
	sub.channel_country
	,sub.banner
	,sub.date_ind
	,sub.month_idnt
	,sub.month_label
	,sub.month_idnt_aligned
	,sub.month_label_aligned
	,sub.month_start_day_date
    ,sub.month_end_day_date
	,sub.department_num
	,sub.dropship_ind
	,sub.category
	,sub.price_band
	,SUM(sub.net_sls_r                          ) AS net_sls_r
	,SUM(sub.net_sls_c                          ) AS net_sls_c
	,SUM(sub.net_sls_units                      ) AS net_sls_units
	,SUM(sub.net_sls_reg_r                      ) AS net_sls_reg_r
	,SUM(sub.net_sls_reg_c                      ) AS net_sls_reg_c
	,SUM(sub.net_sls_reg_units                  ) AS net_sls_reg_units
	,SUM(sub.returns_r                          ) AS returns_r
	,SUM(sub.returns_c                          ) AS returns_c
	,SUM(sub.returns_u                          ) AS returns_u
	,SUM(sub.demand_ttl_r                       ) AS demand_ttl_r
	,SUM(sub.demand_ttl_units                   ) AS demand_ttl_units
	,SUM(sub.eop_ttl_c                          ) AS eop_ttl_c
	,SUM(sub.eop_ttl_units                      ) AS eop_ttl_units
	,SUM(sub.bop_ttl_c                          ) AS bop_ttl_c
	,SUM(sub.bop_ttl_units                      ) AS bop_ttl_units
	,SUM(sub.ttl_porcpt_c                       ) AS ttl_porcpt_c
	,SUM(sub.ttl_porcpt_u                       ) AS ttl_porcpt_u
	,SUM(sub.ttl_porcpt_rp_c                    ) AS ttl_porcpt_rp_c
	,SUM(sub.ttl_porcpt_rp_u                    ) AS ttl_porcpt_rp_u
	,SUM(sub.ttl_porcpt_nrp_c                   ) AS ttl_porcpt_nrp_c
	,SUM(sub.ttl_porcpt_nrp_u                   ) AS ttl_porcpt_nrp_u
	,SUM(sub.pah_tsfr_in_c                      ) AS pah_tsfr_in_c
	,SUM(sub.pah_tsfr_in_u                      ) AS pah_tsfr_in_u
	,SUM(sub.pah_tsfr_out_c                     ) AS pah_tsfr_out_c
	,SUM(sub.pah_tsfr_out_u                     ) AS pah_tsfr_out_u
	,SUM(sub.rk_tsfr_in_c                       ) AS rk_tsfr_in_c
	,SUM(sub.rk_tsfr_in_u                       ) AS rk_tsfr_in_u
	,SUM(sub.rk_tsfr_out_c                      ) AS rk_tsfr_out_c
	,SUM(sub.rk_tsfr_out_u                      ) AS rk_tsfr_out_u
	,SUM(sub.rs_tsfr_in_c                       ) AS rs_tsfr_in_c
	,SUM(sub.rs_tsfr_in_u                       ) AS rs_tsfr_in_u
	,SUM(sub.rs_tsfr_out_c                      ) AS rs_tsfr_out_c
	,SUM(sub.rs_tsfr_out_u                      ) AS rs_tsfr_out_u
	,SUM(sub.plan_bop_c                         ) AS plan_bop_c
	,SUM(sub.plan_bop_u                         ) AS plan_bop_u
	,SUM(sub.plan_eop_c                         ) AS plan_eop_c
	,SUM(sub.plan_eop_u                         ) AS plan_eop_u
	,SUM(sub.plan_receipts_c                    ) AS plan_receipts_c
	,SUM(sub.plan_receipts_u                    ) AS plan_receipts_u
	,SUM(sub.plan_receipts_lr_c                 ) AS plan_receipts_lr_c
	,SUM(sub.plan_receipts_lr_u                 ) AS plan_receipts_lr_u
	,SUM(sub.plan_sales_c                       ) AS plan_sales_c
	,SUM(sub.plan_sales_r                       ) AS plan_sales_r
	,SUM(sub.plan_sales_u                       ) AS plan_sales_u
	,SUM(sub.plan_demand_r                      ) AS plan_demand_r
	,SUM(sub.plan_demand_u                      ) AS plan_demand_u
FROM
(
SELECT * FROM sales
UNION ALL
SELECT * FROM demand
UNION ALL
SELECT * FROM receipts
UNION ALL
SELECT * FROM transfers
UNION ALL
SELECT * FROM eop_ty
UNION ALL
SELECT * FROM eop_ly
UNION ALL
SELECT * FROM plan
) sub
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON combine;

--DROP TABLE final_combine;
CREATE MULTISET VOLATILE TABLE final_combine AS
(
SELECT
	bs.channel_country
	,bs.banner
	,bs.date_ind
	,bs.month_idnt
	,bs.month_label
	,bs.month_idnt_aligned
	,bs.month_label_aligned
	,bs.month_start_day_date
    ,bs.month_end_day_date
    ,hr.active_store_ind
    ,hr.division_desc
    ,hr.subdivision_desc
    ,hr.department_desc
	,bs.department_num
	,bs.dropship_ind
	,bs.category
	,ca.category_planner_1
	,ca.category_planner_2
	,ca.category_group
	,ca.seasonal_designation
	,ca.rack_merch_zone
	,ca.is_activewear
	,ca.channel_category_roles_1
	,ca.channel_category_roles_2
	,ca.bargainista_dept_map
	,bs.price_band
	,SUM(bs.net_sls_r                          ) AS net_sls_r
	,SUM(bs.net_sls_c                          ) AS net_sls_c
	,SUM(bs.net_sls_units                      ) AS net_sls_units
	,SUM(bs.net_sls_reg_r                      ) AS net_sls_reg_r
	,SUM(bs.net_sls_reg_c                      ) AS net_sls_reg_c
	,SUM(bs.net_sls_reg_units                  ) AS net_sls_reg_units
	,SUM(bs.returns_r                          ) AS returns_r
	,SUM(bs.returns_c                          ) AS returns_c
	,SUM(bs.returns_u                          ) AS returns_u
	,SUM(bs.demand_ttl_r                       ) AS demand_ttl_r
	,SUM(bs.demand_ttl_units                   ) AS demand_ttl_units
	,SUM(bs.eop_ttl_c                          ) AS eop_ttl_c
	,SUM(bs.eop_ttl_units                      ) AS eop_ttl_units
	,SUM(bs.bop_ttl_c                          ) AS bop_ttl_c
	,SUM(bs.bop_ttl_units                      ) AS bop_ttl_units
	,SUM(bs.ttl_porcpt_c                       ) AS ttl_porcpt_c
	,SUM(bs.ttl_porcpt_u                       ) AS ttl_porcpt_u
	,SUM(bs.ttl_porcpt_rp_c                    ) AS ttl_porcpt_rp_c
	,SUM(bs.ttl_porcpt_rp_u                    ) AS ttl_porcpt_rp_u
	,SUM(bs.ttl_porcpt_nrp_c                   ) AS ttl_porcpt_nrp_c
	,SUM(bs.ttl_porcpt_nrp_u                   ) AS ttl_porcpt_nrp_u
	,SUM(bs.pah_tsfr_in_c                      ) AS pah_tsfr_in_c
	,SUM(bs.pah_tsfr_in_u                      ) AS pah_tsfr_in_u
	,SUM(bs.pah_tsfr_out_c                     ) AS pah_tsfr_out_c
	,SUM(bs.pah_tsfr_out_u                     ) AS pah_tsfr_out_u
	,SUM(bs.rk_tsfr_in_c                       ) AS rk_tsfr_in_c
	,SUM(bs.rk_tsfr_in_u                       ) AS rk_tsfr_in_u
	,SUM(bs.rk_tsfr_out_c                      ) AS rk_tsfr_out_c
	,SUM(bs.rk_tsfr_out_u                      ) AS rk_tsfr_out_u
	,SUM(bs.rs_tsfr_in_c                       ) AS rs_tsfr_in_c
	,SUM(bs.rs_tsfr_in_u                       ) AS rs_tsfr_in_u
	,SUM(bs.rs_tsfr_out_c                      ) AS rs_tsfr_out_c
	,SUM(bs.rs_tsfr_out_u                      ) AS rs_tsfr_out_u
	,SUM(bs.plan_bop_c                         ) AS plan_bop_c
	,SUM(bs.plan_bop_u                         ) AS plan_bop_u
	,SUM(bs.plan_eop_c                         ) AS plan_eop_c
	,SUM(bs.plan_eop_u                         ) AS plan_eop_u
	,SUM(bs.plan_receipts_c                    ) AS plan_receipts_c
	,SUM(bs.plan_receipts_u                    ) AS plan_receipts_u
	,SUM(bs.plan_receipts_lr_c                 ) AS plan_receipts_lr_c
	,SUM(bs.plan_receipts_lr_u                 ) AS plan_receipts_lr_u
	,SUM(bs.plan_sales_c                       ) AS plan_sales_c
	,SUM(bs.plan_sales_r                       ) AS plan_sales_r
	,SUM(bs.plan_sales_u                       ) AS plan_sales_u
	,SUM(bs.plan_demand_r                      ) AS plan_demand_r
	,SUM(bs.plan_demand_u                      ) AS plan_demand_u
	,CURRENT_TIMESTAMP AS update_timestamp
FROM combine bs
LEFT JOIN cat_attr ca
	ON bs.department_num = ca.dept_num
	AND bs.category = ca.category
LEFT JOIN dept_lkup hr
		ON bs.department_num = hr.dept_num
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
) WITH DATA
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON final_combine;

/************************************************************************************/
/********************************** 8. INSERT ***************************************/
/************************************************************************************/

DELETE FROM {environment_schema}.apt_is_category;

INSERT INTO {environment_schema}.apt_is_category
SELECT * FROM final_combine;

COLLECT STATS
	PRIMARY INDEX (date_ind, month_idnt, banner, channel_country, department_num, category, price_band)
		ON {environment_schema}.apt_is_category;
