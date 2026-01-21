/*
MFP Inventory Channel Plans Main
Author: Asiyah Fox
Date Created: 7/31/24
Date Last Updated: 7/31/24

Datalab: T2DL_DAS_ACE_MFP
Service Account: T2DL_NAP_SEL_BATCH
Deletes and Inserts into Table: mfp_inv_channel_plans
*/

/* Notes: 
    - Uses t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_fact_vw, as dept_num is included but 
        not in NAP table--and aligns to APT reporting. This could be swapped if future table includes dept level.
*/
/******************************************************************/
/***************************** DATES ******************************/
/******************************************************************/

--DROP TABLE date_lkup_wk;
CREATE MULTISET VOLATILE TABLE date_lkup_wk AS (
	SELECT
		dt.week_idnt
		,dt.month_idnt
	FROM prd_nap_usr_vws.day_cal_454_dim dt
	INNER JOIN prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
		ON dt.week_idnt = mfp.week_num
	INNER JOIN (SELECT DISTINCT fiscal_month_idnt FROM t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_fact_vw) apt
		ON dt.month_idnt = apt.fiscal_month_idnt
--	WHERE dt.month_idnt BETWEEN 202403 AND 202409 --TESTING
	GROUP BY 1,2
) WITH DATA
	PRIMARY INDEX (week_idnt) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_idnt)
		ON date_lkup_wk
;

--DROP TABLE date_lkup_mth;
CREATE MULTISET VOLATILE TABLE date_lkup_mth AS (
	SELECT
		mth.month_idnt
		,month_idnt_eop
	FROM date_lkup_wk wk
	INNER JOIN
		(
		SELECT
			month_idnt
			,LEAD(month_idnt, 1) OVER (ORDER BY month_idnt) AS month_idnt_eop
		FROM 
			(
			SELECT DISTINCT
				month_idnt
			FROM prd_nap_usr_vws.day_cal_454_dim
			) eop
		) mth
			ON wk.month_idnt = mth.month_idnt
	GROUP BY 1,2
) WITH DATA
	PRIMARY INDEX (month_idnt,month_idnt_eop) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (month_idnt,month_idnt_eop)
		ON date_lkup_mth
;

/******************************************************************/
/************************ APT MULTIPLIERS *************************/
/******************************************************************/

--DROP TABLE apt_stage;
CREATE MULTISET VOLATILE TABLE apt_stage AS (
SELECT 
	mth.month_idnt
--	,mth.month_idnt_eop
	,CASE 
		WHEN fct.selling_country = 'US' AND fct.selling_brand = 'NORDSTROM' THEN 1
		WHEN fct.selling_country = 'CA' AND fct.selling_brand = 'NORDSTROM' THEN 2
		WHEN fct.selling_country = 'US' AND fct.selling_brand = 'NORDSTROM_RACK' THEN 3
		WHEN fct.selling_country = 'CA' AND fct.selling_brand = 'NORDSTROM_RACK' THEN 4
			END AS banner_country_num
	,CASE 
		WHEN fct.alternate_inventory_model = 'OWN' THEN 3
		WHEN fct.alternate_inventory_model = 'DROPSHIP' THEN 1
			END AS fulfill_type_num
	,CAST(fct.dept_idnt AS INTEGER) AS dept_num
    ,CASE WHEN fct.cluster_name = 'NORDSTROM_CANADA_STORES' THEN 111
		WHEN fct.cluster_name = 'NORDSTROM_CANADA_ONLINE' THEN 121
		WHEN fct.cluster_name = 'NORDSTROM_STORES' THEN 110
		WHEN fct.cluster_name = 'NORDSTROM_ONLINE' THEN 120
		WHEN fct.cluster_name = 'RACK_ONLINE' THEN 250
		WHEN fct.cluster_name = 'RACK_CANADA_STORES' THEN 211
		WHEN fct.cluster_name IN ('RACK STORES', 'PRICE','HYBRID','BRAND') THEN 210
		WHEN fct.cluster_name = 'NORD CA RSWH' THEN 311
		WHEN fct.cluster_name = 'NORD US RSWH' THEN 310
		WHEN fct.cluster_name = 'RACK CA RSWH' THEN 261
		WHEN fct.cluster_name = 'RACK US RSWH' THEN 260
    	END AS channel_num 
	,SUM(fct.beginning_of_period_inventory_units) AS bop_u
	,SUM(fct.beginning_of_period_inventory_cost_amount) AS bop_c
	,SUM(fct.replenishment_receipts_units + nonreplenishment_receipts_units + dropship_receipt_units) AS receipts_act_u
	,SUM(fct.replenishment_receipts_cost_amount + nonreplenishment_receipts_cost_amount + dropship_receipt_cost_amount) AS receipts_act_c
	,SUM(fct.replenishment_receipts_units + nonreplenishment_receipts_units - fct.replenishment_receipts_less_reserve_units - fct.nonreplenishment_receipts_less_reserve_units) AS receipts_res_u
	,SUM(fct.replenishment_receipts_cost_amount + nonreplenishment_receipts_cost_amount - fct.replenishment_receipts_less_reserve_cost_amount - fct.nonreplenishment_receipts_less_reserve_cost_amount) AS receipts_res_c
FROM t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_fact_vw fct
INNER JOIN date_lkup_mth mth
	ON fct.fiscal_month_idnt = mth.month_idnt
GROUP BY 1,2,3,4,5--,6
) WITH DATA
	PRIMARY INDEX (month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (month_idnt,banner_country_num,fulfill_type_num,dept_num,channel_num) 
		ON apt_stage;
;

--DROP TABLE apt_pct;
CREATE MULTISET VOLATILE TABLE apt_pct AS (
SELECT 
	fct.month_idnt
	,fct.banner_country_num
	,fct.fulfill_type_num
	,fct.dept_num
	,fct.channel_num
	,fct.bop_u
	,fct.bop_c
	,fct.receipts_act_u
	,fct.receipts_act_c
	,fct.receipts_res_u
	,fct.receipts_res_c
	,1.0000 * fct.bop_u / NULLIFZERO(SUM(fct.bop_u) OVER (PARTITION BY fct.month_idnt,fct.banner_country_num,fct.fulfill_type_num,fct.dept_num)) AS bop_u_pct
	,1.0000 * fct.bop_c / NULLIFZERO(SUM(fct.bop_c) OVER (PARTITION BY fct.month_idnt,fct.banner_country_num,fct.fulfill_type_num,fct.dept_num)) AS bop_c_pct
	,1.0000 * fct.receipts_act_u / NULLIFZERO(SUM(fct.receipts_act_u) OVER (PARTITION BY fct.month_idnt,fct.banner_country_num,fct.fulfill_type_num,fct.dept_num)) AS receipts_act_u_pct
	,1.0000 * fct.receipts_act_c / NULLIFZERO(SUM(fct.receipts_act_c) OVER (PARTITION BY fct.month_idnt,fct.banner_country_num,fct.fulfill_type_num,fct.dept_num)) AS receipts_act_c_pct
	,1.0000 * fct.receipts_res_u / NULLIFZERO(SUM(fct.receipts_res_u) OVER (PARTITION BY fct.month_idnt,fct.banner_country_num,fct.fulfill_type_num,fct.dept_num)) AS receipts_res_u_pct
	,1.0000 * fct.receipts_res_c / NULLIFZERO(SUM(fct.receipts_res_c) OVER (PARTITION BY fct.month_idnt,fct.banner_country_num,fct.fulfill_type_num,fct.dept_num)) AS receipts_res_c_pct
FROM apt_stage fct
) WITH DATA
	PRIMARY INDEX (month_idnt,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (month_idnt,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON apt_pct;
;

/******************************************************************/
/************************ CHANNEL PLANS ***************************/
/******************************************************************/

--DROP TABLE bop_u;
CREATE MULTISET VOLATILE TABLE bop_u AS (
SELECT 
	 mfp.week_num
	,COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt
	,COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num
	,COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num
	,COALESCE(mfp.dept_num, apt.dept_num) AS dept_num
	,COALESCE(apt.channel_num, 0) AS channel_num
	,CAST(mfp.sp_beginning_of_period_active_qty * COALESCE(NULLIFZERO(apt.bop_u_pct), 1) AS DECIMAL(36,8)) AS sp_beginning_of_period_active_qty
--	,CAST(mfp.sp_beginning_of_period_active_cost_amt * COALESCE(NULLIFZERO(apt.bop_c_pct), 1) AS DECIMAL(36,8)) AS sp_beginning_of_period_active_cost_amt
	,CAST(mfp.op_beginning_of_period_active_qty * COALESCE(NULLIFZERO(apt.bop_u_pct), 1) AS DECIMAL(36,8)) AS op_beginning_of_period_active_qty
--	,CAST(mfp.op_beginning_of_period_active_cost_amt * COALESCE(NULLIFZERO(apt.bop_c_pct), 1) AS DECIMAL(36,8)) AS op_beginning_of_period_active_cost_amt
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
INNER JOIN date_lkup_wk wk
	ON mfp.week_num = wk.week_idnt
LEFT JOIN apt_pct apt
	 ON apt.month_idnt = wk.month_idnt
	AND apt.banner_country_num = mfp.banner_country_num
	AND apt.fulfill_type_num = mfp.fulfill_type_num
	AND apt.dept_num = mfp.dept_num
	AND apt.bop_u > 0
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON bop_u
;


--DROP TABLE bop_c;
CREATE MULTISET VOLATILE TABLE bop_c AS (
SELECT 
	 mfp.week_num
	,COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt
	,COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num
	,COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num
	,COALESCE(mfp.dept_num, apt.dept_num) AS dept_num
	,COALESCE(apt.channel_num, 0) AS channel_num
	,CAST(mfp.sp_beginning_of_period_active_cost_amt * COALESCE(NULLIFZERO(apt.bop_c_pct), 1) AS DECIMAL(36,8)) AS sp_beginning_of_period_active_cost_amt
	,CAST(mfp.op_beginning_of_period_active_cost_amt * COALESCE(NULLIFZERO(apt.bop_c_pct), 1) AS DECIMAL(36,8)) AS op_beginning_of_period_active_cost_amt
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
INNER JOIN date_lkup_wk wk
	ON mfp.week_num = wk.week_idnt
LEFT JOIN apt_pct apt
	 ON apt.month_idnt = wk.month_idnt
	AND apt.banner_country_num = mfp.banner_country_num
	AND apt.fulfill_type_num = mfp.fulfill_type_num
	AND apt.dept_num = mfp.dept_num
	AND apt.bop_c > 0
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON bop_c
;

--DROP TABLE eop_u;
CREATE MULTISET VOLATILE TABLE eop_u AS (
SELECT 
	 mfp.week_num
	,COALESCE(wk.month_idnt, mth.month_idnt) AS month_idnt
	,COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num
	,COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num
	,COALESCE(mfp.dept_num, apt.dept_num) AS dept_num
	,COALESCE(apt.channel_num, 0) AS channel_num
	,CAST(mfp.sp_ending_of_period_active_qty * COALESCE(NULLIFZERO(apt.bop_u_pct), 1) AS DECIMAL(36,8)) AS sp_ending_of_period_active_qty
--	,CAST(mfp.sp_ending_of_period_active_cost_amt * COALESCE(NULLIFZERO(apt.bop_c_pct), 1) AS DECIMAL(36,8)) AS sp_ending_of_period_active_cost_amt
	,CAST(mfp.op_ending_of_period_active_qty * COALESCE(NULLIFZERO(apt.bop_u_pct), 1) AS DECIMAL(36,8)) AS op_ending_of_period_active_qty
--	,CAST(mfp.op_ending_of_period_active_cost_amt * COALESCE(NULLIFZERO(apt.bop_c_pct), 1) AS DECIMAL(36,8)) AS op_ending_of_period_active_cost_amt
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
INNER JOIN date_lkup_wk wk
	ON mfp.week_num = wk.week_idnt
INNER JOIN date_lkup_mth mth
	ON wk.month_idnt = mth.month_idnt
LEFT JOIN apt_pct apt
	 ON apt.month_idnt = mth.month_idnt_eop
	AND apt.banner_country_num = mfp.banner_country_num
	AND apt.fulfill_type_num = mfp.fulfill_type_num
	AND apt.dept_num = mfp.dept_num
	AND apt.bop_u > 0
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON eop_u
;

--DROP TABLE eop_c;
CREATE MULTISET VOLATILE TABLE eop_c AS (
SELECT 
	 mfp.week_num
	,COALESCE(wk.month_idnt, mth.month_idnt) AS month_idnt
	,COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num
	,COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num
	,COALESCE(mfp.dept_num, apt.dept_num) AS dept_num
	,COALESCE(apt.channel_num, 0) AS channel_num
	,CAST(mfp.sp_ending_of_period_active_cost_amt * COALESCE(NULLIFZERO(apt.bop_c_pct), 1) AS DECIMAL(36,8)) AS sp_ending_of_period_active_cost_amt
	,CAST(mfp.op_ending_of_period_active_cost_amt * COALESCE(NULLIFZERO(apt.bop_c_pct), 1) AS DECIMAL(36,8)) AS op_ending_of_period_active_cost_amt
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
INNER JOIN date_lkup_wk wk
	ON mfp.week_num = wk.week_idnt
INNER JOIN date_lkup_mth mth
	ON wk.month_idnt = mth.month_idnt
LEFT JOIN apt_pct apt
	 ON apt.month_idnt = mth.month_idnt_eop
	AND apt.banner_country_num = mfp.banner_country_num
	AND apt.fulfill_type_num = mfp.fulfill_type_num
	AND apt.dept_num = mfp.dept_num
	AND apt.bop_c > 0
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON eop_c
;

--DROP TABLE receipt_u;
CREATE MULTISET VOLATILE TABLE receipt_u AS (
SELECT 
	 mfp.week_num
	,COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt
	,COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num
	,COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num
	,COALESCE(mfp.dept_num, apt.dept_num) AS dept_num
	,COALESCE(apt.channel_num, 0) AS channel_num
	,CAST(mfp.sp_receipts_active_qty * COALESCE(NULLIFZERO(apt.receipts_act_u_pct), 1) AS DECIMAL(36,8)) AS sp_receipts_active_qty
--	,CAST(mfp.sp_receipts_active_cost_amt * COALESCE(NULLIFZERO(apt.receipts_act_c_pct), 1) AS DECIMAL(36,8)) AS sp_receipts_active_cost_amt
	,CAST(mfp.op_receipts_active_qty * COALESCE(NULLIFZERO(apt.receipts_act_u_pct), 1) AS DECIMAL(36,8)) AS op_receipts_active_qty
--	,CAST(mfp.op_receipts_active_cost_amt * COALESCE(NULLIFZERO(apt.receipts_act_c_pct), 1) AS DECIMAL(36,8)) AS op_receipts_active_cost_amt
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
INNER JOIN date_lkup_wk wk
	ON mfp.week_num = wk.week_idnt
LEFT JOIN apt_pct apt
	 ON apt.month_idnt = wk.month_idnt
	AND apt.banner_country_num = mfp.banner_country_num
	AND apt.fulfill_type_num = mfp.fulfill_type_num
	AND apt.dept_num = mfp.dept_num
	AND apt.receipts_act_u > 0
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON receipt_u
;

--DROP TABLE receipt_c;
CREATE MULTISET VOLATILE TABLE receipt_c AS (
SELECT 
	 mfp.week_num
	,COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt
	,COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num
	,COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num
	,COALESCE(mfp.dept_num, apt.dept_num) AS dept_num
	,COALESCE(apt.channel_num, 0) AS channel_num
	,CAST(mfp.sp_receipts_active_cost_amt * COALESCE(NULLIFZERO(apt.receipts_act_c_pct), 1) AS DECIMAL(36,8)) AS sp_receipts_active_cost_amt
	,CAST(mfp.op_receipts_active_cost_amt * COALESCE(NULLIFZERO(apt.receipts_act_c_pct), 1) AS DECIMAL(36,8)) AS op_receipts_active_cost_amt
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
INNER JOIN date_lkup_wk wk
	ON mfp.week_num = wk.week_idnt
LEFT JOIN apt_pct apt
	 ON apt.month_idnt = wk.month_idnt
	AND apt.banner_country_num = mfp.banner_country_num
	AND apt.fulfill_type_num = mfp.fulfill_type_num
	AND apt.dept_num = mfp.dept_num
	AND apt.receipts_act_c > 0
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON receipt_c
;

--DROP TABLE reserve_u;
CREATE MULTISET VOLATILE TABLE reserve_u AS (
SELECT 
	 mfp.week_num
	,COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt
	,COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num
	,COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num
	,COALESCE(mfp.dept_num, apt.dept_num) AS dept_num
	,COALESCE(apt.channel_num, 0) AS channel_num
	,CAST(mfp.sp_receipts_reserve_qty * COALESCE(NULLIFZERO(apt.receipts_res_u_pct), 1) AS DECIMAL(36,8)) AS sp_receipts_reserve_qty
--	,CAST(mfp.sp_receipts_reserve_cost_amt * COALESCE(NULLIFZERO(apt.receipts_res_c_pct), 1) AS DECIMAL(36,8)) AS sp_receipts_reserve_cost_amt
	,CAST(mfp.op_receipts_reserve_qty * COALESCE(NULLIFZERO(apt.receipts_res_u_pct), 1) AS DECIMAL(36,8)) AS op_receipts_reserve_qty
--	,CAST(mfp.op_receipts_reserve_cost_amt * COALESCE(NULLIFZERO(apt.receipts_res_c_pct), 1) AS DECIMAL(36,8)) AS op_receipts_reserve_cost_amt
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
INNER JOIN date_lkup_wk wk
	ON mfp.week_num = wk.week_idnt
LEFT JOIN apt_pct apt
	 ON apt.month_idnt = wk.month_idnt
	AND apt.banner_country_num = mfp.banner_country_num
	AND apt.fulfill_type_num = mfp.fulfill_type_num
	AND apt.dept_num = mfp.dept_num
	AND apt.receipts_res_u > 0
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON reserve_u
;

--DROP TABLE reserve_c;
CREATE MULTISET VOLATILE TABLE reserve_c AS (
SELECT 
	 mfp.week_num
	,COALESCE(wk.month_idnt, apt.month_idnt) AS month_idnt
	,COALESCE(mfp.banner_country_num, apt.banner_country_num) AS banner_country_num
	,COALESCE(mfp.fulfill_type_num, apt.fulfill_type_num) AS fulfill_type_num
	,COALESCE(mfp.dept_num, apt.dept_num) AS dept_num
	,COALESCE(apt.channel_num, 0) AS channel_num
	,CAST(mfp.sp_receipts_reserve_cost_amt * COALESCE(NULLIFZERO(apt.receipts_res_c_pct), 1) AS DECIMAL(36,8)) AS sp_receipts_reserve_cost_amt
	,CAST(mfp.op_receipts_reserve_cost_amt * COALESCE(NULLIFZERO(apt.receipts_res_c_pct), 1) AS DECIMAL(36,8)) AS op_receipts_reserve_cost_amt
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfp
INNER JOIN date_lkup_wk wk
	ON mfp.week_num = wk.week_idnt
LEFT JOIN apt_pct apt
	 ON apt.month_idnt = wk.month_idnt
	AND apt.banner_country_num = mfp.banner_country_num
	AND apt.fulfill_type_num = mfp.fulfill_type_num
	AND apt.dept_num = mfp.dept_num
	AND apt.receipts_res_c > 0
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON reserve_u
;

/******************************************************************/
/************************** FINAL BASE ****************************/
/******************************************************************/

--DROP TABLE base;
CREATE MULTISET VOLATILE TABLE base AS (
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM bop_u
GROUP BY 1,2,3,4,5,6
UNION
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM bop_c
GROUP BY 1,2,3,4,5,6
UNION
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM eop_u
GROUP BY 1,2,3,4,5,6
UNION
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM eop_c
GROUP BY 1,2,3,4,5,6
UNION
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM receipt_u
GROUP BY 1,2,3,4,5,6
UNION
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM receipt_c
GROUP BY 1,2,3,4,5,6
UNION
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM reserve_u
GROUP BY 1,2,3,4,5,6
UNION
SELECT
	 week_num
	,month_idnt
	,banner_country_num
	,fulfill_type_num
	,dept_num
	,channel_num
FROM reserve_c
GROUP BY 1,2,3,4,5,6
) WITH DATA
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON base
;

/******************************************************************/
/************************ DELETE & INSERT *************************/
/******************************************************************/

DELETE FROM {environment_schema}.mfp_inv_channel_plans{env_suffix};
INSERT INTO {environment_schema}.mfp_inv_channel_plans{env_suffix}
SELECT
	 b.week_num
	,b.month_idnt
	,b.banner_country_num
	,b.fulfill_type_num
	,b.dept_num
	,b.channel_num
	,bop_u.sp_beginning_of_period_active_qty
	,bop_c.sp_beginning_of_period_active_cost_amt
	,bop_u.op_beginning_of_period_active_qty
	,bop_c.op_beginning_of_period_active_cost_amt
	,eop_u.sp_ending_of_period_active_qty
	,eop_c.sp_ending_of_period_active_cost_amt
	,eop_u.op_ending_of_period_active_qty
	,eop_c.op_ending_of_period_active_cost_amt
	,receipt_u.sp_receipts_active_qty
	,receipt_c.sp_receipts_active_cost_amt
	,receipt_u.op_receipts_active_qty
	,receipt_c.op_receipts_active_cost_amt
	,reserve_u.sp_receipts_reserve_qty
	,reserve_c.sp_receipts_reserve_cost_amt
	,reserve_u.op_receipts_reserve_qty
	,reserve_c.op_receipts_reserve_cost_amt
	,CURRENT_TIMESTAMP AS update_timestamp
FROM base b
LEFT JOIN bop_u
	 ON b.week_num           = bop_u.week_num          
	AND b.month_idnt         = bop_u.month_idnt        
	AND b.banner_country_num = bop_u.banner_country_num
	AND b.fulfill_type_num   = bop_u.fulfill_type_num  
	AND b.dept_num           = bop_u.dept_num          
	AND b.channel_num        = bop_u.channel_num       
LEFT JOIN bop_c
	 ON b.week_num           = bop_c.week_num          
	AND b.month_idnt         = bop_c.month_idnt        
	AND b.banner_country_num = bop_c.banner_country_num
	AND b.fulfill_type_num   = bop_c.fulfill_type_num  
	AND b.dept_num           = bop_c.dept_num          
	AND b.channel_num        = bop_c.channel_num       
LEFT JOIN eop_u
	 ON b.week_num           = eop_u.week_num          
	AND b.month_idnt         = eop_u.month_idnt        
	AND b.banner_country_num = eop_u.banner_country_num
	AND b.fulfill_type_num   = eop_u.fulfill_type_num  
	AND b.dept_num           = eop_u.dept_num          
	AND b.channel_num        = eop_u.channel_num       
LEFT JOIN eop_c
	 ON b.week_num           = eop_c.week_num          
	AND b.month_idnt         = eop_c.month_idnt        
	AND b.banner_country_num = eop_c.banner_country_num
	AND b.fulfill_type_num   = eop_c.fulfill_type_num  
	AND b.dept_num           = eop_c.dept_num          
	AND b.channel_num        = eop_c.channel_num       
LEFT JOIN receipt_u
	 ON b.week_num           = receipt_u.week_num          
	AND b.month_idnt         = receipt_u.month_idnt        
	AND b.banner_country_num = receipt_u.banner_country_num
	AND b.fulfill_type_num   = receipt_u.fulfill_type_num  
	AND b.dept_num           = receipt_u.dept_num          
	AND b.channel_num        = receipt_u.channel_num       
LEFT JOIN receipt_c
	 ON b.week_num           = receipt_c.week_num          
	AND b.month_idnt         = receipt_c.month_idnt        
	AND b.banner_country_num = receipt_c.banner_country_num
	AND b.fulfill_type_num   = receipt_c.fulfill_type_num  
	AND b.dept_num           = receipt_c.dept_num          
	AND b.channel_num        = receipt_c.channel_num       
LEFT JOIN reserve_u
	 ON b.week_num           = reserve_u.week_num          
	AND b.month_idnt         = reserve_u.month_idnt        
	AND b.banner_country_num = reserve_u.banner_country_num
	AND b.fulfill_type_num   = reserve_u.fulfill_type_num  
	AND b.dept_num           = reserve_u.dept_num          
	AND b.channel_num        = reserve_u.channel_num       	
LEFT JOIN reserve_c
	 ON b.week_num           = reserve_c.week_num          
	AND b.month_idnt         = reserve_c.month_idnt        
	AND b.banner_country_num = reserve_c.banner_country_num
	AND b.fulfill_type_num   = reserve_c.fulfill_type_num  
	AND b.dept_num           = reserve_c.dept_num          
	AND b.channel_num        = reserve_c.channel_num       	
;

COLLECT STATS
	PRIMARY INDEX (week_num,banner_country_num,fulfill_type_num,channel_num,dept_num) 
		ON {environment_schema}.mfp_inv_channel_plans{env_suffix}
;
