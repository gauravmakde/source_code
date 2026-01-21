/*
APT Reporting LY Cost
Author: Jihyun Yu, Michelle Du
Date Created: 10/18/22
Date Last Updated: 9/26/23

Purpose: Create category supplier last year cost tables.

Tables:
    {environment_schema}.category_priceband_cost_ly{env_suffix}
	{environment_schema}.category_suppliergroup_cost_ly{env_suffix}
*/

--supplier store_channel lookup
CREATE MULTISET VOLATILE TABLE STORE_LKUP AS (
SELECT DISTINCT
	store_num
	, channel_country
	, channel_num
	, TRIM (channel_num || ', ' ||channel_desc) AS channel_label
	, CASE WHEN channel_brand = 'NORDSTROM' THEN 'FP'
		   WHEN channel_brand = 'NORDSTROM_RACK' THEN 'OP'
	  END AS banner
	, CASE WHEN channel_num = 111  THEN 'NORDSTROM_CANADA_STORES'
		   WHEN channel_num = 120  THEN 'NORDSTROM_ONLINE'
		   WHEN channel_num = 121  THEN 'NORDSTROM_CANADA_ONLINE'
		   WHEN channel_num = 110  THEN 'NORDSTROM_STORES'
		   WHEN channel_num = 310  THEN 'NORD US RSWH'
		   WHEN channel_num = 311  THEN 'NORD CA RSWH'
		   WHEN channel_num = 250  THEN 'RACK_ONLINE'
		   WHEN channel_num = 211  THEN 'RACK_CANADA_STORES'
		   WHEN channel_num = 260  THEN 'RACK US RSWH'
		   WHEN channel_num = 261  THEN 'RACK CA RSWH'
		   WHEN channel_num = 210  THEN 'RACK STORES'
	  END AS cluster_name
FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
WHERE channel_num NOT IN (580)
) WITH DATA
PRIMARY INDEX (store_num, channel_num) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    COLUMN(channel_country, channel_num,banner, cluster_name)
    ON STORE_LKUP;

--N category_store_channel_cluster lookup
CREATE MULTISET VOLATILE TABLE STORE_LKUP_NORD AS (
SELECT DISTINCT
    store_num
  	, channel_country
  	, channel_num
  	, TRIM (channel_num || ', ' ||channel_desc) AS channel_label
  	, CASE WHEN channel_brand = 'NORDSTROM' THEN 'FP'
		   WHEN channel_brand = 'NORDSTROM_RACK' THEN 'OP'
	  END AS banner
  	, CASE WHEN channel_num = 111  THEN 'NORDSTROM_CANADA_STORES'
		   WHEN channel_num = 120  THEN 'NORDSTROM_ONLINE'
		   WHEN channel_num = 121  THEN 'NORDSTROM_CANADA_ONLINE'
		   WHEN channel_num = 110  THEN 'NORDSTROM_STORES'
		   WHEN channel_num = 310  THEN 'NORD US RSWH'
		   WHEN channel_num = 311  THEN 'NORD CA RSWH'
	  END AS cluster_name
FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW
WHERE channel_num IN (110,120,111,121,310,311)
) WITH DATA
PRIMARY INDEX (store_num, channel_num) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_country ,channel_num ,banner, cluster_name)
    , COLUMN(store_num)
	ON STORE_LKUP_NORD;

--NR category_store_channel_cluster lookup
CREATE MULTISET VOLATILE TABLE STORE_LKUP_RACK AS (
SELECT DISTINCT
    store_num
	, channel_country
	, channel_num
	, channel_label
	, banner
	, cluster_name
FROM(
	SELECT
		st.store_num
		, st.channel_country
		, st.channel_num
		, TRIM (st.channel_num || ', ' ||st.channel_desc) AS channel_label
		, CASE WHEN st.channel_brand = 'NORDSTROM' THEN 'FP'
			   WHEN st.channel_brand = 'NORDSTROM_RACK' THEN 'OP'
		  END AS banner
		, CASE WHEN st.channel_num = 210 AND grp.peer_group_desc = 'BEST' THEN 'BRAND'
			   WHEN st.channel_num = 210 AND grp.peer_group_desc = 'BETTER' THEN 'HYBRID'
			   WHEN st.channel_num = 210 AND grp.peer_group_desc = 'GOOD' THEN 'PRICE'
			   ELSE grp.peer_group_desc
		  END AS cluster_name
	FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW st
		JOIN PRD_NAP_USR_VWS.STORE_PEER_GROUP_DIM grp
			ON grp.store_num = st.store_num
		WHERE grp.peer_group_type_code in ('OCP')
			AND st.channel_num =210
	UNION ALL
	SELECT
		st.store_num
		, st.channel_country
		, st.channel_num
		, TRIM (st.channel_num || ', ' ||st.channel_desc) AS channel_label
		, CASE WHEN st.channel_brand = 'NORDSTROM' THEN 'FP'
			   WHEN st.channel_brand = 'NORDSTROM_RACK' THEN 'OP'
		  END AS banner
		, CASE WHEN st.channel_num = 250 THEN 'RACK_ONLINE'
			   WHEN st.channel_num = 211 THEN 'RACK_CANADA_STORES'
			   WHEN st.channel_num = 260 THEN 'RACK US RSWH'
			   WHEN st.channel_num = 261 THEN 'RACK CA RSWH'
		  END AS cluster_name
	FROM PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW st
	WHERE st.channel_num IN (260,261,211,250)
    ) T1
) WITH DATA
PRIMARY INDEX (store_num,channel_num) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_country, channel_num, banner, cluster_name)
	, COLUMN(store_num)
    ON STORE_LKUP_RACK;

--department lookup
CREATE MULTISET VOLATILE TABLE DEPT_LKUP_CCP AS (
SELECT DISTINCT
    dept_num
	, division_num
	, subdivision_num
	, TRIM(dept_num || ', ' ||dept_name) AS dept_label
	, TRIM(division_num || ', ' ||division_name) AS division_label
	, TRIM(subdivision_num || ', ' ||subdivision_name) AS subdivision_label
FROM PRD_NAP_USR_VWS.DEPARTMENT_DIM
WHERE dept_num IN (SELECT DISTINCT department_number FROM t2dl_das_apt_reporting.merch_assortment_category_cluster_plan_fact)
) WITH DATA
PRIMARY INDEX (dept_num) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    COLUMN(dept_num)
    ON DEPT_LKUP_CCP;

--ly dates lookup
CREATE MULTISET VOLATILE TABLE LY_DATE_LKUP AS (
SELECT DISTINCT
    month_idnt
  	, month_label
  	, month_454_label
  	, month_start_day_date
	, month_end_day_date
  	--,fiscal_month_num
  	, week_idnt
  	, week_start_day_date
  	, week_end_day_date
  	, week_label
  	, week_num_of_fiscal_month
  	--,fiscal_week_num
  	, quarter_label
  	, fiscal_year_num
FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
WHERE week_end_day_date BETWEEN (current_date - 365 * 2) AND current_date
	AND fiscal_year_num <> 2020
) WITH DATA
PRIMARY INDEX (month_idnt, week_idnt) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN (MONTH_START_DAY_DATE, MONTH_END_DAY_DATE, WEEK_START_DAY_DATE, WEEK_END_DAY_DATE)
    , COLUMN (WEEK_IDNT)
	ON LY_DATE_LKUP;

-- category lookup
CREATE MULTISET VOLATILE TABLE CAT_LKUP AS (
    SELECT DISTINCT
        dept_idnt AS dept_num
        , sku_idnt
        , channel_country
        , quantrix_category AS category
        , fanatics_ind
        , fp_supplier_group
		, op_supplier_group
    FROM T2DL_DAS_ASSORTMENT_DIM.ASSORTMENT_HIERARCHY
) WITH DATA
PRIMARY INDEX (dept_num, sku_idnt, channel_country) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	COLUMN(category)
    , COLUMN (dept_num ,sku_idnt, channel_country)
    , COLUMN(sku_idnt)
    , COLUMN(channel_country)
    , COLUMN(fanatics_ind)
	ON CAT_LKUP;

-- N receipted price band
CREATE MULTISET VOLATILE TABLE RECEIPT_PRICE_BAND_NORD AS (
SELECT
	RMS_SKU_NUM AS sku_idnt
	, channel_country
	, MAX((RECEIPTS_REGULAR_RETAIL + RECEIPTS_CLEARANCE_RETAIL + RECEIPTS_CROSSDOCK_REGULAR_RETAIL + RECEIPTS_CROSSDOCK_CLEARANCE_RETAIL) / (RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS)) AS AUR
FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW a
	JOIN STORE_LKUP_NORD b
		ON a.store_num = b.store_num
	JOIN DEPT_LKUP_CCP dept
		ON dept.dept_num = a.DEPARTMENT_NUM
WHERE (RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS) > 0
GROUP BY 1,2
) WITH DATA
PRIMARY INDEX (sku_idnt, channel_country) ON COMMIT PRESERVE ROWS
;

-- NR receipted price band
CREATE MULTISET VOLATILE TABLE RECEIPT_PRICE_BAND_RACK AS (
SELECT
	sku_idnt
	, channel_country
	, MAX(AUR) as AUR
FROM(
	SELECT
		RMS_SKU_NUM AS sku_idnt
		, channel_country
		, MAX((RECEIPTS_REGULAR_RETAIL + RECEIPTS_CLEARANCE_RETAIL + RECEIPTS_CROSSDOCK_REGULAR_RETAIL + RECEIPTS_CROSSDOCK_CLEARANCE_RETAIL) / (RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS)) AS AUR
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW a
		JOIN STORE_LKUP_RACK b
			ON a.store_num = b.store_num
		JOIN DEPT_LKUP_CCP dept
			ON dept.dept_num = a.DEPARTMENT_NUM
	WHERE (RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS) > 0
	GROUP BY 1,2
	UNION ALL
	SELECT
		tsf.rms_sku_num AS sku_idnt
		, channel_country
		, MAX(PACKANDHOLD_TRANSFER_IN_RETAIL / PACKANDHOLD_TRANSFER_IN_UNITS) AS AUR
	FROM PRD_NAP_USR_VWS.MERCH_TRANSFER_SKU_LOC_WEEK_AGG_FACT_VW tsf
		JOIN STORE_LKUP_RACK b
			ON tsf.store_num = b.store_num
		JOIN DEPT_LKUP_CCP dept
			ON dept.dept_num = tsf.DEPARTMENT_NUM
	WHERE PACKANDHOLD_TRANSFER_IN_UNITS > 0
	GROUP BY 1,2
	UNION ALL
	SELECT
		tsf.rms_sku_num AS sku_idnt
		, channel_country
		, MAX(RACKING_TRANSFER_IN_RETAIL / RACKING_TRANSFER_IN_UNITS) AS AUR
	FROM PRD_NAP_USR_VWS.MERCH_TRANSFER_SKU_LOC_WEEK_AGG_FACT_VW tsf
		JOIN STORE_LKUP_RACK b
			ON tsf.store_num = b.store_num
		JOIN DEPT_LKUP_CCP dept
			ON dept.dept_num = tsf.DEPARTMENT_NUM
	WHERE RACKING_TRANSFER_IN_UNITS > 0
	GROUP BY 1,2
    ) TBL
GROUP BY 1,2
) WITH DATA
PRIMARY INDEX (sku_idnt, channel_country) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(sku_idnt, channel_country)
    ON RECEIPT_PRICE_BAND_RACK;

/* Create Category/Price Band Table at Channel/Cluster Grain */
-- 1. Sales
CREATE MULTISET VOLATILE TABLE PRICEBAND_SALES AS (
  -- Nord
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, sls.month_num AS month_idnt
		, sls.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, sls.department_num
		, sls.dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 25 THEN '$0-25'
			   WHEN AUR <= 50 THEN '$25-50'
			   WHEN AUR <= 100 THEN '$50-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 300 THEN '$200-300'
			   WHEN AUR <= 500 THEN '$300-500'
			   WHEN AUR <= 700 THEN '$500-700'
			   WHEN AUR <= 1000 THEN '$700-1000'
			   WHEN AUR <= 1500 THEN '$1000-1500'
			   WHEN AUR <= 1000000000 THEN 'Over $1500'
			   ELSE 'NO RECEIPT FOUND'
		  END AS price_band
		, sales_cost_curr_code as currency
		, SUM(sls.net_sales_tot_retl) AS net_sls_r
		, SUM(sls.net_sales_tot_cost) AS net_sls_c
		, SUM(sls.NET_SALES_TOT_UNITS) AS net_sls_units
		, SUM(sls.gross_sales_tot_retl) AS gross_sls_r
		, SUM(sls.gross_sales_tot_units) AS gross_sls_units
		, CASE WHEN st.channel_num IN (110, 111, 210, 211) THEN SUM(sls.gross_sales_tot_retl) ELSE CAST(0 AS DECIMAL(38, 6)) END AS demand_ttl_r
		, CASE WHEN st.channel_num IN (110, 111, 210, 211) THEN SUM(sls.gross_sales_tot_units) ELSE CAST(0 AS INTEGER) END AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_SALE_RETURN_SKU_STORE_WEEK_FACT_VW sls
		JOIN LY_DATE_LKUP lyd
			ON sls.week_num = lyd.week_idnt
		JOIN STORE_LKUP_NORD st
			ON sls.store_num = st.store_num
		LEFT JOIN CAT_LKUP cat
			ON sls.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_NORD pb
			ON sls.rms_sku_num  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
	UNION ALL
   --Rack
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, sls.month_num AS month_idnt
		, sls.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, sls.department_num
		, sls.dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 10 THEN '$0-10'
			   WHEN AUR <= 15 THEN '$10-15'
			   WHEN AUR <= 20 THEN '$15-20'
			   WHEN AUR <= 25 THEN '$20-25'
			   WHEN AUR <= 30 THEN '$25-30'
			   WHEN AUR <= 40 THEN '$30-40'
			   WHEN AUR <= 50 THEN '$40-50'
			   WHEN AUR <= 60 THEN '$50-60'
			   WHEN AUR <= 80 THEN '$60-80'
			   WHEN AUR <= 100 THEN '$80-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 1000000000 THEN 'Over $200'
			   ELSE 'NO RECEIPT FOUND'
		  END AS price_band
		, sales_cost_curr_code as currency
		, SUM(sls.net_sales_tot_retl) AS net_sls_r
		, SUM(sls.net_sales_tot_cost) AS net_sls_c
		, SUM(sls.NET_SALES_TOT_UNITS) AS net_sls_units
		, SUM(sls.gross_sales_tot_retl) AS gross_sls_r
		, SUM(sls.gross_sales_tot_units) AS gross_sls_units
		, CASE WHEN st.channel_num IN (110, 111, 210, 211) THEN SUM(sls.gross_sales_tot_retl) ELSE CAST(0 AS DECIMAL(38, 6)) END AS demand_ttl_r
		, CASE WHEN st.channel_num IN (110, 111, 210, 211) THEN SUM(sls.gross_sales_tot_units) ELSE CAST(0 AS INTEGER) END AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_SALE_RETURN_SKU_STORE_WEEK_FACT_VW sls
		JOIN LY_DATE_LKUP lyd
			ON sls.week_num = lyd.week_idnt
		JOIN STORE_LKUP_RACK st
			ON sls.STORE_NUM = st.store_num
		LEFT JOIN  CAT_LKUP cat
			ON sls.RMS_SKU_NUM = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_RACK pb
			ON sls.RMS_SKU_NUM  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, price_band) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band)
    , COLUMN(channel_num)
    , COLUMN(week_idnt)
    , COLUMN(department_num)
    , COLUMN(quantrix_category)
    , COLUMN(price_band)
    ON PRICEBAND_SALES;

-- 2. Demand
CREATE MULTISET VOLATILE TABLE PRICEBAND_DEMAND AS (
  -- Nord
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, dmd.month_num AS month_idnt
		, dmd.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, dmd.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 25 THEN '$0-25'
			   WHEN AUR <= 50 THEN '$25-50'
			   WHEN AUR <= 100 THEN '$50-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 300 THEN '$200-300'
			   WHEN AUR <= 500 THEN '$300-500'
			   WHEN AUR <= 700 THEN '$500-700'
			   WHEN AUR <= 1000 THEN '$700-1000'
			   WHEN AUR <= 1500 THEN '$1000-1500'
			   WHEN AUR <= 1000000000 THEN 'Over $1500'
			   ELSE 'NO RECEIPT FOUND'
		  END AS price_band
		, CAST((CASE WHEN st.channel_country = 'US' THEN 'USD' WHEN st.channel_country = 'CA' THEN 'CAD' END) AS VARCHAR (40)) AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CASE WHEN st.channel_num NOT IN (110, 111, 210, 211) THEN SUM(dmd.demand_tot_amt) ELSE CAST(0 AS DECIMAL(38, 6)) END AS demand_ttl_r
		, CASE WHEN st.channel_num NOT IN (110, 111, 210, 211) THEN SUM(dmd.demand_tot_qty) ELSE CAST(0 AS INTEGER) END AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_DEMAND_SKU_STORE_WEEK_AGG_FACT_VW dmd
		JOIN LY_DATE_LKUP lyd
			ON dmd.week_num = lyd.week_idnt
		JOIN STORE_LKUP_NORD st
			ON dmd.STORE_NUM = st.store_num
		LEFT JOIN  CAT_LKUP cat
			ON dmd.RMS_SKU_NUM = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_NORD pb
			ON dmd.RMS_SKU_NUM  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
	UNION ALL
	--Rack
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, dmd.month_num AS month_idnt
		, dmd.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, dmd.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 10 THEN '$0-10'
			   WHEN AUR <= 15 THEN '$10-15'
			   WHEN AUR <= 20 THEN '$15-20'
			   WHEN AUR <= 25 THEN '$20-25'
			   WHEN AUR <= 30 THEN '$25-30'
			   WHEN AUR <= 40 THEN '$30-40'
			   WHEN AUR <= 50 THEN '$40-50'
			   WHEN AUR <= 60 THEN '$50-60'
			   WHEN AUR <= 80 THEN '$60-80'
			   WHEN AUR <= 100 THEN '$80-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 1000000000 THEN 'Over $200'
			   ELSE 'NO RECEIPT FOUND'
		  END  AS price_band
		, CAST((CASE WHEN st.channel_country = 'US' THEN 'USD' WHEN st.channel_country = 'CA' THEN 'CAD' END) AS VARCHAR (40)) AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
		, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CASE WHEN st.channel_num NOT IN (110, 111, 210, 211) THEN SUM(dmd.demand_tot_amt) ELSE CAST(0 AS DECIMAL(38, 6)) END AS demand_ttl_r
		, CASE WHEN st.channel_num NOT IN (110, 111, 210, 211) THEN SUM(dmd.demand_tot_qty) ELSE CAST(0 AS INTEGER) END AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_DEMAND_SKU_STORE_WEEK_AGG_FACT_VW dmd
		JOIN LY_DATE_LKUP lyd
			ON dmd.week_num = lyd.week_idnt
		JOIN STORE_LKUP_RACK st
			ON dmd.STORE_NUM = st.store_num
		LEFT JOIN  CAT_LKUP cat
			ON dmd.RMS_SKU_NUM = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_RACK pb
			ON dmd.RMS_SKU_NUM  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, price_band) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band)
    , COLUMN(channel_num)
    , COLUMN(week_idnt)
    , COLUMN(department_num)
    , COLUMN(quantrix_category)
    , COLUMN(price_band)
    ON PRICEBAND_DEMAND;

-- 3.A. Inventory FP
CREATE MULTISET VOLATILE TABLE PRICEBAND_INVENTORY_FP AS (
  -- Nord
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, inv.month_num AS month_idnt
		, inv.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, inv.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 25 THEN '$0-25'
			   WHEN AUR <= 50 THEN '$25-50'
			   WHEN AUR <= 100 THEN '$50-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 300 THEN '$200-300'
			   WHEN AUR <= 500 THEN '$300-500'
			   WHEN AUR <= 700 THEN '$500-700'
			   WHEN AUR <= 1000 THEN '$700-1000'
			   WHEN AUR <= 1500 THEN '$1000-1500'
			   WHEN AUR <= 1000000000 THEN 'Over $1500'
			   ELSE 'NO RECEIPT FOUND'
		  END AS price_band
		, inv_cost_currency_code as currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, SUM(eoh_total_cost + eoh_in_transit_total_cost) AS eoh_ttl_c
		, SUM(eoh_total_units + eoh_in_transit_total_units) AS eoh_ttl_units
		, SUM(boh_total_cost + boh_in_transit_total_cost) AS boh_ttl_c
		, SUM(boh_total_units + boh_in_transit_total_units) AS boh_ttl_units
		, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN boh_total_cost + boh_in_transit_total_cost END ) AS beginofmonth_bop_c
		, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN boh_total_units + boh_in_transit_total_units END ) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_INVENTORY_SKU_STORE_WEEK_FACT_VW inv
		JOIN LY_DATE_LKUP lyd
			ON inv.week_num = lyd.week_idnt
		JOIN STORE_LKUP_NORD st
			ON inv.store_num = st.store_num
		LEFT JOIN  CAT_LKUP cat
			ON inv.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_NORD pb
			ON inv.rms_sku_num  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, price_band) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band)
	, COLUMN(channel_num)
	, COLUMN(week_idnt)
	, COLUMN(department_num)
	, COLUMN(quantrix_category)
	, COLUMN(price_band)
	ON PRICEBAND_INVENTORY_FP;

-- 3.B. Inventory OP
CREATE MULTISET VOLATILE TABLE PRICEBAND_INVENTORY_OP AS (
  -- Rack
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, inv.month_num AS month_idnt
		, inv.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, inv.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 10 THEN '$0-10'
			   WHEN AUR <= 15 THEN '$10-15'
			   WHEN AUR <= 20 THEN '$15-20'
			   WHEN AUR <= 25 THEN '$20-25'
			   WHEN AUR <= 30 THEN '$25-30'
			   WHEN AUR <= 40 THEN '$30-40'
			   WHEN AUR <= 50 THEN '$40-50'
			   WHEN AUR <= 60 THEN '$50-60'
			   WHEN AUR <= 80 THEN '$60-80'
			   WHEN AUR <= 100 THEN '$80-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 1000000000 THEN 'Over $200'
			   ELSE 'NO RECEIPT FOUND'
		  END AS price_band
		, inv_cost_currency_code as currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, SUM(eoh_total_cost + eoh_in_transit_total_cost) AS eoh_ttl_c
		, SUM(eoh_total_units + eoh_in_transit_total_units) AS eoh_ttl_units
		, SUM(boh_total_cost + boh_in_transit_total_cost) AS boh_ttl_c
		, SUM(boh_total_units + boh_in_transit_total_units) AS boh_ttl_units
		, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN boh_total_cost + boh_in_transit_total_cost END ) AS beginofmonth_bop_c
		, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN boh_total_units + boh_in_transit_total_units END ) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_INVENTORY_SKU_STORE_WEEK_FACT_VW inv
		JOIN LY_DATE_LKUP lyd
			ON inv.week_num = lyd.week_idnt
		JOIN STORE_LKUP_RACK st
			ON inv.store_num = st.store_num
		LEFT JOIN  CAT_LKUP cat
			ON inv.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_RACK pb
			ON inv.rms_sku_num  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, price_band) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band)
	, COLUMN(channel_num)
	, COLUMN(week_idnt)
	, COLUMN(department_num)
	, COLUMN(quantrix_category)
	, COLUMN(price_band)
	ON PRICEBAND_INVENTORY_OP;

-- 4. Receipts
CREATE MULTISET VOLATILE TABLE PRICEBAND_RECEIPTS AS (
  -- Nord
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, rpt.month_num AS month_idnt
		, rpt.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, rpt.department_num
		, rpt.dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 25 THEN '$0-25'
			   WHEN AUR <= 50 THEN '$25-50'
			   WHEN AUR <= 100 THEN '$50-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 300 THEN '$200-300'
			   WHEN AUR <= 500 THEN '$300-500'
			   WHEN AUR <= 700 THEN '$500-700'
			   WHEN AUR <= 1000 THEN '$700-1000'
			   WHEN AUR <= 1500 THEN '$1000-1500'
			   WHEN AUR <= 1000000000 THEN 'Over $1500'
			   ELSE 'NO RECEIPT FOUND'
		  END AS price_band
		, receipts_cost_currency_code AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, SUM(RECEIPTS_REGULAR_COST + RECEIPTS_CLEARANCE_COST + RECEIPTS_CROSSDOCK_REGULAR_COST + RECEIPTS_CROSSDOCK_CLEARANCE_COST) AS ttl_porcpt_c
		, SUM(RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW rpt
		JOIN LY_DATE_LKUP lyd
			ON rpt.week_num = lyd.week_idnt
		JOIN STORE_LKUP_NORD st
			ON rpt.store_num = st.store_num
		LEFT JOIN  CAT_LKUP cat
			ON rpt.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_NORD pb
			ON rpt.rms_sku_num  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
  	UNION ALL
  -- Rack
  	SELECT
  		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, rpt.month_num AS month_idnt
		, rpt.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, rpt.department_num
		, rpt.dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 10 THEN '$0-10'
			   WHEN AUR <= 15 THEN '$10-15'
			   WHEN AUR <= 20 THEN '$15-20'
			   WHEN AUR <= 25 THEN '$20-25'
			   WHEN AUR <= 30 THEN '$25-30'
			   WHEN AUR <= 40 THEN '$30-40'
			   WHEN AUR <= 50 THEN '$40-50'
			   WHEN AUR <= 60 THEN '$50-60'
			   WHEN AUR <= 80 THEN '$60-80'
			   WHEN AUR <= 100 THEN '$80-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 1000000000 THEN 'Over $200'
			   ELSE 'NO RECEIPT FOUND'
		  END  AS price_band
		, receipts_cost_currency_code AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, SUM(RECEIPTS_REGULAR_COST + RECEIPTS_CLEARANCE_COST + RECEIPTS_CROSSDOCK_REGULAR_COST + RECEIPTS_CROSSDOCK_CLEARANCE_COST) AS ttl_porcpt_c
		, SUM(RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW rpt
	JOIN LY_DATE_LKUP lyd
		ON rpt.week_num = lyd.week_idnt
	JOIN STORE_LKUP_RACK st
		ON rpt.store_num = st.store_num
	LEFT JOIN  CAT_LKUP cat
		ON rpt.rms_sku_num = cat.sku_idnt
		AND st.channel_country = cat.channel_country
	LEFT JOIN RECEIPT_PRICE_BAND_RACK pb
		ON rpt.rms_sku_num  = pb.sku_idnt
		AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
  	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, price_band) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band)
	, COLUMN(channel_num)
	, COLUMN(week_idnt)
	, COLUMN(department_num)
	, COLUMN(quantrix_category)
	, COLUMN(price_band)
	ON PRICEBAND_RECEIPTS;


-- 5. Transfers
CREATE MULTISET VOLATILE TABLE PRICEBAND_TRANSFERS AS (
  -- Nord
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, tsf.month_num AS month_idnt
		, tsf.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, tsf.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 25 THEN '$0-25'
			   WHEN AUR <= 50 THEN '$25-50'
			   WHEN AUR <= 100 THEN '$50-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 300 THEN '$200-300'
			   WHEN AUR <= 500 THEN '$300-500'
			   WHEN AUR <= 700 THEN '$500-700'
			   WHEN AUR <= 1000 THEN '$700-1000'
			   WHEN AUR <= 1500 THEN '$1000-1500'
			   WHEN AUR <= 1000000000 THEN 'Over $1500'
			   ELSE 'NO RECEIPT FOUND'
		  END AS price_band
		, CAST((CASE WHEN st.channel_country = 'US' THEN 'USD' WHEN st.channel_country = 'CA' THEN 'CAD' END) AS VARCHAR(40)) AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, SUM(PACKANDHOLD_TRANSFER_IN_COST) AS pah_tsfr_in_c
		, SUM(PACKANDHOLD_TRANSFER_IN_UNITS) AS pah_tsfr_in_u
		, SUM(RESERVESTOCK_TRANSFER_IN_COST ) AS rs_stk_tsfr_in_c
		, SUM(RESERVESTOCK_TRANSFER_IN_UNITS) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_TRANSFER_SKU_LOC_WEEK_AGG_FACT_VW tsf
		JOIN LY_DATE_LKUP lyd
			ON tsf.week_num = lyd.week_idnt
		JOIN STORE_LKUP_NORD st
			ON tsf.store_num = st.store_num
		LEFT JOIN  CAT_LKUP cat
			ON tsf.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_NORD pb
			ON tsf.rms_sku_num  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
	UNION ALL
	-- Rack
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, tsf.month_num AS month_idnt
		, tsf.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, tsf.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category as quantrix_category
		, CASE WHEN AUR <= 0.01 THEN 'NO RECEIPT FOUND'
			   WHEN AUR <= 10 THEN '$0-10'
			   WHEN AUR <= 15 THEN '$10-15'
			   WHEN AUR <= 20 THEN '$15-20'
			   WHEN AUR <= 25 THEN '$20-25'
			   WHEN AUR <= 30 THEN '$25-30'
			   WHEN AUR <= 40 THEN '$30-40'
			   WHEN AUR <= 50 THEN '$40-50'
			   WHEN AUR <= 60 THEN '$50-60'
			   WHEN AUR <= 80 THEN '$60-80'
			   WHEN AUR <= 100 THEN '$80-100'
			   WHEN AUR <= 150 THEN '$100-150'
			   WHEN AUR <= 200 THEN '$150-200'
			   WHEN AUR <= 1000000000 THEN 'Over $200'
			   ELSE 'NO RECEIPT FOUND'
		  END AS price_band
		, CAST((CASE WHEN st.channel_country = 'US' THEN 'USD' WHEN st.channel_country = 'CA' THEN 'CAD' END) AS VARCHAR(40)) AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, SUM(PACKANDHOLD_TRANSFER_IN_COST) AS pah_tsfr_in_c
		, SUM(PACKANDHOLD_TRANSFER_IN_UNITS) AS pah_tsfr_in_u
		, SUM(RESERVESTOCK_TRANSFER_IN_COST ) AS rs_stk_tsfr_in_c
		, SUM(RESERVESTOCK_TRANSFER_IN_UNITS) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_TRANSFER_SKU_LOC_WEEK_AGG_FACT_VW tsf
		JOIN LY_DATE_LKUP lyd
			ON tsf.week_num = lyd.week_idnt
		JOIN STORE_LKUP_RACK st
			ON tsf.store_num = st.store_num
		LEFT JOIN  CAT_LKUP cat
			ON tsf.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
		LEFT JOIN RECEIPT_PRICE_BAND_RACK pb
			ON tsf.rms_sku_num  = pb.sku_idnt
			AND st.channel_country = pb.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, price_band) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, price_band)
	, COLUMN(channel_num)
	, COLUMN(week_idnt)
	, COLUMN(department_num)
	, COLUMN(quantrix_category)
	, COLUMN(price_band)
	ON PRICEBAND_TRANSFERS;

-- Union all & insert into final category/priceband table
DELETE FROM {environment_schema}.category_priceband_cost_ly{env_suffix} ALL;
INSERT INTO {environment_schema}.category_priceband_cost_ly{env_suffix}
SELECT * FROM PRICEBAND_SALES
UNION ALL
SELECT * FROM PRICEBAND_DEMAND
UNION ALL
SELECT * FROM PRICEBAND_INVENTORY_FP
UNION ALL
SELECT * FROM PRICEBAND_INVENTORY_OP
UNION ALL
SELECT * FROM PRICEBAND_RECEIPTS
UNION ALL
SELECT * FROM PRICEBAND_TRANSFERS
;

COLLECT STATS
    PRIMARY INDEX (week_idnt, week_start_day_date, month_idnt, channel_num, banner, channel_country, department_num, quantrix_category, price_band)
	, COLUMN (week_idnt, quantrix_category, price_band)
	ON {environment_schema}.category_priceband_cost_ly{env_suffix};


/* Create Category/Supplier Table at Channel/Cluster Grain */
-- 1. Sales
CREATE MULTISET VOLATILE TABLE SUPPLIER_SALES AS (
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, sls.month_num AS month_idnt
		, sls.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, sls.department_num
		, sls.dropship_ind
		, cat.category AS quantrix_category
		, CASE WHEN st.banner = 'FP' THEN COALESCE(cat.fp_supplier_group, 'OTHER')
			   WHEN st.banner = 'OP' THEN COALESCE(cat.op_supplier_group, 'OTHER')
			   ELSE 'OTHER'
		  END AS supplier_group
		, sales_cost_curr_code as currency
		, SUM(sls.net_sales_tot_retl) as net_sls_r
		, SUM(sls.net_sales_tot_cost) as net_sls_c
		, SUM(sls.NET_SALES_TOT_UNITS) AS net_sls_units
		, SUM(sls.gross_sales_tot_retl) AS gross_sls_r
		, SUM(sls.gross_sales_tot_units) AS gross_sls_units
		, CASE WHEN st.channel_num IN (110, 111, 210, 211) THEN SUM(sls.gross_sales_tot_retl) ELSE CAST(0 AS DECIMAL(38, 6)) END AS demand_ttl_r
		, CASE WHEN st.channel_num IN (110, 111, 210, 211) THEN SUM(sls.gross_sales_tot_units) ELSE CAST(0 AS INTEGER) END AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_SALE_RETURN_SKU_STORE_WEEK_FACT_VW sls
		JOIN LY_DATE_LKUP lyd
			ON sls.week_num = lyd.week_idnt
		JOIN STORE_LKUP st
			ON sls.store_num = st.store_num
		JOIN CAT_LKUP cat
			ON sls.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind)
    , COLUMN(channel_num)
    , COLUMN(week_idnt)
    , COLUMN(department_num)
    , COLUMN(quantrix_category)
    , COLUMN(supplier_group)
    , COLUMN(dropship_ind)
    ON SUPPLIER_SALES;

-- 2. Demand
CREATE MULTISET VOLATILE TABLE SUPPLIER_DEMAND AS (
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, dmd.month_num AS month_idnt
		, dmd.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, dmd.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category AS quantrix_category
		, CASE WHEN st.banner = 'FP' THEN COALESCE(cat.fp_supplier_group, 'OTHER')
			   WHEN st.banner = 'OP' THEN COALESCE(cat.op_supplier_group, 'OTHER')
			   ELSE 'OTHER'
		  END AS supplier_group
		, CAST((CASE WHEN st.channel_country = 'US' THEN 'USD' WHEN st.channel_country = 'CA' THEN 'CAD' END) AS VARCHAR (40)) AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CASE WHEN st.channel_num NOT IN (110, 111, 210, 211) THEN SUM(dmd.demand_tot_amt) ELSE CAST(0 AS DECIMAL(38, 6)) END AS demand_ttl_r
		, CASE WHEN st.channel_num NOT IN (110, 111, 210, 211) THEN SUM(dmd.demand_tot_qty) ELSE CAST(0 AS INTEGER) END AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_DEMAND_SKU_STORE_WEEK_AGG_FACT_VW dmd
		JOIN LY_DATE_LKUP lyd
			ON dmd.week_num = lyd.week_idnt
		JOIN STORE_LKUP st
			ON dmd.store_num = st.store_num
		JOIN CAT_LKUP cat
			ON dmd.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
    COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind)
    , COLUMN(channel_num)
    , COLUMN(week_idnt)
    , COLUMN(department_num)
    , COLUMN(quantrix_category)
    , COLUMN(supplier_group)
    , COLUMN(dropship_ind)
    ON SUPPLIER_DEMAND;


-- 3. Inventory FP
CREATE MULTISET VOLATILE TABLE SUPPLIER_INVENTORY_FP AS (
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, inv.month_num AS month_idnt
		, inv.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, inv.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category AS quantrix_category
		, CASE WHEN st.banner = 'FP' THEN COALESCE(cat.fp_supplier_group, 'OTHER')
			   WHEN st.banner = 'OP' THEN COALESCE(cat.op_supplier_group, 'OTHER')
			   ELSE 'OTHER'
		  END AS supplier_group
		, inv_cost_currency_code as currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, SUM(eoh_total_cost + eoh_in_transit_total_cost) AS eoh_ttl_c
		, SUM(eoh_total_units + eoh_in_transit_total_units) AS eoh_ttl_units
		, SUM(boh_total_cost + boh_in_transit_total_cost) AS boh_ttl_c
		, SUM(boh_total_units + boh_in_transit_total_units) AS boh_ttl_units
		, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN boh_total_cost + boh_in_transit_total_cost END ) AS beginofmonth_bop_c
		, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN boh_total_units + boh_in_transit_total_units END ) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_INVENTORY_SKU_STORE_WEEK_FACT_VW inv
		JOIN LY_DATE_LKUP lyd
			ON inv.week_num = lyd.week_idnt
		JOIN STORE_LKUP st
			ON inv.store_num = st.store_num
			AND st.banner = 'FP'
		JOIN CAT_LKUP cat
			ON inv.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind)
	, COLUMN(channel_num)
	, COLUMN(week_idnt)
	, COLUMN(department_num)
	, COLUMN(quantrix_category)
	, COLUMN(supplier_group)
	, COLUMN(dropship_ind)
	ON SUPPLIER_INVENTORY_FP;


-- 3. Inventory OP
CREATE MULTISET VOLATILE TABLE SUPPLIER_INVENTORY_OP AS (
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, inv.month_num AS month_idnt
		, inv.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, inv.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category AS quantrix_category
		, CASE WHEN st.banner = 'FP' THEN COALESCE(cat.fp_supplier_group, 'OTHER')
			   WHEN st.banner = 'OP' THEN COALESCE(cat.op_supplier_group, 'OTHER')
			   ELSE 'OTHER'
		  END AS supplier_group
		, inv_cost_currency_code as currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, SUM(eoh_total_cost + eoh_in_transit_total_cost) AS eoh_ttl_c
		, SUM(eoh_total_units + eoh_in_transit_total_units) AS eoh_ttl_units
		, SUM(boh_total_cost + boh_in_transit_total_cost) AS boh_ttl_c
		, SUM(boh_total_units + boh_in_transit_total_units) AS boh_ttl_units
		, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN boh_total_cost + boh_in_transit_total_cost END ) AS beginofmonth_bop_c
		, SUM(CASE WHEN week_num_of_fiscal_month= 1 THEN boh_total_units + boh_in_transit_total_units END ) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_INVENTORY_SKU_STORE_WEEK_FACT_VW inv
		JOIN LY_DATE_LKUP lyd
			ON inv.week_num = lyd.week_idnt
		JOIN STORE_LKUP st
			ON inv.store_num = st.store_num
			AND st.banner = 'OP'
		JOIN CAT_LKUP cat
			ON inv.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind)
	, COLUMN(channel_num)
	, COLUMN(week_idnt)
	, COLUMN(department_num)
	, COLUMN(quantrix_category)
	, COLUMN(supplier_group)
	, COLUMN(dropship_ind)
	ON SUPPLIER_INVENTORY_OP;


-- 4. Receipts
CREATE MULTISET VOLATILE TABLE SUPPLIER_RECEIPTS AS (
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, rpt.month_num AS month_idnt
		, rpt.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, rpt.department_num
		, rpt.dropship_ind
		, cat.category AS quantrix_category
		, CASE WHEN st.banner = 'FP' THEN COALESCE(cat.fp_supplier_group, 'OTHER')
			   WHEN st.banner = 'OP' THEN COALESCE(cat.op_supplier_group, 'OTHER')
			   ELSE 'OTHER'
		  END AS supplier_group
		, receipts_cost_currency_code AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_unit
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, SUM(RECEIPTS_REGULAR_COST + RECEIPTS_CLEARANCE_COST + RECEIPTS_CROSSDOCK_REGULAR_COST + RECEIPTS_CROSSDOCK_CLEARANCE_COST) AS ttl_porcpt_c
		, SUM(RECEIPTS_REGULAR_UNITS + RECEIPTS_CLEARANCE_UNITS + RECEIPTS_CROSSDOCK_REGULAR_UNITS + RECEIPTS_CROSSDOCK_CLEARANCE_UNITS) AS ttl_porcpt_c_units
		, CAST(0 AS DECIMAL(38, 4)) AS pah_tsfr_in_c
		, CAST(0 AS INTEGER) AS pah_tsfr_in_u
		, CAST(0 AS DECIMAL(38, 4)) AS rs_stk_tsfr_in_c
		, CAST(0 AS INTEGER) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_PORECEIPT_SKU_STORE_WEEK_FACT_VW rpt
		JOIN LY_DATE_LKUP lyd
			ON rpt.week_num = lyd.week_idnt
		JOIN STORE_LKUP st
			ON rpt.store_num = st.store_num
		JOIN  CAT_LKUP cat
			ON rpt.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind)
	, COLUMN(channel_num)
	, COLUMN(week_idnt)
	, COLUMN(department_num)
	, COLUMN(quantrix_category)
	, COLUMN(supplier_group)
	, COLUMN(dropship_ind)
	ON SUPPLIER_RECEIPTS;


-- 5. Transfers
CREATE MULTISET VOLATILE TABLE SUPPLIER_TRANSFERS AS (
	SELECT
		st.channel_num
		, st.cluster_name
		, st.channel_country
		, st.banner
		, tsf.month_num AS month_idnt
		, tsf.week_num AS week_idnt
		, lyd.month_start_day_date
		, lyd.month_end_day_date
		, lyd.week_start_day_date
		, lyd.week_end_day_date
		, tsf.department_num
		, CAST(NULL AS CHAR(1)) AS dropship_ind
		, cat.category AS quantrix_category
		, CASE WHEN st.banner = 'FP' THEN COALESCE(cat.fp_supplier_group, 'OTHER')
			   WHEN st.banner = 'OP' THEN COALESCE(cat.op_supplier_group, 'OTHER')
			   ELSE 'OTHER'
		  END AS supplier_group
		, CAST((CASE WHEN st.channel_country = 'US' THEN 'USD' WHEN st.channel_country = 'CA' THEN 'CAD' END) AS VARCHAR (40)) AS currency
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_r
		, CAST(0 AS DECIMAL(38, 4)) AS net_sls_c
		, CAST(0 AS INTEGER) AS net_sls_units
	 	, CAST(0 AS DECIMAL(38, 4)) AS gross_sls_r
		, CAST(0 AS INTEGER) AS gross_sls_units
		, CAST(0 AS DECIMAL(38, 6)) AS demand_ttl_r
		, CAST(0 AS INTEGER) AS demand_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS eoh_ttl_c
		, CAST(0 AS INTEGER) AS eoh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS boh_ttl_c
		, CAST(0 AS INTEGER) AS boh_ttl_units
		, CAST(0 AS DECIMAL(38, 2)) AS beginofmonth_bop_c
		, CAST(0 AS INTEGER) AS beginofmonth_bop_u
		, CAST(0 AS DECIMAL(38, 4)) AS ttl_porcpt_c
		, CAST(0 AS INTEGER) AS ttl_porcpt_c_units
		, SUM(PACKANDHOLD_TRANSFER_IN_COST) AS pah_tsfr_in_c
		, SUM(PACKANDHOLD_TRANSFER_IN_UNITS) AS pah_tsfr_in_u
		, SUM(RESERVESTOCK_TRANSFER_IN_COST ) AS rs_stk_tsfr_in_c
		, SUM(RESERVESTOCK_TRANSFER_IN_UNITS) AS rs_stk_tsfr_in_u
	FROM PRD_NAP_USR_VWS.MERCH_TRANSFER_SKU_LOC_WEEK_AGG_FACT_VW tsf
		JOIN LY_DATE_LKUP lyd
			ON tsf.week_num = lyd.week_idnt
		JOIN STORE_LKUP st
			ON tsf.store_num = st.store_num
		JOIN CAT_LKUP cat
			ON tsf.rms_sku_num = cat.sku_idnt
			AND st.channel_country = cat.channel_country
	WHERE cat.fanatics_ind = 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA
PRIMARY INDEX (channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind) ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	COLUMN(channel_num, week_idnt, department_num, quantrix_category, supplier_group, dropship_ind)
	, COLUMN(channel_num)
	, COLUMN(week_idnt)
	, COLUMN(department_num)
	, COLUMN(quantrix_category)
	, COLUMN(supplier_group)
	, COLUMN(dropship_ind)
	ON SUPPLIER_TRANSFERS;

DELETE FROM {environment_schema}.category_suppliergroup_cost_ly{env_suffix} ALL;
INSERT INTO {environment_schema}.category_suppliergroup_cost_ly{env_suffix}
SELECT * FROM SUPPLIER_SALES
UNION ALL
SELECT * FROM SUPPLIER_DEMAND
UNION ALL
SELECT * FROM SUPPLIER_INVENTORY_FP
UNION ALL
SELECT * FROM SUPPLIER_INVENTORY_OP
UNION ALL
SELECT * FROM SUPPLIER_RECEIPTS
UNION ALL
SELECT * FROM SUPPLIER_TRANSFERS
;

COLLECT STATS
    PRIMARY INDEX (week_idnt, week_start_day_date, month_idnt, channel_num, banner, channel_country, department_num, quantrix_category, supplier_group)
    ON {environment_schema}.category_suppliergroup_cost_ly{env_suffix};
