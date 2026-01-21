SET QUERY_BAND = 'App_ID=APP08172;
DAG_ID=brand_weekly_fact_11521_ACE_ENG;
Task_Name=brand_weekly_fact;'
     FOR SESSION VOLATILE;

--T2/Table Name: {nmn_t2_schema}.BRAND_WEEKLY_FACT
--Team/Owner: Customer Engagement Analytics / NMN
--Date Created/Modified: 2023-06-29
--Note:
-- This table supports the NMN Reporting Stack Brand Overview dashboard.
-- Weekly Refresh

CREATE MULTISET VOLATILE TABLE date_variables AS (
    SELECT
        a.week_idnt AS start_week_idnt,
        b.week_idnt AS end_week_idnt,
        a.day_date AS start_date,
        b.day_date AS end_date
    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM a
    JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM b
    ON a.day_date = {start_date} AND b.day_date = {end_date}
) WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE LY_week_mapping AS (
	SELECT	
	   DISTINCT Trim(a.week_idnt) AS week_idnt,
       Trim(b.week_idnt) AS last_year_wk_idnt,
       Trim(a.month_idnt) AS month_idnt,
       Trim(a.quarter_idnt) AS quarter_idnt,
       Trim(a.fiscal_year_num) AS fiscal_year_num
	FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM a
	LEFT JOIN (
		SELECT DISTINCT 
			week_idnt,
	        day_idnt,
	        day_date
	    FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM) b 
	ON b.day_idnt = a.day_idnt_last_year_realigned
	WHERE a.week_idnt BETWEEN 201901 AND 202552
) WITH DATA PRIMARY INDEX (week_idnt) ON COMMIT PRESERVE ROWS;


COLLECT STATISTICS COLUMN (LAST_YEAR_WK_IDNT ,MONTH_IDNT ,QUARTER_IDNT ,FISCAL_YEAR_NUM) ON LY_week_mapping;
COLLECT STATISTICS COLUMN (LAST_YEAR_WK_IDNT ,QUARTER_IDNT) ON LY_week_mapping;
COLLECT STATISTICS COLUMN (WEEK_IDNT) ON LY_week_mapping;

CREATE MULTISET VOLATILE TABLE sku_id_map AS (
	SELECT DISTINCT 
		Trim(sku.rms_sku_num) AS rms_sku_num,
		sku.channel_country AS channel_country,
		Trim(Concat (sku.div_num,', ',sku.div_desc)) AS div_label,
		sku.div_num AS div_num,
		Trim(Concat (sku.grp_num,', ',sku.grp_desc)) AS grp_label,
		sku.grp_num AS grp_num,
		Trim(Concat (sku.dept_num,', ',sku.dept_desc)) AS dept_label,
		sku.dept_num AS dept_num,
		Trim(Concat (sku.class_num,', ',sku.class_desc)) AS class_label,
		sku.class_num AS class_num,
		sku.brand_name AS brand_name,
		v.npg_flag,
		v.vendor_name AS supplier_name
	FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
	LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM v 
		ON sku.prmy_supp_num = v.vendor_num
	LEFT JOIN PRD_NAP_USR_VWS.VENDOR_PAYTO_RELATIONSHIP_DIM vr 
		ON sku.prmy_supp_num = vr.order_from_vendor_num
	WHERE 1 = 1
		AND   v.vendor_status_code = 'A'
		AND   vr.payto_vendor_status_code = 'A'
		AND   sku.channel_country='US'
) WITH DATA PRIMARY INDEX (rms_sku_num,channel_country) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,div_label ,DIV_NUM ,grp_label ,GRP_NUM ,dept_label ,DEPT_NUM ,class_label ,CLASS_NUM,BRAND_NAME ,NPG_FLAG ,SUPPLIER_NAME) ON sku_id_map;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM) ON sku_id_map;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,CHANNEL_COUNTRY) ON sku_id_map;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY) ON sku_id_map;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,grp_label) ON sku_id_map;

-- Sales, Returns, and Inventory
CREATE MULTISET VOLATILE TABLE sales_units_week_level AS (
	SELECT 
		a.chnl_idnt AS channel_num,
		Trim(a.week_idnt) AS week_idnt,
		Trim(lw.last_year_wk_idnt) AS last_year_wk_idnt,
		Trim(lw.month_idnt) AS month_idnt,
		Trim(lw.quarter_idnt) AS quarter_idnt,
		Trim(lw.fiscal_year_num) AS fiscal_year_num,
		b.channel_country,
		b.div_label,
		b.div_num,
		b.grp_label,
		b.grp_num,
		b.dept_label,
		b.dept_num,
		b.class_label,
		b.class_num,
		b.brand_name,
		b.npg_flag,
		b.supplier_name,
		Sum(a.sales_dollars) AS sales_dollars,
		Sum(a.return_dollars) AS return_dollars,
		Sum(a.sales_units) AS sales_units,
		Sum(a.return_units) AS return_units,
		Sum(a.eoh_units) AS eoh_units
	FROM t2dl_das_in_season_management_reporting.wbr_merch_staging a
	LEFT JOIN LY_week_mapping lw 
		ON a.week_idnt = lw.week_idnt
	LEFT JOIN sku_id_map b 
		ON a.sku_idnt = b.rms_sku_num 
		AND a.country = b.channel_country
	WHERE 1=1
		AND a.week_idnt BETWEEN (SELECT start_week_idnt FROM date_variables) AND (SELECT end_week_idnt FROM date_variables)
	  	AND a.country = 'US'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX (channel_num, week_idnt, dept_num, class_num, brand_name, supplier_name) ON COMMIT PRESERVE ROWS;
 
COLLECT STATISTICS COLUMN (channel_num ,week_idnt ,LAST_YEAR_WK_IDNT ,MONTH_IDNT ,QUARTER_IDNT ,FISCAL_YEAR_NUM ,CHANNEL_COUNTRY ,div_label ,DIV_NUM ,grp_label ,GRP_NUM ,dept_label ,DEPT_NUM ,class_label ,CLASS_NUM ,BRAND_NAME ,NPG_FLAG ,SUPPLIER_NAME) ON sales_units_week_level; 	  
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,div_label ,DIV_NUM ,grp_label ,GRP_NUM ,dept_label ,DEPT_NUM ,class_label ,CLASS_NUM ,BRAND_NAME ,NPG_FLAG ,SUPPLIER_NAME) ON sales_units_week_level;
COLLECT STATISTICS COLUMN (CHANNEL_COUNTRY ,div_label ,DIV_NUM, grp_label ,GRP_NUM ,dept_label ,DEPT_NUM ,class_label ,CLASS_NUM ,BRAND_NAME ,NPG_FLAG ,SUPPLIER_NAME) ON sales_units_week_level;

-- Receipt units
CREATE MULTISET VOLATILE TABLE receipts_week_level AS (
	SELECT
		Cast(a.channel_num AS INT) AS channel_num,
		Trim(a.week_num) AS week_idnt,
	    Trim(lw.last_year_wk_idnt) AS last_year_wk_idnt,
		Trim(a.month_num) AS month_idnt,
		Trim(lw.quarter_idnt) AS quarter_idnt,
		Trim(a.year_num) AS fiscal_year_num,
		c.store_country_code AS channel_country,
		b.div_label,
		b.div_num,
		b.brand_name,
		b.npg_flag,
		b.grp_label,
		b.grp_num,
		b.dept_label,
		b.dept_num,
		b.class_label,
		b.class_num,
		b.supplier_name,
		Coalesce(Sum(receipts_regular_units + receipts_crossdock_regular_units) ,0) AS receipt_units
	FROM prd_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw a
	LEFT JOIN LY_week_mapping lw
		ON a.WEEK_NUM = lw.week_idnt
	INNER JOIN sku_id_map b 
		ON a.rms_sku_num = b.rms_sku_num 
		AND b.channel_country = 'US'
	INNER JOIN (
		SELECT DISTINCT 
			 channel_num
			,store_country_code 
		FROM PRD_NAP_USR_VWS.STORE_DIM) c 
		ON a.channel_num = c.channel_num	
		AND c.store_country_code = 'US'	
	WHERE 1=1
		AND a.week_num BETWEEN (SELECT start_week_idnt FROM date_variables) AND (SELECT end_week_idnt FROM date_variables)
		AND a.smart_sample_ind <> 'Y'
		AND a.gift_with_purchase_ind <> 'Y'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX (channel_num, week_idnt, last_year_wk_idnt, dept_num, class_num, brand_name, supplier_name) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (CHANNEL_NUM ,WEEK_IDNT ,LAST_YEAR_WK_IDNT ,MONTH_IDNT ,QUARTER_IDNT ,FISCAL_YEAR_NUM ,CHANNEL_COUNTRY ,div_label ,DIV_NUM ,BRAND_NAME ,NPG_FLAG ,grp_label ,GRP_NUM ,dept_label ,DEPT_NUM ,class_label ,CLASS_NUM ,SUPPLIER_NAME) ON receipts_week_level;

-- Product views & order units at SKU channel week 
 CREATE MULTISET VOLATILE TABLE funnel_week_sku_level AS (
	SELECT
		 Cast(CASE
			WHEN f.channel ='FULL_LINE' THEN '120'
			WHEN f.channel = 'RACK' THEN '250'
		 END AS INT) AS channel_num
		,d.week_idnt
		,f.rms_sku_num
		,Sum(f.product_views) AS product_views
		,Sum(f.product_views * f.pct_instock) AS instock_product_views
		,Sum(f.order_quantity) AS order_quantity
	FROM T2DL_DAS_PRODUCT_FUNNEL.product_price_funnel_daily f
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM d
		ON d.day_date = f.event_date_pacifiC
	WHERE 1=1 
		AND f.event_date_pacific BETWEEN (SELECT start_date FROM date_variables) AND (SELECT end_date FROM date_variables)
		AND f.channelcountry = 'US'
		AND f.channel <> 'UNKNOWN'
	GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX(rms_sku_num, channel_num, week_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (CHANNEL_NUM ,WEEK_IDNT) ON funnel_week_sku_level;
COLLECT STATISTICS COLUMN (WEEK_IDNT) ON funnel_week_sku_level;

-- Product views & order units at brand-dept-class channel week
CREATE MULTISET VOLATILE TABLE funnel_week AS (
	SELECT
		 f.channel_num
		,f.week_idnt
		,lw.last_year_wk_idnt
		,lw.month_idnt
		,lw.quarter_idnt
		,lw.fiscal_year_num
		,sku.channel_country
        ,sku.div_label
        ,sku.div_num
        ,sku.grp_label
        ,sku.grp_num
        ,sku.dept_label
        ,sku.dept_num
        ,sku.class_label
        ,sku.class_num
        ,sku.brand_name
        ,sku.npg_flag
        ,sku.supplier_name
		,Sum(f.product_views) AS product_views
		,Sum(f.instock_product_views) AS instock_product_views
		,Sum(f.order_quantity) AS order_quantity
	FROM funnel_week_sku_level f
	LEFT JOIN sku_id_map sku 
		ON sku.rms_sku_num = f.rms_sku_num
		AND sku.channel_country = 'US'
	LEFT JOIN LY_week_mapping lw 
		ON f.week_idnt = lw.week_idnt	
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX (channel_num, week_idnt, last_year_wk_idnt, class_num, dept_num, brand_name, supplier_name) ON COMMIT PRESERVE ROWS;

 -- TWIST at SKU location day
 CREATE MULTISET VOLATILE TABLE twist_day AS (	
	SELECT
		 base.rms_sku_num
		,base.day_date
		,dd.week_idnt
		,base.store_num
		,st.channel_num
		,Sum(base.allocated_traffic) AS allocated_traffic
		,Sum(CASE WHEN base.mc_instock_ind = 1 THEN base.allocated_traffic ELSE 0 END) AS instock_allocated_traffic
	FROM T2DL_DAS_TWIST.twist_daily base
	INNER JOIN PRD_NAP_USR_VWS.STORE_DIM st
		ON base.store_num = st.store_num
		AND st.channel_num IN (110,120,140,210,240,250)
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
		ON base.day_date = dd.day_date	
	WHERE 1=1
		AND base.day_date BETWEEN (SELECT start_date FROM date_variables) AND (SELECT end_date FROM date_variables)
	GROUP BY 
		1,2,3,4,5
) WITH DATA 
PRIMARY INDEX(rms_sku_num, week_idnt, channel_num) 
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (WEEK_IDNT ,CHANNEL_NUM) ON twist_day;
COLLECT STATISTICS COLUMN (RMS_SKU_NUM) ON twist_day;
COLLECT STATISTICS COLUMN (WEEK_IDNT) ON twist_day;

-- TWIST at dept-class-brand channel week
CREATE MULTISET VOLATILE TABLE twist_week AS (
	SELECT
		 f.channel_num
		,f.week_idnt
		,lw.last_year_wk_idnt
		,lw.month_idnt
		,lw.quarter_idnt
		,lw.fiscal_year_num
		,sku.channel_country
        ,sku.div_label
        ,sku.div_num
        ,sku.grp_label
        ,sku.grp_num
        ,sku.dept_label
        ,sku.dept_num
        ,sku.class_label
        ,sku.class_num
        ,sku.brand_name
        ,sku.npg_flag
        ,sku.supplier_name
		,Sum(f.allocated_traffic) AS allocated_traffic
		,Sum(instock_allocated_traffic) AS instock_allocated_traffic
	FROM twist_day f
	LEFT JOIN LY_week_mapping lw 
		ON f.week_idnt = lw.week_idnt	
	LEFT JOIN sku_id_map sku 
		ON sku.rms_sku_num = f.rms_sku_num
		AND sku.channel_country = 'US'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX (channel_num, week_idnt, last_year_wk_idnt, class_num, dept_num, brand_name, supplier_name) ON COMMIT PRESERVE ROWS;
 
 
-- Combining sales, inventory, receipts, and product views		
CREATE MULTISET VOLATILE TABLE sales_receipts_pv_twist AS (
	SELECT 
		 channel_num
		,week_idnt
		,last_year_wk_idnt AS last_year_week_idnt
		,month_idnt
		,quarter_idnt
		,fiscal_year_num
		,channel_country
		,div_label
		,div_num
		,grp_label
		,grp_num
		,dept_label
		,dept_num
		,class_label
		,class_num
		,brand_name
		,npg_flag
		,supplier_name
		,Coalesce(Sum(sales_dollars),0) AS net_sales_dollars_r
		,Coalesce(Sum(return_dollars),0) AS return_dollars_r
		,Coalesce(Sum(sales_units),0) AS net_sales_units
		,Coalesce(Sum(return_units),0) AS return_units
		,Coalesce(Sum(eoh_units),0) AS eoh_units
		,Coalesce(Sum(receipt_units),0) AS receipt_units
		,Sum(product_views) AS product_views
		,Sum(order_unit_qty) AS order_units
		,Sum(allocated_traffic) AS allocated_traffic
		,Sum(instock_allocated_traffic) AS instock_allocated_traffic
	FROM (
		-- Sales & EOH
		SELECT 
			 Cast(channel_num AS INT) AS channel_num
			,Trim(week_idnt) AS week_idnt
			,Trim(last_year_wk_idnt) AS last_year_wk_idnt
			,Trim(month_idnt) AS month_idnt
			,Trim(quarter_idnt) AS quarter_idnt
			,Trim(fiscal_year_num) AS fiscal_year_num
			,channel_country
			,div_label
			,div_num
			,grp_label
			,grp_num
			,dept_label
			,dept_num
			,class_label
			,class_num
			,brand_name
			,npg_flag
			,supplier_name
			,Sum(sales_dollars) AS sales_dollars
			,Sum(return_dollars) AS return_dollars
			,Sum(sales_units) AS sales_units
			,Sum(return_units) AS return_units
			,Sum(eoh_units) AS eoh_units
			,Cast(0 AS DECIMAL(16,0)) AS receipt_units
			,Cast(0 AS INTEGER) AS product_views
			,Cast(0 AS INTEGER) AS order_unit_qty	
			,Cast(0 AS DECIMAL(16,2)) AS allocated_traffic
			,Cast(0 AS DECIMAL(16,2)) AS instock_allocated_traffic
		FROM sales_units_week_level
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
		
	UNION ALL 
		
		-- Receipts	
		SELECT 
			 Cast(channel_num AS INT) AS channel_num
			,Trim(week_idnt) AS week_idnt
			,Trim(last_year_wk_idnt) AS last_year_wk_idnt
			,Trim(month_idnt) AS month_idnt
			,Trim(quarter_idnt) AS quarter_idnt
			,Trim(fiscal_year_num) AS fiscal_year_num
			,channel_country
			,div_label
			,div_num
			,grp_label
			,grp_num
			,dept_label
			,dept_num
			,class_label
			,class_num
			,brand_name
			,npg_flag
			,supplier_name
			,Cast(0 AS DECIMAL(16,2)) AS sales_dollars
			,Cast(0 AS DECIMAL(16,2)) AS return_dollars
			,Cast(0 AS DECIMAL(16,0))  AS sales_units
			,Cast(0 AS DECIMAL(16,0)) AS return_units
			,Cast(0 AS DECIMAL(16,0)) AS eoh_units
			,Sum(receipt_units) AS receipt_units
			,Cast(0 AS INTEGER) AS product_views
			,Cast(0 AS INTEGER) AS order_unit_qty	
			,Cast(0 AS DECIMAL(16,2)) AS allocated_traffic
			,Cast(0 AS DECIMAL(16,2)) AS instock_allocated_traffic	
		FROM receipts_week_level
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
		
	UNION ALL 

		-- TWIST
		SELECT 
			 Cast(channel_num AS INT) AS channel_num
			,Trim(week_idnt) AS week_idnt
			,Trim(last_year_wk_idnt) AS last_year_wk_idnt
			,Trim(month_idnt) AS month_idnt
			,Trim(quarter_idnt) AS quarter_idnt
			,Trim(fiscal_year_num) AS fiscal_year_num
			,channel_country
			,div_label
			,div_num
			,grp_label
			,grp_num
			,dept_label
			,dept_num
			,class_label
			,class_num
			,brand_name
			,npg_flag
			,supplier_name
			,Cast(0 AS DECIMAL(16,2)) AS sales_dollars
			,Cast(0 AS DECIMAL(16,2)) AS return_dollars
			,Cast(0 AS DECIMAL(16,0))  AS sales_units
			,Cast(0 AS DECIMAL(16,0)) AS return_units
			,Cast(0 AS DECIMAL(16,0)) AS eoh_units
			,Cast(0 AS DECIMAL(16,0)) AS receipt_units
			,Cast(0 AS INTEGER) AS product_views
			,Cast(0 AS INTEGER) AS order_unit_qty	
			,Sum(allocated_traffic) AS allocated_traffic
			,Sum(instock_allocated_traffic) AS instock_allocated_traffic	
		FROM twist_week
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
		
UNION ALL 

		-- Funnel
		SELECT 
			 Cast(channel_num AS INT) AS channel_num
			,Trim(week_idnt) AS week_idnt
			,Trim(last_year_wk_idnt) AS last_year_wk_idnt
			,Trim(month_idnt) AS month_idnt
			,Trim(quarter_idnt) AS quarter_idnt
			,Trim(fiscal_year_num) AS fiscal_year_num
			,channel_country
			,div_label
			,div_num
			,grp_label
			,grp_num
			,dept_label
			,dept_num
			,class_label
			,class_num
			,brand_name
			,npg_flag
			,supplier_name
			,Cast(0 AS DECIMAL(16,2)) AS sales_dollars
			,Cast(0 AS DECIMAL(16,2)) AS return_dollars
			,Cast(0 AS DECIMAL(16,0))  AS sales_units
			,Cast(0 AS DECIMAL(16,0)) AS return_units
			,Cast(0 AS DECIMAL(16,0)) AS eoh_units
			,Cast(0 AS DECIMAL(16,0)) AS receipt_units
			,Sum(product_views) AS product_views
			,Sum(order_quantity) AS order_unit_qty	
			,Cast(0 AS DECIMAL(16,2)) AS allocated_traffic
			,Cast(0 AS DECIMAL(16,2)) AS instock_allocated_traffic
		FROM funnel_week
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
		
	) x
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) WITH DATA PRIMARY INDEX (channel_num, week_idnt, last_year_week_idnt, class_num, dept_num, brand_name) ON COMMIT PRESERVE ROWS;


DELETE {nmn_t2_schema}.BRAND_WEEKLY_FACT WHERE week_idnt BETWEEN (SELECT start_week_idnt FROM date_variables) AND (SELECT end_week_idnt FROM date_variables);

INSERT INTO {nmn_t2_schema}.BRAND_WEEKLY_FACT
SELECT
	 channel_num
	,channel_country
	,week_idnt 
	,last_year_week_idnt
	,month_idnt  
	,quarter_idnt 
	,fiscal_year_num
	,div_label 
	,div_num 
	,grp_label 
	,grp_num 
	,dept_label 
	,dept_num 
	,class_label
	,class_num 
	,brand_name
	,npg_flag 
	,supplier_name
	,net_sales_dollars_r
	,return_dollars_r
	,net_sales_units
	,return_units
	,eoh_units 
	,receipt_units 
	,product_views
	,order_units
	,allocated_traffic 
	,instock_allocated_traffic 
	,Current_Timestamp AS dw_sys_load_tmstp
FROM sales_receipts_pv_twist;

SET QUERY_BAND = NONE FOR SESSION;
