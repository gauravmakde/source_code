/*

T2/Table Name: T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT
Team/Owner: NMN / Custmer Engagement Analytics
Date Created/Modified: 20203-06-15

Note:
--  
-- Weekly Refresh
*/

SET QUERY_BAND = 'App_ID=APP08162;
DAG_ID=campaign_attributed_weekly_fact_11521_ACE_ENG;
Task_Name=campaign_attributed_weekly_fact;' 
FOR SESSION VOLATILE;


CREATE VOLATILE TABLE variable_start_end_week AS
(
  SELECT
    MAX(CASE WHEN day_date < {start_date} THEN week_idnt END) AS start_week_num,
    MAX(CASE WHEN day_date < {end_date} THEN week_idnt END) AS end_week_num
  FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
) WITH DATA PRIMARY INDEX (start_week_num, end_week_num) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE caf_ty AS (
	SELECT
		 dd.week_idnt
		,caf.channel_num
		,caf.channel_country
		,caf.channel_banner
		,caf.mktg_type
		,caf.finance_rollup
		,caf.finance_detail
		,caf.utm_source
		,caf.utm_channel
		,caf.utm_campaign
		,caf.utm_term
		,CASE WHEN caf.utm_channel LIKE '%_EX_%' THEN caf.utm_content ELSE NULL END AS nmn_utm_content
		,caf.supplier_name
		,caf.brand_name
		,Trim(sku.div_num) || ', ' || sku.div_desc AS division
		,Trim(sku.grp_num) || ', ' || sku.grp_desc AS subdivision
		,Trim(sku.dept_num) || ', ' || sku.dept_desc AS department
		,Sum(product_views) AS ty_product_views 
		,Sum(instock_product_views) AS ty_instock_product_views
		,Sum(cart_add_units) AS ty_cart_add_units
		,Sum(order_units) AS ty_order_units
		,Cast(0 AS DECIMAL(12,0)) AS ly_product_views 
		,Cast(0 AS DECIMAL(12,0)) AS ly_instock_product_views
		,Cast(0 AS DECIMAL(12,0)) AS ly_cart_add_units
		,Cast(0 AS DECIMAL(12,0)) AS ly_order_units
	FROM T2dl_DAS_NMN.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT caf
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
		ON caf.activity_date_pacific = dd.DAY_DATE
	LEFT JOIN (SELECT DISTINCT web_style_num, dept_num, div_num, grp_num, div_desc, grp_desc, dept_desc, channel_country FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW) sku
		ON caf.web_style_num = sku.web_style_num	
		AND caf.channel_country = sku.channel_country
	WHERE 1=1
		AND dd.week_idnt BETWEEN (Select start_week_num from variable_start_end_week) AND (Select end_week_num from variable_start_end_week)
	GROUP BY 
		 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(week_idnt, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term,nmn_utm_content, supplier_name, division, subdivision, department, brand_name) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE caf_ly AS(
	SELECT
		 dd.week_idnt
		,caf.channel_num
		,caf.channel_country
		,caf.channel_banner
		,caf.mktg_type
		,caf.finance_rollup
		,caf.finance_detail
		,caf.utm_source
		,caf.utm_channel
		,caf.utm_campaign
		,caf.utm_term
		,CASE WHEN caf.utm_channel LIKE '%_EX_%' THEN caf.utm_content ELSE NULL END AS nmn_utm_content
		,caf.supplier_name
		,caf.brand_name
		,Trim(sku.div_num) || ', ' || sku.div_desc AS division
		,Trim(sku.grp_num) || ', ' || sku.grp_desc AS subdivision
		,Trim(sku.dept_num) || ', ' || sku.dept_desc AS department
		,Cast(0 AS DECIMAL(12,0)) AS ty_product_views 
		,Cast(0 AS DECIMAL(12,0)) AS ty_instock_product_views
		,Cast(0 AS DECIMAL(12,0)) AS ty_cart_add_units
		,Cast(0 AS DECIMAL(12,0)) AS ty_order_units	
		,Sum(product_views) AS ly_product_views 
		,Sum(instock_product_views) AS ly_instock_product_views
		,Sum(cart_add_units) AS ly_cart_add_units
		,Sum(order_units) AS ly_order_units
	FROM T2dl_DAS_NMN.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT caf
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
		ON caf.activity_date_pacific = dd.day_date_last_year_realigned
	LEFT JOIN (SELECT DISTINCT web_style_num, dept_num, div_num, grp_num, div_desc, grp_desc, dept_desc, channel_country FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW) sku
		ON caf.web_style_num = sku.web_style_num	
		AND caf.channel_country = sku.channel_country	
	WHERE 1=1
		AND dd.week_idnt BETWEEN (Select start_week_num from variable_start_end_week) AND (Select end_week_num from variable_start_end_week)
	GROUP BY 
		 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(week_idnt, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term,nmn_utm_content, supplier_name, division, subdivision, department, brand_name) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE caf_agg AS (
	SELECT
		 week_idnt
		,channel_num
		,channel_country
		,channel_banner
		,mktg_type
		,finance_rollup
		,finance_detail
		,utm_source
		,utm_channel
		,utm_campaign
		,utm_term
		,nmn_utm_content
		,supplier_name
		,brand_name
		,division
		,subdivision
		,department
		,ty_product_views 
		,ty_instock_product_views
		,ty_cart_add_units
		,ty_order_units
		,Cast(0 AS DECIMAL(12,2)) AS ty_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ty_gross_sales_units	
		,ly_product_views 
		,ly_instock_product_views
		,ly_cart_add_units
		,ly_order_units
		,Cast(0 AS DECIMAL(12,2)) AS ly_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ly_gross_sales_units	
	FROM caf_ty
	UNION 
	SELECT
		 week_idnt
		,channel_num
		,channel_country
		,channel_banner
		,mktg_type
		,finance_rollup
		,finance_detail
		,utm_source
		,utm_channel
		,utm_term
		,utm_campaign
		,nmn_utm_content
		,supplier_name
		,brand_name
		,division
		,subdivision
		,department
		,ty_product_views 
		,ty_instock_product_views
		,ty_cart_add_units
		,ty_order_units
		,Cast(0 AS DECIMAL(12,2)) AS ty_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ty_gross_sales_units	
		,ly_product_views 
		,ly_instock_product_views
		,ly_cart_add_units
		,ly_order_units
		,Cast(0 AS DECIMAL(12,2)) AS ly_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ly_gross_sales_units	
	FROM caf_ly	
) WITH DATA PRIMARY INDEX(week_idnt, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term,nmn_utm_content, supplier_name, division, subdivision, department, brand_name) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE cas_ty AS (
	SELECT
		 dd.week_idnt
		,cas.channel_num
		,cas.channel_country
		,cas.channel_banner
		,cas.mktg_type
		,cas.finance_rollup
		,cas.finance_detail
		,cas.utm_source
		,cas.utm_channel
		,cas.utm_campaign
		,cas.utm_term
		,CASE WHEN cas.utm_channel LIKE '%_EX_%' THEN cas.utm_content ELSE NULL END AS nmn_utm_content
		,cas.supplier_name
		,cas.brand_name
		,Trim(sku.div_num) || ', ' || sku.div_desc AS division
		,Trim(sku.grp_num) || ', ' || sku.grp_desc AS subdivision
		,Trim(sku.dept_num) || ', ' || sku.dept_desc AS department
		,Sum(cas.gross_sales_amt_usd) AS ty_gross_sales_amt 
		,Sum(cas.gross_sales_units) AS ty_gross_sales_units
		,Cast(0 AS DECIMAL(12,2)) AS ly_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ly_gross_sales_units	
	FROM T2dl_DAS_NMN.CAMPAIGN_ATTRIBUTED_SALES_FACT cas
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
		ON cas.business_day_date = dd.DAY_DATE
	LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
		ON cas.sku_num = sku.rms_sku_num
		AND cas.channel_country = sku.channel_country	
	WHERE 1=1
		AND dd.week_idnt BETWEEN (Select start_week_num from variable_start_end_week) AND (Select end_week_num from variable_start_end_week)
	GROUP BY 
		 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(week_idnt, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, nmn_utm_content, supplier_name, division, subdivision, department, brand_name) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE cas_ly AS (
	SELECT
		 dd.week_idnt
		,cas.channel_num
		,cas.channel_country
		,cas.channel_banner
		,cas.mktg_type
		,cas.finance_rollup
		,cas.finance_detail
		,cas.utm_source
		,cas.utm_channel
		,cas.utm_campaign
		,cas.utm_term
		,CASE WHEN cas.utm_channel LIKE '%_EX_%' THEN cas.utm_content ELSE NULL END AS nmn_utm_content
		,cas.supplier_name
		,cas.brand_name
		,Trim(sku.div_num) || ', ' || sku.div_desc AS division
		,Trim(sku.grp_num) || ', ' || sku.grp_desc AS subdivision
		,Trim(sku.dept_num) || ', ' || sku.dept_desc AS department
		,Cast(0 AS DECIMAL(12,0)) AS ty_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ty_gross_sales_units	
		,Sum(cas.gross_sales_amt_usd) AS ly_gross_sales_amt 
		,Sum(cas.gross_sales_units) AS ly_gross_sales_units
	FROM T2dl_DAS_NMN.CAMPAIGN_ATTRIBUTED_SALES_FACT cas
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
		ON cas.business_day_date = dd.day_date_last_year_realigned
	LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
		ON cas.sku_num = sku.rms_sku_num
		AND cas.channel_country = sku.channel_country
	WHERE 1=1
		AND dd.week_idnt BETWEEN (Select start_week_num from variable_start_end_week) AND (Select end_week_num from variable_start_end_week)
	GROUP BY 
		 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(week_idnt, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, nmn_utm_content, supplier_name, division, subdivision, department, brand_name) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE cas_agg AS (
	SELECT
		 week_idnt
		,channel_num
		,channel_country
		,channel_banner
		,mktg_type
		,finance_rollup
		,finance_detail
		,utm_source
		,utm_channel
		,utm_campaign
		,utm_term
		,nmn_utm_content
		,supplier_name
		,brand_name
		,division
		,subdivision
		,department
		,Cast(0 AS DECIMAL(12,0)) AS ty_product_views 
		,Cast(0 AS DECIMAL(12,0)) AS ty_instock_product_views
		,Cast(0 AS DECIMAL(12,0)) AS ty_cart_add_units
		,Cast(0 AS DECIMAL(12,0)) AS ty_order_units
		,ty_gross_sales_amt
		,ty_gross_sales_units	
		,Cast(0 AS DECIMAL(12,0)) AS ly_product_views 
		,Cast(0 AS DECIMAL(12,0)) AS ly_instock_product_views
		,Cast(0 AS DECIMAL(12,0)) AS ly_cart_add_units
		,Cast(0 AS DECIMAL(12,0)) AS ly_order_units
		,ly_gross_sales_amt
		,ly_gross_sales_units	
	FROM cas_ty
	UNION
	SELECT
		 week_idnt
		,channel_num
		,channel_country
		,channel_banner
		,mktg_type
		,finance_rollup
		,finance_detail
		,utm_source
		,utm_channel
		,utm_campaign
		,utm_term
		,nmn_utm_content
		,supplier_name
		,brand_name
		,division
		,subdivision
		,department
		,Cast(0 AS DECIMAL(12,0)) AS ty_product_views 
		,Cast(0 AS DECIMAL(12,0)) AS ty_instock_product_views
		,Cast(0 AS DECIMAL(12,0)) AS ty_cart_add_units
		,Cast(0 AS DECIMAL(12,0)) AS ty_order_units
		,ty_gross_sales_amt
		,ty_gross_sales_units	
		,Cast(0 AS DECIMAL(12,0)) AS ly_product_views 
		,Cast(0 AS DECIMAL(12,0)) AS ly_instock_product_views
		,Cast(0 AS DECIMAL(12,0)) AS ly_cart_add_units
		,Cast(0 AS DECIMAL(12,0)) AS ly_order_units
		,ly_gross_sales_amt
		,ly_gross_sales_units	
	FROM cas_ly
) WITH DATA PRIMARY INDEX(week_idnt, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, nmn_utm_content, supplier_name, division, subdivision, department, brand_name) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE caf_cas AS (
		SELECT
			 week_idnt
			,channel_num
			,channel_country
			,channel_banner
			,mktg_type
			,finance_rollup
			,finance_detail
			,utm_source
			,utm_channel
			,utm_campaign
			,utm_term
			,nmn_utm_content
			,supplier_name
			,brand_name
			,division
			,subdivision
			,department
			,Sum(ty_product_views) AS ty_product_views 
			,Sum(ty_instock_product_views) AS ty_instock_product_views
			,Sum(ty_cart_add_units) AS ty_cart_add_units
			,Sum(ty_order_units) AS ty_order_units
			,Sum(ly_product_views) AS ly_product_views 
			,Sum(ly_instock_product_views) AS ly_instock_product_views
			,Sum(ly_cart_add_units) AS ly_cart_add_units
			,Sum(ly_order_units) AS ly_order_units
			,Cast(0 AS DECIMAL(12,2)) AS ty_gross_sales_amt
			,Cast(0 AS DECIMAL(12,0)) AS ty_gross_sales_units		
			,Cast(0 AS DECIMAL(12,2)) AS ly_gross_sales_amt
			,Cast(0 AS DECIMAL(12,0)) AS ly_gross_sales_units	
		FROM caf_agg
		GROUP BY 
		 	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
		UNION 
		SELECT
			 week_idnt
			,channel_num
			,channel_country
			,channel_banner
			,mktg_type
			,finance_rollup
			,finance_detail
			,utm_source
			,utm_channel
			,utm_campaign
			,utm_term
			,nmn_utm_content
			,supplier_name
			,brand_name
			,division
			,subdivision
			,department
			,Cast(0 AS DECIMAL(12,0)) AS ty_product_views 
			,Cast(0 AS DECIMAL(12,0)) AS ty_instock_product_views
			,Cast(0 AS DECIMAL(12,0)) AS ty_cart_add_units
			,Cast(0 AS DECIMAL(12,0)) AS ty_order_units
			,Cast(0 AS DECIMAL(12,0)) AS ly_product_views 
			,Cast(0 AS DECIMAL(12,0)) AS ly_instock_product_views
			,Cast(0 AS DECIMAL(12,0)) AS ly_cart_add_units
			,Cast(0 AS DECIMAL(12,0)) AS ly_order_units
			,Sum(ty_gross_sales_amt) AS ty_gross_sales_amt
			,Sum(ty_gross_sales_units) AS ty_gross_sales_units
			,Sum(ly_gross_sales_amt) AS ly_gross_sales_amt
			,Sum(ly_gross_sales_units) AS ly_gross_sales_units	
		FROM cas_agg
		GROUP BY 
		 	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(week_idnt, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, nmn_utm_content, supplier_name, division, subdivision, department, brand_name) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (BRAND_NAME) ON caf_cas;

CREATE MULTISET VOLATILE TABLE caw_fact_stg AS (
SELECT
	 week_idnt
	,channel_num
	,channel_country
	,channel_banner
	,mktg_type
	,finance_rollup
	,finance_detail
	,utm_source
	,utm_channel
	,utm_campaign
	,utm_term
	,nmn_utm_content
	,supplier_name
	,brand_name
	,division
	,subdivision
	,department
	,Sum(ty_product_views) AS ty_product_views 
	,Sum(ty_instock_product_views) AS ty_instock_product_views
	,Sum(ty_cart_add_units) AS ty_cart_add_units
	,Sum(ty_order_units) AS ty_order_units
	,Sum(ty_gross_sales_amt) AS ty_gross_sales_amt
	,Sum(ty_gross_sales_units) AS ty_gross_sales_units
	,Sum(ly_product_views) AS ly_product_views 
	,Sum(ly_instock_product_views) AS ly_instock_product_views
	,Sum(ly_cart_add_units) AS ly_cart_add_units
	,Sum(ly_order_units) AS ly_order_units	
	,Sum(ly_gross_sales_amt) AS ly_gross_sales_amt
	,Sum(ly_gross_sales_units) AS ly_gross_sales_units	
FROM caf_cas
GROUP BY 
 	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(week_idnt, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, nmn_utm_content, supplier_name, division, subdivision, department, brand_name) ON COMMIT PRESERVE ROWS;

DELETE FROM {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT WHERE week_idnt BETWEEN (Select start_week_num from variable_start_end_week) AND (Select end_week_num from variable_start_end_week);

INSERT INTO {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT 
SELECT a.*, CURRENT_TIMESTAMP as dw_sys_load_tmstp from caw_fact_stg a;

COLLECT STATISTICS COLUMN (WEEK_IDNT, CHANNEL_NUM, CHANNEL_COUNTRY, CHANNEL_BANNER, MKTG_TYPE, FINANCE_ROLLUP, FINANCE_DETAIL, UTM_SOURCE, UTM_CHANNEL,utm_campaign, utm_term, NMN_UTM_CONTENT, SUPPLIER_NAME, BRAND_NAME, DIVISION, SUBDIVISION, DEPARTMENT) ON {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT;
COLLECT STATISTICS COLUMN (WEEK_IDNT) ON {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT;
COLLECT STATISTICS COLUMN (BRAND_NAME ,SUBDIVISION) ON {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_WEEKLY_FACT;

SET QUERY_BAND = NONE FOR SESSION;

