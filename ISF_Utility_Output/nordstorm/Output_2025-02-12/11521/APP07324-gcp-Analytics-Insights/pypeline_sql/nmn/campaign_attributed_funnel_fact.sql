SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=campaign_attributed_funnel_fact_11521_ACE_ENG;
     Task_Name=campaign_attributed_funnel_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT
Team/Owner: Customer Analytics / Angelina Allen-St. John (E4JP)
Date Created/Modified: 2023-01-19

Note:
-- This table displays product views, cart adds, and orders at the level of web_style_num - channel - day - utm_channel - utm_content, applying same-session attribution
-- Data begins 2022-01-31, in alignment with the start of session data in NAP
*/


/*
Create product hierarchy lookup with details at web_style_num - country level
*/
CREATE MULTISET VOLATILE TABLE prod_hier AS (
    SELECT DISTINCT
		 sku.channel_country
		,sku.web_style_num
		,sku.dept_num
		,sku.grp_num AS sdiv_num
		,sku.brand_name AS brand_name
		,v.vendor_name AS supplier
    FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
    LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM v
		ON sku.prmy_supp_num = v.vendor_num
    WHERE 1=1
		-- Exclude samples & gifts with purchase
		AND sku.GWP_IND <> 'Y'
		AND sku.SMART_SAMPLE_IND <> 'Y'
) WITH DATA 
PRIMARY INDEX(web_style_num)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (web_style_num, brand_name, supplier) ON prod_hier;
COLLECT STATISTICS COLUMN (web_style_num) ON prod_hier;
COLLECT STATISTICS COLUMN (channel_country, web_style_num) ON prod_hier;

/*
Combine funnel & attributed sales metrics
*/
CREATE MULTISET VOLATILE TABLE nmn_funnel_fact_temp AS (
	SELECT
		 sesh.activity_date_pacific
		,sesh.channelcountry AS channel_country
		,sesh.channel AS channel_banner
		,Cast(CASE
			WHEN sesh.channel = 'NORDSTROM' AND sesh.channelcountry = 'US' THEN 120
			WHEN sesh.channel = 'NORDSTROM' AND sesh.channelcountry = 'CA' THEN 121
			WHEN sesh.channel = 'NORDSTROM_RACK' AND sesh.channelcountry = 'US' THEN 250
		 ELSE NULL END AS INTEGER) AS channel_num
		,sma.mrkt_type AS mktg_type
		,sma.finance_rollup
		,sma.finance_detail
		,sma.utm_source
		,Trim(Left(sma.utm_channel,70)) AS utm_channel
		,Trim(Left(sma.utm_campaign,90)) as utm_campaign
		,Trim(Left(sma.utm_term, 90)) as utm_term 
		,Trim(Left(sma.utm_content,300)) AS utm_content
		,skus.supplier AS supplier_name
		,skus.brand_name AS brand_name
		,skus.web_style_num
		,Sum(sesh.product_views) AS product_views
		,Sum(sesh.instock_product_views) AS instock_product_views
		,Sum(sesh.cart_add_units) AS cart_add_units
		,Sum(sesh.order_units) AS order_units
	FROM (
		SELECT
			 session_id
			,activity_date_pacific
			,channelcountry
			,channel
			,Cast(CASE WHEN Upper(productstyle_id) = Lower(productstyle_id)(CaseSpecific) 
				THEN productstyle_id ELSE NULL END AS INTEGER) 
			 AS productstyle_id
			,Sum(product_views) AS product_views
			,Sum(CASE WHEN product_available_ind = 'Y' THEN product_views ELSE 0 end) AS instock_product_views
			,Cast(0 AS BIGINT) AS cart_add_units
			,Cast(0 AS BIGINT) AS order_units
		FROM PRD_NAP_USR_VWS.CUSTOMER_SESSION_STYLE_FACT
		WHERE 1=1
			AND activity_date_pacific BETWEEN {start_date} AND {end_date}
		GROUP BY
			1,2,3,4,5
		UNION 
		SELECT
			 session_id
			,activity_date_pacific
			,channelcountry
			,channel
			,Cast(CASE WHEN Upper(productstyle_id) = Lower(productstyle_id)(CaseSpecific) 
				THEN productstyle_id ELSE NULL END AS INTEGER) 
			 AS productstyle_id
			,Cast(0 AS BIGINT) AS product_views
			,Cast(0 AS BIGINT) AS instock_product_views
			,Sum(product_cart_add_units) AS cart_add_units
			,Sum(product_order_units) AS order_units
		FROM PRD_NAP_USR_VWS.CUSTOMER_SESSION_STYLE_SKU_FACT
		WHERE 1=1
			AND activity_date_pacific BETWEEN {start_date} AND {end_date}
		GROUP BY
			1,2,3,4,5
		) sesh
	LEFT JOIN prod_hier skus
	    ON sesh.productstyle_id = skus.web_style_num
	    AND sesh.channelcountry = skus.channel_country
	LEFT JOIN T2DL_DAS_MTA.SSA_MKT_ATTR_FACT sma
	    ON sesh.session_id = sma.session_id
	GROUP BY
		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) 
WITH DATA
PRIMARY INDEX(activity_date_pacific, web_style_num, channel_num, utm_channel, utm_content, utm_term)
ON COMMIT PRESERVE ROWS
;

/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM    {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT
WHERE   activity_date_pacific >= {start_date}
AND     activity_date_pacific <= {end_date}
;


INSERT INTO {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT
SELECT    
	  activity_date_pacific
	, channel_country
	, channel_banner
	, channel_num
	, mktg_type
	, finance_rollup
	, finance_detail
	, utm_source
	, utm_channel
	, utm_campaign
	, utm_term
	, utm_content
	, supplier_name
	, brand_name
	, web_style_num
	, product_views
	, instock_product_views
	, cart_add_units
	, order_units
    , Current_Timestamp AS dw_sys_load_tmstp
FROM	nmn_funnel_fact_temp
WHERE   activity_date_pacific >= {start_date}
AND     activity_date_pacific <= {end_date}
;

COLLECT STATISTICS  
		COLUMN (activity_date_pacific, web_style_num, channel_num, utm_channel), -- column names used for primary index
		COLUMN (activity_date_pacific),  -- column names used for partition
		COLUMN (channel_num, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, supplier_name, brand_name) -- recommended statistics to support Tableau data source query
ON {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT;

SET QUERY_BAND = NONE FOR SESSION;
