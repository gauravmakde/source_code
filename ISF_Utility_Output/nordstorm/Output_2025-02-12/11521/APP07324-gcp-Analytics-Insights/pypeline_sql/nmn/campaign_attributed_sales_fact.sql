SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=campaign_attributed_sales_fact_11521_ACE_ENG;
     Task_Name=campaign_attributed_sales_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_SALES_FACT
Team/Owner: Customer Analytics / Angelina Allen-St. John (E4JP)
Date Created/Modified: 2023-01-06

Note:
-- This table displays sales at the level of sku - channel - day - utm_channel - utm_content, applying 30 day last-click attribution
-- Data begins 2022-01-31, in alignment with the start of session data in NAP
*/



/*
Create temp table to insert into final table
*/
CREATE MULTISET VOLATILE TABLE nmn_sales_fact_temp AS (
	SELECT
		 r.business_day_date 
		,store.channel_country channel_country
		,store.channel_brand AS channel_banner
		,store.channel_num
		-- SEO_SEARCH appears in base table as the mktg_type, which must be corrected at mktg_type, finance_rollup, and finance_detail
		,Coalesce(CASE
			WHEN lmt.mktg_type = 'SEO_SEARCH' THEN 'UNPAID'
			ELSE mch.marketing_type
		 END,'BASE') AS mktg_type
		,Coalesce(CASE
			WHEN lmt.mktg_type = 'SEO_SEARCH' THEN 'SEO'
			ELSE Coalesce(mch.finance_rollup,'BASE')
		 END,'BASE') AS finance_rollup
		,Coalesce(CASE
			WHEN lmt.mktg_type = 'SEO_SEARCH' THEN 'SEO_SEARCH'
			ELSE mch.finance_detail
		 END,'BASE') AS finance_detail
		,lmt.utm_source
		,Trim(Left(lmt.utm_channel,70)) as utm_channel
		,trim(Left(lmt.utm_campaign,90)) as utm_campaign
		,Trim(Left(lmt.utm_term, 90)) as utm_term
		,Trim(Left(lmt.utm_content,300)) as utm_content
		,v.vendor_name AS supplier_name 		
		,sku.brand_name 
		,r.sku_num
		,Sum(r.net_sales_usd) AS  net_sales_amt_usd
		,Sum(r.gross_sales_usd) AS gross_sales_amt_usd
		,Sum(r.returns_usd) AS returns_amt_usd
		,Sum(r.net_sales_units) AS net_sales_units
		,Sum(r.gross_sales_units) AS gross_sales_units
		,Sum(r.return_units) AS return_units
	FROM (
		SELECT 
			-- Match MTA_TOUCH_BASE field order_num which coalesces order_num and global_tran_id
			 Trim(Coalesce(sr.order_num, sr.global_tran_id)) AS order_idnt
			,sr.business_day_date
			-- Assign BOPUS order intent to online store
			,CASE 
				WHEN sr.line_item_order_type = 'CustInitWebOrder' AND sr.line_item_fulfillment_type = 'StorePickUp'
				THEN (CASE WHEN sr.business_unit_desc LIKE '%CA%' THEN 867 ELSE 808 END)
			 ELSE sr.intent_store_num end AS store_num
			,sr.sku_num 
			,Sum(sr.shipped_usd_sales) AS  gross_sales_usd
			,Sum(sr.shipped_qty) AS gross_sales_units
			,Sum(sr.return_usd_amt) AS returns_usd
			,Sum(sr.return_qty) AS return_units
			,Sum(sr.shipped_usd_sales) - Sum(ZeroIfNull(sr.return_usd_amt)) AS net_sales_usd
			,Sum(sr.shipped_qty) - Sum(ZeroIfNull(sr.return_qty)) AS net_sales_units
		FROM T2DL_DAS_SALES_RETURNS.SALES_AND_RETURNS_FACT sr
		INNER JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
	    	ON sr.business_day_date = dd.day_date
			AND dd.day_date BETWEEN Cast({start_date} AS DATE) AND Cast({end_date} AS DATE)
		WHERE 1=1
			AND sr.transaction_type = 'retail'
			AND sr.nonmerch_fee_code IS NULL
		GROUP BY 
			1,2,3,4
	) r
	LEFT JOIN T2DL_DAS_MTA.mta_last_touch lmt
		ON lmt.order_number  = r.order_idnt
	LEFT JOIN T2DL_DAS_MTA.UTM_CHANNEL_LOOKUP ucl
	    ON ucl.utm_mkt_chnl = lmt.utm_channel
	LEFT JOIN T2DL_DAS_MTA.MARKETING_CHANNEL_HIERARCHY mch
	    ON mch.join_channel = ucl.bi_channel
	INNER JOIN PRD_NAP_USR_VWS.PRICE_STORE_DIM_VW store 
		ON r.store_num =  store.store_num
	INNER JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
		ON r.sku_num = sku.rms_sku_num
		AND sku.channel_country = store.store_country_code
        AND sku.gwp_ind <> 'Y'
        AND sku.smart_sample_ind <> 'Y'
	LEFT JOIN PRD_NAP_USR_VWS.VENDOR_DIM v
		ON sku.prmy_supp_num = v.vendor_num
	GROUP BY
		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) 
WITH DATA
PRIMARY INDEX(business_day_date, sku_num)
ON COMMIT PRESERVE ROWS
;



/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
DELETE 
FROM    {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT
WHERE   business_day_date >= {start_date}
AND     business_day_date <= {end_date}
;


INSERT INTO {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT
SELECT    
	  business_day_date
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
	, sku_num
	, net_sales_amt_usd
	, gross_sales_amt_usd
	, returns_amt_usd
	, net_sales_units
	, gross_sales_units
	, return_units
	, Current_Timestamp AS dw_sys_load_tmstp
FROM	nmn_sales_fact_temp
WHERE   business_day_date >= {start_date}
AND     business_day_date <= {end_date}
;


COLLECT STATISTICS  
                    COLUMN (business_day_date, sku_num), -- column names used for primary index
                    COLUMN (business_day_date)  -- column names used for partition
ON {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT;

SET QUERY_BAND = NONE FOR SESSION;
