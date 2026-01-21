/*

T2/Table Name: T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_DAILY_FACT
Team/Owner: NMN / Custmer Engagement Analytics
Date Created/Modified: 2023-09-12

Refresh: Daily

*/

SET QUERY_BAND = 'App_ID=APP08162;
DAG_ID=campaign_attributed_daily_fact_11521_ACE_ENG;
Task_Name=campaign_attributed_daily_fact;' 
FOR SESSION VOLATILE;

CREATE MULTISET VOLATILE TABLE caf_ty AS (
SELECT
dd.day_date
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
,caf.utm_content
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
,CASE
    WHEN dd.day_date > date'2023-12-01' THEN (
        CASE
            WHEN caf.utm_term LIKE 'social@_%' ESCAPE '@' THEN NULL
            WHEN caf.utm_term LIKE '%campaign%' THEN NULL
            WHEN caf.utm_channel = 'low_ex_email_marketing' THEN RegExp_Substr(caf.utm_term,'[^_]+',1,1)
            WHEN caf.utm_channel LIKE '%@_ex@_%' ESCAPE '@' THEN (
                CASE
                    WHEN Length(caf.utm_term) - Length(OReplace(caf.utm_term, '_', '')) = 2
                        THEN RegExp_Substr(caf.utm_term,'[^_]+',1,1)
                    WHEN Length(caf.utm_term) - Length(OReplace(caf.utm_term, '-', '')) = 2
                        THEN RegExp_Substr(caf.utm_term,'[^-]+',1,1)
                    WHEN Right(caf.utm_term,4) = 'pmax'
                        THEN Substr(caf.utm_term,1,Instr(caf.utm_term,'_')-1)
                    WHEN Length(caf.utm_term) - Length(OReplace(caf.utm_term, '%5F', '')) >= 6 THEN  
                        Substring(caf.utm_term FROM 1 FOR Position('%5F' IN caf.utm_term) - 1)  
                    ELSE NULL
                END
            )
            ELSE NULL
        END
    )
    ELSE NULL
END AS campaign_id
,Trim(CASE
    WHEN dd.day_date > date'2023-12-01' THEN (
        CASE
            WHEN utm_campaign LIKE '%afterpay%' THEN '5188900'
            WHEN utm_channel = 'low_ex_email_marketing' THEN NULL
            WHEN utm_channel LIKE '%@_ex@_%' ESCAPE '@' THEN (
                CASE
                    WHEN utm_campaign LIKE '%e012894%' THEN NULL
                    WHEN (Length(caf.utm_campaign) - Length(OReplace(caf.utm_campaign, '_', '')) = 8 OR Length(caf.utm_campaign) - Length(OReplace(caf.utm_campaign, '_', '')) = 5)
                    THEN Substr(caf.utm_campaign,Instr(caf.utm_campaign,'_',1,3)+1,(Instr(caf.utm_campaign,'_',1,4)-Instr(caf.utm_campaign,'_',1,3))-1)
                    WHEN (Length(caf.utm_campaign) - Length(OReplace(caf.utm_campaign, '_', '')) = 3 OR (Length(caf.utm_campaign) - Length(OReplace(caf.utm_campaign, '_', '')) = 7))
                    THEN Substr(caf.utm_campaign,Instr(caf.utm_campaign,'_',1,2)+1,(Instr(caf.utm_campaign,'_',1,3)-Instr(caf.utm_campaign,'_',1,2))-1)
                    ELSE NULL
                END
            )
            ELSE NULL
        END
    )
    ELSE NULL
END) AS campaign_brand_id
,CASE
	WHEN utm_campaign like '%n_202%' THEN Substring(utm_campaign From (Length(utm_campaign) - Position('_' IN Reverse(utm_campaign)) + 2))
		WHEN utm_campaign like '%nr_202%' THEN Substring(utm_campaign From (Length(utm_campaign) - Position('_' IN Reverse(utm_campaign)) + 2))
 	ELSE NULL
END AS placementsio_id
FROM T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT caf
LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
ON caf.activity_date_pacific = dd.DAY_DATE
LEFT JOIN (SELECT DISTINCT web_style_num, dept_num, div_num, grp_num, div_desc, grp_desc, dept_desc, channel_country FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW) sku
ON caf.web_style_num = sku.web_style_num
AND caf.channel_country = sku.channel_country
WHERE 1=1
AND dd.day_date BETWEEN date'2024-09-01' AND date'2024-10-10'
GROUP BY
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, campaign_id, campaign_brand_id, placementsio_id) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE caf_ly AS(
	SELECT
		 dd.day_date
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
		,caf.utm_content
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
		,CASE
    WHEN dd.day_date > date'2023-12-01' THEN (
        CASE
            WHEN caf.utm_term LIKE 'social@_%' ESCAPE '@' THEN NULL
            WHEN caf.utm_term LIKE '%campaign%' THEN NULL
            WHEN caf.utm_channel = 'low_ex_email_marketing' THEN RegExp_Substr(caf.utm_term,'[^_]+',1,1)
            WHEN caf.utm_channel LIKE '%@_ex@_%' ESCAPE '@' THEN (
                CASE
                    WHEN Length(caf.utm_term) - Length(OReplace(caf.utm_term, '_', '')) = 2
                        THEN RegExp_Substr(caf.utm_term,'[^_]+',1,1)
                    WHEN Length(caf.utm_term) - Length(OReplace(caf.utm_term, '-', '')) = 2
                        THEN RegExp_Substr(caf.utm_term,'[^-]+',1,1)
                    WHEN Right(caf.utm_term,4) = 'pmax'
                        THEN Substr(caf.utm_term,1,Instr(caf.utm_term,'_')-1)
                    WHEN Length(caf.utm_term) - Length(OReplace(caf.utm_term, '%5F', '')) >= 6 THEN  
                        Substring(caf.utm_term FROM 1 FOR Position('%5F' IN caf.utm_term) - 1)  
                    ELSE NULL
                END
            )
            ELSE NULL
        END
    )
    ELSE NULL
END AS campaign_id
,Trim(CASE
    WHEN dd.day_date > date'2023-12-01' THEN (
        CASE
            WHEN utm_campaign LIKE '%afterpay%' THEN '5188900'
            WHEN utm_channel LIKE '%@_ex@_%' ESCAPE '@' THEN (
                CASE
                    WHEN utm_campaign LIKE '%e012894%' THEN NULL
                    WHEN utm_campaign = 'general_j009192' THEN NULL
                    WHEN utm_campaign LIKE '%j01%' THEN NULL
                    WHEN (Length(caf.utm_campaign) - Length(OReplace(caf.utm_campaign, '_', '')) = 8 OR Length(caf.utm_campaign) - Length(OReplace(caf.utm_campaign, '_', '')) = 5)
                    THEN Substr(caf.utm_campaign,Instr(caf.utm_campaign,'_',1,3)+1,(Instr(caf.utm_campaign,'_',1,4)-Instr(caf.utm_campaign,'_',1,3))-1)
                    WHEN (Length(caf.utm_campaign) - Length(OReplace(caf.utm_campaign, '_', '')) = 3 OR (Length(caf.utm_campaign) - Length(OReplace(caf.utm_campaign, '_', '')) = 7))
                    THEN Substr(caf.utm_campaign,Instr(caf.utm_campaign,'_',1,2)+1,(Instr(caf.utm_campaign,'_',1,3)-Instr(caf.utm_campaign,'_',1,2))-1)
                    ELSE NULL
                END
            )
            ELSE NULL
        END
    )
    ELSE NULL
END) AS campaign_brand_id
,CASE
	WHEN utm_campaign like '%n_202%' THEN Substring(utm_campaign From (Length(utm_campaign) - Position('_' IN Reverse(utm_campaign)) + 2)) 	
		WHEN utm_campaign like '%nr_202%' THEN Substring(utm_campaign From (Length(utm_campaign) - Position('_' IN Reverse(utm_campaign)) + 2))
	ELSE NULL
END AS placementsio_id
	FROM T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT caf
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
		ON caf.activity_date_pacific = dd.day_date_last_year_realigned
	LEFT JOIN (SELECT DISTINCT web_style_num, dept_num, div_num, grp_num, div_desc, grp_desc, dept_desc, channel_country FROM PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW) sku
		ON caf.web_style_num = sku.web_style_num	
		AND caf.channel_country = sku.channel_country	
	WHERE 1=1
		AND dd.day_date BETWEEN date'2024-09-01' AND date'2024-10-10'
	GROUP BY 
		 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, campaign_id, campaign_brand_id, placementsio_id) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE caf_agg AS (
	SELECT
		 day_date
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
		,utm_content
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
		,campaign_id
		,campaign_brand_id
		,placementsio_id
		,Cast(0 AS DECIMAL(12,2)) AS ly_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ly_gross_sales_units	
	FROM caf_ty
	UNION ALL
	SELECT
		 day_date
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
		,utm_content
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
		,campaign_id
		,campaign_brand_id
		,placementsio_id
		,Cast(0 AS DECIMAL(12,2)) AS ly_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ly_gross_sales_units	
	FROM caf_ly	
) WITH DATA PRIMARY INDEX(day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, campaign_id, campaign_brand_id, placementsio_id) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE cas_ty AS (
	SELECT
		 dd.day_date
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
		,cas.utm_content
		,cas.supplier_name
		,cas.brand_name
		,Trim(sku.div_num) || ', ' || sku.div_desc AS division
		,Trim(sku.grp_num) || ', ' || sku.grp_desc AS subdivision
		,Trim(sku.dept_num) || ', ' || sku.dept_desc AS department
		,Sum(cas.gross_sales_amt_usd) AS ty_gross_sales_amt 
		,Sum(cas.gross_sales_units) AS ty_gross_sales_units
		,Cast(0 AS DECIMAL(12,2)) AS ly_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ly_gross_sales_units
		,CASE
    WHEN dd.day_date > date'2023-12-01' THEN (
        CASE
            WHEN cas.utm_term LIKE 'social@_%' ESCAPE '@' THEN NULL
            WHEN cas.utm_term LIKE '%campaign%' THEN NULL
            WHEN cas.utm_channel = 'low_ex_email_marketing' THEN RegExp_Substr(cas.utm_term,'[^_]+',1,1)
            WHEN cas.utm_channel LIKE '%@_ex@_%' ESCAPE '@' THEN (
                CASE
                    WHEN Length(cas.utm_term) - Length(OReplace(cas.utm_term, '_', '')) = 2
                        THEN RegExp_Substr(cas.utm_term,'[^_]+',1,1)
                    WHEN Length(cas.utm_term) - Length(OReplace(cas.utm_term, '-', '')) = 2
                        THEN RegExp_Substr(cas.utm_term,'[^-]+',1,1)
                    WHEN Right(cas.utm_term,4) = 'pmax'
                        THEN Substr(cas.utm_term,1,Instr(cas.utm_term,'_')-1)
                    WHEN Length(cas.utm_term) - Length(OReplace(cas.utm_term, '%5F', '')) >= 6 THEN  
                        Substring(cas.utm_term FROM 1 FOR Position('%5F' IN cas.utm_term) - 1)  
                    ELSE NULL
                END
            )
            ELSE NULL
        END
    )
    ELSE NULL
END AS campaign_id
,Trim(CASE
    WHEN dd.day_date > date'2023-12-01' THEN (
        CASE
            WHEN utm_campaign LIKE '%afterpay%' THEN '5188900'
            WHEN utm_channel LIKE '%@_ex@_%' ESCAPE '@' THEN (
                CASE
                    WHEN utm_campaign LIKE '%e012894%' THEN NULL                    
                    WHEN utm_campaign = 'general_j009192' THEN NULL
                    WHEN utm_campaign LIKE '%j01%' THEN NULL
                    WHEN (Length(cas.utm_campaign) - Length(OReplace(cas.utm_campaign, '_', '')) = 8 OR Length(cas.utm_campaign) - Length(OReplace(cas.utm_campaign, '_', '')) = 5)
                    THEN Substr(cas.utm_campaign,Instr(cas.utm_campaign,'_',1,3)+1,(Instr(cas.utm_campaign,'_',1,4)-Instr(cas.utm_campaign,'_',1,3))-1)
                    WHEN (Length(cas.utm_campaign) - Length(OReplace(cas.utm_campaign, '_', '')) = 3 OR (Length(cas.utm_campaign) - Length(OReplace(cas.utm_campaign, '_', '')) = 7))
                    THEN Substr(cas.utm_campaign,Instr(cas.utm_campaign,'_',1,2)+1,(Instr(cas.utm_campaign,'_',1,3)-Instr(cas.utm_campaign,'_',1,2))-1)
                    ELSE NULL
                END
            )
            ELSE NULL
        END
    )
    ELSE NULL
END) AS campaign_brand_id
,CASE
	WHEN utm_campaign like '%n_202%' THEN Substring(utm_campaign From (Length(utm_campaign) - Position('_' IN Reverse(utm_campaign)) + 2)) 	
		WHEN utm_campaign like '%nr_202%' THEN Substring(utm_campaign From (Length(utm_campaign) - Position('_' IN Reverse(utm_campaign)) + 2))
	ELSE NULL
END AS placementsio_id
	FROM T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_SALES_FACT cas
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
		ON cas.business_day_date = dd.DAY_DATE
	LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
		ON cas.sku_num = sku.rms_sku_num
		AND cas.channel_country = sku.channel_country	
	WHERE 1=1
		AND dd.day_date BETWEEN date'2024-09-01' AND date'2024-10-10'
	GROUP BY 
		 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, campaign_id, campaign_brand_id, placementsio_id) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE cas_ly AS (
	SELECT
		 dd.day_date
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
		,cas.utm_content
		,cas.supplier_name
		,cas.brand_name
		,Trim(sku.div_num) || ', ' || sku.div_desc AS division
		,Trim(sku.grp_num) || ', ' || sku.grp_desc AS subdivision
		,Trim(sku.dept_num) || ', ' || sku.dept_desc AS department
		,Cast(0 AS DECIMAL(12,0)) AS ty_gross_sales_amt
		,Cast(0 AS DECIMAL(12,0)) AS ty_gross_sales_units	
		,Sum(cas.gross_sales_amt_usd) AS ly_gross_sales_amt 
		,Sum(cas.gross_sales_units) AS ly_gross_sales_units
		,CASE
    WHEN dd.day_date > date'2023-12-01' THEN (
        CASE
            WHEN cas.utm_term LIKE 'social@_%' ESCAPE '@' THEN NULL
            WHEN cas.utm_term LIKE '%campaign%' THEN NULL
            WHEN cas.utm_channel = 'low_ex_email_marketing' THEN RegExp_Substr(cas.utm_term,'[^_]+',1,1)
            WHEN cas.utm_channel LIKE '%@_ex@_%' ESCAPE '@' THEN (
                CASE
                    WHEN Length(cas.utm_term) - Length(OReplace(cas.utm_term, '_', '')) = 2
                        THEN RegExp_Substr(cas.utm_term,'[^_]+',1,1)
                    WHEN Length(cas.utm_term) - Length(OReplace(cas.utm_term, '-', '')) = 2
                        THEN RegExp_Substr(cas.utm_term,'[^-]+',1,1)
                    WHEN Right(cas.utm_term,4) = 'pmax'
                        THEN Substr(cas.utm_term,1,Instr(cas.utm_term,'_')-1)
                    WHEN Length(cas.utm_term) - Length(OReplace(cas.utm_term, '%5F', '')) >= 6 THEN  
                        Substring(cas.utm_term FROM 1 FOR Position('%5F' IN cas.utm_term) - 1)  
                    ELSE NULL
                END
            )
            ELSE NULL
        END
    )
    ELSE NULL
END AS campaign_id
,Trim(CASE
    WHEN dd.day_date > date'2023-12-01' THEN (
        CASE
            WHEN utm_campaign LIKE '%afterpay%' THEN '5188900'
            WHEN utm_channel = 'low_ex_email_marketing' THEN NULL
            WHEN utm_channel LIKE '%@_ex@_%' ESCAPE '@' THEN (
                CASE
                    WHEN utm_campaign LIKE '%e012894%' THEN NULL
                    WHEN (Length(cas.utm_campaign) - Length(OReplace(cas.utm_campaign, '_', '')) = 8 OR Length(cas.utm_campaign) - Length(OReplace(cas.utm_campaign, '_', '')) = 5)
                    THEN Substr(cas.utm_campaign,Instr(cas.utm_campaign,'_',1,3)+1,(Instr(cas.utm_campaign,'_',1,4)-Instr(cas.utm_campaign,'_',1,3))-1)
                    WHEN (Length(cas.utm_campaign) - Length(OReplace(cas.utm_campaign, '_', '')) = 3 OR (Length(cas.utm_campaign) - Length(OReplace(cas.utm_campaign, '_', '')) = 7))
                    THEN Substr(cas.utm_campaign,Instr(cas.utm_campaign,'_',1,2)+1,(Instr(cas.utm_campaign,'_',1,3)-Instr(cas.utm_campaign,'_',1,2))-1)
                    ELSE NULL
                END
            )
            ELSE NULL
        END
    )
    ELSE NULL
END) AS campaign_brand_id 
,CASE
	WHEN utm_campaign like '%n_202%' THEN Substring(utm_campaign From (Length(utm_campaign) - Position('_' IN Reverse(utm_campaign)) + 2)) 	
		WHEN utm_campaign like '%nr_202%' THEN Substring(utm_campaign From (Length(utm_campaign) - Position('_' IN Reverse(utm_campaign)) + 2))
	ELSE NULL
END AS placementsio_id
	FROM T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_SALES_FACT cas
	LEFT JOIN PRD_NAP_USR_VWS.DAY_CAL_454_DIM dd
		ON cas.business_day_date = dd.day_date_last_year_realigned
	LEFT JOIN PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
		ON cas.sku_num = sku.rms_sku_num
		AND cas.channel_country = sku.channel_country
	WHERE 1=1
		AND dd.day_date BETWEEN date'2024-09-01' AND date'2024-10-10'
	GROUP BY 
		 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA PRIMARY INDEX(day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, campaign_id, campaign_brand_id, placementsio_id) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE cas_agg AS (
	SELECT
		 day_date
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
		,utm_content
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
		,campaign_id
		,campaign_brand_id
		,placementsio_id
		,ly_gross_sales_amt
		,ly_gross_sales_units	
	FROM cas_ty
	UNION ALL
	SELECT
		 day_date
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
		,utm_content
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
		,campaign_id
		,campaign_brand_id
		,placementsio_id
		,ly_gross_sales_amt
		,ly_gross_sales_units	
	FROM cas_ly
) WITH DATA PRIMARY INDEX(day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, campaign_id, campaign_brand_id, placementsio_id) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE caf_cas AS (
		SELECT
			 day_date
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
			,utm_content
			,supplier_name
			,brand_name
			,division
			,subdivision
			,department
			,campaign_id
			,campaign_brand_id
			,placementsio_id
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
		 	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
		UNION ALL
		SELECT
			 day_date
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
			,utm_content
			,supplier_name
			,brand_name
			,division
			,subdivision
			,department
			,campaign_id
			,campaign_brand_id
			,placementsio_id
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
		 	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
) WITH DATA PRIMARY INDEX(day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, campaign_id, campaign_brand_id, placementsio_id) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (BRAND_NAME) ON caf_cas;

CREATE MULTISET VOLATILE TABLE cad_fact_stg AS (
SELECT
	 day_date
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
	,utm_content
	,supplier_name
	,brand_name
	,division
	,subdivision
	,department
	,campaign_id
	,campaign_brand_id
	,placementsio_id
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
 	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
) WITH DATA PRIMARY INDEX(day_date, channel_num, channel_country, channel_banner, mktg_type, finance_rollup, finance_detail, utm_source, utm_channel, utm_campaign, utm_term, utm_content, supplier_name, brand_name, division, subdivision, department, campaign_id, campaign_brand_id, placementsio_id) ON COMMIT PRESERVE ROWS;

DELETE FROM T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_DAILY_FACT WHERE day_date BETWEEN date'2024-09-01' AND date'2024-10-10';

INSERT INTO T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_DAILY_FACT 
SELECT a.*, Current_Timestamp AS dw_sys_load_tmstp FROM cad_fact_stg a;

COLLECT STATISTICS COLUMN (DAY_DATE, CHANNEL_NUM, CHANNEL_COUNTRY, CHANNEL_BANNER, MKTG_TYPE, FINANCE_ROLLUP, FINANCE_DETAIL, UTM_SOURCE, UTM_CHANNEL,utm_campaign, utm_term, UTM_CONTENT, SUPPLIER_NAME, BRAND_NAME, DIVISION, SUBDIVISION, DEPARTMENT, campaign_id, campaign_brand_id, placementsio_id) ON T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_DAILY_FACT;
COLLECT STATISTICS COLUMN (DAY_DATE) ON T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_DAILY_FACT;
COLLECT STATISTICS COLUMN (BRAND_NAME ,SUBDIVISION) ON T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_DAILY_FACT;

SET QUERY_BAND = NONE FOR SESSION;

