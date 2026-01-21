SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_campaign_attributed_sales_fact_11521_ACE_ENG;
     Task_Name=ddl_campaign_attributed_sales_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_SALES_FACT
Team/Owner: Customer Analytics / Angelina Allen-St. John (E4JP)
Date Created/Modified: 2022-12-21


Note:
-- This table displays sales at the level of sku - channel - day - utm_channel - utm_content, applying 30 day last-click attribution
-- Data begins 2022-01-31, in alignment with the start of session data in NAP
*/

CREATE MULTISET TABLE {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
    (
     business_day_date		DATE
    ,channel_country		CHAR(2) CHARACTER SET Unicode NOT CaseSpecific Compress ('CA','US')
    ,channel_banner			VARCHAR(15) CHARACTER SET Unicode CaseSpecific Compress ('NORDSTROM','NORDSTROM_RACK','TRUNK_CLUB')
    ,channel_num        	INTEGER
    ,mktg_type 				VARCHAR(20) CHARACTER SET Unicode CaseSpecific Compress ('BASE','PAID','UNPAID')
    ,finance_rollup 		VARCHAR(20) CHARACTER SET Unicode CaseSpecific Compress ('AFFILIATES','BASE','DISPLAY','EMAIL','PAID_OTHER','PAID_SEARCH','SEO','SHOPPING','SOCIAL','UNPAID_OTHER')
    ,finance_detail 		VARCHAR(30) CHARACTER SET Unicode CaseSpecific Compress ('AFFILIATES','APP_PUSH_PLANNED','APP_PUSH_TRANSACTIONAL','BASE','DISPLAY','EMAIL_MARKETING','EMAIL_TRANSACT','EMAIL_TRIGGER','PAID_MISC','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED','SEO_LOCAL','SEO_SEARCH','SEO_SHOPPING','SHOPPING','SOCIAL_ORGANIC','SOCIAL_PAID')
    ,utm_source				VARCHAR(200) CHARACTER SET Unicode NOT CaseSpecific
    ,utm_channel			VARCHAR(70) CHARACTER SET Unicode NOT CaseSpecific
    ,utm_campaign       	VARCHAR(90) CHARACTER SET Unicode NOT CaseSpecific
    ,utm_term               VARCHAR(300) CHARACTER SET UNICODE NOT CaseSpecific
    ,utm_content			VARCHAR(300) CHARACTER SET Unicode NOT CaseSpecific
    ,supplier_name 			VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific
    ,brand_name 			VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific
    ,sku_num 				VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific
    ,net_sales_amt_usd 		DECIMAL(16,2)
    ,gross_sales_amt_usd 	DECIMAL(16,2)
    ,returns_amt_usd 		DECIMAL(16,2)
    ,net_sales_units		DECIMAL(12,0) Compress 1. 
    ,gross_sales_units		DECIMAL(12,0) Compress 1.
    ,return_units			DECIMAL(12,0) Compress 1. 
    ,dw_sys_load_tmstp  	TIMESTAMP(6) DEFAULT Current_Timestamp(6) NOT NULL
    )
PRIMARY INDEX(business_day_date, sku_num)
PARTITION BY Range_N(business_day_date BETWEEN DATE '2021-01-31' AND DATE '2028-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT IS 'NMN campaign-attributed sales ';
-- Column comments (OPTIONAL)
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.business_day_date 	IS 'Business date of transaction';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.channel_country	    IS 'Country code of intent store';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.channel_banner 	    IS 'Banner of intent store';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.channel_num 		    IS 'Selling channel of intent store';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.mktg_type 		    IS 'Marketing type (Paid, Unpaid, Base) attributed to order';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.finance_rollup 	    IS 'Finance-defined marketing attribution level of detail one step more granular than mktg_type';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.finance_detail 	    IS 'Finance-defined marketing attribution level of detail one step more granular than finance_rollup';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.utm_source 			IS 'UTM source of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.utm_channel 			IS 'UTM channel of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.utm_campaign 		IS 'UTM campaign of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.utm_term 		    IS 'UTM term of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.utm_content 			IS 'UTM content of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.supplier_name 	    IS 'Order-from vendor of SKU';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.brand_name 		    IS 'Brand name of SKU';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.sku_num 		        IS 'SKU of the ordered item';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.net_sales_amt_usd	IS 'Attributed sales minus returns in US dollars';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.gross_sales_amt_usd	IS 'Attributed sales in US dollars';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.returns_amt_usd 	    IS 'Returns in US dollars';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.net_sales_units	    IS 'Attributed units sold minus returned units';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.gross_sales_units	IS 'Attributed units sold';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.return_units		    IS 'Returned units';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_SALES_FACT.dw_sys_load_tmstp	IS 'Data load timestamp';

SET QUERY_BAND = NONE FOR SESSION;
