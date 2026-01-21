SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_campaign_attributed_funnel_fact_11521_ACE_ENG;
     Task_Name=ddl_campaign_attributed_funnel_fact;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_NMN.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT
Team/Owner: Customer Engagement Analytics / Angelina Allen-St. John (E4JP)
Date Created/Modified: 2022-12-13 / 2024-01-18

Note:
-- This table displays product views and cart adds at the level of web_style_num - channel - day - utm_channel - utm_content, applying same-session attribution
-- Data begins 2022-01-31, in alignment with the start of session data in NAP
*/


CREATE MULTISET TABLE {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
    (
     activity_date_pacific	DATE
    ,channel_country 		CHAR(2) CHARACTER SET Unicode NOT CaseSpecific Compress ('CA','US')
    ,channel_banner 		VARCHAR(15) CHARACTER SET Unicode CaseSpecific Compress ('NORDSTROM','NORDSTROM_RACK','TRUNK_CLUB')
    ,channel_num		INTEGER
    ,mktg_type			VARCHAR(20) CHARACTER SET Unicode CaseSpecific Compress ('BASE','PAID','UNPAID')
    ,finance_rollup		VARCHAR(20) CHARACTER SET Unicode CaseSpecific Compress ('AFFILIATES','BASE','DISPLAY','EMAIL','PAID_OTHER','PAID_SEARCH','SEO','SHOPPING','SOCIAL','UNPAID_OTHER')
    ,finance_detail		VARCHAR(30) CHARACTER SET Unicode CaseSpecific Compress ('AFFILIATES','APP_PUSH_PLANNED','APP_PUSH_TRANSACTIONAL','BASE','DISPLAY','EMAIL_MARKETING','EMAIL_TRANSACT','EMAIL_TRIGGER','PAID_MISC','PAID_SEARCH_BRANDED','PAID_SEARCH_UNBRANDED','SEO_LOCAL','SEO_SEARCH','SEO_SHOPPING','SHOPPING','SOCIAL_ORGANIC','SOCIAL_PAID')
    ,utm_source			VARCHAR(200) CHARACTER SET Unicode NOT CaseSpecific
    ,utm_channel		VARCHAR(70) CHARACTER SET Unicode NOT CaseSpecific
    ,utm_campaign       VARCHAR(90) CHARACTER SET Unicode NOT CaseSpecific
    ,utm_term		VARCHAR(300) CHARACTER SET Unicode NOT CaseSpecific
    ,utm_content		VARCHAR(300) CHARACTER SET Unicode NOT CaseSpecific
    ,supplier_name		VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific
    ,brand_name			VARCHAR(60) CHARACTER SET Unicode NOT CaseSpecific
    ,web_style_num		BIGINT
    ,product_views		INTEGER
    ,instock_product_views	INTEGER
    ,cart_add_units		INTEGER
    ,order_units 		INTEGER
    ,dw_sys_load_tmstp 		TIMESTAMP(6) DEFAULT Current_Timestamp(6) NOT NULL
    )
PRIMARY INDEX(activity_date_pacific, web_style_num, channel_num, utm_channel)
PARTITION BY Range_N(activity_date_pacific BETWEEN DATE '2021-01-31' AND DATE '2028-12-31' EACH INTERVAL '1' DAY)
;

-- Table Comment (STANDARD)
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT IS 'NMN campaign-attributed product views and cart adds';
-- Column comments (OPTIONAL)
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.activity_date_pacific 	IS 'Date of funnel activity';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.channel_country			IS 'Country code of online channel';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.channel_banner 			IS 'Banner of online channel';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.channel_num 			IS 'Online channel';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.mktg_type 				IS 'Marketing type (Paid, Unpaid, Base) attributed to funnel activity';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.finance_rollup 			IS 'Finance-defined marketing attribution level of detail one step more granular than mktg_type';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.finance_detail 			IS 'Finance-defined marketing attribution level of detail one step more granular than finance_rollup';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.utm_source 				IS 'UTM source of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.utm_channel 			IS 'UTM channel of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.utm_campaign 			IS 'UTM campaign of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.utm_term				IS 'UTM term of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.utm_content 			IS 'UTM content of the marketing touch associated with funnel session';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.supplier_name 			IS 'Order-from vendor of funnel activity item';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.brand_name 				IS 'Brand name of funnel activity item';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.web_style_num			IS 'web_style_num of the funnel activity item';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.product_views			IS 'Total number of views for funnel activity item';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.instock_product_views	IS 'Number of views for funnel activity item that was instock at time of view';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.cart_add_units			IS 'Number of units of funnel activity item added to shopping cart(s)';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.order_units			IS 'Number of orders placed for funnel activity item';
COMMENT ON  {nmn_t2_schema}.CAMPAIGN_ATTRIBUTED_FUNNEL_FACT.dw_sys_load_tmstp		IS 'Data load timestamp';

SET QUERY_BAND = NONE FOR SESSION;
