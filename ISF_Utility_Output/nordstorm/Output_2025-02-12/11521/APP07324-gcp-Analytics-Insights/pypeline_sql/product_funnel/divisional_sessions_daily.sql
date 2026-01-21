SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=divisional_sessions_daily_11521_ACE_ENG;
     Task_Name=divisional_sessions_daily;'
     FOR SESSION VOLATILE;


-- FILENAME: divisional_sessions_daily.sql
-- GOAL: Sessions at divisional level daily
-- AUTHOR: Rasagnya Avala (rasagnya.avala@nordstrom.com)

-- add to bag sessions

create multiset volatile table atb_sessions as 
(SELECT DISTINCT session_id
    , activity_date_pacific
    , case when csssf.channel = 'NORDSTROM_RACK' then 'RACK' when csssf.channel = 'NORDSTROM' then 'FULL_LINE' end as channel
    , case when experience = 'MOBILE_WEB' then 'MOW' 
    when experience = 'DESKTOP_WEB' then 'WEB' 
    when experience = 'IOS_APP' then 'IOS'
    when experience = 'ANDROID_APP' then 'ANDROID'
    when experience = 'POINT_OF_SALE' then 'POS' 
    when experience = 'UNKNOWN' then 'UNKNOWN' 
    when experience = 'CARE_PHONE' then 'CSR_PHONE'
    when experience = 'THIRD_PARTY_VENDOR' then 'VENDOR' 
    end as platform
    , channelcountry 
    , product_idtype
    , product_id AS sku_id
    , productstyle_id AS style_id
    , productstyle_idtype
    , NULL AS product_views
    , product_cart_add_units
    , NULL AS product_order_units
    , NULL AS product_order_value
 from PRD_NAP_USR_VWS.CUSTOMER_SESSION_STYLE_SKU_FACT csssf 
    WHERE csssf.activity_date_pacific BETWEEN {start_date} AND {end_date})
with data primary index(activity_date_pacific, session_id, sku_id, style_id, channelcountry, platform) on commit preserve rows;

collect stats column(activity_date_pacific, session_id, sku_id, style_id, channelcountry, platform) on atb_sessions;

-- order sessions

create multiset volatile table order_sessions as 
(SELECT DISTINCT session_id
    , activity_date_pacific
    , case when csssf.channel = 'NORDSTROM_RACK' then 'RACK' when csssf.channel = 'NORDSTROM' then 'FULL_LINE' end as channel
    , case when experience = 'MOBILE_WEB' then 'MOW' 
    when experience = 'DESKTOP_WEB' then 'WEB' 
    when experience = 'IOS_APP' then 'IOS'
    when experience = 'ANDROID_APP' then 'ANDROID'
    when experience = 'POINT_OF_SALE' then 'POS' 
    when experience = 'UNKNOWN' then 'UNKNOWN' 
    when experience = 'CARE_PHONE' then 'CSR_PHONE'
    when experience = 'THIRD_PARTY_VENDOR' then 'VENDOR' 
    end as platform
    , channelcountry 
    , product_idtype 
    , product_id AS sku_id
    , productstyle_id AS style_id
    , productstyle_idtype
    , NULL AS product_views
    , NULL AS product_cart_add_units
    , product_order_units
    , product_order_value
    from PRD_NAP_USR_VWS.CUSTOMER_SESSION_STYLE_SKU_FACT csssf 
    WHERE csssf.activity_date_pacific BETWEEN {start_date} AND {end_date})
with data primary index(activity_date_pacific, session_id, sku_id, style_id, channelcountry, platform) on commit preserve rows;

collect stats column(activity_date_pacific, session_id, sku_id, style_id, channelcountry, platform) on order_sessions;

-- product sessions

create multiset volatile table product_sessions as (
    SELECT DISTINCT session_id
    , pfd.event_date_pacific AS activity_date_pacific
    , pfd.channel
    , pfd.platform
    , pfd.channelcountry
    , 'RMS' AS productid_type
    , pfd.rms_sku_num AS sku_id
    , CAST(pfd.web_style_num AS varchar(10)) AS style_id
    , productstyle_idtype
    , product.product_views
    , product_cart_add_units
    , product_order_units
    , product_order_value
   FROM (
    SELECT DISTINCT session_id
    , activity_date_pacific
    , case when channel = 'NORDSTROM_RACK' then 'RACK' when channel = 'NORDSTROM' then 'FULL_LINE' end as channel
    , case when experience = 'MOBILE_WEB' then 'MOW' 
    when experience = 'DESKTOP_WEB' then 'WEB' 
    when experience = 'IOS_APP' then 'IOS'
    when experience = 'ANDROID_APP' then 'ANDROID'
    when experience = 'POINT_OF_SALE' then 'POS' 
    when experience = 'UNKNOWN' then 'UNKNOWN' 
    when experience = 'CARE_PHONE' then 'CSR_PHONE'
    when experience = 'THIRD_PARTY_VENDOR' then 'VENDOR' 
    end as platform
    , channelcountry
    , productstyle_id 
    , productstyle_idtype
    , product_views
    , NULL AS product_cart_add_units
    , NULL AS product_order_units
    , NULL AS product_order_value
 FROM PRD_NAP_USR_VWS.CUSTOMER_SESSION_STYLE_FACT 
     WHERE activity_date_pacific BETWEEN {start_date} AND {end_date}
) product
RIGHT JOIN T2DL_DAS_PRODUCT_FUNNEL.product_price_funnel_daily pfd 
    ON CAST(pfd.web_style_num AS varchar(10)) = productstyle_id 
    AND pfd.platform = product.platform 
    AND pfd.channel = product.channel 
    AND pfd.event_date_pacific = product.activity_date_pacific 
    AND pfd.channelcountry = product.channelcountry 
    WHERE event_date_pacific BETWEEN {start_date} AND {end_date}
) with data primary index(activity_date_pacific, session_id, sku_id, style_id, channelcountry, platform) on commit preserve rows;

collect stats column(activity_date_pacific, session_id, sku_id, style_id, channelcountry, platform) on product_sessions;

-- union add to bag , order and product sessions

CREATE MULTISET VOLATILE table sessions_fact AS 
(
SELECT * FROM product_sessions
UNION ALL
SELECT * FROM atb_sessions
UNION ALL 
SELECT * FROM order_sessions
) with data primary index(activity_date_pacific, session_id, sku_id, style_id, channelcountry, platform) on commit preserve rows;

collect stats column(activity_date_pacific, session_id, sku_id, style_id, channelcountry, platform) on sessions_fact;

-- bring additional metrics from ppfd

CREATE VOLATILE MULTISET TABLE ppfd AS (SELECT 
event_date_pacific
      , ppfd.channelcountry
      , ppfd.channel
      , ppfd.platform
      , ppfd.web_style_num as style_id
      , ppfd.rms_sku_num as sku_id
      , order_quantity
      , order_demand
      , add_to_bag_quantity
      , product_views
      , current_price_type
      , current_price_amt
      , regular_price_amt
FROM T2DL_DAS_PRODUCT_FUNNEL.product_price_funnel_daily ppfd WHERE event_date_pacific BETWEEN {start_date} AND {end_date})
with data primary index(event_date_pacific, sku_id, style_id, channelcountry, platform) on commit preserve rows;

collect stats column(event_date_pacific, sku_id, style_id, channelcountry, platform) on ppfd;

-- product hierarchy
 create multiset volatile table sku_base as (
    select
        sku.rms_sku_num
        , sku.channel_country
        , cast(sku.web_style_num as varchar(10)) as web_style_num
        , sty.style_group_num
        , CAST(sku.div_num AS VARCHAR(20)) || ', ' || sku.div_desc AS division
        , CAST(sku.grp_num AS VARCHAR(20)) || ', ' || sku.grp_desc AS subdiv
        , CAST(sku.dept_num AS VARCHAR(20)) || ', ' || sku.dept_desc AS department
        , CAST(sku.class_num AS VARCHAR(20)) || ', ' || sku.class_desc AS "class"
        , CAST(sku.sbclass_num AS VARCHAR(20)) || ', ' || sku.sbclass_desc AS subclass
        , UPPER(sku.brand_name) AS brand_name
        , UPPER(supp.vendor_name) AS supplier
        , UPPER(sty.type_level_1_desc) AS product_type_1
        , UPPER(sty.style_desc) AS style_desc
    from prd_nap_usr_vws.product_sku_dim_vw sku
    inner join prd_nap_usr_vws.product_style_dim sty
    on sku.epm_style_num = sty.epm_style_num
      and sku.channel_country = sty.channel_country
    left join prd_nap_usr_vws.vendor_dim supp
    on sku.prmy_supp_num =supp.vendor_num
    --Exclude GWP
    where not sku.dept_num IN ('584', '585', '523') and not (sku.div_num = '340' and sku.class_num = '90')
) with data primary index(rms_sku_num, channel_country) on commit preserve rows;

collect stats primary index(rms_sku_num, channel_country), column(rms_sku_num, channel_country) on sku_base;

-- divisional sessions daily

CREATE MULTISET VOLATILE table div_sessions_daily AS 
(
SELECT session_id
    , activity_date_pacific
    , sf.channel
    , sf.platform
    , sf.channelcountry
    , division
    , subdiv
    , department
    , sf.regular_price_amt
    , sf.current_price_amt
    , sf.current_price_type
    , sum(sf.pfd_product_views) AS product_views
    , count(distinct case when sf.product_views  > 0 then 1 end) as product_viewed_session
    , sum(sf.pfd_add_to_bag_quantity) AS add_to_bag_quantity
    , count(distinct case when sf.product_cart_add_units > 0 then 1 end) as cart_add_session
    , sum(sf.pfd_order_demand) AS order_demand
    , sum(sf.pfd_order_quantity) AS order_quantity
    , count(distinct case when sf.product_order_units > 0 then 1 end) as web_order_session
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
FROM (SELECT 
      session_id
    , activity_date_pacific
    , sf.sku_id
    , sf.style_id
    , sf.channel
    , sf.platform
    , sf.channelcountry
    , ppfd.current_price_type
    , ppfd.regular_price_amt
    , ppfd.current_price_amt
    , CASE WHEN ROW_NUMBER() OVER (PARTITION BY ppfd.sku_id,ppfd.style_id,ppfd.channel,ppfd.platform,activity_date_pacific  ORDER BY activity_date_pacific) = 1 THEN ppfd.product_views ELSE 0 END AS pfd_product_views
    , sf.product_views 
    , CASE WHEN ROW_NUMBER() OVER (PARTITION BY ppfd.sku_id,ppfd.style_id,ppfd.channel,ppfd.platform,activity_date_pacific  ORDER BY activity_date_pacific) = 1 THEN add_to_bag_quantity ELSE 0 END AS pfd_add_to_bag_quantity
    , sf.product_cart_add_units
    , CASE WHEN ROW_NUMBER() OVER (PARTITION BY ppfd.sku_id,ppfd.style_id,ppfd.channel,ppfd.platform,activity_date_pacific ORDER BY activity_date_pacific) = 1 THEN order_quantity ELSE 0 END AS pfd_order_quantity
    , CASE WHEN ROW_NUMBER() OVER (PARTITION BY ppfd.sku_id,ppfd.style_id,ppfd.channel,ppfd.platform,activity_date_pacific  ORDER BY activity_date_pacific) = 1 THEN order_demand ELSE 0 END AS pfd_order_demand
    , sf.product_order_units
    FROM sessions_fact sf
    LEFT JOIN ppfd  
       ON sf.activity_date_pacific = ppfd.event_date_pacific 
	   AND sf.sku_id = ppfd.sku_id 
	   AND sf.style_id = ppfd.style_id
	   AND sf.channel = ppfd.channel
	   AND sf.platform = ppfd.platform
 	   AND sf.channelcountry = ppfd.channelcountry) sf
 	INNER JOIN sku_base psdv
       ON psdv.rms_sku_num = sf.sku_id
       AND psdv.web_style_num = sf.style_id
       AND psdv.channel_country = sf.channelcountry
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(activity_date_pacific, session_id, channelcountry, platform) on commit preserve rows;

collect stats column(activity_date_pacific, session_id, channelcountry, platform) on divisional_sessions_daily;

-- final insert 

delete from {product_funnel_t2_schema}.divisional_sessions_daily where activity_date_pacific between {start_date} and {end_date};
insert into {product_funnel_t2_schema}.divisional_sessions_daily
select * from div_sessions_daily;

collect stats column(activity_date_pacific, session_id, channelcountry, platform) on {product_funnel_t2_schema}.divisional_sessions_daily;

SET QUERY_BAND = NONE FOR SESSION;