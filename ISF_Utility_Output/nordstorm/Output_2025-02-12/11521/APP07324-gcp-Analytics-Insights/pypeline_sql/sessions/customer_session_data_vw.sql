
SET QUERY_BAND = 'App_ID=APP09330;
     DAG_ID=customer_session_data_vw_11521_ACE_ENG;
     Task_Name=customer_session_data_vw;'
     FOR SESSION VOLATILE;


CREATE VIEW {sessions_t2_schema}.customer_session_data_vw 
AS
LOCK ROW FOR ACCESS 
SELECT
    day_dt
    , DAY_NUM
    , FISCAL_WEEK
    , channel
    , mrkt_type
    , finance_rollup
    , finance_detail
    , engagement_cohort
    , LOYALTY_STATUS
     -- TY
    , sum(total_sessions) as total_sessions
    , sum(bounced_sessions) as bounced_sessions
    , sum(product_views) as product_views
    , sum(cart_adds) as cart_adds
    , sum(web_orders) as web_orders
    , sum(web_demands) as web_demands
    , sum(web_ordered_units) as web_ordered_units
    , sum(web_demand_usd) as web_demand_usd
     -- LY
    , sum(ly_total_sessions) as ly_total_sessions
    , sum(ly_bounced_sessions) as ly_bounced_sessions
    , sum(ly_product_views) as ly_product_views
    , sum(ly_cart_adds) as ly_cart_adds
    , sum(ly_web_orders) as ly_web_orders
    , sum(ly_web_demands) as ly_web_demands
    , sum(ly_web_ordered_units) as ly_web_ordered_units
    , sum(ly_web_demand_usd) as ly_web_demand_usd
FROM (
    --TY
    SELECT
        d.day_date AS day_dt
       , d.DAY_NUM
        ,CONCAT(d.month_short_desc,' ',d.week_desc) AS FISCAL_WEEK
        , csd_ty.channel
        , csd_ty.mrkt_type
        , csd_ty.finance_rollup
        , csd_ty.finance_detail
        , csd_ty.engagement_cohort
        , csd_ty.LOYALTY_STATUS      
        -- TY
        , csd_ty.total_sessions AS total_sessions
        , csd_ty.bounced_sessions as bounced_sessions
        , csd_ty.product_views as product_views
        , csd_ty.cart_adds as cart_adds
        , csd_ty.web_orders as web_orders
        , csd_ty.web_demands as web_demands
        , csd_ty.web_ordered_units as web_ordered_units
        , csd_ty.web_demand_usd as web_demand_usd
        -- LY
        , CAST(0 AS FLOAT) as ly_total_sessions
        , CAST(0 AS FLOAT) as ly_bounced_sessions
        , CAST(0 AS FLOAT) as ly_product_views
        , CAST(0 AS FLOAT) as ly_cart_adds
        , CAST(0 AS FLOAT) as ly_web_orders
        , CAST(0 AS FLOAT) as ly_web_demands
        , CAST(0 AS FLOAT) as ly_web_ordered_units
        , CAST(0 AS FLOAT) as ly_web_demand_usd
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {sessions_t2_schema}.customer_session_data csd_ty
            on d.day_date =  csd_ty.day_dt
    UNION ALL
    --LY
    SELECT
        d.day_date AS day_dt
        ,d.DAY_NUM
       ,CONCAT(d.month_short_desc,' ',d.week_desc) AS FISCAL_WEEK
        , csd_ly.channel
        , csd_ly.mrkt_type
        , csd_ly.finance_rollup
        , csd_ly.finance_detail
        , csd_ly.engagement_cohort
        , csd_ly.LOYALTY_STATUS
        -- TY
        , CAST(0 AS FLOAT) AS total_sessions
        , CAST(0 AS FLOAT) AS bounced_sessions
        , CAST(0 AS FLOAT) AS product_views
        , CAST(0 AS FLOAT) AS cart_adds
        , CAST(0 AS FLOAT) AS web_orders
        , CAST(0 AS FLOAT) AS web_demands
        , CAST(0 AS FLOAT) AS web_ordered_units
        , CAST(0 AS FLOAT) AS web_demand_usd
        -- LY
        , csd_ly.total_sessions     AS ly_total_sessions
        , csd_ly.bounced_sessions      AS ly_bounced_sessions
        , csd_ly.product_views     AS ly_product_views
        , csd_ly.cart_adds   AS ly_cart_adds
        , csd_ly.web_orders              AS ly_web_orders
        , csd_ly.web_demands      AS ly_web_demands
        , csd_ly.web_ordered_units      AS ly_web_ordered_units
        , csd_ly.web_demand_usd      AS ly_web_demand_usd
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {sessions_t2_schema}.customer_session_data csd_ly
            on d.last_year_day_date_realigned =  csd_ly.day_dt
) csd
WHERE csd.day_dt <= (select max(day_dt ) from {sessions_t2_schema}.customer_session_data)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

SET QUERY_BAND = NONE FOR SESSION;
