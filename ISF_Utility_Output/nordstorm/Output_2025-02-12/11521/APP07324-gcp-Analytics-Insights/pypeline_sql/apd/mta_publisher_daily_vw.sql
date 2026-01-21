SET QUERY_BAND = 'App_ID=APP08629;
     DAG_ID=mta_publisher_daily_vw_11521_ACE_ENG;
     Task_Name=mta_publisher_daily_vw;'
     FOR SESSION VOLATILE;

--T2/View Name: {apd_t2_schema}.mta_publisher_daily_vw
--Team/Owner: Analytics Engineering
--Date Created/Modified: 2023-09-27
--Note:
-- This view supports the 4 BOX Affiliates Performance Dashboard-Consolidated Version.


REPLACE VIEW {apd_t2_schema}.mta_publisher_daily_vw 
AS
LOCK ROW FOR ACCESS 
SELECT
    day_dt
    , DAY_NUM
    , FISCAL_WEEK
    , BANNER
    , UTM_CAMPAIGN
    , PUBLISHER
    , PUBLISHER_GROUP
    , PUBLISHER_SUBGROUP
    , AARE_STATUS_CODE
    , LOYALTY_STATUS
    , DIVISION
    , SUB_DIVISION
    , DEPARTMENT
    , BRAND_NAME
    , NPG_CODE
    , PRICE_TYPE
     -- TY
    , sum(attributed_demand) as attributed_demand
    , sum(attributed_orders) as attributed_orders
    , sum(attributed_net_sales) as attributed_net_sales
    , sum(retained_attributed_orders) as retained_attributed_orders
    , sum(acquired_attributed_orders) as acquired_attributed_orders
    , sum(acquired_attributed_demand) as acquired_attributed_demand
     -- LY
    , sum(ly_attributed_demand) as ly_attributed_demand
    , sum(ly_attributed_orders) as ly_attributed_orders
    , sum(ly_attributed_net_sales) as ly_attributed_net_sales
    , sum(ly_retained_attributed_orders) as ly_retained_attributed_orders
    , sum(ly_acquired_attributed_orders) as ly_acquired_attributed_orders
    , sum(ly_acquired_attributed_demand) as ly_acquired_attributed_demand
FROM (
    --TY
    SELECT
        d.day_date AS day_dt
       , d.DAY_NUM
        ,CONCAT(d.month_short_desc,' ',d.week_desc) AS FISCAL_WEEK
        , mpcs_ty.BANNER
        , mpcs_ty.UTM_CAMPAIGN
        , mpcs_ty.PUBLISHER
        , mpcs_ty.PUBLISHER_GROUP
        , mpcs_ty.PUBLISHER_SUBGROUP
        , mpcs_ty.AARE_STATUS_CODE
        , mpcs_ty.LOYALTY_STATUS
        , mpcs_ty.DIVISION
        , mpcs_ty.SUB_DIVISION
        , mpcs_ty.DEPARTMENT
        , mpcs_ty.BRAND_NAME
        , mpcs_ty.NPG_CODE
        , mpcs_ty.PRICE_TYPE        
        -- TY
        , mpcs_ty.attributed_demand
        , mpcs_ty.attributed_orders
        , mpcs_ty.attributed_net_sales
        , mpcs_ty.retained_attributed_orders
        , mpcs_ty.acquired_attributed_orders
        , mpcs_ty.acquired_attributed_demand        
        -- LY
        , CAST(0 AS FLOAT) as ly_attributed_demand
        , CAST(0 AS FLOAT) as ly_attributed_orders
        , CAST(0 AS FLOAT) as ly_attributed_net_sales
        , CAST(0 AS FLOAT) as ly_retained_attributed_orders
        , CAST(0 AS FLOAT) as ly_acquired_attributed_orders
        , CAST(0 AS FLOAT) as ly_acquired_attributed_demand
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {apd_t2_schema}.mta_publisher_daily mpcs_ty
            on d.day_date =  mpcs_ty.day_dt
    UNION ALL
    --LY
    SELECT
        d.day_date AS day_dt
        ,d.DAY_NUM
       ,CONCAT(d.month_short_desc,' ',d.week_desc) AS FISCAL_WEEK
        , mpcs_ly.BANNER
        , mpcs_ly.UTM_CAMPAIGN
        , mpcs_ly.PUBLISHER
        , mpcs_ly.PUBLISHER_GROUP
        , mpcs_ly.PUBLISHER_SUBGROUP
        , mpcs_ly.AARE_STATUS_CODE
        , mpcs_ly.LOYALTY_STATUS
        , mpcs_ly.DIVISION
        , mpcs_ly.SUB_DIVISION
        , mpcs_ly.DEPARTMENT
        , mpcs_ly.BRAND_NAME
        , mpcs_ly.NPG_CODE
        , mpcs_ly.PRICE_TYPE  
        -- TY
        , CAST(0 AS FLOAT) AS attributed_demand
        , CAST(0 AS FLOAT) AS attributed_orders
        , CAST(0 AS FLOAT) AS attributed_net_sales
        , CAST(0 AS FLOAT) AS retained_attributed_orders
        , CAST(0 AS FLOAT) AS acquired_attributed_orders
        , CAST(0 AS FLOAT) AS acquired_attributed_demand
        -- LY
        , mpcs_ly.attributed_demand     AS ly_attributed_demand
        , mpcs_ly.attributed_orders      AS ly_attributed_orders
        , mpcs_ly.attributed_net_sales     AS ly_attributed_net_sales
        , mpcs_ly.retained_attributed_orders   AS ly_retained_attributed_orders
        , mpcs_ly.acquired_attributed_orders              AS ly_acquired_attributed_orders
        , mpcs_ly.acquired_attributed_demand      AS ly_acquired_attributed_demand
    FROM PRD_NAP_USR_VWS.DAY_CAL d
        inner join {apd_t2_schema}.mta_publisher_daily mpcs_ly
            on d.last_year_day_date_realigned =  mpcs_ly.day_dt
) mpcs
WHERE mpcs.day_dt <= (select max(day_dt ) from {apd_t2_schema}.mta_publisher_daily)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16;


SET QUERY_BAND = NONE FOR SESSION;

