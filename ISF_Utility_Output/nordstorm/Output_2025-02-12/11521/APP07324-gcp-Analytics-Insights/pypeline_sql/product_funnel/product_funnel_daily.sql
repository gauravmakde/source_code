SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=product_funnel_daily_11521_ACE_ENG;
     Task_Name=product_funnel_daily;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_PRODUCT_FUNNEL.product_funnel_daily
Owner: Analytics Engineering
Modified: 2022-10-17

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete
from    {product_funnel_t2_schema}.product_funnel_daily
where   event_date_pacific between {start_date} and {end_date}
;

insert into {product_funnel_t2_schema}.product_funnel_daily
SELECT  event_date_pacific
        , channelcountry
        , channel
        , platform
        , site_source
        , CAST(REGEXP_REPLACE(style_id,'(S|G_|U)','') as integer) style_id
        , sku_id
        , averagerating
        , review_count
        , order_quantity
        , order_demand
        , order_sessions
        , add_to_bag_quantity
        , add_to_bag_sessions
        , product_views
        , product_view_sessions
        , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from    (select DISTINCT * FROM {product_funnel_t2_schema}.product_funnel_daily_ldg) a
where   event_date_pacific between {start_date} and {end_date}
AND LENGTH(style_id) > 5
;

COLLECT STATISTICS COLUMN (CHANNELCOUNTRY, SKU_ID) ON {product_funnel_t2_schema}.product_funnel_daily;
COLLECT STATISTICS COLUMN (STYLE_ID) ON {product_funnel_t2_schema}.product_funnel_daily;
COLLECT STATISTICS COLUMN (EVENT_DATE_PACIFIC) ON {product_funnel_t2_schema}.product_funnel_daily;
COLLECT STATISTICS COLUMN (CHANNELCOUNTRY, STYLE_ID) ON {product_funnel_t2_schema}.product_funnel_daily;
COLLECT STATISTICS COLUMN (PARTITION) ON {product_funnel_t2_schema}.product_funnel_daily;
COLLECT STATISTICS COLUMN (PARTITION, STYLE_ID) ON {product_funnel_t2_schema}.product_funnel_daily;

SET QUERY_BAND = NONE FOR SESSION;
