
create table if not exists {hive_schema}.product_funnel_daily 
(
    event_date_pacific date
    , channelcountry string
    , channel string
    , platform string
    , site_source string
    , style_id string
    , sku_id string
    , averagerating float
    , review_count integer
    , order_quantity integer
    , order_demand double
    , order_sessions integer
    , add_to_bag_quantity integer
    , add_to_bag_sessions integer
    , product_views double
    , product_view_sessions double
    , partition_date date
)
using ORC
location 's3://{s3_bucket_root_var}/product_funnel_daily/'
partitioned by (partition_date);

--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
msck repair table {hive_schema}.product_funnel_daily; 

--get pdps at the style level. The event only occurs at the style level so we have to try to
--attribute to sku later
create or replace temporary view pdp_style as
(
    select  pdp.event_date_pacific
            , pdp.channelcountry
            , CASE
                WHEN upper(pdp.channel) = 'HAUTELOOK' THEN 'RACK'
                else pdp.channel
            end as channel
            , pdp.platform
            , CASE
                when pdp.app_id in ('APP02646', 'APP02890') then 'LEGACY'
                else 'MODERN'
            end as site_source
            , pdp.style_id
            , max(pdp.averagerating) as averagerating
            , max(pdp.review_count) as review_count
            , count(pdp.event_id) as product_views
            , sum(0) as product_view_sessions
    from    acp_event_intermediate.funnel_events_product_viewed pdp
    where   coalesce(lower(pdp.identified_bot),'false') = 'false'
    and     not arrays_overlap(pdp.header_keys,array('Nord-Load','nord-load','nord-test','Nord-Test','sretest'))
    and     pdp.event_date_pacific between {start_date} and {end_date}
    and     length(pdp.style_id) <= 7 
    group by 1,2,3,4,5,6
);


create or replace temporary view acp_bag as
(
    select  bag.event_date_pacific
            , bag.channelcountry
            , CASE
                WHEN upper(bag.channel) = 'HAUTELOOK' THEN 'RACK'
                else bag.channel
            end as channel
            , bag.platform
            , CASE
                when bag.app_id in ('APP02646', 'APP02890') then 'LEGACY'
                else 'MODERN'
            end as site_source
            , bag.style_id
            , bag.sku_id
            , sum(bag.quantity) add_to_bag_quantity
            , sum(0) as add_to_bag_sessions
    from    acp_event_intermediate.funnel_events_add_to_bag bag
    where   event_date_pacific between {start_date} and {end_date}
    and     coalesce(lower(identified_bot),'false') = 'false'
    and     not arrays_overlap(header_keys,array('Nord-Load','nord-load','nord-test','Nord-Test','sretest'))
    and     (bag_type = 'DEFAULT' 
                and ((bag_action_type = 'UNDETERMINED' and source_feature = 'ShoppingBag') or bag_action_type  = 'ADD' )
                or channel <> 'FULL_LINE' )
    group by 1,2,3,4,5,6,7
);

create or replace temporary view acp_order as
(
    select  ord.event_date_pacific
            , ord.channelcountry
            , ord.channel
            , ord.platform
            , CASE
                when ord.app_id in ('APP02646', 'APP02890') then 'LEGACY'
                else 'MODERN'
            end as site_source
            , ord.style_id
            , ord.sku_id
            , count(ord.orderLineId) as order_quantity
            , sum(ord.current_price + ord.employee_discount) as order_demand
            , sum(0) as order_sessions
    from acp_event_intermediate.funnel_events_order_submitted ord
    left join
    (   select  orderLineId
        from    acp_event_intermediate.funnel_events_order_cancelled
        where   event_date_pacific between date_sub({start_date}, 15) and {end_date}
        and     cancel_reason like '%FRAUD%'
    ) c on ord.orderLineId = c.orderLineId
    where   ord.event_date_pacific between date_sub({start_date}, 15) and {end_date}
    and     coalesce(lower(ord.identified_bot),'false') = 'false'
    and     ord.platform <> 'POS' 
    and     not arrays_overlap(ord.header_keys,array('Nord-Load','nord-load','nord-test','Nord-Test','sretest'))
    and     c.orderLineId is null
    group by 1,2,3,4,5,6,7
);

--product views occur at the style level, but reporting occurs at the sku
--level. The standard method to attribute pv's to SKUs is to look at 15 days
--historical orders for SKUs across a style and use the proportion of SKU orders
--style orders to split out style product views by that proportion.
create or replace temporary view sku_distribution as
(
    select  style.event_date_pacific
            , style.channel
            , style.channelcountry
            , style.style_id
            , style.style_orders
            , sku.sku_id
            , sum(sku_orders) as sku_orders
            , sum(cast(sku_orders as float))/cast(style_orders as float) as product_view_fractional
    from 
     (  select  event_date_pacific
                , channel
                , channelcountry
                , style_id
                , sum(style_orders) over (
                    partition by channel, channelcountry, style_id
                    order by event_date_pacific asc rows between 15 preceding and current row
                ) as style_orders
        from
            (
            select   event_date_pacific
                    , channel
                    , channelcountry
                    , style_id
                    , sum(order_quantity) as style_orders
            from    acp_order
            group by 1,2,3,4
            )
    ) style
    left join 
    (   select  event_date_pacific
                , channel
                , channelcountry
                , style_id
                , sku_id
                , sum(order_quantity) sku_orders
        from acp_order
        group by 1,2,3,4,5
    ) sku on sku.event_date_pacific between date_sub(style.event_date_pacific,15) and style.event_date_pacific
    and     sku.channel = style.channel
    and     sku.channelcountry = style.channelcountry
    and     sku.style_id = style.style_id
    group by 1,2,3,4,5,6
);
--apply style product views to each sku in style, using 15 day order attribution
create or replace temporary view pdp_sku as (
    select  pdp.event_date_pacific
            , pdp.channelcountry
            , pdp.channel
            , pdp.platform
            , pdp.site_source
            , pdp.style_id
            , coalesce(map.sku_id, 'unknown') as sku_id
            , max(pdp.averagerating) as averagerating
            , max(coalesce(pdp.review_count, 0)) as review_count
            , max(pdp.product_views)*max(coalesce(map.product_view_fractional,1)) as product_views
            , max(pdp.product_view_sessions)*max(coalesce(map.product_view_fractional,1)) as product_view_sessions
    from    pdp_style pdp
    left join sku_distribution map on pdp.event_date_pacific = map.event_date_pacific
    and     pdp.channelcountry = map.channelcountry
    and     pdp.channel = map.channel
    and     pdp.style_id = map.style_id
    group by 1,2,3,4,5,6,7
);

--final union of metric data at SKU level
create or replace temp view product_funnel_output
as
select  acp.event_date_pacific
        , acp.channelcountry
        , acp.channel
        , acp.platform
        , acp.site_source
        , acp.style_id
        , acp.sku_id
        , max(acp.averagerating) as averagerating
        , max(acp.review_count) as review_count
        , sum(acp.order_quantity) as order_quantity
        , sum(cast(acp.order_demand as double)) as order_demand
        , sum(acp.order_sessions) as order_sessions
        , sum(acp.add_to_bag_quantity) as add_to_bag_quantity
        , sum(acp.add_to_bag_sessions) as add_to_bag_sessions
        , sum(acp.product_views) as product_views
        , sum(acp.product_view_sessions) as product_view_sessions
        , acp.event_date_pacific as partition_date
from
    (   select  event_date_pacific
                , channelcountry
                , channel
                , platform
                , site_source
                , style_id
                , sku_id
                , 0 averagerating
                , 0 review_count
                , order_quantity
                , order_demand
                , order_sessions
                , 0 add_to_bag_quantity
                , 0 add_to_bag_sessions
                , 0 product_views
                , 0 product_view_sessions
        from    acp_order
        where   event_date_pacific between {start_date} and {end_date}

        union all

        select  event_date_pacific
                , channelcountry
                , channel
                , platform
                , site_source
                , style_id
                , sku_id
                , 0 averagerating
                , 0 review_count
                , 0 order_quantity
                , 0 order_demand
                , 0 order_sessions
                , add_to_bag_quantity
                , add_to_bag_sessions
                , 0 product_views
                , 0 product_view_sessions
        from    acp_bag

        union all

        select  event_date_pacific
                , channelcountry
                , channel
                , platform
                , site_source
                , style_id
                , sku_id
                , averagerating
                , review_count
                , 0 order_quantity
                , 0 order_demand
                , 0 order_sessions
                , 0 add_to_bag_quantity
                , 0 add_to_bag_sessions
                , product_views
                , product_view_sessions
        from    pdp_sku
    ) acp
group by 1,2,3,4,5,6,7,17
;

-- Writing to hive table
insert overwrite table {hive_schema}.product_funnel_daily partition (partition_date)
select /*+ REPARTITION(100) */ * from product_funnel_output
;

-- writing to TD landing table
-- job was previously rounding precision for product_views automatically (i.e. 5.866 was getting truncated to 5.87 - TPT needs this explicitly done)
INSERT OVERWRITE TABLE product_funnel_daily_ldg_output 
select  event_date_pacific
        , channelcountry
        , channel
        , platform
        , site_source
        , style_id
        , sku_id
        , averagerating
        , review_count
        , order_quantity
        , order_demand
        , order_sessions
        , add_to_bag_quantity
        , add_to_bag_sessions
        , round(product_views, 2) as product_views
        , product_view_sessions
from    product_funnel_output
;
