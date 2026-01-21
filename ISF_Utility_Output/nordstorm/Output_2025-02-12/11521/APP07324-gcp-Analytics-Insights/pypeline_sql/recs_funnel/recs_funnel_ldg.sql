SET QUERY_BAND = 'App_ID=APP08142;
     DAG_ID=recs_funnel_11521_ACE_ENG;
     Task_Name=recs_funnel_ldg;'
     FOR SESSION VOLATILE;

--output to landing table

create or replace temp view add_to_bag 
as
select * from acp_event_view.customer_activity_added_to_bag
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
;


create or replace temp view online_order
as
select * from acp_event_view.customer_activity_order_submitted
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
;


--product_summary_selected
CREATE OR REPLACE TEMPORARY VIEW recs_product_summary_selected_view AS
select
    country,
    channel,
    platform,
    user_id,
    user_id_type,
    feature,
    cast(substr(cast(from_utc_timestamp(event_time_stamp, 'US/Pacific') as string), 1, 10) as date) as pst_date,
    context_page_type,
    recs_detail,
    split(recs_detail,'/')[0] as placement,
    split(recs_detail,'/')[1] as strategy,
    product_style_id,
    event_time_stamp,
    from_utc_timestamp(event_time_stamp, 'US/Pacific') as pst_timestamp
FROM
(
SELECT
    cast(source.channelCountry as string) as country,
    cast(source.channel as string) as channel,
    cast(source.platform as string) as platform,
    CAST(customer.id AS STRING) AS user_id,
    CAST(customer.idType AS STRING) AS user_id_type,
    CAST(FROM_UNIXTIME(SUBSTR(CAST(headers.EventTime AS STRING), 1, 10)) AS DATE) AS event_date,
    to_timestamp(from_unixtime(substr(cast(headers.EventTime as string), 1, 10))) as event_time_stamp,
    cast(source.feature as string) as feature,
    CAST(productSummary.productStyle.idType AS STRING) AS product_style_id_type,
    CAST(productSummary.productStyle.id AS STRING) AS product_style_id,
    CAST(context.pageType AS STRING) AS context_page_type,
    CAST((CASE WHEN  from_utc_timestamp((CAST(FROM_UNIXTIME(SUBSTR(CAST(headers.EventTime AS STRING), 1, 10)) AS DATE)), 'US/Pacific') >= '2021-09-24' THEN coalesce(productSummary.element.subid, productSummary.element.id) ELSE productSummary.element.id END) AS STRING) AS recs_detail
 FROM acp_event_view.customer_activity_product_summary_selected
 WHERE source.feature = 'RECOMMENDATIONS'
  and productSummary.productStyle.id is not null
  and productSummary.productStyle.idType = 'WEB'
  and headers['Sretest'] is null
  and headers['sretest'] is null
  and headers['Nord-Load'] is null
  and headers['nord-load'] is null
  and headers['nord-test'] is null
  and headers['Nord-Test'] is null
  and to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
);



CREATE OR REPLACE TEMPORARY view add_to_bag_view AS
SELECT
country,
channel,
platform,
user_id,
user_id_type,
credentials_shopper_id,
feature,
cast(substr(cast(from_utc_timestamp(event_time_stamp, 'US/Pacific') as string), 1, 10) as date) as pst_date,
product_style_id,
event_time_stamp,
from_utc_timestamp(event_time_stamp, 'US/Pacific') as pst_timestamp
FROM
(
   SELECT
        cast(source.channelCountry as string) as country,
        cast(source.channel as string) as channel,
        cast(source.platform as string) as platform,
        CAST(customer.id AS STRING) AS user_id,
        cast(bagType as string) as bag_type,
        CAST(customer.idType AS STRING) AS user_id_type,
        cast(source.feature as string) as feature,
        cast(credentials.shopperId as string) as credentials_shopper_id,
        CAST(FROM_UNIXTIME(SUBSTR(CAST(headers.EventTime AS STRING), 1, 10)) AS DATE) AS event_date,
        to_timestamp(from_unixtime(substr(cast(headers.EventTime as string), 1, 10))) as event_time_stamp,
        CAST(productStyle.id AS STRING) AS product_style_id

    FROM add_to_bag
   where
        productStyle.id is not null
     and
     ( 
        cast(bagType as string) = 'DEFAULT' 
        and ((cast(source.feature as string) = 'ShoppingBag' and cast(bagActionType as string) = 'UNDETERMINED') 
            OR cast(bagActionType as string) = 'ADD') 
        and cast(source.channel as string) in ('FULL_LINE', 'RACK','HAUTELOOK')
        )
     and headers['Sretest'] is null
     and headers['sretest'] is null
     and headers['Nord-Load'] is null
     and headers['nord-load'] is null
     and headers['nord-test'] is null
     and headers['Nord-Test'] is null
);


create or replace temporary view acp_order as
(
    select
      ord.channelcountry,
      ord.channel,
      ord.platform,
      ord.shopper_id,
      ord.event_date_pacific,
      ord.event_timestamp_utc,
      from_utc_timestamp(ord.event_timestamp_utc, 'US/Pacific') as pst_timestamp,
      ord.order_number,
      ord.style_id,
      ord.orderLineId,
      ord.current_price,
      ord.employee_discount
    from acp_event_intermediate.funnel_events_order_submitted ord
    left join
    (
        select
            orderLineId
        from acp_event_intermediate.funnel_events_order_cancelled
        where event_date_pacific BETWEEN date_sub({start_date},1) AND date({end_date})
        and cancel_reason like '%FRAUD%'
    ) c
    on ord.orderLineId = c.orderLineId
    where ord.event_date_pacific BETWEEN date_sub({start_date},1) AND date({end_date})
    and coalesce(lower(ord.identified_bot),'false') = 'false'
    and not arrays_overlap(ord.header_keys,array('Nord-Load','nord-load','nord-test','Nord-Test','sretest'))
    and c.orderLineId is null
);




insert into table recs_funnel_output
    SELECT
        recs_product_summary_selected_view.country,
        recs_product_summary_selected_view.channel,
        recs_product_summary_selected_view.platform,
        recs_product_summary_selected_view.context_page_type,
        recs_product_summary_selected_view.placement,
        recs_product_summary_selected_view.strategy,
        recs_product_summary_selected_view.user_id_type,
        CAST((case when recs_product_summary_selected_view.country = 'US' THEN 'USD' ELSE 'CAD' END) AS STRING) AS currencycode,
        substr(cast(from_utc_timestamp(current_timestamp(), 'US/Pacific') as string), 1, 19) as etl_timestamp,
        recs_product_summary_selected_view.pst_date,
        CAST(COUNT(DISTINCT(recs_product_summary_selected_view.user_id)) AS string) AS recs_clicked_users,
        CAST(COUNT(DISTINCT(add_to_bag_view.user_id)) AS string) AS adding_users,
        CAST(COUNT(DISTINCT(acp_order.shopper_id)) AS string) AS ordering_users,
        CAST(COUNT(recs_product_summary_selected_view.product_style_id) as string) as product_summary_selected_views,
        CAST(COUNT(add_to_bag_view.product_style_id) as string) as add_to_bag_items,
        CAST(COUNT(acp_order.style_id) as string) order_items,
        CAST(COUNT(distinct acp_order.order_number) as string) orders,
        CAST(SUM(current_price + employee_discount) as string) AS demand
    FROM recs_product_summary_selected_view
    LEFT JOIN add_to_bag_view
    ON recs_product_summary_selected_view.pst_date = add_to_bag_view.pst_date
    AND recs_product_summary_selected_view.user_id_type = add_to_bag_view.user_id_type
    AND recs_product_summary_selected_view.user_id = add_to_bag_view.user_id
    AND recs_product_summary_selected_view.country = add_to_bag_view.country
    and recs_product_summary_selected_view.channel = add_to_bag_view.channel
    and recs_product_summary_selected_view.platform = add_to_bag_view.platform
    and recs_product_summary_selected_view.product_style_id = add_to_bag_view.product_style_id
    and recs_product_summary_selected_view.pst_timestamp < add_to_bag_view.pst_timestamp
    LEFT JOIN acp_order
    ON recs_product_summary_selected_view.user_id = acp_order.shopper_id
    AND recs_product_summary_selected_view.pst_date = acp_order.event_date_pacific
    AND recs_product_summary_selected_view.country = acp_order.channelcountry
    and recs_product_summary_selected_view.channel = acp_order.channel
    and recs_product_summary_selected_view.platform = acp_order.platform
    and recs_product_summary_selected_view.product_style_id = acp_order.style_id
    and add_to_bag_view.pst_timestamp < acp_order.pst_timestamp
    GROUP BY 1,2,3,4,5,6,7,8,9,10
;


