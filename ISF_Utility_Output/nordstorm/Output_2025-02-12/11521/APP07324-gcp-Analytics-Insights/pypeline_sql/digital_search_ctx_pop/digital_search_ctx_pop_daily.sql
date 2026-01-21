-- For Contextual Score ETL Stage1 to Stage2 --
-- Stakeholders: Weiye Deng, Kunal Sonalkar
-- AE support: Grace Qiu, Jared Hesse
-- File creation date: 06/22/2022


--- Importing data from S3
CREATE OR REPLACE TEMPORARY VIEW customer_keyword_search_results_generated USING avro OPTIONS(path 's3://tf-nap-prod-event-landing/topics/customer-keyword-search-results-generated-avro/');
CREATE OR REPLACE TEMPORARY VIEW customer_activity_product_summary_selected USING orc OPTIONS(path 's3://tf-nap-prod-styling-presentation/event/customer-activity-productsummaryselected/' );
CREATE OR REPLACE TEMPORARY VIEW customer_activity_added_to_bag USING orc OPTIONS(path 's3://tf-nap-prod-styling-presentation/event/customer-activity-addedtobag-v1/');
CREATE OR REPLACE TEMPORARY VIEW customer_activity_order_submitted USING orc OPTIONS(path 's3://tf-nap-prod-styling-presentation/event/customer-activity-ordersubmitted-v1/');

----- Job creation -------
create or replace temp view ctx_pop_daily_sum
as
select
    base.event_date,
    base.channel,
    base.channelcountry as channel_country,
    base.search_phrase,
    base.product_style_id_type,
    base.product_style_id,
    sum(case when clicked_client.product_style_id is not null then 1 else 0 end) as click_count,
    sum(case when atb_client.product_style_id is not null then 1 else 0 end) as atb_count,
    sum(case when order_client.product_style_id is not null then 1 else 0 end) as order_count
from
(
select distinct
    cast(value.eventTime as date) as event_date,
    replace(replace(replace(replace(lower(trim(value.phrase)),' ','@>'),'>@',''),'@>',' '), '"', '') as search_phrase,
    value.contextId as context_id,
    value.source.channelcountry as channelcountry,
    value.source.channel as channel,
    value.customer.idType as customer_id_type,
    value.customer.id as customer_id,
    explode_outer(value.searchResults) as results,
    results.productstyle.idtype as product_style_id_type,
    results.productstyle.id as product_style_id
from customer_keyword_search_results_generated
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
) as base
left join 
(
select distinct
    cast(eventtime as date) as event_date,
    context.id as context_id,
    source.channelcountry,
    source.channel,
    customer.idtype as customer_id_type,
    customer.id as customer_id,
    productsummary.productstyle.idtype as product_style_id_type,
    productsummary.productstyle.id as product_style_id
from customer_activity_product_summary_selected
where 1=1
and to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
and productsummary.element.id in ('ProductResults/ProductGallery/ProductSummary')
) as clicked_client
on base.event_date = clicked_client.event_date
and base.context_id = clicked_client.context_id
and base.channel = clicked_client.channel
and base.channelcountry = clicked_client.channelcountry
and base.customer_id_type = clicked_client.customer_id_type
and base.customer_id = clicked_client.customer_id
and base.product_style_id_type = clicked_client.product_style_id_type
and base.product_style_id = clicked_client.product_style_id
left join 
(
select distinct
    cast(cast(year as string)||'-'||cast(month as string)||'-'||cast(day as string) as date) as event_date,
    source.channelcountry,
    source.channel,
    customer.idtype as customer_id_type,
    customer.id as customer_id,
    productstyle.idtype as product_style_id_type,
    productstyle.id as product_style_id
from customer_activity_added_to_bag
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
) as atb_client
on base.event_date = atb_client.event_date
and base.channel = atb_client.channel
and base.channelcountry = atb_client.channelcountry
and base.product_style_id_type = atb_client.product_style_id_type
and base.product_style_id = atb_client.product_style_id
and base.customer_id_type = atb_client.customer_id_type
and base.customer_id = atb_client.customer_id
left join
(
select distinct
    event_date,
    channelcountry,
    channel,
    customer_id_type,
    customer_id,
    product_style_id_type,
    product_style_id
from
(
select
    cast(cast(year as string)||'-'||cast(month as string)||'-'||cast(day as string) as date) as event_date,
    source.channelcountry,
    source.channel,
    'SHOPPER_ID' as customer_id_type,
    credentials.shopperid as customer_id,
    explode_outer(items) as items_arr,
    items_arr.productstyle.idtype as product_style_id_type,
    items_arr.productstyle.id as product_style_id
from customer_activity_order_submitted
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
)
) as order_client
on atb_client.event_date = order_client.event_date
and atb_client.channel = order_client.channel
and atb_client.channelcountry = order_client.channelcountry
and atb_client.product_style_id_type = order_client.product_style_id_type
and atb_client.product_style_id = order_client.product_style_id
and atb_client.customer_id_type = order_client.customer_id_type
and atb_client.customer_id = order_client.customer_id
group by 1, 2, 3, 4, 5, 6
having click_count > 0
;

------- Job log creation --------
create or replace temp view ctx_pop_daily_sum_log
as
select
    channel,
    channel_country,
    count(*) row_count,
    count(distinct product_style_id) as unique_style_ids,
    count(distinct search_phrase) as unique_searches,
    percentile_approx(click_count, 0.5) as median_click,
    max(click_count) as max_click,
    percentile_approx(atb_count, 0.5) as median_atb,
    max(atb_count) as max_atb,
    percentile_approx(order_count, 0.5) as median_order,
    max(order_count) as max_order,
    current_timestamp() as etl_datetime,
    event_date
from ctx_pop_daily_sum
group by channel, channel_country, event_date
;

--- Send output to S3 ----
insert into table ctx_pop_sum_daily partition (event_date)
select * from ctx_pop_daily_sum;
--- Send output log to S3 ----
insert into table ctx_pop_sum_daily_log partition (event_date)
select * from ctx_pop_daily_sum_log;
--- Send output log to Hive -----
create table if not exists {hive_schema}.ctx_pop_daily_sum_log
(
	 channel              string
	,channel_country      string
	,row_count            string
	,unique_style_ids     string
	,unique_searches      string
	,median_click         string
	,max_click            string
	,median_atb           string
	,max_atb              string
	,median_order         string
	,max_order            string
	,etl_datetime         string
	,event_date           date
)
using orc
location 's3://{hive_schema}/Isf_output/contextual-popularity/ctx_pop_sum_daily_log_hive/'
partitioned by (event_date);
----
insert into table {hive_schema}.ctx_pop_daily_sum_log partition (event_date)
select * from ctx_pop_daily_sum_log;

