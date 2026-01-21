create table if not exists {hive_schema}.search_session_performance (
	channel string, 
	experience string,
    query_bucket string,
    search_sessions bigint,
    search_ep_sessions bigint,
    search_ep_bounced_sessions bigint,
    search_ep_nb_sessions bigint,
    query_count bigint,
    context_count bigint,
    sessions_clicked bigint,
    sessions_atb bigint,
    session_orders bigint,
    hm_mrr decimal(38,15),
    order_value decimal(38,15),
    sessions_filtered bigint,
    session_count bigint,
    activity_week date
)
using PARQUET
location "s3://{s3_bucket_root_var}/search_session_performance"
partitioned by (activity_week); 

MSCK REPAIR table {hive_schema}.search_session_performance;

create or replace temporary view p1 as 
select 
session_id,event_time_utc,pageinstance_id,search_parent_id,search_parent_rank,activity_date_partition,result_query_rank 
,min(event_time_utc) over (partition by session_id) as session_start_time 
,max(event_time_utc) over (partition by session_id) as session_end_time
,min_by(pageinstance_id,result_query_rank) over(partition by session_id) as session_start_page 
,max_by(pageinstance_id,result_query_rank) over(partition by session_id) as session_end_page
,min_by(search_parent_id,search_parent_rank)over(partition by session_id) as session_landing_query
,max_by(search_parent_id,search_parent_rank)over(partition by session_id) as session_exit_query
from ace_etl.search_session_analytical_v2
where activity_date_partition between {start_date} and {end_date}
group by 1,2,3,4,5,6,7
;

create or replace temporary view p2 as
select 
    a.*
    ,b.session_start_time
    ,b.session_end_time
    ,b.session_start_page
    ,b.session_end_page
    ,b.session_landing_query
    ,b.session_exit_query
from 
    (select * 
    from ace_etl.search_session_analytical_v2
    where activity_date_partition between {start_date} and {end_date}
    )a
left join  
    (select 
    session_id
    ,event_time_utc
    ,session_start_time
    ,session_end_time
    ,session_start_page
    ,session_end_page
    ,session_landing_query
    ,session_exit_query
    from p1
    where activity_date_partition between {start_date} and {end_date}
    )b
on a.session_id = b.session_id
and a.event_time_utc = b.event_time_utc
;

create or replace temporary view p3 as
select
activity_date_partition
,activity_week_start as activity_week 
,session_id
,search_parent_id
,cust_id
,search_value
,matchstrategy
,channel
,experience as platform
,context_id
,max(case when pageinstance_id = session_start_page then 1 else 0 end ) as session_lp_query_flag 
,max(case when pageinstance_id = session_end_page then 1 else 0 end ) as session_exit_query_flag
,max(case when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' then 1 else 0 end) as clicked
,count(distinct case when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' then productstyle_id  else null end) as products_selected
,max(case when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' then  atb_ct else null end) as products_add_to_bag
,max(case when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' then  order_ct else null end) as products_ordered
,count(distinct case when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' and order_ct >= 1 then search_parent_id else null end) as ordered_query
,sum(distinct case when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' then  order_units else null end) as order_demand
,round(1/(min(case when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' then ((cast(element_index as int))+1) else null end)*1.00),2) as first_click_position
,max(case when (search_filters_key != 'DesignerSale') and (search_filters_key <> '') and (search_filters_key <> 'NULL') then 1 else 0 end) as filter_flag
from p2
where activity_date_partition between {start_date} and {end_date}
group by 1,2,3,4,5,6,7,8,9,10
;

create or replace temporary view p4 as
select
channel
,activity_week 
,session_id
,platform as experience
,count(distinct search_parent_id) as total_search_query_volume
,max(case when session_exit_query_flag = 1 and clicked = 0 then 1 else 0 end) as search_dep
,max(case when session_exit_query_flag = 1 and clicked = 0 and session_lp_query_flag = 0 then 1 else 0 end) as search_dep_excl_lp
,count(distinct case when products_selected >=1 then session_id end) as clicked_sessions
,count(distinct case when products_add_to_bag >=1 then session_id end) as atb_sessions
,count(distinct case when products_ordered >=1  then session_id end) as ordered_sessions
,max(clicked) as clicked
,sum(order_demand) as demand
,round(1/nullif(sum(nullif(first_click_position,0))/(sum(nullif(clicked,0))*1.000),0),3) as hm_mrr
,count(distinct context_id) as context_count
,count(distinct case when filter_flag = 1 then session_id end) as filtered_sessions
from p3
group by 1,2,3,4
;

create or replace temporary view p5 as
select 
activity_week 
,channel
,experience
,case when context_count = 1 then '1'
when context_count = 2 then '2'
when context_count = 3 then '3'
else '4+' end as query_bucket
,count(session_id) as search_sessions
,count(case when search_dep = 1 then session_id end) as search_ep_sessions
,count(case when search_dep = 1 and search_dep_excl_lp = 0 then session_id end) as search_ep_bounced_sessions
,count(case when search_dep_excl_lp = 1 then session_id end) as search_ep_nb_sessions
,sum(total_search_query_volume) as query_count
,sum(context_count) as context_count
,sum(clicked_sessions) as sessions_clicked
,sum(atb_sessions) as sessions_atb
,sum(ordered_sessions) as session_orders
,sum(hm_mrr) as hm_mrr
,sum(demand) as order_value
,sum(filtered_sessions) as sessions_filtered
from p4
group by 1,2,3,4
;

create or replace temporary view total_sessions_denominator as
select 
((date_trunc('week',(date(event_time_pst)) + interval '1' day)) - interval '1' day) as activity_week
,channel
,experience
,count(distinct session_id) as session_count
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where activity_date_partition between {start_date} and {end_date}
and channelcountry = 'US'
group by 1,2,3
;

create or replace temporary view output as
select 
a.channel 
,a.experience
,a.query_bucket string
,a.search_sessions
,a.search_ep_sessions
,a.search_ep_bounced_sessions
,a.search_ep_nb_sessions
,a.query_count
,a.context_count
,a.sessions_clicked
,a.sessions_atb
,a.session_orders
,a.hm_mrr
,a.order_value
,a.sessions_filtered
,b.session_count
,a.activity_week
from p5 a
left join total_sessions_denominator b
on a.activity_week = b.activity_week
and a.channel = b.channel
and a.experience = b.experience
;

insert
    OVERWRITE TABLE {hive_schema}.search_session_performance PARTITION (activity_week)
select
    *
from
    output;


    