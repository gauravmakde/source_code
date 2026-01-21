SET QUERY_BAND = 'App_ID=APP08742;
     DAG_ID=speed_session_fact_daily_11521_ACE_ENG;
     Task_Name=speed_session_fact_daily_load;'
     FOR SESSION VOLATILE;

-- Definine new Hive table for output  
create table if not exists {hive_schema}.speed_session_fact_daily 
(
 activity_date_pacific date,
   channel VARCHAR(32),
   experience VARCHAR(32),
   channelcountry VARCHAR(8),
   landing_page VARCHAR(100),
   exit_page VARCHAR(100),
   mrkt_type varchar(50),
   finance_rollup varchar(50),
   finance_detail varchar(50),
   bounce_ind integer,
   acp_id_populated integer,
   loyalty_id_populated integer,
   total_pages_visited bigint,
   total_unique_pages_visited bigint,
   session_count bigint,
   session_duration_seconds decimal(38, 3),
   hp_sessions bigint,
   hp_engaged_sessions bigint,
   hp_views bigint,
   pdp_sessions bigint,
   pdp_engaged_sessions bigint,
   pdp_views bigint,
   browse_sessions bigint,
   browse_engaged_sessions bigint,
   browse_views bigint,
   search_sessions bigint,
   search_engaged_sessions bigint,
   search_views bigint,
   sbn_sessions bigint,
   sbn_engaged_sessions bigint,
   sbn_views bigint,
   wishlist_sessions bigint,
   wishlist_engaged_sessions bigint,
   wishlist_views bigint,
   checkout_sessions bigint,
   checkout_engaged_sessions bigint,
   checkout_views bigint,
   shopb_sessions bigint,
   shopb_engaged_sessions bigint,
   shopb_views bigint,
   atb_sessions bigint,
   atb_views bigint,
   atb_events bigint,
   ordered_sessions bigint,
   sales_qty bigint,
   web_orders bigint,
   web_demand_usd decimal(38, 6),
   dw_batch_date date not null,
   dw_sys_load_tmstp  TIMESTAMP NOT NULL
    )using ORC
location 's3://{s3_bucket_root_var}/speed_session_fact_daily/'
partitioned by (activity_date_pacific);

-- Reading data from upstream Hive table and aggregating across dimensions
create or replace temporary view speed_session_daily as
(
SELECT 
channel
,experience
,channelcountry
,substring(entry_context_pagetype, 1, 100)  landing_page
,substring(exit_context_pagetype, 1, 100) exit_page
,first_arrival_mrkt_type AS mrkt_type
,first_arrival_finance_rollup AS finance_rollup
,first_arrival_finance_detail AS finance_Detail
,bounced AS bounce_ind
,acp_id_populated
,loyalty_id_populated
,sum(page_ct) AS total_pages_visited
,sum(pageinstance_ct) AS total_unique_pages_visited
,count(DISTINCT session_id) AS session_count
,sum( unix_timestamp(session_max_time_pst )- unix_timestamp(session_min_time_pst)) session_duration_seconds
,count(DISTINCT CASE WHEN home_pageinstance_ct >0 THEN session_id end) hp_sessions
,count(DISTINCT CASE WHEN home_pageinstance_ct >0 AND click_occurred=1 THEN session_id end) hp_engaged_sessions
,sum(home_pageinstance_ct) AS hp_views
,count(DISTINCT CASE WHEN  pdp_pageinstance_ct>0 THEN session_id end) pdp_sessions
,count(DISTINCT CASE WHEN  pdp_pageinstance_ct>0 AND click_occurred=1 THEN session_id end) pdp_engaged_sessions
,sum(pdp_pageinstance_ct) AS pdp_views
,count(DISTINCT CASE WHEN  browse_pageinstance_ct>0 THEN session_id end) browse_sessions
,count(DISTINCT CASE WHEN  browse_pageinstance_ct>0 AND click_occurred=1 THEN session_id end) browse_engaged_sessions
,sum(browse_pageinstance_ct) AS browse_views
,count(DISTINCT CASE WHEN  search_pageinstance_ct>0 THEN session_id end) search_sessions
,count(DISTINCT CASE WHEN  search_pageinstance_ct>0 AND click_occurred=1  THEN session_id end) search_engaged_sessions
,sum(search_pageinstance_ct) AS search_views
,count(DISTINCT CASE WHEN  search_pageinstance_ct>0 AND browse_pageinstance_ct>0 THEN session_id end) sbn_sessions
,count(DISTINCT CASE WHEN  search_pageinstance_ct>0 AND browse_pageinstance_ct>0  AND click_occurred=1  THEN session_id end) sbn_engaged_sessions
,sum(search_pageinstance_ct+browse_pageinstance_ct)sbn_views
,count(DISTINCT CASE WHEN  wishlist_pageinstance_ct>0  THEN session_id end) wishlist_sessions
,count(DISTINCT CASE WHEN  wishlist_pageinstance_ct>0 AND click_occurred=1 THEN session_id end) wishlist_engaged_sessions
,sum(wishlist_pageinstance_ct) AS wishlist_views
,count(DISTINCT CASE WHEN  checkout_pageinstance_ct>0 THEN session_id end) checkout_sessions
,count(DISTINCT CASE WHEN  checkout_pageinstance_ct>0 AND click_occurred=1 THEN session_id end) checkout_engaged_sessions
,sum(checkout_pageinstance_ct) AS checkout_views
,count(DISTINCT CASE WHEN  shopping_bag_pageinstance_ct>0 THEN session_id end) shopb_sessions
,count(DISTINCT CASE WHEN  shopping_bag_pageinstance_ct>0 AND click_occurred=1 THEN session_id end) shopb_engaged_sessions
,sum(shopping_bag_pageinstance_ct) AS shopb_views
,count(DISTINCT CASE WHEN  atb_onpage_ct>0 THEN session_id end) atb_sessions
,sum(atb_onpage_ct) AS atb_views
,sum(atb_event_ct) AS atb_events
,count(DISTINCT CASE WHEN  order_onpage_ct>0 THEN session_id end) ordered_sessions
,sum(order_item_qty) sales_qty
,sum(order_event_ct) web_orders
,sum(order_current_price) web_demand_usd
, CURRENT_DATE() as dw_batch_date
, CURRENT_TIMESTAMP() as dw_sys_load_tmstp
, activity_date_partition as activity_date_pacific
from acp_event_intermediate.session_fact_attributes_parquet
where activity_date_partition BETWEEN {start_date} and {end_date}
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,47,48,49
);
-- Writing output to new Hive table
insert overwrite table {hive_schema}.speed_session_fact_daily partition (activity_date_pacific)
select /*+ REPARTITION(100) */ * 
from speed_session_daily
;
--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
msck repair table {hive_schema}.speed_session_fact_daily;
-- Writing output to teradata landing table.  
insert into table speed_session_fact_daily_ldg_output
select * from {hive_schema}.speed_session_fact_daily
where activity_date_pacific BETWEEN {start_date} and {end_date}
;