SET QUERY_BAND = 'App_ID=APP08742;
     DAG_ID=usl_digital_funnel_11521_ACE_ENG; 
     Task_Name=usl_digital_funnel;'
     FOR SESSION VOLATILE;




-- Definine new Hive table for output  
create table if not exists ace_etl.usl_digital_funnel
(
 channel  varchar(32)
,experience varchar(32)
,channelcountry varchar(32)
,acp_id   varchar(100)
,inception_session_id varchar(100)
,new_recognized varchar(100)
,session_start TIMESTAMP
,landing_page varchar(100)
,exit_page varchar(100)
,mrkt_type varchar(100)
,finance_rollup varchar(100)
,finance_detail varchar(100)
,session_count bigint
,acp_count bigint
,unique_cust_count bigint
,bounced_sessions bigint
,total_pages_visited     bigint
,total_unique_pages_visited bigint
--,hp_sessions bigint
,hp_engaged_sessions     bigint
,hp_views bigint
,pdp_sessions bigint
,pdp_engaged_sessions bigint
,pdp_views bigint
,browse_sessions    bigint
,browse_engaged_sessions bigint
,browse_views bigint
,search_sessions    bigint
,search_engaged_sessions bigint
,search_views bigint
,sbn_sessions bigint
,sbn_engaged_sessions bigint
,sbn_views bigint
,wishlist_sessions bigint
,wishlist_engaged_sessions bigint
,wishlist_views bigint
,checkout_sessions bigint
,checkout_engaged_sessions bigint
,checkout_views bigint
,shopb_sessions bigint
,shopb_engaged_sessions bigint
,shopb_views   bigint
,atb_sessions bigint
,atb_views bigint
,atb_events bigint
,ordered_sessions bigint
,sales_qty bigint
,web_orders bigint
,web_demand_usd     decimal(38,6)
,removed_from_sb bigint
,atw bigint
,remove_from_wl bigint
,atb_from_wl bigint
,filters_used bigint
,reviews_eng bigint
,content_eng bigint
,reccommendations bigint
,saved_for_later bigint
,sfl_to_atb bigint
,sfl_removed bigint
,dw_batch_date date not null
,dw_sys_load_tmstp  TIMESTAMP NOT NULL
,activity_date_partition date
)using PARQUET 
location 's3://ace-etl/usl_digital_funnel/'
partitioned by (activity_date_partition);


-- Reading data from upstream Hive table and aggregating across dimensions
create 
or replace temp view base as 
select 
      activity_date_partition 
     ,session_id 
     ,event_id 
     ,event_name 
     ,element_id
from acp_event_intermediate.session_evt_expanded_attributes_parquet 
where event_name = 'com.nordstrom.event.customer.Engaged' 
and channelcountry = 'US'
and (element_id like '%%ShoppingBag/Remove%%'
  or element_id like '%%ProductDetail/AddToWishlist%%'
  or element_id like '%%Wishlist/Item/Remove%%'
  or element_id like '%%Wishlist/Item/AddToBag%%'
  or element_id like '%%ProductResults/Filters%%'
  or element_id like '%%ProductDetail/Reviews%%'
  or element_id like '%%Content%%'
  or element_id like '%%Recommendations%%'
  or element_id like '%%ShoppingBag/Saveforlater%%'
  or element_id like '%%ShoppingBag/SavedforLater/MoveToBag%%' 
  or element_id like '%%ShoppingBag/SavedForLater/Remove%%')
and activity_date_partition BETWEEN date'2024-01-07' and date'2024-01-15' ;


create 
or replace temp view actions as 
select 
      activity_date_partition
     ,session_id
     ,event_name 
     ,count(distinct case when element_id like '%%ShoppingBag/Remove%%' then event_id end) as removed_from_sb
     ,count(distinct case when element_id like '%%ProductDetail/AddToWishlist%%' then event_id end) as atw 
     ,count(distinct case when element_id like '%%Wishlist/Item/Remove%%' then event_id end) as remove_from_wl
     ,count(distinct case when element_id like '%%Wishlist/Item/AddToBag%%' then event_id end) as atb_from_wl
     ,count(distinct case when element_id like '%%ProductResults/Filters%%' then event_id end) as filters_used
     ,count(distinct case when element_id like '%%ProductDetail/Reviews%%' then event_id end) as reviews_eng
     ,count(distinct case when element_id like '%%Content%%' then event_id end) as content_eng
     ,count(distinct case when element_id like '%%Recommendations%%' then event_id end) as reccommendations
     ,count(distinct case when element_id like '%%ShoppingBag/Saveforlater%%' then event_id end) as saved_for_later
     ,count(distinct case when element_id like '%%ShoppingBag/SavedforLater/MoveToBag%%' then event_id end) as sfl_to_atb
     ,count(distinct case when element_id like '%%ShoppingBag/SavedForLater/Remove%%' then event_id end) as sfl_removed
from base      
group by 1,2,3
;

create 
or replace temp view pages as
select 
	 distinct 
	 session_id
	,activity_date_partition
	,context_pagetype_l2
	,click_occurred
from acp_event_intermediate.session_page_attributes_parquet
where context_pagetype_l2 in ('HOME', 'PRODUCT_DETAIL', 'BROWSE', 'SEARCH', 'WISH_LIST', 'CHECKOUT', 'SHOPPING_BAG', 'PDP') 
and click_occurred = 1 
and activity_date_partition BETWEEN date'2024-01-07' and date'2024-01-15' ;


create 
or replace temp view engaged as
select 
	 session_id 
	,activity_date_partition 
	,COUNT(DISTINCT case when context_pagetype_l2 = 'HOME' then session_id END) as hp_engaged_sessions
	,COUNT(DISTINCT case when context_pagetype_l2  = 'PDP' then session_id  END) as pdp_engaged_sessions
	,count(DISTINCT CASE WHEN context_pagetype_l2 = 'BROWSE' then session_id  END ) AS browse_engaged_sessions
	,count(DISTINCT CASE WHEN context_pagetype_l2 = 'SEARCH' then session_id  END ) AS search_engaged_sessions
	,count(DISTINCT CASE WHEN context_pagetype_l2 in ('SEARCH','BROWSE') then session_id  END ) AS sbn_engaged_sessions
	,count(distinct case when context_pagetype_l2 = 'WISH_LIST' then session_id  end) as wishlist_engaged_sessions
	,COUNT(DISTINCT case when context_pagetype_l2 = 'CHECKOUT'  then session_id  END) as checkout_engaged_sessions
	,COUNT(DISTINCT case when context_pagetype_l2 =  'SHOPPING_BAG' then session_id  END) as shopb_engaged_sessions
from pages
group by 1,2 ;


create 
or replace temp view usl_digital_funnel as

SELECT 
 f.channel
,f.experience
,f.channelcountry
,u.unique_acp_id_max as acp_id
,u.inception_session_id
,min_by(new_recognized,f.session_min_time_pst) as new_recognized
,min(f.session_min_time_pst) as session_start
,min_by(f.entry_context_pagetype,f.session_min_time_pst) as landing_page
,min_by(f.exit_context_pagetype,f.session_min_time_pst) as exit_page
,min_by(f.first_arrival_mrkt_type,f.session_min_time_pst) AS mrkt_type
,min_by(f.first_arrival_finance_rollup,f.session_min_time_pst) AS finance_rollup
,min_by(f.first_arrival_finance_detail,f.session_min_time_pst) AS finance_Detail
,count(DISTINCT f.session_id) AS session_count
,count(distinct u.unique_acp_id_max) as acp_count
,count(distinct inception_session_id) as unique_cust_count
,count(distinct case when f.bounced  = 1 then f.session_id end) AS bounced_sessions
,sum(page_ct) AS total_pages_visited
,sum(pageinstance_ct) AS total_unique_pages_visited
--,count(DISTINCT CASE WHEN home_pageinstance_ct >0 THEN f.session_id end) hp_sessions
,sum(hp_engaged_sessions) as hp_engaged_sessions
,sum(home_pageinstance_ct) AS hp_views
,count(DISTINCT CASE WHEN  pdp_pageinstance_ct>0 THEN f.session_id end) pdp_sessions
,sum(pdp_engaged_sessions) as pdp_engaged_sessions
,sum(pdp_pageinstance_ct) AS pdp_views
,count(DISTINCT CASE WHEN  browse_pageinstance_ct>0 THEN f.session_id end) browse_sessions
,sum(browse_engaged_sessions) as browse_engaged_sessions
,sum(browse_pageinstance_ct) AS browse_views
,count(DISTINCT CASE WHEN  search_pageinstance_ct>0 THEN f.session_id end) search_sessions
,sum(search_engaged_sessions) as search_engaged_sessions 
,sum(search_pageinstance_ct) AS search_views
,count(DISTINCT CASE WHEN  search_pageinstance_ct>0 AND browse_pageinstance_ct>0 THEN f.session_id end) sbn_sessions
,sum(sbn_engaged_sessions) as sbn_engaged_sessions
,sum(search_pageinstance_ct+browse_pageinstance_ct) sbn_views
,count(DISTINCT CASE WHEN  wishlist_pageinstance_ct>0  THEN f.session_id end) wishlist_sessions
,sum(wishlist_engaged_sessions) as wishlist_engaged_sessions
,sum(wishlist_pageinstance_ct) AS wishlist_views
,count(DISTINCT CASE WHEN  checkout_pageinstance_ct>0 THEN f.session_id end) checkout_sessions
,sum(checkout_engaged_sessions) as checkout_engaged_sessions
,sum(checkout_pageinstance_ct) AS checkout_views
,count(DISTINCT CASE WHEN  shopping_bag_pageinstance_ct>0 THEN f.session_id end) shopb_sessions
,sum(shopb_engaged_sessions) as shopb_engaged_sessions
,sum(shopping_bag_pageinstance_ct) AS shopb_views
,count(DISTINCT CASE WHEN  atb_onpage_ct>0 THEN f.session_id end) atb_sessions
,sum(atb_onpage_ct) AS atb_views
,sum(f.atb_event_ct) AS atb_events
,count(DISTINCT CASE WHEN  order_onpage_ct>0 THEN f.session_id end) ordered_sessions
,sum(f.order_item_qty) sales_qty
,sum(f.order_event_ct) web_orders
,sum(f.order_current_price) web_demand_usd
,sum(a.removed_from_sb) as removed_from_sb
,sum(a.atw ) as atw 
,sum(a.remove_from_wl) as remove_from_wl
,sum(a.atb_from_wl) as atb_from_wl
,sum(a.filters_used) as filters_used
,sum(a.reviews_eng) as reviews_eng
,sum(a.content_eng) as content_eng
,sum(a.reccommendations) as reccommendations
,sum(a.saved_for_later) as saved_for_later
,sum(a.sfl_to_atb) as sfl_to_atb
,sum(a.sfl_removed) as sfl_removed
,CURRENT_DATE() as dw_batch_date
,CURRENT_TIMESTAMP() as dw_sys_load_tmstp
,f.activity_date_partition
from acp_event_intermediate.session_fact_attributes_parquet f 
left join acp_event_intermediate.session_user_attributes_parquet u on f.session_id = u.session_id and f.activity_date_partition = u.activity_date_partition
left join actions a on a.session_id = f.session_id and a.activity_date_partition = f.activity_date_partition
left join engaged e on e.session_id = f.session_id and e.activity_date_partition = f.activity_date_partition
where f.activity_date_partition BETWEEN date'2024-01-07' and date'2024-01-15'
and f.channelcountry = 'US'
GROUP BY 1,2,3,4,5,60,61,62
;


-- Writing output to new Hive table
insert overwrite table ace_etl.usl_digital_funnel partition (activity_date_partition)
select /*+ REPARTITION(100) */ * 
from usl_digital_funnel
;
--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
msck repair table ace_etl.usl_digital_funnel;
-- Writing output to teradata landing table.  
insert overwrite table usl_digital_funnel_ldg_output
select 
 channel  
,experience 
,channelcountry 
,acp_id 
,inception_session_id 
,new_recognized 
,session_start 
,landing_page 
,exit_page 
,mrkt_type 
,finance_rollup 
,finance_detail 
,session_count 
,acp_count 
,unique_cust_count 
,bounced_sessions 
,total_pages_visited     
,total_unique_pages_visited 
,hp_engaged_sessions     
,hp_views 
,pdp_sessions 
,pdp_engaged_sessions 
,pdp_views 
,browse_sessions    
,browse_engaged_sessions 
,browse_views 
,search_sessions    
,search_engaged_sessions 
,search_views 
,sbn_sessions 
,sbn_engaged_sessions 
,sbn_views 
,wishlist_sessions 
,wishlist_engaged_sessions 
,wishlist_views 
,checkout_sessions 
,checkout_engaged_sessions 
,checkout_views 
,shopb_sessions 
,shopb_engaged_sessions 
,shopb_views   
,atb_sessions 
,atb_views 
,atb_events 
,ordered_sessions 
,sales_qty 
,web_orders 
,web_demand_usd 
,removed_from_sb 
,atw 
,remove_from_wl 
,atb_from_wl 
,filters_used 
,reviews_eng 
,content_eng 
,reccommendations 
,saved_for_later 
,sfl_to_atb 
,sfl_removed 
,dw_batch_date 
,dw_sys_load_tmstp
,activity_date_partition 
from ace_etl.usl_digital_funnel
where activity_date_partition BETWEEN date'2024-01-07' and date'2024-01-15'
;
