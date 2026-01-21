-- Table Name:browse_session_analytical_v2
-- Team/Owner: Discovery/Anupama Manjunath
-- Date Created/Modified: created on 11/02/2023

-- Note:
-- What is the the purpose of the table: to enable analyst and engineer to do query level analysis
-- What is the update cadence/lookback window: refresh daily and 2 days lookback window

CREATE TABLE if not exists {hive_schema}.browse_session_analytical_v2 
(
 activity_week_start date
,event_time_utc timestamp 
,event_time_pst  timestamp 
,cust_id string 
,channelcountry string
,channel string
,session_id string 
,event_id string 
,event_name string 
,feature string 
,element_id string 
,element_value string 
,element_type string 
,element_index string 
,element_hierarchy string 
,element_subid string 
,element_subindex string 
,context_id string 
,context_pagetype string 
,pageinstance_id string 
,referrer string 
,mrkt_type string 
,finance_rollup string 
,finance_detail string 
,arrival_rank int
,browse_origin_source string 
,browse_context_origin_source string 
,browse_context_origin string 
,browse_page_origin string
,browse_value string
,prev_browse_value string
,next_browse_value string
,browse_parent_rank int
,browse_parent_id string
,browse_navigation_rank int
,browse_navigation_id string
,browse_filters_key string
,browse_sortorder string
,browse_totalresultcount int
,matchstrategy string
,resultcountrequested int 
,productstyle_id string
,path_count int
,path1 string
,path2 string
,path3 string
,path4 string
,path5 string
,path6 string
,nav_parent_path string 
,nav_child_path1 string
,nav_child_path2 string
,nav_child_path3 string
,nav_child_path4 string
,nav_child_path5 string
,nav_child_path6 string
,total_child_path int
,page_rank int
,original_browse_parent_rank int
,original_browse_navigation_rank int
,prior_context_pagetype string
,prior_last_element string
,prior_last_context_pagetype string
,prior_last_feature string
,prior_last_element_type string
,next_browse_origin_source string
,next_browse_context_origin string
,atb_ct bigint
,atb_units decimal(38,6)
,order_ct bigint
,order_units decimal(38,6)
,activity_date_partition date
,experience string
) 
using PARQUET
location "s3://{s3_bucket_root_var}/browse_session_analytical_v2" partitioned BY (activity_date_partition, experience);

msck repair table {hive_schema}.browse_session_analytical_v2;

create or replace temp view browse_analytical_base as
select 
cast((date_trunc('week',(activity_date_partition + interval '1' day))) - interval '1' day as date) as activity_week_start 
,activity_date_partition
,session_page_rank_id
,session_id
,cust_id 
,channel
,experience
,channelcountry
,event_id
,element_id 
,element_value 
,element_type 
,element_index 
,element_hierarchy 
,element_subid 
,element_subindex 
,browse_context_id as context_id
,pageinstance_id
,page_rank
,rank() over (partition by session_id order by page_rank asc) as browse_session_page_rank
,cast(arrival_rank as int) as arrival_rank
,max(case when arrival_rank >= 1 then 1 else 0 end) over (partition by session_id,pageinstance_id) as external_signal
,prior_context_pagetype
,referrer
,mrkt_type 
,finance_rollup 
,finance_detail 
,event_time_utc
,event_time_pst 
,event_name
,productstyle_id
,feature
,context_pagetype
,page_indicator
,browse_value
,last_value(browse_value) over (partition by session_id order by page_rank,event_time_utc,event_id rows between unbounded preceding and 1 preceding) as prior_browse_value
,browse_filters_key
,browse_sortorder
,browse_totalresultcount
,row_number() over (partition by session_id order by page_rank,event_time_utc,event_id) as rn
,matchstrategy 
,cast(resultcountrequested as int) as resultcountrequested
from
(
select 
	cast(activity_date_partition as date) as activity_date_partition
	,session_page_rank_id
	,session_id
	,cust_id
	,channel
	,experience
	,channelcountry
	,event_id
	,element_id 
	,element_value 
	,element_type 
	,element_index 
	,element_hierarchy 
	,element_subid 
	,element_subindex 
	,context_id
	,pageinstance_id
	,page_rank
	,cast(arrival_rank as int) as arrival_rank
	,prior_context_pagetype
	,referrer
	,mrkt_type 
	,finance_rollup 
	,finance_detail 
	,event_time_utc
	,event_time_pst 
	,event_name
	,productstyle_id
	,feature
	,context_pagetype
	,page_indicator
	,browse_value
	,browse_filters_key
	,browse_sortorder
	,browse_totalresultcount 
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where event_name not in ('com.nordstrom.customer.OrderSubmitted','com.nordstrom.customer.AddedToBag')
) a
left join 
(select
    session_id as browse_session_id
    ,key_page_instance_id
    ,context_id as browse_context_id
    ,matchstrategy
    ,resultcountrequested
from acp_event_intermediate.browse_event_attributes_parquet
where activity_date_partition between {start_date} and {end_date}
) b
on a.session_id = b.browse_session_id
and a.pageinstance_id = b.key_page_instance_id
where activity_date_partition between {start_date} and {end_date}
and channelcountry = 'US'
and page_indicator = 'BROWSE'
;

create or replace temp view browse_analytical_last_events as
select
 current.session_id
 ,current.pageinstance_id
 ,rank() over (partition by current.session_id,current.pageinstance_id order by current.page_rank) as page_internal_rank
 ,prior.context_pagetype as prior_last_context_pagetype
 ,prior.last_click_feature as prior_last_feature
 ,prior.last_click_element_id as prior_last_element
 ,prior.last_click_element_type as prior_last_element_type
from acp_event_intermediate.session_page_attributes_parquet current
left join 
(select
	distinct session_id
	,page_rank
	,context_pagetype
	,last_value(case when event_name in ('com.nordstrom.event.customer.Engaged') and lower(action) = 'click' then feature end)over
	(partition by session_page_rank_id order by event_time_pst, event_id rows between unbounded preceding and unbounded following) as last_click_feature
    ,last_value(case when event_name in ('com.nordstrom.event.customer.Engaged')and lower(action) = 'click' then element_id end) over 
    (partition by session_page_rank_id order by event_time_pst,event_id rows between unbounded preceding and unbounded following) as last_click_element_id
    ,last_value(case when event_name in ('com.nordstrom.event.customer.Engaged')and lower(action) = 'click' then element_type end) over 
    (partition by session_page_rank_id order by event_time_pst, event_id rows between unbounded preceding and unbounded following) as last_click_element_type
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where activity_date_partition between {start_date} and {end_date}
) prior 
on current.session_id = prior.session_id
and current.page_rank - 1 = prior.page_rank
where current.activity_date_partition between {start_date} and {end_date}
and page_indicator = 'BROWSE'
;

create or replace temp view browse_analytical_base_ref_origin as
select
	cast(base.activity_week_start as date) as activity_week_start
    ,base.activity_date_partition
    ,base.session_page_rank_id
    ,base.session_id
    ,base.cust_id  
    ,base.channel
    ,base.experience
    ,base.channelcountry
    ,base.event_id
    ,base.element_id  
    ,base.element_value 
    ,base.element_type  
    ,base.element_index  
    ,base.element_hierarchy  
    ,base.element_subid  
    ,base.element_subindex 
    ,base.context_id
    ,base.pageinstance_id
    ,base.page_rank
    ,base.browse_session_page_rank
    ,base.arrival_rank
    ,base.prior_context_pagetype
    ,base.referrer
    ,base.mrkt_type 
    ,base.finance_rollup 
    ,base.finance_detail 
    ,base.event_time_utc
    ,base.event_time_pst 
    ,base.event_name
    ,base.productstyle_id
    ,base.feature
    ,base.context_pagetype
    ,base.page_indicator
    ,base.browse_value
    ,base.prior_browse_value
    ,base.browse_filters_key
    ,base.browse_sortorder
    ,base.browse_totalresultcount
    ,base.rn
    ,base.matchstrategy  
    ,base.resultcountrequested  
    ,prior_last_element
    ,prior_last_context_pagetype
    ,prior_last_feature
    ,prior_last_element_type
	,case when external_signal = 0 and prior_last_element = 'Content/Component' and prior_last_feature = 'CONTENT' then 'Content Navigation'
          when external_signal = 0 and prior_last_element = 'ProductResults/Navigation/Link' and prior_last_feature = 'SEARCH_BROWSE' then 'Category Navigation'
          when external_signal = 0 and prior_last_element = 'Global/Navigation/Section/Link' then 'Global Navigation'
          when external_signal = 0 and prior_last_element = 'StoreDetail/ShopMyStore/Department' then 'Shop My Store Engagement'
          when external_signal = 0 and prior_last_element in ('ProductDetail/BrandTitleLink', 'MiniPDP/BrandTitleLink','ProductDetail/ShopTheBrand','ProductDetail/BrandTitle') then 'Shop the Brand'
          when external_signal = 0 and prior_last_element in 
          (
            'ProductResults/Filters/Section/Filter/Select',
            'ProductResults/Filters/Section/Filter/Deselect',
            'ProductResults/Filters/ActiveFilters/Filter/Deselect',
            'ProductResults/Filters/ActiveFilters/ClearFilters',
            'ProductResults/FilterTab',
            'ProductResults/Filters/Section/CurrentLocation',
            'ProductResults/Filters/ViewResults'
        ) and prior_last_feature = 'SEARCH_BROWSE' then 'Filter Engagement'
        when external_signal = 0 and prior_last_element in ('ProductResults/ProductGallery/SortSelect') and prior_last_feature = 'SEARCH_BROWSE' then 'Sort Engagement'
        when external_signal = 0 and prior_last_element is null and experience in ('DESKTOP_WEB') and prior_context_pagetype in 
        (
            'BRAND_RESULTS',
            'CATEGORY_RESULTS',
            'FLASH_EVENT_RESULTS',
            'FLASH_EVENTS',
            'SEARCH_RESULTS',
            'UNDETERMINED'
        ) then 'Path Engagement'
        when external_signal = 0 and (lower(prior_last_element) like '%tabbar%' and experience <> 'ANDROID_APP') or (lower(prior_last_element) in ('accounttab', 'hometab', 'bagtab', 'wishlisttab') and experience = 'ANDROID_APP') then 'TabBar Engagement'
        when external_signal = 0 and prior_last_element is null and browse_session_page_rank = 1 then 'Returning Session'
        when external_signal = 0 and prior_last_element is null and experience not in ('DESKTOP_WEB', 'MOBILE_WEB') and browse_session_page_rank > 1 then 'Pagination'
        when external_signal = 0 and (prior_last_element = 'ProductResults/ProductGallery/Pagination' and experience in ('DESKTOP_WEB', 'MOBILE_WEB')) and browse_session_page_rank > 1 then 'Pagination'
        when external_signal = 0 and prior_last_element is not null and browse_session_page_rank = 1 then 'Other' else 'Other'
        end as browse_int_origin
    ,case when external_signal = 1 then browse_value end as external_browse_path
    ,external_signal
from browse_analytical_base base
left join browse_analytical_last_events le 
	on base.session_id = le.session_id
	and base.pageinstance_id = le.pageinstance_id
	and page_internal_rank = 1
;

create or replace temp view browse_analytical_parent_path as
select
     activity_week_start 
    ,activity_date_partition
    ,session_page_rank_id
    ,session_id
    ,cust_id 
    ,channel
    ,experience
    ,channelcountry
    ,event_id
    ,element_id 
	,element_value 
	,element_type 
	,element_index 
	,element_hierarchy 
	,element_subid 
	,element_subindex 
    ,context_id
    ,pageinstance_id
    ,page_rank
    ,arrival_rank
    ,prior_context_pagetype
    ,referrer
    ,mrkt_type 
	,finance_rollup 
	,finance_detail 
    ,event_time_utc
    ,event_time_pst 
    ,event_name
    ,productstyle_id
    ,feature
    ,context_pagetype
    ,page_indicator
    ,case when browse_value like '/browse/%' then REPLACE(browse_value, '/browse/', '')
         when browse_value like '/clearance%' then REPLACE(browse_value,'/clearance','clearance')
         else browse_value end as browse_value
    ,case
        when prior_browse_value like '/browse/%' then REPLACE(prior_browse_value, '/browse/', '')
        when prior_browse_value like '/clearance%' then REPLACE(prior_browse_value,'/clearance','clearance')
        else prior_browse_value end as prior_browse_value
    ,browse_filters_key
    ,browse_sortorder
    ,browse_totalresultcount
    ,matchstrategy 
    ,resultcountrequested 
    ,first_value(ext_int_browse_origin) over (partition by session_id,pageinstance_id order by rn rows between unbounded preceding and unbounded following) as browse_page_origin
    ,first_value(ext_int_browse_origin) over (partition by session_id,context_id order by rn rows between unbounded preceding and unbounded following) as browse_context_origin
    ,prior_last_element
    ,prior_last_context_pagetype
    ,prior_last_feature
    ,prior_last_element_type
    ,rn
from
    (
select
     activity_week_start 
    ,activity_date_partition
    ,session_page_rank_id
    ,session_id
    ,cust_id 
    ,channel
    ,experience
    ,channelcountry
    ,event_id
	,element_id 
	,element_value 
	,element_type 
	,element_index 
	,element_hierarchy 
	,element_subid 
	,element_subindex 
	,context_id
    ,pageinstance_id
    ,page_rank
    ,arrival_rank
    ,prior_context_pagetype
    ,referrer
    ,mrkt_type 
	,finance_rollup 
	,finance_detail 
    ,event_time_utc
    ,event_time_pst 
    ,event_name
    ,productstyle_id
    ,feature
    ,context_pagetype
    ,page_indicator
    ,browse_value
    ,prior_browse_value
    ,browse_filters_key
    ,browse_sortorder
    ,browse_totalresultcount
    ,matchstrategy 
    ,resultcountrequested 
    ,prior_last_element
    ,prior_last_context_pagetype
    ,prior_last_feature
    ,prior_last_element_type
    ,browse_int_origin
    ,rn
   ,coalesce (case when external_signal = 1 then referrer end,browse_int_origin) as ext_int_browse_origin
from browse_analytical_base_ref_origin
)
;

create or replace temp view browse_analytical_parent_nav_rank as
select
*
,sum(staging) over (partition by session_id order by rn) as original_browse_parent_rank
,path_count - prior_path_count as var_path_count
,case when path1 = prior_path1 then 9999 else 1 end as var_path1
,case when path2 = prior_path2 then 9999 else 2 end as var_path2
,case when path3 = prior_path3 then 9999 else 3 end as var_path3
,case when path4 = prior_path4 then 9999 else 4 end as var_path4
,case when path5 = prior_path5 then 9999 else 5 end as var_path5
,case when path6 = prior_path6 then 9999 else 6 end as var_path6
from
(
select
    activity_week_start 
    ,activity_date_partition
    ,session_page_rank_id
    ,session_id
    ,cust_id 
    ,channel
    ,experience
    ,channelcountry
    ,event_id
   	,element_id 
	,element_value 
	,element_type 
	,element_index 
	,element_hierarchy 
	,element_subid 
	,element_subindex 
    ,context_id
    ,pageinstance_id
    ,page_rank
    ,arrival_rank
    ,prior_context_pagetype
    ,referrer
    ,mrkt_type 
	,finance_rollup 
	,finance_detail 
    ,event_time_utc
    ,event_time_pst 
    ,event_name
    ,productstyle_id
    ,feature
    ,context_pagetype
    ,page_indicator
    ,browse_value
    ,browse_filters_key
    ,browse_sortorder
    ,browse_totalresultcount
    ,matchstrategy 
    ,resultcountrequested 
    ,prior_browse_value
    ,prior_last_element
    ,prior_last_context_pagetype
    ,prior_last_feature
    ,prior_last_element_type
    ,browse_page_origin
    ,rn
    ,browse_context_origin
,length(browse_value) - length(replace(browse_value, '/', '')) + 1 as path_count
,split(browse_value, '/') [0] as path1
,split(browse_value, '/') [1] as path2
,split(browse_value, '/') [2] as path3
,split(browse_value, '/') [3] as path4
,split(browse_value, '/') [4] as path5
,split(browse_value, '/') [5] as path6
,case when browse_value is not null and  rn = 1 then 1 
      when browse_value = prior_browse_value then 0
      when browse_value <> prior_browse_value then 1
      else null end as staging
,coalesce(length(prior_browse_value) - length(replace(prior_browse_value, '/', '')) + 1,0) as prior_path_count
,split(prior_browse_value, '/') [0] as prior_path1
,split(prior_browse_value, '/') [1] as prior_path2
,split(prior_browse_value, '/') [2] as prior_path3
,split(prior_browse_value, '/') [3] as prior_path4
,split(prior_browse_value, '/') [4] as prior_path5
,split(prior_browse_value, '/') [5] as prior_path6
from browse_analytical_parent_path
 )
;

create or replace temp view browse_analytical_parent_original_rank as
select
    activity_week_start 
    ,activity_date_partition
    ,session_page_rank_id
    ,session_id
    ,cust_id 
    ,channel
    ,experience
    ,channelcountry
    ,event_id
    ,element_id 
	,element_value 
	,element_type 
	,element_index 
	,element_hierarchy 
	,element_subid 
	,element_subindex 
    ,context_id
    ,pageinstance_id
    ,page_rank
    ,arrival_rank
    ,prior_context_pagetype
    ,referrer
    ,mrkt_type 
	,finance_rollup 
	,finance_detail 
    ,event_time_utc
    ,event_time_pst 
    ,event_name
    ,productstyle_id
    ,feature
    ,context_pagetype
    ,page_indicator
    ,browse_value
    ,browse_filters_key
    ,browse_sortorder
    ,browse_totalresultcount
    ,matchstrategy 
    ,resultcountrequested 
    ,browse_context_origin
    ,prior_last_element
    ,prior_last_context_pagetype
    ,prior_last_feature
    ,prior_last_element_type
    ,browse_page_origin
    ,rn
    ,path_count
    ,path1
    ,path2
    ,path3
    ,path4
    ,path5
    ,path6
    ,original_browse_parent_rank
,sum(id_change_check) over (partition by (session_id)order by rn) as original_browse_navigation_rank
from
(
select
 *
,case when rn = 1 then 1
     when (rn = min_rn and (arrival_rank is not null or browse_page_origin in ( 'Global Navigation','Shop My Store Engagement', 'Shop the Brand')))
     and incremental_check = 0 and min_var < 10 then 1 when rn = min_rn and incremental_check = 1 then 1 else 0 end as id_change_check
from
(
select
    *
,min(rn) over (partition by session_id,pageinstance_id order by rn) as min_rn
,least(var_path1,var_path2,var_path3,var_path4,var_path5,var_path6) as min_var
,case when (var_path_count >= 0 and least(var_path1,var_path2,var_path3,var_path4,var_path5,var_path6) >= prior_path_count)
      or (var_path_count < 0 and least(var_path1,var_path2,var_path3,var_path4,var_path5,var_path6) > path_count)
      or least(var_path1, var_path2, var_path3,var_path4,var_path5,var_path6) = 9999 then 0 else 1 end as incremental_check
from browse_analytical_parent_nav_rank
)
	)
;	

create or replace temp view browse_analytical_parent_ranks as
select
activity_week_start 
,activity_date_partition
,session_page_rank_id
,session_id
,cust_id 
,channel
,experience
,channelcountry
,event_id
,element_id 
,element_value 
,element_type 
,element_index 
,element_hierarchy 
,element_subid 
,element_subindex 
,context_id
,pageinstance_id
,page_rank
,arrival_rank
,prior_context_pagetype
,referrer
,mrkt_type 
,finance_rollup 
,finance_detail 
,event_time_utc
,event_time_pst 
,event_name
,productstyle_id
,feature
,context_pagetype
,page_indicator
,browse_value
,browse_filters_key
,browse_sortorder
,browse_totalresultcount
,matchstrategy 
,resultcountrequested 
,browse_page_origin
,browse_context_origin
,prior_last_element
,prior_last_context_pagetype
,prior_last_feature
,prior_last_element_type
,path_count
,path1
,path2
,path3
,path4
,path5
,path6
,original_browse_parent_rank
,dense_rank () over (partition by session_id order by browse_parent_rank) as browse_parent_rank
,original_browse_navigation_rank
,dense_rank () over (partition by session_id order by browse_navigation_rank) as browse_navigation_rank
from
    (
select
activity_week_start 
,activity_date_partition
,session_page_rank_id
,session_id
,cust_id 
,channel
,experience
,channelcountry
,event_id
,element_id 
,element_value 
,element_type 
,element_index 
,element_hierarchy 
,element_subid 
,element_subindex 
,context_id
,pageinstance_id
,page_rank
,arrival_rank    
,prior_context_pagetype
,referrer
,mrkt_type 
,finance_rollup 
,finance_detail 
,event_time_utc
,event_time_pst 
,event_name
,productstyle_id
,feature
,context_pagetype
,page_indicator
,browse_value
,browse_filters_key
,browse_sortorder
,browse_totalresultcount
,matchstrategy
,resultcountrequested 
,browse_page_origin
,browse_context_origin
,rn
,prior_last_element
,prior_last_context_pagetype
,prior_last_feature
,prior_last_element_type
,path_count
,path1
,path2
,path3
,path4
,path5
,path6
,original_browse_parent_rank
,first_value(original_browse_parent_rank) over (partition by session_id,context_id order by rn rows between unbounded preceding and unbounded following) as browse_parent_rank
,original_browse_navigation_rank
,first_value(original_browse_navigation_rank) over (partition by session_id,context_id order by rn rows between unbounded preceding and unbounded following) as browse_navigation_rank
from browse_analytical_parent_original_rank
) 
  ; 

create or replace temp view browse_analytical_final_part1 as    
select
activity_week_start 
,activity_date_partition
,session_page_rank_id
,session_id
,cust_id 
,channel
,experience
,channelcountry
,event_id
,element_id 
,element_value 
,element_type 
,element_index 
,element_hierarchy 
,element_subid 
,element_subindex 
,context_id
,pageinstance_id
,page_rank
,arrival_rank
,prior_context_pagetype
,referrer
,mrkt_type 
,finance_rollup 
,finance_detail 
,event_time_utc
,event_time_pst 
,event_name
,feature
,context_pagetype
,page_indicator
,browse_value
,browse_filters_key
,browse_sortorder
,browse_totalresultcount
,matchstrategy 
,resultcountrequested 
,browse_page_origin
,browse_context_origin
,prior_last_element
,prior_last_context_pagetype
,prior_last_feature
,prior_last_element_type
,path_count
,path1
,path2
,path3
,path4
,path5
,path6
,original_browse_parent_rank
,browse_parent_rank
,concat(session_id, cast(browse_parent_rank as string)) as browse_parent_id
,original_browse_navigation_rank
,browse_navigation_rank
,concat(session_id,cast(browse_navigation_rank as string)) as browse_navigation_id
,a.productstyle_id
,atb_ct
,atb_units
,order_ct
,order_units
from browse_analytical_parent_ranks a
left join 
(
select
session_id as atb_session_id
,atb_style_id
,count(distinct event_id) as atb_ct
,sum(atb_current_units) as atb_units
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where event_name = 'com.nordstrom.customer.AddedToBag'
and atb_flag = 1
and activity_date_partition between {start_date} and {end_date}
group by 1,2
) atb 
on  a.productstyle_id = atb.atb_style_id
and a.event_name = 'com.nordstrom.event.customer.ProductSummarySelected'
and a.session_id = atb.atb_session_id
left join 
(select
session_id as order_session_id
,order_style_id
,count(distinct orderlineid) as order_ct
,sum(order_line_current_units) as order_units
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where event_name = 'com.nordstrom.customer.OrderSubmitted'
and activity_date_partition between {start_date} and {end_date}
group by 1,2
) ord
on  a.productstyle_id = ord.order_style_id
and a.event_name = 'com.nordstrom.event.customer.ProductSummarySelected'
and a.session_id = ord.order_session_id
; 

create or replace temp view browse_analytical_base_nav_path_step1 as
select 
activity_date_partition
,parent_query_eventtime
,channel
,experience
,session_id
,browse_value
,browse_origin_source
,browse_context_origin
,browse_context_origin_source 
,browse_parent_id
,browse_navigation_id
,path1
,path2
,path3
,path4
,path5
,path6
,path_count
,prev_browse_value
,next_browse_value
,next_browse_origin_source
,next_browse_context_origin
,total_child_path
,nav_parent_path
,first_value(child_1,true)over(partition by session_id,browse_navigation_id order by  parent_query_eventtime asc rows between unbounded preceding and unbounded following) as child_1
,first_value(child_2,true)over(partition by session_id,browse_navigation_id order by  parent_query_eventtime asc rows between unbounded preceding and unbounded following) as child_2
,first_value(child_3,true)over(partition by session_id,browse_navigation_id order by  parent_query_eventtime asc rows between unbounded preceding and unbounded following) as child_3
,first_value(child_4,true)over(partition by session_id,browse_navigation_id order by  parent_query_eventtime asc rows between unbounded preceding and unbounded following) as child_4
,first_value(child_5,true)over(partition by session_id,browse_navigation_id order by  parent_query_eventtime asc rows between unbounded preceding and unbounded following) as child_5
,first_value(child_6,true)over(partition by session_id,browse_navigation_id order by  parent_query_eventtime asc rows between unbounded preceding and unbounded following) as child_6
from 
(select
*
,first_value(browse_value,true)over(partition by session_id,browse_navigation_id order by parent_query_eventtime) as nav_parent_path
,(case when child_rank = 1 then first_value(browse_value,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) end) as child_1
,(case when child_rank = 2 then first_value(browse_value,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) end) as child_2
,(case when child_rank = 3 then first_value(browse_value,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) end) as child_3
,(case when child_rank = 4 then first_value(browse_value,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) end) as child_4
,(case when child_rank = 5 then first_value(browse_value,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) end) as child_5
,(case when child_rank = 6 then first_value(browse_value,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) end) as child_6
from
(
select * 
,(case when child_path_flag is not null then dense_rank()over(partition by session_id,browse_navigation_id order by parent_query_eventtime,child_row_num)end) as child_rank
from  
(select 
* 
,(case when child_path_flag is not null then row_number()over(partition by session_id,browse_navigation_id order by parent_query_eventtime) end) as child_row_num
,count(child_path_flag)over(partition by session_id,browse_navigation_id) as total_child_path
from 
(select *
,(case when (browse_context_origin_source in ('Global Navigation','External Navigation','Category Navigation','Content Navigation') and next_browse_context_origin in ('Category Navigation','Content Navigation'))
	 then 1 end) as child_path_flag
,first_value(browse_origin_source,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) as next_browse_origin_source
from
(
select 
activity_date_partition
,parent_query_eventtime
,channel
,experience
,session_id
,browse_value
,browse_context_origin
,browse_parent_id
,browse_navigation_id
,path1
,path2
,path3
,path4
,path5
,path6
,path_count
,browse_context_origin_source 
,(case when rnk = 1 then last_value(browse_value,true) over (partition by session_id order by parent_query_eventtime rows between unbounded preceding and 1 preceding) end) as prev_browse_value
,(case when rnk = 1 then first_value(browse_value,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) end) as next_browse_value
,first_value(browse_context_origin,true) over (partition by session_id order by parent_query_eventtime rows between  1 following and unbounded following) as next_browse_context_origin
,first_value(browse_origin_source,true) over (partition by session_id,browse_parent_id,browse_value order by parent_query_eventtime rows between unbounded preceding and unbounded following) as browse_origin_source
from 
(
select *
,(case when browse_context_origin_source not in ('Filter Engagement','Sort Engagement','Pagination') 
	   then	(first_value(browse_context_origin_source,true) over (partition by session_id,browse_parent_id order by parent_query_eventtime rows between unbounded preceding and unbounded following))end) as browse_origin_source
,rank()over(partition by session_id,browse_parent_id order by parent_query_eventtime) as rnk
from 
(
select 
activity_date_partition
,min(event_time_pst) as parent_query_eventtime
,channel
,experience
,session_id
,browse_value
,browse_context_origin
,browse_parent_id
,browse_navigation_id
,path1
,path2
,path3
,path4
,path5
,path6
,path_count
,(case when browse_context_origin not in ('Global Navigation','Filter Engagement','Category Navigation','App-Foreground','Sort Engagement','Content Navigation','Pagination'
		   ,'Returning Session','Other','Shop the Brand','App-None','Path Engagement','App-PushNotification','TabBar Engagement','App-Launch','Shop My Store Engagement'
		   ,'App-OpenURL','') then 'External Navigation' else browse_context_origin end) browse_context_origin_source 

from browse_analytical_final_part1
where activity_date_partition between {start_date} and {end_date}
group by 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
	)
		)
			)
				)	
					)
						)    
;


create or replace temp view browse_analytical_base_nav_path_step2 as
select a.*
,b.prev_browse_value
,b.next_browse_value
,b.nav_parent_path
,b.child_1
,b.child_2
,b.child_3
,b.child_4
,b.child_5
,b.child_6 
,b.total_child_path
,b.browse_context_origin_source 
,b.browse_origin_source
,b.next_browse_origin_source
,b.next_browse_context_origin
from
(select 
*
from browse_analytical_final_part1
where activity_date_partition between {start_date} and {end_date}
) a
left join 
(select 
*
from browse_analytical_base_nav_path_step1
where activity_date_partition between {start_date} and {end_date}
) b
on a.session_id = b.session_id 
and a.browse_parent_id = b.browse_parent_id
and a.browse_navigation_id = b.browse_navigation_id
and a.event_time_pst = b.parent_query_eventtime
and a.browse_value = b.browse_value
and a.activity_date_partition = b.activity_date_partition
;

create or replace temp view browse_analytical_base_nav_path_step3 as
select
activity_date_partition
,session_id 
,browse_navigation_id
,max(nav_parent_path) as nav_parent_path
,max(child_1) as child_1
,max(child_2) as child_2
,max(child_3) as child_3
,max(child_4) as child_4
,max(child_5) as child_5
,max(child_6) as child_6
,max(total_child_path) as total_child_path
,max(browse_context_origin_source) as browse_context_origin_source  
,max(browse_origin_source) as browse_origin_source
,max(next_browse_origin_source) as next_browse_origin_source
,max(next_browse_context_origin) as next_browse_context_origin
from browse_analytical_base_nav_path_step2
group by 1,2,3
;

create or replace temp view browse_analytical_base_nav_path_step4 as
select
activity_date_partition
,session_id 
,browse_parent_id
,max(prev_browse_value) as prev_browse_value
,max(next_browse_value) as next_browse_value
from browse_analytical_base_nav_path_step2
group by 1,2,3
;

create or replace temp view browse_analytical_output as
select
a.activity_week_start
,a.event_time_utc
,a.event_time_pst
,a.cust_id 
,a.channelcountry
,a.channel
,a.session_id
,a.event_id
,a.event_name
,a.feature
,a.element_id 
,a.element_value 
,a.element_type 
,a.element_index 
,a.element_hierarchy 
,a.element_subid 
,a.element_subindex 
,a.context_id
,a.context_pagetype
,a.pageinstance_id
,a.referrer
,a.mrkt_type 
,a.finance_rollup 
,a.finance_detail 
,cast(a.arrival_rank as int) as arrival_rank
,b.browse_origin_source
,b.browse_context_origin_source
,a.browse_page_origin
,a.browse_context_origin
,a.browse_value
,c.prev_browse_value
,c.next_browse_value
,cast(a.browse_parent_rank as int) as browse_parent_rank
,a.browse_parent_id
,cast(a.browse_navigation_rank as int) as browse_navigation_rank
,a.browse_navigation_id
,a.browse_filters_key
,a.browse_sortorder
,cast(a.browse_totalresultcount as int) as browse_totalresultcount
,a.matchstrategy 
,cast(a.resultcountrequested as int) as resultcountrequested
,a.productstyle_id
,cast(path_count as int) as path_count
,path1
,path2
,path3
,path4
,path5
,path6
,b.nav_parent_path
,b.child_1 as nav_child_path1
,b.child_2 as nav_child_path2
,b.child_3 as nav_child_path3
,b.child_4 as nav_child_path4
,b.child_5 as nav_child_path5
,b.child_6 as nav_child_path6
,cast(b.total_child_path as int) as total_child_path
,page_rank
,original_browse_parent_rank
,original_browse_navigation_rank
,prior_context_pagetype
,prior_last_element
,prior_last_context_pagetype
,prior_last_feature
,prior_last_element_type
,b.next_browse_origin_source
,b.next_browse_context_origin
,cast(a.atb_ct as bigint) as atb_ct
,a.atb_units
,a.order_ct
,a.order_units
,cast(a.activity_date_partition as date) as activity_date_partition
,a.experience
from 
(select * 
from browse_analytical_final_part1
where activity_date_partition between {start_date} and {end_date}
)a

left join 

(select * 
from browse_analytical_base_nav_path_step3
)b
on a.session_id = b.session_id 
and a.browse_navigation_id = b.browse_navigation_id 
and a.activity_date_partition = b.activity_date_partition

left join 
(select * 
from browse_analytical_base_nav_path_step4
)c
on a.session_id = c.session_id 
and a.browse_parent_id = c.browse_parent_id
and a.activity_date_partition = c.activity_date_partition
;




insert
    OVERWRITE TABLE {hive_schema}.browse_session_analytical_v2 PARTITION (activity_date_partition, experience)
select
/*+ REPARTITION(100) */
*
from browse_analytical_output
;       