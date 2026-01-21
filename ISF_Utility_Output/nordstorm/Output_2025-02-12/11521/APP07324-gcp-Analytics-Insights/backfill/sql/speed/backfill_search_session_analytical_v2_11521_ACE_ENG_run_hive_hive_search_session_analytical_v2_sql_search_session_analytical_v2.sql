-- Table Name:search_session_analytical_v2
-- Team/Owner: Discovery/Anupama Manjunath & Soren Stime
-- Date Created/Modified: created on 01/05/2024

-- Note:
-- What is the the purpose of the table: to enable analyst and engineer to do query level analysis
-- What is the update cadence/lookback window: refresh daily and 2 days lookback window


create table if not exists ace_etl.search_session_analytical_v2
( activity_week_start date
,event_time_utc timestamp
,event_time_pst timestamp
,cust_id string
,channelcountry string
,channel string
,experience string
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
,context_pagetype string
,context_id string 
,pageinstance_id string
,referrer string
,mrkt_type string
,finance_rollup string
,finance_detail string
,suggestion_value string
,arrival_rank integer
,search_value string
,prev_search_value string
,next_search_value string 
,search_parent_rank integer
,search_parent_id string
,repeat_search_value_rank integer
,repeat_search_value_count integer
,result_query_rank integer
,result_query_id string
,matchstrategy string 
,parent_query_filter integer
,parent_query_sorted integer
,search_filters_key string 
,search_sortorder string
,search_totalresultcount integer
,productstyle_id string  
,atb_ct bigint
,atb_units decimal(38,6)
,order_ct bigint
,order_units decimal(38,6)
,activity_date_partition date   
)
using PARQUET
location 's3://ace-etl/search_session_analytical_v2/'
partitioned by (activity_date_partition);


MSCK REPAIR table ace_etl.search_session_analytical_v2;

create or replace temp view search_analytical_base as
select
cast((date_trunc('week',(activity_date_partition + interval '1' day))) - interval '1' day as date) as activity_week_start
,activity_date_partition
,event_time_utc
,event_time_pst
,cust_id
,channelcountry
,channel
,experience
,se.session_id
,event_id
,event_name
,feature
,element_id
,element_value
,element_type
,element_index
,element_hierarchy
,element_subid
,element_subindex
,context_id
,context_pagetype
,pageinstance_id
,page_indicator
,page_rank
,referrer
,mrkt_type
,finance_rollup
,finance_detail
,arrival_rank  
,search_value
,search_context_id as result_context_id
,matchstrategy
,search_filters_key
,search_sortorder
,search_totalresultcount
,resultcountrequested
,productstyle_id
,rank() over (partition by se.session_id order by page_rank asc) as search_session_page_rank
,row_number() over (partition by se.session_id order by page_rank,event_time_utc,event_id) as rn
,result_query_rank
from
(select
activity_date_partition
,event_time_utc
,event_time_pst
,cust_id
,channelcountry
,channel
,experience
,session_id
,event_id
,event_name
,feature
,element_id
,element_value
,element_type
,element_index
,element_hierarchy
,element_subid
,element_subindex
,context_id
,context_pagetype
,pageinstance_id
,page_indicator
,page_rank
,referrer
,mrkt_type
,finance_rollup
,finance_detail
,arrival_rank  
,search_value
,search_filters_key
,search_sortorder
,search_totalresultcount
,productstyle_id
from acp_event_intermediate.session_evt_expanded_attributes_parquet 
where event_name not in ('com.nordstrom.customer.OrderSubmitted','com.nordstrom.customer.AddedToBag')
and activity_date_partition between date'2023-03-06' and date'2023-03-11’
)se
left join 
(select
session_id as search_session_id
,key_page_instance_id
,context_id as search_context_id
,matchstrategy
,resultcountrequested
,offset
,row_number() over(partition by session_id order by key_page_min_eventtime,event_id asc) as result_query_rank
from acp_event_intermediate.search_event_attributes_parquet
where activity_date_partition between date'2023-03-06' and date'2023-03-11’

) search 
on se.session_id = search.search_session_id
and se.pageinstance_id = search.key_page_instance_id
where activity_date_partition between date'2023-03-06' and date'2023-03-11’
and channelcountry = 'US'
and page_indicator = 'SEARCH'
;

create or replace temp view search_analytical_last_events as
select
current.session_id
,current.pageinstance_id
,rank() over (partition by current.session_id,current.pageinstance_id order by current.page_rank) as page_internal_rank
,prior.source_feature as prior_last_feature
,suggestion_value
from
(select
    session_id
    ,pageinstance_id
    ,page_rank
    ,search_browse_value
from acp_event_intermediate.session_page_attributes_parquet
where activity_date_partition between date'2023-03-06' and date'2023-03-11’
and page_indicator = 'SEARCH'
) current
left join 
(
select
distinct 
session_id
,page_rank
,context_pagetype
,last_value(case when event_name in ('com.nordstrom.event.customer.Impressed','com.nordstrom.event.customer.Engaged') then feature end,true) 
				 over (partition by session_page_rank_id order by event_time_pst,event_id rows between unbounded preceding and unbounded following) as source_feature
,last_value(case when element_id = 'Global/KeywordSearch/Suggestion' and event_name = 'com.nordstrom.event.customer.Engaged' then element_value end,true) 
				 over (partition by session_page_rank_id order by event_time_pst,event_id rows between unbounded preceding and unbounded following) as suggestion_value
from acp_event_intermediate.session_evt_expanded_attributes_parquet 
where activity_date_partition between date'2023-03-06' and date'2023-03-11’
) 
prior 
on current.session_id = prior.session_id
and current.page_rank -1 = prior.page_rank
;

create or replace temp view search_analytical_next_prior_values as
select 
parent_query_eventtime
,session_id
,result_context_id
,search_value
,activity_date_partition
,last_value(search_value) over (partition by session_id order by parent_query_eventtime asc rows between unbounded preceding and 1 preceding) as prior_search_value
,first_value(search_value) over (partition by session_id order by parent_query_eventtime rows between 1 following and unbounded following) as  next_search_value
from 
(select 
min(event_time_pst) as parent_query_eventtime
,session_id
,result_context_id
,search_value
,activity_date_partition
from search_analytical_base
group by 2,3,4,5
)
;	

create or replace temp view search_analytical_all_events as
select
 activity_week_start 
,activity_date_partition 
,event_time_utc 
,event_time_pst
,cust_id
,channelcountry
,channel
,experience
,session_id
,event_id
,event_name 
,feature
,element_id
,element_value
,element_type
,element_index
,element_hierarchy
,element_subid
,element_subindex
,context_pagetype
,result_context_id
,pageinstance_id 
,referrer
,mrkt_type
,finance_rollup
,finance_detail
,suggestion_value 
,arrival_rank
,search_value
,prior_search_value
,next_search_value 
,parent_query_rank
,parent_query_id
,result_query_rank
,matchstrategy 
,search_filters_key
,search_sortorder
,search_totalresultcount 
,productstyle_id  
,max(case when (search_filters_key != 'DesignerSale') and (search_filters_key <> '') and (search_filters_key <> 'NULL')
		  then 1 else 0 end) over (partition by session_id,parent_query_rank) as parent_query_filter
,max(case when search_sortorder <> 'FEATURED' THEN 1 else 0 end) over (partition by session_id, parent_query_rank) as parent_query_sorted
,max(search_value_repeat_rank) over (partition by session_id, search_value) AS repeat_search_value_rank
,(max(search_value_repeat_rank) over (partition by session_id, search_value)) -1 AS repeat_search_value_count
,concat(session_id, cast(result_query_rank AS string)) AS result_query_id
from
(
select
*
,concat(session_id,cast(parent_query_rank AS string)) as parent_query_id
,dense_rank () over (partition by session_id,search_value order by parent_query_rank) as search_value_repeat_rank
from
(select
*
,dense_rank () over (partition by session_id order by original_parent_query_rank) as parent_query_rank
from
(
select
*
,first_value(original_search_parent_rank) over (partition by session_id,result_context_id order by rn rows between unbounded preceding and unbounded following) as original_parent_query_rank
from
(
select
*
,sum(staging) over (partition by session_id order by rn) as original_search_parent_rank
 from
(select
*
,(case when page_internal_rn = 1 and (prior_search_value <> search_value or search_session_page_rank = 1 or lower(prior_last_feature) = 'keywordsearch' or arrival_rank >= 1) then 1 else 0 end) AS staging
from
(select
base.activity_week_start 
,base.activity_date_partition 
,base.event_time_utc 
,base.event_time_pst
,base.cust_id
,base.channelcountry
,base.channel
,base.experience
,base.session_id
,base.event_id
,base.event_name 
,base.feature
,base.element_id
,base.element_value
,base.element_type
,base.element_index
,base.element_hierarchy
,base.element_subid
,base.element_subindex
,base.context_pagetype
,base.result_context_id 
,base.pageinstance_id 
,base.referrer
,base.mrkt_type
,base.finance_rollup
,base.finance_detail
,base.arrival_rank
,base.search_value
,result_query_rank
,matchstrategy 
,search_totalresultcount
,search_session_page_rank
,search_filters_key
,search_sortorder
,rn
,productstyle_id  
,le.prior_last_feature
,le.suggestion_value
,np.next_search_value
,np.prior_search_value
,row_number() over (partition by base.session_id,page_rank order by rn) as page_internal_rn
from search_analytical_base base
left join search_analytical_last_events le 
on base.session_id = le.session_id
and base.pageinstance_id = le.pageinstance_id
and page_internal_rank = 1 
left join search_analytical_next_prior_values np
on base.session_id = np.session_id
and base.result_context_id = np.result_context_id
	)
		)
			)
				)
					)
						)
;	

create or replace temp view search_analytical_output_v2 as 
select 
search_exp.activity_week_start 
,search_exp.event_time_utc 
,search_exp.event_time_pst
,search_exp.cust_id
,search_exp.channelcountry
,search_exp.channel
,search_exp.experience
,search_exp.session_id
,search_exp.event_id
,search_exp.event_name 
,search_exp.feature
,search_exp.element_id
,search_exp.element_value
,search_exp.element_type
,search_exp.element_index
,search_exp.element_hierarchy
,search_exp.element_subid
,search_exp.element_subindex
,search_exp.context_pagetype
,result_context_id as context_id
,search_exp.pageinstance_id 
,search_exp.referrer
,search_exp.mrkt_type
,search_exp.finance_rollup
,search_exp.finance_detail
,suggestion_value 
,cast(arrival_rank as integer) as arrival_rank
,search_exp.search_value
,search_exp.prior_search_value as prev_search_value
,search_exp.next_search_value 
,cast(search_exp.parent_query_rank as integer) as search_parent_rank 
,parent_query_id as search_parent_id
,repeat_search_value_rank 
,repeat_search_value_count 
,result_query_rank 
,result_query_id 
,matchstrategy 
,parent_query_filter
,parent_query_sorted
,search_exp.search_filters_key
,search_exp.search_sortorder
,search_totalresultcount 
,search_exp.productstyle_id  
,atb_ct
,atb_units
,order_ct
,order_units
,search_exp.activity_date_partition 
from search_analytical_all_events search_exp
left join 
(select
session_id as atb_session_id
,atb_style_id
,count(distinct event_id) as atb_ct
,sum(atb_current_units) as atb_units
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where event_name = 'com.nordstrom.customer.AddedToBag'
and atb_flag = 1
and activity_date_partition between date'2023-03-06' and date'2023-03-11’
group by 1,2 ) atb
on search_exp.productstyle_id = atb.atb_style_id
and search_exp.event_name = 'com.nordstrom.event.customer.ProductSummarySelected'
and search_exp.session_id = atb.atb_session_id
left join
(select
session_id as order_session_id
,order_style_id
,count(distinct orderlineid) as order_ct
,sum(order_line_current_units) as order_units
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where event_name = 'com.nordstrom.customer.OrderSubmitted'
and activity_date_partition between date'2023-03-06' and date'2023-03-11’
group by 1,2
) ord 
on search_exp.productstyle_id = ord.order_style_id
and search_exp.event_name = 'com.nordstrom.event.customer.ProductSummarySelected'
and search_exp.session_id = ord.order_session_id
;

		
insert
    OVERWRITE TABLE ace_etl.search_session_analytical_v2 PARTITION (activity_date_partition)
select
/*+ REPARTITION(100) */
*
from search_analytical_output_v2
;               
         
                                                    