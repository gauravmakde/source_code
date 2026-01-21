
create or replace temporary view date_range as
select distinct to_date(cal.day_date, 'MM/dd/yyyy') as date_par
from OBJECT_MODEL.DAY_CAL_454_DIM cal 
where to_date(day_date, 'MM/dd/yyyy') between {start_date} and {end_date}
;

create or replace temporary view Recs_raw as
select *
from 
	(select activity_date_partition, session_id, channel, experience, channelcountry, product_style_id, event_time_pst, event_id, product_action, context_pagetype, action_feature,
			coalesce(split_part(element_subid,'/',1),'Unknown') as placement, 
			coalesce(split_part(element_subid,'/',2),'Unknown') as strategy
	from ace_etl.session_product_discovery_funnel_intermediate
	where activity_date_partition in (select date_par from date_range)
		and (product_action in ('Click on Product','See Product') and upper(action_feature) = 'RECOMMENDATIONS')
	)
where  placement not like ('%"%')
	and placement not like ('%netsparker%')
	and placement not like ('%#%')
	and placement not like ('%$%')
	and placement not like ('%& SET%')
	and placement not like ('%:%')
	and placement not like ('%+%')
	and placement not like ('%=%')
	and placement not like ('%|%')
	and placement not like ('%\u003b%')
	and placement not like ('%<%')
	and placement not like ('%*%')
	and placement not like ('%.%')
	and placement not like ('%0xFFF9999%') 
	and strategy not like ('%A 0xFFF9999%')
	and strategy not like ('%.%') 
;

create or replace temporary view PSS
as
select activity_date_partition, session_id, channel, experience, channelcountry, product_style_id, event_time_pst, event_id, product_action, context_pagetype, placement, strategy
from Recs_raw
where product_action in ('Click on Product')
;

create or replace temporary view ATB as
select activity_date_partition, session_id, channel, experience, channelcountry, product_style_id, event_time_pst, event_id, product_action, event_current_price
from ace_etl.session_product_discovery_funnel_intermediate
where activity_date_partition in (select date_par from date_range)
	and product_action in ('Add To Bag')
;

create or replace temporary view orders as
select activity_date_partition, session_id, channel, experience, channelcountry, product_style_id, event_time_pst, event_id, product_action, event_current_price, orderlineid
from ace_etl.session_product_discovery_funnel_intermediate
where activity_date_partition in (select date_par from date_range)
	and product_action in ('Order Item')
;

create or replace temporary view atb_cte as
select  activity_date_partition, channel, experience, channelcountry, ATB_session_id, ATB_product_style, atb_time_pst, ATB_event_id, ATB_price, context_pagetype, placement, strategy, click_time_pst, click_event_id
from
(select distinct a.activity_date_partition, a.channel, a.experience, a.channelcountry, 
			a.session_id as ATB_session_id, a.product_style_id as ATB_product_style, a.event_time_pst as atb_time_pst, a.event_id as ATB_event_id, a.event_current_price as ATB_price, 
			p.event_time_pst as click_time_pst, p.event_id as click_event_id, p.context_pagetype, p.placement, p.strategy,
            row_number() over(partition by a.activity_date_partition, a.session_id, a.product_style_id, a.event_id order by (a.event_time_pst - p.event_time_pst), p.event_id ) as rk
	FROM PSS p
	JOIN ATB a
	ON p.activity_date_partition = a.activity_date_partition
		and p.session_id = a.session_id
	    and p.product_style_id = a.product_style_id
	    and p.event_time_pst < a.event_time_pst
	)
where rk = 1
;

create or replace temporary view order_cte as 
select activity_date_partition, channel, experience, channelcountry, Order_session_id, order_product_style, order_time_pst, order_event_id, orderlineid, order_price, context_pagetype, placement, strategy, click_time_pst, click_event_id
from
	(select distinct o.activity_date_partition, o.channel, o.experience, o.channelcountry, 
			o.session_id as Order_session_id, o.product_style_id as order_product_style, o.event_time_pst as order_time_pst, o.event_id as order_event_id, orderlineid, o.event_current_price as order_price,
			p.event_time_pst as click_time_pst, p.event_id as click_event_id, p.context_pagetype, p.placement, p.strategy,
            row_number() over(partition by  o.activity_date_partition, o.session_id, o.product_style_id, o.event_id, o.orderlineid order by (o.event_time_pst - p.event_time_pst), p.event_id ) as rk
	FROM PSS p
	JOIN ATB a
	ON p.activity_date_partition = a.activity_date_partition
		and p.session_id = a.session_id
	    and p.product_style_id = a.product_style_id
	    and p.event_time_pst < a.event_time_pst
	JOIN orders o
	ON a.activity_date_partition = o.activity_date_partition
	and a.session_id = o.session_id
	    and a.product_style_id = o.product_style_id
	    and a.event_time_pst < o.event_time_pst
	    and p.event_time_pst < o.event_time_pst
	)
where rk = 1
;

create or replace temporary view Topline_and_Recs_Upper_Metrics as
select a.activity_date_partition, a.channel, a.experience, a.channelcountry, 
		a.Topline_sessions, a.Topline_Orders, b.Topline_session_PDV, b.Topline_session_ATB, b.Topline_session_Ord, b.Topline_Demand,
		b.Recs_View_session, b.Recs_View_product, b.Recs_Click_session, b.Recs_Click_product
from 
	(select activity_date_partition, channel, experience, channelcountry, count(distinct session_id) as Topline_sessions, sum(order_event_ct) as Topline_Orders
	from acp_event_intermediate.session_fact_attributes_parquet
	where activity_date_partition in (select date_par from date_range)
	group by 1,2,3,4
	) a
left join (
	select
		aa.activity_date_partition,
		aa.channel,
		aa.experience,
		aa.channelcountry,
		aa.Topline_session_PDV,
		aa.Topline_session_ATB,
		aa.Topline_session_Ord,
		aa.Topline_Demand,
		bb.Recs_View_session,
		bb.Recs_View_product,
		bb.Recs_Click_session,
		bb.Recs_Click_product
	from (
		select activity_date_partition, channel, experience, channelcountry,
			count(distinct case when product_action = 'View Product Detail' then session_id else null end) as Topline_session_PDV,
			count(distinct case when product_action = 'Add To Bag' then session_id else null end) as Topline_session_ATB,
			count(distinct case when product_action = 'Order Item' then session_id else null end) as Topline_session_Ord,
			sum(case when product_action = 'Order Item' then event_current_price else 0 end) as Topline_Demand
		from ace_etl.session_product_discovery_funnel_intermediate
		where activity_date_partition in (select date_par from date_range)
		group by 1,2,3,4
	) as aa
	left join (
		select activity_date_partition, channel, experience, channelcountry,
			count(distinct case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_View_session,
			count(case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_View_product,
			count(distinct case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_Click_session,
			count(case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_Click_product
		from ace_etl.session_product_discovery_funnel_intermediate
		where activity_date_partition in (select date_par from date_range)
			and split_part(element_subid,'/',1) not like ('%"%')
			and split_part(element_subid,'/',1) not like ('%netsparker%')
			and split_part(element_subid,'/',1) not like ('%#%')
			and split_part(element_subid,'/',1) not like ('%$%')
			and split_part(element_subid,'/',1) not like ('%& SET%')
			and split_part(element_subid,'/',1) not like ('%:%')
			and split_part(element_subid,'/',1) not like ('%+%')
			and split_part(element_subid,'/',1) not like ('%=%')
			and split_part(element_subid,'/',1) not like ('%|%') 
			and split_part(element_subid,'/',1) not like ('%\u003b%')
			and split_part(element_subid,'/',1) not like ('%<%')
			and split_part(element_subid,'/',1) not like ('%*%')
			and split_part(element_subid,'/',1) not like ('%.%')
			and split_part(element_subid,'/',1) not like ('%0xFFF9999%') 
			and split_part(element_subid,'/',2) not like ('%A 0xFFF9999%')
			and split_part(element_subid,'/',2) not like ('%.%') 
		group by 1,2,3,4
	 ) as bb
	 on aa.activity_date_partition = bb.activity_date_partition
	 	and aa.channel = bb.channel
		and aa.experience = bb.experience
		and aa.channelcountry = bb.channelcountry
	) b
on a.activity_date_partition = b.activity_date_partition
	and a.channel = b.channel
	and a.experience = b.experience
	and a.channelcountry = b.channelcountry
;

create or replace temporary view ATB_Metrics_ttl as
SELECT activity_date_partition, channel, experience, channelcountry,
		COUNT(DISTINCT ATB_session_id) AS Recs_ATB_session,
		COUNT(ATB_product_style) Recs_ATB_item,
		SUM(ATB_price) AS Recs_ATB_demand
from atb_cte
group by 1,2,3,4
;

create or replace temporary view Order_Metrics_ttl as
SELECT activity_date_partition, channel, experience, channelcountry,
		COUNT(DISTINCT Order_session_id) AS Recs_Ord_session,
		COUNT(distinct order_event_id) Recs_Order,
		COUNT(distinct orderlineid) Recs_Ord_item,
		SUM(order_price) AS Recs_Ord_demand
from order_cte
group by 1,2,3,4
;

create or replace temporary view Total_level as
select 'Total Level' as aggregation_level,
		a.activity_date_partition as activity_date, a.channel, a.experience, a.channelcountry, 'Total' as context_pagetype, 'Total' as placement, 'Total' as strategy,
		a.Topline_sessions, 
		a.Topline_session_PDV, 
		a.Topline_session_ATB, 
		a.Topline_session_Ord, 
		a.Topline_Demand,
		a.Topline_Orders,
		a.Recs_View_session, 
		a.Recs_View_product, 
		a.Recs_Click_session, 
		a.Recs_Click_product,
		b.Recs_ATB_session,
		b.Recs_ATB_item,
		b.Recs_ATB_demand,
		o.Recs_Ord_session,
		o.Recs_Order,
		o.Recs_Ord_item,
		o.Recs_Ord_demand,
		a.activity_date_partition
from Topline_and_Recs_Upper_Metrics a
left join ATB_Metrics_ttl b
on a.activity_date_partition = b.activity_date_partition
	and a.channel = b.channel
	and a.experience = b.experience
	and a.channelcountry = b.channelcountry
left join Order_Metrics_ttl o
on a.activity_date_partition = o.activity_date_partition
	and a.channel = o.channel
	and a.experience = o.experience
	and a.channelcountry = o.channelcountry
;

create or replace temporary view Recs_Upper_Metrics_L1 as
select activity_date_partition, channel, experience, channelcountry, context_pagetype, 
		count(distinct case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_View_session,
		count(case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_View_product, 
		count(distinct case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_Click_session,
		count(case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_Click_product
from Recs_raw
group by 1,2,3,4,5
;

create or replace temporary view ATB_Metrics_L1 as
SELECT activity_date_partition, channel, experience, channelcountry, context_pagetype,
		COUNT(DISTINCT ATB_session_id) AS Recs_ATB_session,
		COUNT(ATB_product_style) Recs_ATB_item,
		SUM(ATB_price) AS Recs_ATB_demand
from atb_cte
group by 1,2,3,4,5
;

create or replace temporary view Order_Metrics_L1 as
SELECT activity_date_partition, channel, experience, channelcountry, context_pagetype,
		COUNT(DISTINCT Order_session_id) AS Recs_Ord_session,
		COUNT(distinct order_event_id) Recs_Order,
		COUNT(distinct orderlineid) Recs_Ord_item,
		SUM(order_price) AS Recs_Ord_demand
from order_cte
group by 1,2,3,4,5
;

create or replace temporary view Level1 as
select 'By pagetype' as aggregation_level,
		a.activity_date_partition as activity_date, a.channel, a.experience, a.channelcountry, a.context_pagetype, 'Total' as placement, 'Total' as strategy,
		null as Topline_sessions, 
		null as Topline_session_PDV, 
		null as Topline_session_ATB, 
		null as Topline_session_Ord, 
		null as Topline_Demand,
		null as Topline_Orders,
		a.Recs_View_session, 
		a.Recs_View_product, 
		a.Recs_Click_session, 
		a.Recs_Click_product,
		b.Recs_ATB_session,
		b.Recs_ATB_item,
		b.Recs_ATB_demand,
		o.Recs_Ord_session,
		o.Recs_Order,
		o.Recs_Ord_item,
		o.Recs_Ord_demand,
		a.activity_date_partition
from Recs_Upper_Metrics_L1 a 
left join ATB_Metrics_L1 b
on a.activity_date_partition = b.activity_date_partition
	and a.channel = b.channel
	and a.experience = b.experience
	and a.channelcountry = b.channelcountry
	and a.context_pagetype = b.context_pagetype
left join Order_Metrics_L1 o
on a.activity_date_partition = o.activity_date_partition
	and a.channel = o.channel
	and a.experience = o.experience
	and a.channelcountry = o.channelcountry
	and a.context_pagetype = o.context_pagetype
;

create or replace temporary view Recs_Upper_Metrics_L2 as
select activity_date_partition, channel, experience, channelcountry, context_pagetype, strategy,
		count(distinct case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_View_session,
		count(case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_View_product, 
		count(distinct case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_Click_session,
		count(case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_Click_product
from Recs_raw
group by 1,2,3,4,5,6
;

create or replace temporary view ATB_Metrics_L2 as
SELECT activity_date_partition, channel, experience, channelcountry, context_pagetype, strategy,
		COUNT(DISTINCT ATB_session_id) AS Recs_ATB_session,
		COUNT(ATB_product_style) Recs_ATB_item,
		SUM(ATB_price) AS Recs_ATB_demand
from atb_cte
group by 1,2,3,4,5,6
;

create or replace temporary view Order_Metrics_L2 as
SELECT activity_date_partition, channel, experience, channelcountry, context_pagetype, strategy,
		COUNT(DISTINCT Order_session_id) AS Recs_Ord_session,
		COUNT(distinct order_event_id) Recs_Order,
		COUNT(distinct orderlineid) Recs_Ord_item,
		SUM(order_price) AS Recs_Ord_demand
from order_cte
group by 1,2,3,4,5,6
;

create or replace temporary view Level2 as
select 'By pagetype, strategy' as aggregation_level,
		a.activity_date_partition as activity_date, a.channel, a.experience, a.channelcountry, a.context_pagetype, 'Total' as placement, a.strategy,
		null as Topline_sessions, 
		null as Topline_session_PDV, 
		null as Topline_session_ATB, 
		null as Topline_session_Ord, 
		null as Topline_Demand,
		null as Topline_Orders,
		a.Recs_View_session, 
		a.Recs_View_product, 
		a.Recs_Click_session, 
		a.Recs_Click_product,
		b.Recs_ATB_session,
		b.Recs_ATB_item,
		b.Recs_ATB_demand,
		o.Recs_Ord_session,
		o.Recs_Order,
		o.Recs_Ord_item,
		o.Recs_Ord_demand,
		a.activity_date_partition
from Recs_Upper_Metrics_L2 a 
left join ATB_Metrics_L2 b
on a.activity_date_partition = b.activity_date_partition
	and a.channel = b.channel
	and a.experience = b.experience
	and a.channelcountry = b.channelcountry
	and a.context_pagetype = b.context_pagetype
	and a.strategy = b.strategy
left join Order_Metrics_L2 o
on a.activity_date_partition = o.activity_date_partition
	and a.channel = o.channel
	and a.experience = o.experience
	and a.channelcountry = o.channelcountry
	and a.context_pagetype = o.context_pagetype
	and a.strategy = o.strategy
;

create or replace temporary view Recs_Upper_Metrics_L3 as
select activity_date_partition, channel, experience, channelcountry, context_pagetype, placement, 
		count(distinct case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_View_session,
		count(case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_View_product, 
		count(distinct case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_Click_session,
		count(case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_Click_product
from Recs_raw
group by 1,2,3,4,5,6
;

create or replace temporary view ATB_Metrics_L3 as
SELECT activity_date_partition, channel, experience, channelcountry, context_pagetype, placement,
		COUNT(DISTINCT ATB_session_id) AS Recs_ATB_session,
		COUNT(ATB_product_style) Recs_ATB_item,
		SUM(ATB_price) AS Recs_ATB_demand
from atb_cte
group by 1,2,3,4,5,6
;

create or replace temporary view Order_Metrics_L3 as
SELECT activity_date_partition, channel, experience, channelcountry, context_pagetype, placement, 
		COUNT(DISTINCT Order_session_id) AS Recs_Ord_session,
		COUNT(distinct order_event_id) Recs_Order,
		COUNT(distinct orderlineid) Recs_Ord_item,
		SUM(order_price) AS Recs_Ord_demand
from order_cte
group by 1,2,3,4,5,6
;

create or replace temporary view Level3 as
select 'By pagetype, placement' as aggregation_level,
		a.activity_date_partition as activity_date, a.channel, a.experience, a.channelcountry, a.context_pagetype, a.placement, 'Total' as strategy,
		null as Topline_sessions, 
		null as Topline_session_PDV, 
		null as Topline_session_ATB, 
		null as Topline_session_Ord, 
		null as Topline_Demand,
		null as Topline_Orders,
		a.Recs_View_session, 
		a.Recs_View_product, 
		a.Recs_Click_session, 
		a.Recs_Click_product,
		b.Recs_ATB_session,
		b.Recs_ATB_item,
		b.Recs_ATB_demand,
		o.Recs_Ord_session,
		o.Recs_Order,
		o.Recs_Ord_item,
		o.Recs_Ord_demand,
		a.activity_date_partition
from Recs_Upper_Metrics_L3 a  
left join ATB_Metrics_L3 b
on a.activity_date_partition = b.activity_date_partition
	and a.channel = b.channel
	and a.experience = b.experience
	and a.channelcountry = b.channelcountry
	and a.context_pagetype = b.context_pagetype
	and a.placement = b.placement
left join Order_Metrics_L3 o
on a.activity_date_partition = o.activity_date_partition
	and a.channel = o.channel
	and a.experience = o.experience
	and a.channelcountry = o.channelcountry
	and a.context_pagetype = o.context_pagetype
	and a.placement = o.placement
;

create or replace temporary view Recs_Upper_Metrics_L4 as
select activity_date_partition, channel, experience, channelcountry, context_pagetype, placement, strategy,
		count(distinct case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_View_session,
		count(case when product_action = 'See Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_View_product, 
		count(distinct case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then session_id else null end) as Recs_Click_session,
		count(case when product_action = 'Click on Product' and upper(action_feature) = 'RECOMMENDATIONS' then product_style_id else null end) as Recs_Click_product
from Recs_raw
group by 1,2,3,4,5,6,7
;

create or replace temporary view ATB_Metrics_L4 as
SELECT activity_date_partition, channel, experience, channelcountry, context_pagetype, placement, strategy,
		COUNT(DISTINCT ATB_session_id) AS Recs_ATB_session,
		COUNT(ATB_product_style) Recs_ATB_item,
		SUM(ATB_price) AS Recs_ATB_demand
from atb_cte
group by 1,2,3,4,5,6,7
;

create or replace temporary view Order_Metrics_L4 as
SELECT activity_date_partition, channel, experience, channelcountry, context_pagetype, placement, strategy,
		COUNT(DISTINCT Order_session_id) AS Recs_Ord_session,
		COUNT(distinct order_event_id) Recs_Order,
		COUNT(distinct orderlineid) Recs_Ord_item,
		SUM(order_price) AS Recs_Ord_demand
from order_cte
group by 1,2,3,4,5,6,7
;

create or replace temporary view Level4 as
select 'By pagetype, placement, strategy' as aggregation_level,
		a.activity_date_partition as activity_date, a.channel, a.experience, a.channelcountry, a.context_pagetype, a.placement, a.strategy,
		null as Topline_sessions, 
		null as Topline_session_PDV, 
		null as Topline_session_ATB,  
		null as Topline_session_Ord, 
		null as Topline_Demand,
		null as Topline_Orders,
		a.Recs_View_session, 
		a.Recs_View_product, 
		a.Recs_Click_session, 
		a.Recs_Click_product,
		b.Recs_ATB_session,
		b.Recs_ATB_item,
		b.Recs_ATB_demand,
		o.Recs_Ord_session,
		o.Recs_Order,
		o.Recs_Ord_item,
		o.Recs_Ord_demand,
		a.activity_date_partition
from Recs_Upper_Metrics_L4 a
left join ATB_Metrics_L4 b
on a.activity_date_partition = b.activity_date_partition
	and a.channel = b.channel
	and a.experience = b.experience
	and a.channelcountry = b.channelcountry
	and a.context_pagetype = b.context_pagetype
	and a.placement = b.placement
	and a.strategy = b.strategy
left join Order_Metrics_L4 o
on a.activity_date_partition = o.activity_date_partition
	and a.channel = o.channel
	and a.experience = o.experience
	and a.channelcountry = o.channelcountry
	and a.context_pagetype = o.context_pagetype
	and a.placement = o.placement
	and a.strategy = o.strategy
;

create table if not exists {hive_schema}.recs_total_level
(
    aggregation_level string,
	activity_date date,
	channel string,
	experience string,
	channelcountry string,
	context_pagetype string,
	placement string,
	strategy string,
	Topline_sessions bigint, 
	Topline_session_PDV bigint, 
	Topline_session_ATB bigint, 
	Topline_session_Ord decimal(36, 6), 
	Topline_Demand decimal(36, 6),
    Topline_Orders decimal(36, 6),
	Recs_View_session bigint, 
	Recs_View_product bigint, 
	Recs_Click_session bigint, 
	Recs_Click_product bigint,
	Recs_ATB_session bigint,
	Recs_ATB_item bigint,
	Recs_ATB_demand decimal(36, 6),
	Recs_Ord_session bigint,
	Recs_Order decimal(36, 6),
	Recs_Ord_item bigint,
	Recs_Ord_demand decimal(36, 6),
	activity_date_partition date
)
using PARQUET
location 's3://{s3_bucket_root_var}/recs_total_level/'
;


insert overwrite table {hive_schema}.recs_total_level
select *
from Total_level
;

create table if not exists {hive_schema}.recs_level1
(
    aggregation_level string,
	activity_date date,
	channel string,
	experience string,
	channelcountry string,
	context_pagetype string,
	placement string,
	strategy string,
	Topline_sessions bigint, 
	Topline_session_PDV bigint, 
	Topline_session_ATB bigint, 
	Topline_session_Ord decimal(36, 6), 
	Topline_Demand decimal(36, 6),
    Topline_Orders decimal(36, 6),
	Recs_View_session bigint, 
	Recs_View_product bigint, 
	Recs_Click_session bigint, 
	Recs_Click_product bigint,
	Recs_ATB_session bigint,
	Recs_ATB_item bigint,
	Recs_ATB_demand decimal(36, 6),
	Recs_Ord_session bigint,
	Recs_Order decimal(36, 6),
	Recs_Ord_item bigint,
	Recs_Ord_demand decimal(36, 6),
	activity_date_partition date
)
using PARQUET
location 's3://{s3_bucket_root_var}/recs_level1/'
;

insert overwrite table {hive_schema}.recs_level1
select *
from Level1
;

create table if not exists {hive_schema}.recs_level2
(
    aggregation_level string,
	activity_date date,
	channel string,
	experience string,
	channelcountry string,
	context_pagetype string,
	placement string,
	strategy string,
	Topline_sessions bigint, 
	Topline_session_PDV bigint, 
	Topline_session_ATB bigint, 
	Topline_session_Ord decimal(36, 6), 
	Topline_Demand decimal(36, 6),
    Topline_Orders decimal(36, 6),
	Recs_View_session bigint, 
	Recs_View_product bigint, 
	Recs_Click_session bigint, 
	Recs_Click_product bigint,
	Recs_ATB_session bigint,
	Recs_ATB_item bigint,
	Recs_ATB_demand decimal(36, 6),
	Recs_Ord_session bigint,
	Recs_Order decimal(36, 6),
	Recs_Ord_item bigint,
	Recs_Ord_demand decimal(36, 6),
	activity_date_partition date
)
using PARQUET
location 's3://{s3_bucket_root_var}/recs_level2/'
;

insert overwrite table {hive_schema}.recs_level2
select *
from Level2
;

create table if not exists {hive_schema}.recs_level3
(
    aggregation_level string,
	activity_date date,
	channel string,
	experience string,
	channelcountry string,
	context_pagetype string,
	placement string,
	strategy string,
	Topline_sessions bigint, 
	Topline_session_PDV bigint, 
	Topline_session_ATB bigint, 
	Topline_session_Ord decimal(36, 6), 
	Topline_Demand decimal(36, 6),
    Topline_Orders decimal(36, 6),
	Recs_View_session bigint, 
	Recs_View_product bigint, 
	Recs_Click_session bigint, 
	Recs_Click_product bigint,
	Recs_ATB_session bigint,
	Recs_ATB_item bigint,
	Recs_ATB_demand decimal(36, 6),
	Recs_Ord_session bigint,
	Recs_Order decimal(36, 6),
	Recs_Ord_item bigint,
	Recs_Ord_demand decimal(36, 6),
	activity_date_partition date
)
using PARQUET
location 's3://{s3_bucket_root_var}/recs_level3/'
;

insert overwrite table {hive_schema}.recs_level3
select *
from Level3
;

create table if not exists {hive_schema}.recs_level4
(
    aggregation_level string,
	activity_date date,
	channel string,
	experience string,
	channelcountry string,
	context_pagetype string,
	placement string,
	strategy string,
	Topline_sessions bigint, 
	Topline_session_PDV bigint, 
	Topline_session_ATB bigint, 
	Topline_session_Ord decimal(36, 6), 
	Topline_Demand decimal(36, 6),
    Topline_Orders decimal(36, 6),
	Recs_View_session bigint, 
	Recs_View_product bigint, 
	Recs_Click_session bigint, 
	Recs_Click_product bigint,
	Recs_ATB_session bigint,
	Recs_ATB_item bigint,
	Recs_ATB_demand decimal(36, 6),
	Recs_Ord_session bigint,
	Recs_Order decimal(36, 6),
	Recs_Ord_item bigint,
	Recs_Ord_demand decimal(36, 6),
	activity_date_partition date
)
using PARQUET
location 's3://{s3_bucket_root_var}/recs_level4/'
;


insert overwrite table {hive_schema}.recs_level4
select *
from Level4
;

create table if not exists {hive_schema}.recs_dash
(
    aggregation_level string,
	activity_date date,
	channel string,
	experience string,
	channelcountry string,
	context_pagetype string,
	placement string,
	strategy string,
	Topline_sessions bigint, 
	Topline_session_PDV bigint, 
	Topline_session_ATB bigint, 
	Topline_session_Ord decimal(36, 6), 
	Topline_Demand decimal(36, 6),
    Topline_Orders decimal(36, 6),
	Recs_View_session bigint, 
	Recs_View_product bigint, 
	Recs_Click_session bigint, 
	Recs_Click_product bigint,
	Recs_ATB_session bigint,
	Recs_ATB_item bigint,
	Recs_ATB_demand decimal(36, 6),
	Recs_Ord_session bigint,
	Recs_Order decimal(36, 6),
	Recs_Ord_item bigint,
	Recs_Ord_demand decimal(36, 6),
	activity_date_partition date
)
using PARQUET
location 's3://{s3_bucket_root_var}/recs_dash/'
partitioned by (activity_date_partition);

insert overwrite table {hive_schema}.recs_dash partition (activity_date_partition)
select *
from {hive_schema}.recs_total_level
union all
select *
from {hive_schema}.recs_level1
union all
select *
from {hive_schema}.recs_level2
union all
select *
from {hive_schema}.recs_level3
union all
select *
from {hive_schema}.recs_level4
;

MSCK REPAIR TABLE {hive_schema}.recs_dash;