create table if not exists {hive_schema}.content_session_analytical (
	fiscal_year_num int, 
	fiscal_quarter_num int, 
	fiscal_month_num int, 
	fiscal_week_num int,
	event_time_pst timestamp,
	channel string,
	experience string,
	channelcountry string,
	session_page_rank_id string, 
	session_id string,
	event_id string,
	event_name string,
	loyalty_flag int,
	pageinstance_id string,
	context_pagetype string,
	page_indicator string,
	page_rank int,
	page_location string,
	content_browse_page_flag int,
	prior_pageinstance_id string,
	prior_context_pagetype string,
	next_pageinstance_id string,
	next_context_pagetype string,
	next_page_indicator string,
	next_page_location string,
	next_content_browse_page_flag int,
	next_products_viewed int,
	next_products_selected int,
	next_click_occurred int,
	landing_page_flag int,
	exit_page_flag int,
	element_id string,
	sub_component string,
	element_value string,
	canvas_id string,
	component_name string,
	digitalcontents_type string,
	mrkt_type string,
	finance_rollup string,
	finance_detail string,
	activity_date_partition date
)
using ORC
location "s3://{s3_bucket_root_var}/content_session_analytical"
partitioned by (activity_date_partition); 

MSCK REPAIR table {hive_schema}.content_session_analytical;

create or replace temporary view raw_content as 
select
	activity_date_partition,
	event_time_pst,
	channel,
	experience,
	channelcountry,
	session_page_rank_id,
	session_id,
	event_id,
	event_name,
	case when loyalty_id is not null then 1 else 0 end as loyalty_flag,
	pageinstance_id,
	context_pagetype,
	page_indicator,
	case 
		when context_pagetype in ('BRAND_RESULTS', 'CATEGORIES', 'CATEGORY_RESULTS', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS') and page_indicator is null then 1 
		else 0 
	end as content_browse_page_flag,
	page_rank, 
	prior_context_pagetype,
	prior_pageinstance_id,
	next_context_pagetype,
	next_pageinstance_id,
	case 
		when entry_pageinstance_id = pageinstance_id and page_rank = 1 then 1 
		else 0 
	end as landing_page_flag,
	case 
		when exit_pageinstance_id = pageinstance_id then 1 
		else 0 
	end as exit_page_flag,
	element_id,
	element_type as sub_component,
	element_value,
	digitalcontents_id as canvas_id,
	digitalcontents_type,
	case 
		when page_indicator = 'BROWSE' then browse_value 
		else null 
	end as browse_value,
	productstyle_type,
	mrkt_type,
	finance_rollup,
	finance_detail
from acp_event_intermediate.session_evt_expanded_attributes_parquet
	where activity_date_partition between {start_date}
    	and {end_date}
	and cust_idtype = 'SHOPPER_ID'
	and feature = 'CONTENT'
	and digitalcontents_type in ('CANVAS','CANVAS_BLOCK') 
	and channelcountry = 'US'
;

create or replace temporary view downstream as 
select
	click.last_click_event_id,
	click.pageinstance_id as click_pageinstance_id,
	click.page_rank as click_page_rank,
	browse.browse_value as next_browse_value,
	browse.page_indicator as next_page_indicator,
	case 
	    when context_pagetype in ('BRAND_RESULTS', 'CATEGORIES', 'CATEGORY_RESULTS', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS') and page_indicator is null then 1 
	    else 0 
	end as next_content_browse_page_flag,
	browse.context_pagetype as next_context_pagetype,
	browse.click_occurred as next_click_occurred,
	browse_v2.products_viewed as next_products_viewed, 
	browse_v2.products_selected as next_products_selected
from
	(
	select
		session_id,
		pageinstance_id,
		next_pageinstance_id,
		page_rank,
		last_click_event_id 
	from acp_event_intermediate.session_page_attributes_parquet	
	where activity_date_partition between {start_date}
    	and {end_date}
	and last_click_element_id like ('Content/%')
	) click
	left join 
	(
	select
		session_id,
		pageinstance_id,
		page_rank,
		case 
		    when page_indicator = 'BROWSE' then search_browse_value 
		    else null 
		end as browse_value,
		page_indicator,
		context_pagetype,
		click_occurred
	from acp_event_intermediate.session_page_attributes_parquet	
	where activity_date_partition between {start_date}
    	and {end_date}
	and arrival_rank is not null
	) browse
	on click.session_id = browse.session_id
	and click.next_pageinstance_id = browse.pageinstance_id
	and click.page_rank + 1 = browse.page_rank
	left join
	(
	select
		session_id,
		pageinstance_id, 
		page_rank,
		count(distinct case when event_name = 'com.nordstrom.event.customer.ProductSummaryCollectionViewed' then productstyle_id end) as products_viewed,
		count(distinct case when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' then productstyle_id end) as products_selected
	from acp_event_intermediate.session_evt_expanded_attributes_parquet
	where activity_date_partition between {start_date}
    	and {end_date}
	and productstyle_type = 'WEB'
	and arrival_rank is not null
	group by 1,2,3
	) browse_v2
	on browse.session_id = browse_v2.session_id
	and browse.pageinstance_id = browse_v2.pageinstance_id
	and browse.page_rank = browse_v2.page_rank
;

create or replace temporary view pre_fix as 
select distinct 
	cast(cal.fiscal_year_num as int) as fiscal_year_num, 
	cast(cal.fiscal_quarter_num as int) as fiscal_quarter_num, 
	cast(cal.fiscal_month_num as int) as fiscal_month_num, 
	cast(cal.fiscal_week_num as int) as fiscal_week_num,
	base.event_time_pst,
	base.channel,
	base.experience,
	base.channelcountry,
	base.session_page_rank_id,
	base.session_id,
	base.event_id,
	base.event_name,
	base.loyalty_flag,
	base.pageinstance_id,
	base.context_pagetype,
	base.page_indicator,
	base.page_rank,
	base.browse_value as page_location,
	base.content_browse_page_flag,
	base.prior_pageinstance_id,
	base.prior_context_pagetype,
	base.next_pageinstance_id,
	base.next_context_pagetype,
	down.next_page_indicator,
	down.next_browse_value as next_page_location,
	down.next_content_browse_page_flag,
	down.next_products_viewed,
	down.next_products_selected,
	down.next_click_occurred,
	base.landing_page_flag,
	base.exit_page_flag,
	base.element_id,
	base.sub_component,
	base.element_value,
	base.canvas_id,
	coalesce(c.component_name, ca.component_name, 'blank') as component_name,
	base.digitalcontents_type,
	base.mrkt_type,
	base.finance_rollup,
	base.finance_detail,
	base.activity_date_partition
from raw_content base
left join downstream down 
on base.event_id = down.last_click_event_id
and base.page_rank = down.click_page_rank 
and base.pageinstance_id = down.click_pageinstance_id
left join 
	(SELECT cp.id as canvas_id, max(cp.name) as component_name, route
	FROM acp_event.customer_canvas_page_published
	LATERAL VIEW OUTER explode(components) as cp
	where to_date(
                concat(
                    year,
                    '-',
                    LPAD(month, 2, '0'),
                    '-',
                    LPAD(day, 2, '0')
                ),
                'yyyy-MM-dd'
            ) > date('2022-01-01')
	group by 1,3) c
on base.canvas_id = c.canvas_id
left join 
	(select canvas_id, max(component_name) as component_name
	from digital_optimization_sandbox.hqky_canvas_component_name_oneoff
	group by 1) ca
on base.canvas_id = ca.canvas_id
left join OBJECT_MODEL.DAY_CAL_454_DIM cal 
on base.activity_date_partition =  TO_DATE(CAST(UNIX_TIMESTAMP(day_date, 'MM/dd/yyyy') AS TIMESTAMP))
;

create or replace temporary view new_engaged as 
select
	eng.fiscal_year_num,
	eng.fiscal_quarter_num, 
	eng.fiscal_month_num, 
	eng.fiscal_week_num,
	eng.event_time_pst,
	eng.channel,
	eng.experience,
	eng.channelcountry,
	max(coalesce(imp.session_page_rank_id, eng.session_page_rank_id)) as session_page_rank_id,
	eng.session_id,
	eng.event_id,
	eng.event_name,
	eng.loyalty_flag,
	max(coalesce(imp.pageinstance_id, eng.pageinstance_id)) as pageinstance_id,
	max(coalesce(imp.context_pagetype, eng.context_pagetype)) as context_pagetype,
	max(coalesce(imp.page_indicator, eng.page_indicator)) as page_indicator,
	max(coalesce(imp.page_rank, eng.page_rank)) as page_rank,
	max(coalesce(imp.page_location, eng.page_location)) as page_location,
	max(coalesce(imp.prior_context_pagetype, eng.prior_context_pagetype)) as prior_context_pagetype,
	max(coalesce(imp.prior_pageinstance_id, eng.prior_pageinstance_id)) as prior_pageinstance_id,
	eng.next_pageinstance_id,
	eng.next_context_pagetype,
	eng.next_page_indicator,
	eng.next_page_location,
	eng.next_content_browse_page_flag,
	eng.next_products_viewed,
	eng.next_products_selected,
	eng.next_click_occurred,
	max(coalesce(imp.landing_page_flag, eng.landing_page_flag)) as landing_page_flag,
	max(coalesce(imp.exit_page_flag, eng.exit_page_flag)) as exit_page_flag,
	eng.element_id,
	eng.sub_component,
	eng.element_value,
	eng.canvas_id,
	eng.component_name,
	eng.digitalcontents_type,
	max(coalesce(imp.mrkt_type, eng.mrkt_type)) as mrkt_type,
	max(coalesce(imp.finance_rollup, eng.finance_rollup)) as finance_rollup,
	max(coalesce(imp.finance_detail, eng.finance_detail)) as finance_detail,
	eng.activity_date_partition
from 
(
select * 
from pre_fix
where activity_date_partition between {start_date}
    and {end_date}
	and activity_date_partition >= date('2023-03-02')
and event_name = 'com.nordstrom.event.customer.Engaged'
and experience in ('DESKTOP_WEB', 'MOBILE_WEB')
) eng
left join 
(
select *
from pre_fix
where activity_date_partition between {start_date}
    and {end_date}
	and activity_date_partition >= date('2023-03-02')
and event_name = 'com.nordstrom.event.customer.Impressed'
and experience in ('DESKTOP_WEB', 'MOBILE_WEB')
) imp
on eng.session_id = imp.session_id 
and eng.canvas_id = imp.canvas_id
and eng.activity_date_partition = imp.activity_date_partition
and imp.event_time_pst + interval '45' second >= eng.event_time_pst
group by 1,2,3,4,5,6,7,8,10,11,12,13,21,22,23,24,25,26,27,28,31,32,33,34,35,36,40
;

create or replace temporary view output as
(select * 
from pre_fix 
where (event_name in ('com.nordstrom.event.customer.Impressed','com.nordstrom.event.customer.ProductSummaryCollectionViewed','com.nordstrom.event.customer.ProductSummarySelected')  
		and activity_date_partition >= date('2023-03-02') 
		and experience in ('DESKTOP_WEB', 'MOBILE_WEB')) 
	or (activity_date_partition < date'2023-03-02')
	or (experience in ('IOS_APP', 'ANDROID_APP')))
union all 
(select
	fiscal_year_num,
	fiscal_quarter_num, 
	fiscal_month_num, 
	fiscal_week_num,
	event_time_pst,
	channel,
	experience,
	channelcountry,
	session_page_rank_id,
	session_id,
	event_id,
	event_name,
	loyalty_flag,
	pageinstance_id,
	context_pagetype,
	page_indicator,
	page_rank,
	page_location,
	case 
		when context_pagetype in ('BRAND_RESULTS', 'CATEGORIES', 'CATEGORY_RESULTS', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS') and page_indicator is null then 1 
		else 0 
	end as content_browse_page_flag,
	prior_context_pagetype,
	prior_pageinstance_id,
	next_pageinstance_id,
	next_context_pagetype,
	next_page_indicator,
	next_page_location,
	next_content_browse_page_flag,
	next_products_viewed,
	next_products_selected,
	next_click_occurred,
	landing_page_flag,
	exit_page_flag,
	element_id,
	sub_component,
	element_value,
	canvas_id,
	component_name,
	digitalcontents_type,
	mrkt_type,
	finance_rollup,
	finance_detail,
	activity_date_partition
from new_engaged
where activity_date_partition >= date('2023-03-02'))
;

insert
    OVERWRITE TABLE {hive_schema}.content_session_analytical PARTITION (activity_date_partition)
select
    *
from
    output;
