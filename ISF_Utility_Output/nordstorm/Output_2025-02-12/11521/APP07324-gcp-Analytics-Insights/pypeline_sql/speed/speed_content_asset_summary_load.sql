
create or replace temporary view asset_agg as  
(
select 
	fiscal_year_num, 
	fiscal_quarter_num, 
	fiscal_month_num, 
	fiscal_week_num,
	channel,
	experience,
	channelcountry,
	loyalty_flag,
	context_pagetype,
	case
		when page_indicator = 'BROWSE' then 'Browse Results Page'
		when content_browse_page_flag = 1 then 'Content Browse Page'
		else context_pagetype
	end as page_level,
	case 
		when page_location is not null then regexp_replace(page_location, "\\r\\n|\\n|\\r", '')
		when content_browse_page_flag = 1 then 'Content Browse Page'
		else context_pagetype
	end as page_detail,
	case when (page_location like '%anniv%' or page_location = '/browse/nordy-club') then 1 else 0 end as anniversary_flag,
	landing_page_flag,
	exit_page_flag,
	sub_component,
	regexp_replace(element_value, "\\r\\n|\\n|\\r", '') as element_value,
	canvas_id,
	component_name,
	mrkt_type,
	finance_rollup,
	finance_detail,
	count(distinct case when event_name in ('com.nordstrom.event.customer.Impressed','com.nordstrom.event.customer.ProductSummaryCollectionViewed') then event_id end) as event_imp_count,
	count(distinct case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') then event_id end) as event_eng_count,
	count(distinct case when event_name in ('com.nordstrom.event.customer.Impressed','com.nordstrom.event.customer.ProductSummaryCollectionViewed') then session_id end) as session_imp_count,
    count(distinct case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') then session_id end) as session_eng_count,
	count(distinct case when event_name in ('com.nordstrom.event.customer.Impressed','com.nordstrom.event.customer.ProductSummaryCollectionViewed') then pageinstance_id end) as page_imp_count,
    count(distinct case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') then pageinstance_id end) as page_eng_count,
	case
	    when (sum(case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') then next_products_viewed end)) is null then 0
	    else sum(case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') then next_products_viewed end)
	end as next_products_viewed,
	case
	    when (sum(case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') then next_products_selected end)) is null then 0
	    else sum(case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') then next_products_selected end)
	end as next_products_selected,
	count(distinct case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') and next_products_viewed > 0 and next_products_selected > 0 then session_id end) as next_page_sessions_selected_product,
	count(distinct case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected')and next_products_viewed > 0 and next_products_selected = 0 then session_id end) as next_page_sessions_veiwed_product,
	count(distinct case when event_name in ('com.nordstrom.event.customer.Engaged', 'com.nordstrom.event.customer.ProductSummarySelected') and next_click_occurred > 0 then session_id end) as next_page_sessions_click_occurred,
	activity_date_partition
from  ace_etl.content_session_analytical
where activity_date_partition between {start_date} 
    and {end_date}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,33
);

insert overwrite table speed_content_asset_summary_ldg_output
select * 
from asset_agg
where char_length(canvas_id) <= 100
and char_length(page_detail) <= 700
;
