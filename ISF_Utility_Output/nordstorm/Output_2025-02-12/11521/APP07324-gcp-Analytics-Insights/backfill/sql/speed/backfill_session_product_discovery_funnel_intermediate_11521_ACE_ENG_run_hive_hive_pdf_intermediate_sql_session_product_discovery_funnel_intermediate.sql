-- Table Name:session_product_discovery_funnel
-- Team/Owner: Cassie Zhang
-- Date Created/Modified: created on 4/21/2023, last modified on 2/29/2024
-- What is the update cadence/lookback window: refresh daily and 3 days lookback window 

create table if not exists ace_etl.session_product_discovery_funnel_intermediate (
    channel string, 
    experience string, 
    channelcountry string, 
    session_id string, 
    shopper_id string,
	pageinstance_id string, 
	context_pagetype string, 
	page_indicator string, 
	page_rank bigint, 
	pageinstance_rank bigint, 
	first_rank_flag int, 
	page_source_flag string,
	prior_pageinstance_id string, 
	prior_context_pagetype string, 
	source_pageinstance_id string, 
	source_pagetype string, 
	source_click_event_id string, 
	source_click_event_name string, 
	source_click_feature string, 
	source_click_action string, 
	source_click_element_id string, 
	source_click_element_type string,
	arrival_rank bigint, 
	mrkt_type string, 
	finance_rollup string, 
	finance_detail string,
	SBN_value string, 
	SBN_filter_key string, 
	SBN_sortorder string, 
	SBN_totalresultcount bigint,
	pdp_productstyle_id string, 
	pdp_reviewcount bigint,
	pdp_avg_rating decimal(36, 6), 
	pdp_isavailable int, 
	pdp_percentoff int, 
	pdp_max_current_price decimal(36, 6), 
	pdp_min_current_price decimal(36, 6), 
	pdp_original_price decimal(36, 6),
	event_time_pst timestamp, 
	event_id string, 
	event_name string, 
	product_action string, 
	action_feature string,
	element_id string, 
	element_value string,
	element_subid string,
	product_style_id string,
	product_rmssku_id string,
	sponsor_flag int,
	product_index bigint,
	product_index_on_site bigint,
	looks_seed_style_id string,
	looks_curation_typeid string,
	orderlineid string, 
	orderlinenumber string, 
	event_regular_price decimal(36, 6), 
	event_current_price decimal(36, 6), 
	orderline_discount_dollar decimal(36, 6),
	prod_avg_reviewcount decimal(36, 6),
	prod_avg_rating decimal(36, 6), 
	prod_isavailable int, 
	prod_max_percentoff decimal(36, 6), 
	prod_max_price decimal(36, 6), 
	prod_min_price decimal(36, 6), 
	prod_original_price decimal(36, 6),
	rmsstylegroupid string, 
	epmsytleid string,
	brandid string, 
	brandname string,
	department string, 
	department_num string, 
	class string, 
	class_num string, 
	subclass string, 
	subclass_num string,
	product_type1 string, 
	product_type2 string, 
	gender string, 
	agegroup string,
	activity_date_partition date
)
using ORC
location 's3://ace-etl/session_product_discovery_funnel_intermediate' 
partitioned by (activity_date_partition);


create or replace temp view session_raw as 

select 
	case when page_rank_rk = 1 then 1 else 0 end as first_rank_flag,
	case when page_rank = 1 then 'Landing Page'
		when page_rank_rk = 1 and arrival_rank is not null  then 'External'
		else 'Internal'
	end as page_source_flag,
	*
from
	(select dense_rank() over(partition by session_id, pageinstance_id order by page_rank) as page_rank_rk, *
	from acp_event_intermediate.session_evt_expanded_attributes_parquet 
	where activity_date_partition between date'2023-05-14' and date'2023-05-18'
	)
;


create or replace temp view session_base as 

select *
from session_raw
where event_name != 'com.nordstrom.event.customer.Impressed'
	and 
	(
		(event_name in ('com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected', 'com.nordstrom.customer.ProductDetailViewed') and productstyle_id is not null)
	or 
		(event_name = 'com.nordstrom.customer.AddedToBag' and atb_style_id is not null and atb_flag = 1)
	or 
		(event_name = 'com.nordstrom.customer.OrderSubmitted' and order_style_id is not null)
	or 
		(event_name = 'com.nordstrom.event.customer.Engaged' and (digitalcontents_id is not null and digitalcontents_type in ('STYLE','RMSSKU')))
	)
;





CREATE or replace temp view session_base_event as 

select activity_date_partition, session_id, cust_id as shopper_id, channel, experience, channelcountry, event_time_pst, event_id, event_name, 
	pageinstance_id, context_pagetype, page_indicator, page_rank, first_rank_flag, page_source_flag,
	product_action, 
	action_feature, 
	element_id, 
	element_value,
	element_subid,
	product_style_id,
	product_rmssku_id,
	sponsor_flag,
	product_index,
	product_index_on_site,
	looks_seed_style_id,
	looks_curation_typeid,
	orderlineid, orderlinenumber, event_regular_price, event_current_price, order_line_discount_units
from
(select distinct activity_date_partition, session_id, cust_id, channel, experience, channelcountry, event_time_pst, event_id, event_name, 
	pageinstance_id, context_pagetype, page_indicator, page_rank, first_rank_flag, page_source_flag,
	case when event_name = 'com.nordstrom.event.customer.ProductSummaryCollectionViewed' then 'See Product'
		when event_name = 'com.nordstrom.event.customer.ProductSummarySelected' then 'Click on Product'
		when event_name = 'com.nordstrom.customer.ProductDetailViewed' then 'View Product Detail'
		when event_name = 'com.nordstrom.customer.AddedToBag' then 'Add To Bag'
		when event_name = 'com.nordstrom.customer.OrderSubmitted' then 'Order Item' 
		when event_name = 'com.nordstrom.event.customer.Engaged' and element_id in ('ProductDetail/Size', 'ProductDetail/SizeGroup', 'ProductDetail/Width', 'ProductDetail/Color', 'ProductDetail/ViewAllColors', 'ProductResults/Color',
																					'MiniPDP/Size', 'MiniPDP/SizeGroup','MiniPDP/Width', 'MiniPDP/Color') then 'SKU Selection Attemped'
		when event_name = 'com.nordstrom.event.customer.Engaged' and element_id in ('ProductDetail/Size/Select', 'ProductDetail/SizeGroup/Select', 'ProductDetail/Width/Select', 'ProductDetail/Color/Select', 'ProductDetail/ViewAllColors/Select', 'ProductDetail/ViewAllColors/Color/Select', 
																					'ProductResults/Color/Select', 'MiniPDP/Size/Select', 'MiniPDP/SizeGroup/Select','MiniPDP/Width/Select', 'MiniPDP/Color/Select') then 'SKU Selected'
		when event_name = 'com.nordstrom.event.customer.Engaged' and element_id like ('%AddToWishlist%') then 'Add To Wishlist'
		else 'Engaged'
	end as product_action,
	case when full_pdp_flag = 1 then 'Full PDP' else feature end as action_feature,
	element_id, element_value, element_subid,
	case when event_name in ('com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected','com.nordstrom.customer.ProductDetailViewed') then productstyle_id
		when event_name = 'com.nordstrom.customer.AddedToBag' then atb_style_id
		when event_name = 'com.nordstrom.customer.OrderSubmitted' then order_style_id
		when event_name = 'com.nordstrom.event.customer.Engaged' then max(case when digitalcontents_type = 'STYLE' then digitalcontents_id else null end) over(partition by session_id, pageinstance_id, event_id)
	end as product_style_id,
	case when event_name = 'com.nordstrom.customer.AddedToBag' then atb_rmssku_id
		when event_name = 'com.nordstrom.customer.OrderSubmitted' then order_rmssku_id
		when event_name = 'com.nordstrom.event.customer.Engaged' then max(case when digitalcontents_type = 'RMSSKU' then digitalcontents_id else null end) over(partition by session_id, pageinstance_id, event_id)
	end as product_rmssku_id,
	case when event_name in ('com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected') and element_subid = 'SponsoredAdResult' then 1 
		when event_name in ('com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected') and element_subid != 'SponsoredAdResult' then 0
		else null
	end as sponsor_flag,
	case when event_name in ('com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected') then element_index
	end as product_index,
	case when event_name in ('com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected') then element_subindex
	end as product_index_on_site,
	case when event_name in ('com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected') and upper(feature) in ('LOOKS')
		then max(case when digitalcontents_type = 'STYLE' then digitalcontents_id else null end) over(partition by session_id, pageinstance_id, event_id)
	end as looks_seed_style_id,
	case when event_name in ('com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected') and upper(feature) in ('LOOKS')
		then max(case when digitalcontents_type = 'CURATION_TYPE' then digitalcontents_id else null end) over(partition by session_id, pageinstance_id, event_id)
	end as looks_curation_typeid,
	coalesce(atb_regular_units, order_line_regular_units) event_regular_price,
	coalesce(atb_current_units, order_line_current_units) event_current_price,
	orderlineid, orderlinenumber, order_line_discount_units,
	pdv_productstyle_id, pdv_reviewcount, pdv_avg_rating, pdv_isavailable, pdv_percentoff, pdv_max_current_price, pdv_min_current_price, pdv_original_price
from session_base a
)
where product_action != 'Engaged'
;



create or replace temp view session_base_with_pdv as 

select a.*,
    b.reviewcount as prod_avg_reviewcount,
    b.avg_rating as prod_avg_rating,
    b.isavailable as prod_isavailable,
    b.percentoff as prod_max_percentoff,
    b.max_current_price as prod_max_price,
    b.min_current_price as prod_min_price,
    b.original_price as prod_original_price
from session_base_event a
left join
	(select activity_date_partition, session_id, event_id, 
			reviewcount, avg_rating, isavailable, percentoff, max_current_price, min_current_price, original_price 
	from acp_event_intermediate.product_detail_viewed_event_attributes_parquet 
	where activity_date_partition between date'2023-05-14' and date'2023-05-18'
	) b
on a.activity_date_partition  = b.activity_date_partition
	and a.session_id = b.session_id
	and a.event_id = b.event_id
;



create or replace temp view product_funnel_events as 

select a.activity_date_partition, channel, experience, channelcountry, a.session_id, shopper_id, 
	a.pageinstance_id, context_pagetype, page_indicator, a.page_rank, pageinstance_rank, first_rank_flag, page_source_flag,
	prior_pageinstance_id, prior_context_pagetype, 
	source_pageinstance_id, source_pagetype, 
	source_click_event_id, source_click_event_name, source_click_feature, source_click_action, source_click_element_id, source_click_element_type,
	arrival_rank, mrkt_type, finance_rollup, finance_detail,
	search_browse_value as SBN_value, search_browse_filters_key as SBN_filter_key, search_browse_sortorder as SBN_sortorder, search_browse_totalresultcount as SBN_totalresultcount,
	pdv_productstyle_id as pdp_productstyle_id, pdv_reviewcount as pdp_reviewcount, pdv_avg_rating as pdp_avg_rating, pdv_isavailable as pdp_isavailable, pdv_percentoff as pdp_percentoff, 
	pdv_max_current_price as pdp_max_current_price, pdv_min_current_price as pdp_min_current_price, pdv_original_price as pdp_original_price,
	event_time_pst, event_id, event_name, 
	product_action, 
	action_feature,
	element_id, 
	element_value,
	element_subid,
	product_style_id,
	product_rmssku_id,
	sponsor_flag,
	product_index,
	product_index_on_site,
	looks_seed_style_id,
	looks_curation_typeid,
	orderlineid, orderlinenumber, event_regular_price, event_current_price, order_line_discount_units,
	prod_avg_reviewcount, prod_avg_rating, prod_isavailable, prod_max_percentoff, prod_max_price, prod_min_price, prod_original_price
from session_base_with_pdv a
left join
(select activity_date_partition, session_id, pageinstance_id, page_rank, pageinstance_rank,
		prior_pageinstance_id, prior_context_pagetype, 
		source_pageinstance_id, source_pagetype, 
		arrival_rank, mrkt_type, finance_rollup, finance_detail,
		search_browse_value, search_browse_filters_key, search_browse_sortorder, search_browse_totalresultcount,
		pdv_productstyle_id, pdv_reviewcount, pdv_avg_rating, pdv_isavailable, pdv_percentoff, pdv_max_current_price, pdv_min_current_price, pdv_original_price,
		last_click_event_id,  
		min_by(source_click_event_id, page_rank) over(partition by session_id, pageinstance_id) as source_click_event_id,
		last_click_event_name,  
		min_by(source_click_event_name, page_rank) over(partition by session_id, pageinstance_id) as source_click_event_name,
		last_click_feature,  
		min_by(source_click_feature, page_rank) over(partition by session_id, pageinstance_id) as source_click_feature,
		last_click_action,  
		min_by(source_click_action, page_rank) over(partition by session_id, pageinstance_id) as source_click_action,
		last_click_element_id,  
		min_by(source_click_element_id, page_rank) over(partition by session_id, pageinstance_id) as source_click_element_id,
		last_click_element_type,  
		min_by(source_click_element_type, page_rank) over(partition by session_id, pageinstance_id) as source_click_element_type
from
	(SELECT distinct activity_date_partition, session_id, pageinstance_id, page_rank, pageinstance_rank, 
			prior_pageinstance_id, prior_context_pagetype, source_pageinstance_id, source_pagetype, arrival_rank, mrkt_type, finance_rollup, finance_detail, 
			search_browse_value, search_browse_filters_key, search_browse_sortorder, search_browse_totalresultcount,
			pdv_productstyle_id, pdv_reviewcount, pdv_avg_rating, pdv_isavailable, pdv_percentoff, pdv_max_current_price, pdv_min_current_price, pdv_original_price,
			last_click_event_id,  last_value(last_click_event_id) over(partition by session_id order by page_rank rows between unbounded preceding  AND 1 preceding) as source_click_event_id,
			last_click_event_name, last_value(last_click_event_name) over(partition by session_id order by page_rank rows between unbounded preceding  AND 1 preceding) as source_click_event_name,
			last_click_feature, last_value(last_click_feature) over(partition by session_id order by page_rank rows between unbounded preceding  AND 1 preceding) as source_click_feature,
			last_click_action, last_value(last_click_action) over(partition by session_id order by page_rank rows between unbounded preceding  AND 1 preceding) as source_click_action,
			last_click_element_id, last_value(last_click_element_id) over(partition by session_id order by page_rank rows between unbounded preceding  AND 1 preceding) as source_click_element_id,
			last_click_element_type, last_value(last_click_element_type) over(partition by session_id order by page_rank rows between unbounded preceding  AND 1 preceding) as source_click_element_type
    from acp_event_intermediate.session_page_attributes_parquet
	where activity_date_partition between date'2023-05-14' and date'2023-05-18'
    )
) b
on a.activity_date_partition  = b.activity_date_partition
	and a.session_id = b.session_id
	and a.page_rank = b.page_rank
;





/*
with find_single_epmstyleid as (
select webstyleid, marketcode, parentepmstyleid, rmsstylegroupid, brandlabelid, brandlabeldisplayname, sourcepublishtimestamp 
from
    (select *,
        row_number() over(partition by webstyleid, marketcode order by sourcepublishtimestamp desc, parentepmstyleid, rmsstylegroupid, brandlabelid, brandlabeldisplayname) rk 
    from
    	(select distinct webstyleid, marketcode, rmsstylegroupid, parentepmstyleid, brandlabelid, brandlabeldisplayname, 
    	        from_utc_timestamp(sourcepublishtimestamp,'PST') as sourcepublishtimestamp
    	from nap_product.sku_digital_webstyleid_bucketed
		where webstyleid is not null
			and cast(from_utc_timestamp(sourcepublishtimestamp,'PST') as date) <= date'2023-05-14'
		)
    )
where rk = 1
),



find_single_attribute as (
select marketcode, empstyleid, departmentdescription, departmentnumber, subclassdescription, subclassnumber, classdescription, classnumber, typelevel1description, typelevel2description, gender, agegroup, sourcepublishtimestamp
from 
	(select *,
		row_number() over(partition by empstyleid, marketcode order by sourcepublishtimestamp desc, departmentnumber, classnumber, typelevel1description, typelevel2description, gender, agegroup) rk
	from
		(select distinct marketcode, empstyleid, 
				genderdescription as gender, 
				agedescription as agegroup, 
				departmentdescription, departmentnumber, subclassdescription, subclassnumber, classdescription, classnumber, typelevel1description, typelevel2description, 
				from_utc_timestamp(sourcepublishtimestamp,'PST') as sourcepublishtimestamp
		from nap_product.style_digital_epmstyleid_bucketed
		where empstyleid is not null
			and cast(from_utc_timestamp(sourcepublishtimestamp,'PST') as date)  <= date'2023-05-14'
		)
	)
where rk = 1
)    
--join conditions
left join find_single_epmstyleid b 
on a.product_style_id = b.webstyleid
	and a.channelcountry = b.marketcode
left join find_single_attribute c
on b.parentepmstyleid = c.empstyleid 
	and b.marketcode = c.marketcode
*/

create or replace temp view final_view as 

select channel, experience, a.channelcountry, session_id, shopper_id, 
	pageinstance_id, context_pagetype, page_indicator, page_rank, pageinstance_rank, first_rank_flag, page_source_flag,
	prior_pageinstance_id, prior_context_pagetype, 
	source_pageinstance_id, source_pagetype, 
	source_click_event_id, source_click_event_name, source_click_feature, source_click_action, source_click_element_id, source_click_element_type,
	arrival_rank, mrkt_type, finance_rollup, finance_detail,
	SBN_value, SBN_filter_key, SBN_sortorder, SBN_totalresultcount,
	pdp_productstyle_id, pdp_reviewcount, pdp_avg_rating, pdp_isavailable, pdp_percentoff, pdp_max_current_price, pdp_min_current_price, pdp_original_price,
	event_time_pst, event_id, event_name, 
	product_action, 
	upper(action_feature) as action_feature,
	element_id, 
	element_value,
	element_subid,
	product_style_id,
	product_rmssku_id,
	sponsor_flag,
	product_index,
	product_index_on_site,
	looks_seed_style_id,
	looks_curation_typeid,
	orderlineid, orderlinenumber, event_regular_price, event_current_price, order_line_discount_units as orderline_discount_dollar,
	coalesce(prod_avg_reviewcount, d.avg_reviewcount) as prod_avg_reviewcount,
	coalesce(prod_avg_rating, d.avg_rating) as prod_avg_rating, 
	coalesce(prod_isavailable, d.in_stock) as prod_isavailable, 
	coalesce(prod_max_percentoff, d.max_percentoff) as prod_max_percentoff, 
	coalesce(prod_max_price, d.max_price) as prod_max_price, 
	coalesce(prod_min_price, d.min_price) as prod_min_price, 
	coalesce(prod_original_price, d.original_price) as prod_original_price,
	null as rmsstylegroupid, 
	null as epmsytleid, 
	null as brandid, 
	null as brandname,
	null as department, 
    null as department_num, 
	null as class, 
    null as class_num, 
	null as subclass, 
    null as subclass_num,
	null as product_type1, 
	null as product_type2, 
	null as ender, 
	null as agegroup,
	a.activity_date_partition
from product_funnel_events a
left join
	(select activity_date_partition, styleid, channelcountry, 
			avg(reviewcount) as avg_reviewcount, avg(avg_rating) as avg_rating, min(isavailable) as in_stock, max(percentoff) max_percentoff, 
			max(max_current_price) as max_price, min(min_current_price) as min_price, max(original_price) as original_price
	from acp_event_intermediate.product_detail_viewed_event_attributes_parquet 
	where activity_date_partition between date'2023-05-14' and date'2023-05-18'
	group by 1,2,3
	) d
on a.product_style_id = d.styleid
	and a.channelcountry = d.channelcountry
	and a.activity_date_partition = d.activity_date_partition
;


insert OVERWRITE TABLE ace_etl.session_product_discovery_funnel_intermediate PARTITION (activity_date_partition) 
select *
from final_view;

MSCK REPAIR table ace_etl.session_product_discovery_funnel_intermediate;  