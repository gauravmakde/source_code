-- Table Name:session_product_discovery_funnel_analytical
-- Team/Owner: Digital Product Analytics/Cassie Zhang
-- Date Created/Modified: created on 8/29/2023, last modified on 3/14/2024
-- What is the update cadence/lookback window: refresh daily and 4 days lookback window 
 
--DROP TABLE IF EXISTS {hive_schema}.session_product_discovery_funnel_analytical;
create table if not exists {hive_schema}.session_product_discovery_funnel_analytical (
    channel string, 
    platform string, 
    channelcountry string, 
    session_id string,
    product_style_id bigint, 
    style_group_id string, 
	style_group_desc string, 
    first_full_pdv_flag string,
    discovery_source string, 
    brand string,
    division string,
    subdivision string,
    department string, 
    class string, 
    subclass string, 
    product_type1 string, 
    product_type2 string, 
    merch_themes string,
    quantrix_category string,
    prod_min_price decimal(36, 6), 
    prod_original_price decimal(36, 6), 
    price_range string, 
    mrkt_type string, 
    finance_rollup string,
    finance_detail string, 
    pdv_source_flag string, 
    pdv_source_lp_flag int, 
    source_pageid string,
    source_pagetype string, 
    source_page_sbn_value string, 
    imp_source_flag string, 
    imp_lp_flag int, 
    imp_source_action_feature string, 
    imp_source_feature string, 
    imp_source_pageid string, 
    imp_source_pagetype string, 
    imp_source_sbn_value string, 
    imp_position bigint, 
    imp_position_site bigint,
    imp_final_flag int,
    click_final_flag int,
    full_pdv_flag int, 
    first_full_pdv_instock_flag int, 
    sku_flag int, 
    atwl_flag int,
    atb_flag int,
    ttl_atb bigint, 
    ttl_atb_value decimal(36, 6),
    ttl_atb_demand decimal(36, 6), 
    order_flag int,
    ttl_order_qty bigint, 
    ttl_ord_value decimal(36, 6), 
    ttl_ord_demand decimal(36, 6),
    activity_date_partition date
) using ORC location 's3://{s3_bucket_root_var}/session_product_discovery_funnel_analytical' partitioned by (activity_date_partition)
;

create or replace temp view prep as 

select activity_date_partition, channel, experience, channelcountry, session_id, 
		product_style_id, rmsstylegroupid, pageinstance_id, page_indicator, context_pagetype, page_rank, pageinstance_rank, 
		source_pageinstance_id, source_pagetype,
		arrival_rank, mrkt_type, finance_rollup, finance_detail, 
		sbn_value,
		event_time_pst, product_action, action_feature, event_id, element_id, element_subid, product_index,product_index_on_site,
		orderlineid, event_regular_price, event_current_price, 
		prod_isavailable,
		max(case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' then 1 else 0 end) over(partition by session_id, pageinstance_id, product_style_id) as full_pdv_page_flag,
		min(case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' then event_time_pst else null end) over(partition by session_id, product_style_id) first_full_PDV_time,
		min_by(pageinstance_id, case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' then page_rank else null end) over(partition by session_id, product_style_id) first_full_PDV_pageid
from ace_etl.session_product_discovery_funnel_intermediate 
where activity_date_partition between {start_date} and {end_date}
	and (product_action != 'View Product Detail' or (product_action = 'View Product Detail' and action_feature != 'FULL PDP') or (product_action = 'View Product Detail' and action_feature = 'FULL PDP' and page_indicator = 'PRODUCT_DETAIL'))
;


create or replace temp view pdf_prep as 

select distinct 
		a.activity_date_partition, a.channel, a.experience, a.channelcountry, a.session_id, 
		a.product_style_id, a.rmsstylegroupid, pageinstance_id, page_indicator, context_pagetype, page_rank, pageinstance_rank, 
		case when mrkt_type is null then source_pageinstance_id else null end as source_pageinstance_id, 
		case when mrkt_type is null then source_pagetype else null end as source_pagetype, 
		arrival_rank, mrkt_type, finance_rollup, finance_detail, 
		sbn_value,
		event_time_pst, product_action, action_feature, event_id, element_id, element_subid, product_index,product_index_on_site,
		a.prod_isavailable as pdv_instock, orderlineid, event_regular_price, event_current_price, 
		coalesce(
		max(case when first_full_pdv_pageid = pageinstance_id and pageinstance_rank = 1 then 'Entry Page'
				when first_full_pdv_pageid = pageinstance_id and mrkt_type is not null then 'External Landing'
				when first_full_pdv_pageid = pageinstance_id and mrkt_type is null then 'Internal Discovery'
				else null
			end) 
		over(partition by session_id, product_style_id),
		'No full PDV' )as first_full_pdv_flag, 
		case when pageinstance_id = first_full_PDV_pageid then 1 
			when full_pdv_page_flag = 1 then 0
			else null
		end as first_full_PDV_pageid, 
		case when event_time_pst <= first_full_PDV_time then 1 when event_time_pst > first_full_PDV_time then 0 end as before_first_full_PDV,
        first_full_PDV_time, 
        first_full_PDV_time - event_time_pst as timediff_first_PDV, 
		c.prod_min_price, c.prod_original_price
from prep a
left join 
	(select activity_date_partition, styleid, channelcountry, 
			min(min_current_price) as prod_min_price, max(original_price) as prod_original_price
	from acp_event_intermediate.product_detail_viewed_event_attributes_parquet 
	where activity_date_partition between {start_date} and {end_date}
	group by 1,2,3
	) c
on a.product_style_id = c.styleid
	and a.channelcountry = c.channelcountry
	and a.activity_date_partition = c.activity_date_partition
;
	
	
	
	
	
create or replace temp view first_PDV as 

select distinct a.activity_date_partition, a.session_id, a.product_style_id, rmsstylegroupid,
		first_full_pdv_flag,
		case when first_full_pdv_flag in ('Entry Page', 'External Landing' ) then 'External' else 'Internal' end as discovery_source,
		mrkt_type, 
		finance_rollup, 
		finance_detail,
		source_flag as pdv_source_flag, 
		case when first_full_pdv_flag in ('Entry Page', 'External Landing' ) then null
			when source_flag = 'Internal' then 0 
			else 1 
		end as pdv_source_LP_flag, 
		source_pageinstance_id as source_pageid, 
		case when pageinstance_rank = 1 then null
			when mrkt_type is null and page_indicator = 'PRODUCT_DETAIL' and source_pagetype in ('BRAND_RESULTS', 'CATEGORIES', 'CATEGORY_RESULTS', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS') then 'BROWSE' 
			when mrkt_type is null and page_indicator = 'PRODUCT_DETAIL' and source_pagetype in ('SEARCH_RESULTS','PRODUCT_DETAIL','SHOPPING_BAG','SEARCH','HOME','WISH_LIST','PURCHASE_HISTORY') then source_pagetype 
			when mrkt_type is null and page_indicator = 'PRODUCT_DETAIL'  then 'Other'
			when mrkt_type is null then 'Not on PDP'
			else null 
		end as source_pagetype, 
		case when pageinstance_rank = 1 then null
			when mrkt_type is null and page_indicator = 'PRODUCT_DETAIL' and source_pagetype in ('BRAND_RESULTS', 'CATEGORIES', 'CATEGORY_RESULTS', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS')
				then (case when substring(b.search_browse_value, 1,7) = '/browse' then substring(b.search_browse_value, 9) 
							when substring(b.search_browse_value, 1,1) = '/' then substring(b.search_browse_value, 2) 
							else b.search_browse_value 
						end) 
			when mrkt_type is null and page_indicator = 'PRODUCT_DETAIL' then 'no Browse URL'
			when mrkt_type is null then 'Not on PDP'
			else null 
		end as source_page_sbn_value
from pdf_prep a
left join 
	(SELECT distinct activity_date_partition, session_id, pageinstance_id, search_browse_value, 
			case when pageinstance_rank = 1 then 'Entry Page' 
				when mrkt_type is not null then 'External' 
				else 'Internal' 
			end as source_flag
	from acp_event_intermediate.session_page_attributes_parquet
	where activity_date_partition between {start_date} and {end_date}
	) b
on a.activity_date_partition = b.activity_date_partition
	and a.session_id = b.session_id
	and a.source_pageinstance_id = b.pageinstance_id
where  a.product_action = 'View Product Detail'
	and a.action_feature = 'FULL PDP'
	and a.page_indicator = 'PRODUCT_DETAIL'
	and a.first_full_pdv_pageid = 1
;

	
	


create or replace temp view imp_source as 

select activity_date_partition, session_id, product_style_id, rmsstylegroupid, first_full_pdv_flag, product_action,
		imp_source_flag, case when imp_source_flag = 'Internal' then 0 else 1 end as imp_LP_flag, 
		imp_source_action_feature, imp_source_feature, imp_source_pageid, imp_source_pagetype, imp_source_sbn_value, imp_position, imp_position_site
from 
	(select activity_date_partition, session_id, product_style_id, rmsstylegroupid, first_full_pdv_flag, product_action, 
			case when pageinstance_rank = 1 then 'Entry Page' 
				when mrkt_type is not null then 'External' 
				else 'Internal' 
			end as imp_source_flag, 
			action_feature as imp_source_action_feature, 
			case when action_feature = 'SEARCH_BROWSE' and element_subid = 'KeywordSearchResult' then 'SBN - SEARCH' 
				when action_feature = 'SEARCH_BROWSE' and element_subid = 'BrowseSearchResult' then 'SBN - BROWSE' 
				when action_feature = 'SEARCH_BROWSE' and element_subid = 'SponsoredAdResult' then 'SBN - SPONSOR_AD' 
				when action_feature = 'SEARCH_BROWSE' and element_subid = 'PinnedResult' then 'SBN - PINNED' 
				when action_feature = 'SEARCH_BROWSE' then 'SBN - UNKNOWN'
				else action_feature
			end as imp_source_feature, 
			pageinstance_id as imp_source_pageid,
			context_pagetype as imp_source_pagetype,
			case when page_indicator = 'BROWSE' then 
							(case when substring(sbn_value, 1,7) = '/browse' then substring(sbn_value, 9) 
								when substring(sbn_value, 1,1) = '/' then substring(sbn_value, 2) 
								else sbn_value 
							end) 
				else null 
			end as imp_source_sbn_value,
			product_index as imp_position, product_index_on_site as imp_position_site,
			row_number() over(partition by activity_date_partition, session_id, product_style_id order by timediff_first_PDV, event_id) as rn_internal,
			row_number() over(partition by activity_date_partition, session_id, product_style_id order by event_time_pst, event_id) as rn_no_PDV
	from pdf_prep
	where product_action = 'See Product'
		and first_full_pdv_flag not in ('Entry Page', 'External Landing' )
		and ((first_full_pdv_flag = 'Internal Discovery' and before_first_full_PDV = 1) or (first_full_pdv_flag = 'No full PDV'))
	) as imp_prep
where (first_full_pdv_flag = 'Internal Discovery' and rn_internal = 1 ) or (first_full_pdv_flag = 'No full PDV' and rn_no_PDV = 1)
;




create or replace temp view down_funnel_metrics as 

select *,
		case when first_full_pdv_flag = 'Internal Discovery' then imp_before_first_pdv_flag
			when first_full_pdv_flag = 'No full PDV' then imp_flag
			else 0
		end as imp_final_flag,
		case when first_full_pdv_flag = 'Internal Discovery' then click_before_first_pdv_flag
			when first_full_pdv_flag = 'No full PDV' then click_flag
			else 0
		end as click_final_flag,
		case when channel = 'NORDSTROM' and prod_original_price >=0 and prod_original_price <50 then '$0~50'
			when channel = 'NORDSTROM' and prod_original_price >=50 and prod_original_price <100 then '$50~100'
			when channel = 'NORDSTROM' and prod_original_price >=100 and prod_original_price <200 then '$100~200'
			when channel = 'NORDSTROM' and prod_original_price >=200 and prod_original_price <500 then '$200~500'
			when channel = 'NORDSTROM' and prod_original_price >=500 and prod_original_price <1000 then '$500~1000'
			when channel = 'NORDSTROM' and prod_original_price >=1000 then '$1000+'
			when channel = 'NORDSTROM_RACK' and prod_original_price >=0 and prod_original_price <25 then '$0~25'
			when channel = 'NORDSTROM_RACK' and prod_original_price >=25 and prod_original_price <50 then '$25~50'
			when channel = 'NORDSTROM_RACK' and prod_original_price >=50 and prod_original_price <75 then '$50~75'
			when channel = 'NORDSTROM_RACK' and prod_original_price >=75 and prod_original_price <100 then '$75~100'
			when channel = 'NORDSTROM_RACK' and prod_original_price >=100 and prod_original_price <150 then '$100~150'
			when channel = 'NORDSTROM_RACK' and prod_original_price >=150 and prod_original_price <200 then '$150~200'
			when channel = 'NORDSTROM_RACK' and prod_original_price >=200 and prod_original_price <400 then '$200~400'
			when channel = 'NORDSTROM_RACK' and prod_original_price >=400 then '$400+'
		else 'Unknown'
		end as price_range
from
	(select activity_date_partition, channel, experience, channelcountry, session_id, product_style_id, rmsstylegroupid,
			first_full_pdv_flag, 
			case when first_full_pdv_flag in ('Entry Page', 'External Landing' ) then 'External' else 'Internal' end as Discovery_source,
			prod_min_price, 
			prod_original_price, 
			max(case when product_action = 'See Product' then 1 else 0 end) as imp_flag,  
			max(case when product_action = 'See Product' and before_first_full_PDV = 1 then 1 else 0 end) as imp_before_first_PDV_flag,  
			max(case when product_action = 'Click on Product' then 1 else 0 end) as click_flag,
			max(case when product_action = 'Click on Product' and before_first_full_PDV = 1 then 1 else 0 end) as click_before_first_PDV_flag,
			max(case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' then 1 else 0 end) as full_pdv_flag,
			max(case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' and first_full_PDV_pageid = 1 and pdv_instock = 1 then 1 else 0 end) first_full_PDV_instock_flag,
			max(case when product_action = 'SKU Selected' then 1 else 0 end) as SKU_flag,
			max(case when product_action = 'Add To Wishlist' then 1 else 0 end) as ATWL_flag,
			max(case when product_action = 'Add To Bag' then 1 else 0 end) as ATB_flag,
			count(distinct case when product_action = 'Add To Bag' then event_id else null end) ttl_ATB, 
			sum(case when product_action = 'Add To Bag' then event_regular_price else 0 end) as ttl_atb_value, 
			sum(case when product_action = 'Add To Bag' then event_current_price else 0 end) as ttl_atb_demand,
			max(case when product_action = 'Order Item' then 1 else 0 end) as Order_flag,
			count(distinct orderlineid) ttl_order_qty, 
			sum(case when product_action = 'Order Item' then event_regular_price else 0 end) as ttl_ord_value, 
			sum(case when product_action = 'Order Item' then event_current_price else 0 end) as ttl_ord_demand
	from pdf_prep
	group by 1,2,3,4,5,6,7,8,9,10,11
	)
;


create or replace temp view style_merch_hierarchy as 

select day_date, 
		cast(web_style_num as varchar(20)) as web_style_num, 
		selling_channel, 
		cast(style_group_num as varchar(20)) as style_group_id,
		upper(style_group_short_desc) as style_group_desc,
		upper(brand_name) as brand,
		upper(case when concat(cast(div_num as varchar(20)),': ', div_desc) = ': ' then null else concat(cast(div_num as varchar(20)),': ', div_desc) end) as division,
		upper(case when concat(cast(grp_num as varchar(20)),': ', grp_desc) = ': ' then null else concat(cast(grp_num as varchar(20)),': ', grp_desc) end) as subdivision,
		upper(case when concat(cast(dept_num as varchar(20)),': ', dept_desc) = ': ' then null else concat(cast(dept_num as varchar(20)),': ', dept_desc) end) as department,
		upper(case when concat(cast(class_num as varchar(20)),': ', class_desc) = ': ' then null else concat(cast(class_num as varchar(20)),': ', class_desc) end) as class,
		upper(case when concat(cast(sbclass_num as varchar(20)),': ', sbclass_desc) = ': ' then null else concat(cast(sbclass_num as varchar(20)),': ', sbclass_desc) end) as subclass,
		upper(case when concat(cast(type_level_1_num as varchar(20)),': ', type_level_1_desc) = ': ' then null else concat(cast(type_level_1_num as varchar(20)),': ', type_level_1_desc) end) as product_type1,
		upper(case when concat(cast(type_level_2_num as varchar(20)),': ', type_level_2_desc) = ': ' then null else concat(cast(type_level_2_num as varchar(20)),': ', type_level_2_desc) end) as product_type2,
		upper(MERCH_THEMES) as merch_themes,
		upper(QUANTRIX_CATEGORY) as quantrix_category
from ace_etl.digital_merch_table
where day_date between {start_date} and {end_date}
;



create or replace temp view final_view as 

select a.channel, 
		a.experience as platform, 
		a.channelcountry, 
		a.session_id, 
		cast(a.product_style_id as bigint) as product_style_id, 
		smh.style_group_id, 
		smh.style_group_desc,
		a.first_full_pdv_flag, 
		a.discovery_source,
		smh.brand,
		smh.division, 
		smh.subdivision, 
		smh.department,
		smh.class,
		smh.subclass,
		smh.product_type1,
		smh.product_type2,
		smh.merch_themes,
		smh.quantrix_category,
		a.prod_min_price,
		a.prod_original_price,
		a.price_range,
		b.mrkt_type, 
		b.finance_rollup, 
		b.finance_detail,
		b.pdv_source_flag,
		b.pdv_source_LP_flag,
		b.source_pageid, 
		b.source_pagetype, 
		b.source_page_sbn_value,
		c.imp_source_flag, 
		c.imp_LP_flag, 
		c.imp_source_action_feature, 
		c.imp_source_feature, 
		c.imp_source_pageid,
		c.imp_source_pagetype, 
		c.imp_source_sbn_value, 
		c.imp_position, 
		c.imp_position_site, 
		a.imp_final_flag,
		a.click_final_flag,
		a.full_pdv_flag,
		a.first_full_PDV_instock_flag,
		a.SKU_flag,
		a.ATWL_flag,
		a.ATB_flag,
		a.ttl_ATB,
		a.ttl_atb_value, 
		a.ttl_atb_demand,
		a.Order_flag,
		a.ttl_order_qty, 
		a.ttl_ord_value, 
		a.ttl_ord_demand,
		a.activity_date_partition
from down_funnel_metrics a
left join first_PDV b 
on a.activity_date_partition = b.activity_date_partition
	and a.session_id = b.session_id 
	and a.product_style_id = b.product_style_id
left join imp_source c
on a.activity_date_partition = c.activity_date_partition
	and a.session_id = c.session_id 
	and a.product_style_id = c.product_style_id
left join style_merch_hierarchy smh
on a.activity_date_partition = smh.day_date
	and a.channel = smh.selling_channel
	and a.product_style_id = smh.web_style_num
order by a.session_id, a.product_style_id
;


insert OVERWRITE TABLE {hive_schema}.session_product_discovery_funnel_analytical PARTITION (activity_date_partition) 
select *
from final_view
;

MSCK REPAIR table {hive_schema}.session_product_discovery_funnel_analytical
;









