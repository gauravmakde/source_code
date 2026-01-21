-- Table Name:product_discovery_funnel_daily_Ncom
-- Team/Owner: Digital Product Analytics/Cassie Zhang
-- Date Created/Modified: created on 8/29/2023, last modified on 3/14/2024
-- What is the update cadence/lookback window: refresh daily and 4 days lookback window 

--DROP TABLE IF EXISTS {hive_schema}.product_discovery_funnel_daily_Ncom;
create table if not exists {hive_schema}.product_discovery_funnel_daily_Ncom (
	fiscal_year_num int, 
	quarter_abrv string, 
	fiscal_month_num int, 
	month_abrv string, 
	week_num_of_fiscal_month int, 
	fiscal_week_num int, 
	week_start_day_date date,
	channel string, 
	platform string, 
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
	price_range string,
	product_style_id bigint, 
	style_group_id bigint,
	style_group_desc string,
	discovery_source string,
	mrkt_type string, 
	finance_rollup string, 
	source_pagetype string,
	source_page_sbn_value string,
	source_tier1 string,
	source_tier2 string,
	source_tier3 string, 
	source_tier4 string,
	source_action_feature string, 
	source_feature string, 
	imp_final_session int,
	imp_price_min decimal(36, 6),
	imp_price_ori decimal(36, 6),
	imp_position bigint,
	imp_position_site bigint,
	click_final_session int,
	click_price_min decimal(36, 6),
	click_price_ori decimal(36, 6),
	full_pdv_session int,
	pdv_price_min decimal(36, 6),
	pdv_price_ori decimal(36, 6),
	first_full_PDV_instock_session int,
	SKU_session int,
	sku_price_min decimal(36, 6),
	sku_price_ori decimal(36, 6),
	ATWL_session int,
	ATWL_price_min decimal(36, 6),
	ATWL_price_ori decimal(36, 6),
	ATB_session int,
	ttl_ATB bigint,
	ttl_atb_value decimal(36, 6), 
	ttl_atb_demand decimal(36, 6),
	Order_session int,
	ttl_order_qty bigint, 
	ttl_ord_value decimal(36, 6), 
	ttl_ord_demand decimal(36, 6),
	activity_date_partition date
) using ORC location 's3://{s3_bucket_root_var}/product_discovery_funnel_daily_Ncom' partitioned by (activity_date_partition)
;



/* 
Create daily table for Ncom
*/
create or replace temp view daily_Ncom as 

select cast(fiscal_year_num as int) as fiscal_year_num, 
	quarter_abrv, 
	cast(fiscal_month_num as int) as fiscal_month_num, 
	month_abrv, 
	cast(week_num_of_fiscal_month as int) as week_num_of_fiscal_month, 
	cast(fiscal_week_num as int) as fiscal_week_num, 
	to_date(regexp_replace(week_start_day_date, '/','-'), 'MM-dd-yyyy') as week_start_day_date, 
	channel, 
	platform, 
	brand, division, subdivision, department, class, subclass, product_type1, product_type2, merch_themes, quantrix_category,
	price_range,
	cast(product_style_id as bigint) as product_style_id, 
	cast(style_group_id as bigint) as style_group_id,
	style_group_desc,
	discovery_source,
	mrkt_type, 
	finance_rollup, 
	source_pagetype,
	source_page_sbn_value,
	split(source_page_sbn_value,'/') [0] as source_tier1,
	split(source_page_sbn_value,'/') [1] as source_tier2,
	case when split(source_page_sbn_value,'/') [0] = 'v2' and split(source_page_sbn_value,'/') [1] = 'brand' then replace(regexp_replace(split(source_page_sbn_value,'/') [2],'[0-9]+',''), '-', '') else split(source_page_sbn_value,'/') [2] end as source_tier3, 
	split(source_page_sbn_value,'/') [3] as source_tier4,
	source_action_feature, 
	source_feature, 
	imp_final_session,
	imp_price_min,
	imp_price_ori,
	imp_position,
	imp_position_site,
	click_final_session,
	click_price_min,
	click_price_ori,
	full_pdv_session,
	pdv_price_min,
	pdv_price_ori,
	first_full_PDV_instock_session,
	SKU_session,
	sku_price_min,
	sku_price_ori,
	ATWL_session,
	ATWL_price_min,
	ATWL_price_ori,
	ATB_session,
	ttl_ATB,
	ttl_atb_value, 
	ttl_atb_demand,
	Order_session,
	ttl_order_qty, 
	ttl_ord_value, 
	ttl_ord_demand,
	activity_date_partition
from
(select fiscal_year_num, quarter_abrv, fiscal_month_num, month_abrv, week_num_of_fiscal_month, fiscal_week_num, week_start_day_date, 
	activity_date_partition, 
	channel, platform, 
	brand, division, subdivision, department, class, subclass, product_type1, product_type2, merch_themes, quantrix_category,
	price_range,
	product_style_id, 
	style_group_id, style_group_desc,
	discovery_source,
	mrkt_type, finance_rollup, 
		case when imp_source_pageid is null then source_pagetype
			else
			(case when imp_source_action_feature in ('RECOMMENDATIONS') and imp_source_pagetype in ('PRODUCT_DETAIL','CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS', 'SEARCH_RESULTS', 'SEARCH', 'SHOPPING_BAG','HOME','ADD_TO_BAG_CONFIRMATION') then imp_source_pagetype 
				when imp_source_action_feature in ('LOOKS') and imp_source_pagetype in ('PRODUCT_DETAIL') then imp_source_pagetype 
				when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_feature = 'SBN - BROWSE' then 'BROWSE'
				when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_feature = 'SBN - SEARCH' then 'SEARCH'
				when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_pagetype in ('CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES', 'SEARCH_RESULTS','FLASH_EVENT_RESULTS','FLASH_EVENTS') then imp_source_pagetype
				when imp_source_action_feature not in ('SEARCH_BROWSE', 'RECOMMENDATIONS', 'LOOKS' ) then 'Not specified'
				else 'Other'
			end)
		end as source_pagetype,
		coalesce(
			(case when imp_source_pageid is null then source_page_sbn_value
				else 
				(case 
					when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_feature = 'SBN - BROWSE' then imp_source_sbn_value
					when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_feature = 'SBN - SEARCH' then 'no Browse URL'
					when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_pagetype in ('CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS') then imp_source_sbn_value 
					when imp_source_action_feature not in ('SEARCH_BROWSE' ) then 'no Browse URL'
					else 'no Browse URL'
				end)
			end), 
			'no Browse URL') as source_page_sbn_value,
		imp_source_action_feature as source_action_feature, 
		imp_source_feature as source_feature, 
	sum(imp_final_flag) as imp_final_session,
	sum(prod_min_price * imp_final_flag) as imp_price_min,
	sum(prod_original_price * imp_final_flag) as imp_price_ori,
	sum(imp_position) as imp_position,
	sum(imp_position_site) as imp_position_site,
	sum(click_final_flag) as click_final_session,
	sum(prod_min_price * click_final_flag) as click_price_min,
	sum(prod_original_price * click_final_flag) as click_price_ori,
	sum(full_pdv_flag) as full_pdv_session,
	sum(prod_min_price * full_pdv_flag) as pdv_price_min,
	sum(prod_original_price * full_pdv_flag) as pdv_price_ori,
	sum(first_full_PDV_instock_flag) as first_full_PDV_instock_session,
	sum(SKU_flag) as SKU_session,
	sum(prod_min_price * SKU_flag) as sku_price_min,
	sum(prod_original_price * SKU_flag) as sku_price_ori,
	sum(ATWL_flag) as ATWL_session,
	sum(prod_min_price * ATWL_flag) as ATWL_price_min,
	sum(prod_original_price * ATWL_flag) as ATWL_price_ori,
	sum(ATB_flag) as ATB_session,
	sum(ttl_ATB) as ttl_ATB,
	sum(ttl_atb_value) as ttl_atb_value, 
	sum(ttl_atb_demand) as ttl_atb_demand,
	sum(Order_flag) as Order_session,
	sum(ttl_order_qty) as ttl_order_qty, 
	sum(ttl_ord_value) as ttl_ord_value, 
	sum(ttl_ord_demand) as ttl_ord_demand
from {hive_schema}.session_product_discovery_funnel_analytical a
join OBJECT_MODEL.DAY_CAL_454_DIM cal 
on a.activity_date_partition = date_format(to_date(cal.day_date, 'MM/dd/yyyy'),'yyyy-MM-dd')
where activity_date_partition between {start_date} and {end_date}
	and channel = 'NORDSTROM'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
)
;

 


insert OVERWRITE TABLE {hive_schema}.product_discovery_funnel_daily_Ncom PARTITION (activity_date_partition) 
select *
from daily_Ncom
;

MSCK REPAIR table {hive_schema}.product_discovery_funnel_daily_Ncom
;








-- Table Name:product_discovery_funnel_daily_Rcom
-- Team/Owner: Digital Product Analytics/Cassie Zhang
-- Date Created/Modified: created on 8/29/2023, last modified on 3/14/2023
-- What is the update cadence/lookback window: refresh daily and 4 days lookback window 

--DROP TABLE IF EXISTS {hive_schema}.product_discovery_funnel_daily_Rcom;
create table if not exists {hive_schema}.product_discovery_funnel_daily_Rcom (
	fiscal_year_num int, 
	quarter_abrv string, 
	fiscal_month_num int, 
	month_abrv string, 
	week_num_of_fiscal_month int, 
	fiscal_week_num int, 
	week_start_day_date date,
	channel string, 
	platform string, 
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
	price_range string,
	product_style_id bigint, 
	style_group_id bigint,
	style_group_desc string,
	discovery_source string,
	mrkt_type string, 
	finance_rollup string, 
	source_pagetype string,
	source_page_sbn_value string,
	source_tier1 string,
	source_tier2 string,
	source_tier3 string, 
	source_tier4 string,
	source_action_feature string, 
	source_feature string, 
	imp_final_session int,
	imp_price_min decimal(36, 6),
	imp_price_ori decimal(36, 6),
	imp_position bigint,
	imp_position_site bigint,
	click_final_session int,
	click_price_min decimal(36, 6),
	click_price_ori decimal(36, 6),
	full_pdv_session int,
	pdv_price_min decimal(36, 6),
	pdv_price_ori decimal(36, 6),
	first_full_PDV_instock_session int,
	SKU_session int,
	sku_price_min decimal(36, 6),
	sku_price_ori decimal(36, 6),
	ATWL_session int,
	ATWL_price_min decimal(36, 6),
	ATWL_price_ori decimal(36, 6),
	ATB_session int,
	ttl_ATB bigint,
	ttl_atb_value decimal(36, 6), 
	ttl_atb_demand decimal(36, 6),
	Order_session int,
	ttl_order_qty bigint, 
	ttl_ord_value decimal(36, 6), 
	ttl_ord_demand decimal(36, 6),
	activity_date_partition date
) using ORC location 's3://{s3_bucket_root_var}/product_discovery_funnel_daily_Rcom' partitioned by (activity_date_partition)
;



/* 
Create daily table for Rcom
*/
create or replace temp view daily_Rcom as 

select cast(fiscal_year_num as int) as fiscal_year_num, 
	quarter_abrv, 
	cast(fiscal_month_num as int) as fiscal_month_num, 
	month_abrv, 
	cast(week_num_of_fiscal_month as int) as week_num_of_fiscal_month, 
	cast(fiscal_week_num as int) as fiscal_week_num, 
	to_date(regexp_replace(week_start_day_date, '/','-'), 'MM-dd-yyyy') as week_start_day_date, 
	channel, 
	platform, 
	brand, division, subdivision, department, class, subclass, product_type1, product_type2, merch_themes, quantrix_category,
	price_range,
	cast(product_style_id as bigint) as product_style_id, 
	cast(style_group_id as bigint) as style_group_id,
	style_group_desc,
	discovery_source,
	mrkt_type, 
	finance_rollup, 
	source_pagetype,
	source_page_sbn_value,
	split(source_page_sbn_value,'/') [0] as source_tier1,
	split(source_page_sbn_value,'/') [1] as source_tier2,
	case when split(source_page_sbn_value,'/') [0] = 'v2' and split(source_page_sbn_value,'/') [1] = 'brand' then replace(regexp_replace(split(source_page_sbn_value,'/') [2],'[0-9]+',''), '-', '') else split(source_page_sbn_value,'/') [2] end as source_tier3, 
	split(source_page_sbn_value,'/') [3] as source_tier4,
	source_action_feature, 
	source_feature, 
	imp_final_session,
	imp_price_min,
	imp_price_ori,
	imp_position,
	imp_position_site,
	click_final_session,
	click_price_min,
	click_price_ori,
	full_pdv_session,
	pdv_price_min,
	pdv_price_ori,
	first_full_PDV_instock_session,
	SKU_session,
	sku_price_min,
	sku_price_ori,
	ATWL_session,
	ATWL_price_min,
	ATWL_price_ori,
	ATB_session,
	ttl_ATB,
	ttl_atb_value, 
	ttl_atb_demand,
	Order_session,
	ttl_order_qty, 
	ttl_ord_value, 
	ttl_ord_demand,
	activity_date_partition
from
(select fiscal_year_num, quarter_abrv, fiscal_month_num, month_abrv, week_num_of_fiscal_month, fiscal_week_num, week_start_day_date, 
	activity_date_partition, 
	channel, platform, 
	brand, division, subdivision, department, class, subclass, product_type1, product_type2, merch_themes, quantrix_category,
	price_range,
	product_style_id, 
	style_group_id, style_group_desc,
	discovery_source,
	mrkt_type, finance_rollup, 
		case when imp_source_pageid is null then source_pagetype
			else
			(case when imp_source_action_feature in ('RECOMMENDATIONS') and imp_source_pagetype in ('PRODUCT_DETAIL','CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS', 'SEARCH_RESULTS', 'SEARCH', 'SHOPPING_BAG','HOME','ADD_TO_BAG_CONFIRMATION') then imp_source_pagetype 
				when imp_source_action_feature in ('LOOKS') and imp_source_pagetype in ('PRODUCT_DETAIL') then imp_source_pagetype 
				when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_feature = 'SBN - BROWSE' then 'BROWSE'
				when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_feature = 'SBN - SEARCH' then 'SEARCH'
				when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_pagetype in ('CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES', 'SEARCH_RESULTS','FLASH_EVENT_RESULTS','FLASH_EVENTS') then imp_source_pagetype
				when imp_source_action_feature not in ('SEARCH_BROWSE', 'RECOMMENDATIONS', 'LOOKS' ) then 'Not specified'
				else 'Other'
			end)
		end as source_pagetype,
		coalesce(
			(case when imp_source_pageid is null then source_page_sbn_value
				else 
				(case 
					when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_feature = 'SBN - BROWSE' then imp_source_sbn_value
					when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_feature = 'SBN - SEARCH' then 'no Browse URL'
					when imp_source_action_feature = 'SEARCH_BROWSE' and imp_source_pagetype in ('CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS') then imp_source_sbn_value 
					when imp_source_action_feature not in ('SEARCH_BROWSE' ) then 'no Browse URL'
					else 'no Browse URL'
				end)
			end), 
			'no Browse URL') as source_page_sbn_value,
		imp_source_action_feature as source_action_feature, 
		imp_source_feature as source_feature, 
	sum(imp_final_flag) as imp_final_session,
	sum(prod_min_price * imp_final_flag) as imp_price_min,
	sum(prod_original_price * imp_final_flag) as imp_price_ori,
	sum(imp_position) as imp_position,
	sum(imp_position_site) as imp_position_site,
	sum(click_final_flag) as click_final_session,
	sum(prod_min_price * click_final_flag) as click_price_min,
	sum(prod_original_price * click_final_flag) as click_price_ori,
	sum(full_pdv_flag) as full_pdv_session,
	sum(prod_min_price * full_pdv_flag) as pdv_price_min,
	sum(prod_original_price * full_pdv_flag) as pdv_price_ori,
	sum(first_full_PDV_instock_flag) as first_full_PDV_instock_session,
	sum(SKU_flag) as SKU_session,
	sum(prod_min_price * SKU_flag) as sku_price_min,
	sum(prod_original_price * SKU_flag) as sku_price_ori,
	sum(ATWL_flag) as ATWL_session,
	sum(prod_min_price * ATWL_flag) as ATWL_price_min,
	sum(prod_original_price * ATWL_flag) as ATWL_price_ori,
	sum(ATB_flag) as ATB_session,
	sum(ttl_ATB) as ttl_ATB,
	sum(ttl_atb_value) as ttl_atb_value, 
	sum(ttl_atb_demand) as ttl_atb_demand,
	sum(Order_flag) as Order_session,
	sum(ttl_order_qty) as ttl_order_qty, 
	sum(ttl_ord_value) as ttl_ord_value, 
	sum(ttl_ord_demand) as ttl_ord_demand
from {hive_schema}.session_product_discovery_funnel_analytical a
join OBJECT_MODEL.DAY_CAL_454_DIM cal 
on a.activity_date_partition = date_format(to_date(cal.day_date, 'MM/dd/yyyy'),'yyyy-MM-dd')
where activity_date_partition between {start_date} and {end_date}
	and channel = 'NORDSTROM_RACK'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
)
;




insert OVERWRITE TABLE {hive_schema}.product_discovery_funnel_daily_Rcom PARTITION (activity_date_partition) 
select *
from daily_Rcom
;

MSCK REPAIR table {hive_schema}.product_discovery_funnel_daily_Rcom
;