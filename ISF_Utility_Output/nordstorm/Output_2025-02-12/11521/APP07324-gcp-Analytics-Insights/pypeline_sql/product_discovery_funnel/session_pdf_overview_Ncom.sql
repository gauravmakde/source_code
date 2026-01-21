-- Table Name:session_pdf_overview_Ncom, session_pdf_browse_dive_Ncom
-- Team/Owner: Customer Analytics/Cassie Zhang
-- Date Created/Modified: created on 3/27/2024, last modified on 5/8/2024
-- What is the update cadence/lookback window: refresh daily and 1 days lookback window 
 
-- Drop TABLE IF EXISTS {hive_schema}.session_pdf_overview_Ncom;
create table if not exists {hive_schema}.session_pdf_overview_Ncom (
    fiscal_year_num int, 
    quarter_abrv string, 
    fiscal_month_num int, 
    month_abrv string, 
    week_num_of_fiscal_month int, 
    fiscal_week_num int, 
    week_start_day_date date, 
    channel string, 
    platform string, 
	product_style_id string, 
	style_group_num string,
	style_group_desc string,
	marketplace_flag string,
    brand string, 
    division string, 
    subdivision string, 
    department string, 
    class string, 
    merch_themes string, 
    quantrix_category string, 
    parent_group string, 
    npg_ind string, 
    gender string, 
    price_range string, 
    discovery_source string, 
    source_group string, 
    source_detail string, 
    source_pagetype string, 
    imp_final_session bigint, 
    ly_imp_final_session bigint, 
    imp_position bigint, 
    ly_imp_position bigint, 
    click_final_session bigint, 
    ly_click_final_session bigint, 
    full_pdv_session bigint, 
    ly_full_pdv_session bigint, 
    first_full_pdv_instock_session bigint, 
    ly_first_full_pdv_instock_session bigint, 
    sku_session bigint, 
    ly_sku_session bigint,
    atwl_session bigint,
    ly_atwl_session bigint,
    atb_session bigint,
    ly_atb_session bigint,
    order_session bigint,
    ly_order_session bigint,
    ttl_order_qty bigint,
    ly_ttl_order_qty bigint,
    ttl_ord_demand decimal(36, 6),
    ly_ttl_ord_demand decimal(36, 6),
    week_partition date
) using ORC location 's3://{s3_bucket_root_var}/session_pdf_overview_Ncom' partitioned by (week_partition)
;





create or replace temp view date_para as 

select
    {start_date} as start_date,
    {end_date} as end_date,
    {start_date} - INTERVAL '1' DAY * (dayofweek({start_date}) - 1) AS TY_start_week,
    {end_date} + INTERVAL '1' DAY * (7 - dayofweek({end_date})) AS TY_end_week,
    {start_date} - INTERVAL '1' DAY * (dayofweek({start_date}) - 1) - interval '364' day AS LY_start_week,
    {end_date} + INTERVAL '1' DAY * (7 - dayofweek({end_date})) - interval '364' day AS LY_end_week
;





create or replace temp view addition_attr as

select a.*, coalesce(npg_ind, 'Unknown') as npg_ind, parent_group, coalesce(genders_desc, 'Other') as genders_desc,
		b.style_group_num, b.style_group_desc, case when c.partnerrelationshiptype = 'ECONCESSION' then 'MP' else 'NMP' end as marketplace_flag
from
(select
	activity_date_partition, week_start_day_date, 
	channel, platform, brand, division, subdivision, department, class, merch_themes, quantrix_category, price_range,
	cast(a.product_style_id as varchar(20)) as product_style_id, discovery_source, mrkt_type, finance_rollup, source_action_feature, source_feature, source_pagetype, source_page_sbn_value,
	imp_final_session, imp_position, imp_position_site, click_final_session, full_pdv_session, first_full_pdv_instock_session, sku_session, 
	atwl_session, atb_session, order_session, ttl_order_qty, ttl_ord_demand
from ace_etl.product_discovery_funnel_daily_Ncom a
where 
	(
	(week_start_day_date between (select TY_start_week from date_para) and (select TY_end_week from date_para))
		or
	(week_start_day_date between (select LY_start_week from date_para) and (select LY_end_week from date_para))
	)
) a
left join 
(select web_style_num, day_date, selling_channel, style_group_num, style_group_desc, cast(npg_ind as varchar(2)) as npg_ind, parent_group, case when lower(genders_desc) in ('female', 'male', 'unisex') then genders_desc else 'Other' end as genders_desc
from ace_etl.digital_merch_table 
where 
	(
	(day_date between (select TY_start_week from date_para) and (select TY_end_week from date_para))
		or
	(day_date between (select LY_start_week from date_para) and (select LY_end_week from date_para))
	)
)b
on a.product_style_id = b.web_style_num
	and a.activity_date_partition = b.day_date
	and a.channel = b.selling_channel
left join 
(select join_date, webstyleid, partnerrelationshiptype
from ace_etl.marketplace_product_relationship
where 
	(
	(join_date between (select TY_start_week from date_para) and (select TY_end_week from date_para))
		or
	(join_date between (select LY_start_week from date_para) and (select LY_end_week from date_para))
	)
) c
on a.product_style_id = c.webstyleid 
	and a.activity_date_partition = c.join_date
;





create or replace temp view weekly_raw_overview as

select  
	week_start_day_date,
	channel, platform, 
	product_style_id, style_group_num, style_group_desc, marketplace_flag,
	brand, division, subdivision, department, class, merch_themes, quantrix_category, parent_group, npg_ind, genders_desc, 
	price_range, 
	discovery_source,
	coalesce(case when discovery_source = 'External' then coalesce(mrkt_type, 'UNATTRIBUTED') end,
				case when source_action_feature in ('CHECKOUT', 'CONTENT', 'LOOKS', 'RECOMMENDATIONS', 'SEARCH_BROWSE', 'SPONSORED_CAROUSEL', 'WISH_LIST') then source_action_feature 
					when source_action_feature is null then 'No impression source'
					else 'OTHER' 
				end
			) as source_group,
	coalesce(case when discovery_source = 'External' then coalesce(finance_rollup, 'UNATTRIBUTED') end,
				case when source_action_feature in ('CHECKOUT', 'CONTENT', 'LOOKS', 'RECOMMENDATIONS', 'SEARCH_BROWSE', 'SPONSORED_CAROUSEL', 'WISH_LIST') then source_feature 
					when source_action_feature is null then 'No impression source'
					else 'OTHER' 
				end
			) as source_detail,
	case when discovery_source = 'External' then source_pagetype
			when source_action_feature = 'SEARCH_BROWSE' and source_pagetype = 'SEARCH_RESULTS' then 'SEARCH'
			when source_action_feature = 'SEARCH_BROWSE' and source_pagetype in ('CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES') then 'BROWSE'
			when source_action_feature = 'LOOKS' then 'Not specified'
			when source_action_feature = 'RECOMMENDATIONS' and source_pagetype = 'ADD_TO_BAG_CONFIRMATION' then 'Other'
			when source_action_feature = 'RECOMMENDATIONS' and source_pagetype in ('CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES')  then 'BROWSE'
			when source_action_feature = 'RECOMMENDATIONS' and source_pagetype = 'SEARCH_RESULTS' then 'SEARCH'
			when source_action_feature = 'RECOMMENDATIONS' and source_pagetype in ('HOME','SEARCH')  then 'HOME/SHOP'
			when source_action_feature is null and source_pagetype in ('WISH_LIST')  then 'Other'
			when source_action_feature is null and source_pagetype in ('SEARCH_RESULTS')  then 'SEARCH'
			when source_action_feature is null and source_pagetype in ('HOME','SEARCH')  then 'HOME/SHOP'
			when source_action_feature is null and source_pagetype is null then 'No source page'
			else source_pagetype
	end as source_pagetype,
	sum(imp_final_session) as imp_final_session, 
	sum(imp_position) as imp_position, 
	sum(click_final_session) as click_final_session, 
	sum(full_pdv_session) as full_pdv_session,
	sum(first_full_pdv_instock_session) as first_full_pdv_instock_session,
	sum(sku_session) as sku_session, 
	sum(atwl_session) as atwl_session, 
	sum(atb_session) as atb_session, 
	sum(order_session) as order_session, 
	sum(ttl_order_qty) as ttl_order_qty, 
	sum(ttl_ord_demand) as ttl_ord_demand
from addition_attr
where 
	(
	(week_start_day_date between (select TY_start_week from date_para) and (select TY_end_week from date_para))
		or
	(week_start_day_date between (select LY_start_week from date_para) and (select LY_end_week from date_para))
	)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
;






create or replace temp view final_view_overview as

select cast(c.fiscal_year_num as int) as fiscal_year_num,
		c.quarter_abrv, 
		cast(c.fiscal_month_num as int) as fiscal_month_num, 
		c.month_abrv, 
		cast(c.week_num_of_fiscal_month as int) as week_num_of_fiscal_month, 
		cast(c.fiscal_week_num as int) as fiscal_week_num, 
		a.*,
		a.week_start_day_date as week_partition
from
(select 
		coalesce(t.week_start_day_date, l.week_start_day_date + interval '364' day) as week_start_day_date, 
		coalesce(t.channel,l.channel) as channel, 
		coalesce(t.platform,l.platform) as platform, 
		coalesce(t.product_style_id,l.product_style_id) as product_style_id, 
		coalesce(t.style_group_num,l.style_group_num) as style_group_num, 
		coalesce(t.style_group_desc,l.style_group_desc) as style_group_desc,
		coalesce(t.marketplace_flag,l.marketplace_flag) as marketplace_flag,
		coalesce(t.brand,l.brand) as brand, 
		coalesce(t.division,l.division) as division, 
		coalesce(t.subdivision,l.subdivision) as subdivision, 
		coalesce(t.department,l.department) as department, 
		coalesce(t.class,l.class) as class, 
		coalesce(t.merch_themes,l.merch_themes) as merch_themes, 
		coalesce(t.quantrix_category,l.quantrix_category) as quantrix_category, 
		coalesce(t.parent_group,l.parent_group) as parent_group, 
		coalesce(t.npg_ind,l.npg_ind) as npg_ind, 
		coalesce(t.genders_desc,l.genders_desc) as gender, 
		coalesce(t.price_range,l.price_range) as price_range, 
		coalesce(t.discovery_source,l.discovery_source) as discovery_source, 
		coalesce(t.source_group,l.source_group) as source_group, 
		coalesce(t.source_detail,l.source_detail) as source_detail, 
		coalesce(t.source_pagetype,l.source_pagetype) as source_pagetype,
		t.imp_final_session, l.imp_final_session as LY_imp_final_session,
		t.imp_position, l.imp_position as LY_imp_position,
		t.click_final_session, l.click_final_session as LY_click_final_session,
		t.full_pdv_session, l.full_pdv_session as LY_full_pdv_session,
		t.first_full_pdv_instock_session, l.first_full_pdv_instock_session as LY_first_full_pdv_instock_session,
		t.sku_session, l.sku_session as LY_sku_session,
		t.atwl_session, l.atwl_session as LY_atwl_session,
		t.atb_session, l.atb_session as LY_atb_session,
		t.order_session, l.order_session as LY_order_session,
		t.ttl_order_qty, l.ttl_order_qty as LY_ttl_order_qty,
		t.ttl_ord_demand, l.ttl_ord_demand as LY_ttl_ord_demand
from 
	(select * from weekly_raw_overview where week_start_day_date between (select TY_start_week from date_para) and (select TY_end_week from date_para) ) t
full outer join 
	(select * from weekly_raw_overview where week_start_day_date between (select LY_start_week from date_para) and (select LY_end_week from date_para) ) l
on t.week_start_day_date = l.week_start_day_date + interval '364' day
	and t.channel = l.channel 
	and t.platform = l.platform
	and coalesce(t.product_style_id,'') = coalesce(l.product_style_id,'')
	and coalesce(t.style_group_num,'') = coalesce(l.style_group_num,'')  
	and coalesce(t.style_group_desc,'') = coalesce(l.style_group_desc,'')  
	and coalesce(t.marketplace_flag,'') = coalesce(l.marketplace_flag,'')  
	and coalesce(t.brand,'') = coalesce(l.brand,'') 
	and coalesce(t.division,'') = coalesce(l.division,'') 
	and coalesce(t.subdivision,'') = coalesce(l.subdivision,'') 
	and coalesce(t.department,'') = coalesce(l.department,'') 
	and coalesce(t.class,'') = coalesce(l.class,'') 
	and coalesce(t.merch_themes,'') = coalesce(l.merch_themes,'') 
	and coalesce(t.quantrix_category,'') = coalesce(l.quantrix_category,'') 
	and coalesce(t.parent_group,'') = coalesce(l.parent_group,'') 
	and coalesce(t.npg_ind,'') = coalesce(l.npg_ind,'') 
	and coalesce(t.genders_desc,'') = coalesce(l.genders_desc,'') 
	and coalesce(t.price_range,'') = coalesce(l.price_range,'') 
	and coalesce(t.discovery_source,'') = coalesce(l.discovery_source,'') 
	and coalesce(t.source_group,'') = coalesce(l.source_group,'') 
	and coalesce(t.source_detail,'') = coalesce(l.source_detail,'') 
	and coalesce(t.source_pagetype,'') = coalesce(l.source_pagetype,'') 
) a
left join 
	(SELECT distinct fiscal_year_num, quarter_abrv, fiscal_month_num, month_abrv, week_num_of_fiscal_month, cast(fiscal_week_num as integer) as fiscal_week_num, 
			date_format(to_date(week_start_day_date, 'MM/dd/yyyy'),'yyyy-MM-dd') as week_start_day_date
	FROM OBJECT_MODEL.DAY_CAL_454_DIM cal) c
on a.week_start_day_date = c.week_start_day_date
order by 8,9,10,11,12,13,14,15,16,17,18,19,20,21,7
;





insert OVERWRITE TABLE {hive_schema}.session_pdf_overview_Ncom PARTITION (week_partition) 
select *
from final_view_overview
;

MSCK REPAIR table {hive_schema}.session_pdf_overview_Ncom
;

















-- Drop TABLE IF EXISTS {hive_schema}.session_pdf_browse_dive_Ncom;
create table if not exists {hive_schema}.session_pdf_browse_dive_Ncom (
    fiscal_year_num int, 
    quarter_abrv string, 
    fiscal_month_num int, 
    month_abrv string, 
    week_num_of_fiscal_month int, 
    fiscal_week_num int, 
    week_start_day_date date, 
    channel string, 
    platform string, 
	product_style_id string, 
	style_group_num string,
	style_group_desc string,
	marketplace_flag string,
    brand string, 
    division string, 
    subdivision string, 
    department string, 
    class string, 
    merch_themes string, 
    quantrix_category string, 
    parent_group string, 
    npg_ind string, 
    gender string, 
    price_range string, 
    discovery_source string, 
    source_group string, 
    source_detail string, 
    source_pagetype string, 
    source_page_sbn_value string,
    source_tier1 string, 
    source_tier2 string, 
    source_tier3 string, 
    source_tier4 string, 
    source_tier5 string, 
    imp_final_session bigint, 
    ly_imp_final_session bigint, 
    imp_position bigint, 
    ly_imp_position bigint, 
    click_final_session bigint, 
    ly_click_final_session bigint, 
    full_pdv_session bigint, 
    ly_full_pdv_session bigint, 
    first_full_pdv_instock_session bigint, 
    ly_first_full_pdv_instock_session bigint, 
    sku_session bigint, 
    ly_sku_session bigint,
    atwl_session bigint,
    ly_atwl_session bigint,
    atb_session bigint,
    ly_atb_session bigint,
    order_session bigint,
    ly_order_session bigint,
    ttl_order_qty bigint,
    ly_ttl_order_qty bigint,
    ttl_ord_demand decimal(36, 6),
    ly_ttl_ord_demand decimal(36, 6),
    week_partition date
) using PARQUET location 's3://{s3_bucket_root_var}/session_pdf_browse_dive_Ncom' partitioned by (week_partition);




create or replace temp view weekly_raw_browse as
select 
	week_start_day_date, 
	channel, platform, 
	product_style_id, style_group_num, style_group_desc, marketplace_flag,
	brand, division, subdivision, department, class, merch_themes, quantrix_category, parent_group, npg_ind, genders_desc, 
	price_range,
	discovery_source,
	coalesce(case when discovery_source = 'External' then coalesce(mrkt_type, 'UNATTRIBUTED') end,
				case when source_action_feature in ('CHECKOUT', 'CONTENT', 'LOOKS', 'RECOMMENDATIONS', 'SEARCH_BROWSE', 'SPONSORED_CAROUSEL', 'WISH_LIST') then source_action_feature 
					when source_action_feature is null then 'No impression source'
					else 'OTHER' 
				end
			) as source_group,
	coalesce(case when discovery_source = 'External' then coalesce(finance_rollup, 'UNATTRIBUTED') end,
				case when source_action_feature in ('CHECKOUT', 'CONTENT', 'LOOKS', 'RECOMMENDATIONS', 'SEARCH_BROWSE', 'SPONSORED_CAROUSEL', 'WISH_LIST') then source_feature 
					when source_action_feature is null then 'No impression source'
					else 'OTHER' 
				end
			) as source_detail,
	case when discovery_source = 'External' then source_pagetype
			when source_action_feature = 'SEARCH_BROWSE' and source_pagetype = 'SEARCH_RESULTS' then 'SEARCH'
			when source_action_feature = 'SEARCH_BROWSE' and source_pagetype in ('CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES') then 'BROWSE'
			when source_action_feature = 'LOOKS' then 'Not specified'
			when source_action_feature = 'RECOMMENDATIONS' and source_pagetype = 'ADD_TO_BAG_CONFIRMATION' then 'Other'
			when source_action_feature = 'RECOMMENDATIONS' and source_pagetype in ('CATEGORY_RESULTS','BRAND_RESULTS','CATEGORIES')  then 'BROWSE'
			when source_action_feature = 'RECOMMENDATIONS' and source_pagetype = 'SEARCH_RESULTS' then 'SEARCH'
			when source_action_feature = 'RECOMMENDATIONS' and source_pagetype in ('HOME','SEARCH')  then 'HOME/SHOP'
			when source_action_feature is null and source_pagetype in ('WISH_LIST')  then 'Other'
			when source_action_feature is null and source_pagetype in ('SEARCH_RESULTS')  then 'SEARCH'
			when source_action_feature is null and source_pagetype in ('HOME','SEARCH')  then 'HOME/SHOP'
			when source_action_feature is null and source_pagetype is null then 'No source page'
			else source_pagetype
	end as source_pagetype, 
	source_page_sbn_value, 
	sum(imp_final_session) as imp_final_session, 
	sum(imp_position) as imp_position, 
	sum(click_final_session) as click_final_session, 
	sum(full_pdv_session) as full_pdv_session,
	sum(first_full_pdv_instock_session) as first_full_pdv_instock_session,
	sum(sku_session) as sku_session, 
	sum(atwl_session) as atwl_session, 
	sum(atb_session) as atb_session, 
	sum(order_session) as order_session, 
	sum(ttl_order_qty) as ttl_order_qty, 
	sum(ttl_ord_demand) as ttl_ord_demand
from addition_attr
where 
	(
	(week_start_day_date between (select TY_start_week from date_para) and (select TY_end_week from date_para))
		or
	(week_start_day_date between (select LY_start_week from date_para) and (select LY_end_week from date_para))
	)
	and discovery_source = 'Internal'
	and ((source_action_feature = 'SEARCH_BROWSE' and source_feature != 'SBN - SEARCH' and source_pagetype not in ('SEARCH_RESULTS','Other')) or (source_action_feature is null and source_pagetype = 'BROWSE'))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
;





create or replace temp view final_view_browse as

select cast(c.fiscal_year_num as int) as fiscal_year_num,
		c.quarter_abrv, 
		cast(c.fiscal_month_num as int) as fiscal_month_num, 
		c.month_abrv, 
		cast(c.week_num_of_fiscal_month as int) as week_num_of_fiscal_month, 
		cast(c.fiscal_week_num as int) as fiscal_week_num, 
		a.week_start_day_date,
		channel,platform, 
		product_style_id, style_group_num, style_group_desc, marketplace_flag,
		brand, division, subdivision, department, class, merch_themes, quantrix_category, parent_group, npg_ind, gender, price_range, 
		discovery_source, source_group, source_detail, source_pagetype, source_page_sbn_value,
		case when source_page_sbn_value like ('v2/brand%') then 'brand' else split(source_page_sbn_value,'/') [0] end as source_tier1,
		case when source_page_sbn_value like ('v2/brand%') then split(source_page_sbn_value,'/') [2] else split(source_page_sbn_value,'/') [1] end as source_tier2, 
		case when source_page_sbn_value like ('v2/brand%') then split(source_page_sbn_value,'/') [3] else split(source_page_sbn_value,'/') [2] end as source_tier3,
		case when source_page_sbn_value like ('v2/brand%') then split(source_page_sbn_value,'/') [4] else split(source_page_sbn_value,'/') [3] end as source_tier4,
		case when source_page_sbn_value like ('v2/brand%') then split(source_page_sbn_value,'/') [5] else split(source_page_sbn_value,'/') [4] end as source_tier5,
		imp_final_session, LY_imp_final_session,
		imp_position, LY_imp_position,
		click_final_session, LY_click_final_session,
		full_pdv_session, LY_full_pdv_session,
		first_full_pdv_instock_session, LY_first_full_pdv_instock_session,
		sku_session, LY_sku_session,
		atwl_session, LY_atwl_session,
		atb_session, LY_atb_session,
		order_session, LY_order_session,
		ttl_order_qty, LY_ttl_order_qty,
		ttl_ord_demand, LY_ttl_ord_demand,
		a.week_start_day_date as week_partition
from (
select 
		coalesce(t.week_start_day_date, l.week_start_day_date + interval '364' day) as week_start_day_date, 
		coalesce(t.channel,l.channel) as channel, 
		coalesce(t.platform,l.platform) as platform, 
		coalesce(t.product_style_id,l.product_style_id) as product_style_id, 
		coalesce(t.style_group_num,l.style_group_num) as style_group_num, 
		coalesce(t.style_group_desc,l.style_group_desc) as style_group_desc,
		coalesce(t.marketplace_flag,l.marketplace_flag) as marketplace_flag,
		coalesce(t.brand,l.brand) as brand, 
		coalesce(t.division,l.division) as division, 
		coalesce(t.subdivision,l.subdivision) as subdivision, 
		coalesce(t.department,l.department) as department, 
		coalesce(t.class,l.class) as class, 
		coalesce(t.merch_themes,l.merch_themes) as merch_themes, 
		coalesce(t.quantrix_category,l.quantrix_category) as quantrix_category, 
		coalesce(t.parent_group,l.parent_group) as parent_group, 
		coalesce(t.npg_ind,l.npg_ind) as npg_ind, 
		coalesce(t.genders_desc,l.genders_desc) as gender,
		coalesce(t.price_range,l.price_range) as price_range, 
		coalesce(t.discovery_source,l.discovery_source) as discovery_source, 
		coalesce(t.source_group,l.source_group) as source_group, 
		coalesce(t.source_detail,l.source_detail) as source_detail, 
		coalesce(t.source_pagetype,l.source_pagetype) as source_pagetype,
		coalesce(t.source_page_sbn_value,l.source_page_sbn_value) as source_page_sbn_value,
		t.imp_final_session, l.imp_final_session as LY_imp_final_session,
		t.imp_position, l.imp_position as LY_imp_position,
		t.click_final_session, l.click_final_session as LY_click_final_session,
		t.full_pdv_session, l.full_pdv_session as LY_full_pdv_session,
		t.first_full_pdv_instock_session, l.first_full_pdv_instock_session as LY_first_full_pdv_instock_session,
		t.sku_session, l.sku_session as LY_sku_session,
		t.atwl_session, l.atwl_session as LY_atwl_session,
		t.atb_session, l.atb_session as LY_atb_session,
		t.order_session, l.order_session as LY_order_session,
		t.ttl_order_qty, l.ttl_order_qty as LY_ttl_order_qty,
		t.ttl_ord_demand, l.ttl_ord_demand as LY_ttl_ord_demand
from 
	(select * from weekly_raw_browse where week_start_day_date between (select TY_start_week from date_para) and (select TY_end_week from date_para) ) t
full outer join 
	(select * from weekly_raw_browse where week_start_day_date between (select LY_start_week from date_para) and (select LY_end_week from date_para) ) l
on t.week_start_day_date = l.week_start_day_date + interval '364' day
	and t.channel = l.channel 
	and t.platform = l.platform
	and coalesce(t.product_style_id,'') = coalesce(l.product_style_id,'')
	and coalesce(t.style_group_num,'') = coalesce(l.style_group_num,'')  
	and coalesce(t.style_group_desc,'') = coalesce(l.style_group_desc,'')  
	and coalesce(t.marketplace_flag,'') = coalesce(l.marketplace_flag,'') 
	and coalesce(t.brand,'') = coalesce(l.brand,'') 
	and coalesce(t.division,'') = coalesce(l.division,'') 
	and coalesce(t.subdivision,'') = coalesce(l.subdivision,'') 
	and coalesce(t.department,'') = coalesce(l.department,'') 
	and coalesce(t.class,'') = coalesce(l.class,'') 
	and coalesce(t.merch_themes,'') = coalesce(l.merch_themes,'') 
	and coalesce(t.quantrix_category,'') = coalesce(l.quantrix_category,'') 
	and coalesce(t.parent_group,'') = coalesce(l.parent_group,'') 
	and coalesce(t.npg_ind,'') = coalesce(l.npg_ind,'') 
	and coalesce(t.genders_desc,'') = coalesce(l.genders_desc,'') 
	and coalesce(t.price_range,'') = coalesce(l.price_range,'') 
	and coalesce(t.discovery_source,'') = coalesce(l.discovery_source,'') 
	and coalesce(t.source_group,'') = coalesce(l.source_group,'') 
	and coalesce(t.source_detail,'') = coalesce(l.source_detail,'') 
	and coalesce(t.source_pagetype,'') = coalesce(l.source_pagetype,'') 
	and coalesce(t.source_page_sbn_value,'') = coalesce(l.source_page_sbn_value,'') 
) a
left join 
	(SELECT distinct fiscal_year_num, quarter_abrv, fiscal_month_num, month_abrv, week_num_of_fiscal_month, cast(fiscal_week_num as integer) as fiscal_week_num, 
			date_format(to_date(week_start_day_date, 'MM/dd/yyyy'),'yyyy-MM-dd') as week_start_day_date
	FROM OBJECT_MODEL.DAY_CAL_454_DIM cal) c
on a.week_start_day_date = c.week_start_day_date

;





insert OVERWRITE TABLE {hive_schema}.session_pdf_browse_dive_Ncom PARTITION (week_partition) 
select *
from final_view_browse
;

MSCK REPAIR table {hive_schema}.session_pdf_browse_dive_Ncom
;









