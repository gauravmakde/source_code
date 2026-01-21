-- Table Name:flashoverviewtable_sessionlogic_customergrouping, flashoverviewtable_eventlogic_customergrouping, flashoverviewtable_sessionlogic_merchgrouping, flashoverviewtable_eventlogic_merchgrouping
-- Team/Owner: Customer Analytics/Sean Larkin
-- Date Created/Modified: created on 6/25/2024, last modified on 7/12/2024
-- What is the update cadence/lookback window: refresh daily and 14 days lookback window 


--DROP TABLE IF EXISTS ace_etl.flashoverviewtable_sessionlogic_customergrouping;
create table if not exists ace_etl.flashoverviewtable_sessionlogic_customergrouping (
flash_session_flag int
, digitalcontents_id string
, engagement_cohort string
, loyalty_status string
, predicted_ct_segment string
, aare_acquired int
, aare_activated int
, source_flag string
, mrkt_type string
, finance_rollup string
, finance_detail string
, internal_source_pagetype string
, nav_flag string
, experience string
, num_sessions_ttl int
, num_sessions int
, num_imp_sessions_ttl int
, num_imp_sessions_direct int
, num_imp_sessions int
, num_imp_sessions_halo int
, num_pdv_sessions_ttl int
, num_pdv_sessions_direct int
, num_pdv_sessions int
, num_pdv_sessions_halo int
, num_atb_sessions_ttl int
, num_atb_sessions_direct int
, num_atb_sessions int
, num_atb_sessions_halo int
, num_order_sessions_ttl int
, num_order_sessions_direct int
, num_order_sessions int
, num_order_sessions_halo int
, halo_imp_sessions int
, indirect_imp_sessions int
, direct_imp_sessions int
, halo_pdv_sessions int
, indirect_pdv_sessions int
, direct_pdv_sessions int
, halo_atb_sessions int
, indirect_atb_sessions int
, direct_atb_sessions int
, halo_order_sessions int
, indirect_order_sessions int
, direct_order_sessions int
, halo_order_qty int
, indirect_order_qty int
, direct_order_qty int
, halo_order_value decimal(36, 6)
, indirect_order_value decimal(36, 6)
, direct_order_value decimal(36, 6)
, halo_order_demand decimal(36, 6)
, indirect_order_demand decimal(36, 6)
, direct_order_demand decimal(36, 6)
, activity_date_partition date
) using parquet location 's3://ace-etl/flash_attribution_test/flashoverviewtable_sessionlogic_customergrouping' partitioned by (activity_date_partition);


--DROP TABLE IF EXISTS ace_etl.flashoverviewtable_sessionlogic_merchgrouping;
create table if not exists ace_etl.flashoverviewtable_sessionlogic_merchgrouping (
flash_session_flag int
, digitalcontents_id string
, brand_name string
, div_desc string
, grp_desc string
, dept_desc string
, class_desc string
, sbclass_desc string 
, new_flag string
, product_style_id string
, style_group_desc string
, style_group_num string
, quantrix_category string
, rack_role_desc string
, parent_group string
, source_flag string
, mrkt_type string
, finance_rollup string
, finance_detail string
, internal_source_pagetype string
, nav_flag string
, experience string
, halo_imp_sessions int
, indirect_imp_sessions int
, direct_imp_sessions int
, halo_pdv_sessions int
, indirect_pdv_sessions int
, direct_pdv_sessions int
, halo_atb_sessions int
, indirect_atb_sessions int
, direct_atb_sessions int
, halo_order_sessions int
, indirect_order_sessions int
, direct_order_sessions int
, halo_order_qty int
, indirect_order_qty int
, direct_order_qty int
, halo_order_value decimal(36, 6)
, indirect_order_value decimal(36, 6)
, direct_order_value decimal(36, 6)
, halo_order_demand decimal(36, 6)
, indirect_order_demand decimal(36, 6)
, direct_order_demand decimal(36, 6)
, activity_date_partition date
) using parquet location 's3://ace-etl/flash_attribution_test/flashoverviewtable_sessionlogic_merchgrouping' partitioned by (activity_date_partition);

--DROP TABLE IF EXISTS ace_etl.flashoverviewtable_eventlogic_merchgrouping;
create table if not exists ace_etl.flashoverviewtable_eventlogic_merchgrouping (
flash_session_flag int
, digitalcontents_id string
, brand_name string
, div_desc string
, grp_desc string
, dept_desc string
, class_desc string
, sbclass_desc string 
, new_flag string
, product_style_id string
, style_group_desc string
, style_group_num string
, quantrix_category string
, rack_role_desc string
, parent_group string
, source_flag string
, mrkt_type string
, finance_rollup string
, finance_detail string
, internal_source_pagetype string
, nav_flag string
, experience string
, halo_imp_sessions int
, indirect_imp_sessions int
, direct_imp_sessions int
, halo_pdv_sessions int
, indirect_pdv_sessions int
, direct_pdv_sessions int
, halo_atb_sessions int
, indirect_atb_sessions int
, direct_atb_sessions int
, halo_order_sessions int
, indirect_order_sessions int
, direct_order_sessions int
, halo_order_qty int
, indirect_order_qty int
, direct_order_qty int
, halo_order_value decimal(36, 6)
, indirect_order_value decimal(36, 6)
, direct_order_value decimal(36, 6)
, halo_order_demand decimal(36, 6)
, indirect_order_demand decimal(36, 6)
, direct_order_demand decimal(36, 6)
, activity_date_partition date
) using parquet location 's3://ace-etl/flash_attribution_test/flashoverviewtable_eventlogic_merchgrouping' partitioned by (activity_date_partition);


--DROP TABLE IF EXISTS ace_etl.flashoverviewtable_eventlogic_customergrouping;
create table if not exists ace_etl.flashoverviewtable_eventlogic_customergrouping (
flash_session_flag int
, digitalcontents_id string
, engagement_cohort string
, loyalty_status string
, predicted_ct_segment string
, aare_acquired string
, aare_activated string
, source_flag string
, mrkt_type string
, finance_rollup string
, finance_detail string
, internal_source_pagetype string
, nav_flag string
, experience string
, num_sessions_ttl int
, num_sessions int
, num_imp_sessions_ttl int
, num_imp_sessions_direct int
, num_imp_sessions int
, num_imp_sessions_halo int
, num_pdv_sessions_ttl int
, num_pdv_sessions_direct int
, num_pdv_sessions int
, num_pdv_sessions_halo int
, num_atb_sessions_ttl int
, num_atb_sessions_direct int
, num_atb_sessions int
, num_atb_sessions_halo int
, num_order_sessions_ttl int
, num_order_sessions_direct int
, num_order_sessions int
, num_order_sessions_halo int
, halo_imp_sessions int
, indirect_imp_sessions int
, direct_imp_sessions int
, halo_pdv_sessions int
, indirect_pdv_sessions int
, direct_pdv_sessions int
, halo_atb_sessions int
, indirect_atb_sessions int
, direct_atb_sessions int
, halo_order_sessions int
, indirect_order_sessions int
, direct_order_sessions int
, halo_order_qty int
, indirect_order_qty int
, direct_order_qty int
, halo_order_value decimal(36, 6)
, indirect_order_value decimal(36, 6)
, direct_order_value decimal(36, 6)
, halo_order_demand decimal(36, 6)
, indirect_order_demand decimal(36, 6)
, direct_order_demand decimal(36, 6)
, activity_date_partition date
) using parquet location 's3://ace-etl/flash_attribution_test/flashoverviewtable_eventlogic_customergrouping' partitioned by (activity_date_partition);







create or replace temp view step0_allflashstyles as (
select 
distinct  
seeap.productstyle_id
, seeap.digitalcontents_id
from acp_event_intermediate.session_evt_expanded_attributes_parquet seeap 
	where seeap.context_pagetype in ('FLASH_EVENTS', 'FLASH_EVENT_RESULTS')
	and seeap.digitalcontents_type = 'FLASH_EVENT'
	and seeap.event_name = 'com.nordstrom.event.customer.ProductSummaryCollectionViewed'
	and seeap.page_indicator = 'BROWSE'
	and seeap.activity_date_partition between date'2024-09-16' and date'2024-10-15'
);


create or replace temp view step0_allflashdates as (
select distinct
seeap.digitalcontents_id 
, cast(event_time_pst as DATE) activity_date_partition
from acp_event_intermediate.session_evt_expanded_attributes_parquet seeap 
	where seeap.context_pagetype in ('FLASH_EVENTS', 'FLASH_EVENT_RESULTS')
	and seeap.digitalcontents_type = 'FLASH_EVENT'
	and seeap.event_name = 'com.nordstrom.event.customer.ProductSummaryCollectionViewed'
	and seeap.page_indicator = 'BROWSE'
	and seeap.activity_date_partition between date'2024-09-16' and date'2024-10-15'
);


create or replace temp view step0_flashbrandedflag as (
with brand_counter as (
select digitalcontents_id
, count(distinct brand) brand_count
from step0_allflashstyles afs 
left join ace_etl.digital_merch_table_as_is dmt 
	on dmt.web_style_num = cast(afs.productstyle_id as string)
	and dmt.selling_channel = 'NORDSTROM_RACK'
group by 1
)
select 
digitalcontents_id
, case when brand_count <= 5 then 'branded'
       else 'curated'
       end as branded_flag
from brand_counter 
);





create or replace temp view step1_sessionidmatching as ( 
with all_sessions as (
select
seeap.session_id
, max(seeap.activity_date_partition) activity_date_partition
from acp_event_intermediate.session_evt_expanded_attributes_parquet seeap 
where seeap.channel = 'NORDSTROM_RACK'
	and seeap.activity_date_partition between date'2024-09-16' and date'2024-10-15'
group by 1
)
select 
ases.session_id
, max(sulp.unique_acp_id_max) new_acp_id
, max(sulp.unique_shopper_id_max) max_shopper_id
, ases.activity_date_partition
from all_sessions ases 
left join acp_event_intermediate.session_user_lookup_parquet sulp
	on sulp.session_id = ases.session_id
group by 1,4
);




create or replace temp view step2_flashclicks as (
select distinct bsa.session_id 
, coalesce(asm.new_acp_id, asm.max_shopper_id) session_cust_id
, bsa.activity_date_partition
, bsa.digitalcontents_id 
, bsa.productstyle_id 
from acp_event_intermediate.session_evt_expanded_attributes_parquet bsa
inner join step0_allflashdates afd 
	on afd.digitalcontents_id = bsa.digitalcontents_id 
	and afd.activity_date_partition = bsa.activity_date_partition
left join step1_sessionidmatching asm 
	on asm.session_id = bsa.session_id
where bsa.channel = 'NORDSTROM_RACK'
and bsa.event_name = 'com.nordstrom.event.customer.ProductSummarySelected'
and bsa.activity_date_partition between date'2024-09-16' and date'2024-10-15'
and context_pagetype in ('FLASH_EVENTS', 'FLASH_EVENT_RESULTS')
and bsa.productstyle_id is not null
);



create or replace temp view step2_flashimpressions as (
select distinct bsa.session_id 
, coalesce(asm.new_acp_id, asm.max_shopper_id) session_cust_id
, bsa.activity_date_partition
, bsa.digitalcontents_id
, bsa.productstyle_id 
from acp_event_intermediate.session_evt_expanded_attributes_parquet bsa
inner join step0_allflashdates afd 
	on afd.digitalcontents_id = bsa.digitalcontents_id 
	and afd.activity_date_partition = bsa.activity_date_partition
left join step1_sessionidmatching asm 
	on asm.session_id = bsa.session_id
where bsa.channel = 'NORDSTROM_RACK'
and bsa.event_name = 'com.nordstrom.event.customer.ProductSummaryCollectionViewed'
and bsa.activity_date_partition between date'2024-09-16' and date'2024-10-15'
and context_pagetype in ('FLASH_EVENTS', 'FLASH_EVENT_RESULTS')
and bsa.productstyle_id is not null
);



create or replace temp view step3_customerattributes as (
select distinct 
acp_id
, activity_date_pacific
, engagement_cohort
, loyalty_status
, predicted_ct_segment 
from ace_etl.customer_attributes_table
where activity_date_pacific between  date'2024-09-16' and date'2024-10-15'
);





create or replace temp view step3_aareacquireddate as (
select
acp_id
, max(aare_status_date) aare_status_date
from ace_etl.customer_attributes_table
where activity_date_pacific between date'2024-09-16' and date'2024-10-15'
group by 1
);


create or replace temp view step3_aareactivateddate as (
select
acp_id
, max(activated_date) activated_date
from ace_etl.customer_attributes_table
where activity_date_pacific between date'2024-09-16' and date'2024-10-15'
group by 1
);




create or replace temp view step4_flashSessionsAndCustIdFinder as (
select seeap.session_id
, coalesce(asm.new_acp_id, asm.max_shopper_id) session_cust_id
, seeap.digitalcontents_id 
, min(event_time_pst) min_event_time_pst
from acp_event_intermediate.session_evt_expanded_attributes_parquet seeap 
left join step1_sessionidmatching asm 
	on asm.session_id = seeap.session_id
inner join step0_allflashdates afd 
	on afd.digitalcontents_id = seeap.digitalcontents_id 
	and afd.activity_date_partition = seeap.activity_date_partition
where seeap.context_pagetype in ('FLASH_EVENTS', 'FLASH_EVENT_RESULTS')
	and seeap.channel = 'NORDSTROM_RACK'
	and seeap.activity_date_partition between date'2024-09-16' and date'2024-10-15'
	and digitalcontents_type = 'FLASH_EVENT'
group by 1,2,3
);


create or replace temp view step4_custIdFlashEventMatcher as (
select session_cust_id
, digitalcontents_id
, min(min_event_time_pst) min_event_time_pst
from step4_flashSessionsAndCustIdFinder
group by 1,2
);



create or replace temp view step4_eventFlashIdAllSessionMatcher as (
select 
asm.session_id
, asm.activity_date_partition
, ce.session_cust_id
, ce.digitalcontents_id
, ce.min_event_time_pst
from step1_sessionidmatching asm
inner join step4_custIdFlashEventMatcher  ce
	on coalesce(asm.new_acp_id, asm.max_shopper_id) = ce.session_cust_id
	and asm.activity_date_partition >= cast(ce.min_event_time_pst as DATE)
inner join step0_allflashdates afd 
	on afd.digitalcontents_id = ce.digitalcontents_id 
	and afd.activity_date_partition = asm.activity_date_partition
);


create or replace temp view step5_sessionInteractionsSessionLogic as (
select distinct
spdfa.session_id 
, afses.session_cust_id 
, spdfa.activity_date_partition
, spdfa.product_style_id 
, spdfa.imp_final_flag 
, spdfa.full_pdv_flag
, spdfa.atb_flag 
, spdfa.order_flag 
, spdfa.ttl_order_qty 
, spdfa.ttl_ord_value 
, spdfa.ttl_ord_demand 
, afses.digitalcontents_id
from ace_etl.session_product_discovery_funnel_analytical spdfa 
inner join step4_flashSessionsAndCustIdFinder afses 
	on afses.session_id = spdfa.session_id
where spdfa.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
);



create or replace temp view step5_sessionInteractionsEventLogic as (
select
afses.session_cust_id
, afses.digitalcontents_id
, spdfa.activity_date_partition
, spdfa.product_style_id 
, count(distinct spdfa.session_id) num_sessions
, MAX(spdfa.imp_final_flag) imp_final_flag
, MAX(spdfa.full_pdv_flag) full_pdv_flag
, MAX(spdfa.atb_flag) atb_flag
, MAX(spdfa.order_flag) order_flag
, SUM(spdfa.ttl_order_qty) ttl_order_qty
, SUM(spdfa.ttl_ord_value) ttl_ord_value
, SUM(spdfa.ttl_ord_demand) ttl_ord_demand 
from ace_etl.session_product_discovery_funnel_analytical spdfa 
inner join step4_eventFlashIdAllSessionMatcher afses 
	on afses.session_id = spdfa.session_id
where spdfa.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
group by 1,2,3,4
);

create or replace temp view step6_attributionLogicSessionLogic as (
select 
pobs.session_id
, pobs.session_cust_id
, pobs.activity_date_partition
, pobs.product_style_id 
, pobs.digitalcontents_id
, MIN(case when owfpdv.digitalcontents_id is not null then 1 
           when owfpdv.digitalcontents_id is null and fi.digitalcontents_id is not null then 2 
           else 3 
           end) as attribution_logic
, MAX(imp_final_flag) imp_final_flag
, MAX(full_pdv_flag) full_pdv_flag
, MAX(atb_flag) atb_flag
, MAX(order_flag) order_flag
, MAX(ttl_order_qty) ttl_order_qty
, MAX(ttl_ord_value) ttl_ord_value
, MAX(ttl_ord_demand) ttl_ord_demand
from step5_sessionInteractionsSessionLogic pobs
left join step2_flashclicks owfpdv
	on owfpdv.session_id = pobs.session_id
	and owfpdv.productstyle_id = cast(pobs.product_style_id as string)
	and owfpdv.digitalcontents_id = pobs.digitalcontents_id
left join step2_flashimpressions fi 
	on fi.session_id = pobs.session_id 
	and fi.productstyle_id = cast(pobs.product_style_id as string)
	and fi.digitalcontents_id = pobs.digitalcontents_id
where pobs.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
group by 1,2,3,4,5
); 


--also changed this

create or replace temp view step6_revisedEventTimeframes as (
select distinct digitalcontents_id
, activity_date_partition
from step6_attributionLogicSessionLogic
where ttl_ord_demand > 0 
);

create or replace temp view step6_attributionLogicEventLogic as (
select 
pobs.session_cust_id
, pobs.product_style_id 
, pobs.digitalcontents_id
, pobs.activity_date_partition
, MIN(case when owfpdv.digitalcontents_id is not null then 1 
           when owfpdv.digitalcontents_id is null and fi.digitalcontents_id is not null then 2 
           else 3 
           end) as attribution_logic
, MAX(imp_final_flag) imp_final_flag
, MAX(full_pdv_flag) full_pdv_flag
, MAX(atb_flag) atb_flag
, MAX(order_flag) order_flag
, MAX(ttl_order_qty) ttl_order_qty
, MAX(ttl_ord_value) ttl_ord_value
, MAX(ttl_ord_demand) ttl_ord_demand
, MAX(num_sessions) num_sessions
from step5_sessionInteractionsEventLogic pobs
left join step2_flashclicks owfpdv
	on owfpdv.session_cust_id = pobs.session_cust_id
	and owfpdv.productstyle_id = cast(pobs.product_style_id as string)
	and owfpdv.digitalcontents_id = pobs.digitalcontents_id
left join step2_flashimpressions fi 
	on fi.session_cust_id = pobs.session_cust_id 
	and fi.productstyle_id = cast(pobs.product_style_id as string)
	and fi.digitalcontents_id = pobs.digitalcontents_id
where pobs.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
group by 1,2,3,4
);


create or replace temp view stepexperience_experiencesessiontable as (
with date_para as (
SELECT  date'2024-09-16' AS start_date, date'2024-10-15' AS end_date    
),
first_flash_landing as (
select *
from
(select activity_date_partition, experience, digitalcontents_id as EID, session_id, event_time_pst, context_pagetype, pageinstance_id, page_rank, 
		arrival_rank, mrkt_type, finance_rollup, finance_detail,
		case when page_rank = 1 then 'Entry Page'  when arrival_rank is not null then 'External' else 'Internal' end as entry_flag,
		prior_pageinstance_id, 
		prior_context_pagetype, 
		case when prior_context_pagetype in ('FLASH_EVENTS','FLASH_EVENT_RESULTS','HOME','PRODUCT_DETAIL','CATEGORY_RESULTS','SEARCH_RESULTS','SHOPPING_BAG','BRAND_RESULTS') then prior_context_pagetype
				when prior_context_pagetype = 'SEARCH' then 'SHOP Tab'
				else 'OTHER'
		end as internal_source_pagetype,
		row_number() over(partition by digitalcontents_id, session_id, activity_date_partition order by event_time_pst) as rk
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where channel = 'NORDSTROM_RACK'
	and activity_date_partition between (select start_date from date_para) and (select end_date from date_para)
	and event_name IN ( 'com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected')
	and context_pagetype in ('FLASH_EVENTS', 'FLASH_EVENT_RESULTS')
	and digitalcontents_type = 'FLASH_EVENT'
	and productstyle_id is not NULL
)
where rk = 1
),
navigation_to_flash as (
select distinct activity_date_partition, session_id, pageinstance_id, element_id, 
		case when element_hierarchy = 'Flash Events'  or element_value in ('Flash Events: Get Inspired','Shop Limited-Time Flash Events') then 1 else 0 end as nav_to_flash_hp
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where channel = 'NORDSTROM_RACK'
	and activity_date_partition between (select start_date from date_para) and (select end_date from date_para)
	and event_name = 'com.nordstrom.event.customer.Engaged'
	and element_id = 'Global/Navigation/Section/Link'
	and split_part(element_hierarchy,'|',1) = 'Flash Events' 
),
source_of_flash_hp as (
select a.activity_date_partition, a.session_id, a.pageinstance_id, a.context_pagetype,
		case when a.entry_flag in ('Entry Page', 'External' ) then 'External' else 'Internal' end as F_hp_Source_Flag, 
		mrkt_type as F_hp_mrkt_type, finance_rollup as F_hp_finance_rollup, finance_detail as F_hp_finance_detail,
		case when entry_flag = 'Internal' and b.pageinstance_id is not null then 'Navigation' when entry_flag != 'Internal' then null else 'Non-Nav' end as F_hp_Nav_flag
from 
	(select distinct activity_date_partition, session_id, pageinstance_id, context_pagetype,
		arrival_rank, mrkt_type, finance_rollup, finance_detail,
		case when pageinstance_rank = 1 then 'Entry Page'  when arrival_rank is not null then 'External' else 'Internal' end as entry_flag,
		source_pageinstance_id, source_pagetype, 
		case when source_pagetype in ('FLASH_EVENTS','FLASH_EVENT_RESULTS','HOME','PRODUCT_DETAIL','CATEGORY_RESULTS','SEARCH_RESULTS','SHOPPING_BAG','BRAND_RESULTS') then source_pagetype
				when source_pagetype = 'SEARCH' then 'SHOP Tab'
				else 'OTHER'
		end as internal_source_pagetype
	from acp_event_intermediate.session_page_attributes_parquet
	where channel = 'NORDSTROM_RACK'
		and activity_date_partition between (select start_date from date_para) and (select end_date from date_para)
		and context_pagetype = 'FLASH_EVENTS'
	) a
left join 
(select distinct activity_date_partition, session_id, pageinstance_id from navigation_to_flash) b
on a.activity_date_partition = b.activity_date_partition
	and a.session_id = b.session_id
	and a.source_pageinstance_id = b.pageinstance_id
)
select activity_date_partition, 
		EID, 
		session_id, 
		experience,
		case when entry_flag in ('Entry Page', 'External' ) then 'External' else 'Internal' end as Source_Flag,
		mrkt_type,
		finance_rollup,
		finance_detail,
		case when entry_flag = 'Internal' then internal_source_pagetype end as internal_source_pagetype,
		Nav_flag,
		case when entry_flag = 'Internal' and prior_context_pagetype = 'FLASH_EVENTS' then F_hp_Source_Flag end as F_hp_Source_Flag, 
		case when entry_flag = 'Internal' and prior_context_pagetype = 'FLASH_EVENTS' then F_hp_mrkt_type end as F_hp_mrkt_type, 
		case when entry_flag = 'Internal' and prior_context_pagetype = 'FLASH_EVENTS' then F_hp_finance_rollup end as F_hp_finance_rollup, 
		case when entry_flag = 'Internal' and prior_context_pagetype = 'FLASH_EVENTS' then F_hp_finance_detail end as F_hp_finance_detail, 
		case when entry_flag = 'Internal' and prior_context_pagetype = 'FLASH_EVENTS' then F_hp_Nav_flag end as F_hp_Nav_flag
from 
	(select a.*, 
		case when a.entry_flag = 'Internal' and b.pageinstance_id is not null then 'Navigation' when entry_flag != 'Internal' then null else 'Non-Nav' end as Nav_flag,
		F_hp_Source_Flag, F_hp_mrkt_type, F_hp_finance_rollup, F_hp_finance_detail, F_hp_Nav_flag
	from first_flash_landing a
	left join (select distinct * from navigation_to_flash where nav_to_flash_hp = 0) b
	on a.activity_date_partition = b.activity_date_partition
		and a.session_id = b.session_id
		and a.prior_pageinstance_id = b.pageinstance_id
	left join source_of_flash_hp c
	on a.activity_date_partition = c.activity_date_partition
		and a.session_id = c.session_id
		and a.prior_pageinstance_id = c.pageinstance_id)
);




create or replace temp view stepexperience_experienceeventtable as (
with date_para as (
SELECT  date'2024-09-16' AS start_date, date'2024-10-15' AS end_date    
),
first_flash_landing_session as (
select *
from
(select activity_date_partition, experience, digitalcontents_id as EID, session_id, event_time_pst, context_pagetype, pageinstance_id, page_rank, 
		arrival_rank, mrkt_type, finance_rollup, finance_detail,
		case when page_rank = 1 then 'Entry Page'  when arrival_rank is not null then 'External' else 'Internal' end as entry_flag,
		prior_pageinstance_id, 
		prior_context_pagetype, 
		case when prior_context_pagetype in ('FLASH_EVENTS','FLASH_EVENT_RESULTS','HOME','PRODUCT_DETAIL','CATEGORY_RESULTS','SEARCH_RESULTS','SHOPPING_BAG','BRAND_RESULTS') then prior_context_pagetype
				when prior_context_pagetype = 'SEARCH' then 'SHOP Tab'
				else 'OTHER'
		end as internal_source_pagetype,
		row_number() over(partition by digitalcontents_id, session_id, activity_date_partition order by event_time_pst) as rk
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where channel = 'NORDSTROM_RACK'
	and activity_date_partition between (select start_date from date_para) and (select end_date from date_para)
	and event_name IN ( 'com.nordstrom.event.customer.ProductSummaryCollectionViewed', 'com.nordstrom.event.customer.ProductSummarySelected')
	and context_pagetype in ('FLASH_EVENTS', 'FLASH_EVENT_RESULTS')
	and digitalcontents_type = 'FLASH_EVENT'
	and productstyle_id is not NULL
)
where rk = 1
),
first_flash_landing_cust as (
select *
from
	(
	select a.activity_date_partition, experience, EID, coalesce(b.new_acp_id, b.max_shopper_id) session_cust_id, a.session_id, event_time_pst, context_pagetype, pageinstance_id, page_rank, 
		arrival_rank, mrkt_type, finance_rollup, finance_detail,
		case when page_rank = 1 then 'Entry Page'  when arrival_rank is not null then 'External' else 'Internal' end as entry_flag,
		prior_pageinstance_id, 
		prior_context_pagetype, 
		case when prior_context_pagetype in ('FLASH_EVENTS','FLASH_EVENT_RESULTS','HOME','PRODUCT_DETAIL','CATEGORY_RESULTS','SEARCH_RESULTS','SHOPPING_BAG','BRAND_RESULTS') then prior_context_pagetype
				when prior_context_pagetype = 'SEARCH' then 'SHOP Tab'
				else 'OTHER'
		end as internal_source_pagetype,
		row_number() over(partition by EID, coalesce(b.new_acp_id, b.max_shopper_id) order by event_time_pst) as rk
	from first_flash_landing_session a
	left join step1_sessionidmatching b
	on a.session_id = b.session_id
	)
where rk = 1
order by 3,4,1,6
),
navigation_to_flash as (
select distinct activity_date_partition, session_id, pageinstance_id, element_id, 
		case when element_hierarchy = 'Flash Events'  or element_value in ('Flash Events: Get Inspired','Shop Limited-Time Flash Events') then 1 else 0 end as nav_to_flash_hp
from acp_event_intermediate.session_evt_expanded_attributes_parquet
where channel = 'NORDSTROM_RACK'
	and activity_date_partition between (select start_date from date_para) and (select end_date from date_para)
	and event_name = 'com.nordstrom.event.customer.Engaged'
	and element_id = 'Global/Navigation/Section/Link'
	and split_part(element_hierarchy,'|',1) = 'Flash Events' 
)
select 
		EID, 
		session_cust_id,
		session_id as first_session,
		activity_date_partition, 
		experience,
		case when entry_flag in ('Entry Page', 'External' ) then 'External' else 'Internal' end as Source_Flag,
		mrkt_type,
		finance_rollup,
		finance_detail,
		case when entry_flag = 'Internal' then internal_source_pagetype end as internal_source_pagetype,
		Nav_flag
from 
	(select a.*, 
			case when a.entry_flag = 'Internal' and b.pageinstance_id is not null then 'Navigation' when entry_flag != 'Internal' then null else 'Non-Nav' end as Nav_flag
	from first_flash_landing_cust a 
	left join (select distinct * from navigation_to_flash where nav_to_flash_hp = 0) b
	on a.activity_date_partition = b.activity_date_partition
		and a.session_id = b.session_id
		and a.prior_pageinstance_id = b.pageinstance_id
	)
);


--inner joins on this added to remove the full scope

create or replace temp view step9_sessionInteractionsSessionsLogic as (
with ranking as (
select alsl.session_id
, alsl.session_cust_id
, alsl.activity_date_partition
, alsl.digitalcontents_id
, alsl.product_style_id
, alsl.attribution_logic 
, bf.branded_flag 
, step4.min_event_time_pst 
, row_number() over(partition by alsl.product_style_id, alsl.session_id order by alsl.attribution_logic asc, bf.branded_flag asc, step4.min_event_time_pst asc) as rk
from step6_attributionLogicSessionLogic alsl
left join step0_flashbrandedflag bf 
	on bf.digitalcontents_id = alsl.digitalcontents_id
left join step4_flashSessionsAndCustIdFinder step4 
	on step4.session_id = alsl.session_id
	and step4.digitalcontents_id = alsl.digitalcontents_id
	and step4.session_cust_id = alsl.session_cust_id
), already_done_attribution as (
select 
session_id
, session_cust_id
, activity_date_partition
, digitalcontents_id
, product_style_id
, attribution_logic
from ranking 
where rk = 1 
)
select 
spdfa.session_id 
, coalesce(asm.new_acp_id, asm.max_shopper_id) session_cust_id
, case when ada.attribution_logic is not null then 1 else 0 end as flash_session_flag
, case when ada.attribution_logic is not null then ada.attribution_logic else 3 end attribution_logic
, spdfa.product_style_id 
, ada.digitalcontents_id
, spdfa.imp_final_flag 
, spdfa.full_pdv_flag
, spdfa.atb_flag 
, spdfa.order_flag 
, spdfa.ttl_order_qty 
, spdfa.ttl_ord_value 
, spdfa.ttl_ord_demand 
, spdfa.activity_date_partition
from ace_etl.session_product_discovery_funnel_analytical spdfa 
inner join (select distinct session_id from step4_flashSessionsAndCustIdFinder) afses 
	on afses.session_id = spdfa.session_id
left join step1_sessionidmatching asm 
	on asm.session_id = spdfa.session_id
left join already_done_attribution ada 
	on ada.session_id = spdfa.session_id 
	and ada.session_cust_id = coalesce(asm.new_acp_id, asm.max_shopper_id)
	and ada.product_style_id = spdfa.product_style_id
	and ada.activity_date_partition = spdfa.activity_date_partition
where spdfa.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
and spdfa.channel = 'NORDSTROM_RACK'
);


create or replace temp view step9_flashoverviewtable_sessionlogic_merchgrouping as (
select 
testing.flash_session_flag
, testing.digitalcontents_id
, dmt.brand_name
, concat(cast(dmt.div_num as string), ': ', dmt.div_desc) div_desc
, concat(cast(dmt.grp_num as string), ': ', dmt.grp_desc) grp_desc
, concat(cast(dmt.dept_num as string), ': ', dmt.dept_desc) dept_desc
, concat(cast(dmt.class_num as string), ': ', dmt.class_desc) class_desc
, concat(cast(dmt.sbclass_num as string), ': ', dmt.sbclass_desc) sbclass_desc 
, case when dmt.sku_count_new is null then 'Unknown'
       when dmt.sku_count_new = 0 then 'Not-New'
       when dmt.sku_count_new > 0 then 'New'
       end new_flag
, case when afs.digitalcontents_id is not null then cast(testing.product_style_id as string) else 'non-flash-item' end as product_style_id
, case when afs.digitalcontents_id is not null then cast(dmt.style_group_desc as string) else 'non-flash-item' end as style_group_desc
, case when afs.digitalcontents_id is not null then cast(dmt.style_group_num as string) else 'non-flash-item' end as style_group_num
, dmt.quantrix_category
, dmt.rack_role_desc
, dmt.parent_group
, s.source_flag
, s.mrkt_type
, s.finance_rollup
, s.finance_detail
, s.internal_source_pagetype
, s.nav_flag
, s.experience
, SUM(case when attribution_logic = 3 then imp_final_flag end) halo_imp_sessions
, SUM(case when attribution_logic = 2 then imp_final_flag end) indirect_imp_sessions
, SUM(case when attribution_logic = 1 then imp_final_flag end) direct_imp_sessions
, SUM(case when attribution_logic = 3 then full_pdv_flag end) halo_pdv_sessions
, SUM(case when attribution_logic = 2 then full_pdv_flag end) indirect_pdv_sessions
, SUM(case when attribution_logic = 1 then full_pdv_flag end) direct_pdv_sessions
, SUM(case when attribution_logic = 3 then atb_flag end) halo_atb_sessions
, SUM(case when attribution_logic = 2 then atb_flag end) indirect_atb_sessions
, SUM(case when attribution_logic = 1 then atb_flag end) direct_atb_sessions
, SUM(case when attribution_logic = 3 then order_flag end) halo_order_sessions
, SUM(case when attribution_logic = 2 then order_flag end) indirect_order_sessions
, SUM(case when attribution_logic = 1 then order_flag end) direct_order_sessions
, SUM(case when attribution_logic = 3 then ttl_order_qty end) halo_order_qty
, SUM(case when attribution_logic = 2 then ttl_order_qty end) indirect_order_qty
, SUM(case when attribution_logic = 1 then ttl_order_qty end) direct_order_qty
, SUM(case when attribution_logic = 3 then ttl_ord_value end) halo_order_value
, SUM(case when attribution_logic = 2 then ttl_ord_value end) indirect_order_value
, SUM(case when attribution_logic = 1 then ttl_ord_value end) direct_order_value
, SUM(case when attribution_logic = 3 then ttl_ord_demand end) halo_order_demand
, SUM(case when attribution_logic = 2 then ttl_ord_demand end) indirect_order_demand
, SUM(case when attribution_logic = 1 then ttl_ord_demand end) direct_order_demand
, testing.activity_date_partition
from step9_sessionInteractionsSessionsLogic testing
left join ace_etl.digital_merch_table dmt 
	on dmt.web_style_num = cast(testing.product_style_id as string)
	and dmt.day_date = testing.activity_date_partition
	and dmt.selling_channel = 'NORDSTROM_RACK'
left join stepexperience_experiencesessiontable s
	on testing.activity_date_partition = s.activity_date_partition
	and testing.digitalcontents_id = s.EID
	and testing.session_id = s.session_id 	
left join step0_allflashstyles afs
	on afs.digitalcontents_id = testing.digitalcontents_id 
	and afs.productstyle_id = cast(testing.product_style_id as string)
where testing.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,44
);





create or replace temp view step9_flashoverviewtable_sessionlogic_customergrouping as (
select 
testing.flash_session_flag
,testing.digitalcontents_id
, cat.engagement_cohort
, cat.loyalty_status
, cat.predicted_ct_segment 
, case when aare_acquired.acp_id is not null then 1 else 0 end as aare_acquired
, case when aare_activated.acp_id is not null then 1 else 0 end as aare_activated 
, s.source_flag
, s.mrkt_type
, s.finance_rollup
, s.finance_detail
, s.internal_source_pagetype
, s.nav_flag
, s.experience
, count(distinct testing.session_id) num_sessions_ttl
, count(distinct case when attribution_logic in (1,2) then testing.session_id end) num_sessions
, count(distinct case when testing.imp_final_flag = 1 then testing.session_id end) num_imp_sessions_ttl
, count(distinct case when attribution_logic = 1  and testing.imp_final_flag = 1 then testing.session_id end) num_imp_sessions_direct
, count(distinct case when attribution_logic in (1,2) and testing.imp_final_flag = 1 then testing.session_id end) num_imp_sessions
, count(distinct case when attribution_logic = 3  and testing.imp_final_flag = 1 then testing.session_id end) num_imp_sessions_halo
, count(distinct case when testing.full_pdv_flag = 1 then testing.session_id end) num_pdv_sessions_ttl
, count(distinct case when attribution_logic = 1  and testing.full_pdv_flag = 1 then testing.session_id end) num_pdv_sessions_direct
, count(distinct case when attribution_logic in (1,2) and testing.full_pdv_flag = 1 then testing.session_id end) num_pdv_sessions
, count(distinct case when attribution_logic = 3  and testing.full_pdv_flag = 1 then testing.session_id end) num_pdv_sessions_halo
, count(distinct case when testing.atb_flag = 1 then testing.session_id end) num_atb_sessions_ttl
, count(distinct case when attribution_logic = 1  and testing.atb_flag = 1 then testing.session_id end) num_atb_sessions_direct
, count(distinct case when attribution_logic in (1,2) and testing.atb_flag = 1 then testing.session_id end) num_atb_sessions
, count(distinct case when attribution_logic = 3  and testing.atb_flag = 1 then testing.session_id end) num_atb_sessions_halo
, count(distinct case when testing.order_flag = 1 then testing.session_id end) num_order_sessions_ttl
, count(distinct case when attribution_logic = 1  and testing.order_flag = 1 then testing.session_id end) num_order_sessions_direct
, count(distinct case when attribution_logic in (1,2) and testing.order_flag = 1 then testing.session_id end) num_order_sessions
, count(distinct case when attribution_logic = 3  and testing.order_flag = 1 then testing.session_id end) num_order_sessions_halo
, SUM(case when attribution_logic = 3 then imp_final_flag end) halo_imp_sessions
, SUM(case when attribution_logic = 2 then imp_final_flag end) indirect_imp_sessions
, SUM(case when attribution_logic = 1 then imp_final_flag end) direct_imp_sessions
, SUM(case when attribution_logic = 3 then full_pdv_flag end) halo_pdv_sessions
, SUM(case when attribution_logic = 2 then full_pdv_flag end) indirect_pdv_sessions
, SUM(case when attribution_logic = 1 then full_pdv_flag end) direct_pdv_sessions
, SUM(case when attribution_logic = 3 then atb_flag end) halo_atb_sessions
, SUM(case when attribution_logic = 2 then atb_flag end) indirect_atb_sessions
, SUM(case when attribution_logic = 1 then atb_flag end) direct_atb_sessions
, SUM(case when attribution_logic = 3 then order_flag end) halo_order_sessions
, SUM(case when attribution_logic = 2 then order_flag end) indirect_order_sessions
, SUM(case when attribution_logic = 1 then order_flag end) direct_order_sessions
, SUM(case when attribution_logic = 3 then ttl_order_qty end) halo_order_qty
, SUM(case when attribution_logic = 2 then ttl_order_qty end) indirect_order_qty
, SUM(case when attribution_logic = 1 then ttl_order_qty end) direct_order_qty
, SUM(case when attribution_logic = 3 then ttl_ord_value end) halo_order_value
, SUM(case when attribution_logic = 2 then ttl_ord_value end) indirect_order_value
, SUM(case when attribution_logic = 1 then ttl_ord_value end) direct_order_value
, SUM(case when attribution_logic = 3 then ttl_ord_demand end) halo_order_demand
, SUM(case when attribution_logic = 2 then ttl_ord_demand end) indirect_order_demand
, SUM(case when attribution_logic = 1 then ttl_ord_demand end) direct_order_demand
, testing.activity_date_partition
from step9_sessionInteractionsSessionsLogic testing
left join step3_customerattributes cat 
	on cat.acp_id = testing.session_cust_id
	and testing.activity_date_partition = cat.activity_date_pacific
left join (select digitalcontents_id, min(activity_date_partition) min_date, max(activity_date_partition) max_date from step0_allflashdates group by 1) fdd 
	on fdd.digitalcontents_id = testing.digitalcontents_id
left join step3_aareacquireddate aare_acquired
	on aare_acquired.acp_id = testing.session_cust_id 
	and aare_acquired.aare_status_date between fdd.min_date and fdd.max_date
left join step3_aareactivateddate aare_activated
	on aare_activated.acp_id = testing.session_cust_id 
	and aare_activated.activated_date between fdd.min_date and fdd.max_date
left join stepexperience_experiencesessiontable s
	on testing.activity_date_partition = s.activity_date_partition
	and testing.digitalcontents_id = s.EID
	and testing.session_id = s.session_id 
where testing.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,54
);



insert OVERWRITE TABLE ace_etl.flashoverviewtable_sessionlogic_customergrouping PARTITION (activity_date_partition) 
select *
from step9_flashoverviewtable_sessionlogic_customergrouping
;

insert OVERWRITE TABLE ace_etl.flashoverviewtable_sessionlogic_merchgrouping PARTITION (activity_date_partition) 
select *
from step9_flashoverviewtable_sessionlogic_merchgrouping
;


MSCK REPAIR table ace_etl.flashoverviewtable_sessionlogic_customergrouping
;

MSCK REPAIR table ace_etl.flashoverviewtable_sessionlogic_merchgrouping
;





create or replace temp view step9_sessionInteractionsEventLogic as (
with ranking as (
select
alsl.session_cust_id
, alsl.activity_date_partition
, alsl.digitalcontents_id
, alsl.product_style_id
, alsl.attribution_logic 
, bf.branded_flag 
, step4.min_event_time_pst 
, row_number() over(partition by alsl.product_style_id, alsl.session_cust_id, alsl.activity_date_partition order by alsl.attribution_logic asc, bf.branded_flag asc, step4.min_event_time_pst asc) as rk
from step6_attributionLogicEventLogic alsl
left join step0_flashbrandedflag bf 
	on bf.digitalcontents_id = alsl.digitalcontents_id
left join (select distinct activity_date_partition, session_cust_id, digitalcontents_id, min_event_time_pst from step4_eventFlashIdAllSessionMatcher) step4 
	on step4.session_cust_id = alsl.session_cust_id
	and step4.digitalcontents_id = alsl.digitalcontents_id
	and step4.activity_date_partition = alsl.activity_date_partition
where alsl.activity_date_partition between  date'2024-09-16' + 12 and date'2024-10-15'
), already_done_attribution as (
select 
session_cust_id
, activity_date_partition
, digitalcontents_id
, product_style_id
, attribution_logic
from ranking 
where rk = 1 
)
select 
spdfa.session_id 
, coalesce(asm.new_acp_id, asm.max_shopper_id) session_cust_id
, case when ada.attribution_logic is not null then 1 else 0 end as flash_session_flag
, case when ada.attribution_logic is not null then ada.attribution_logic else 3 end attribution_logic
, spdfa.product_style_id 
, ada.digitalcontents_id
, spdfa.imp_final_flag 
, spdfa.full_pdv_flag
, spdfa.atb_flag 
, spdfa.order_flag 
, spdfa.ttl_order_qty 
, spdfa.ttl_ord_value 
, spdfa.ttl_ord_demand 
, spdfa.activity_date_partition
from ace_etl.session_product_discovery_funnel_analytical spdfa 
inner join (select distinct session_id from step4_eventFlashIdAllSessionMatcher) afses 
	on afses.session_id = spdfa.session_id
left join step1_sessionidmatching asm 
	on asm.session_id = spdfa.session_id
left join already_done_attribution ada  
	on ada.session_cust_id = coalesce(asm.new_acp_id, asm.max_shopper_id)
	and ada.product_style_id = spdfa.product_style_id
	and ada.activity_date_partition = spdfa.activity_date_partition
where spdfa.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
	and spdfa.channel = 'NORDSTROM_RACK'
);




create or replace temp view step9_flashoverviewtable_eventlogic_merchgrouping as (
select 
testing.flash_session_flag
, testing.digitalcontents_id
, dmt.brand_name
, concat(cast(dmt.div_num as string), ': ', dmt.div_desc) div_desc
, concat(cast(dmt.grp_num as string), ': ', dmt.grp_desc) grp_desc
, concat(cast(dmt.dept_num as string), ': ', dmt.dept_desc) dept_desc
, concat(cast(dmt.class_num as string), ': ', dmt.class_desc) class_desc
, concat(cast(dmt.sbclass_num as string), ': ', dmt.sbclass_desc) sbclass_desc 
, case when dmt.sku_count_new is null then 'Unknown'
       when dmt.sku_count_new = 0 then 'Not-New'
       when dmt.sku_count_new > 0 then 'New'
       end new_flag
, case when afs.digitalcontents_id is not null then cast(testing.product_style_id as string) else 'non-flash-item' end as product_style_id
, case when afs.digitalcontents_id is not null then cast(dmt.style_group_desc as string) else 'non-flash-item' end as style_group_desc
, case when afs.digitalcontents_id is not null then cast(dmt.style_group_num as string) else 'non-flash-item' end as style_group_num
, dmt.quantrix_category
, dmt.rack_role_desc
, dmt.parent_group
, s.source_flag
, s.mrkt_type
, s.finance_rollup
, s.finance_detail
, s.internal_source_pagetype
, s.nav_flag
, s.experience
, SUM(case when attribution_logic = 3 then imp_final_flag end) halo_imp_sessions
, SUM(case when attribution_logic = 2 then imp_final_flag end) indirect_imp_sessions
, SUM(case when attribution_logic = 1 then imp_final_flag end) direct_imp_sessions
, SUM(case when attribution_logic = 3 then full_pdv_flag end) halo_pdv_sessions
, SUM(case when attribution_logic = 2 then full_pdv_flag end) indirect_pdv_sessions
, SUM(case when attribution_logic = 1 then full_pdv_flag end) direct_pdv_sessions
, SUM(case when attribution_logic = 3 then atb_flag end) halo_atb_sessions
, SUM(case when attribution_logic = 2 then atb_flag end) indirect_atb_sessions
, SUM(case when attribution_logic = 1 then atb_flag end) direct_atb_sessions
, SUM(case when attribution_logic = 3 then order_flag end) halo_order_sessions
, SUM(case when attribution_logic = 2 then order_flag end) indirect_order_sessions
, SUM(case when attribution_logic = 1 then order_flag end) direct_order_sessions
, SUM(case when attribution_logic = 3 then ttl_order_qty end) halo_order_qty
, SUM(case when attribution_logic = 2 then ttl_order_qty end) indirect_order_qty
, SUM(case when attribution_logic = 1 then ttl_order_qty end) direct_order_qty
, SUM(case when attribution_logic = 3 then ttl_ord_value end) halo_order_value
, SUM(case when attribution_logic = 2 then ttl_ord_value end) indirect_order_value
, SUM(case when attribution_logic = 1 then ttl_ord_value end) direct_order_value
, SUM(case when attribution_logic = 3 then ttl_ord_demand end) halo_order_demand
, SUM(case when attribution_logic = 2 then ttl_ord_demand end) indirect_order_demand
, SUM(case when attribution_logic = 1 then ttl_ord_demand end) direct_order_demand
, testing.activity_date_partition
from step9_sessionInteractionsEventLogic testing
left join ace_etl.digital_merch_table dmt 
	on dmt.web_style_num = cast(testing.product_style_id as string)
	and dmt.day_date = testing.activity_date_partition
	and dmt.selling_channel = 'NORDSTROM_RACK'
left join stepexperience_experienceeventtable s
	on testing.digitalcontents_id = s.EID
	and testing.session_cust_id = s.session_cust_id 
left join step0_allflashstyles afs
	on afs.digitalcontents_id = testing.digitalcontents_id 
	and afs.productstyle_id = cast(testing.product_style_id as string)
where testing.activity_date_partition between  date'2024-09-16' + 12 and date'2024-10-15'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,44
);


create or replace temp view step9_flashoverviewtable_eventlogic_customergrouping as (
select 
testing.flash_session_flag
, testing.digitalcontents_id
, cat.engagement_cohort
, cat.loyalty_status
, cat.predicted_ct_segment 
, case when aare_acquired.acp_id is not null then 1 else 0 end as aare_acquired
, case when aare_activated.acp_id is not null then 1 else 0 end as aare_activated
, s.source_flag
, s.mrkt_type
, s.finance_rollup
, s.finance_detail
, s.internal_source_pagetype
, s.nav_flag
, s.experience
, count(distinct testing.session_cust_id) num_sessions_ttl
, count(distinct case when attribution_logic in (1,2) then testing.session_cust_id end) num_sessions
, count(distinct case when testing.imp_final_flag = 1 then testing.session_cust_id end) num_imp_sessions_ttl
, count(distinct case when attribution_logic = 1  and testing.imp_final_flag = 1 then testing.session_cust_id end) num_imp_sessions_direct
, count(distinct case when attribution_logic in (1,2) and testing.imp_final_flag = 1 then testing.session_cust_id end) num_imp_sessions
, count(distinct case when attribution_logic = 3  and testing.imp_final_flag = 1 then testing.session_cust_id end) num_imp_sessions_halo
, count(distinct case when testing.full_pdv_flag = 1 then testing.session_cust_id end) num_pdv_sessions_ttl
, count(distinct case when attribution_logic = 1  and testing.full_pdv_flag = 1 then testing.session_cust_id end) num_pdv_sessions_direct
, count(distinct case when attribution_logic in (1,2) and testing.full_pdv_flag = 1 then testing.session_cust_id end) num_pdv_sessions
, count(distinct case when attribution_logic = 3  and testing.full_pdv_flag = 1 then testing.session_cust_id end) num_pdv_sessions_halo
, count(distinct case when testing.atb_flag = 1 then testing.session_cust_id end) num_atb_sessions_ttl
, count(distinct case when attribution_logic = 1  and testing.atb_flag = 1 then testing.session_cust_id end) num_atb_sessions_direct
, count(distinct case when attribution_logic in (1,2) and testing.atb_flag = 1 then testing.session_cust_id end) num_atb_sessions
, count(distinct case when attribution_logic = 3  and testing.atb_flag = 1 then testing.session_cust_id end) num_atb_sessions_halo
, count(distinct case when testing.order_flag = 1 then testing.session_cust_id end) num_order_sessions_ttl
, count(distinct case when attribution_logic = 1  and testing.order_flag = 1 then testing.session_cust_id end) num_order_sessions_direct
, count(distinct case when attribution_logic in (1,2) and testing.order_flag = 1 then testing.session_cust_id end) num_order_sessions
, count(distinct case when attribution_logic = 3  and testing.order_flag = 1 then testing.session_cust_id end) num_order_sessions_halo
, SUM(case when attribution_logic = 3 then imp_final_flag end) halo_imp_sessions
, SUM(case when attribution_logic = 2 then imp_final_flag end) indirect_imp_sessions
, SUM(case when attribution_logic = 1 then imp_final_flag end) direct_imp_sessions
, SUM(case when attribution_logic = 3 then full_pdv_flag end) halo_pdv_sessions
, SUM(case when attribution_logic = 2 then full_pdv_flag end) indirect_pdv_sessions
, SUM(case when attribution_logic = 1 then full_pdv_flag end) direct_pdv_sessions
, SUM(case when attribution_logic = 3 then atb_flag end) halo_atb_sessions
, SUM(case when attribution_logic = 2 then atb_flag end) indirect_atb_sessions
, SUM(case when attribution_logic = 1 then atb_flag end) direct_atb_sessions
, SUM(case when attribution_logic = 3 then order_flag end) halo_order_sessions
, SUM(case when attribution_logic = 2 then order_flag end) indirect_order_sessions
, SUM(case when attribution_logic = 1 then order_flag end) direct_order_sessions
, SUM(case when attribution_logic = 3 then ttl_order_qty end) halo_order_qty
, SUM(case when attribution_logic = 2 then ttl_order_qty end) indirect_order_qty
, SUM(case when attribution_logic = 1 then ttl_order_qty end) direct_order_qty
, SUM(case when attribution_logic = 3 then ttl_ord_value end) halo_order_value
, SUM(case when attribution_logic = 2 then ttl_ord_value end) indirect_order_value
, SUM(case when attribution_logic = 1 then ttl_ord_value end) direct_order_value
, SUM(case when attribution_logic = 3 then ttl_ord_demand end) halo_order_demand
, SUM(case when attribution_logic = 2 then ttl_ord_demand end) indirect_order_demand
, SUM(case when attribution_logic = 1 then ttl_ord_demand end) direct_order_demand
, testing.activity_date_partition
from step9_sessionInteractionsEventLogic testing
left join step3_customerattributes cat 
	on cat.acp_id = testing.session_cust_id
	and testing.activity_date_partition = cat.activity_date_pacific
left join (select digitalcontents_id, min(activity_date_partition) min_date, max(activity_date_partition) max_date from step0_allflashdates group by 1) fdd 
	on fdd.digitalcontents_id = testing.digitalcontents_id
left join step3_aareacquireddate aare_acquired
	on aare_acquired.acp_id = testing.session_cust_id 
	and aare_acquired.aare_status_date between fdd.min_date and fdd.max_date
left join step3_aareactivateddate aare_activated
	on aare_activated.acp_id = testing.session_cust_id 
	and aare_activated.activated_date between fdd.min_date and fdd.max_date
left join stepexperience_experienceeventtable s
	on testing.digitalcontents_id = s.EID
	and testing.session_cust_id = s.session_cust_id 
where testing.activity_date_partition between date'2024-09-16' + 12 and date'2024-10-15'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,54
);




insert OVERWRITE TABLE ace_etl.flashoverviewtable_eventlogic_customergrouping PARTITION (activity_date_partition) 
select *
from step9_flashoverviewtable_eventlogic_customergrouping
;

insert OVERWRITE TABLE ace_etl.flashoverviewtable_eventlogic_merchgrouping PARTITION (activity_date_partition) 
select *
from step9_flashoverviewtable_eventlogic_merchgrouping
;


MSCK REPAIR table ace_etl.flashoverviewtable_eventlogic_customergrouping
;

MSCK REPAIR table ace_etl.flashoverviewtable_eventlogic_merchgrouping
;