/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09142;
     DAG_ID=digital_merch_table_11521_ACE_ENG;
     Task_Name=digital_merch_table;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_digitalmerch_flash
Team/Owner: Nicole Miao, Sean Larkin, Cassie Zhang, Rae Ann Boswell
Date Created/Modified: 01/24/2024, modified on 06/12/2024
*/


CREATE VOLATILE MULTISET TABLE sku_date_exp AS (
with Ncom_sku as (
select 
distinct CAST(BEGIN(eff_period) AS date) AS day_date,
		cast('NORDSTROM' as varchar(20)) as selling_channel,
		rms_sku_num,
		epm_style_num,
		web_style_num,
		partner_relationship_num,
		partner_relationship_type_code,
     	partner_relationship_type_code || '-' || trim(coalesce(partner_relationship_num, 0)) as partner_relationship_type,
        brand_name,
        div_num,
        div_desc,
        grp_num,
        grp_desc,
        dept_num,
        dept_desc,
        class_num,
        class_desc,
        sbclass_num,
        sbclass_desc,
        prmy_supp_num,
        selling_status_code,
        selling_status_desc, 
        drop_ship_eligible_ind,
        npg_ind,
        color_num
from prd_nap_usr_vws.product_sku_dim_hist EXPAND ON
        PERIOD(eff_begin_tmstp, case when year(eff_end_tmstp) = 9999 then eff_end_tmstp else eff_end_tmstp + interval '1' day end) AS eff_period
        BY interval '1' day 
        for PERIOD(CAST({start_date} AS TIMESTAMP(0)), CAST({end_date} AS TIMESTAMP(0)) + interval '1' day)
where channel_country = 'US'
	and oreplace(oreplace(lower(selling_channel_eligibility_list), ' ', ''), 'rackweb', 'rack online') like '%web%'
	and web_style_num is not null
	and eff_end_tmstp >= date'2022-01-29'
	AND dept_desc not like '%INACT%'
    AND grp_desc not like '%INACT%'
    AND div_desc not like '%INACT%'
),
    
Rcom_sku as (
select 
distinct CAST(BEGIN(eff_period) AS date) AS day_date,
		cast('NORDSTROM_RACK' as varchar(20)) as selling_channel,
		rms_sku_num,
		epm_style_num,
		web_style_num,
		partner_relationship_num,
		partner_relationship_type_code,
     	partner_relationship_type_code || '-' || trim(coalesce(partner_relationship_num, 0)) as partner_relationship_type,
        brand_name,
        div_num,
        div_desc,
        grp_num,
        grp_desc,
        dept_num,
        dept_desc,
        class_num,
        class_desc,
        sbclass_num,
        sbclass_desc,
        prmy_supp_num,
        selling_status_code,
        selling_status_desc, 
        drop_ship_eligible_ind,
        npg_ind,
        color_num
from prd_nap_usr_vws.product_sku_dim_hist EXPAND ON
        PERIOD(eff_begin_tmstp, case when year(eff_end_tmstp) = 9999 then eff_end_tmstp else eff_end_tmstp + interval '1' day end) AS eff_period
        BY interval '1' day 
        for PERIOD(CAST({start_date} AS TIMESTAMP(0)), CAST({end_date} AS TIMESTAMP(0)) + interval '1' day)
where channel_country = 'US'
	and oreplace(oreplace(lower(selling_channel_eligibility_list), ' ', ''), 'rackweb', 'rack online') like '%rack online%'
	and web_style_num is not null
	and eff_end_tmstp >= date'2022-01-29'
	AND dept_desc not like '%INACT%'
    AND grp_desc not like '%INACT%'
    AND div_desc not like '%INACT%'
)

select * from Ncom_sku
union all
select * from Rcom_sku
) WITH DATA PRIMARY INDEX(day_date, rms_sku_num, selling_channel) ON COMMIT PRESERVE ROWS;





CREATE VOLATILE MULTISET TABLE sku_style_mapping AS (
select day_date, rms_sku_num, selling_channel, web_style_num,partner_relationship_num, partner_relationship_type_code, partner_relationship_type, selling_status_code, selling_status_desc, drop_ship_eligible_ind, npg_ind, color_num
from
	(select day_date, rms_sku_num, selling_channel, web_style_num,partner_relationship_num, partner_relationship_type_code, partner_relationship_type, selling_status_code, selling_status_desc, drop_ship_eligible_ind, npg_ind, color_num
	from
		(select row_number() over(partition by day_date, rms_sku_num, selling_channel order by web_style_num, selling_status_code, selling_status_desc, drop_ship_eligible_ind, npg_ind, color_num) as rk, 
				a.*
		from sku_date_exp a
		) b
	where rk = 1
	) sku
) WITH DATA PRIMARY INDEX(day_date, rms_sku_num, selling_channel) ON COMMIT PRESERVE ROWS;




------------------------------------------------------------------------------
--- Merch Attributes
------------------------------------------------------------------------------
CREATE VOLATILE MULTISET TABLE style_sku_cnt AS (
select day_date, selling_channel, web_style_num,
	max(case when npg_ind = 'Y' then 1 else 0 end) as npg_ind,
	count(distinct rms_sku_num) as sku_count,
	count(distinct color_num) as color_count,
	count(distinct case when selling_status_code  = 'T1' then  rms_sku_num else null end ) as sku_count_DS,
	count(distinct case when selling_status_code = 'S1' then  rms_sku_num else null end ) as sku_count_Unsellable,
	count(distinct case when selling_status_code = 'C3' then  rms_sku_num else null end ) as sku_count_Owned,
	count(distinct case when selling_status_code= 'T1' then  color_num else null end) as color_count_DS,
	count(distinct case when selling_status_code= 'S1' then  color_num else null end) as color_count_Unsellable,
	count(distinct case when selling_status_code= 'C3' then  color_num else null end) as color_count_Owned,
	count(distinct case when partner_relationship_type_code = 'ECONCESSION' then rms_sku_num else null end) as sku_count_mp,
    count(distinct case when partner_relationship_type_code = 'UNKNOWN' then rms_sku_num else null end) as sku_count_nmp,
    count(distinct case when partner_relationship_type_code = 'ECONCESSION' then color_num else null end) as color_count_mp,
    count(distinct case when partner_relationship_type_code = 'UNKNOWN' then color_num else null end) as color_count_nmp
from sku_style_mapping
group by 1,2,3
) WITH DATA PRIMARY INDEX(day_date, web_style_num, selling_channel) ON COMMIT PRESERVE ROWS;



CREATE VOLATILE MULTISET TABLE style_base AS (
select day_date,
		web_style_num,
        partner_relationship_type,
		selling_channel,
		epm_style_num,
        brand_name,
        div_num,
        div_desc,
        grp_num,
        grp_desc,
        dept_num,
        dept_desc,
        class_num,
        class_desc,
        sbclass_num,
        sbclass_desc,
        prmy_supp_num
from
(select row_number()
		over(partition by day_date, web_style_num, selling_channel
		order by ranking_flag desc, epm_style_num, div_num, dept_num, class_num, sbclass_num, brand_name, prmy_supp_num
		) as rk,
		a.*
from
	(select distinct day_date,
			web_style_num,
			partner_relationship_num,
			partner_relationship_type_code,
            partner_relationship_type_code || '-' || trim(coalesce(partner_relationship_num, 0)) as partner_relationship_type,
            case when partner_relationship_num is not null then 1 else 0 end as ranking_flag,
			selling_channel,
			epm_style_num,
	        brand_name,
	        div_num,
	        div_desc,
	        grp_num,
	        grp_desc,
	        dept_num,
	        dept_desc,
	        class_num,
	        class_desc,
	        sbclass_num,
	        sbclass_desc,
	        prmy_supp_num
	from sku_date_exp
	) a
) b
where rk = 1
) WITH DATA PRIMARY INDEX(day_date, web_style_num, selling_channel) ON COMMIT PRESERVE ROWS;



CREATE VOLATILE MULTISET TABLE epm_style_date_exp AS (
select 
distinct CAST(BEGIN(eff_period) AS date) AS day_date,
		epm_style_num,
        style_group_num,
        style_group_desc,
        style_group_short_desc,
        type_level_1_num,
		type_level_1_desc,
		type_level_2_num,
		type_level_2_desc,
		genders_code,
		genders_desc,
		age_groups_code,
		age_groups_desc
from prd_nap_usr_vws.product_style_dim_hist EXPAND ON
        PERIOD(eff_begin_tmstp, case when year(eff_end_tmstp) = 9999 then eff_end_tmstp else eff_end_tmstp + interval '1' day end) AS eff_period
        BY interval '1' day 
        for PERIOD(CAST({start_date} AS TIMESTAMP(0)), CAST({end_date} AS TIMESTAMP(0)) + interval '1' day)
where channel_country = 'US'
	and epm_style_num is not null
	and eff_end_tmstp >= date'2022-01-29'
	AND dept_desc not like '%INACT%'
    AND grp_desc not like '%INACT%'
    AND div_desc not like '%INACT%'
) WITH DATA PRIMARY INDEX(day_date, epm_style_num) ON COMMIT PRESERVE ROWS;




CREATE VOLATILE MULTISET TABLE emp_base AS (
select day_date, 
		epm_style_num,
        style_group_num,
        style_group_desc,
        style_group_short_desc,
        type_level_1_num,
		type_level_1_desc,
		type_level_2_num,
		type_level_2_desc,
		genders_code,
		genders_desc,
		age_groups_code,
		age_groups_desc
from
(select row_number() over(partition by day_date, epm_style_num order by style_group_num, style_group_desc, type_level_1_num, type_level_2_num, genders_code, age_groups_code) as rk, 
		a.*
from epm_style_date_exp a
) b
where rk = 1
) WITH DATA PRIMARY INDEX(day_date, epm_style_num) ON COMMIT PRESERVE ROWS;




CREATE VOLATILE MULTISET TABLE style_merch_hierarchy AS (
select distinct sb.day_date,
		sb.web_style_num,
		sb.selling_channel,
		sb.epm_style_num,
		sb.partner_relationship_type,
		sb.brand_name,
	    sb.div_num,
	    sb.div_desc,
	    sb.grp_num,
	    sb.grp_desc,
	    sb.dept_num,
	    sb.dept_desc,
	    sb.class_num,
	    sb.class_desc,
	    sb.sbclass_num,
	    sb.sbclass_desc,
	    sb.prmy_supp_num,
		v.vender_name,
        eb.style_group_num,
        eb.style_group_desc,
        eb.style_group_short_desc,
        eb.type_level_1_num,
		eb.type_level_1_desc,
		eb.type_level_2_num,
		eb.type_level_2_desc,
		eb.genders_code,
		eb.genders_desc,
		eb.age_groups_code,
		eb.age_groups_desc,
		mt.QUANTRIX_CATEGORY,
		mt.CCS_CATEGORY,
		mt.CCS_SUBCATEGORY,
		mt.NORD_ROLE,
		mt.NORD_ROLE_DESC,
		mt.RACK_ROLE,
		mt.RACK_ROLE_DESC,
		mt.PARENT_GROUP,
		mt.MERCH_THEMES,
		mt.ANNIVERSARY_21,
		mt.ANNIVERSARY_THEME,
		mt.HOLIDAY_21,
		mt.HOLIDAY_THEME_TY,
		mt.GIFT_IND,
		mt.STOCKING_STUFFER,
		mt.ASSORTMENT_GROUPING,
		sc.npg_ind,
		sc.sku_count,
		sc.color_count,
		sc.sku_count_DS,
		sc.sku_count_Unsellable,
		sc.sku_count_Owned,
		sc.color_count_DS,
		sc.color_count_Unsellable,
		sc.color_count_Owned,
		sc.sku_count_mp,
        sc.sku_count_nmp,
        sc.color_count_mp,
        sc.color_count_nmp
from style_base sb
left join emp_base eb
on sb.day_date = eb.day_date
	and sb.epm_style_num = eb.epm_style_num
left join 
	(SELECT vendor_num, min(vendor_name) as vender_name
	FROM PRD_NAP_USR_VWS.VENDOR_DIM
	group by 1
	) v
on sb.prmy_supp_num = v.vendor_num
LEFT JOIN  t2dl_das_po_visibility.ccs_merch_themes mt
	ON mt.div_idnt = sb.div_num
	AND mt.sdiv_idnt = sb.grp_num
	AND mt.dept_idnt = sb.dept_num
	AND mt.class_idnt = sb.class_num
	AND mt.sbclass_idnt = sb.sbclass_num
left join style_sku_cnt sc
on sb.day_date = sc.day_date
	and sb.web_style_num = sc.web_style_num
	and sb.selling_channel = sc.selling_channel
) WITH DATA PRIMARY INDEX(day_date, web_style_num, selling_channel) ON COMMIT PRESERVE ROWS;

collect statistics primary index (day_date, web_style_num, selling_channel)
  , column(day_date)
  , column(web_style_num)
  , column(selling_channel)
on style_merch_hierarchy;




CREATE VOLATILE MULTISET TABLE cc_lkp AS (
SELECT cc, min(web_style_num) as web_style_num
FROM T2DL_DAS_MARKDOWN.SKU_LKP
WHERE cc IS NOT NULL
    AND channel_country = 'US'
group by 1
) WITH DATA PRIMARY INDEX(cc) ON COMMIT PRESERVE ROWS;


------------------------------------------------------------------------------
--- Sold Units
------------------------------------------------------------------------------
create multiset volatile table daily_sold_units as (
SELECT case when source_channel_code = 'FULL_LINE' then 'NORDSTROM'
			when source_channel_code = 'RACK' then 'NORDSTROM_RACK' 
		end as channel_brand,
		web_style_num,
		ORDER_DATE_PACIFIC as day_date,
		sum(ORDER_LINE_QUANTITY) as unfiltered_units,
		count(distinct o.order_num) as unfiltered_orders,
		sum(order_line_amount_usd) as unfiltered_demand,

		sum(case when coalesce(o.gift_with_purchase_ind, 'N') = 'N' and coalesce(o.beauty_sample_ind, 'N') = 'N' and coalesce(o.source_platform_code, '') <> 'POS' AND COALESCE(fraud_cancel_ind,'N')='N'  then ORDER_LINE_QUANTITY else 0 end) as filtered_units,
		count(distinct case when coalesce(o.gift_with_purchase_ind, 'N') = 'N' and coalesce(o.beauty_sample_ind, 'N') = 'N' and coalesce(o.source_platform_code, '') <> 'POS' AND COALESCE(fraud_cancel_ind,'N')='N'  then  o.order_num else null end) as filtered_orders,
		sum(case when coalesce(o.gift_with_purchase_ind, 'N') = 'N' and coalesce(o.beauty_sample_ind, 'N') = 'N' and coalesce(o.source_platform_code, '') <> 'POS' AND COALESCE(fraud_cancel_ind,'N')='N'  then order_line_amount_usd else 0 end) as filtered_demand,

		sum(case when FIRST_RELEASED_NODE_TYPE_CODE = 'DS' then ORDER_LINE_QUANTITY else 0 end ) as ds_fulfilled_unfiltered_units,
		count(distinct case when FIRST_RELEASED_NODE_TYPE_CODE = 'DS' then o.order_num else null end) as ds_fulfilled_unfiltered_orders,
		sum(case when FIRST_RELEASED_NODE_TYPE_CODE = 'DS' then order_line_amount_usd else 0 end) as ds_fulfilled_unfiltered_demand,

        sum(case when FIRST_RELEASED_NODE_TYPE_CODE = 'DS' and coalesce(o.gift_with_purchase_ind, 'N') = 'N' and coalesce(o.beauty_sample_ind, 'N') = 'N' and coalesce(o.source_platform_code, '') <> 'POS' AND COALESCE(fraud_cancel_ind,'N')='N'  then  ORDER_LINE_QUANTITY else 0 end ) as ds_fulfilled_filtered_units,
		count(distinct case when FIRST_RELEASED_NODE_TYPE_CODE = 'DS' and coalesce(o.gift_with_purchase_ind, 'N') = 'N' and coalesce(o.beauty_sample_ind, 'N') = 'N' and coalesce(o.source_platform_code, '') <> 'POS' AND COALESCE(fraud_cancel_ind,'N')='N'  then  o.order_num else null end) as ds_fulfilled_filtered_orders,
		sum(case when FIRST_RELEASED_NODE_TYPE_CODE = 'DS' and coalesce(o.gift_with_purchase_ind, 'N') = 'N' and coalesce(o.beauty_sample_ind, 'N') = 'N' and coalesce(o.source_platform_code, '') <> 'POS' AND COALESCE(fraud_cancel_ind,'N')='N'  then  order_line_amount_usd else 0 end) as ds_fulfilled_filtered_demand
FROM prd_nap_usr_vws.order_line_detail_fact o
left join sku_style_mapping s 
on o.rms_sku_num = s.rms_sku_num 
	and o.ORDER_DATE_PACIFIC = s.day_date
	and (case when o.source_channel_code = 'FULL_LINE' then 'NORDSTROM' when o.source_channel_code = 'RACK' then 'NORDSTROM_RACK' end) = s.selling_channel
WHERE order_date_pacific between {start_date} and {end_date}
	AND SOURCE_CHANNEL_COUNTRY_CODE = 'US'
GROUP by 1,2,3
) with data primary index (channel_brand, web_style_num, day_date) on commit preserve rows;



  ------------------------------------------------------------------------------
--- Product views
------------------------------------------------------------------------------
create multiset volatile table daily_product_views as (
select
	event_date_pacific as day_date,
	case when channel = 'FULL_LINE' then 'NORDSTROM' else 'NORDSTROM_RACK' end as channel_brand,
	ppfd.web_style_num as web_style_num,
	sum(product_views) as product_views_ppfd
from t2dl_das_product_funnel.product_price_funnel_daily ppfd
where 1 = 1
	and event_date_pacific between {start_date} and {end_date}
	and channelcountry = 'US'
	and channel in ('FULL_LINE','RACK')
	and ppfd.web_style_num is not null 
group by 1,2,3
) with data primary index (day_date, channel_brand, web_style_num) on commit preserve rows;



------------------------------------------------------------------------------
--- Daily Pricing
------------------------------------------------------------------------------
create multiset volatile table daily_price as (
select cl.web_style_num,
       day_date,
       channel_brand,
       min(selling_retail_price_amt) as min_selling_price,
       max(selling_retail_price_amt) as max_selling_price,
       avg(selling_retail_price_amt) as avg_selling_price,
       count(distinct selling_retail_price_amt) as number_selling_price,
       min(REGULAR_PRICE_AMT) as min_reg_price,
       max(REGULAR_PRICE_AMT) as max_reg_price,
       avg(REGULAR_PRICE_AMT) as avg_reg_price,
       count(distinct REGULAR_PRICE_AMT) as number_reg_price,
       min(last_md_discount) as min_md_discount,
       max(last_md_discount) as max_md_discount,
       avg(last_md_discount) as avg_md_discount,
       count(distinct last_md_discount) as number_md_discount,
       max(LAST_MD_VERSION) as num_md
from 
(select distinct
        cc,
		dc.day_date,
		channel_brand,
		selling_retail_price_amt,
		REGULAR_PRICE_AMT,
		last_md_discount,
		LAST_MD_VERSION
from t2dl_das_markdown.cc_price_wkly price
left join PRD_NAP_USR_VWS.DAY_CAL dc on price.run_wk_idnt  = dc.week_num
where channel_country = 'US'
	and day_date between {start_date} and {end_date}
) sub
left join cc_lkp cl 
on sub.cc = cl.cc
group by 1,2,3
) with data primary index (channel_brand, web_style_num, day_date) on commit preserve rows;

   


------------------------------------------------------------------------------
--- Inventory Age
------------------------------------------------------------------------------
create multiset volatile table daily_inventory_age as (
select *
from (
select channel_brand,
		web_style_num,
		dc.day_date,
		min(inventory_age) as min_inventory_age,
		max(inventory_age) as max_inventory_age,
		avg(inventory_age) * 1.00 as avg_inventory_age
from {digital_merch_t2_schema}.digital_inventory_age a
left join PRD_NAP_USR_VWS.DAY_CAL dc on a.week_num = dc.week_num
left join sku_style_mapping s 
on a.sku_idnt = s.rms_sku_num 
	and dc.day_date = s.day_date
	and a.channel_brand = s.selling_channel	
where web_style_num is not null
group by 1,2,3
) A
where day_date between {start_date} and {end_date}
) with data primary index (channel_brand,web_style_num,day_date) on commit preserve rows;



------------------------------------------------------------------------------
--- LOS
------------------------------------------------------------------------------
create multiset volatile table los_tbl as (
       select los.channel_brand,
              los.day_date,
              s.web_style_num
       from T2DL_DAS_SITE_MERCH.LIVE_ON_SITE_DAILY los
       left join sku_style_mapping s
            on los.sku_id = s.rms_sku_num
                and los.day_date = s.day_date
                and los.channel_brand = s.selling_channel
       where channel_country = 'US'
            and los.day_date between {start_date} and {end_date}
       group by 1,2,3
) with data primary index (channel_brand, day_date, web_style_num) on commit preserve rows
;



---------------------------------------------------------------
--------------------- TWIST + Inventory -----------------------
---------------------------------------------------------------
create multiset volatile table twist as (
select
	day_date,
	case when banner = 'NORD' then 'NORDSTROM' else 'NORDSTROM_RACK' end as channel,
	country,
	rms_sku_num,
	sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_views,
	sum(allocated_traffic) as total_views,
	case when total_views = 0 then null else instock_views/total_views end as pct_instock
from t2dl_das_twist.twist_daily
where business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE')
	and country = 'US'
	and product_views > 0
	and day_date between {start_date} and {end_date}
group by 1,2,3,4
) with data primary index (rms_sku_num, day_date, channel) on commit preserve rows;

collect statistics primary index (rms_sku_num, day_date, channel)
  , column(rms_sku_num)
  , column(day_date)
  , column(channel)
on twist;


create multiset volatile table product_price_daily as (
select distinct 
    event_date_pacific as day_date,
    case when channel = 'FULL_LINE' then 'NORDSTROM' else 'NORDSTROM_RACK' end as channel,
    rms_sku_num,
    rp_ind,
    current_price_type
from t2dl_das_product_funnel.product_price_funnel_daily ppfd
where channelcountry = 'US'
and event_date_pacific between {start_date} and {end_date}
) with data primary index (rms_sku_num, day_date, channel) on commit preserve rows;

collect statistics primary index (rms_sku_num, day_date, channel)
  , column(day_date)
  , column(channel)
  , column(rms_sku_num)
on product_price_daily;





create multiset volatile table selection_lkp as (
select 
	day_date,
	sku_idnt as rms_sku_num,
	channel_brand as channel,
	new_flag,
	sum(boh_units) as boh_units,
	sum(eoh_units) as eoh_units,
	sum(nonsellable_units) as nonsellable_units,
	sum(eoh_reg_units) as eoh_reg_units,
	sum(eoh_clr_units) as eoh_clr_units,
	sum(eoh_pro_units) as eoh_pro_units
from t2dl_das_selection.selection_productivity_base
where channel_country = 'US'
    and channel = 'DIGITAL'
    and day_date between {start_date} and {end_date}
group by 1,2,3,4
) WITH DATA PRIMARY INDEX(rms_sku_num, day_date, channel) ON COMMIT PRESERVE ROWS;

collect statistics primary index (rms_sku_num, day_date, channel)
  , column(rms_sku_num)
  , column(day_date)
  , column(channel)
on selection_lkp;



create multiset volatile table sku_channel_lkp as (
       select day_date, rms_sku_num, channel from selection_lkp
       union
       select day_date, rms_sku_num, channel from product_price_daily
       union
       select day_date, rms_sku_num, channel from twist
) with data primary index (day_date, rms_sku_num, channel) on commit preserve rows;
collect statistics primary index (day_date, rms_sku_num, channel)
          , column(day_date)
          , column(rms_sku_num)
          , column(channel)
on sku_channel_lkp;


create multiset volatile table daily_twist_sku as (
select base.day_date,
		base.rms_sku_num,
		base.channel,
		mapp.web_style_num,
    	mapp.color_num,
                t.instock_views,
                t.total_views,
                p.rp_ind,
                p.current_price_type,
                new_flag,
                s.boh_units,
                eoh_units,
                nonsellable_units,
                eoh_reg_units,
                eoh_clr_units,
                eoh_pro_units,
                mapp.selling_status_code
       from sku_channel_lkp base
       left join twist t
            on base.rms_sku_num = t.rms_sku_num
                and base.day_date = t.day_date
                and base.channel = t.channel
       left join product_price_daily p
            on base.rms_sku_num = p.rms_sku_num
                and base.day_date = p.day_date
                and base.channel = p.channel
       left join selection_lkp s
            on base.rms_sku_num = s.rms_sku_num
                and base.day_date = s.day_date
                and base.channel = s.channel
       left join sku_style_mapping mapp
            on base.rms_sku_num = mapp.rms_sku_num
                and base.day_date = mapp.day_date
                and base.channel = mapp.selling_channel
where web_style_num is not null
)WITH DATA PRIMARY INDEX(rms_sku_num, day_date, channel, web_style_num) ON COMMIT PRESERVE ROWS;
collect statistics primary index (day_date, rms_sku_num, channel, web_style_num)
          , column(day_date)
          , column(rms_sku_num)
          , column(channel)
          , column(web_style_num)
on daily_twist_sku;




create multiset volatile table daily_twist_inventory as (
select day_date
       , web_style_num
        , channel

        , sum(boh_units) as boh_units
        , sum(eoh_units) as eoh_units
        , sum(nonsellable_units) as nonsellable_units
        , sum(instock_views) as instock_views
        , sum(total_views) as total_views
        , sum(instock_views)*1.0000 / sum(total_views+0.001) as twist

       , count(distinct case when new_flag = 1 then  rms_sku_num else null end ) as sku_count_new
       , count(distinct case when new_flag = 1 then  color_num else null end) as color_count_new
       , count(distinct case when new_flag = 0 then  rms_sku_num else null end ) as sku_count_not_new
       , count(distinct case when new_flag = 0 then  color_num else null end) as color_count_not_new
       , sum(case when new_flag = 1 then boh_units end) as new_boh_units
       , sum(case when new_flag = 1 then eoh_units end) as new_eoh_units
       , sum(case when new_flag = 1 then nonsellable_units end) as new_nonsellable_units
       , sum(case when new_flag = 0 then boh_units end) as not_new_boh_units
       , sum(case when new_flag = 0 then eoh_units end) as not_new_eoh_units
       , sum(case when new_flag = 0 then nonsellable_units end) as not_new_nonsellable_units
       ,  sum(case when new_flag = 1 then instock_views end)*1.0000 / sum(case when new_flag = 1 then total_views+0.001 end) as new_twist
       ,  sum(case when new_flag = 0 then instock_views end)*1.0000 / sum(case when new_flag = 0 then total_views+0.001 end) as not_new_twist

       , count(distinct case when rp_ind= 1 then  rms_sku_num else null end ) as sku_count_rp
       , count(distinct case when rp_ind= 1 then  color_num else null end) as color_count_rp
       , count(distinct case when rp_ind= 0 then  rms_sku_num else null end ) as sku_count_non_rp
       , count(distinct case when rp_ind= 0 then  color_num else null end) as color_count_non_rp
       , sum(case when rp_ind = 1 then boh_units end) as rp_boh_units
       , sum(case when rp_ind = 1 then eoh_units end) as rp_eoh_units
       , sum(case when rp_ind = 1 then nonsellable_units end) as rp_nonsellable_units
       , sum(case when rp_ind = 0 then boh_units end) as not_rp_boh_units
       , sum(case when rp_ind = 0 then eoh_units end) as not_rp_eoh_units
       , sum(case when rp_ind = 0 then nonsellable_units end) as not_rp_nonsellable_units
        ,  sum(case when rp_ind = 1 then instock_views end)*1.0000 / sum(case when rp_ind = 1 then total_views+0.001 end) as rp_twist
        ,  sum(case when rp_ind = 0 then instock_views end)*1.0000 / sum(case when rp_ind = 0 then total_views+0.001 end) as not_rp_twist

       , count(distinct case when current_price_type = 'R' then  rms_sku_num else null end ) as sku_count_R
       , count(distinct case when current_price_type = 'P' then  rms_sku_num else null end ) as sku_count_P
       , count(distinct case when current_price_type = 'C' then  rms_sku_num else null end ) as sku_count_C
       , count(distinct case when current_price_type= 'R' then  color_num else null end) as color_count_R
       , count(distinct case when current_price_type= 'P' then  color_num else null end) as color_count_P
       , count(distinct case when current_price_type= 'C' then  color_num else null end) as color_count_C
        , sum(eoh_reg_units) as eoh_reg_units
        , sum(eoh_clr_units) as eoh_clr_units
        , sum(eoh_pro_units) as eoh_pro_units
        ,  sum(case when current_price_type= 'R' then instock_views end)*1.0000 / sum(case when current_price_type= 'R' then total_views+0.001 end) as R_twist
        ,  sum(case when current_price_type= 'P' then instock_views end)*1.0000 / sum(case when current_price_type= 'P' then total_views+0.001 end) as P_twist
        ,sum(case when current_price_type= 'C' then instock_views end)*1.0000 / sum(case when current_price_type= 'C' then total_views+0.001 end) as C_twist

       , sum(case when selling_status_code  = 'T1' then boh_units end) as DS_boh_units
       , sum(case when selling_status_code  = 'S1' then boh_units end) as Unsellable_boh_units
       , sum(case when selling_status_code  = 'C3' then boh_units end) as Owned_boh_units
       , sum(case when selling_status_code  = 'T1' then eoh_units end) as DS_eoh_units
       , sum(case when selling_status_code  = 'S1' then eoh_units end) as Unsellable_eoh_units
       , sum(case when selling_status_code  = 'C3' then eoh_units end) as Owned_eoh_units
        ,  sum(case when  selling_status_code  = 'T1' then instock_views end)*1.0000 / sum(case when selling_status_code  = 'T1' then total_views+0.001 end) as DS_twist
        ,  sum(case when selling_status_code  = 'S1' then instock_views end)*1.0000 / sum(case when selling_status_code  = 'S1' then total_views+0.001 end) as Unsellable_twist
        ,  sum(case when selling_status_code  = 'C3' then instock_views end)*1.0000 / sum(case when selling_status_code  = 'C3' then total_views+0.001 end) as Owned_twist
from daily_twist_sku
group by 1,2,3
) with data primary index (day_date, web_style_num, channel) ON COMMIT PRESERVE ROWS;
collect statistics primary index (day_date, web_style_num, channel)
  , column(day_date)
  , column(web_style_num)
  , column(channel)
on daily_twist_inventory;



CREATE VOLATILE MULTISET TABLE style_flash_sku AS (
with flash_sku_date_exp as (
select distinct rms_sku_num, channel_brand, CAST(BEGIN(eff_period) AS date) AS day_date
from
(select distinct 
	   s.sku_num as rms_sku_num, 
	   s.channel_brand, 
	   (case when s.item_start_tmstp > s.item_end_tmstp then s.item_end_tmstp else s.item_start_tmstp end ) at time zone 'America Pacific'  as item_start_tmstp, 
	   (case when s.item_start_tmstp > s.item_end_tmstp then s.item_start_tmstp else s.item_end_tmstp end ) at time zone 'America Pacific'  as item_end_tmstp 
from prd_nap_usr_vws.product_selling_event_valid_sku_vw s
join prd_nap_usr_vws.product_selling_event_tags_dim e
on s.selling_event_num = e.selling_event_num
	and s.selling_event_association_id = e.selling_event_association_id
where e.tag_name = 'FLASH'
	and s.selling_event_status = 'APPROVED'
	and s.channel_country = 'US'
	and s.channel_brand = 'NORDSTROM_RACK'
) flash_sku 
	EXPAND ON
	PERIOD(item_start_tmstp, case when year(item_end_tmstp) = 9999 then item_end_tmstp else item_end_tmstp + interval '1' day end) AS eff_period
	BY interval '1' day 
	for PERIOD(CAST({start_date} AS TIMESTAMP(0)), CAST({end_date} AS TIMESTAMP(0)) + interval '1' day)
)
select mapp.web_style_num, 
		fs.day_date, 
		fs.channel_brand,
		count(distinct fs.rms_sku_num) as flash_sku_count
from flash_sku_date_exp fs
left join 
	(select distinct rms_sku_num, day_date, web_style_num 
	from sku_style_mapping 
	where selling_channel = 'NORDSTROM_RACK'
	) mapp
on fs.rms_sku_num = mapp.rms_sku_num
   and fs.day_date = mapp.day_date
group by 1,2,3
) with data primary index (day_date, web_style_num, channel_brand) ON COMMIT PRESERVE ROWS;
collect statistics primary index (day_date, web_style_num, channel_brand)
  , column(day_date)
  , column(web_style_num)
  , column(channel_brand)
on style_flash_sku;


------------------------------------------------------------------------------
--- Consolidate the Tables
------------------------------------------------------------------------------
DELETE 
FROM    {digital_merch_t2_schema}.digital_merch_table
; 


--CREATE MULTISET TABLE {digital_merch_t2_schema}.digital_merch_table AS (
INSERT INTO {digital_merch_t2_schema}.digital_merch_table
SELECT 
base.day_date,
trim(base.web_style_num) as web_style_num,
base.selling_channel,
trim(base.epm_style_num) as epm_style_num,
base.partner_relationship_type,
base.npg_ind,
base.brand_name,
base.div_num,
base.div_desc,
base.grp_num,
base.grp_desc,
base.dept_num,
base.dept_desc,
base.class_num,
base.class_desc,
base.sbclass_num,
base.sbclass_desc,
base.prmy_supp_num,
oreplace(base.vender_name, ',', '/') as vender_name,
base.style_group_num,
REGEXP_REPLACE(REGEXP_REPLACE(oreplace(REGEXP_REPLACE(oreplace(style_group_desc, ',', '/'), '\r?\n', ' '), '"', 'inch '), '[^a-zA-Z0-9]+$',''), '^[^a-zA-Z0-9]+','') as style_group_desc,
REGEXP_REPLACE(REGEXP_REPLACE(oreplace(REGEXP_REPLACE(oreplace(style_group_short_desc, ',', '/'), '\r?\n', ' '), '"', 'inch '), '[^a-zA-Z0-9]+$',''), '^[^a-zA-Z0-9]+','') as style_group_short_desc,
base.type_level_1_num,
oreplace(base.type_level_1_desc, ',', '/') as type_level_1_desc,
base.type_level_2_num,
oreplace(base.type_level_2_desc, ',', '/') as type_level_2_desc,
base.genders_code,
oreplace(base.genders_desc, ',', '/') as genders_desc,
base.age_groups_code,
oreplace(base.age_groups_desc, ',', '/') as age_groups_desc,
oreplace(base.QUANTRIX_CATEGORY, ',', '/') as QUANTRIX_CATEGORY,
oreplace(base.CCS_CATEGORY, ',', '/') as CCS_CATEGORY,
oreplace(base.CCS_SUBCATEGORY, ',', '/') as CCS_SUBCATEGORY,
base.NORD_ROLE,
base.NORD_ROLE_DESC,
base.RACK_ROLE,
base.RACK_ROLE_DESC,
base.PARENT_GROUP,
base.MERCH_THEMES,
base.ANNIVERSARY_21,
base.ANNIVERSARY_THEME,
base.HOLIDAY_21,
base.HOLIDAY_THEME_TY,
base.GIFT_IND,
base.STOCKING_STUFFER,
base.ASSORTMENT_GROUPING,

dpv.product_views_ppfd,
case when los.web_style_num is not null then 1 else 0 end as los_flag,

dsu.unfiltered_units,
dsu.unfiltered_orders,
dsu.unfiltered_demand,
dsu.filtered_units,
dsu.filtered_orders,
dsu.filtered_demand,
dsu.ds_fulfilled_unfiltered_units,
dsu.ds_fulfilled_unfiltered_orders,
dsu.ds_fulfilled_unfiltered_demand,
dsu.ds_fulfilled_filtered_units,
dsu.ds_fulfilled_filtered_orders,
dsu.ds_fulfilled_filtered_demand,

dp.min_selling_price,
dp.max_selling_price,
dp.avg_selling_price,
dp.number_selling_price,
dp.min_reg_price,
dp.max_reg_price,
dp.avg_reg_price,
dp.number_reg_price,
dp.min_md_discount,
dp.max_md_discount,
dp.avg_md_discount,
dp.number_md_discount,
dp.num_md,

dia.min_inventory_age,
dia.max_inventory_age,
dia.avg_inventory_age,

base.sku_count,
base.color_count,
dt.boh_units,
dt.eoh_units,
dt.nonsellable_units,
dt.instock_views,
dt.total_views,
dt.twist,

dt.sku_count_new,
dt.color_count_new,
dt.sku_count_not_new,
dt.color_count_not_new,
dt.new_boh_units,
dt.new_eoh_units,
dt.new_nonsellable_units,
dt.not_new_boh_units,
dt.not_new_eoh_units,
dt.not_new_nonsellable_units,
dt.new_twist,
dt.not_new_twist,

dt.sku_count_rp,
dt.color_count_rp,
dt.sku_count_non_rp,
dt.color_count_non_rp,
dt.rp_boh_units,
dt.rp_eoh_units,
dt.rp_nonsellable_units,
dt.not_rp_boh_units,
dt.not_rp_eoh_units,
dt.not_rp_nonsellable_units,
dt.rp_twist,
dt.not_rp_twist,

dt.sku_count_R,
dt.sku_count_P,
dt.sku_count_C,
dt.color_count_R,
dt.color_count_P,
dt.color_count_C,
dt.eoh_reg_units,
dt.eoh_clr_units,
dt.eoh_pro_units,
dt.R_twist,
dt.P_twist,
dt.C_twist,

base.sku_count_DS,
base.sku_count_Unsellable,
base.sku_count_Owned,
base.color_count_DS,
base.color_count_Unsellable,
base.color_count_Owned,
dt.DS_boh_units,
dt.Unsellable_boh_units,
dt.Owned_boh_units,
dt.DS_eoh_units,
dt.Unsellable_eoh_units,
dt.Owned_eoh_units,
dt.DS_twist,
dt.Unsellable_twist,
dt.Owned_twist,
sfs.flash_sku_count,

case when base.sku_count_mp > 0 then 1 else 0 end as mp_flag,
base.sku_count_mp,
base.sku_count_nmp,
base.color_count_mp,
base.color_count_nmp,

CURRENT_TIMESTAMP as dw_sys_load_tmstp
from style_merch_hierarchy base
left join daily_product_views dpv on base.web_style_num = dpv.web_style_num and base.day_date = dpv.day_date and base.selling_channel = dpv.channel_brand
left join daily_price dp on base.web_style_num = dp.web_style_num and base.day_date = dp.day_date and base.selling_channel = dp.channel_brand
left join daily_inventory_age dia on base.web_style_num = dia.web_style_num and base.day_date = dia.day_date and base.selling_channel = dia.channel_brand
left join daily_sold_units dsu on base.web_style_num = dsu.web_style_num and base.day_date = dsu.day_date and base.selling_channel = dsu.channel_brand
left join daily_twist_inventory dt on base.web_style_num = dt.web_style_num and base.day_date = dt.day_date and base.selling_channel = dt.channel
left join style_flash_sku sfs on base.web_style_num = sfs.web_style_num and base.day_date = sfs.day_date and base.selling_channel = sfs.channel_brand
left join los_tbl los on base.web_style_num = los.web_style_num and base.day_date = los.day_date and base.selling_channel = los.channel_brand
;
--) with data primary index (selling_channel, web_style_num, day_date)

COLLECT STATISTICS 
COLUMN (PARTITION),
COLUMN (selling_channel, web_style_num, day_date), -- column names used for primary index
COLUMN (day_date)  -- column names used for partition
on {digital_merch_t2_schema}.digital_merch_table;





/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
