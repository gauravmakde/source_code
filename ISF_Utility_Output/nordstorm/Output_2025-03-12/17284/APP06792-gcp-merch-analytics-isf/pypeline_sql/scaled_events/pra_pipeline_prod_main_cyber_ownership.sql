/*
Name: Cyber Ownership Report
APPID-NAME: APP08204 - Scaled Events Reporting
Purpose: populate the backend table that powers the Scaled Events Cyber Ownership Report
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
Dag: merch_pra_se_cyber_ownership
Author(s): Manuela Hurtado Gonzalez
Date created: 09-07-2023
Date last updated: 09-30-2023
*/


/******************************************************************************
** start: volatile tables
******************************************************************************/

-- UDIG table
create multiset volatile table udig_main as (
select
    case
        when banner = 'OP' and udig_rank = 3 then '1, HOL23_NR_CYBER_SPECIAL PURCHASE'
        when banner = 'FP' and udig_rank = 1 then '1, HOL23_N_CYBER_SPECIAL PURCHASE'
        when banner = 'FP' and udig_rank = 2 then '2, CYBER 2023_US'
    end as udig
    , rnk.sku_idnt
    , banner
from
    (select
        sku_idnt
        , banner
        , min(case
            when item_group = '1, HOL23_N_CYBER_SPECIAL PURCHASE' then 1
            when item_group = '2, CYBER 2023_US' then 2
            when item_group = '1, HOL23_NR_CYBER_SPECIAL PURCHASE' then 3
            end) as udig_rank
        from
            (
            select distinct
                sku_idnt,
                case
                  when udig_colctn_idnt = 50009 then 'OP'
                  else 'FP'
                end as banner,
                udig_itm_grp_idnt || ', ' || udig_itm_grp_nm as item_group
            from prd_usr_vws.udig_itm_grp_sku_lkup
            where udig_colctn_idnt in ('50008','50009','25032')
            and udig_itm_grp_nm in ('HOL23_NR_CYBER_SPECIAL PURCHASE', 'HOL23_N_CYBER_SPECIAL PURCHASE', 'CYBER 2023_US')
            ) item_group
        group by 1, 2
    ) rnk
	)
	with data
	primary index ( sku_idnt, banner )
	on commit preserve rows;
	collect statistics
	primary index ( sku_idnt, banner )
    ,column ( sku_idnt )
    ,column ( banner )
	   on udig_main;

-- Nordstrom Cyber skus
create volatile multiset table n_sku as (
select
  distinct
  pptd.rms_sku_num as sku_idnt
  , 'FP' as banner
  from prd_nap_usr_vws.product_promotion_timeline_dim pptd
  inner join prd_nap_usr_vws.day_cal dt
    on dt.day_date between cast(enticement_start_tmstp as date) and cast(enticement_end_tmstp as date)
    where dt.day_date between date {start_date} and date {end_date}
  and pptd.channel_country = 'US'
  and pptd.channel_brand = 'NORDSTROM'
    and enticement_tags = 'NATURAL_BEAUTY'
)
with data
primary index ( sku_idnt )
  on commit preserve rows;
collect statistics
  primary index ( sku_idnt )
  ,column ( sku_idnt )
    on n_sku;

-- all cyber skus
create volatile multiset table cyber_sku as (
  select distinct sku_idnt, banner from udig_main where banner = 'OP'
		union all
 	select * from n_sku
)
with data
primary index ( sku_idnt, banner )
  on commit preserve rows;
collect statistics
	primary index ( sku_idnt, banner )
	,column ( sku_idnt )
	,column ( banner )
  	on cyber_sku;

-- inventory staging table
create multiset volatile table inv_stg
as (
	select
	sub.rms_sku_id as sku_idnt
	, sub.loc_idnt
	, sub.banner
	, sum(sub.soh_qty) as soh_qty
	, sum(sub.in_transit_qty) as in_transit_qty
	from(
		select distinct
			rms_sku_id
			, location_id as loc_idnt
			, 'FP' as banner
			, stock_on_hand_qty as soh_qty
			, in_transit_qty as in_transit_qty
		from prd_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact inv
		inner join prd_nap_usr_vws.store_dim loc
    	on inv.location_id = loc.store_num
		    and channel_num in (110, 120, 310)
    inner join cyber_sku sku
      on inv.rms_sku_id = sku.sku_idnt
      and sku.banner = 'FP'
    where snapshot_date = current_date - 1

		union all

		select distinct
			rms_sku_id
			, location_id as loc_idnt
			, 'OP' as banner
			, stock_on_hand_qty as soh_qty
			, in_transit_qty as in_transit_qty
		from prd_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact inv
		inner join prd_nap_usr_vws.store_dim loc
    	on inv.location_id = loc.store_num
		    and channel_num in (210, 250, 260, 220)
    inner join cyber_sku sku
      on inv.rms_sku_id = sku.sku_idnt
      and sku.banner = 'OP'
    where snapshot_date = current_date - 1

	) sub
	group by 1,2,3
)
with data
primary index ( sku_idnt, loc_idnt)
  on commit preserve rows;
collect stats
    primary index (sku_idnt, loc_idnt)
    ,column (sku_idnt, loc_idnt)
        on inv_stg;

-- Pull future OO and union with inventory
create multiset volatile table ownership_stg
as (
    select
        sub.sku_idnt
        , sub.loc_idnt
				, sub.banner
        , sum(coalesce(sub.on_order_dollars, 0)) as on_order_dollars
        , sum(coalesce(sub.on_order_cost, 0)) as on_order_cost
        , sum(coalesce(sub.on_order_units, 0)) as on_order_units
        , sum(coalesce(sub.oo_ordered_dollars, 0)) as total_ordered_oo_dollars
        , sum(coalesce(sub.oo_ordered_cost, 0)) as total_ordered_oo_cost
        , sum(coalesce(sub.oo_ordered, 0)) as total_ordered_oo_units
        , sum(coalesce(sub.oo_received_dollars, 0)) as total_received_oo_dollars
        , sum(coalesce(sub.oo_received_cost, 0)) as total_received_oo_cost
        , sum(coalesce(sub.oo_received, 0)) as total_received_oo_units
        , sum(coalesce(sub.oo_canceled, 0)) as total_canceled_oo_units
        , sum(coalesce(sub.in_transit_dollars, 0)) as in_transit_dollars
        , sum(coalesce(sub.in_transit_cost, 0)) as in_transit_cost
        , sum(coalesce(sub.in_transit_units, 0)) as in_transit_units
        , sum(coalesce(sub.eoh_dollars, 0)) as eoh_dollars
        , sum(coalesce(sub.eoh_cost, 0)) as eoh_cost
        , sum(coalesce(sub.eoh_units, 0)) as eoh_units
	  from (select oo.rms_sku_num as sku_idnt
          , cast(oo.store_num as varchar(20)) as loc_idnt
					, 'FP' as banner
          , sum(oo.anticipated_retail_amt * oo.quantity_open) as on_order_dollars
          , sum(oo.unit_estimated_landing_cost  * oo.quantity_open) as on_order_cost
          , sum(oo.quantity_open) as on_order_units
          , sum(oo.anticipated_retail_amt * oo.quantity_ordered) as oo_ordered_dollars
          , sum(oo.unit_estimated_landing_cost  * oo.quantity_ordered) as oo_ordered_cost
          , sum(oo.quantity_ordered) as oo_ordered
          , sum(oo.anticipated_retail_amt * oo.quantity_received) as oo_received_dollars
          , sum(oo.unit_estimated_landing_cost  * oo.quantity_received) as oo_received_cost
          , sum(oo.quantity_received) as oo_received
          , sum(oo.quantity_canceled) as oo_canceled
    			, cast(0 as decimal(38,2)) as eoh_dollars
    			, cast(0 as decimal(38,2)) as eoh_cost
    			, cast(0 as integer) as eoh_units
    			, cast(0 as decimal(38,2)) as in_transit_dollars
    			, cast(0 as decimal(38,2)) as in_transit_cost
    			, cast(0 as integer) as in_transit_units
      from
          prd_nap_usr_vws.merch_on_order_fact_vw oo
      inner join prd_nap_usr_vws.store_dim loc
          on oo.store_num = loc.store_num
          and channel_num in (110, 120, 310)
			inner join 	cyber_sku sku
					on sku.sku_idnt = oo.rms_sku_num
					and sku.banner = 'FP'
      where week_num <= {end_week} -- 202343 - november 26th - last day of ownership
          and week_num >= {start_week} -- 202336 - october 3rd 2023 - target prod date
					and oo.quantity_open > 0
					and (oo.status = 'APPROVED' or (oo.status = 'CLOSED' and oo.end_ship_date >= current_date - 45))
      group by 1, 2, 3

			union all

			select oo.rms_sku_num as sku_idnt
				, cast(oo.store_num as varchar(20)) as loc_idnt
				, 'OP' as banner
				, sum(oo.anticipated_retail_amt * oo.quantity_open) as on_order_dollars
				, sum(oo.unit_estimated_landing_cost  * oo.quantity_open) as on_order_cost
				, sum(oo.quantity_open) as on_order_units
				, sum(oo.anticipated_retail_amt * oo.quantity_ordered) as oo_ordered_dollars
				, sum(oo.unit_estimated_landing_cost  * oo.quantity_ordered) as oo_ordered_cost
				, sum(oo.quantity_ordered) as oo_ordered
				, sum(oo.anticipated_retail_amt * oo.quantity_received) as oo_received_dollars
				, sum(oo.unit_estimated_landing_cost  * oo.quantity_received) as oo_received_cost
				, sum(oo.quantity_received) as oo_received
				, sum(oo.quantity_canceled) as oo_canceled
	    	, cast(0 as decimal(38,2)) as eoh_dollars
	    	, cast(0 as decimal(38,2)) as eoh_cost
	    	, cast(0 as integer) as eoh_units
	    	, cast(0 as decimal(38,2)) as in_transit_dollars
	    	, cast(0 as decimal(38,2)) as in_transit_cost
	    	, cast(0 as integer) as in_transit_units
			from prd_nap_usr_vws.merch_on_order_fact_vw oo
			inner join prd_nap_usr_vws.store_dim loc
					on oo.store_num = loc.store_num
          and channel_num in (210, 250, 260, 220)
			inner join 	cyber_sku sku
					on sku.sku_idnt = oo.rms_sku_num
					and sku.banner = 'OP'
			where week_num <= {end_week}
					and week_num >= {start_week}
					and oo.quantity_open > 0
					and (oo.status = 'APPROVED' or (oo.status = 'CLOSED' and oo.end_ship_date >= current_date - 45))
			group by 1, 2, 3

	    union all

      select
          inv.sku_idnt
        , inv.loc_idnt
				, inv.banner
        , cast(0 as decimal(38,9)) as on_order_dollars
        , cast(0 as decimal(38,9)) as on_order_cost
        , cast(0 as integer) as on_order_units
        , cast(0 as decimal(38,9)) as oo_ordered_dollars
        , cast(0 as decimal(38,9)) as oo_ordered_cost
        , cast(0 as integer) as oo_ordered
        , cast(0 as integer) as oo_received
        , cast(0 as decimal(38,9)) as oo_received_dollars
        , cast(0 as decimal(38,9)) as oo_received_cost
        , cast(0 as integer) as oo_canceled
        , sum(inv.soh_qty * ownership_retail_price_amt) as eoh_dollars
        , sum(inv.soh_qty * mf.weighted_average_cost) as eoh_cost
        , sum(inv.soh_qty) as eoh_units
        , sum(inv.in_transit_qty * ownership_retail_price_amt) as in_transit_dollars
        , sum(inv.in_transit_qty * mf.weighted_average_cost) as in_transit_cost
        , sum(inv.in_transit_qty) as in_transit_units
    from inv_stg inv
    inner join prd_nap_usr_vws.price_store_dim_vw loc
      on inv.loc_idnt = loc.store_num
    inner join prd_nap_usr_vws.product_price_timeline_dim pr
      on pr.store_num = loc.price_store_num
      and pr.rms_sku_num = inv.sku_idnt
      and period(pr.eff_begin_tmstp, pr.eff_end_tmstp) contains current_timestamp at time zone 'GMT'
		left join t2dl_das_ace_mfp.sku_loc_pricetype_day mf
    	on mf.sku_idnt = inv.sku_idnt
    	and mf.loc_idnt = inv.loc_idnt
    	and mf.day_dt = current_date - 1
    	and mf.price_type = left(pr.ownership_retail_price_type_code, 1)
      group by 1, 2, 3
    ) sub
    group by 1, 2, 3
)
with data
primary index ( sku_idnt, loc_idnt)
  on commit preserve rows;

collect stats
    primary index (sku_idnt, loc_idnt)
    ,column (sku_idnt, loc_idnt)
        on ownership_stg;

/******************************************************************************
** end: volatile tables
******************************************************************************/

/******************************************************************************
** start: final insert
******************************************************************************/
/*

call sys_mgmt.drop_if_exists_sp ('{environment_schema}', 'CYBER_OWNERSHIP', out_return_msg);
create multiset table {environment_schema}.CYBER_OWNERSHIP
    , fallback
    , no before journal
    , no after journal
    , checksum = default
    , default mergeblockratio
    , map = td_map1
(
		channel				varchar(73) character set unicode not casespecific
    , channel_idnt			integer not null compress(110, 120, 210, 250, 310, 260, 220)
    , region				varchar(73) character set unicode not casespecific
    , location				varchar(46) character set unicode not casespecific not null
    , division				varchar(73) character set unicode not casespecific
    , subdivision			varchar(73) character set unicode not casespecific
    , department			varchar(73) character set unicode not casespecific
    , "class"				varchar(73) character set unicode not casespecific
    , subclass				varchar(73) character set unicode not casespecific
    , style_group_num		varchar(10) character set unicode not casespecific
    , style_group_desc		varchar(100) character set unicode not casespecific
    , style_num 			bigint
    , style_desc			varchar(300) character set unicode not casespecific
    , vpn					varchar(30) character set unicode not casespecific not null
    , supp_color			varchar(80) character set unicode not casespecific not null
    , nrf_colr				varchar(60) character set unicode not casespecific
    , brand_tier 			varchar(100) character set unicode not casespecific
    , brand 				varchar(60) character set unicode not casespecific
		, banner 				varchar(2) character set unicode not casespecific not null compress ('FP','OP')
    , supplier_num 			varchar(10) character set unicode not casespecific
    , supplier 				varchar(60) character set unicode not casespecific
    , udig					varchar(100) character set unicode not casespecific
    , npg_ind				varchar(1) character set unicode not casespecific not null compress ('Y', 'N')
    , rp_ind				varchar(1) character set unicode not casespecific not null compress ('Y', 'N')
    , on_order_dollars		decimal(20,2) not null default 0.00 compress 0.00
    , on_order_cost 		decimal(20,2) not null default 0.00 compress 0.00
    , on_order_units		integer not null
    , ttl_ordered_dollars   decimal(20,2) not null default 0.00 compress 0.00
    , ttl_ordered_cost      decimal(20,2) not null default 0.00 compress 0.00
    , ttl_ordered_units 	integer not null
    , ttl_received_dollars  decimal(20,2) not null default 0.00 compress 0.00
    , ttl_received_cost     decimal(20,2) not null default 0.00 compress 0.00
    , ttl_received_units	integer not null
    , ttl_canceled_units	integer not null
    , in_transit_dollars	decimal(20,2) not null default 0.00 compress 0.00
    , in_transit_cost   	decimal(20,2) not null default 0.00 compress 0.00
    , in_transit_units		integer not null
    , eoh_dollars			decimal(20,2) not null default 0.00 compress 0.00
    , eoh_cost  			decimal(20,2) not null default 0.00 compress 0.00
    , eoh_units				integer not null
    , table_update_date 		timestamp(6) with time zone
)
primary index (vpn, supp_color, location);
grant select on {environment_schema}.CYBER_OWNERSHIP to public;
*/

delete from {environment_schema}.CYBER_OWNERSHIP all;
insert into {environment_schema}.CYBER_OWNERSHIP

select trim(loc.chnl_label) as channel
    , loc.chnl_idnt as channel_idnt
    , trim(loc.regn_label) as region
    , trim(loc.loc_label) as location
    , trim(sku.div_num|| ', '||sku.div_desc) as division
    , trim(sku.grp_num|| ', '||sku.grp_desc) as subdivision
    , trim(sku.dept_num|| ', '||sku.dept_desc) as department
    , trim(sku.class_num|| ', '||sku.class_desc) as "CLASS"
    , trim(sku.sbclass_num|| ', '||sku.sbclass_desc) as subclass
    , sku.style_group_num as style_group_num
    , sku.style_group_desc
    , sku.web_style_num as style_num
    , sku.style_desc
    , sku.supp_part_num as vpn
    , coalesce(sku.supp_color, 'NONE') as supp_color
    , sku.color_desc as nrf_colr
    , spdpt.preferred_partner_desc as brand_tier
    , sku.brand_name as brand
		, apo.banner
    , sku.prmy_supp_num as supplier_num
    , ven.vendor_name as supplier
    , udig.udig
    , coalesce(sku.npg_ind, 'N') as npg_ind
    , max(case when rp.rp_ind = 1 then 'Y' else 'N' end) as rp_ind
    , sum(apo.on_order_dollars) as on_order_dollars
    , sum(apo.on_order_cost) as on_order_cost
    , sum(apo.on_order_units) as on_order_units
    , sum(apo.total_ordered_oo_dollars) as ttl_ordered_dollars
    , sum(apo.total_ordered_oo_cost) as ttl_ordered_cost
    , sum(apo.total_ordered_oo_units) as ttl_ordered_units
    , sum(apo.total_received_oo_dollars) as ttl_received_dollars
    , sum(apo.total_received_oo_cost) as ttl_received_cost
    , sum(apo.total_received_oo_units) as ttl_received_units
    , sum(apo.total_canceled_oo_units) as ttl_canceled_units
    , sum(apo.in_transit_dollars) as in_transit_dollars
    , sum(apo.in_transit_cost) as in_transit_cost
    , sum(apo.in_transit_units) as in_transit_units
    , sum(apo.eoh_dollars) as eoh_dollars
    , sum(apo.eoh_cost) as eoh_cost
    , sum(apo.eoh_units) as eoh_units
    , current_timestamp as table_update_date
from ownership_stg apo
inner join prd_nap_usr_vws.product_sku_dim_vw sku
    on apo.sku_idnt = sku.rms_sku_num
    	and sku.channel_country = 'US'
inner join (
    select
    	channel_num||', '||channel_desc as chnl_label
        , channel_num as chnl_idnt
        , region_num||', '||region_desc as regn_label
        , cast(store_num as varchar(4))||', '||store_short_name as loc_label
        , store_num as loc_idnt
        , channel_desc as chnl_desc
    from prd_nap_usr_vws.store_dim
    where store_country_code = 'US'
        and store_close_date is null
)loc
    on apo.loc_idnt = loc.loc_idnt
left join prd_nap_usr_vws.vendor_dim ven
	on ven.vendor_num = sku.prmy_supp_num
left join prd_nap_usr_vws.supp_dept_map_dim spdpt
	on spdpt.supplier_num = sku.prmy_supp_num
		and spdpt.dept_num = sku.dept_num
		and spdpt.banner = apo.banner
		and current_date between cast(spdpt.eff_begin_tmstp as date format 'YYYY-MM-DD') and cast(spdpt.eff_end_tmstp as date format 'YYYY-MM-DD')
left join (select distinct rms_sku_num, location_num, 1 as rp_ind
      from prd_nap_usr_vws.merch_rp_sku_loc_dim_hist rp
      where rp_period contains (select distinct day_date from prd_nap_usr_vws.day_cal_454_dim where day_date = (current_date - interval '1' day))
    ) rp
    on apo.sku_idnt = rp.rms_sku_num
    and apo.loc_idnt = rp.location_num
left join udig_main udig
    on apo.sku_idnt = udig.sku_idnt
		and apo.banner = udig.banner

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
;

collect statistics column(vpn, supp_color, location) on {environment_schema}.CYBER_OWNERSHIP;
--grant select on {environment_schema}.cyber_ownership to public;
/******************************************************************************
** end: final insert
******************************************************************************/
