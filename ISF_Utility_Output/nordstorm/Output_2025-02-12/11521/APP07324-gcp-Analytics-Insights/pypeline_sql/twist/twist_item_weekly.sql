SET QUERY_BAND = 'App_ID=APP08364;
     DAG_ID=twist_weekly_11521_ACE_ENG;
     Task_Name=twist_item_weekly;'
     FOR SESSION VOLATILE;

/*
T2/Table Name:T2DL_DAS_TWIST.twist_item_weekly
Team/Owner:Meghan Hickey (meghan.d.hickey@nordstrom.com)
Date Created/Modified: 04/03/2023 

Updates
  * modified with department weighting approach
  * update eoh with asoh
  * replace instock_inds

GOAL: Roll up in-stock rates for RP item level dashboard
*/


-- Rolling 52 weeks
CREATE MULTISET VOLATILE TABLE dt AS (
  select
      distinct day_date as day_dt
  from prd_nap_usr_vws.day_cal_454_dim a
  where week_end_day_date <= current_date
  QUALIFY ROW_NUMBER() OVER (ORDER BY week_idnt DESC) <= 52*7
) WITH DATA PRIMARY INDEX(day_dt) ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX (day_dt)
		ON dt;


-- Product Hierarchy Look-up
CREATE MULTISET VOLATILE TABLE product_sku AS (
  select
      rms_sku_num
      , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
      , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
      , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
      , npg_ind
      , dept_num
      , prmy_supp_num
      , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
      , upper(sku.brand_name) as brand_name
      , UPPER(supp.vendor_name) as supplier
      , trim(sku.class_num || ', ' || sku.class_desc) as class_desc
      , trim(sku.sbclass_num || ', ' || sku.sbclass_desc) as subclass_desc
      , sku.supp_part_num || ', ' || sku.style_desc as vpn
      , sku.supp_color as color
      , vpn || ', ' || color as cc

from prd_nap_usr_vws.product_sku_dim_vw sku
left join prd_nap_usr_vws.vendor_dim supp
    on sku.prmy_supp_num =supp.vendor_num
where channel_country = 'US'

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) WITH DATA PRIMARY INDEX(rms_sku_num) ON COMMIT PRESERVE ROWS;


-- Dept Weighting
create multiset volatile table shr as (
    select
        week_idnt as wk_idnt
        , dept_desc
        , sum(case when channel_num = 110 then hist_items else 0 end)*1.000/nullif(sum(case when banner = 'NORD' then hist_items else 0 end)*1.000,0) as share_fp
        , sum(case when channel_num = 210 then hist_items else 0 end)*1.000/nullif(sum(case when banner = 'RACK' then hist_items else 0 end)*1.000,0) as share_op
    from (
        select
            day_date,
            sku.dept_desc,
            store_num,
            banner,
            sum(hist_items) as hist_items
        from {twist_t2_schema}.twist_daily td
        inner join product_sku sku
        on td.rms_sku_num = sku.rms_sku_num
           where day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num-1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
         and (select max(day_date) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date)
        group by 1,2,3,4
    ) base
    -- fiscal lookup
    inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
      on base.day_date = dy.day_date
    -- location lookup
    inner join prd_nap_usr_vws.store_dim loc
      on base.store_num = loc.store_num
    group by 1,2
) with data primary index (wk_idnt) on commit preserve rows;

COLLECT STATS
	PRIMARY INDEX (wk_idnt)
		ON shr;


--Filter to RP TWIST Items Only
CREATE MULTISET VOLATILE TABLE rp_base as (
    select *
    from {twist_t2_schema}.twist_daily
    where day_date >= (select min(day_dt) from dt)
       and day_date <= (select max(day_dt) from dt)
       and rp_idnt = 1
) WITH DATA PRIMARY INDEX(rms_sku_num, day_date, store_num) ON COMMIT PRESERVE ROWS;

collect statistics primary index (rms_sku_num, day_date, store_num)
  , column(rms_sku_num)
  , column(day_date)
  , column(store_num)
on rp_base;


-- Add ASOH by FC (changed EOH to ASOH)
CREATE MULTISET VOLATILE TABLE fc_asoh as (
    select
        snapshot_date as day_date
        , rms_sku_id as rms_sku_num
        , join_store as store_num
        , sum(case when location_id = 489 then asoh else 0 end) as asoh_489
        , sum(case when location_id = 568 then asoh else 0 end) as asoh_568
        , sum(case when location_id = 584 then asoh else 0 end) as asoh_584
        , sum(case when location_id = 599 then asoh else 0 end) as asoh_599
        , sum(case when location_id = 659 then asoh else 0 end) as asoh_659
        , sum(case when location_id = 5629 then asoh else 0 end) as asoh_5629 -- replace location 873 with 5629
        , sum(case when location_id = 881 then asoh else 0 end) as asoh_881
    from (
        select
            snapshot_date
            , rms_sku_id
            , location_id
            , case when location_id in (489, 568, 584, 599, 659) then 808 else 828 end as join_store
            , sum(stock_on_hand_qty) as eoh
            , sum(immediately_sellable_qty) as asoh
        from PRD_NAP_USR_VWS.INVENTORY_STOCK_QUANTITY_BY_DAY_PHYSICAL_FACT a
        where snapshot_date  >= (select min(day_dt) from dt)
               and snapshot_date <= (select max(day_dt) from dt)
               and location_id in (489, 568, 584, 599, 659, 5629, 881)
        group by 1,2,3,4
        having asoh > 0
    ) b
    group by 1,2,3
) with data primary index (day_date, rms_sku_num, store_num) on commit preserve rows;

collect statistics primary index (day_date, rms_sku_num, store_num)
  , column(rms_sku_num)
  , column(day_date)
  , column(store_num)
on fc_asoh;


CREATE MULTISET VOLATILE TABLE rp_base2 as (
select
  a.*
  , b.asoh_489
  , b.asoh_568
  , b.asoh_584
  , b.asoh_599
  , b.asoh_659
  , b.asoh_5629
  , b.asoh_881
from rp_base a
left join fc_asoh b
on a.day_date = b.day_date + 1
  and a.rms_sku_num = b.rms_sku_num
  and a.store_num = b.store_num
) WITH DATA PRIMARY INDEX(rms_sku_num, day_date, store_num) ON COMMIT PRESERVE ROWS;

collect statistics primary index (rms_sku_num, day_date, store_num)
  , column(rms_sku_num)
  , column(day_date)
  , column(store_num)
on rp_base2;

-- Anchor Brands
CREATE MULTISET VOLATILE TABLE anc AS (
SELECT
	dept_num
	,supplier_idnt
FROM t2dl_das_in_season_management_reporting.anchor_brands
WHERE anchor_brand_ind = 'Y'
GROUP BY 1,2
) WITH DATA
	PRIMARY INDEX (dept_num,supplier_idnt) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (dept_num,supplier_idnt) 
		ON anc	
;

--RSB 
CREATE MULTISET VOLATILE TABLE rsb AS (
SELECT
	dept_num
	,supplier_idnt
FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
WHERE rack_strategic_brand_ind = 'Y'
GROUP BY 1,2
) WITH DATA
	PRIMARY INDEX (dept_num,supplier_idnt) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (dept_num,supplier_idnt) 
		ON rsb	
;

-- Final Roll-up and Table Creation
DELETE from {twist_t2_schema}.twist_item_weekly;

insert into {twist_t2_schema}.twist_item_weekly
select
  tot.*
  , percent_rank() over (partition by tot.wk_idnt, tot.banner, tot.dept_desc order by tot.items desc, tot.traffic desc) as quartile
  , share_fp
  , share_op
  , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from (
    select
        dy.fiscal_week_num as wk_num
        , dy.week_end_day_date as wk_end_day_dt
        , dy.week_idnt as wk_idnt
        , dy.week_454_label as wk_label
        , dy.fiscal_month_num as mth_num
        , dy.month_454_label as fiscal_mth_label
        , dy.fiscal_quarter_num as qtr_num
        , dy.quarter_label as qtr_label
        , dy.fiscal_year_num as yr_num
        , base.banner
        , base.country
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN anc.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS anchor_brand_ind
	      , CASE WHEN rsb.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS rack_strategic_brand_ind
        , sku.div_desc
        , sku.subdiv_desc
        , sku.dept_desc
        , sku.npg_ind
        , sku.supp_npg_desc
        , sku.brand_name
        , sku.supplier
        , sku.class_desc
        , sku.subclass_desc
        , sku.vpn
        , sku.color
        , sku.cc
        , current_price_type as price_type

        , sum(allocated_traffic) as traffic
        , sum(case when base.business_unit_desc = 'N.COM'  then allocated_traffic else 0 end) as ncom_traffic
        , sum(case when base.business_unit_desc = 'N.COM' and mc_instock_ind = 1 then allocated_traffic else 0 end) as ncom_instock_traffic
        , sum(case when base.business_unit_desc = 'N.COM' and fc_instock_ind = 1 then allocated_traffic else 0 end) as ncom_fc_instock_traffic

        , sum(case when base.business_unit_desc = 'FULL LINE'  then allocated_traffic else 0 end) as fls_traffic
        , sum(case when base.business_unit_desc = 'FULL LINE' and mc_instock_ind = 1 then allocated_traffic else 0 end) as fls_instock_traffic
        , sum(case when base.business_unit_desc = 'FULL LINE' and fc_instock_ind = 1 then allocated_traffic else 0 end) as fls_fc_instock_traffic

        , sum(case when base.business_unit_desc = 'RACK'  then allocated_traffic else 0 end) as rack_traffic
        , sum(case when base.business_unit_desc = 'RACK' and mc_instock_ind = 1 then allocated_traffic else 0 end) as rack_instock_traffic
        , sum(case when base.business_unit_desc = 'RACK' and fc_instock_ind = 1 then allocated_traffic else 0 end) as rack_fc_instock_traffic

        , sum(case when base.business_unit_desc = 'OFFPRICE ONLINE'  then allocated_traffic else 0 end) as rcom_traffic
        , sum(case when base.business_unit_desc = 'OFFPRICE ONLINE' and mc_instock_ind = 1 then allocated_traffic else 0 end) as rcom_instock_traffic
        , sum(case when base.business_unit_desc = 'OFFPRICE ONLINE' and fc_instock_ind = 1 then allocated_traffic else 0 end) as rcom_fc_instock_traffic

        , sum(demand) as demand
        , sum(case when base.business_unit_desc = 'FULL LINE' then demand else 0 end) as fls_demand
        , sum(case when base.business_unit_desc = 'N.COM' then demand else 0 end) as ncom_demand
        , sum(case when base.business_unit_desc = 'RACK' then demand else 0 end) as rack_demand
        , sum(case when base.business_unit_desc = 'OFFPRICE ONLINE' then demand else 0 end) as rcom_demand

        , sum(items) as items
        , sum(case when base.business_unit_desc = 'FULL LINE' then items else 0 end) as fls_items
        , sum(case when base.business_unit_desc = 'N.COM' then items else 0 end) as ncom_items
        , sum(case when base.business_unit_desc = 'RACK' then items else 0 end) as rack_items
        , sum(case when base.business_unit_desc = 'OFFPRICE ONLINE' then items else 0 end) as rcom_items

        , sum(case when base.day_date = week_end_day_date then eoh_mc else 0 end) as eoh_mc
        , sum(case when base.day_date = week_end_day_date then eoh else 0 end) as eoh_fc
        , sum(case when base.day_date = week_end_day_date then asoh_mc else 0 end) as asoh_mc
        , sum(case when base.day_date = week_end_day_date then asoh else 0 end) as asoh_fc

        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc = 'FULL LINE' then eoh else 0 end) as fls_eoh 
        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc = 'FULL LINE' then asoh else 0 end) as fls_asoh 

        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc = 'N.COM' then eoh_mc else 0 end) as ncom_eoh_mc 
        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc = 'N.COM' then eoh else 0 end) as ncom_eoh_fc
        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc = 'N.COM' then asoh_mc else 0 end) as ncom_asoh_mc 
        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc = 'N.COM' then asoh else 0 end) as ncom_asoh_fc

        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc = 'RACK' then eoh else 0 end) as rack_eoh 
        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc = 'RACK' then asoh else 0 end) as rack_asoh 

        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc =  'OFFPRICE ONLINE' then eoh else 0 end) as rcom_eoh 
        , sum(case when base.day_date = week_end_day_date and base.business_unit_desc =  'OFFPRICE ONLINE' then asoh else 0 end) as rcom_asoh 

        , sum(case when base.day_date = week_end_day_date then asoh_489 else 0 end) as asoh_489
        , sum(case when base.day_date = week_end_day_date then asoh_568 else 0 end) as asoh_568
        , sum(case when base.day_date = week_end_day_date then asoh_584 else 0 end) as asoh_584
        , sum(case when base.day_date = week_end_day_date then asoh_599 else 0 end) as asoh_599
        , sum(case when base.day_date = week_end_day_date then asoh_659 else 0 end) as asoh_659
        , sum(case when base.day_date = week_end_day_date then asoh_5629 else 0 end) as asoh_5629 -- replace location 873 with 5629
        , sum(case when base.day_date = week_end_day_date then asoh_881 else 0 end) as asoh_881

        , sum(case when base.business_unit_desc = 'N.COM' and div_desc = '340, BEAUTY' and fc_instock_ind = 1 and asoh_489 > 0 then allocated_traffic else 0 end) as instock_489
        , sum(case when base.business_unit_desc = 'N.COM' and fc_instock_ind = 1 and asoh_568 > 0 then allocated_traffic else 0 end) as instock_568
        , sum(case when base.business_unit_desc = 'N.COM' and fc_instock_ind = 1 and asoh_584 > 0 then allocated_traffic else 0 end) as instock_584
        , sum(case when base.business_unit_desc = 'N.COM' and fc_instock_ind = 1 and asoh_599 > 0 then allocated_traffic else 0 end) as instock_599
        , sum(case when base.business_unit_desc = 'N.COM' and div_desc = '340, BEAUTY' and fc_instock_ind = 1 and asoh_659 > 0 then allocated_traffic else 0 end) as instock_659
        , sum(case when base.business_unit_desc = 'OFFPRICE ONLINE' and fc_instock_ind = 1 and asoh_5629 > 0 then allocated_traffic else 0 end) as instock_5629 -- replace location 873 with 5629
        , sum(case when base.business_unit_desc = 'OFFPRICE ONLINE' and fc_instock_ind = 1 and asoh_881 > 0 then allocated_traffic else 0 end) as instock_881

        , sum(CAST(0 AS DECIMAL(10,0))) as oo_4wk_units
        , sum(CAST(0 AS DECIMAL(10,0))) as oo_4wk_dollars
        , sum(CAST(0 AS DECIMAL(10,0))) as fls_oo_4wk_units
        , sum(CAST(0 AS DECIMAL(10,0))) as ncom_oo_4wk_units
        , sum(CAST(0 AS DECIMAL(10,0))) as rack_oo_4wk_units
        , sum(CAST(0 AS DECIMAL(10,0))) as rcom_oo_4wk_units

    from rp_base2 base
    -- fiscal lookup
    inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
      on base.day_date = dy.day_date
    -- location lookup
    inner join (
        select distinct store_num
        from prd_nap_usr_vws.store_dim
        where channel_num not in (220, 240) and store_abbrev_name <> 'CLOSED' and selling_store_ind = 'S'
     ) loc
      on base.store_num = loc.store_num
      -- hierarchy lookup
    inner join product_sku sku
      on base.rms_sku_num = sku.rms_sku_num
    --ANCHOR BRANDS
    LEFT JOIN anc
	    ON sku.dept_num = anc.dept_num
	    AND sku.prmy_supp_num = anc.supplier_idnt
	    AND base.banner = 'NORD'
    --RSB
    LEFT JOIN rsb
	    ON sku.dept_num = rsb.dept_num
	    AND sku.prmy_supp_num = rsb.supplier_idnt
	    AND base.banner = 'RACK'

    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
) tot
left join shr s
on tot.wk_idnt = s.wk_idnt and tot.dept_desc = s.dept_desc
;

collect stats primary index(wk_idnt, cc), column(wk_idnt) , column(cc)
  on {twist_t2_schema}.twist_item_weekly;

SET QUERY_BAND = NONE FOR SESSION;
