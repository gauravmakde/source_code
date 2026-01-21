SET QUERY_BAND = 'App_ID=APP08364;
     DAG_ID=twist_weekly_11521_ACE_ENG;
     Task_Name=twist_summary_weekly;'
     FOR SESSION VOLATILE;

/*
T2/Table Name:{twist_t2_schema}.twist_summary_weekly
Team/Owner:Meghan Hickey (meghan.d.hickey@nordstrom.com)
Date Created/Modified: 04/03/2024 

Updates 
    * modified with department weighting approach
    * update eoh with asoh replace location 873 with 5629
    * update instock_traffic to refer to mc_instock_ind and fc_instock_traffic to fc_instock_ind

Date Created/Modified: 05/07/2024 

Updates 
    * add 873 location back

GOAL: Roll up in-stock rates for summary dashboard in tableau
*/

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

/* 1. Grab Weighting for Banner In-stock Rates */
create multiset volatile table hist as (
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

collect stats primary index (wk_idnt), column (wk_idnt) on hist;

-- Add FC level inventory (changed EOH to ASOH)
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
        , sum(case when location_id = 873 then asoh else 0 end) as asoh_873 
        , sum(case when location_id = 5629 then asoh else 0 end) as asoh_5629 
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
        where snapshot_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
                and (select max(day_date) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date)
               and location_id in (489, 568, 584, 599, 659, 873, 5629, 881)
        group by 1,2,3,4
        having asoh > 0
    ) b
    group by 1,2,3
) with data primary index (day_date, rms_sku_num, store_num) on commit preserve rows;


/* 2. Roll up across channel/time frame but break down for performance */
-- TY N.com
create multiset volatile table ty_ncom as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(case when div_desc = 'BEAUTY' and fc_instock_ind = 1 and f.asoh_489 > 0 then allocated_traffic else 0 end) as instock_489
        , sum(case when fc_instock_ind = 1 and f.asoh_568 > 0 then allocated_traffic else 0 end) as instock_568
        , sum(case when fc_instock_ind = 1 and f.asoh_584 > 0 then allocated_traffic else 0 end) as instock_584
        , sum(case when fc_instock_ind = 1 and f.asoh_599 > 0 then allocated_traffic else 0 end) as instock_599
        , sum(case when div_desc = 'BEAUTY' and fc_instock_ind = 1 and f.asoh_659 > 0 then allocated_traffic else 0 end) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     left join fc_asoh f
        on base.day_date = f.day_date
        and base.rms_sku_num = f.rms_sku_num
        and base.store_num = f.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
            and (select max(day_date) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date)
        and channel_num = 120
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, npg_ind, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, npg_ind, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ty_ncom;

-- LY N.com
create multiset volatile table ly_ncom as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(case when sku.div_desc = 'BEAUTY' and fc_instock_ind = 1 and f.asoh_489 > 0 then allocated_traffic else 0 end) as instock_489
        , sum(case when fc_instock_ind = 1 and f.asoh_568 > 0 then allocated_traffic else 0 end) as instock_568
        , sum(case when fc_instock_ind = 1 and f.asoh_584 > 0 then allocated_traffic else 0 end) as instock_584
        , sum(case when fc_instock_ind = 1 and f.asoh_599 > 0 then allocated_traffic else 0 end) as instock_599
        , sum(case when sku.div_desc = 'BEAUTY' and fc_instock_ind = 1 and f.asoh_659 > 0 then allocated_traffic else 0 end) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     left join fc_asoh f
        on base.day_date = f.day_date
        and base.rms_sku_num = f.rms_sku_num
        and base.store_num = f.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
            and (select max(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
        and channel_num = 120
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ly_ncom;


-- TY R.com
create multiset volatile table ty_rcom as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num  
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(case when fc_instock_ind = 1 and f.asoh_873 > 0 then allocated_traffic else 0 end) as instock_873
        , sum(case when fc_instock_ind = 1 and f.asoh_5629 > 0 then allocated_traffic else 0 end) as instock_5629
        , sum(case when fc_instock_ind = 1 and f.asoh_881 > 0 then allocated_traffic else 0 end) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     left join fc_asoh f
        on base.day_date = f.day_date
        and base.rms_sku_num = f.rms_sku_num
        and base.store_num = f.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
            and (select max(day_date) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date)
        and channel_num = 250
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ty_rcom;

-- LY R.com
create multiset volatile table ly_rcom as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(case when fc_instock_ind = 1 and f.asoh_873 > 0 then allocated_traffic else 0 end) as instock_873
        , sum(case when fc_instock_ind = 1 and f.asoh_5629 > 0 then allocated_traffic else 0 end) as instock_5629
        , sum(case when fc_instock_ind = 1 and f.asoh_881 > 0 then allocated_traffic else 0 end) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     left join fc_asoh f
        on base.day_date = f.day_date
        and base.rms_sku_num = f.rms_sku_num
        and base.store_num = f.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
            and (select max(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
        and channel_num = 250
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ly_rcom;


-- TY Rack H1
create multiset volatile table ty_rack_h1 as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
            and (select max(day_date) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date and fiscal_quarter_num <= 2)
        and channel_num = 210
        and rp_idnt = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ty_rack_h1;


-- TY Rack H2
create multiset volatile table ty_rack_h2 as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1) and fiscal_quarter_num > 2)
            and (select max(day_date) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date)
        and channel_num = 210
        and rp_idnt = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ty_rack_h2;

--LY Rack H1
create multiset volatile table ly_rack_h1 as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
            and (select max(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1) and fiscal_quarter_num <= 2)
        and channel_num = 210
        and rp_idnt = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ly_rack_h1;

-- LY Rack H2
create multiset volatile table ly_rack_h2 as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1) and fiscal_quarter_num > 2)
            and (select max(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1) and fiscal_quarter_num > 2)
        and channel_num = 210
        and rp_idnt = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ly_rack_h2;

-- TY FLS H1
create multiset volatile table ty_fls_h1 as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
            and (select max(day_date) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date and fiscal_quarter_num <= 2)
        and channel_num = 110
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ty_fls_h1;

-- TY FLS H2
create multiset volatile table ty_fls_h2 as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1) and fiscal_quarter_num > 2)
            and (select max(day_date) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date)
        and channel_num = 110
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
  , column (week_idnt)
  , column (store_num)
  , column (market)
on ty_fls_h2;

--LY FLS H1
create multiset volatile table ly_fls_h1 as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1))
            and (select max(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1) and fiscal_quarter_num <= 2)
        and channel_num = 110
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

-- LY FLS H2
create multiset volatile table ly_fls_h2 as (
    select
        dy.week_idnt
        , base.banner
        , base.country
        , base.store_num
        , base.dma as market
        , rp_idnt as rp_product_ind
        , case when rp_idnt =1 then 'RP'
            when rp_idnt =0 then 'NRP'
            else 'OTHER' end as rp_desc
        , CASE WHEN sku.partner_relationship_type_code = 'ECONCESSION' THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when npg_ind = 'Y' then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS DECIMAL(38,4))) as instock_489
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_568
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_584
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_599
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_659
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_873
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_5629
        , sum(CAST(0 AS DECIMAL(38,4))) as instock_881
     from {twist_t2_schema}.twist_daily base
     inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
        on base.day_date = dy.day_date and dy.ty_ly_lly_ind <> 'LLY'
      -- hierarchy lookup
     inner join prd_nap_usr_vws.product_sku_dim_vw sku
        on base.rms_sku_num = sku.rms_sku_num
        and sku.channel_country = 'US'
     left join prd_nap_usr_vws.vendor_dim supp
        on sku.prmy_supp_num =supp.vendor_num
     inner join prd_nap_usr_vws.store_dim loc
        on base.store_num = loc.store_num
     where base.day_date between (select min(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1) and fiscal_quarter_num > 2)
            and (select max(day_date) from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where fiscal_year_num = (select distinct fiscal_year_num - 1 from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1) and fiscal_quarter_num > 2)
        and channel_num = 110
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) with data primary index (week_idnt, store_num, market, dept_desc, supplier, price_type) on commit preserve rows;

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

-- Final Roll-up and Table Creation
DELETE from {twist_t2_schema}.twist_summary_weekly;

insert into {twist_t2_schema}.twist_summary_weekly
select
    wk.fiscal_week_num as wk_num
    , wk.week_end_day_date as wk_end_day_dt
    , base.week_idnt as wk_idnt
    , wk.week_454_label as wk_label
    , wk.fiscal_month_num as mth_num
    , wk.month_454_label as fiscal_mth_label
    , wk.fiscal_quarter_num as qtr_num
    , wk.quarter_label as qtr_label
    , wk.fiscal_year_num as yr_num
    , base.banner
    , base.country
    , trim(loc.channel_num || ', ' || channel_desc)  as channel
    , trim(loc.store_num || ', ' || store_name) as location
    , market
    , coalesce(upper(dma_desc), 'UNKNOWN') as dma_desc
    , rp_product_ind
    , rp_desc
    , mp_ind -- MP
    , CASE WHEN anc.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS anchor_brand_ind
	, CASE WHEN rsb.supplier_idnt IS NOT NULL THEN 1 ELSE 0 END AS rack_strategic_brand_ind
    , div_desc
    , subdiv_desc
    , base.dept_desc
    , supplier
    , npg_ind
    , supp_npg_desc
    , price_type
    , traffic
    , instock_traffic
    , fc_instock_traffic
    , hist_items
    , share_fp
    , share_op
    , sku_count
    , instock_489
    , instock_568
    , instock_584
    , instock_599
    , instock_659
    , instock_873
    , instock_5629 
    , instock_881
    , CURRENT_TIMESTAMP as dw_sys_load_tmstp
from (
select * from ty_ncom
union all
select * from ly_ncom
union all
select * from ty_rcom
union all
select * from ly_rcom
union all
select * from ty_rack_h1
union all
select * from ty_rack_h2
union all
select * from ly_rack_h1
union all
select * from ly_rack_h2
union all
select * from ty_fls_h1
union all
select * from ty_fls_h2
union all
select * from ly_fls_h1
union all
select * from ly_fls_h2
) base
-- fiscal lookup
inner join (
    select
        distinct fiscal_week_num
        , week_end_day_date
        , week_454_label
        , fiscal_month_num
        , month_454_label
        , fiscal_quarter_num
        , quarter_label
        , fiscal_year_num
        , week_idnt
    from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW
) wk
  on base.week_idnt = wk.week_idnt
-- location lookup
inner join prd_nap_usr_vws.store_dim loc
  on base.store_num = loc.store_num
  -- dma lookup
left join prd_nap_usr_vws.org_dma dma
  on base.market = dma.dma_code
--ANCHOR BRANDS
LEFT JOIN anc
  ON base.dept_num = anc.dept_num
  AND base.prmy_supp_num = anc.supplier_idnt
  AND base.banner = 'NORD'
--RSB
LEFT JOIN rsb
  ON base.dept_num = rsb.dept_num
  AND base.prmy_supp_num = rsb.supplier_idnt
  AND base.banner = 'RACK'
left join hist shr
  on base.week_idnt = shr.wk_idnt and base.dept_desc = shr.dept_desc
where channel_num not in (220, 240) and store_abbrev_name <> 'CLOSED' and selling_store_ind = 'S';

collect stats primary index(wk_idnt, location, dept_desc, supplier), column(wk_idnt) , column(location), column(dept_desc), column(supplier)
  on {twist_t2_schema}.twist_summary_weekly;

SET QUERY_BAND = NONE FOR SESSION;
