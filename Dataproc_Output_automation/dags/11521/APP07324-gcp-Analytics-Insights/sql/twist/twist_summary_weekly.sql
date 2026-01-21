
CREATE TEMPORARY TABLE IF NOT EXISTS product_sku AS (
  select
      rms_sku_num
      , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
      , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
      , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
      , npg_ind
      , dept_num
      , prmy_supp_num
      , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
      , upper(sku.brand_name) as brand_name
      , UPPER(supp.vendor_name) as supplier
      , trim(sku.class_num || ', ' || sku.class_desc) as class_desc
      , trim(sku.sbclass_num || ', ' || sku.sbclass_desc) as subclass_desc
      , sku.supp_part_num || ', ' || sku.style_desc as vpn
      , sku.supp_color as color
      , sku.supp_part_num || ', ' || sku.style_desc || ', ' || sku.supp_color as cc

from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
    on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
where LOWER(channel_country) = LOWER('US')

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) ;


CREATE TEMPORARY TABLE IF NOT EXISTS hist as (
    select
        week_idnt as wk_idnt
        , dept_desc
        , sum(case when channel_num = 110 then hist_items else 0 end)*1.000/nullif(sum(case when lower(banner) = LOWER('NORD') then hist_items else 0 end)*1.000,0) as share_fp
        , sum(case when channel_num = 210 then hist_items else 0 end)*1.000/nullif(sum(case when lower(banner) = LOWER('RACK') then hist_items else 0 end)*1.000,0) as share_op
    from (
        select
            day_date,
            sku.dept_desc,
            store_num,
            banner,
            sum(hist_items) as hist_items
        from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily td
        inner join product_sku sku
        on td.rms_sku_num = sku.rms_sku_num
           where day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num-1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
         and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date('PST8PDT'))
        group by 1,2,3,4
    ) base
    -- fiscal lookup
    inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
      on base.day_date = dy.day_date
    -- location lookup
    inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
      on LOWER(base.store_num) =LOWER(CAST(loc.store_num AS STRING))
    group by 1,2
) ;

-- collect stats primary index (wk_idnt), column (wk_idnt) on hist;

-- Add FC level inventory (changed EOH to ASOH)
CREATE TEMPORARY TABLE IF NOT EXISTS fc_asoh as (
    select
        snapshot_date as day_date
        , rms_sku_id as rms_sku_num
        , join_store as store_num
        , sum(case when LOWER(location_id) = LOWER('489') then asoh else 0 end) as asoh_489
        , sum(case when LOWER(location_id) = LOWER('568') then asoh else 0 end) as asoh_568
        , sum(case when LOWER(location_id) = LOWER('584') then asoh else 0 end) as asoh_584
        , sum(case when LOWER(location_id) = LOWER('599') then asoh else 0 end) as asoh_599
        , sum(case when LOWER(location_id) = LOWER('659') then asoh else 0 end) as asoh_659
        , sum(case when LOWER(location_id) = LOWER('873') then asoh else 0 end) as asoh_873 
        , sum(case when LOWER(location_id) = LOWER('5629') then asoh else 0 end) as asoh_5629 
        , sum(case when LOWER(location_id) = LOWER('881') then asoh else 0 end) as asoh_881
    from (
        select
            snapshot_date
            , rms_sku_id
            , location_id
            , case when LOWER(location_id) in (LOWER('489'), LOWER('568'), LOWER('584'), LOWER('599'), LOWER('659')) then 808 else 828 end as join_store
            , sum(stock_on_hand_qty) as eoh
            , sum(immediately_sellable_qty) as asoh
        from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact a
        where snapshot_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
                and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date('PST8PDT'))
               and LOWER(location_id) in (LOWER('489'),LOWER('568'),LOWER('584'),LOWER('599'),LOWER('659'),LOWER('873'),LOWER('5629'),LOWER('881'))
        group by 1,2,3,4
        having asoh > 0
    ) b
    group by 1,2,3
) ;


CREATE TEMPORARY TABLE IF NOT EXISTS ty_ncom as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(case when LOWER(div_desc) = LOWER('BEAUTY') and fc_instock_ind = 1 and f.asoh_489 > 0 then allocated_traffic else 0 end) as instock_489
        , sum(case when fc_instock_ind = 1 and f.asoh_568 > 0 then allocated_traffic else 0 end) as instock_568
        , sum(case when fc_instock_ind = 1 and f.asoh_584 > 0 then allocated_traffic else 0 end) as instock_584
        , sum(case when fc_instock_ind = 1 and f.asoh_599 > 0 then allocated_traffic else 0 end) as instock_599
        , sum(case when LOWER(div_desc) = LOWER('BEAUTY') and fc_instock_ind = 1 and f.asoh_659 > 0 then allocated_traffic else 0 end) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on LOWER(base.store_num) = LOWER(CAST(loc.store_num AS STRING))
     left join fc_asoh f
        on base.day_date = f.day_date
        and LOWER(base.rms_sku_num) = LOWER(f.rms_sku_num)
        and base.store_num = lower(cast(f.store_num as string))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date('PST8PDT'))
        and channel_num = 120
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, npg_ind, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ty_ncom;

-- LY N.com
CREATE TEMPORARY TABLE IF NOT EXISTS ly_ncom as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(case when LOWER(sku.div_desc) = LOWER('BEAUTY') and fc_instock_ind = 1 and f.asoh_489 > 0 then allocated_traffic else 0 end) as instock_489
        , sum(case when fc_instock_ind = 1 and f.asoh_568 > 0 then allocated_traffic else 0 end) as instock_568
        , sum(case when fc_instock_ind = 1 and f.asoh_584 > 0 then allocated_traffic else 0 end) as instock_584
        , sum(case when fc_instock_ind = 1 and f.asoh_599 > 0 then allocated_traffic else 0 end) as instock_599
        , sum(case when LOWER(sku.div_desc) = LOWER('BEAUTY') and fc_instock_ind = 1 and f.asoh_659 > 0 then allocated_traffic else 0 end) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     left join fc_asoh f
        on base.day_date = f.day_date
        and LOWER(base.rms_sku_num) = LOWER(f.rms_sku_num)
        and base.store_num = LOWER(CAST(f.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
        and channel_num = 120
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ly_ncom;



CREATE TEMPORARY TABLE IF NOT EXISTS ty_rcom as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num  
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(case when fc_instock_ind = 1 and f.asoh_873 > 0 then allocated_traffic else 0 end) as instock_873
        , sum(case when fc_instock_ind = 1 and f.asoh_5629 > 0 then allocated_traffic else 0 end) as instock_5629
        , sum(case when fc_instock_ind = 1 and f.asoh_881 > 0 then allocated_traffic else 0 end) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     left join fc_asoh f
        on base.day_date = f.day_date
        and LOWER(base.rms_sku_num) = LOWER(f.rms_sku_num)
        and base.store_num = LOWER(CAST(f.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date('PST8PDT'))
        and channel_num = 250
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- -- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ty_rcom;

-- LY R.com
CREATE TEMPORARY TABLE IF NOT EXISTS ly_rcom as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(case when fc_instock_ind = 1 and f.asoh_873 > 0 then allocated_traffic else 0 end) as instock_873
        , sum(case when fc_instock_ind = 1 and f.asoh_5629 > 0 then allocated_traffic else 0 end) as instock_5629
        , sum(case when fc_instock_ind = 1 and f.asoh_881 > 0 then allocated_traffic else 0 end) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     left join fc_asoh f
        on base.day_date = f.day_date
        and LOWER(base.rms_sku_num) = LOWER(f.rms_sku_num)
        and base.store_num = LOWER(CAST(f.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
        and channel_num = 250
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ly_rcom;


-- TY Rack H1
CREATE TEMPORARY TABLE IF NOT EXISTS ty_rack_h1 as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date('PST8PDT') and fiscal_quarter_num <= 2)
        and channel_num = 210
        and rp_idnt = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ty_rack_h1;


-- TY Rack H2
CREATE TEMPORARY TABLE IF NOT EXISTS ty_rack_h2 as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) = LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)) and fiscal_quarter_num > 2)
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date('PST8PDT'))
        and channel_num = 210
        and rp_idnt = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ty_rack_h2;

--LY Rack H1
CREATE TEMPORARY TABLE IF NOT EXISTS ly_rack_h1 as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)) and fiscal_quarter_num <= 2)
        and channel_num = 210
        and rp_idnt = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ly_rack_h1;

-- LY Rack H2
CREATE TEMPORARY TABLE IF NOT EXISTS ly_rack_h2 as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)) and fiscal_quarter_num > 2)
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)) and fiscal_quarter_num > 2)
        and channel_num = 210
        and rp_idnt = 1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ly_rack_h2;

-- TY FLS H1
CREATE TEMPORARY TABLE IF NOT EXISTS ty_fls_h1 as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date('PST8PDT') and fiscal_quarter_num <= 2)
        and channel_num = 110
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ty_fls_h1;

-- TY FLS H2
CREATE TEMPORARY TABLE IF NOT EXISTS ty_fls_h2 as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)) and fiscal_quarter_num > 2)
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date('PST8PDT'))
        and channel_num = 110
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- collect stats primary index(week_idnt, store_num, market, dept_desc, supplier, price_type)
--   , column (week_idnt)
--   , column (store_num)
--   , column (market)
-- on ty_fls_h2;

--LY FLS H1
CREATE TEMPORARY TABLE IF NOT EXISTS ly_fls_h1 as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)))
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)) and fiscal_quarter_num <= 2)
        and channel_num = 110
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- LY FLS H2
CREATE TEMPORARY TABLE IF NOT EXISTS ly_fls_h2 as (
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
        , CASE WHEN LOWER(sku.partner_relationship_type_code) = LOWER('ECONCESSION') THEN 'Y' ELSE 'N' END AS mp_ind -- MP
        , trim(sku.div_num || ', ' || sku.div_desc) as div_desc
        , trim(sku.grp_num || ', ' || sku.grp_desc) as subdiv_desc
        , trim(sku.dept_num || ', ' || sku.dept_desc) as dept_desc
        , dept_num
        , prmy_supp_num
        , UPPER(supp.vendor_name) as supplier
        , npg_ind
        , case when LOWER(npg_ind) = LOWER('Y') then 'NPG' else 'NON-NPG' end as supp_npg_desc
        , current_price_type as price_type
        , sum(allocated_traffic) as traffic
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
        , sum(case when fc_instock_ind = 1 then allocated_traffic else 0 end) as fc_instock_traffic
        , sum(hist_items) as hist_items
        , count(distinct base.rms_sku_num) as sku_count

        , sum(CAST(0 AS NUMERIC)) as instock_489
        , sum(CAST(0 AS NUMERIC)) as instock_568
        , sum(CAST(0 AS NUMERIC)) as instock_584
        , sum(CAST(0 AS NUMERIC)) as instock_599
        , sum(CAST(0 AS NUMERIC)) as instock_659
        , sum(CAST(0 AS NUMERIC)) as instock_873
        , sum(CAST(0 AS NUMERIC)) as instock_5629
        , sum(CAST(0 AS NUMERIC)) as instock_881
     from `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_daily base
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw dy
        on base.day_date = dy.day_date and LOWER(dy.ty_ly_lly_ind) <> LOWER('LLY')
      -- hierarchy lookup
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw sku
        on LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
        and LOWER(sku.channel_country) = LOWER('US')
     left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.vendor_dim supp
        on LOWER(sku.prmy_supp_num) =LOWER(supp.vendor_num)
     inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
        on base.store_num = LOWER(CAST(loc.store_num AS STRING))
     where base.day_date between (select min(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)) and fiscal_quarter_num > 2)
            and (select max(day_date) from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where fiscal_year_num = (select distinct fiscal_year_num - 1 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw where day_date = DATE_SUB(current_date('PST8PDT'),INTERVAL 1 DAY)) and fiscal_quarter_num > 2)
        and channel_num = 110
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) ;

-- Anchor Brands
CREATE TEMPORARY TABLE IF NOT EXISTS anc AS (
SELECT
	dept_num
	,supplier_idnt
FROM t2dl_das_in_season_management_reporting.anchor_brands
WHERE LOWER(anchor_brand_ind) = LOWER('Y')
GROUP BY 1,2
);

-- COLLECT STATS
-- 	PRIMARY INDEX (dept_num,supplier_idnt) 
-- 		ON anc	
-- ;

--RSB 
CREATE TEMPORARY TABLE IF NOT EXISTS rsb AS (
SELECT
	dept_num
	,supplier_idnt
FROM t2dl_das_in_season_management_reporting.rack_strategic_brands
WHERE LOWER(rack_strategic_brand_ind) = LOWER('Y')
GROUP BY 1,2
);

-- Final Roll-up and Table Creation
TRUNCATE TABLE  `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_summary_weekly;

insert into `{{params.gcp_project_id}}`.{{params.twist_t2_schema}}.twist_summary_weekly
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
    , CAST(share_fp AS BIGNUMERIC) AS share_fp
    , CAST(share_op AS BIGNUMERIC) AS share_op
    , sku_count
    , instock_489
    , instock_568
    , instock_584
    , instock_599
    , instock_659
    , instock_873
    , instock_5629 
    , instock_881
    , current_datetime('PST8PDT') as dw_sys_load_tmstp
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
    from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.realigned_date_lkup_vw
) wk
  on base.week_idnt = wk.week_idnt

inner join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim loc
  on base.store_num = LOWER(CAST(loc.store_num AS STRING))

left join `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_dma dma
  on base.market = dma.dma_code

LEFT JOIN anc
  ON base.dept_num = anc.dept_num
  AND LOWER(base.prmy_supp_num) = LOWER(CAST(anc.supplier_idnt AS STRING))
  AND LOWER(base.banner) = LOWER('NORD')
--RSB
LEFT JOIN rsb
  ON base.dept_num = rsb.dept_num
  AND LOWER(base.prmy_supp_num) = LOWER(CAST(rsb.supplier_idnt AS STRING))
  AND LOWER(base.banner) = LOWER('RACK')
left join hist shr
  on base.week_idnt = shr.wk_idnt and base.dept_desc = shr.dept_desc
where channel_num not in (220, 240) and LOWER(store_abbrev_name) <> LOWER('CLOSED') and LOWER(selling_store_ind) = LOWER('S');

-- collect stats primary index(wk_idnt, location, dept_desc, supplier), column(wk_idnt) , column(location), column(dept_desc), column(supplier)
  -- on t2dl_das_twist.twist_summary_weekly;

-- SET QUERY_BAND = NONE FOR SESSION;
