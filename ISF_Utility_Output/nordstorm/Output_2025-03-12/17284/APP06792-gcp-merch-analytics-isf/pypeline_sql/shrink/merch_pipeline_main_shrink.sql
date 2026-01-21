/* Pull LY & TY YTD */
--drop table dates;
create multiset volatile table dates as (
    select
        a.day_date
        , a.ty_ly_lly_ind
        , a.week_end_day_date
        , a.week_idnt
        , a.week_454_label
        , a.fiscal_week_num
        , a.month_idnt
        , a.month_454_label
        , a.quarter_label
        , a.half_label
        , a.fiscal_year_num
  		  , b.week_idnt AS week_idnt_true
  		  , case when a.week_end_day_date = a.month_end_day_date then 1 else 0 end as last_week_of_month_ind
  		  , case when a.month_end_day_date < current_date then 1 else 0 end as completed_month_ind
  		  , case when max(a.week_idnt) over() = a.week_idnt then 1 else 0 end as max_week_ind
    from  PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW a
  	left join prd_nap_usr_vws.day_cal_454_dim b
          	on a.day_date = b.day_date
    where a.ty_ly_lly_ind in ('TY', 'LY') and a.week_end_day_date < current_date
) with data primary index(day_date, week_idnt)  on commit preserve rows
;

/*All unit adjustments in TC23*/
--drop table ua;
create multiset volatile table ua as (
    select
        week_end_day_date
        , cast(a.location_num as int) as location_num
        , b.channel_num
        , a.general_ledger_reference_number as gl_ref_no
        , e.shrink_category
        , e.reason_description
        , a.department_num
        , a.class_num
        , a.subclass_num
        , upper(d.brand_name) as brand_name
        , upper(f.vendor_name) as supplier
       -- , a.rms_sku_num
        , sum(total_cost_amount) as shrink_cost
        , sum(total_retail_amount) as shrink_retail
        , sum(quantity) as shrink_units
    from PRD_NAP_BASE_VWS.MERCH_EXPENSE_TRANSFER_LEDGER_FACT a
    inner join prd_nap_usr_vws.store_dim b
      on a.location_num = b.store_num
      and b.store_country_code = 'US'
    inner join dates c
      on a.transaction_date = c.day_date
    left join prd_nap_usr_vws.product_sku_dim_vw d
      on a.rms_sku_num = d.rms_sku_num
      and d.channel_country = 'US'
    inner join T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.shrink_gl_mapping e
      on a.general_ledger_reference_number = e.reason_code
    left join prd_nap_usr_vws.vendor_dim f
      on d.prmy_supp_num = f.vendor_num
    where a.department_num not in (select distinct dept_num from prd_nap_usr_vws.product_sku_dim_vw where div_num = 600)
      and store_short_name not like '%CLSD%'
      /* Goofy NPR codes relevant only to specific locs starting May 2024 */
      and not (a.location_num = 808 and gl_ref_no = 376 and transaction_date >= '2024-05-05')
      and not (a.location_num = 828 and gl_ref_no = 248 and transaction_date >= '2024-05-05')
      and channel_num not in (930, 240)
    group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(week_end_day_date, location_num, gl_ref_no, department_num, subclass_num, brand_name) on commit preserve rows
;

/* SL data includes weekly data for PI in stores and additional shrink in FCs */
--drop table sl;
create multiset volatile table sl as (
    select
        a.end_of_week_date as week_end_day_date
        , cast(a.location_num as int) as location_num
        , b.channel_num
        , cast(null as varchar(50)) as gl_ref_no
        , case when (channel_num in (110, 210) or location_num in (82, 592)) and location_num not in (209, 297, 697) then 'PI/Estimated'
            when channel_num in (120, 220, 250, 260, 310, 920, 930) or location_num in (209, 297, 697) then 'Additional Shrink'
            else null end as shrink_category
        , cast(null as varchar(150)) as reason_description
        , a.product_hierarchy_dept_num as department_num
        , a.product_hierarchy_class_num as class_num
        , a.product_hierarchy_subclass_num as subclass_num
        , cast(null as varchar(150)) as brand_name
        , cast(null as varchar(150)) as supplier
       -- , a.rms_sku_num
        , sum(total_shrinkage_cost*-1) as shrink_cost
        , cast(0 as DECIMAL(38,9)) as shrink_retail
        , cast(0 as DECIMAL(38,0)) as shrink_units
    from PRD_NAP_BASE_VWS.MERCH_STOCKLEDGER_WEEK_FACT a
    inner join prd_nap_usr_vws.store_dim b
      on a.location_num = b.store_num
      and b.store_country_code = 'US'
    inner join dates c
      on a.end_of_week_date = c.day_date
      and c.day_date = c.week_end_day_date
    where channel_num in (110, 120, 210, 220, 250, 260, 310, 920, 930)
        and a.product_hierarchy_dept_num not in (select distinct dept_num from prd_nap_usr_vws.product_sku_dim_vw where div_num = 600)
        and total_shrinkage_cost <> 0
        and store_short_name not like '%CLSD%'
    group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(week_end_day_date, location_num, department_num, subclass_num)  on commit preserve rows
;

/* Bring in Additional Shrink by Week for TC22s */
--drop table tc22;
create multiset volatile table tc22 as (
    select
        week_end_day_date
        , cast(a.store_num as int) as location_num
        , b.channel_num
        , a.general_reference_number as gl_ref_no
        , 'Additional Shrink' as shrink_category
        , e.reason_description
        , a.dept_num as department_num
        , a.class_num
        , a.subclass_num
        , upper(d.brand_name) as brand_name
        , upper(f.vendor_name) as supplier
       -- , a.rms_sku_num
        , sum(total_cost_amt) as shrink_cost
        , sum(total_retail_amt) as shrink_retail
        , sum(total_units) as shrink_units
    from PRD_NAP_BASE_VWS.MERCH_SHRINK_TC22_SKU_STORE_DAY_FACT a
    inner join prd_nap_usr_vws.store_dim b
      on a.store_num = b.store_num
      and b.store_country_code = 'US'
    inner join dates c
      on a.posting_date = c.day_date
    left join prd_nap_usr_vws.product_sku_dim_vw d
      on a.rms_sku_num = d.rms_sku_num
      and d.channel_country = 'US'
    left join T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.shrink_gl_mapping e
      on a.general_reference_number = e.reason_code
    left join prd_nap_usr_vws.vendor_dim f
      on d.prmy_supp_num = f.vendor_num
    where (channel_num in (120, 220, 250, 260, 310, 920, 930) or a.store_num in (209, 297, 697))
          and a.dept_num not in (select distinct dept_num from prd_nap_usr_vws.product_sku_dim_vw where div_num = 600)
            and store_short_name not like '%CLSD%'
    group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(week_end_day_date, location_num, department_num, subclass_num)  on commit preserve rows
;

/* Create insert for gap between total additional shrink and TC22s that gets inserted to tie out at end of month */
-- drop table eom_additional;
create multiset volatile table eom_additional as (
    select
        c.week_end_day_date
        , cast(a.location_num as int) as location_num
        , channel_num
        , cast(null as varchar(50))  as gl_ref_no
        , 'Additional Shrink' as shrink_category
        , cast(null as varchar(150)) reason_description
        , a.department_num
        , a.class_num
        , a.subclass_num
        , cast(null as varchar(150)) as brand_name
        , cast(null as varchar(150)) as supplier
        , a.shrink_c - coalesce(b.shrink_c,0) as shrink_cost
        , cast(0 as DECIMAL(38,9)) as shrink_retail
        , cast(0 as DECIMAL(38,0)) as shrink_units
    from (
        select
            month_idnt
            , location_num
            , department_num
            , class_num
            , subclass_num
            , sum(shrink_cost) as shrink_c
        from sl a
        inner join dates c
          on a.week_end_day_date = c.day_date
          and c.day_date = c.week_end_day_date
        where shrink_category = 'Additional Shrink'
          and not (shrink_cost between -.01 and .01)
        group by 1,2,3,4,5
    ) a
    left join (
        select
            month_idnt
            , location_num
            , department_num
            , class_num
            , subclass_num
            , sum(shrink_cost) as shrink_c
        from tc22 a
        inner join dates c
          on a.week_end_day_date = c.day_date
          and c.day_date = c.week_end_day_date
        where shrink_category = 'Additional Shrink'
        group by 1,2,3,4,5
    ) b
      on a.month_idnt = b.month_idnt
        and a.location_num = b.location_num
        and a.department_num = b.department_num
        and a.class_num = b.class_num
        and a.subclass_num = b.subclass_num
    inner join (
        select
            distinct week_end_day_date
            , month_idnt
            , fiscal_year_num
        from dates
        where last_week_of_month_ind = 1 and completed_month_ind = 1
    ) c
      on a.month_idnt = c.month_idnt
    inner join prd_nap_usr_vws.store_dim d
      on a.location_num = d.store_num
      and d.store_country_code = 'US'
    where not(cast(shrink_cost as decimal(10,2)) between - 1 and 1) and fiscal_year_num > 2023
) with data primary index(week_end_day_date, location_num, department_num, subclass_num)  on commit preserve rows
;


/*Final Combined Additional Table */
-- drop table addtl;
create multiset volatile table addtl as (
    select * from sl where shrink_category = 'Additional Shrink' and week_end_day_date <= '2024-02-04'
    union all

    select * from eom_additional
    union all

    select a.*
    from tc22 a
    inner join (select distinct week_end_day_date, month_idnt, completed_month_ind from dates) b
      on a.week_end_day_date = b.week_end_day_date
    left join (
          select
              month_idnt
              , location_num
              , department_num
              , class_num
              , subclass_num
          from sl a
          inner join dates c
            on a.week_end_day_date = c.day_date
            and c.day_date = c.week_end_day_date
          where shrink_category = 'Additional Shrink'
            and not (shrink_cost between -.01 and .01)
          group by 1,2,3,4,5
     ) c
       on b.month_idnt = c.month_idnt
          and a.location_num = c.location_num
          and a.department_num = c.department_num
          and a.class_num = c.class_num
          and a.subclass_num = c.subclass_num
     where not(shrink_cost between -.01 and .01)
        and a.week_end_day_date > '2024-02-04'
        and (c.month_idnt is not null or completed_month_ind = 0)
) with data primary index(week_end_day_date, location_num, department_num, subclass_num)  on commit preserve rows
;

/* Manual Accrual Data */
--drop table ma;
create multiset volatile table ma as (
    select
        week_end_day_date
        , cast(a.store_num as int) as location_num
        , b.channel_num
        , cast(null as varchar(50)) as gl_ref_no
        , 'Manual Accural/Adjustment' as shrink_category
        , cast(null as varchar(150)) as reason_description
        , cast(null as int) as department_num
        , cast(null as int) as class_num
        , cast(null as int) as subclass_num
        , cast(null as varchar(150)) as brand_name
        , cast(null as varchar(150)) as supplier
        , sum(shrink_cost_amt) as shrink_cost
        , sum(shrink_retail_amt) as shrink_retail
        , sum(shrink_units) as shrink_units
    from PRD_NAP_BASE_VWS.MERCH_SHRINK_ACCRUAL_STORE_WEEK_FACT a
    inner join prd_nap_usr_vws.store_dim b
      on a.store_num = b.store_num
      and b.store_country_code = 'US'
    inner join (select distinct week_idnt, week_end_day_date from dates) c
      on a.week_num = c.week_idnt
    group by 1,2,3,4,5,6,7,8,9,10,11
) with data primary index(week_end_day_date, location_num)  on commit preserve rows
;

create multiset volatile table sales as (
    select
        week_end_day_date
        , cast(a.store_num as int) as store_num
        , a.channel_num
        , department_num
        , class_num
        , subclass_num
        , cast(null as varchar(150)) as brand_name
        , cast(null as varchar(150)) as supplier
        , cast(null as varchar(150)) as shrink_category
       -- , a.rms_sku_num
        , sum(net_sales_cost) as net_sales_cost
        , sum(net_sales_retail) as net_sales_retail
        , sum(sales_units) as net_sales_units
    from PRD_NAP_VWS.MERCH_EOW_SL_OLAP_VW a
    inner join prd_nap_usr_vws.store_dim b
      on a.store_num = b.store_num
      and b.store_country_code = 'US'
    inner join (
        select
            distinct week_idnt
            , week_idnt_true
            , week_end_day_date
        from dates
     ) c
      on a.week_num = c.week_idnt_true
    where a.channel_num in (110, 120, 210, 220, 250, 260, 310, 920, 930)
        and store_short_name not like '%CLSD%'
        and sales_units <> 0
      and a.department_num not in (select distinct dept_num from prd_nap_usr_vws.product_sku_dim_vw where div_num in (600,800,96))
    group by 1,2,3,4,5,6,7,8,9
) with data primary index(week_end_day_date, store_num, department_num, subclass_num)  on commit preserve rows
;

/* Create weekly shrink detail table */
delete from {environment_schema}.shrink_detail_weekly all;
insert into {environment_schema}.shrink_detail_weekly
    select * from ua
    union all
    select * from sl where shrink_category <> 'Additional Shrink'
    union all
    select * from ma
    union all
    select * from addtl
;

update {environment_schema}.shrink_detail_weekly
  set channel_num = 120
    where location_num = 209
;

/* Create weekly shrink summary table */
delete from {environment_schema}.shrink_summary_weekly all;
insert into {environment_schema}.shrink_summary_weekly
  select
       g.store_name
       , g.region_desc
       , case when location_num = 209 then '120, N.COM' else trim(g.channel_num || ', ' || g.channel_desc) end as channel
      -- , district
       , case when g.channel_num in (110, 120, 140, 310, 920) then 'NORDSTROM'
             when g.channel_num in (210, 220, 240, 250, 260) then 'NORDSTROM_RACK'
         end as banner
       , h.week_454_label
       , h.week_idnt
       , h.fiscal_week_num
       , h.month_454_label
       , h.month_idnt
       , h.quarter_label
       , h.half_label
       , h.fiscal_year_num
       , h.ty_ly_lly_ind
       , i.region as store_region
       , i.shrink_cup_group
       , f.*
   from (
       select
           coalesce(a.week_end_day_date, b.week_end_day_date) as week_end_day_date
           , cast(coalesce(a.location_num, b.store_num) as varchar(10)) as location_num
           , coalesce(a.channel_num, b.channel_num) as channel_num
           , coalesce(a.shrink_category, b.shrink_category) as shrink_category
           , coalesce(a.department_num, b.department_num) as department_num
           , coalesce(a.class_num, b.class_num) as class_num
           , coalesce(a.subclass_num, b.subclass_num) as subclass_num
           , coalesce(a.brand_name, b.brand_name) as brand_name
           , coalesce(a.supplier, b.supplier) as supplier
           , coalesce(shrink_c, 0) as shrink_c
           , coalesce(shrink_u, 0) as shrink_u
           , coalesce(shrink_r, 0) as shrink_r
           , coalesce(net_sales_cost, 0) as sales_c
           , coalesce(net_sales_units, 0) as sales_u
           , coalesce(net_sales_retail, 0) as sales_r
       from (
           select
               a.week_end_day_date
               , location_num
               , case when location_num = 209 then 120 else channel_num end as channel_num
               , shrink_category
               , department_num
               , class_num
               , subclass_num
               , brand_name
               , supplier
               , sum(shrink_cost) as shrink_c
               , sum(shrink_units) as shrink_u
               , sum(shrink_retail) as shrink_r
           from {environment_schema}.shrink_detail_weekly a
           inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW c
             on a.week_end_day_date = c.day_date
             and c.day_date = c.week_end_day_date
           group by 1,2,3,4,5,6,7,8,9
       ) a
       full outer join sales b
         on a.week_end_day_date = b.week_end_day_date
         and a.location_num = b.store_num
         and a.channel_num = b.channel_num
         and a.shrink_category = b.shrink_category
         and a.department_num = b.department_num
         and a.class_num = b.class_num
         and a.subclass_num = b.subclass_num
         and a.brand_name = b.brand_name
         and a.supplier = b.supplier
    ) f
    inner join prd_nap_usr_vws.store_dim g
     on f.location_num = g.store_num
    inner join (
       select
           distinct week_end_day_date
           , week_454_label
           , week_idnt
           , fiscal_week_num
           , month_454_label
           , month_idnt
           , quarter_label
           , half_label
           , fiscal_year_num
           , ty_ly_lly_ind
       from dates
    ) h
     on f.week_end_day_date = h.week_end_day_date
    left join T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.shrink_cup_stores i
     on f.location_num = i.store
;
