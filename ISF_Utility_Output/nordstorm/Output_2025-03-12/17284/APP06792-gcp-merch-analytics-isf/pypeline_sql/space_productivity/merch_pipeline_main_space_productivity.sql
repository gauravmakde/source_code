/* Create Mapping from Depts to Space Data */
create multiset volatile table custom_hierarchy as (
select
    distinct dept_num
    , dept_name
    , subdivision_num
    , subdivision_name
    , division_num
    , division_name
    , case when subdivision_num = 700 then 'Wmns Non-Dsnr Shoes'
        when dept_num = 835 then 'Wmns Dsnr Shoes'
        when subdivision_num = 705 or dept_num in (836, 892) then 'Mens Dsnr/Non-Dsnr Shoes'
        when subdivision_num = 710 or dept_num = 837 then 'Kids Dsnr/Non-Dsnr Shoes'
        when dept_num = 829 then 'Wmns Dsnr Apparel'
        when dept_num = 846 then 'Wmns Plus Apparel'
        when dept_num = 855 then 'Wmns Dresses'
        when dept_num = 858 then 'Wmns YA Apparel'
        when subdivision_num = 775 then 'Wmns Btr, Best, Denim, Outrwr'
        when dept_num in (861, 142, 856, 3) then 'Wmns Lingerie, Sleepwear & Legwear'
        --when dept_num = 3 then 'Wmns Legwear'
        when dept_num in (854, 857) then 'Wmns Active & Swim'
        when dept_num in (886) then 'Mens Dresswear'
        when dept_num in (863, 864, 824, 952) then 'Mens Sportswear'
        when dept_num = 866 then 'Mens YA Apparel'
        when dept_num = 885 then 'Mens Denim'
        when dept_num in (871, 868) then 'Mens Spec & Sleepwear'
        when dept_num in (867, 869, 870) then 'Mens Active, Swim & Outrwr'
        when dept_num in (827, 828) then 'Mens Dsnr App & Acess'
        when dept_num in (834) then 'Kids Dsnr Apparel'
        when dept_num in (872, 873) then 'Baby Apparel'
        when dept_num in (874, 893) then 'Baby Gear & Toys'
        when dept_num in (875, 888) then 'Girls Apparel'
        when dept_num in (889, 887) then 'Boys Apparel'
        when dept_num = 818 then 'Mens Beauty'
        when division_num = 340 then 'Wmns Beauty'
        when dept_num in (876, 294, 951) then 'Accessories & Trend'
        when dept_num = 877 then 'Eyewear'
        when dept_num in (878, 879) then 'Fashion Jewelry & Watches'
        when dept_num in (881, 905) then 'Fine Jewelry'
        when dept_num = 832 then 'Wmns Dsnr Accessories'
        when dept_num = 833 then 'Dsnr Handbags'
        when dept_num = 882 then 'Non-Dsnr Handbags'
        when division_num = 365 then 'Home'
        when dept_num = 694 then 'Mens Dsnr App & Acess'
        when dept_num in (645,705) then 'Nordstrom x Nike'
        when dept_num = 608 then 'Wmns Emerging/Space'
        when dept_num = 587 or division_num = 700 then 'Pop In Shop'
        when division_num = 800 then 'Leased Boutique'
      else 'No Match' end as subdepartment
      , '110' as chnl_idnt
from prd_nap_usr_vws.department_dim
where division_num <> 800

union all

select
    distinct dept_num
    , dept_name
    , subdivision_num
    , subdivision_name
    , division_num
    , division_name
    , case when subdivision_num = 700 then 'Wmns Shoes'
        when subdivision_num = 705 then 'Mens Shoes'
        when subdivision_num = 710 then 'Kids Shoes'
        when dept_num in (835, 836, 837) then 'Dsnr Shoes'
        when subdivision_num in (775, 785) or dept_num = 829 then 'Wmns App/Spec/Dsnr'
        when subdivision_num in (780, 790) or dept_num = 827 then 'Mens App/Spec/Dsnr'
        when subdivision_num in (770) or dept_num = 834 then 'Kids App w/Dsnr'
        when division_num in (360) then 'Accessories'
        WHEN division_num in (365) THEN 'Home'
        WHEN division_num in  (340) then 'Beauty'
        when dept_name not like 'INACT%' then 'Other'
        else 'No Match' end as subdepartment
      , '210' as chnl_idnt
from prd_nap_usr_vws.department_dim
where division_num <> 800
) with data index (chnl_idnt, subdepartment) on commit preserve rows;


/* Grab TY/LY/LLY */
create multiset volatile table dates as (
    select
        ty_ly_lly_ind
        , fiscal_month_num
        , month_idnt
        , month_label
        , month_end_day_date
        , month_start_day_date
        , fiscal_year_num
        , week_idnt
        , day_date
        , week_end_day_date
    from prd_nap_vws.realigned_date_lkup_vw
    where month_end_day_date <= current_date
    group by 1,2,3,4,5,6,7,8,9,10
) with data primary index(month_idnt) on commit preserve rows;


/* Create Monthly KPI Base */
create multiset volatile table kpis as (
    select
        store_num
        , subdepartment
        , department
        , selling_type
        , channel_num
        , month_num
        , year_num
        , sum(net_sales_cost) as net_sales_cost
        , sum(net_sales_retail) as net_sales_retail
        , avg(eoh_r) as eoh_r
        , avg(eoh_c) as eoh_c
    from (
        select
            store_num
            , c.subdepartment
            , department
            , selling_type
            , a.week_num
            , a.month_num
            , a.year_num
            , channel_num
            , sum(net_sales_cost) as net_sales_cost
            , sum(net_sales_retail) as net_sales_retail
            , sum(closing_stock_retail) as eoh_r
            , sum(closing_stock_cost) as eoh_c
            , sum(open_stock_retail) as boh_r
            , sum(open_stock_cost) as boh_c
        from PRD_NAP_VWS.MERCH_EOW_SL_OLAP_VW a
        inner join (select distinct week_idnt from dates) b
          on a.week_num = b.week_idnt
        left join custom_hierarchy c
          on a.department_num = c.dept_num
          and a.channel_num = c.chnl_idnt
        left join (select distinct chnl_idnt, department, subdepartment, selling_type from T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.space_productivity_area_mapping) d
          on c.subdepartment = d.subdepartment
          and c.chnl_idnt = d.chnl_idnt
        where a.channel_num in (110, 210)
        group by 1,2,3,4,5,6,7,8
    ) a
    group by 1,2,3,4,5,6,7
) with data primary index(store_num, month_num, subdepartment, channel_num) on commit preserve rows;


create multiset volatile table store_base as (
    select
        e.chnl_idnt
        , e.department
        , e.subdepartment
        , e.selling_type
        , e.store_num
        , e.store_name
        , e.comp_status_desc
        , e.region_detail
        , case when e.region_detail  = 'US FL - MANHATTAN' then 'US FL - NY'
              when e.region_detail in ('US FL - NCAL', 'US FL - OR', 'US FL - WA/AK') then 'US FL - NW'
              when e.region_detail = 'US FL - TX' then 'US FL - SW'
              when e.region_detail in ('US NO - SC', 'US SO - SC') then 'US FL - SCAL'
              when e.region_detail = 'US RK - SCAL' then 'US RK - SCAL'
              else e.region_detail end as region
        , e.month_idnt
        , e.fiscal_month_num
        , e.fiscal_year_num
        , sum(coalesce(e.area_sq_ft,0)) as area_sq_ft
    from (
        select
            distinct a.*
            , b.store_num
            , b.store_name
            , b.comp_status_desc
            , b.region_detail
            , d.month_idnt
            , d.fiscal_month_num
            , d.fiscal_year_num
            , coalesce(c.area_sq_ft,0) as area_sq_ft
        from T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.space_productivity_area_mapping a
        inner join (
            select
                distinct store_num
                , store_name
                , channel_num
                , 'US ' || store_type_code || ' - ' || strtok(region_medium_desc, ' ', 1) as region_detail
                , comp_status_desc
            from prd_nap_usr_vws.store_dim
            where store_name not like '%CLOSED%'
                and store_name not like 'VS%'
                and store_name not like 'FINANCIAL STORE%'
                and store_name not like '%TEST %'
                and store_name not like '%CLSD%'
                and store_name not like '%EMPLOYEE%'
                and store_name not like '%BULK%'
                and store_name not like '%UNASSIGNED%'
                and channel_num in (110, 210)
                and store_type_code in ('FL', 'RK')
             --   and comp_status_desc = 'COMP'
        ) b
          on a.chnl_idnt = b.channel_num
        left join (
            select
                distinct store_num
                , dept_key
                , area_sq_ft
                , eff_start_date
                , eff_end_date
            from T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.store_square_footage
        ) c
          on a.dept_key = c.dept_key
          and b.store_num = c.store_num
        left join dates d
          on d.day_date between coalesce(c.eff_start_date, cast('2022-01-30' as date)) and coalesce(c.eff_end_date, cast('9999-12-31' as date))
        where d.day_date = d.month_start_day_date
    ) e
    group by 1,2,3,4,5,6,7,8,9,10,11,12
) with data primary index(store_num, subdepartment) on commit preserve rows;


/* Create Different Analysis Levels */
create multiset volatile table month_base as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.subdepartment, c.month_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.subdepartment, c.month_num order by pct_store_volume desc) as rank_volume_fleet
        , 'SUBDEPARTMENT / STORE' as level
        , 'Month' as timeframe
    from (
        select
            a.*
            , b.net_sales_retail
            , b.net_sales_cost
            , b.eoh_r
            , b.eoh_c
            , sum(area_sq_ft) over(partition by a.store_num, a.month_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.store_num, a.month_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                a.chnl_idnt
                , a.department
                , a.subdepartment
                , a.selling_type
                , a.store_num
                , a.store_name
                , a.comp_status_desc
                , a.region_detail
                , a.region
                , a.month_idnt as month_num
                , a.fiscal_year_num
                , a.area_sq_ft
            from store_base a
        ) a
        left join kpis b
          on a.chnl_idnt = b.channel_num
          and a.store_num = b.store_num
          and a.subdepartment = b.subdepartment
          and a.month_num = b.month_num
    ) c
) with data on commit preserve rows
;

create multiset volatile table sub_region_month as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.subdepartment, c.month_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.subdepartment, c.month_num order by pct_store_volume desc) as rank_volume_fleet
        , 'SUBDEPARTMENT / REGION' as level
        , 'Month' as timeframe
    from (
        select
            a.*
            , sum(area_sq_ft) over(partition by a.region, a.month_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.region, a.month_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                chnl_idnt
                , department
                , subdepartment
                , selling_type
                , 0 as store_num
                , 'N/A' as store_name
                , comp_status_desc
                , 'N/A' as region_detail
                , region
                , month_num
                , fiscal_year_num
                , sum(area_sq_ft) as area_sq_ft
                , sum(net_sales_retail) as net_sales_retail
                , sum(net_sales_cost) as net_sales_cost
                , sum(eoh_r) as eoh_r
                , sum(eoh_c) as eoh_c
            from month_base
            where comp_status_desc = 'COMP'
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) a
    ) c
) with data on commit preserve rows
;

create multiset volatile table dep_region_month as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.department, c.month_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.department, c.month_num order by pct_store_volume desc) as rank_volume_fleet
        , 'DEPARTMENT / REGION' as level
        , 'Month' as timeframe
    from (
        select
            a.*
            , sum(area_sq_ft) over(partition by a.region, a.month_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.region, a.month_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.department, a.month_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.department, a.month_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.department, a.month_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.department, a.month_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                chnl_idnt
                , department
                , 'N/A' subdepartment
                , selling_type
                , 0 as store_num
                , 'N/A' as store_name
                , comp_status_desc
                , 'N/A' as region_detail
                , region
                , month_num
                , fiscal_year_num
                , sum(area_sq_ft) as area_sq_ft
                , sum(net_sales_retail) as net_sales_retail
                , sum(net_sales_cost) as net_sales_cost
                , sum(eoh_r) as eoh_r
                , sum(eoh_c) as eoh_c
            from month_base
            where comp_status_desc = 'COMP'
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) a
    ) c
) with data on commit preserve rows
;

create multiset volatile table dep_store_month as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.department, c.month_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.department, c.month_num order by pct_store_volume desc) as rank_volume_fleet
        , 'DEPARTMENT / STORE' as level
        , 'Month' as timeframe
    from (
        select
            a.*
            , sum(area_sq_ft) over(partition by a.store_num, a.month_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.store_num, a.month_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.department, a.month_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.department, a.month_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.department, a.month_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.department, a.month_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                chnl_idnt
                , department
                , 'N/A' subdepartment
                , selling_type
                , store_num
                , store_name
                , comp_status_desc
                , region_detail
                , region
                , month_num
                , fiscal_year_num
                , sum(area_sq_ft) as area_sq_ft
                , sum(net_sales_retail) as net_sales_retail
                , sum(net_sales_cost) as net_sales_cost
                , sum(eoh_r) as eoh_r
                , sum(eoh_c) as eoh_c
            from month_base
            where comp_status_desc = 'COMP'
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) a
    ) c
) with data on commit preserve rows
;

create multiset volatile table tot_store_month as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.month_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.month_num order by pct_store_volume desc) as rank_volume_fleet
        , 'TOTAL / STORE' as level
        , 'Month' as timeframe
    from (
        select
            a.*
            , sum(area_sq_ft) over(partition by a.region, a.month_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.region, a.month_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.month_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.month_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.month_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.month_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                chnl_idnt
                , 'N/A' as department
                , 'N/A' subdepartment
                , selling_type
                , store_num
                , store_name
                , comp_status_desc
                , 'N/A'  as region_detail
                , region
                , month_num
                , fiscal_year_num
                , sum(area_sq_ft) as area_sq_ft
                , sum(net_sales_retail) as net_sales_retail
                , sum(net_sales_cost) as net_sales_cost
                , sum(eoh_r) as eoh_r
                , sum(eoh_c) as eoh_c
            from month_base
            where comp_status_desc = 'COMP'
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) a
    ) c
) with data on commit preserve rows
;


/* Yearly Rankings */
create multiset volatile table year_base as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.subdepartment, c.fiscal_year_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.subdepartment, c.fiscal_year_num order by pct_store_volume desc) as rank_volume_fleet
        , 'SUBDEPARTMENT / STORE' as level
        , 'Year' as timeframe
    from (
        select
            a.*
            , b.net_sales_retail
            , b.net_sales_cost
            , b.eoh_r
            , b.eoh_c
            , sum(area_sq_ft) over(partition by a.store_num, a.fiscal_year_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.store_num, a.fiscal_year_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.fiscal_year_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.fiscal_year_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.fiscal_year_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.month_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                a.chnl_idnt
                , a.department
                , a.subdepartment
                , a.selling_type
                , a.store_num
                , a.store_name
                , a.comp_status_desc
                , a.region_detail
                , a.region
                , null as month_num
                , a.fiscal_year_num
                , a.area_sq_ft
            from store_base a
            where fiscal_month_num = 12 and fiscal_year_num in (select distinct fiscal_year_num from dates where ty_ly_lly_ind in ('LLY', 'LY'))
        ) a
        left join (
            select
                store_num
                , subdepartment
                , department
                , selling_type
                , channel_num
                , year_num
                , sum(net_sales_cost) as net_sales_cost
                , sum(net_sales_retail) as net_sales_retail
                , avg(eoh_r) as eoh_r
                , avg(eoh_c) as eoh_c
            from kpis
            group by 1,2,3,4,5,6
        ) b
          on a.chnl_idnt = b.channel_num
          and a.store_num = b.store_num
          and a.subdepartment = b.subdepartment
          and a.fiscal_year_num = b.year_num
    ) c
) with data on commit preserve rows
;

create multiset volatile table sub_region_year as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.subdepartment, c.fiscal_year_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.subdepartment, c.fiscal_year_num order by pct_store_volume desc) as rank_volume_fleet
        , 'SUBDEPARTMENT / REGION' as level
        , 'Year' as timeframe
    from (
        select
            a.*
            , sum(area_sq_ft) over(partition by a.region, a.fiscal_year_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.region, a.fiscal_year_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.fiscal_year_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.fiscal_year_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.fiscal_year_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.subdepartment, a.fiscal_year_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                chnl_idnt
                , department
                , subdepartment
                , selling_type
                , 0 as store_num
                , 'N/A' as store_name
                , comp_status_desc
                , 'N/A' as region_detail
                , region
                , month_num
                , fiscal_year_num
                , sum(area_sq_ft) as area_sq_ft
                , sum(net_sales_retail) as net_sales_retail
                , sum(net_sales_cost) as net_sales_cost
                , sum(eoh_r) as eoh_r
                , sum(eoh_c) as eoh_c
            from year_base
            where comp_status_desc = 'COMP'
                and fiscal_year_num in (select distinct fiscal_year_num from dates where ty_ly_lly_ind in ('LLY', 'LY'))
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) a
    ) c
) with data on commit preserve rows
;

create multiset volatile table dep_region_year as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.department, c.fiscal_year_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.department, c.fiscal_year_num order by pct_store_volume desc) as rank_volume_fleet
        , 'DEPARTMENT / REGION' as level
        , 'Year' as timeframe
    from (
        select
            a.*
            , sum(area_sq_ft) over(partition by a.region, a.fiscal_year_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.region, a.fiscal_year_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.department, a.fiscal_year_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.department, a.fiscal_year_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.department, a.fiscal_year_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.department, a.fiscal_year_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                chnl_idnt
                , department
                , 'N/A' subdepartment
                , selling_type
                , 0 as store_num
                , 'N/A' as store_name
                , comp_status_desc
                , 'N/A' as region_detail
                , region
                , month_num
                , fiscal_year_num
                , sum(area_sq_ft) as area_sq_ft
                , sum(net_sales_retail) as net_sales_retail
                , sum(net_sales_cost) as net_sales_cost
                , sum(eoh_r) as eoh_r
                , sum(eoh_c) as eoh_c
            from year_base
            where comp_status_desc = 'COMP'
                and fiscal_year_num in (select distinct fiscal_year_num from dates where ty_ly_lly_ind in ('LLY', 'LY'))
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) a
    ) c
) with data on commit preserve rows
;

create multiset volatile table dep_store_year as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.department, c.fiscal_year_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.department, c.fiscal_year_num order by pct_store_volume desc) as rank_volume_fleet
        , 'DEPARTMENT / STORE' as level
        , 'Year' as timeframe
    from (
        select
            a.*
            , sum(area_sq_ft) over(partition by a.store_num, a.fiscal_year_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.store_num, a.fiscal_year_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.department, a.fiscal_year_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.department, a.fiscal_year_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.department, a.fiscal_year_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.department, a.fiscal_year_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                chnl_idnt
                , department
                , 'N/A' subdepartment
                , selling_type
                , store_num
                , store_name
                , comp_status_desc
                , region_detail
                , region
                , month_num
                , fiscal_year_num
                , sum(area_sq_ft) as area_sq_ft
                , sum(net_sales_retail) as net_sales_retail
                , sum(net_sales_cost) as net_sales_cost
                , sum(eoh_r) as eoh_r
                , sum(eoh_c) as eoh_c
            from year_base
            where comp_status_desc = 'COMP'
                and fiscal_year_num in (select distinct fiscal_year_num from dates where ty_ly_lly_ind in ('LLY', 'LY'))
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) a
    ) c
) with data on commit preserve rows
;

create multiset volatile table tot_store_year as (
    select
        c.*
        , rank() over(partition by c.chnl_idnt, c.fiscal_year_num order by pct_store_sq_ft desc) as rank_pct_store_fleet
        , rank() over(partition by c.chnl_idnt, c.fiscal_year_num order by pct_store_volume desc) as rank_volume_fleet
        , 'TOTAL / STORE' as level
        , 'Year' as timeframe
    from (
        select
            a.*
            , sum(area_sq_ft) over(partition by a.region, a.fiscal_year_num) as store_sq_ft
            , case when store_sq_ft = 0 then 0 else area_sq_ft*1.000/store_sq_ft end as pct_store_sq_ft
            , case when area_sq_ft = 0 then 1 else 0 end as no_space_ind
            , sum(net_sales_retail) over(partition by a.region, a.fiscal_year_num) as store_volume
            , case when store_volume = 0 then 0 else net_sales_retail*1.000/store_volume end as pct_store_volume
            , rank() over(partition by a.chnl_idnt, a.fiscal_year_num order by area_sq_ft desc) as rank_area_fleet
            , rank() over(partition by a.chnl_idnt, a.fiscal_year_num order by net_sales_retail desc) as rank_sales_fleet
            , case when area_sq_ft = 0 then null else net_sales_retail/area_sq_ft end as sales_psf
            , rank() over(partition by a.chnl_idnt, a.fiscal_year_num order by sales_psf desc) as rank_sales_psf_fleet
            , case when area_sq_ft = 0 then null else eoh_r/area_sq_ft end as eoh_r_psf
            , case when area_sq_ft = 0 then null else eoh_c/area_sq_ft end as eoh_c_psf
            , case when net_sales_retail = 0 then 0 else (net_sales_retail - net_sales_cost)/net_sales_retail end as gm_pct
            , rank() over(partition by a.chnl_idnt, a.fiscal_year_num order by gm_pct desc) as rank_gm_fleet
        from (
            select
                chnl_idnt
                , 'N/A' as department
                , 'N/A' subdepartment
                , selling_type
                , store_num
                , store_name
                , comp_status_desc
                , 'N/A'  as region_detail
                , region
                , month_num
                , fiscal_year_num
                , sum(area_sq_ft) as area_sq_ft
                , sum(net_sales_retail) as net_sales_retail
                , sum(net_sales_cost) as net_sales_cost
                , sum(eoh_r) as eoh_r
                , sum(eoh_c) as eoh_c
            from year_base
            where comp_status_desc = 'COMP'
                and fiscal_year_num in (select distinct fiscal_year_num from dates where ty_ly_lly_ind in ('LLY', 'LY'))
            group by 1,2,3,4,5,6,7,8,9,10,11
        ) a
    ) c
) with data on commit preserve rows
;

delete from {environment_schema}.space_productivity{env_suffix} all;
insert into {environment_schema}.space_productivity{env_suffix}
    select
      a.*
      , upper(b.store_address_city) as store_city
      , current_timestamp as updated_timestamp
    from (
        select * from month_base
        union all
        select * from sub_region_month
        union all
        select * from dep_store_month
        union all
        select * from dep_region_month
        union all
        select * from tot_store_month
        union all
        select * from year_base
        union all
        select * from sub_region_year
        union all
        select * from dep_store_year
        union all
        select * from dep_region_year
        union all
        select * from tot_store_year
    ) a
    left join prd_nap_usr_vws.store_dim b
    on a.store_num = b.store_num
;
