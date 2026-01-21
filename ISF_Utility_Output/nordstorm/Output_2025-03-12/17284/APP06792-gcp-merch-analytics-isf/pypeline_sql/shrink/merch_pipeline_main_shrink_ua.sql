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
    where a.ty_ly_lly_ind in ('TY', 'LY') and a.day_date < current_date
) with data primary index(day_date, week_idnt)  on commit preserve rows
;

delete from {environment_schema}.shrink_unit_adj_daily all;
insert into {environment_schema}.shrink_unit_adj_daily
    select
        c.day_date
        , week_end_day_date
        , cast(a.location_num as int) as location_num
        , b.channel_num
        , g.pi_count
        , a.general_ledger_reference_number as gl_ref_no
        , 23 as tran_code
        , e.shrink_category
        , e.reason_description
        , a.department_num
        , a.class_num
        , a.subclass_num
        , upper(d.brand_name) as brand_name
        , upper(f.vendor_name) as supplier
        , a.rms_sku_num
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
    left join t2dl_das_in_season_management_reporting.shrink_pi_count_dates g
      on a.location_num = g.location
      and c.day_date = g.day_date
    where a.department_num not in (select distinct dept_num from prd_nap_usr_vws.product_sku_dim_vw where div_num = 600)
      and store_short_name not like '%CLSD%'
      /* Goofy NPR codes relevant only to specific locs starting May 2024 */
      and not (a.location_num = 808 and gl_ref_no = 376 and transaction_date >= '2024-05-05')
      and not (a.location_num = 828 and gl_ref_no = 248 and transaction_date >= '2024-05-05')
      and channel_num not in (930, 240)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

    union all

    select
        c.day_date
        , week_end_day_date
        , cast(a.store_num as int) as location_num
        , b.channel_num
        , g.pi_count
        , a.general_reference_number as gl_ref_no
        , 22 as tran_code
        , 'Additional Shrink' as shrink_category
        , e.reason_description
        , a.dept_num as department_num
        , a.class_num
        , a.subclass_num
        , upper(d.brand_name) as brand_name
        , upper(f.vendor_name) as supplier
        , a.rms_sku_num
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
    left join t2dl_das_in_season_management_reporting.shrink_pi_count_dates g
      on a.store_num = g.location
      and c.day_date = g.day_date
    where (channel_num in (120, 220, 250, 260, 310, 920, 930) or a.store_num in (209, 297, 697))
          and a.dept_num not in (select distinct dept_num from prd_nap_usr_vws.product_sku_dim_vw where div_num = 600)
            and store_short_name not like '%CLSD%'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;
