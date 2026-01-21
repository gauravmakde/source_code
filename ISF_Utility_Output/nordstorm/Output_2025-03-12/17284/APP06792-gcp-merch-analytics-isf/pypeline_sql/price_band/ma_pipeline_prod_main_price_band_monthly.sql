
/*
Name:                price_band_monthly
APPID-Name:          APP09094
Purpose:             Price band reporting. Create view t2dl_das_in_season_management_reporting.price_band_monthly_vw. 
Variable(s):         {{environment_schema}} t2dl_das_in_season_management_reporting 
                     {{env_suffix}} dev or prod as appropriate

DAG: 
Author(s):           Jevon Barlas
Date Created:        1/3/2024
Date Last Updated:   1/3/2024
*/


create view {environment_schema}.price_band_monthly_vw{env_suffix}
--create view t3dl_ace_pra.price_band_monthly_vw  -- this line exists only for testing purposes
as
lock row for access
   select
      d.ty_ly_ind
      ,d.fiscal_year_num
      ,d.fiscal_halfyear_num
      ,d.quarter_abrv
      ,d.quarter_label
      ,d.month_idnt
      ,d.month_abrv
      ,d.month_label
      ,d.month_start_day_date
      ,d.month_end_day_date
      ,d.banner
      ,d.channel_type
      ,d.channel_label
      ,d.channel_num
      ,d.div_label
      ,d.subdiv_label
      ,d.dept_label
      ,d.class_label
      ,d.subclass_label
      ,d.div_num
      ,d.subdiv_num
      ,d.dept_num
      ,d.class_num
      ,d.subclass_num
      ,d.quantrix_category
      ,d.quantrix_category_group
      ,d.supplier_number
      ,d.supplier_name
      ,d.brand_name
      ,d.npg_ind
      ,d.rp_ind
      ,d.ds_ind
      ,d.price_type
      ,d.price_band_one_lower_bound
      ,d.price_band_one_upper_bound
      ,d.price_band_two_lower_bound
      ,d.price_band_two_upper_bound
      ,coalesce(sum(d.sales_u),0) as sales_u
      ,coalesce(sum(d.sales_c),0) as sales_c
      ,coalesce(sum(d.sales_r),0) as sales_r
      ,coalesce(sum(d.sales_r_with_cost),0) as sales_r_with_cost
      ,coalesce(sum(d.demand_u),0) as demand_u
      ,coalesce(sum(d.demand_r),0) as demand_r
      ,coalesce(sum(case when d.week_end_day_date = d.month_end_day_date then d.eop_u end),0) as eop_u
      ,coalesce(sum(case when d.week_end_day_date = d.month_end_day_date then d.eop_c end),0) as eop_c
      ,coalesce(sum(case when d.week_end_day_date = d.month_end_day_date then d.eop_r end),0) as eop_r
      ,current_timestamp as load_timestamp 
   from {environment_schema}.price_band_weekly{env_suffix} d
   -- from t3dl_ace_pra.price_band_weekly d -- this line exists only for testing purposes
   -- restrict to completed months TY
   where d.week_idnt <= 
      (
      select max(week_idnt)
      from prd_nap_usr_vws.day_cal_454_dim
      where month_end_day_date <= current_date - 1
      )
   group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
            ,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
;