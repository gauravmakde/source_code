
/*
Name:                price_band_weekly
APPID-Name:          APP09094
Purpose:             Price band reporting. Insert data in t2dl_das_in_season_management_reporting.price_band_weekly.
Variable(s):         {environment_schema} t2dl_das_in_season_management_reporting
                     {env_suffix} dev or prod as appropriate

DAG:
Author(s):           Jevon Barlas & Trang Pham
Date Created:        12/05/2023
Date Last Updated:   10/24/2024
*/


/*
 * FISCAL DATES
 *
 * Full year LY and completed weeks TY
 *
 */

-- drop table date_lkup;
create volatile multiset table date_lkup as (
   select
      re.ty_ly_lly_ind as ty_ly_ind
      ,re.fiscal_year_num
      ,re.fiscal_halfyear_num
      ,re.quarter_abrv
      ,re.quarter_label
      ,re.month_idnt
      ,re.month_abrv
      ,re.month_label
      ,re.month_start_day_date
      ,re.month_end_day_date
      ,dcd.week_idnt as week_idnt_true
      ,re.week_idnt
      ,re.fiscal_week_num
      ,trim(both from re.fiscal_year_num) || ' ' || re.week_desc as week_label
      ,re.month_abrv || ' ' || 'WK' || trim(both from re.week_num_of_fiscal_month) as week_of_month_abrv
      ,left(re.week_label, 9) || 'WK' || trim(both from re.week_num_of_fiscal_month) as week_of_month_label
      ,re.week_start_day_date
      ,re.week_end_day_date
   from prd_nap_vws.realigned_date_lkup_vw re
   left join prd_nap_usr_vws.day_cal_454_dim dcd
         on dcd.day_date = re.day_date
   where 1=1
      -- limit to current and prior fiscal year
      and re.ty_ly_lly_ind in ('TY','LY')
      -- our data for this project will be weekly
      and re.day_date = re.week_end_day_date
      -- latest week in our data should be most recently completed week
      and re.week_end_day_date <= current_date - 1
      -- conditions for testing
      --and dcd.week_idnt between 202330 and 202335)
)
with data unique primary index(week_idnt) on commit preserve rows
;

collect statistics
   unique primary index (week_idnt)
   ,column(week_idnt_true)
on date_lkup
;


/*
 * LOCATIONS LOOKUP
 *
 * DCs (channel 920) are excluded because they are not tied to a banner in the inventory table we're using
 * Ideally we would included channel 920 at some point to see inventory that is in JWN network but hasn't made it to selling location yet
 *
 */

--drop table store_lkup;
create volatile multiset table store_lkup as (
   select distinct
      -- store_num is text in NAP fact table
      cast(store_num as char(4)) as store_num
      ,channel_num
      ,trim(both from channel_num) ||', '|| channel_desc as channel_label
      ,selling_channel as channel_type
      ,channel_brand as banner
   from prd_nap_usr_vws.price_store_dim_vw
   where 1=1
      and store_country_code = 'US'
      -- exclude Trunk Club, Last Chance, DCs, NQC, NPG, Faconnable
      and channel_num not in (140,240,920,930,940,990)
      -- exclude stores that closed before the time period in our data
      -- include stores that are not closed, or that closed during the time period in our data
      and (store_close_date is null or store_close_date >= (select min(week_start_day_date) from date_lkup))
      -- exclude locations with no channels (should be only CLOSED stores, but keeping the condition for future-proofing)
      and channel_num is not null
)
with data unique primary index(store_num) on commit preserve rows
;

collect statistics
   unique primary index (store_num)
on store_lkup
;


/*
 * CATEGORY LOOKUP
 * This table is only used as an input to the SKU lookup table
 *
 */

--drop table category_lkup;
create volatile multiset table category_lkup
as (
   select distinct
      h.dept_num
      ,h.class_num
      ,h.sbclass_num
      ,coalesce(cat1.category, cat2.category, 'UNKNOWN')              as quantrix_category
      ,coalesce(cat1.category_group, cat2.category_group, 'UNKNOWN')  as quantrix_category_group
   from (
      select distinct
         dept_num
         ,class_num
         ,sbclass_num
      from prd_nap_usr_vws.product_sku_dim_vw
      where channel_country = 'US'
   ) as h
   -- Table contains records at Dept + Class + Subclass and Dept + Class
   -- If category has been assigned at Dept + Class then the table will have subclass = -1
   -- We use the Dept + Class + Subclass record if it exists, and if it does not then we use the Dept + Class record
   left join prd_nap_usr_vws.catg_subclass_map_dim as cat1
       on h.dept_num = cat1.dept_num
       and h.class_num = cat1.class_num
       and h.sbclass_num = cat1.sbclass_num
   left join prd_nap_usr_vws.catg_subclass_map_dim as cat2
       on h.dept_num = cat2.dept_num
       and h.class_num = cat2.class_num
       and cat2.sbclass_num = -1
)
with data unique primary index(dept_num, class_num, sbclass_num) on commit preserve rows
;

collect statistics
  unique primary index (dept_num, class_num, sbclass_num)
on category_lkup
;


/*
 * SKU LOOKUP
 *
 * Differences relative to supplier profitability
 * - removed conditions on dept or supplier = -1 because no records in base table meet those conditions
 * - added condition to remove smart samples
 * - excluded brand_label_display_name and brand_label_num because they were often missing and/or in conflict with brand_name
 */

--drop table sku_lkup;
create volatile multiset table sku_lkup as (
   select distinct
      sku.rms_sku_num
      ,trim(both from sku.div_num) ||', '|| trim(both from sku.div_desc) as div_label
      ,trim(both from sku.grp_num) ||', '|| trim(both from sku.grp_desc) as subdiv_label
      ,trim(both from sku.dept_num) ||', '|| trim(both from sku.dept_desc) as dept_label
      ,trim(both from sku.class_num) ||', '|| trim(both from sku.class_desc) as class_label
      ,trim(both from sku.sbclass_num) ||', '|| trim(both from sku.sbclass_desc) as subclass_label
      ,sku.div_num
      ,sku.grp_num as subdiv_num
      ,sku.dept_num
      ,sku.class_num
      ,sku.sbclass_num as subclass_num
      ,cat.quantrix_category
      ,cat.quantrix_category_group
      -- need to avoid nulls because we need supplier_number to be part of an index in a later table
      ,coalesce(sku.prmy_supp_num, -1) as supplier_number
      ,supp.vendor_name as supplier_name
      -- need to avoid nulls because we need brand_name to be part of an index in a later table
      ,coalesce(sku.brand_name,'UNKNOWN') as brand_name
      ,sku.npg_ind
   from prd_nap_usr_vws.product_sku_dim_vw as sku
   left join prd_nap_usr_vws.vendor_dim as supp
      on sku.prmy_supp_num = supp.vendor_num
   left join category_lkup cat
      on cat.dept_num = sku.dept_num
      and cat.class_num = sku.class_num
      and cat.sbclass_num = sku.sbclass_num
   -- remove inactive areas
   inner join prd_nap_usr_vws.department_dim as dd
      on dd.dept_num = sku.dept_num and dd.active_store_ind = 'A'
   where 1=1
      -- fact table still includes Canada so limit to US only
      and sku.channel_country = 'US'
      -- exclude samples and gift with purchase
      and sku.smart_sample_ind <> 'Y'
      and sku.gwp_ind <> 'Y'
      -- conditions for testing
      --and sku.rms_style_num = 37791261 and color_num = 309
      --and sku.dept_num = 3
      --and supplier_number = -1
)
with data unique primary index(rms_sku_num) on commit preserve rows
;

collect statistics
  unique primary index (rms_sku_num)
on sku_lkup
;


/*
 * PRICE BAND LOOKUP
 *
 */

--drop table price_band_lkup;
create volatile multiset table price_band_lkup
as (
   select * from t2dl_das_in_season_management_reporting.price_bands_by_dollar
)
with data unique primary index (next_highest_dollar) on commit preserve rows
;

collect statistics
   unique primary index(next_highest_dollar)
on price_band_lkup
;


/*
 * SALES REGULAR PRICE
 *
 * Reg price only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures
 * NAP table has RP and DS indicator so bring it in here rather than joining to separate RP table
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 *
 */

--drop table sales_reg;
create volatile multiset table sales_reg
as (
   select
      s.rms_sku_num
      ,s.store_num
      ,dt.week_idnt
      ,s.rp_ind
      ,s.dropship_ind                           as ds_ind
      ,cast('R' as char(1))                     as price_type
      ,coalesce(net_sales_tot_regular_retl,0)   as net_sales_tot_retl
      ,coalesce(net_sales_tot_regular_cost,0)   as net_sales_tot_cost
      ,coalesce(net_sales_tot_regular_units,0)  as net_sales_tot_units
      -- for calculating product margin we need to use only sales where item cost is known
      ,coalesce(
         case
            when wac_avlbl_ind = 'Y' then net_sales_tot_regular_retl
         end
      ,0)   as net_sales_tot_retl_with_cost
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (net_sales_tot_retl*1.00)
         / (net_sales_tot_units*1.00)
      ) as sales_price_next_highest_dollar
   from prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw as s
   inner join date_lkup as dt
      on s.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on s.store_num = store.store_num
   inner join sku_lkup as sku
      on s.rms_sku_num = sku.rms_sku_num
   where 1=1
      and s.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
      and net_sales_tot_units <> 0
)
with data primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

collect stats
   primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
on sales_reg
;


/*
 * SALES CLEARANCE
 *
 * Clearance only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures
 * NAP table has RP and DS indicator so bring it in here rather than joining to separate RP table
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 *
 */

--drop table sales_clr;
create volatile multiset table sales_clr
as (
   select
      s.rms_sku_num
      ,s.store_num
      ,dt.week_idnt
      ,s.rp_ind
      ,s.dropship_ind                              as ds_ind
      ,cast('C' as char(1))                        as price_type
      ,coalesce(net_sales_tot_clearance_retl,0)    as net_sales_tot_retl
      ,coalesce(net_sales_tot_clearance_cost,0)    as net_sales_tot_cost
      ,coalesce(net_sales_tot_clearance_units,0)   as net_sales_tot_units
      -- for calculating product margin we need to use only sales where item cost is known
      ,coalesce(
         case
            when wac_avlbl_ind = 'Y' then net_sales_tot_clearance_retl
         end
      ,0)   as net_sales_tot_retl_with_cost
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (net_sales_tot_retl*1.00)
         / (net_sales_tot_units*1.00)
      ) as sales_price_next_highest_dollar
   from prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw as s
   inner join date_lkup as dt
      on s.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on s.store_num = store.store_num
   inner join sku_lkup as sku
      on s.rms_sku_num = sku.rms_sku_num
   where 1=1
      and s.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
      and net_sales_tot_units <> 0
)
with data primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

collect stats
   primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
on sales_clr
;


/*
 * SALES PROMO
 *
 * Promo only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures
 * NAP table has RP and DS indicator so bring it in here rather than joining to separate RP table
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 *
 */

--drop table sales_promo
create volatile multiset table sales_pro
as (
   select
      s.rms_sku_num
      ,s.store_num
      ,dt.week_idnt
      ,s.rp_ind
      ,s.dropship_ind                           as ds_ind
      ,cast('P' as char(1))                     as price_type
      ,coalesce(net_sales_tot_promo_retl,0)     as net_sales_tot_retl
      ,coalesce(net_sales_tot_promo_cost,0)     as net_sales_tot_cost
      ,coalesce(net_sales_tot_promo_units,0)    as net_sales_tot_units
      -- for calculating product margin we need to use only sales where item cost is known
      ,coalesce(
         case
            when wac_avlbl_ind = 'Y' then net_sales_tot_promo_retl
         end
      ,0)   as net_sales_tot_retl_with_cost
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (net_sales_tot_retl*1.00)
         / (net_sales_tot_units*1.00)
      ) as sales_price_next_highest_dollar
   from prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw as s
   inner join date_lkup as dt
      on s.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on s.store_num = store.store_num
   inner join sku_lkup as sku
      on s.rms_sku_num = sku.rms_sku_num
   where 1=1
      and s.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
      and net_sales_tot_units <> 0
)
with data primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

collect stats
    primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
on sales_pro
;


/*
 * SALES STAGING BASE
 *
 * Resulting table will be at SKU + Loc + Week grain
 * RP_ind and Price Type are also included but not needed to define a unique row
 * We'll use this as the fact table for bringing in all our other dimensions
 *
 */

--drop table sales_stg_base;
create volatile multiset table sales_stg_base
as (
   select * from sales_reg
   union all
   select * from sales_clr
   union all
   select * from sales_pro
)
with data primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

-- explain plan recommendations varied during testing, so capturing all stats that were noted as high confidence at some point
collect stats
   primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
   ,column(sales_price_next_highest_dollar)
   ,column(rms_sku_num, week_idnt)
on sales_stg_base
;


/*
 *  SALES STAGING
 *
 * Table will be at SKU + Loc + Week but will include only dimensions we want in our final table
 * Adding dimensions from lookup tables and finding the price band
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *
 */

--drop table sales_stg;
create volatile multiset table sales_stg
as (
   select
      dt.week_idnt
      ,s.banner
      ,s.channel_type
      ,s.channel_label
      ,s.channel_num
      ,sku.div_label
      ,sku.subdiv_label
      ,sku.dept_label
      ,sku.class_label
      ,sku.subclass_label
      ,sku.div_num
      ,sku.subdiv_num
      ,sku.dept_num
      ,sku.class_num
      ,sku.subclass_num
      ,sku.quantrix_category
      ,sku.quantrix_category_group
      ,sku.supplier_number
      ,sku.supplier_name
      ,sku.brand_name
      ,sku.npg_ind
      ,b.rp_ind
      ,b.ds_ind
      ,b.price_type
      -- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band
      ,coalesce(pb.price_band_one_lower_bound,(select max(price_band_one_lower_bound) from price_band_lkup)) as price_band_one_lower_bound
      ,coalesce(pb.price_band_one_upper_bound,(select max(price_band_one_upper_bound) from price_band_lkup)) as price_band_one_upper_bound
      ,coalesce(pb.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) as price_band_two_lower_bound
      ,coalesce(pb.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) as price_band_two_upper_bound
      ,net_sales_tot_retl
      ,net_sales_tot_cost
      ,net_sales_tot_units
      ,net_sales_tot_retl_with_cost
   from sales_stg_base as b
   -- staging data uses realigned week_idnt so we join on that instead of week_idnt_true
   inner join date_lkup as dt
      on b.week_idnt = dt.week_idnt
   inner join store_lkup as s
      on b.store_num = s.store_num
   inner join sku_lkup as sku
      on b.rms_sku_num = sku.rms_sku_num
   -- left join because lookup table does not contain all possible price values
   left join price_band_lkup pb
      on pb.next_highest_dollar = b.sales_price_next_highest_dollar
)
-- grain is still at SKU + Loc + Week but we aren't bringing in all those dimensions so index is not unique
-- set index on dimensions that will eventually define a unique row after grouping in a later step
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- high confidence
   ,column(
      week_idnt, banner, channel_type, channel_label, channel_num,
      div_label, subdiv_label, dept_label, class_label, subclass_label, div_num, subdiv_num, dept_num, class_num, subclass_num,
      quantrix_category, quantrix_category_group, supplier_number, supplier_name, brand_name, npg_ind, rp_ind, ds_ind,
      price_type, price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- medium confidence
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on sales_stg
;


/*
 * SALES FINAL
 *
 * Aggregate up to desired level for use in final data set
 *
 */

--drop table sales;
create volatile multiset table sales
as
(
   select
      s.week_idnt
      ,s.banner
      ,s.channel_type
      ,s.channel_label
      ,s.channel_num
      ,s.div_label
      ,s.subdiv_label
      ,s.dept_label
      ,s.class_label
      ,s.subclass_label
      ,s.div_num
      ,s.subdiv_num
      ,s.dept_num
      ,s.class_num
      ,s.subclass_num
      ,s.quantrix_category
      ,s.quantrix_category_group
      ,s.supplier_number
      ,s.supplier_name
      ,s.brand_name
      ,s.npg_ind
      ,s.rp_ind
      ,s.ds_ind
      ,s.price_type
      ,s.price_band_one_lower_bound
      ,s.price_band_one_upper_bound
      ,s.price_band_two_lower_bound
      ,s.price_band_two_upper_bound
      ,sum(net_sales_tot_units)  as sales_u
      ,sum(net_sales_tot_cost)   as sales_c
      ,sum(net_sales_tot_retl)   as sales_r
      ,sum(net_sales_tot_retl_with_cost)  as sales_r_with_cost
      ,cast(0 as decimal(18,0))  as demand_u
      ,cast(0 as decimal(18,2))  as demand_r
      ,cast(0 as decimal(18,0))  as eoh_u
      ,cast(0 as decimal(18,2))  as eoh_c
      ,cast(0 as decimal(18,2))  as eoh_r
      ,cast(0 as decimal(18,0))  as eop_u
      ,cast(0 as decimal(18,2))  as eop_c
      ,cast(0 as decimal(18,2))  as eop_r
      ,cast(0 as decimal(18,0))  as oo_4wk_u
      ,cast(0 as decimal(18,2))  as oo_4wk_c
      ,cast(0 as decimal(18,2))  as oo_4wk_r
      ,cast(0 as decimal(18,0))  as oo_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as oo_14wk_u
      ,cast(0 as decimal(18,2))  as oo_14wk_c
      ,cast(0 as decimal(18,2))  as oo_14wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_4wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_14wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_r
   from sales_stg as s
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on sales
;

/*
 * SALES CLEANUP
 *
 * Drop volatile tables that we don't need anymore
 *
 */

drop table sales_reg;
drop table sales_clr;
drop table sales_pro;
drop table sales_stg_base;
drop table sales_stg;


/*
 * DEMAND REG PRICE
 *
 * Reg price only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 *
 */

--drop table demand_reg;
create volatile multiset table demand_reg
as (
   -- dropship = Y
   select
      d.rms_sku_num
      ,d.store_num
      ,dt.week_idnt
      ,d.rp_ind
      ,cast('R' as char(1)) as price_type
      ,cast('Y' as char(1)) as ds_ind
      ,coalesce(d.jwn_demand_dropship_fulfilled_regular_retail_amt_ty,0)   as demand_tot_retl
      ,coalesce(d.jwn_demand_dropship_fulfilled_regular_units_ty,0)        as demand_tot_units
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (demand_tot_retl*1.00)
         / (demand_tot_units*1.00)
      ) as demand_price_next_highest_dollar
   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where 1=1
      and d.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
      and demand_tot_units <> 0

   union all

   -- dropship = N
   select
      d.rms_sku_num
      ,d.store_num
      ,dt.week_idnt
      ,d.rp_ind
      ,cast('R' as char(1)) as price_type
      ,cast('N' as char(1)) as ds_ind
      -- Calculate demand non-DS = total demand - demand DS
      ,coalesce(d.jwn_demand_regular_retail_amt_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_regular_retail_amt_ty,0)  as demand_tot_retl
      ,coalesce(d.jwn_demand_regular_units_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_regular_units_ty,0)            as demand_tot_units
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (demand_tot_retl*1.00)
         / (demand_tot_units*1.00)
      ) as demand_price_next_highest_dollar
   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where 1=1
      and d.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
      and demand_tot_units <> 0
)
with data primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

collect stats
    primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
on demand_reg
;


/*
 * DEMAND CLEARANCE
 *
 * Clearance only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 *
 */

--drop table demand_clr;
create volatile multiset table demand_clr
as (
   --dropship = Y
   select
      d.rms_sku_num
      ,d.store_num
      ,dt.week_idnt
      ,d.rp_ind
      ,cast('C' as char(1)) as price_type
      ,cast('Y' as char(1)) as ds_ind
      ,coalesce(d.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty,0) as demand_tot_retl
      ,coalesce(d.jwn_demand_dropship_fulfilled_clearance_units_ty,0)      as demand_tot_units
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (demand_tot_retl*1.00)
         / (demand_tot_units*1.00)
      ) as demand_price_next_highest_dollar
   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where 1=1
      and d.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
     and demand_tot_units <> 0

   union all

   -- dropship = N
   select
      d.rms_sku_num
      ,d.store_num
      ,dt.week_idnt
      ,d.rp_ind
      ,cast('C' as char(1)) as price_type
      ,cast('N' as char(1)) as ds_ind
      -- Calculate demand non-DS = total demand - demand DS
      ,coalesce(d.jwn_demand_clearance_retail_amt_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty,0) as demand_tot_retl
      ,coalesce(d.jwn_demand_clearance_units_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_clearance_units_ty,0)           as demand_tot_units
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (demand_tot_retl*1.00)
         / (demand_tot_units*1.00)
      ) as demand_price_next_highest_dollar
   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where 1=1
      and d.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
      and demand_tot_units <> 0
)
with data primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

collect stats
    primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
on demand_clr
;


/*
 * DEMAND PROMO
 *
 * Promo only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 *
 */

--drop table demand_pro;
create volatile multiset table demand_pro
as (
   select
   -- dropship = Y
      d.rms_sku_num
      ,d.store_num
      ,dt.week_idnt
      ,d.rp_ind
      ,cast('P' as char(1))   as price_type
      ,cast('Y' as char(1))   as ds_ind
      ,coalesce(d.jwn_demand_dropship_fulfilled_promo_retail_amt_ty,0)  as demand_tot_retl
      ,coalesce(d.jwn_demand_dropship_fulfilled_promo_units_ty,0)       as demand_tot_units
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (demand_tot_retl*1.00)
         / (demand_tot_units*1.00)
      ) as demand_price_next_highest_dollar
   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where 1=1
      and d.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
      and demand_tot_units <> 0

   union all

   -- dropship = N
   select
      d.rms_sku_num
      ,d.store_num
      ,dt.week_idnt
      ,d.rp_ind
      ,cast('P' as char(1)) as price_type
      ,cast('N' as char(1)) as ds_ind
      -- Calculate demand non-DS = total demand - demand DS
      ,coalesce(jwn_demand_promo_retail_amt_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_promo_retail_amt_ty,0)  as demand_tot_retl
      ,coalesce(d.jwn_demand_promo_units_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_promo_units_ty,0)          as demand_tot_units
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         (demand_tot_retl*1.00)
         / (demand_tot_units*1.00)
      ) as demand_price_next_highest_dollar
   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where 1=1
      and d.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
      and demand_tot_units <> 0
)
with data  primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

collect stats
   primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
on demand_pro
;


/*
 * DEMAND STAGING BASE
 *
 * Resulting table will be at SKU + Loc + Week grain
 * RP_ind and Price Type are also included but not needed to define a unique row
 * We'll use this as the fact table for bringing in all our other dimensions
 *
 */

--drop table demand_stg_base;
create volatile multiset table demand_stg_base
as (
   select * from demand_reg
   union all
   select * from demand_clr
   union all
   select * from demand_pro
)
with data primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

-- explain plan recommendations varied during testing, so capturing all stats that were noted as high confidence at some point
collect stats
   primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
   ,column(demand_price_next_highest_dollar)
   ,column(rms_sku_num, week_idnt)
on demand_stg_base
;


/*
 * DEMAND STAGING
 *
 * Table will be at SKU + Loc + Week but will include only dimensions we want in our final table
 * Adding dimensions from lookup tables and finding the price band
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *
 */

--drop table demand_stg;
create volatile multiset table demand_stg
as (
   select
      dt.week_idnt
      ,s.banner
      ,s.channel_type
      ,s.channel_label
      ,s.channel_num
      ,sku.div_label
      ,sku.subdiv_label
      ,sku.dept_label
      ,sku.class_label
      ,sku.subclass_label
      ,sku.div_num
      ,sku.subdiv_num
      ,sku.dept_num
      ,sku.class_num
      ,sku.subclass_num
      ,sku.quantrix_category
      ,sku.quantrix_category_group
      ,sku.supplier_number
      ,sku.supplier_name
      ,sku.brand_name
      ,sku.npg_ind
      ,b.rp_ind
      ,b.ds_ind
      ,b.price_type
      -- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band
      ,coalesce(pb.price_band_one_lower_bound,(select max(price_band_one_lower_bound) from price_band_lkup)) as price_band_one_lower_bound
      ,coalesce(pb.price_band_one_upper_bound,(select max(price_band_one_upper_bound) from price_band_lkup)) as price_band_one_upper_bound
      ,coalesce(pb.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) as price_band_two_lower_bound
      ,coalesce(pb.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) as price_band_two_upper_bound
      ,b.demand_tot_retl
      ,b.demand_tot_units
   from demand_stg_base as b
   -- staging data uses realigned week_idnt so we join on that instead of week_idnt_true
   inner join date_lkup as dt
      on b.week_idnt = dt.week_idnt
   inner join store_lkup as s
      on b.store_num = s.store_num
   inner join sku_lkup as sku
      on b.rms_sku_num = sku.rms_sku_num
   -- left join because lookup table does not contain all possible price values
   left join price_band_lkup pb
      on pb.next_highest_dollar = b.demand_price_next_highest_dollar
   where 1=1
)
-- grain is still at SKU + Loc + Week but we aren't bringing in all those dimensions so index is not unique
-- set index on dimensions that will eventually define a unique row after grouping in a later step
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- high confidence
   ,column(
      week_idnt, banner, channel_type, channel_label, channel_num,
      div_label, subdiv_label, dept_label, class_label, subclass_label, div_num, subdiv_num, dept_num, class_num, subclass_num,
      quantrix_category, quantrix_category_group, supplier_number, supplier_name, brand_name, npg_ind, rp_ind, ds_ind,
      price_type, price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- medium confidence
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on demand_stg
;


/*
 * DEMAND FINAL
 *
 * Aggregate up to desired level for use in final data set
 *
 */

--drop table demand;
create volatile multiset table demand
as (
   select
       d.week_idnt
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
      ,cast(0 as decimal(18,0))  as sales_u
      ,cast(0 as decimal(18,2))  as sales_c
      ,cast(0 as decimal(18,2))  as sales_r
      ,cast(0 as decimal(18,2))  as sales_r_with_cost
      ,sum(demand_tot_units)     as demand_u
      ,sum(demand_tot_retl)      as demand_r
      ,cast(0 as decimal(18,0))  as eoh_u
      ,cast(0 as decimal(18,2))  as eoh_c
      ,cast(0 as decimal(18,2))  as eoh_r
      ,cast(0 as decimal(18,0))  as eop_u
      ,cast(0 as decimal(18,2))  as eop_c
      ,cast(0 as decimal(18,2))  as eop_r
      ,cast(0 as decimal(18,0))  as oo_4wk_u
      ,cast(0 as decimal(18,2))  as oo_4wk_c
      ,cast(0 as decimal(18,2))  as oo_4wk_r
      ,cast(0 as decimal(18,0))  as oo_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as oo_14wk_u
      ,cast(0 as decimal(18,2))  as oo_14wk_c
      ,cast(0 as decimal(18,2))  as oo_14wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_4wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_14wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_r
   from demand_stg as d
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on demand
;


/*
 * DEMAND CLEANUP
 *
 * Drop volatile tables that we don't need anymore
 *
 */

drop table demand_reg;
drop table demand_clr;
drop table demand_pro;
drop table demand_stg_base;
drop table demand_stg;


/*
 * At initial launch we created Inventory Reg and Inventory Clr, then unioned them, then joined to price band table, then aggregated.
 * As fiscal year end approached we hit CPU timeout errors at the step of unioning Reg & Clr.
 * To get around that, we changed the sequence to join to price band table and aggregate each of Reg and Clr first, then union the aggregated Reg & Clr as the final step.
 */

/*
 * INVENTORY REG BASE
 *
 * Reg price only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures
 * Select relevant inventory fields and calculate price, keeping data at same grain as NAP fact table
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 *
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 *
 */

--drop table inventory_reg_base;
create volatile multiset table inventory_reg_base
as (
   select
      i.rms_sku_num
      ,i.store_num
      ,dt.week_idnt
      ,i.rp_ind
      ,cast('R' as char(1)) as price_type
      ,coalesce(i.eoh_regular_units,0)    as eoh_u
      ,coalesce(i.eoh_regular_cost,0)     as eoh_c
      ,coalesce(i.eoh_regular_retail,0)   as eoh_r
      ,coalesce(i.eoh_regular_units,0) + coalesce(i.eoh_in_transit_regular_units,0)         as eop_u
      ,coalesce(i.eoh_regular_cost,0) + coalesce(i.eoh_in_transit_regular_cost,0)           as eop_c
      ,coalesce(i.eoh_regular_retail,0) + coalesce(i.eoh_in_transit_regular_retail,0)       as eop_r
      -- in some situations eop = 0 but eoh <> 0, so we need a price for those situations
      -- price will be positive even if inventory is negative (both R and U will be negative)
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         case
            when eop_u = 0 then eoh_r*1.00 / eoh_u*1.00
            else eop_r*1.00 / eop_u*1.00
         end
      ) as inv_price_next_highest_dollar
   from prd_nap_usr_vws.merch_inventory_sku_store_week_fact_vw as i
   inner join date_lkup as dt
      on i.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on i.store_num = store.store_num
   inner join sku_lkup as sku
      on i.rms_sku_num = sku.rms_sku_num
   where 1=1
      -- only keep records that have inventory
      and (eoh_u <> 0 or eop_u <> 0)
      -- adding condition on partitioned field for better performance (logically redundant due to inner join)
      and i.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
)
with data unique primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

collect stats
   -- high confidence
   unique primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
   ,column(inv_price_next_highest_dollar)
   ,column(store_num, week_idnt)
   -- medium confidence
   ,column(rms_sku_num, store_num)
on inventory_reg_base
;

/*
 * INVENTORY REG STAGING
 *
 * Table will be at SKU + Loc + Week but will include only dimensions we want in our final table
 * Adding dimensions from lookup tables and finding the price band
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *
 */

--drop table inventory_reg_stg;
create volatile multiset table inventory_reg_stg
as (
   select
      dt.week_idnt
      ,s.banner
      ,s.channel_type
      ,s.channel_label
      ,s.channel_num
      ,sku.div_label
      ,sku.subdiv_label
      ,sku.dept_label
      ,sku.class_label
      ,sku.subclass_label
      ,sku.div_num
      ,sku.subdiv_num
      ,sku.dept_num
      ,sku.class_num
      ,sku.subclass_num
      ,sku.quantrix_category
      ,sku.quantrix_category_group
      ,sku.supplier_number
      ,sku.supplier_name
      ,sku.brand_name
      ,sku.npg_ind
      ,i.rp_ind
      ,i.price_type
      -- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band
      ,coalesce(pb.price_band_one_lower_bound,(select max(price_band_one_lower_bound) from price_band_lkup)) as price_band_one_lower_bound
      ,coalesce(pb.price_band_one_upper_bound,(select max(price_band_one_upper_bound) from price_band_lkup)) as price_band_one_upper_bound
      ,coalesce(pb.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) as price_band_two_lower_bound
      ,coalesce(pb.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) as price_band_two_upper_bound
      ,i.eoh_u
      ,i.eoh_c
      ,i.eoh_r
      ,i.eop_u
      ,i.eop_c
      ,i.eop_r
   from inventory_reg_base as i
   -- staging data uses realigned week_idnt so we join on that instead of week_idnt_true
   inner join date_lkup as dt
      on i.week_idnt = dt.week_idnt
   inner join store_lkup as s
      on i.store_num = s.store_num
   inner join sku_lkup as sku
      on i.rms_sku_num = sku.rms_sku_num
   -- left join because lookup table does not contain all possible price values
   left join price_band_lkup pb
      on pb.next_highest_dollar = i.inv_price_next_highest_dollar
)
-- grain is still at SKU + Loc + Week but we aren't bringing in all those dimensions so index is not unique
-- set index on dimensions that will eventually define a unique row after grouping in a later step
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  )
   -- high confidence
   ,column(
      week_idnt, banner, channel_type, channel_label, channel_num,
      div_label, subdiv_label, dept_label, class_label, subclass_label, div_num, subdiv_num, dept_num, class_num, subclass_num,
      quantrix_category, quantrix_category_group, supplier_number, supplier_name, brand_name, npg_ind, rp_ind,
      price_type, price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- medium confidence
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on inventory_reg_stg
;


/*
 * INVENTORY REG FINAL
 *
 * Adding ds_ind dimension
 * Aggregate up to desired level for use in final data set
 *
 */

--drop table inventory_reg_final;
create volatile multiset table inventory_reg_final
as (
   select
      i.week_idnt
      ,i.banner
      ,i.channel_type
      ,i.channel_label
      ,i.channel_num
      ,i.div_label
      ,i.subdiv_label
      ,i.dept_label
      ,i.class_label
      ,i.subclass_label
      ,i.div_num
      ,i.subdiv_num
      ,i.dept_num
      ,i.class_num
      ,i.subclass_num
      ,i.quantrix_category
      ,i.quantrix_category_group
      ,i.supplier_number
      ,i.supplier_name
      ,i.brand_name
      ,i.npg_ind
      ,i.rp_ind
      ,cast('N' as char(1)) as ds_ind
      ,i.price_type
      ,i.price_band_one_lower_bound
      ,i.price_band_one_upper_bound
      ,i.price_band_two_lower_bound
      ,i.price_band_two_upper_bound
      ,cast(0 as decimal(18,0))  as sales_u
      ,cast(0 as decimal(18,2))  as sales_c
      ,cast(0 as decimal(18,2))  as sales_r
      ,cast(0 as decimal(18,2))  as sales_r_with_cost
      ,cast(0 as decimal(18,0))  as demand_u
      ,cast(0 as decimal(18,2))  as demand_r
      ,sum(eoh_u)                as eoh_u
      ,sum(eoh_c)                as eoh_c
      ,sum(eoh_r)                as eoh_r
      ,sum(eop_u)                as eop_u
      ,sum(eop_c)                as eop_c
      ,sum(eop_r)                as eop_r
      ,cast(0 as decimal(18,0))  as oo_4wk_u
      ,cast(0 as decimal(18,2))  as oo_4wk_c
      ,cast(0 as decimal(18,2))  as oo_4wk_r
      ,cast(0 as decimal(18,0))  as oo_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as oo_14wk_u
      ,cast(0 as decimal(18,2))  as oo_14wk_c
      ,cast(0 as decimal(18,2))  as oo_14wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_4wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_14wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_r
      -- placeholder for other measures (sales, demand, on order, etc)
   from inventory_reg_stg as i
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on inventory_reg_final
;


/*
 * INVENTORY REG CLEANUP
 *
 * Drop volatile tables that we don't need anymore
 *
 */

drop table inventory_reg_base;
drop table inventory_reg_stg;


/*
 * INVENTORY CLR BASE
 *
 * Clearance only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures
 * Select relevant inventory fields and calculate price, keeping data at same grain as NAP fact table
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)
 *
 * Multiplication by 1.00 should be unnecessary but keeping as a precaution to ensure decimal places
 *
 */

--drop table inventory_clr_base;
create volatile multiset table inventory_clr_base
as (
   select
      i.rms_sku_num
      ,i.store_num
      ,dt.week_idnt
      ,i.rp_ind
      ,cast('C' as char(1)) as price_type
      ,coalesce(i.eoh_clearance_units,0)    as eoh_u
      ,coalesce(i.eoh_clearance_cost,0)     as eoh_c
      ,coalesce(i.eoh_clearance_retail,0)   as eoh_r
      ,coalesce(i.eoh_clearance_units,0) + coalesce(i.eoh_in_transit_clearance_units,0)         as eop_u
      ,coalesce(i.eoh_clearance_cost,0) + coalesce(i.eoh_in_transit_clearance_cost,0)           as eop_c
      ,coalesce(i.eoh_clearance_retail,0) + coalesce(i.eoh_in_transit_clearance_retail,0)       as eop_r
      -- in some situations eop = 0 but eoh <> 0, so we need a price for those situations
      -- price will be positive even if inventory is negative (both R and U will be negative)
      -- calculate price to two decimal places, then round up to next highest dollar for later join to price band table
      ,ceiling(
         case
            when eop_u = 0 then eoh_r*1.00 / eoh_u*1.00
            else eop_r*1.00 / eop_u*1.00
         end
      ) as inv_price_next_highest_dollar
   from prd_nap_usr_vws.merch_inventory_sku_store_week_fact_vw as i
   inner join date_lkup as dt
      on i.week_num = dt.week_idnt_true
   inner join store_lkup as s
      on i.store_num = s.store_num
   inner join sku_lkup as sku
      on i.rms_sku_num = sku.rms_sku_num
   where 1=1
      -- only keep records that have inventory
      and (eoh_u <> 0 or eop_u <> 0)
      -- adding condition on partitioned field for better performance (logically redundant due to inner join)
      and i.week_num between (select min(week_idnt) from date_lkup) and (select max(week_idnt) from date_lkup)
)
with data unique primary index (rms_sku_num, store_num, week_idnt) on commit preserve rows
;

collect stats
   -- high confidence
   unique primary index(rms_sku_num, store_num, week_idnt)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_idnt)
   ,column(inv_price_next_highest_dollar)
   ,column(store_num, week_idnt)
   -- medium confidence
   ,column(rms_sku_num, store_num)
on inventory_clr_base
;


/*
 * INVENTORY CLR STAGING
 *
 * Table will be at SKU + Loc + Week but will include only dimensions we want in our final table
 * Adding dimensions from lookup tables and finding the price band
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *
 */

--drop table inventory_clr_stg;
create volatile multiset table inventory_clr_stg
as (
   select
      dt.week_idnt
      ,s.banner
      ,s.channel_type
      ,s.channel_label
      ,s.channel_num
      ,sku.div_label
      ,sku.subdiv_label
      ,sku.dept_label
      ,sku.class_label
      ,sku.subclass_label
      ,sku.div_num
      ,sku.subdiv_num
      ,sku.dept_num
      ,sku.class_num
      ,sku.subclass_num
      ,sku.quantrix_category
      ,sku.quantrix_category_group
      ,sku.supplier_number
      ,sku.supplier_name
      ,sku.brand_name
      ,sku.npg_ind
      ,i.rp_ind
      ,i.price_type
      -- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band
      ,coalesce(pb.price_band_one_lower_bound,(select max(price_band_one_lower_bound) from price_band_lkup)) as price_band_one_lower_bound
      ,coalesce(pb.price_band_one_upper_bound,(select max(price_band_one_upper_bound) from price_band_lkup)) as price_band_one_upper_bound
      ,coalesce(pb.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) as price_band_two_lower_bound
      ,coalesce(pb.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) as price_band_two_upper_bound
      ,i.eoh_u
      ,i.eoh_c
      ,i.eoh_r
      ,i.eop_u
      ,i.eop_c
      ,i.eop_r
   from inventory_clr_base as i
   -- staging data uses realigned week_idnt so we join on that instead of week_idnt_true
   inner join date_lkup as dt
      on i.week_idnt = dt.week_idnt
   inner join store_lkup as s
      on i.store_num = s.store_num
   inner join sku_lkup as sku
      on i.rms_sku_num = sku.rms_sku_num
   -- left join because lookup table does not contain all possible price values
   left join price_band_lkup pb
      on pb.next_highest_dollar = i.inv_price_next_highest_dollar
)
-- grain is still at SKU + Loc + Week but we aren't bringing in all those dimensions so index is not unique
-- set index on dimensions that will eventually define a unique row after grouping in a later step
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  )
   -- high confidence
   ,column(
      week_idnt, banner, channel_type, channel_label, channel_num,
      div_label, subdiv_label, dept_label, class_label, subclass_label, div_num, subdiv_num, dept_num, class_num, subclass_num,
      quantrix_category, quantrix_category_group, supplier_number, supplier_name, brand_name, npg_ind, rp_ind,
      price_type, price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- medium confidence
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on inventory_clr_stg
;


/*
 * INVENTORY CLR FINAL
 *
 * Adding ds_ind dimension
 * Aggregate up to desired level for use in final data set
 *
 */

--drop table inventory_clr_final;
create volatile multiset table inventory_clr_final
as (
   select
      i.week_idnt
      ,i.banner
      ,i.channel_type
      ,i.channel_label
      ,i.channel_num
      ,i.div_label
      ,i.subdiv_label
      ,i.dept_label
      ,i.class_label
      ,i.subclass_label
      ,i.div_num
      ,i.subdiv_num
      ,i.dept_num
      ,i.class_num
      ,i.subclass_num
      ,i.quantrix_category
      ,i.quantrix_category_group
      ,i.supplier_number
      ,i.supplier_name
      ,i.brand_name
      ,i.npg_ind
      ,i.rp_ind
      ,cast('N' as char(1)) as ds_ind
      ,i.price_type
      ,i.price_band_one_lower_bound
      ,i.price_band_one_upper_bound
      ,i.price_band_two_lower_bound
      ,i.price_band_two_upper_bound
      ,cast(0 as decimal(18,0))  as sales_u
      ,cast(0 as decimal(18,2))  as sales_c
      ,cast(0 as decimal(18,2))  as sales_r
      ,cast(0 as decimal(18,2))  as sales_r_with_cost
      ,cast(0 as decimal(18,0))  as demand_u
      ,cast(0 as decimal(18,2))  as demand_r
      ,sum(eoh_u)                as eoh_u
      ,sum(eoh_c)                as eoh_c
      ,sum(eoh_r)                as eoh_r
      ,sum(eop_u)                as eop_u
      ,sum(eop_c)                as eop_c
      ,sum(eop_r)                as eop_r
      ,cast(0 as decimal(18,0))  as oo_4wk_u
      ,cast(0 as decimal(18,2))  as oo_4wk_c
      ,cast(0 as decimal(18,2))  as oo_4wk_r
      ,cast(0 as decimal(18,0))  as oo_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as oo_14wk_u
      ,cast(0 as decimal(18,2))  as oo_14wk_c
      ,cast(0 as decimal(18,2))  as oo_14wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_4wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_14wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_r
      -- placeholder for other measures (sales, demand, on order, etc)
   from inventory_clr_stg as i
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on inventory_clr_final
;


/*
 * INVENTORY CLR CLEANUP
 *
 * Drop volatile tables that we don't need anymore
 *
 */

drop table inventory_clr_base;
drop table inventory_clr_stg;


/*
 * INVENTORY FINAL
 *
 */

--drop table inventory;
create volatile multiset table inventory
as (
   select * from inventory_reg_final
   union all
   select * from inventory_clr_final
)
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on inventory
;


/*
 * INVENTORY FINAL CLEANUP
 *
 * Drop volatile tables that we don't need anymore
 *
 */

drop table inventory_reg_final;
drop table inventory_clr_final;


/*
 * ON ORDER STAGING
 *
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 * Table will be at SKU + Loc but will include only dimensions we want in our final table
 *
 * No join to date_lkup because we want to place all future On Order into the most recent completed week
 *
 * oo_4wk column will have the data of <=4 weeks, oo_5wk_13wk column will have data for 5 to 13 weeks and oo_14wk column will have data
 * for >13 weeks
 *
 * Research in FY23 Week 37 showed that 0.04% of units (after joining to our lkup tables) had no value for anticipated_retail_amt.
 * Most of those units were beauty samples, beauty gift-with-purchase, or sunglasses cases - all items that will not be sold separately.
 * We decided that the quantity of missing-price data was negligible so did not create a price band for "Unknown".
 *
 */

-- drop table on_order_stg;
create volatile multiset table on_order_stg
as (
   select
      -- we capture all future on order and assign it to most recent completed week
      (select max(week_idnt)
       from prd_nap_usr_vws.day_cal_454_dim
       where week_end_day_date < CURRENT_DATE
      ) as week_idnt
      ,s.banner
      ,s.channel_type
      ,s.channel_label
      ,s.channel_num
      ,sku.div_label
      ,sku.subdiv_label
      ,sku.dept_label
      ,sku.class_label
      ,sku.subclass_label
      ,sku.div_num
      ,sku.subdiv_num
      ,sku.dept_num
      ,sku.class_num
      ,sku.subclass_num
      ,sku.quantrix_category
      ,sku.quantrix_category_group
      ,sku.supplier_number
      ,sku.supplier_name
      ,sku.brand_name
      ,sku.npg_ind
      -- same rp_ind definition as WBR
      ,cast(
         case when oo.order_type = 'AUTOMATIC_REORDER' then 'Y' else 'N' end
         as char(1)
      )                                as rp_ind
      ,oo.anticipated_price_type       AS price_type
      -- if price is in the highest price band it won't match any value in the lookup table, so instead use the bounds for the highest price band
      ,coalesce(pb.price_band_one_lower_bound,(select max(price_band_one_lower_bound) from price_band_lkup)) as price_band_one_lower_bound
      ,coalesce(pb.price_band_one_upper_bound,(select max(price_band_one_upper_bound) from price_band_lkup)) as price_band_one_upper_bound
      ,coalesce(pb.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) as price_band_two_lower_bound
      ,coalesce(pb.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) as price_band_two_upper_bound
      -- ,oo.quantity_open                                                             as on_order_units
      -- ,cast((oo.quantity_open * oo.anticipated_retail_amt) AS decimal(18,2))        as on_order_retail
      -- ,cast((oo.quantity_open * oo.unit_estimated_landing_cost) AS decimal(18,2))   as on_order_cost
      ,CASE WHEN week_end_day_date < CURRENT_DATE+35 then oo.quantity_open end as on_order_4wk_units
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+35 and week_end_day_date < CURRENT_DATE+98 then oo.quantity_open end as on_order_5wk_13wk_units
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+98 then oo.quantity_open end as on_order_14wk_units
      ,CASE WHEN week_end_day_date < CURRENT_DATE+35 then cast((oo.quantity_open * oo.unit_estimated_landing_cost) AS decimal(18,2)) end as on_order_4wk_cost
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+35 and week_end_day_date < CURRENT_DATE+98 then cast((oo.quantity_open * oo.unit_estimated_landing_cost) AS decimal(18,2)) end as on_order_5wk_13wk_cost
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+98 then cast((oo.quantity_open * oo.unit_estimated_landing_cost) AS decimal(18,2)) end as on_order_14wk_cost
      ,CASE WHEN week_end_day_date < CURRENT_DATE+35 then cast((oo.quantity_open * oo.anticipated_retail_amt) AS decimal(18,2)) end as on_order_4wk_retail
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+35 and week_end_day_date < CURRENT_DATE+98 then cast((oo.quantity_open * oo.anticipated_retail_amt) AS decimal(18,2)) end as on_order_5wk_13wk_retail
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+98 then cast((oo.quantity_open * oo.anticipated_retail_amt) AS decimal(18,2)) end as on_order_14wk_retail
   from prd_nap_usr_vws.merch_on_order_fact_vw oo
   left join (SELECT DISTINCT week_idnt, week_start_day_date, week_end_day_date FROM prd_nap_usr_vws.DAY_CAL_454_DIM) DC
      on OO.WEEK_NUM = DC.WEEK_IDNT
   inner join store_lkup as s
      on oo.store_num = s.store_num
   inner join sku_lkup as sku
      on oo.rms_sku_num = sku.rms_sku_num
   -- left join because lookup table does not contain all possible price values
   left join price_band_lkup pb
      on pb.next_highest_dollar = ceiling(coalesce(oo.anticipated_retail_amt,0))
   where 1=1
      -- only get records that have a price so we can assign them to a price band
      and coalesce(oo.anticipated_retail_amt,0) <> 0
      -- conditions below this point are same as in WBR as of Oct 6, 2023
      -- only want items that haven't been received yet
      and oo.quantity_open > 0
      -- only include approved POs
      and oo.first_approval_date is not null
      and (
         -- include closed POs with ship dates in the recent past
         (oo.status = 'CLOSED' and oo.end_ship_date >= (SELECT MAX(week_end_day_date) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < CURRENT_DATE) - 45)
         -- include POs that aren't closed yet
         OR oo.status IN ('APPROVED','WORKSHEET')
      )
)
-- grain is still at SKU + Loc but we aren't bringing in those dimensions so index is not unique
-- set index on dimensions that will eventually define a unique row after grouping in a later step
with data primary index (
                     channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
  )
   -- high confidence
   ,column(
      week_idnt, banner, channel_type, channel_label, channel_num,
      div_label, subdiv_label, dept_label, class_label, subclass_label, div_num, subdiv_num, dept_num, class_num, subclass_num,
      quantrix_category, quantrix_category_group, supplier_number, supplier_name, brand_name, npg_ind, rp_ind,
      price_type, price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- medium confidence
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on on_order_stg;


/*
 * ON ORDER FINAL
 *
 */

--drop table on_order;
create volatile multiset table on_order
as (
   select
      oo.week_idnt
      ,oo.banner
      ,oo.channel_type
      ,oo.channel_label
      ,oo.channel_num
      ,oo.div_label
      ,oo.subdiv_label
      ,oo.dept_label
      ,oo.class_label
      ,oo.subclass_label
      ,oo.div_num
      ,oo.subdiv_num
      ,oo.dept_num
      ,oo.class_num
      ,oo.subclass_num
      ,oo.quantrix_category
      ,oo.quantrix_category_group
      ,oo.supplier_number
      ,oo.supplier_name
      ,oo.brand_name
      ,oo.npg_ind
      ,oo.rp_ind
      ,cast('N' as char(1)) as ds_ind
      ,oo.price_type
      ,oo.price_band_one_lower_bound
      ,oo.price_band_one_upper_bound
      ,oo.price_band_two_lower_bound
      ,oo.price_band_two_upper_bound
      ,cast(0 as decimal(18,0))  as sales_u
      ,cast(0 as decimal(18,2))  as sales_c
      ,cast(0 as decimal(18,2))  as sales_r
      ,cast(0 as decimal(18,2))  as sales_r_with_cost
      ,cast(0 as decimal(18,0))  as demand_u
      ,cast(0 as decimal(18,2))  as demand_r
      ,cast(0 as decimal(18,0))  as eoh_u
      ,cast(0 as decimal(18,2))  as eoh_c
      ,cast(0 as decimal(18,2))  as eoh_r
      ,cast(0 as decimal(18,0))  as eop_u
      ,cast(0 as decimal(18,2))  as eop_c
      ,cast(0 as decimal(18,2))  as eop_r
      ,sum(on_order_4wk_units)   as oo_4wk_u
      ,sum(on_order_4wk_cost)    as oo_4wk_c
      ,sum(on_order_4wk_retail)  as oo_4wk_r
      ,sum(on_order_5wk_13wk_units)   as oo_5wk_13wk_u
      ,sum(on_order_5wk_13wk_cost)    as oo_5wk_13wk_c
      ,sum(on_order_5wk_13wk_retail)  as oo_5wk_13wk_r
      ,sum(on_order_14wk_units)  as oo_14wk_u
      ,sum(on_order_14wk_cost)   as oo_14wk_c
      ,sum(on_order_14wk_retail) as oo_14wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_4wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_4wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as rp_ant_spd_14wk_u
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_c
      ,cast(0 as decimal(18,2))  as rp_ant_spd_14wk_r
      -- placeholder for other measures (sales, demand, inventory, etc)
   from on_order_stg oo
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)
with data primary index (
                     channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                     channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   ,column(week_idnt)
   ,column(channel_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(supplier_number)
   ,column(brand_name)
   ,column(rp_ind)
   ,column(price_type)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on on_order
;


/*
 * ON ORDER CLEANUP
 *
 * Drop volatile tables that we don't need anymore
 *
 */

drop table on_order_stg;

/*
 * CATEGORY LOOKUP 2
 * This table is only used to join with rp ant spd table
 *
 */

--drop table category_lkup_2
create volatile multiset table category_lkup_2
as (
   select distinct
      div_label
      ,subdiv_label
      ,dept_label
      ,class_label
      ,subclass_label
      ,div_num
      ,subdiv_num
      ,dept_num
      ,class_num
      ,subclass_num
      ,quantrix_category
      ,quantrix_category_group
   from sku_lkup
)
with data primary index(dept_num, class_num, subclass_num) on commit preserve rows
;

collect statistics
  unique primary index (dept_num, class_num, subclass_num)
on category_lkup_2
;

/*
 * RP ANT SPD STAGING
 *
 * Similar table to ON ORDER table above
 * No grouping yet; purpose of this step is to avoid CPU timeout in a later query
 *
 * No join to date_lkup because we want to place all future Ant Spd into the most recent completed week
 *
 * Will add this data to ON ORDER columns in the final table
 */

--drop table rp_ant_spd_stg;
create volatile multiset table rp_ant_spd_stg
as (
   select
      (select max(week_idnt)
       from prd_nap_usr_vws.day_cal_454_dim
       where week_end_day_date < CURRENT_DATE
      ) as week_idnt
      ,s.banner
      ,s.channel_type
      ,s.channel_label
      ,s.channel_num
      ,cat.div_label
      ,cat.subdiv_label
      ,cat.dept_label
      ,cat.class_label
      ,cat.subclass_label
      ,cat.div_num
      ,cat.subdiv_num
      ,cat.dept_num
      ,cat.class_num
      ,cat.subclass_num
      ,cat.quantrix_category
      ,cat.quantrix_category_group
      ,rp.supplier_idnt as supplier_number
      ,rp.Supplier as supplier_name
      ,rp.vendor_label_name as brand_name
	   ,coalesce(vd.npg_flag, 'N') as npg_ind
      ,'Y' as rp_ind
      ,'R' as price_type
      ,coalesce(pb.price_band_one_lower_bound,(select max(price_band_one_lower_bound) from price_band_lkup)) as price_band_one_lower_bound
      ,coalesce(pb.price_band_one_upper_bound,(select max(price_band_one_upper_bound) from price_band_lkup)) as price_band_one_upper_bound
      ,coalesce(pb.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) as price_band_two_lower_bound
      ,coalesce(pb.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) as price_band_two_upper_bound
      ,CASE WHEN week_end_day_date < CURRENT_DATE+35 then cast(rp.RP_ANT_SPD_U as INT) end as rp_ant_spd_4wk_units
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+35 and week_end_day_date < CURRENT_DATE+98 then cast(rp.RP_ANT_SPD_U as INT) end as rp_ant_spd_5wk_13wk_units
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+98 then cast(rp.RP_ANT_SPD_U as INT) end as rp_ant_spd_14wk_units
      ,CASE WHEN week_end_day_date < CURRENT_DATE+35 then rp.RP_ANT_SPD_C end as rp_ant_spd_4wk_cost
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+35 and week_end_day_date < CURRENT_DATE+98 then rp.RP_ANT_SPD_C end as rp_ant_spd_5wk_13wk_cost
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+98 then rp.RP_ANT_SPD_C end as rp_ant_spd_14wk_cost
      ,CASE WHEN week_end_day_date < CURRENT_DATE+35 then rp.RP_ANT_SPD_R end as rp_ant_spd_4wk_retail
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+35 and week_end_day_date < CURRENT_DATE+98 then rp.RP_ANT_SPD_R end as rp_ant_spd_5wk_13wk_retail
      ,CASE WHEN week_end_day_date >= CURRENT_DATE+98 then rp.RP_ANT_SPD_R end as rp_ant_spd_14wk_retail
from T2DL_DAS_OPEN_TO_BUY.rp_ant_spend_report rp
left join (SELECT DISTINCT week_idnt, week_start_day_date, week_end_day_date FROM prd_nap_usr_vws.DAY_CAL_454_DIM) DC
   on rp.WEEK_IDNT = DC.WEEK_IDNT
join category_lkup_2 cat
   on cat.dept_num = TRIM(SUBSTRING(rp.Department FROM 1 FOR POSITION(' ' IN rp.Department) - 1))
   and cat.class_num = TRIM(SUBSTRING("Class" FROM 1 FOR POSITION(' ' IN "Class") - 1))
   and cat.subclass_num = TRIM(SUBSTRING(Subclass FROM 1 FOR POSITION(' ' IN Subclass) - 1))
join
   (SELECT DISTINCT
   channel_num
   ,channel_label
   ,channel_type
   ,banner
   from STORE_lkup) S
   ON S.CHANNEL_NUM = TRIM(SUBSTRING(channel_number FROM 1 FOR POSITION(' ' IN channel_number) - 1))
left join prd_nap_usr_vws.vendor_dim vd
   on rp.supplier_idnt = vd.vendor_num
left join price_band_lkup pb
   on pb.next_highest_dollar = ceiling(coalesce(case when RP_ANT_SPD_U = 0 then 0 else cast(RP_ANT_SPD_R/RP_ANT_SPD_U as decimal(18,2)) end, 0))
where 1=1
-- only get records that have a price so we can assign them to a price band
   and coalesce(RP_ANT_SPD_R,0) <> 0
   and RP_ANT_SPD_U > 0
)
with data primary index (
                     channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
  )
   -- high confidence
   ,column(
      week_idnt, banner, channel_type, channel_label, channel_num,
      div_label, subdiv_label, dept_label, class_label, subclass_label, div_num, subdiv_num, dept_num, class_num, subclass_num,
      quantrix_category, quantrix_category_group, supplier_number, supplier_name, brand_name, npg_ind, rp_ind,
      price_type, price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- medium confidence
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on rp_ant_spd_stg;

/*
 * RP ANT SPEND FINAL
 *
 */

--drop table rp_ant_spd ;
create volatile multiset table rp_ant_spd
as (
   select
      week_idnt
      ,banner
      ,channel_type
      ,channel_label
      ,channel_num
      ,div_label
      ,subdiv_label
      ,dept_label
      ,class_label
      ,subclass_label
      ,div_num
      ,subdiv_num
      ,dept_num
      ,class_num
      ,subclass_num
      ,quantrix_category
      ,quantrix_category_group
      ,supplier_number
      ,supplier_name
      ,brand_name
      ,npg_ind
      ,rp_ind
      ,cast('N' as char(1)) as ds_ind
      ,price_type
      ,price_band_one_lower_bound
      ,price_band_one_upper_bound
      ,price_band_two_lower_bound
      ,price_band_two_upper_bound
      ,cast(0 as decimal(18,0))  as sales_u
      ,cast(0 as decimal(18,2))  as sales_c
      ,cast(0 as decimal(18,2))  as sales_r
      ,cast(0 as decimal(18,2))  as sales_r_with_cost
      ,cast(0 as decimal(18,0))  as demand_u
      ,cast(0 as decimal(18,2))  as demand_r
      ,cast(0 as decimal(18,0))  as eoh_u
      ,cast(0 as decimal(18,2))  as eoh_c
      ,cast(0 as decimal(18,2))  as eoh_r
      ,cast(0 as decimal(18,0))  as eop_u
      ,cast(0 as decimal(18,2))  as eop_c
      ,cast(0 as decimal(18,2))  as eop_r
      ,cast(0 as decimal(18,0))  as oo_4wk_u
      ,cast(0 as decimal(18,2))  as oo_4wk_c
      ,cast(0 as decimal(18,2))  as oo_4wk_r
      ,cast(0 as decimal(18,0))  as oo_5wk_13wk_u
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_c
      ,cast(0 as decimal(18,2))  as oo_5wk_13wk_r
      ,cast(0 as decimal(18,0))  as oo_14wk_u
      ,cast(0 as decimal(18,2))  as oo_14wk_c
      ,cast(0 as decimal(18,2))  as oo_14wk_r
      ,cast(sum(rp_ant_spd_4wk_units) as decimal(18,0))       as rp_ant_spd_4wk_u
      ,cast(sum(rp_ant_spd_4wk_cost) as decimal(18,2))        as rp_ant_spd_4wk_c
      ,cast(sum(rp_ant_spd_4wk_retail) as decimal(18,2))      as rp_ant_spd_4wk_r
      ,cast(sum(rp_ant_spd_5wk_13wk_units) as decimal(18,0))  as rp_ant_spd_5wk_13wk_u
      ,cast(sum(rp_ant_spd_5wk_13wk_cost) as decimal(18,2))   as rp_ant_spd_5wk_13wk_c
      ,cast(sum(rp_ant_spd_5wk_13wk_retail) as decimal(18,2)) as rp_ant_spd_5wk_13wk_r
      ,cast(sum(rp_ant_spd_14wk_units) as decimal(18,0))      as rp_ant_spd_14wk_u
      ,cast(sum(rp_ant_spd_14wk_cost) as decimal(18,2))       as rp_ant_spd_14wk_c
      ,cast(sum(rp_ant_spd_14wk_retail) as decimal(18,2))     as rp_ant_spd_14wk_r
   from rp_ant_spd_stg
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)
with data primary index (
                     channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                     channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   ,column(week_idnt)
   ,column(channel_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(supplier_number)
   ,column(brand_name)
   ,column(rp_ind)
   ,column(price_type)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on rp_ant_spd
;

/*
 * ANT SPD CLEANUP
 *
 * Drop volatile tables that we don't need anymore
 *
 */

drop table category_lkup_2;
drop table rp_ant_spd_stg;

/*
 * FULL DATA SET - STAGING
 *
 */

--drop table full_data_stg_base;
create volatile multiset table full_data_stg_base
as (
   select
      dt.ty_ly_ind
      ,dt.fiscal_year_num
      ,dt.fiscal_halfyear_num
      ,dt.quarter_abrv
      ,dt.quarter_label
      ,dt.month_idnt
      ,dt.month_abrv
      ,dt.month_label
      ,dt.month_start_day_date
      ,dt.month_end_day_date
      ,dt.fiscal_week_num
      ,dt.week_label
      ,dt.week_of_month_label
      ,dt.week_start_day_date
      ,dt.week_end_day_date
      ,sub.*
   from (
      select * from sales
      union all
      select * from demand
      union all
      select * from inventory
      union all
      select * from on_order
      union all
      select * from rp_ant_spd
   ) sub
   left join date_lkup dt
      -- fact staging data uses realigned week_idnt so we join on that instead of week_idnt_true
      on dt.week_idnt = sub.week_idnt
)
with data primary index (
                     week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                     price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
                  ) on commit preserve rows
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- high confidence
   ,column(
      ty_ly_ind, fiscal_year_num, fiscal_halfyear_num, quarter_abrv, quarter_label,
      month_idnt, month_abrv, month_label, month_start_day_date, month_end_day_date,
      fiscal_week_num, week_label, week_of_month_label, week_start_day_date, week_end_day_date, week_idnt,
      banner, channel_type, channel_label, channel_num,
      div_label, subdiv_label, dept_label, class_label, subclass_label, div_num, subdiv_num, dept_num, class_num, subclass_num,
      quantrix_category, quantrix_category_group, supplier_number, supplier_name, brand_name, npg_ind, rp_ind, ds_ind,
      price_type, price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
   -- medium confidence
   ,column(week_idnt)
   ,column(channel_num)
   ,column(div_num)
   ,column(subdiv_num)
   ,column(dept_num)
   ,column(class_num)
   ,column(subclass_num)
   ,column(price_band_one_lower_bound)
   ,column(price_band_one_upper_bound)
   ,column(price_band_two_lower_bound)
   ,column(price_band_two_upper_bound)
on full_data_stg_base
;


/*
 * STAGING CLEANUP
 *
 * Drop volatile tables that we don't need anymore
 *
 */

drop table sales;
drop table demand;
drop table inventory;
drop table on_order;
drop table rp_ant_spd;


/*
 * FULL DATA SET - FINAL
 *
 * Explicit null handling is included in queries above where necessary.
 * Explicit null handling included here for all fields as a precautionary step in case production tables have unexpected nulls.
 */

delete from T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.price_band_weekly
--delete from t3dl_ace_pra.price_band_weekly -- this line exists only for testing purposes
;

insert into T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.price_band_weekly
--insert into t3dl_ace_pra.price_band_weekly -- this line exists only for testing purposes
   select
      coalesce(d.ty_ly_ind,'U')                                      as ty_ly_ind
      ,coalesce(d.fiscal_year_num, -1)                               as fiscal_year_num
      ,coalesce(d.fiscal_halfyear_num, -1)                           as fiscal_halfyear_num
      ,coalesce(d.quarter_abrv,'U')                                  as quarter_abrv
      ,coalesce(d.quarter_label,'U')                                 as quarter_label
      ,coalesce(d.month_idnt, -1)                                    as month_idnt
      ,coalesce(d.month_abrv,'U')                                    as month_abrv
      ,coalesce(d.month_label,'U')                                   as month_label
      ,coalesce(d.month_start_day_date, cast('1970-01-01' as date))  as month_start_day_date
      ,coalesce(d.month_end_day_date, cast('1970-01-01' as date))    as month_end_day_date
      ,coalesce(d.fiscal_week_num, -1)                               as fiscal_week_num
      ,coalesce(d.week_label,'U')                                    as week_label
      ,coalesce(d.week_of_month_label,'U')                           as week_of_month_label
      ,coalesce(d.week_start_day_date, cast('1970-01-01' as date))   as week_start_day_date
      ,coalesce(d.week_end_day_date, cast('1970-01-01' as date))     as week_end_day_date
      ,coalesce(d.week_idnt, -1)                                     as week_idnt
      ,coalesce(d.banner,'UNKNOWN')                                  as banner
      ,coalesce(d.channel_type,'UNKNOWN')                            as channel_type
      ,coalesce(d.channel_label,'UNKNOWN')                           as channel_label
      ,coalesce(d.channel_num, -1)                                   as channel_num
      ,coalesce(d.div_label,'UNKNOWN')                               as div_label
      ,coalesce(d.subdiv_label,'UNKNOWN')                            as subdiv_label
      ,coalesce(d.dept_label,'UNKNOWN')                              as dept_label
      ,coalesce(d.class_label,'UNKNOWN')                             as class_label
      ,coalesce(d.subclass_label,'UNKNOWN')                          as subclass_label
      ,coalesce(d.div_num, -1)                                       as div_num
      ,coalesce(d.subdiv_num, -1)                                    as subdiv_num
      ,coalesce(d.dept_num, -1)                                      as dept_num
      ,coalesce(d.class_num, -1)                                     as class_num
      ,coalesce(d.subclass_num, -1)                                  as subclass_num
      ,coalesce(d.quantrix_category,'UNKNOWN')                       as quantrix_category
      ,coalesce(d.quantrix_category_group,'UNKNOWN')                 as quantrix_category_group
      ,coalesce(d.supplier_number, -1)                               as supplier_number
      ,coalesce(d.supplier_name,'UNKNOWN')                           as supplier_name
      ,coalesce(d.brand_name,'UNKNOWN')                              as brand_name
      ,coalesce(d.npg_ind,'U')                                       as npg_ind
      ,coalesce(d.rp_ind,'U')                                        as rp_ind
      ,coalesce(d.ds_ind,'U')                                        as ds_ind
      ,coalesce(d.price_type,'U')                                    as price_type
      ,coalesce(d.price_band_one_lower_bound, -1)                    as price_band_one_lower_bound
      ,coalesce(d.price_band_one_upper_bound, -1)                    as price_band_one_upper_bound
      ,coalesce(d.price_band_two_lower_bound, -1)                    as price_band_two_lower_bound
      ,coalesce(d.price_band_two_upper_bound, -1)                    as price_band_two_upper_bound
      ,coalesce(sum(d.sales_u),0)                                    as sales_u
      ,coalesce(sum(d.sales_c),0)                                    as sales_c
      ,coalesce(sum(d.sales_r),0)                                    as sales_r
      ,coalesce(sum(d.sales_r_with_cost),0)                          as sales_r_with_cost
      ,coalesce(sum(d.demand_u),0)                                   as demand_u
      ,coalesce(sum(d.demand_r),0)                                   as demand_r
      ,coalesce(sum(d.eoh_u),0)                                      as eoh_u
      ,coalesce(sum(d.eoh_c),0)                                      as eoh_c
      ,coalesce(sum(d.eoh_r),0)                                      as eoh_r
      ,coalesce(sum(d.eop_u),0)                                      as eop_u
      ,coalesce(sum(d.eop_c),0)                                      as eop_c
      ,coalesce(sum(d.eop_r),0)                                      as eop_r
      ,coalesce(sum(d.oo_4wk_u + d.rp_ant_spd_4wk_u),0)              as oo_4wk_u
      ,coalesce(sum(d.oo_4wk_c + d.rp_ant_spd_4wk_c),0)              as oo_4wk_c
      ,coalesce(sum(d.oo_4wk_r + d.rp_ant_spd_4wk_r),0)              as oo_4wk_r
      ,coalesce(sum(d.oo_5wk_13wk_u + d.rp_ant_spd_5wk_13wk_u),0)    as oo_5wk_13wk_u
      ,coalesce(sum(d.oo_5wk_13wk_c + d.rp_ant_spd_5wk_13wk_c),0)    as oo_5wk_13wk_c
      ,coalesce(sum(d.oo_5wk_13wk_r + d.rp_ant_spd_5wk_13wk_r),0)    as oo_5wk_13wk_r
      ,coalesce(sum(d.oo_14wk_u + d.rp_ant_spd_14wk_u),0)            as oo_14wk_u
      ,coalesce(sum(d.oo_14wk_c + d.rp_ant_spd_14wk_c),0)            as oo_14wk_c
      ,coalesce(sum(d.oo_14wk_r + d.rp_ant_spd_14wk_r),0)            as oo_14wk_r
      ,current_timestamp as load_timestamp
   from full_data_stg_base d
   group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
            ,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40
            ,41,42,43
;

collect stats
   primary index (
                  week_idnt, channel_num, dept_num, class_num, subclass_num, supplier_number, brand_name, rp_ind, ds_ind, price_type,
                  price_band_one_lower_bound, price_band_one_upper_bound, price_band_two_lower_bound, price_band_two_upper_bound
   )
on T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.price_band_weekly
--on t3dl_ace_pra.price_band_weekly -- this line exists only for testing purposes
;