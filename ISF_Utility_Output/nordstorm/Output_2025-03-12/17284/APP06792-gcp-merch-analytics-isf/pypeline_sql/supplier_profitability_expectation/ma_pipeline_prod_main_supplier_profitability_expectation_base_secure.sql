/*
Purpose:          Inserts data in {{dsa_ai_secure_schema}} tables for supplier profitability
                            supplier_profitability_expectation_channel_week_dept_agg
Variable(s):     {{dsa_ai_secure_schema}} PROTO_DSA_AI_BASE_VWS for dev or PRD_NAP_DSA_AI_BASE_VWS for prod
                 {{wk_end_dt}} current_date -1(refreshes every week)
Author(s):     Jevon Barlas (copied from primary SPD code by Manuela Hurtado Gonzalez)
Updated: 		 10/14/2024 by Jevon Barlas - add net sales from Nordstrom Exhaust (Racking)
*/

create volatile multiset table sku_lkup as (
  select	distinct
    sku.rms_sku_num,
    sku.channel_country,
    trim(sku.div_num)||', '||trim(sku.div_desc) as div_label,
    trim(sku.grp_num||', '||sku.grp_desc) as sdiv_label,
    trim(sku.dept_num||', '||sku.dept_desc) as dept_label,
    trim(sku.dept_num) as dept_idnt,
    sku.prmy_supp_num as supplier_number,
    sku.brand_name,
    sku.brand_label_display_name as brand_label,
    sku.brand_label_num,
    sku.npg_ind as npg_ind,
    supp.vendor_name as supplier
  from prd_nap_base_vws.product_sku_dim_vw sku
  left join prd_nap_base_vws.vendor_dim supp
    on sku.prmy_supp_num =supp.vendor_num
  where sku.gwp_ind <> 'Y'
  and dept_idnt <> -1
  and channel_country = 'US'
 	and supplier_number <> '-1'
 	and dept_label not like '%inact%'
 	and div_label not like '%inact%'
 	and sdiv_label not like '%inact%'
 	 	
) with data unique primary index(rms_sku_num) on commit preserve rows;

collect statistics
  unique primary index ( rms_sku_num ),
  column ( rms_sku_num ) on sku_lkup;

  create volatile multiset table date_lkup
  as (
  	select distinct
  		ty_ly_lly_ind as ty_ly_ind,
  		wk.fiscal_year_num,
  		wk.fiscal_halfyear_num,
  		wk.quarter_abrv,
  		wk.quarter_label,
  		wk.month_abrv,
  		wk.month_idnt,
  		wk.month_label,
  		wk.week_desc,
  		concat(left(wk.week_label, 9), 'WK', trim(wk.week_num_of_fiscal_month)) as week_label,
  		wk.week_idnt,
  		wk.fiscal_week_num,
  		wk.week_start_day_date,
  		wk.week_end_day_date,
  		b.week_idnt AS week_idnt_true
  		--b.fiscal_year_num AS fiscal_year_num_true -- TRUE-DAY (aka actual) fiscal YEAR num
  	from prd_nap_vws.REALIGNED_DATE_LKUP_VW wk
  	left join prd_nap_base_vws.day_cal_454_dim b
          	on wk.day_date = b.day_date
  	where ty_ly_lly_ind IN ('TY', 'LY')
  	and wk.week_end_day_date <= current_date -1
  ) with data unique primary index(week_idnt) on commit preserve rows;

  collect statistics unique primary index (week_idnt),
  	column(week_idnt) on date_lkup;

create volatile multiset table supplier_lkup
as (
	select
    supplier_num,
    dept_num,
    banner,
    supplier_group
	from prd_nap_base_vws.supp_dept_map_dim
	where 1=1
		and supplier_group <> '-1'
		and (select max(week_end_day_date) from date_lkup) between eff_begin_tmstp and eff_end_tmstp
) with data unique primary index(supplier_num, dept_num, banner) on commit preserve rows;

collect statistics
  unique primary index (supplier_num, dept_num, banner),
  column ( supplier_num ),
  column ( dept_num ),
  column ( banner ) on supplier_lkup;

  create volatile multiset table supp_prof
as (
select distinct
  s.supplier_group,
  s.department_number as dept_num,
  case
  when b.banner_desc like 'OFF_PRICE' then 'OP'
  when b.banner_desc like 'FULL_PRICE' then 'FP'
  end as banner,
  s.selling_country as channel_country,
  s.year_num as fiscal_year_num,
  s.planned_profitability_expectation_percent as ty_profitability_expectation,
  s.regular_price_sales_expectation_percent as ty_reg_sales_expectation,
  s.initial_markup_expectation_percent as ty_initial_markup_expectation
from prd_nap_vws.merch_spe_profitability_expectation_vw s
inner join prd_nap_vws.org_banner_country_dim b
  on s.banner_num = b.banner_country_num
where s.year_num in (select fiscal_year_num from prd_nap_base_vws.day_cal_454_dim group by 1 where fiscal_year_num >= (select max(fiscal_year_num) - 1 as ty_fiscal_yr from prd_nap_base_vws.day_cal_454_dim where week_end_day_date < current_date))
and supplier_group <> '-1'
and channel_country = 'US'
) with data unique primary index(fiscal_year_num, dept_num, supplier_group, banner) on commit preserve rows;

collect statistics
unique primary index (fiscal_year_num, dept_num, supplier_group, banner),
column ( dept_num ),
column ( supplier_group ),
column ( fiscal_year_num ),
column ( banner ) on supp_prof;

create volatile multiset table store_lkup
as (
	select distinct
	  store_num,
	  business_unit_desc,
	  channel_num,
	  channel_desc,
	  store_country_code,
	  case
		  when channel_num in (210, 211, 220, 221, 240, 250, 260, 261) then 'OP'
		  else 'FP'
	  end as banner
	from prd_nap_base_vws.store_dim
  where channel_num not in (580,922,921,990,940)
  and store_country_code = 'US'
) with data unique primary index(store_num) on commit preserve rows;

collect statistics unique primary index (store_num),
	column(store_num) on store_lkup;

create volatile multiset table sb_lkup
as (
  select
    case
      when sb_banner = 'NORDSTROM' then 'FP'
      else 'OP'
    end as banner,
    vendor_brand_code,
    vendor_brand_name_adj as vendor_brand_name,
    supplier_num,
    dept_num,
    supplier_group,
    'Y' as sb_ind
  from t2dl_das_in_season_management_reporting.strat_brands_supp_curr_vw
  where supplier_group not like 'OTHER'
  and supplier_group not like '% OTHER'
  and supplier_group not like 'OTHER %'
  and banner = 'FP'
) with data primary index(supplier_num) on commit preserve rows;

collect statistics primary index (supplier_num),
	column(supplier_num),
	column(banner),
	column(vendor_brand_code),
  column(dept_num) on sb_lkup;

/*
 * NET SALES
 */

create volatile multiset table sales_stg_base as (

   select
      dt.ty_ly_ind,
      dt.week_idnt,
      coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
      coalesce(store.channel_num,-1) as channel_num,
      coalesce(store.channel_desc,'N/A') as channel_desc,
      cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
      cast(coalesce(store.banner,'N/A') as char(3)) as banner,
      sku.div_label,
      sku.sdiv_label,
      sku.dept_label,
      sku.dept_idnt,
      sku.supplier_number,
      sku.brand_name,
      sku.brand_label,
      sku.brand_label_num,
      sku.npg_ind,
      sku.supplier,
    
      -- net sales
      coalesce(net_sales_tot_retl,0) as net_sales_tot_retl ,
      coalesce(net_sales_tot_regular_retl,0) as net_sales_tot_regular_retl ,
      coalesce(net_sales_tot_promo_retl,0) as net_sales_tot_promo_retl ,
      coalesce(net_sales_tot_clearance_retl,0) as net_sales_tot_clearance_retl ,
      coalesce(net_sales_tot_cost,0) as net_sales_tot_cost ,
      coalesce(net_sales_tot_regular_cost,0) as net_sales_tot_regular_cost ,
      coalesce(net_sales_tot_promo_cost,0) as net_sales_tot_promo_cost ,
      coalesce(net_sales_tot_clearance_cost,0) as net_sales_tot_clearance_cost ,
      coalesce(net_sales_tot_units,0) as net_sales_tot_units ,
      coalesce(net_sales_tot_regular_units,0) as net_sales_tot_regular_units ,
      coalesce(net_sales_tot_promo_units,0) as net_sales_tot_promo_units ,
      coalesce(net_sales_tot_clearance_units,0) as net_sales_tot_clearance_units ,

      -- NX-adjusted sales starts in this query as net sales, then adjustments are made in later queries
      coalesce(net_sales_tot_retl,0) as net_sales_nx_adj_tot_retl ,
      coalesce(net_sales_tot_regular_retl,0) as net_sales_nx_adj_tot_regular_retl ,
      coalesce(net_sales_tot_promo_retl,0) as net_sales_nx_adj_tot_promo_retl ,
      coalesce(net_sales_tot_clearance_retl,0) as net_sales_nx_adj_tot_clearance_retl ,
      coalesce(net_sales_tot_cost,0) as net_sales_nx_adj_tot_cost ,
      coalesce(net_sales_tot_regular_cost,0) as net_sales_nx_adj_tot_regular_cost ,
      coalesce(net_sales_tot_promo_cost,0) as net_sales_nx_adj_tot_promo_cost ,
      coalesce(net_sales_tot_clearance_cost,0) as net_sales_nx_adj_tot_clearance_cost ,
      coalesce(net_sales_tot_units,0) as net_sales_nx_adj_tot_units ,
      coalesce(net_sales_tot_regular_units,0) as net_sales_nx_adj_tot_regular_units ,
      coalesce(net_sales_tot_promo_units,0) as net_sales_nx_adj_tot_promo_units ,
      coalesce(net_sales_tot_clearance_units,0) as net_sales_nx_adj_tot_clearance_units ,

      -- returns
      coalesce(returns_tot_retl,0) as returns_tot_retl ,
      coalesce(returns_tot_regular_retl,0) as returns_tot_regular_retl ,
      coalesce(returns_tot_promo_retl,0) as returns_tot_promo_retl ,
      coalesce(returns_tot_clearance_retl,0) as returns_tot_clearance_retl ,
      coalesce(returns_tot_cost,0) as returns_tot_cost ,
      coalesce(returns_tot_regular_cost,0) as returns_tot_regular_cost ,
      coalesce(returns_tot_promo_cost,0) as returns_tot_promo_cost ,
      coalesce(returns_tot_clearance_cost,0) as returns_tot_clearance_cost ,
      coalesce(returns_tot_units,0) as returns_tot_units ,
      coalesce(returns_tot_regular_units,0) as returns_tot_regular_units ,
      coalesce(returns_tot_promo_units,0) as returns_tot_promo_units ,
      coalesce(returns_tot_clearance_units,0) as returns_tot_clearance_units ,
      
      -- drop ship sales
      coalesce(case when dropship_ind = 'Y' then net_sales_tot_retl else 0 end,0) as dropship_net_sales_tot_retl,
      coalesce(case when dropship_ind = 'Y' then net_sales_tot_cost else 0 end,0) as dropship_net_sales_tot_cost,
      coalesce(case when dropship_ind = 'Y' then net_sales_tot_units else 0 end,0) as dropship_net_sales_tot_units

   from prd_nap_base_vws.merch_sale_return_sku_store_week_fact_vw as sales
      inner join date_lkup as dt
         on sales.week_num = dt.week_idnt_true
      inner join store_lkup as store
         on sales.store_num = store.store_num
      inner join sku_lkup as sku
         on sales.rms_sku_num = sku.rms_sku_num
      
   where 1=1 
      and store.channel_num <> 920
      and wac_avlbl_ind = 'Y'
      
) 
with data 
primary index (week_idnt, channel_num, supplier_number, dept_idnt) 
on commit preserve rows
;

collect stats 
   primary index(week_idnt, channel_num, supplier_number, dept_idnt),
   column(supplier_number),
   column(week_idnt),
   column(dept_idnt),
   column(channel_num) 
on sales_stg_base
;

/*
 * RACK BANNER NET SALES FROM NORDSTROM EXHAUST ITEMS
 * 
 * For adjusting Rack banner net sales
 * NX Adj sales are set to opposite sign so that they will be removed from Rack net sales
 */

create volatile multiset table sales_nx_rack_stg as (

   select
      dt.ty_ly_ind,
      dt.week_idnt,
      coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
      coalesce(store.channel_num,-1) as channel_num,
      coalesce(store.channel_desc,'N/A') as channel_desc,
      cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
      cast(coalesce(store.banner,'N/A') as char(3)) as banner,
      sku.div_label,
      sku.sdiv_label,
      sku.dept_label,
      sku.dept_idnt,
      sku.supplier_number,
      sku.brand_name,
      sku.brand_label,
      sku.brand_label_num,
      sku.npg_ind,
      sku.supplier,
      cast(0 as decimal(12,2)) as net_sales_tot_retl ,
      cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
      cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
      cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as net_sales_tot_cost ,
      cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
      cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
      cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as net_sales_tot_units ,
      cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
      cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
      cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      coalesce(-1.00*net_sales_tot_retl,0) as net_sales_nx_adj_tot_retl ,
      coalesce(-1.00*net_sales_tot_regular_retl,0) as net_sales_nx_adj_tot_regular_retl ,
      coalesce(-1.00*net_sales_tot_promo_retl,0) as net_sales_nx_adj_tot_promo_retl ,
      coalesce(-1.00*net_sales_tot_clearance_retl,0) as net_sales_nx_adj_tot_clearance_retl ,
      coalesce(-1.00*net_sales_tot_cost,0) as net_sales_nx_adj_tot_cost ,
      coalesce(-1.00*net_sales_tot_regular_cost,0) as net_sales_nx_adj_tot_regular_cost ,
      coalesce(-1.00*net_sales_tot_promo_cost,0) as net_sales_nx_adj_tot_promo_cost ,
      coalesce(-1.00*net_sales_tot_clearance_cost,0) as net_sales_nx_adj_tot_clearance_cost ,
      coalesce(-1*net_sales_tot_units,0) as net_sales_nx_adj_tot_units ,
      coalesce(-1*net_sales_tot_regular_units,0) as net_sales_nx_adj_tot_regular_units ,
      coalesce(-1*net_sales_tot_promo_units,0) as net_sales_nx_adj_tot_promo_units ,
      coalesce(-1*net_sales_tot_clearance_units,0) as net_sales_nx_adj_tot_clearance_units ,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
      cast(0 as decimal(12,2)) as dropship_net_sales_tot_retl,
      cast(0 as decimal(12,2)) as dropship_net_sales_tot_cost,
      cast(0 as decimal(12,2)) as dropship_net_sales_tot_units
      
   from prd_nap_base_vws.merch_sale_return_sku_store_week_fact_vw as sales
      inner join date_lkup as dt
         on sales.week_num = dt.week_idnt_true
      inner join store_lkup as store
         on sales.store_num = store.store_num
      inner join sku_lkup as sku
         on sales.rms_sku_num = sku.rms_sku_num
      inner join prd_nap_dsa_ai_base_vws.source_of_goods_fact as sog
         on sog.rms_sku_num = sales.rms_sku_num
         and sog.channel_num = store.channel_num
         and dt.week_end_day_date between sog.eff_begin_date and sog.eff_end_date
         
   where 1=1
      and store.channel_num <> 920
      and wac_avlbl_ind = 'Y'
      -- only NX sales from Rack banner
      and sog.channel_num in (210,250)
      and sog.source_of_goods_primary = 'NORDSTROM EXHAUST'
      
) 
with data 
primary index (week_idnt, channel_num, supplier_number, dept_idnt) 
on commit preserve rows
;

collect stats 
   primary index(week_idnt, channel_num, supplier_number, dept_idnt),
   column(supplier_number),
   column(week_idnt),
   column(dept_idnt),
   column(channel_num) 
on sales_nx_rack_stg
;

/*
 * RACK BANNER NET SALES FROM NORDSTROM EXHAUST ITEMS
 * 
 * For adjusting Nord banner net sales
 * Assign Rack stores to Nord stores and r.com to n.com
 * NX Adj sales set to opposite sign from Rack banner so that they will be added to Nord net sales
 */
--drop table sales_nx_nord_agg;
create volatile multiset table sales_nx_nord_stg as (

   select
      ty_ly_ind,
      week_idnt,
      case snr.channel_num
         when 210 then 'FULL LINE'
         when 250 then 'N.COM'
      end as business_unit_desc,   
      case snr.channel_num
         when 210 then 110
         when 250 then 120
      end as channel_num,
      case snr.channel_num
         when 210 then 'FULL LINE STORES'
         when 250 then 'N.COM'
      end as channel_desc,
      country_code,
      'FP' as banner,
      div_label,
      sdiv_label,
      dept_label,
      dept_idnt,
      supplier_number,
      brand_name,
      brand_label,
      brand_label_num,
      npg_ind,
      supplier,
      cast(0 as decimal(12,2)) as net_sales_tot_retl ,
      cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
      cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
      cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as net_sales_tot_cost ,
      cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
      cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
      cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as net_sales_tot_units ,
      cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
      cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
      cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      -1.00*net_sales_nx_adj_tot_retl as net_sales_nx_adj_tot_retl,
      -1.00*net_sales_nx_adj_tot_regular_retl as net_sales_nx_adj_tot_regular_retl,
      -1.00*net_sales_nx_adj_tot_promo_retl as net_sales_nx_adj_tot_promo_retl,
      -1.00*net_sales_nx_adj_tot_clearance_retl as net_sales_nx_adj_tot_clearance_retl,
      -1.00*net_sales_nx_adj_tot_cost as net_sales_nx_adj_tot_cost,
      -1.00*net_sales_nx_adj_tot_regular_cost as net_sales_nx_adj_tot_regular_cost,
      -1.00*net_sales_nx_adj_tot_promo_cost as net_sales_nx_adj_tot_promo_cost,
      -1.00*net_sales_nx_adj_tot_clearance_cost as net_sales_nx_adj_tot_clearance_cost,
      -1*net_sales_nx_adj_tot_units as net_sales_nx_adj_tot_units,
      -1*net_sales_nx_adj_tot_regular_units as net_sales_nx_adj_tot_regular_units,
      -1*net_sales_nx_adj_tot_promo_units as net_sales_nx_adj_tot_promo_units,
      -1*net_sales_nx_adj_tot_clearance_units as net_sales_nx_adj_tot_clearance_units,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
      cast(0 as decimal(12,2)) as dropship_net_sales_tot_retl,
      cast(0 as decimal(12,2)) as dropship_net_sales_tot_cost,
      cast(0 as decimal(12,2)) as dropship_net_sales_tot_units

   from sales_nx_rack_stg as snr
               
) 
with data 
primary index (week_idnt, channel_num, supplier_number, dept_idnt) 
on commit preserve rows
;

collect stats 
   primary index(week_idnt, channel_num, supplier_number, dept_idnt),
   column(supplier_number),
   column(week_idnt),
   column(dept_idnt),
   column(channel_num) 
on sales_nx_nord_stg
;


/*
 * STAGING TABLE FOR ALL NET SALES INCLUDING NX ADJ
 * 
 */

create volatile multiset table sales_stg as (

   select * from sales_stg_base
   union all
   select * from sales_nx_rack_stg
   union all
   select * from sales_nx_nord_stg
   
) 
with data 
primary index (week_idnt, channel_num, supplier_number, dept_idnt) 
on commit preserve rows
;

collect stats 
   primary index(week_idnt, channel_num, supplier_number, dept_idnt),
   column(supplier_number),
   column(week_idnt),
   column(dept_idnt),
   column(channel_num) 
on sales_stg
;
   
   
/*
 * FINAL TABLE FOR NET SALES 
 */
--drop table sales_final;
create volatile multiset table sales_final as (

	select
      s.ty_ly_ind
      ,s.week_idnt
      ,s.business_unit_desc
      ,s.channel_num
      ,s.channel_desc
      ,s.country_code
      ,s.banner
      ,s.div_label
      ,s.sdiv_label
      ,s.dept_label
      ,s.dept_idnt
      ,s.supplier_number
      ,s.brand_name
      ,s.brand_label
      ,s.brand_label_num
      ,s.npg_ind
      ,s.supplier,
      
      sum(s.net_sales_tot_retl) as net_sales_tot_retl ,
		sum(s.net_sales_tot_regular_retl) as net_sales_tot_regular_retl ,
		sum(s.net_sales_tot_promo_retl) as net_sales_tot_promo_retl ,
		sum(s.net_sales_tot_clearance_retl) as net_sales_tot_clearance_retl ,
		sum(s.net_sales_tot_cost) as net_sales_tot_cost ,
		sum(s.net_sales_tot_regular_cost) as net_sales_tot_regular_cost ,
		sum(s.net_sales_tot_promo_cost) as net_sales_tot_promo_cost ,
		sum(s.net_sales_tot_clearance_cost) as net_sales_tot_clearance_cost ,
		sum(s.net_sales_tot_units) as net_sales_tot_units ,
		sum(s.net_sales_tot_regular_units) as net_sales_tot_regular_units ,
		sum(s.net_sales_tot_promo_units) as net_sales_tot_promo_units ,
		sum(s.net_sales_tot_clearance_units) as net_sales_tot_clearance_units ,
		
		sum(s.net_sales_nx_adj_tot_retl ) as net_sales_nx_adj_tot_retl  ,
      sum(s.net_sales_nx_adj_tot_regular_retl ) as net_sales_nx_adj_tot_regular_retl  ,
      sum(s.net_sales_nx_adj_tot_promo_retl ) as net_sales_nx_adj_tot_promo_retl  ,
      sum(s.net_sales_nx_adj_tot_clearance_retl ) as net_sales_nx_adj_tot_clearance_retl  ,
      sum(s.net_sales_nx_adj_tot_cost ) as net_sales_nx_adj_tot_cost  ,
      sum(s.net_sales_nx_adj_tot_regular_cost ) as net_sales_nx_adj_tot_regular_cost  ,
      sum(s.net_sales_nx_adj_tot_promo_cost ) as net_sales_nx_adj_tot_promo_cost  ,
      sum(s.net_sales_nx_adj_tot_clearance_cost ) as net_sales_nx_adj_tot_clearance_cost  ,
      sum(s.net_sales_nx_adj_tot_units ) as net_sales_nx_adj_tot_units  ,
      sum(s.net_sales_nx_adj_tot_regular_units ) as net_sales_nx_adj_tot_regular_units  ,
      sum(s.net_sales_nx_adj_tot_promo_units ) as net_sales_nx_adj_tot_promo_units  ,
      sum(s.net_sales_nx_adj_tot_clearance_units ) as net_sales_nx_adj_tot_clearance_units  ,
      
		sum(s.returns_tot_retl) as returns_tot_retl ,
		sum(s.returns_tot_regular_retl) as returns_tot_regular_retl ,
		sum(s.returns_tot_promo_retl) as returns_tot_promo_retl ,
		sum(s.returns_tot_clearance_retl) as returns_tot_clearance_retl ,
		sum(s.returns_tot_cost) as returns_tot_cost ,
		sum(s.returns_tot_regular_cost) as returns_tot_regular_cost ,
		sum(s.returns_tot_promo_cost) as returns_tot_promo_cost ,
		sum(s.returns_tot_clearance_cost) as returns_tot_clearance_cost ,
		sum(s.returns_tot_units) as returns_tot_units ,
		sum(s.returns_tot_regular_units) as returns_tot_regular_units ,
		sum(s.returns_tot_promo_units) as returns_tot_promo_units ,
		sum(s.returns_tot_clearance_units) as returns_tot_clearance_units ,

      sum(s.dropship_net_sales_tot_retl) as dropship_net_sales_tot_retl ,
      sum(s.dropship_net_sales_tot_cost) as dropship_net_sales_tot_cost ,
      sum(s.dropship_net_sales_tot_units) as dropship_net_sales_tot_units ,
		
		cast(0 as decimal(12,2)) as demand_tot_amt ,
		cast(0 as decimal(12,2)) as demand_regular_amt ,
		cast(0 as decimal(12,2)) as demand_promo_amt ,
		cast(0 as decimal(12,2)) as demand_clearence_amt ,
		cast(0 as decimal(12,2)) as demand_unknown_amt ,
		cast(0 as decimal(10,0)) as demand_tot_qty ,
		cast(0 as decimal(10,0)) as demand_regular_qty ,
		cast(0 as decimal(10,0)) as demand_promo_qty ,
		cast(0 as decimal(10,0)) as demand_clearence_qty ,
		cast(0 as decimal(10,0)) as demand_unknown_qty ,
		cast(0 as decimal(10,0)) as total_receipts_units ,
		cast(0 as decimal(12,2)) as total_receipts_cost ,
		cast(0 as decimal(12,2)) as total_receipts_retail ,
		cast(0 as decimal(10,0)) as dropship_receipts_units ,
		cast(0 as decimal(12,2)) as dropship_receipts_cost ,
		cast(0 as decimal(12,2)) as dropship_receipts_retail ,
		cast(0 as decimal(10,0)) as packandhold_transfer_in_units ,
		cast(0 as decimal(10,0)) as packandhold_transfer_out_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_in_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_out_units ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_cost ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_retail ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_retail ,
		cast(0 as decimal(12,2)) as disc_terms_cost ,
		cast(0 as decimal(12,2)) as vendor_funds_retail,
		cast(0 as decimal(12,2)) as vendor_funds_cost,
		cast(0 as decimal(12,2)) as boh_total_cost,
		cast(0 as decimal(10,0)) as boh_total_units,
		cast(0 as decimal(12,2)) as boh_total_retail,
		cast(0 as decimal(12,2)) as boh_clearance_cost,
		cast(0 as decimal(10,0)) as boh_clearance_units,
		cast(0 as decimal(12,2)) as boh_clearance_retail,
		cast(0 as decimal(12,2)) as boh_regular_cost,
		cast(0 as decimal(10,0)) as boh_regular_units,
		cast(0 as decimal(12,2)) as boh_regular_retail,
      cast(0 as decimal(12,2)) as eoh_total_cost,
      cast(0 as decimal(10,0)) as eoh_total_units,
		cast(0 as decimal(12,2)) as eoh_total_retail,
      cast(0 as decimal(12,2)) as eoh_clearance_cost,
      cast(0 as decimal(10,0)) as eoh_clearance_units,
		cast(0 as decimal(12,2)) as eoh_clearance_retail,
      cast(0 as decimal(12,2)) as eoh_regular_cost,
      cast(0 as decimal(10,0)) as eoh_regular_units,
		cast(0 as decimal(12,2)) as eoh_regular_retail,
      cast(0 as decimal(12,2)) as mos_total_cost_amt,
		cast(0 as decimal(10,0)) as mos_total_units,
		cast(0 as decimal(12,2)) as mos_total_retail_amt,
		cast(0 as decimal(12,2)) as rtv_total_cost,
		cast(0 as decimal(12,2)) as rtv_total_retail,
		cast(0 as decimal(10,0)) as rtv_total_units

   from sales_stg as s
		
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
	
) 
with data 
primary index (week_idnt, channel_num, supplier_number, dept_idnt) 
on commit preserve rows
;

collect stats 
   primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) 
on sales_final
;


create volatile multiset table demand_stg
as (
	select
		dt.ty_ly_ind,
		dt.week_idnt,
		coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
		coalesce(store.channel_num,-1) as channel_num,
		coalesce(store.channel_desc,'N/A') as channel_desc,
		cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
		cast(coalesce(store.banner,'N/A') as char(3)) as banner,
		sku.div_label,
    sku.sdiv_label,
   	sku.dept_label,
    sku.dept_idnt,
    sku.supplier_number,
    sku.brand_name,
	  sku.brand_label,
	  sku.brand_label_num,
	  sku.npg_ind,
	  sku.supplier,
		coalesce(dmd.demand_tot_amt,0) as demand_tot_amt ,
		coalesce(dmd.demand_regular_amt,0) as demand_regular_amt ,
		coalesce(dmd.demand_promo_amt,0) as demand_promo_amt ,
		coalesce(dmd.demand_clearence_amt,0) as demand_clearence_amt ,
		coalesce(dmd.demand_unknown_amt,0) as demand_unknown_amt ,
		coalesce(dmd.demand_tot_qty,0) as demand_tot_qty ,
		coalesce(dmd.demand_regular_qty,0) as demand_regular_qty ,
		coalesce(dmd.demand_promo_qty,0) as demand_promo_qty ,
		coalesce(dmd.demand_clearence_qty,0) as demand_clearence_qty ,
		coalesce(dmd.demand_unknown_qty,0) as demand_unknown_qty
	from prd_nap_base_vws.merch_demand_sku_store_week_agg_fact_vw dmd
	inner join date_lkup as dt
		on dmd.week_num = dt.week_idnt_true
	inner join store_lkup as store
		on dmd.store_num = store.store_num
	inner join sku_lkup as sku
		on dmd.rms_sku_num = sku.rms_sku_num
  where store.channel_num <> 920
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on demand_stg;

create volatile multiset table demand_final
as (
	select
		ty_ly_ind,
		week_idnt,
		business_unit_desc,
		channel_num,
		channel_desc,
		country_code,
		banner,
		div_label,
	  sdiv_label,
	  dept_label,
	  dept_idnt,
	  supplier_number,
	  brand_name,
	  brand_label,
	  brand_label_num,
	  npg_ind,
	  supplier,
	  cast(0 as decimal(12,2)) as net_sales_tot_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as net_sales_tot_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_cost  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_regular_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_promo_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_clearance_units  ,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_retl ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_cost ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_units,
		sum(demand_tot_amt) as demand_tot_amt ,
		sum(demand_regular_amt) as demand_regular_amt ,
		sum(demand_promo_amt) as demand_promo_amt ,
		sum(demand_clearence_amt) as demand_clearence_amt ,
		sum(demand_unknown_amt) as demand_unknown_amt ,
		sum(demand_tot_qty) as demand_tot_qty ,
		sum(demand_regular_qty) as demand_regular_qty ,
		sum(demand_promo_qty) as demand_promo_qty ,
		sum(demand_clearence_qty) as demand_clearence_qty ,
		sum(demand_unknown_qty) as demand_unknown_qty ,
		cast(0 as decimal(10,0)) as total_receipts_units ,
		cast(0 as decimal(12,2)) as total_receipts_cost ,
		cast(0 as decimal(12,2)) as total_receipts_retail ,
		cast(0 as decimal(10,0)) as dropship_receipts_units ,
		cast(0 as decimal(12,2)) as dropship_receipts_cost ,
		cast(0 as decimal(12,2)) as dropship_receipts_retail ,
		cast(0 as decimal(10,0)) as packandhold_transfer_in_units ,
		cast(0 as decimal(10,0)) as packandhold_transfer_out_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_in_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_out_units ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_cost ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_retail ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_retail ,
		cast(0 as decimal(12,2)) as disc_terms_cost ,
		cast(0 as decimal(12,2)) as vendor_funds_retail,
		cast(0 as decimal(12,2)) as vendor_funds_cost,
		cast(0 as decimal(12,2)) as boh_total_cost,
		cast(0 as decimal(10,0)) as boh_total_units,
		cast(0 as decimal(12,2)) as boh_total_retail,
		cast(0 as decimal(12,2)) as boh_clearance_cost,
		cast(0 as decimal(10,0)) as boh_clearance_units,
		cast(0 as decimal(12,2)) as boh_clearance_retail,
		cast(0 as decimal(12,2)) as boh_regular_cost,
		cast(0 as decimal(10,0)) as boh_regular_units,
		cast(0 as decimal(12,2)) as boh_regular_retail,
	  cast(0 as decimal(12,2)) as eoh_total_cost,
	  cast(0 as decimal(10,0)) as eoh_total_units,
		cast(0 as decimal(12,2)) as eoh_total_retail,
	  cast(0 as decimal(12,2)) as eoh_clearance_cost,
	  cast(0 as decimal(10,0)) as eoh_clearance_units,
		cast(0 as decimal(12,2)) as eoh_clearance_retail,
	  cast(0 as decimal(12,2)) as eoh_regular_cost,
	  cast(0 as decimal(10,0)) as eoh_regular_units,
		cast(0 as decimal(12,2)) as eoh_regular_retail,
	  cast(0 as decimal(12,2)) as mos_total_cost_amt,
		cast(0 as decimal(10,0)) as mos_total_units,
		cast(0 as decimal(12,2)) as mos_total_retail_amt,
		cast(0 as decimal(12,2)) as rtv_total_cost,
		cast(0 as decimal(12,2)) as rtv_total_retail,
		cast(0 as decimal(10,0)) as rtv_total_units
	from demand_stg
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on demand_final;

create volatile multiset table receipts_stg
as (
	select
		dt.ty_ly_ind,
		dt.week_idnt,
		coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
		coalesce(store.channel_num,-1) as channel_num,
		coalesce(store.channel_desc,'N/A') as channel_desc,
		cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
		cast(coalesce(store.banner,'N/A') as char(3)) as banner,
		sku.div_label,
    sku.sdiv_label,
   	sku.dept_label,
    sku.dept_idnt,
    sku.supplier_number,
    sku.brand_name,
	  sku.brand_label,
	  sku.brand_label_num,
	  sku.npg_ind,
	  sku.supplier,
  	coalesce(receipts_regular_units,0) + coalesce(receipts_clearance_units,0) + coalesce(receipts_crossdock_regular_units,0) + coalesce(receipts_crossdock_clearance_units,0) as total_receipts_units,
	  coalesce(receipts_regular_cost,0) + coalesce(receipts_clearance_cost,0) + coalesce(receipts_crossdock_regular_cost,0) + coalesce(receipts_crossdock_clearance_cost,0) as total_receipts_cost,
	  coalesce(receipts_regular_retail,0) + coalesce(receipts_clearance_retail,0) + coalesce(receipts_crossdock_regular_retail,0) + coalesce(receipts_crossdock_clearance_retail,0) as total_receipts_retail,
	  coalesce(case when dropship_ind = 'Y' then coalesce(receipts_regular_units,0) + coalesce(receipts_clearance_units,0) + coalesce(receipts_crossdock_regular_units,0) + coalesce(receipts_crossdock_clearance_units,0) else 0 end,0) as dropship_receipts_units,
    coalesce(case when dropship_ind = 'Y' then coalesce(receipts_regular_cost,0) + coalesce(receipts_clearance_cost,0) + coalesce(receipts_crossdock_regular_cost,0) + coalesce(receipts_crossdock_clearance_cost,0) else 0 end,0) as dropship_receipts_cost,
    coalesce(case when dropship_ind = 'Y' then coalesce(receipts_regular_retail,0) + coalesce(receipts_clearance_retail,0) + coalesce(receipts_crossdock_regular_retail,0) + coalesce(receipts_crossdock_clearance_retail,0) else 0 end,0) as dropship_receipts_retail
	from prd_nap_base_vws.merch_poreceipt_sku_store_week_fact_vw as receipts
	inner join date_lkup as dt
		on receipts.week_num = dt.week_idnt_true
	inner join store_lkup as store
		on receipts.store_num = store.store_num
	inner join sku_lkup as sku
		on receipts.rms_sku_num = sku.rms_sku_num
  where store.channel_num <> 920
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on receipts_stg;

create volatile multiset table receipts_final
as (
	select
		ty_ly_ind,
		week_idnt,
		business_unit_desc,
		channel_num,
		channel_desc,
		country_code,
		banner,
		div_label,
	  sdiv_label,
	  dept_label,
	  dept_idnt,
	  supplier_number,
	  brand_name,
	  brand_label,
	  brand_label_num,
	  npg_ind,
	  supplier,
	  cast(0 as decimal(12,2)) as net_sales_tot_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as net_sales_tot_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_cost  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_regular_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_promo_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_clearance_units  ,
		cast(0 as decimal(12,2)) as returns_tot_retl ,
		cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
		cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
		cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as returns_tot_cost ,
		cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
		cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
		cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as returns_tot_units ,
		cast(0 as decimal(10,0)) as returns_tot_regular_units ,
		cast(0 as decimal(10,0)) as returns_tot_promo_units ,
		cast(0 as decimal(10,0)) as returns_tot_clearance_units,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_retl ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_cost ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_units,
		cast(0 as decimal(12,2)) as demand_tot_amt ,
		cast(0 as decimal(12,2)) as demand_regular_amt ,
		cast(0 as decimal(12,2)) as demand_promo_amt ,
		cast(0 as decimal(12,2)) as demand_clearence_amt ,
		cast(0 as decimal(12,2)) as demand_unknown_amt ,
		cast(0 as decimal(10,0)) as demand_tot_qty ,
		cast(0 as decimal(10,0)) as demand_regular_qty ,
		cast(0 as decimal(10,0)) as demand_promo_qty ,
		cast(0 as decimal(10,0)) as demand_clearence_qty ,
		cast(0 as decimal(10,0)) as demand_unknown_qty ,
		sum(total_receipts_units) as total_receipts_units,
		sum(total_receipts_cost) as total_receipts_cost,
		sum(total_receipts_retail) as total_receipts_retail,
		sum(dropship_receipts_units) as dropship_receipts_units,
    sum(dropship_receipts_cost) as dropship_receipts_cost,
    sum(dropship_receipts_retail) as dropship_receipts_retail,
		cast(0 as decimal(10,0)) as packandhold_transfer_in_units ,
		cast(0 as decimal(10,0)) as packandhold_transfer_out_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_in_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_out_units ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_cost ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_retail ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_retail ,
		cast(0 as decimal(12,2)) as disc_terms_cost,
		cast(0 as decimal(12,2)) as vendor_funds_retail,
		cast(0 as decimal(12,2)) as vendor_funds_cost,
		cast(0 as decimal(12,2)) as boh_total_cost,
		cast(0 as decimal(10,0)) as boh_total_units,
		cast(0 as decimal(12,2)) as boh_total_retail,
		cast(0 as decimal(12,2)) as boh_clearance_cost,
		cast(0 as decimal(10,0)) as boh_clearance_units,
		cast(0 as decimal(12,2)) as boh_clearance_retail,
		cast(0 as decimal(12,2)) as boh_regular_cost,
		cast(0 as decimal(10,0)) as boh_regular_units,
		cast(0 as decimal(12,2)) as boh_regular_retail,
	  cast(0 as decimal(12,2)) as eoh_total_cost,
	  cast(0 as decimal(10,0)) as eoh_total_units,
		cast(0 as decimal(12,2)) as eoh_total_retail,
	  cast(0 as decimal(12,2)) as eoh_clearance_cost,
	  cast(0 as decimal(10,0)) as eoh_clearance_units,
		cast(0 as decimal(12,2)) as eoh_clearance_retail,
	  cast(0 as decimal(12,2)) as eoh_regular_cost,
	  cast(0 as decimal(10,0)) as eoh_regular_units,
		cast(0 as decimal(12,2)) as eoh_regular_retail,
	  cast(0 as decimal(12,2)) as mos_total_cost_amt,
		cast(0 as decimal(10,0)) as mos_total_units,
		cast(0 as decimal(12,2)) as mos_total_retail_amt,
		cast(0 as decimal(12,2)) as rtv_total_cost,
		cast(0 as decimal(12,2)) as rtv_total_retail,
		cast(0 as decimal(10,0)) as rtv_total_units
	from receipts_stg
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on receipts_final;

create volatile multiset table transfer_stg
as (
	select
		dt.ty_ly_ind,
		dt.week_idnt,
		coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
		coalesce(store.channel_num,-1) as channel_num,
		coalesce(store.channel_desc,'N/A') as channel_desc,
		cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
		cast(coalesce(store.banner,'N/A') as char(3)) as banner,
		sku.div_label,
    sku.sdiv_label,
   	sku.dept_label,
    sku.dept_idnt,
    sku.supplier_number,
    sku.brand_name,
	  sku.brand_label,
	  sku.brand_label_num,
    sku.npg_ind,
    sku.supplier,
		coalesce(packandhold_transfer_in_units,0) as packandhold_transfer_in_units,
		coalesce(packandhold_transfer_out_units,0)as packandhold_transfer_out_units,
    coalesce(reservestock_transfer_in_units,0) as reservestock_transfer_in_units,
		coalesce(reservestock_transfer_out_units,0) as reservestock_transfer_out_units,
		coalesce(packandhold_transfer_in_retail,0) as packandhold_transfer_in_retail,
		coalesce(packandhold_transfer_out_retail,0) as packandhold_transfer_out_retail,
		coalesce(packandhold_transfer_in_cost,0) as packandhold_transfer_in_cost,
		coalesce(packandhold_transfer_out_cost,0) as packandhold_transfer_out_cost,
		coalesce(reservestock_transfer_in_cost,0) as reservestock_transfer_in_cost,
		coalesce(reservestock_transfer_out_cost,0) as reservestock_transfer_out_cost,
		coalesce(reservestock_transfer_in_retail,0) as reservestock_transfer_in_retail,
		coalesce(reservestock_transfer_out_retail,0) as reservestock_transfer_out_retail
	from prd_nap_base_vws.merch_transfer_sku_loc_week_agg_fact_vw transfer
	inner join date_lkup as dt
		on transfer.week_num = dt.week_idnt_true
	inner join store_lkup as store
		on transfer.store_num = store.store_num
	inner join sku_lkup as sku
		on transfer.rms_sku_num = sku.rms_sku_num
  where store.channel_num <> 920
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on transfer_stg;

create volatile multiset table transfer_final
as (
	select
		ty_ly_ind,
		week_idnt,
		business_unit_desc,
		channel_num,
		channel_desc,
		country_code,
		banner,
		div_label,
    sdiv_label,
   	dept_label,
    dept_idnt,
    supplier_number,
    brand_name,
	  brand_label,
	  brand_label_num,
	  npg_ind,
    supplier,
	  cast(0 as decimal(12,2)) as net_sales_tot_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as net_sales_tot_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_cost  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_regular_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_promo_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_clearance_units  ,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_retl ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_cost ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_units,
		cast(0 as decimal(12,2)) as demand_tot_amt ,
		cast(0 as decimal(12,2)) as demand_regular_amt ,
		cast(0 as decimal(12,2)) as demand_promo_amt ,
		cast(0 as decimal(12,2)) as demand_clearence_amt ,
		cast(0 as decimal(12,2)) as demand_unknown_amt ,
		cast(0 as decimal(10,0)) as demand_tot_qty ,
		cast(0 as decimal(10,0)) as demand_regular_qty ,
		cast(0 as decimal(10,0)) as demand_promo_qty ,
		cast(0 as decimal(10,0)) as demand_clearence_qty ,
		cast(0 as decimal(10,0)) as demand_unknown_qty ,
		cast(0 as decimal(10,0)) as total_receipts_units ,
		cast(0 as decimal(12,2)) as total_receipts_cost ,
		cast(0 as decimal(12,2)) as total_receipts_retail ,
		cast(0 as decimal(10,0)) as dropship_receipts_units ,
		cast(0 as decimal(12,2)) as dropship_receipts_cost ,
		cast(0 as decimal(12,2)) as dropship_receipts_retail ,
		sum(packandhold_transfer_in_units) as packandhold_transfer_in_units,
		sum(packandhold_transfer_out_units)as packandhold_transfer_out_units,
    sum(reservestock_transfer_in_units) as reservestock_transfer_in_units,
		sum(reservestock_transfer_out_units) as reservestock_transfer_out_units,
		sum(packandhold_transfer_in_retail) as packandhold_transfer_in_retail,
		sum(packandhold_transfer_out_retail) as packandhold_transfer_out_retail,
		sum(packandhold_transfer_in_cost) as packandhold_transfer_in_cost,
		sum(packandhold_transfer_out_cost) as packandhold_transfer_out_cost,
		sum(reservestock_transfer_in_cost) as reservestock_transfer_in_cost,
		sum(reservestock_transfer_out_cost) as reservestock_transfer_out_cost,
		sum(reservestock_transfer_in_retail) as reservestock_transfer_in_retail,
		sum(reservestock_transfer_out_retail) as reservestock_transfer_out_retail,
		cast(0 as decimal(12,2)) as disc_terms_cost,
		cast(0 as decimal(12,2)) as vendor_funds_retail,
		cast(0 as decimal(12,2)) as vendor_funds_cost,
		cast(0 as decimal(12,2)) as boh_total_cost,
		cast(0 as decimal(10,0)) as boh_total_units,
		cast(0 as decimal(12,2)) as boh_total_retail,
		cast(0 as decimal(12,2)) as boh_clearance_cost,
		cast(0 as decimal(10,0)) as boh_clearance_units,
		cast(0 as decimal(12,2)) as boh_clearance_retail,
		cast(0 as decimal(12,2)) as boh_regular_cost,
		cast(0 as decimal(10,0)) as boh_regular_units,
		cast(0 as decimal(12,2)) as boh_regular_retail,
	  cast(0 as decimal(12,2)) as eoh_total_cost,
	  cast(0 as decimal(10,0)) as eoh_total_units,
		cast(0 as decimal(12,2)) as eoh_total_retail,
	  cast(0 as decimal(12,2)) as eoh_clearance_cost,
	  cast(0 as decimal(10,0)) as eoh_clearance_units,
		cast(0 as decimal(12,2)) as eoh_clearance_retail,
	  cast(0 as decimal(12,2)) as eoh_regular_cost,
	  cast(0 as decimal(10,0)) as eoh_regular_units,
		cast(0 as decimal(12,2)) as eoh_regular_retail,
	  cast(0 as decimal(12,2)) as mos_total_cost_amt,
		cast(0 as decimal(10,0)) as mos_total_units,
		cast(0 as decimal(12,2)) as mos_total_retail_amt,
		cast(0 as decimal(12,2)) as rtv_total_cost,
		cast(0 as decimal(12,2)) as rtv_total_retail,
		cast(0 as decimal(10,0)) as rtv_total_units
	from transfer_stg
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on transfer_final;

create volatile multiset table vendor_terms_final
as (
	select
		dt.ty_ly_ind,
		dt.week_idnt,
		coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
		coalesce(store.channel_num,-1) as channel_num,
		coalesce(store.channel_desc,'N/A') as channel_desc,
		cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
		cast(coalesce(store.banner,'N/A') as char(3)) as banner,
		sku.div_label,
    sku.sdiv_label,
   	sku.dept_label,
    sku.dept_idnt,
    sku.supplier_number,
    sku.brand_name,
	  sku.brand_label,
	  sku.brand_label_num,
	  sku.npg_ind,
	  sku.supplier,
	  cast(0 as decimal(12,2)) as net_sales_tot_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as net_sales_tot_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_cost  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_regular_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_promo_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_clearance_units  ,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_retl ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_cost ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_units,
		cast(0 as decimal(12,2)) as demand_tot_amt ,
		cast(0 as decimal(12,2)) as demand_regular_amt ,
		cast(0 as decimal(12,2)) as demand_promo_amt ,
		cast(0 as decimal(12,2)) as demand_clearence_amt ,
		cast(0 as decimal(12,2)) as demand_unknown_amt ,
		cast(0 as decimal(10,0)) as demand_tot_qty ,
		cast(0 as decimal(10,0)) as demand_regular_qty ,
		cast(0 as decimal(10,0)) as demand_promo_qty ,
		cast(0 as decimal(10,0)) as demand_clearence_qty ,
		cast(0 as decimal(10,0)) as demand_unknown_qty ,
		cast(0 as decimal(10,0)) as total_receipts_units ,
		cast(0 as decimal(12,2)) as total_receipts_cost ,
		cast(0 as decimal(12,2)) as total_receipts_retail ,
		cast(0 as decimal(10,0)) as dropship_receipts_units ,
		cast(0 as decimal(12,2)) as dropship_receipts_cost ,
		cast(0 as decimal(12,2)) as dropship_receipts_retail ,
		cast(0 as decimal(10,0)) as packandhold_transfer_in_units ,
		cast(0 as decimal(10,0)) as packandhold_transfer_out_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_in_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_out_units ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_cost ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_retail ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_retail ,
	  sum(coalesce(disc_terms_cost,0)) as disc_terms_cost,
		cast(0 as decimal(12,2)) as vendor_funds_retail,
		cast(0 as decimal(12,2)) as vendor_funds_cost,
		cast(0 as decimal(12,2)) as boh_total_cost,
		cast(0 as decimal(10,0)) as boh_total_units,
		cast(0 as decimal(12,2)) as boh_total_retail,
		cast(0 as decimal(12,2)) as boh_clearance_cost,
		cast(0 as decimal(10,0)) as boh_clearance_units,
		cast(0 as decimal(12,2)) as boh_clearance_retail,
		cast(0 as decimal(12,2)) as boh_regular_cost,
		cast(0 as decimal(10,0)) as boh_regular_units,
		cast(0 as decimal(12,2)) as boh_regular_retail,
	  cast(0 as decimal(12,2)) as eoh_total_cost,
	  cast(0 as decimal(10,0)) as eoh_total_units,
		cast(0 as decimal(12,2)) as eoh_total_retail,
	  cast(0 as decimal(12,2)) as eoh_clearance_cost,
	  cast(0 as decimal(10,0)) as eoh_clearance_units,
		cast(0 as decimal(12,2)) as eoh_clearance_retail,
	  cast(0 as decimal(12,2)) as eoh_regular_cost,
	  cast(0 as decimal(10,0)) as eoh_regular_units,
		cast(0 as decimal(12,2)) as eoh_regular_retail,
	  cast(0 as decimal(12,2)) as mos_total_cost_amt,
		cast(0 as decimal(10,0)) as mos_total_units,
		cast(0 as decimal(12,2)) as mos_total_retail_amt,
		cast(0 as decimal(12,2)) as rtv_total_cost,
		cast(0 as decimal(12,2)) as rtv_total_retail,
		cast(0 as decimal(10,0)) as rtv_total_units
	from prd_nap_base_vws.merch_disc_terms_sku_loc_week_agg_fact_vw as terms
	inner join date_lkup as dt
		on terms.week_num = dt.week_idnt_true
	inner join store_lkup as store
		on terms.store_num = store.store_num
	inner join sku_lkup as sku
		on terms.rms_sku_num = sku.rms_sku_num
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on vendor_terms_final;

create volatile multiset table vendor_funds_stg
as (
	select
		dt.ty_ly_ind,
		dt.week_idnt,
		coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
		coalesce(store.channel_num,-1) as channel_num,
		coalesce(store.channel_desc,'N/A') as channel_desc,
		cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
		cast(coalesce(store.banner,'N/A') as char(3)) as banner,
		sku.div_label,
    sku.sdiv_label,
   	sku.dept_label,
    sku.dept_idnt,
    sku.supplier_number,
    sku.brand_name,
	  sku.brand_label,
	  sku.brand_label_num,
	  sku.npg_ind,
	  sku.supplier,
	  coalesce(vfm.vendor_funds_retail,0) as vendor_funds_retail,
		coalesce(vfm.vendor_funds_cost,0) as vendor_funds_cost
	from prd_nap_base_vws.merch_vendor_funds_sku_store_week_fact_vw vfm
	inner join date_lkup as dt
		on vfm.week_num = dt.week_idnt_true
	inner join store_lkup as store
		on vfm.store_num = store.store_num
	inner join sku_lkup as sku
		on vfm.rms_sku_num = sku.rms_sku_num
  where store.channel_num <> 920
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on vendor_funds_stg;

create volatile multiset table vfm_final
as (
	select
		ty_ly_ind,
		week_idnt,
		business_unit_desc,
		channel_num,
		channel_desc,
		country_code,
		banner,
		div_label,
	 	sdiv_label,
	  dept_label,
	  dept_idnt,
	  supplier_number,
	 	brand_name,
	 	brand_label,
	  brand_label_num,
	  npg_ind,
	  supplier,
	  cast(0 as decimal(12,2)) as net_sales_tot_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as net_sales_tot_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_cost  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_regular_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_promo_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_clearance_units  ,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_retl ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_cost ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_units,
		cast(0 as decimal(12,2)) as demand_tot_amt ,
		cast(0 as decimal(12,2)) as demand_regular_amt ,
		cast(0 as decimal(12,2)) as demand_promo_amt ,
		cast(0 as decimal(12,2)) as demand_clearence_amt ,
		cast(0 as decimal(12,2)) as demand_unknown_amt ,
		cast(0 as decimal(10,0)) as demand_tot_qty ,
		cast(0 as decimal(10,0)) as demand_regular_qty ,
		cast(0 as decimal(10,0)) as demand_promo_qty ,
		cast(0 as decimal(10,0)) as demand_clearence_qty ,
		cast(0 as decimal(10,0)) as demand_unknown_qty ,
		cast(0 as decimal(10,0)) as total_receipts_units ,
		cast(0 as decimal(12,2)) as total_receipts_cost ,
		cast(0 as decimal(12,2)) as total_receipts_retail ,
		cast(0 as decimal(10,0)) as dropship_receipts_units ,
		cast(0 as decimal(12,2)) as dropship_receipts_cost ,
		cast(0 as decimal(12,2)) as dropship_receipts_retail ,
		cast(0 as decimal(10,0)) as packandhold_transfer_in_units ,
		cast(0 as decimal(10,0)) as packandhold_transfer_out_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_in_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_out_units ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_cost ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_retail ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_retail ,
		cast(0 as decimal(12,2)) as disc_terms_cost,
		sum(vendor_funds_retail) as vendor_funds_retail,
		sum(vendor_funds_cost) as vendor_funds_cost,
		cast(0 as decimal(12,2)) as boh_total_cost,
		cast(0 as decimal(10,0)) as boh_total_units,
		cast(0 as decimal(12,2)) as boh_total_retail,
		cast(0 as decimal(12,2)) as boh_clearance_cost,
		cast(0 as decimal(10,0)) as boh_clearance_units,
		cast(0 as decimal(12,2)) as boh_clearance_retail,
		cast(0 as decimal(12,2)) as boh_regular_cost,
		cast(0 as decimal(10,0)) as boh_regular_units,
		cast(0 as decimal(12,2)) as boh_regular_retail,
	  cast(0 as decimal(12,2)) as eoh_total_cost,
	  cast(0 as decimal(10,0)) as eoh_total_units,
		cast(0 as decimal(12,2)) as eoh_total_retail,
	  cast(0 as decimal(12,2)) as eoh_clearance_cost,
	  cast(0 as decimal(10,0)) as eoh_clearance_units,
		cast(0 as decimal(12,2)) as eoh_clearance_retail,
	  cast(0 as decimal(12,2)) as eoh_regular_cost,
	  cast(0 as decimal(10,0)) as eoh_regular_units,
		cast(0 as decimal(12,2)) as eoh_regular_retail,
	  cast(0 as decimal(12,2)) as mos_total_cost_amt,
		cast(0 as decimal(10,0)) as mos_total_units,
		cast(0 as decimal(12,2)) as mos_total_retail_amt,
		cast(0 as decimal(12,2)) as rtv_total_cost,
		cast(0 as decimal(12,2)) as rtv_total_retail,
		cast(0 as decimal(10,0)) as rtv_total_units
	from vendor_funds_stg
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on vfm_final;

create volatile multiset table inventory_stg
as (
	select
		dt.ty_ly_ind,
		dt.week_idnt,
		coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
		coalesce(store.channel_num,-1) as channel_num,
		coalesce(store.channel_desc,'N/A') as channel_desc,
		cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
		cast(coalesce(store.banner,'N/A') as char(3)) as banner,
		sku.div_label,
    sku.sdiv_label,
   	sku.dept_label,
    sku.dept_idnt,
    sku.supplier_number,
    sku.brand_name,
	  sku.brand_label,
	  sku.brand_label_num,
	  sku.npg_ind,
	  sku.supplier,
		(coalesce(inventory.boh_total_cost,0)) as boh_total_cost,
		(coalesce(inventory.boh_total_units,0)) as boh_total_units,
		(coalesce(inventory.boh_total_retail,0)) as boh_total_retail,
		(coalesce(inventory.boh_clearance_cost,0)) as boh_clearance_cost,
		(coalesce(inventory.boh_clearance_units,0)) as boh_clearance_units,
		(coalesce(inventory.boh_clearance_retail,0)) as boh_clearance_retail,
		(coalesce(inventory.boh_regular_cost,0)) as boh_regular_cost,
		(coalesce(inventory.boh_regular_units,0)) as boh_regular_units,
		(coalesce(inventory.boh_regular_retail,0)) as boh_regular_retail,
		(coalesce(inventory.eoh_total_cost,0)) as eoh_total_cost,
		(coalesce(inventory.eoh_total_units,0)) as eoh_total_units,
		(coalesce(inventory.eoh_total_retail,0)) as eoh_total_retail,
		(coalesce(inventory.eoh_clearance_cost,0)) as eoh_clearance_cost,
		(coalesce(inventory.eoh_clearance_units,0)) as eoh_clearance_units,
		(coalesce(inventory.eoh_clearance_retail,0)) as eoh_clearance_retail,
		(coalesce(inventory.eoh_regular_cost,0)) as eoh_regular_cost,
		(coalesce(inventory.eoh_regular_units,0)) as eoh_regular_units,
		(coalesce(inventory.eoh_regular_retail,0)) as eoh_regular_retail
	from prd_nap_base_vws.merch_inventory_sku_store_week_fact_vw inventory
	inner join date_lkup as dt
		on inventory.week_num = dt.week_idnt_true
	inner join store_lkup as store
		on inventory.store_num = store.store_num
	inner join sku_lkup as sku
		on inventory.rms_sku_num = sku.rms_sku_num
  where store.channel_num <> 920
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on inventory_stg;

create volatile multiset table inventory_final
as (
	select
		ty_ly_ind,
		week_idnt,
		business_unit_desc,
		channel_num,
		channel_desc,
		country_code,
		banner,
		div_label,
    sdiv_label,
   	dept_label,
    dept_idnt,
    supplier_number,
    brand_name,
	  brand_label,
	  brand_label_num,
	  npg_ind,
	  supplier,
	  cast(0 as decimal(12,2)) as net_sales_tot_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as net_sales_tot_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_cost  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_regular_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_promo_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_clearance_units  ,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_retl ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_cost ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_units,
		cast(0 as decimal(12,2)) as demand_tot_amt ,
		cast(0 as decimal(12,2)) as demand_regular_amt ,
		cast(0 as decimal(12,2)) as demand_promo_amt ,
		cast(0 as decimal(12,2)) as demand_clearence_amt ,
		cast(0 as decimal(12,2)) as demand_unknown_amt ,
		cast(0 as decimal(10,0)) as demand_tot_qty ,
		cast(0 as decimal(10,0)) as demand_regular_qty ,
		cast(0 as decimal(10,0)) as demand_promo_qty ,
		cast(0 as decimal(10,0)) as demand_clearence_qty ,
		cast(0 as decimal(10,0)) as demand_unknown_qty ,
		cast(0 as decimal(10,0)) as total_receipts_units ,
		cast(0 as decimal(12,2)) as total_receipts_cost ,
		cast(0 as decimal(12,2)) as total_receipts_retail ,
		cast(0 as decimal(10,0)) as dropship_receipts_units ,
		cast(0 as decimal(12,2)) as dropship_receipts_cost ,
		cast(0 as decimal(12,2)) as dropship_receipts_retail ,
		cast(0 as decimal(10,0)) as packandhold_transfer_in_units ,
		cast(0 as decimal(10,0)) as packandhold_transfer_out_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_in_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_out_units ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_cost ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_retail ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_retail ,
		cast(0 as decimal(12,2)) as disc_terms_cost,
		cast(0 as decimal(12,2)) as vendor_funds_retail,
		cast(0 as decimal(12,2)) as vendor_funds_cost,
		sum(boh_total_cost) as boh_total_cost,
		sum(boh_total_units) as boh_total_units,
		sum(boh_total_retail) as boh_total_retail,
		sum(boh_clearance_cost) as boh_clearance_cost,
		sum(boh_clearance_units) as boh_clearance_units,
		sum(boh_clearance_retail) as boh_clearance_retail,
		sum(boh_regular_cost) as boh_regular_cost,
		sum(boh_regular_units) as boh_regular_units,
		sum(boh_regular_retail) as boh_regular_retail,
		sum(eoh_total_cost) as eoh_total_cost,
		sum(eoh_total_units) as eoh_total_units,
		sum(eoh_total_retail) as eoh_total_retail,
		sum(eoh_clearance_cost) as eoh_clearance_cost,
		sum(eoh_clearance_units) as eoh_clearance_units,
		sum(eoh_clearance_retail) as eoh_clearance_retail,
		sum(eoh_regular_cost) as eoh_regular_cost,
		sum(eoh_regular_units) as eoh_regular_units,
		sum(eoh_regular_retail) as eoh_regular_retail,
	  cast(0 as decimal(12,2)) as mos_total_cost_amt,
		cast(0 as decimal(10,0)) as mos_total_units,
		cast(0 as decimal(12,2)) as mos_total_retail_amt,
		cast(0 as decimal(12,2)) as rtv_total_cost,
		cast(0 as decimal(12,2)) as rtv_total_retail,
		cast(0 as decimal(10,0)) as rtv_total_units
	from inventory_stg
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on inventory_final;

create volatile multiset table mos_final
as (
	select
		dt.ty_ly_ind,
		dt.week_idnt,
		coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
		coalesce(store.channel_num,-1) as channel_num,
		coalesce(store.channel_desc,'N/A') as channel_desc,
		cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
		cast(coalesce(store.banner,'N/A') as char(3)) as banner,
		sku.div_label,
    sku.sdiv_label,
   	sku.dept_label,
    sku.dept_idnt,
    sku.supplier_number,
    sku.brand_name,
	  sku.brand_label,
	  sku.brand_label_num,
	  sku.npg_ind,
	  sku.supplier,
	  cast(0 as decimal(12,2)) as net_sales_tot_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as net_sales_tot_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_cost  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_regular_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_promo_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_clearance_units  ,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_retl ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_cost ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_units,
		cast(0 as decimal(12,2)) as demand_tot_amt ,
		cast(0 as decimal(12,2)) as demand_regular_amt ,
		cast(0 as decimal(12,2)) as demand_promo_amt ,
		cast(0 as decimal(12,2)) as demand_clearence_amt ,
		cast(0 as decimal(12,2)) as demand_unknown_amt ,
		cast(0 as decimal(10,0)) as demand_tot_qty ,
		cast(0 as decimal(10,0)) as demand_regular_qty ,
		cast(0 as decimal(10,0)) as demand_promo_qty ,
		cast(0 as decimal(10,0)) as demand_clearence_qty ,
		cast(0 as decimal(10,0)) as demand_unknown_qty ,
		cast(0 as decimal(10,0)) as total_receipts_units ,
		cast(0 as decimal(12,2)) as total_receipts_cost ,
		cast(0 as decimal(12,2)) as total_receipts_retail ,
		cast(0 as decimal(10,0)) as dropship_receipts_units ,
		cast(0 as decimal(12,2)) as dropship_receipts_cost ,
		cast(0 as decimal(12,2)) as dropship_receipts_retail ,
		cast(0 as decimal(10,0)) as packandhold_transfer_in_units ,
		cast(0 as decimal(10,0)) as packandhold_transfer_out_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_in_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_out_units ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_cost ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_retail ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_retail ,
	  cast(0 as decimal(12,2)) as  disc_terms_cost,
		cast(0 as decimal(12,2)) as vendor_funds_retail,
		cast(0 as decimal(12,2)) as vendor_funds_cost,
		cast(0 as decimal(12,2)) as boh_total_cost,
		cast(0 as decimal(10,0)) as boh_total_units,
		cast(0 as decimal(12,2)) as boh_total_retail,
		cast(0 as decimal(12,2)) as boh_clearance_cost,
		cast(0 as decimal(10,0)) as boh_clearance_units,
		cast(0 as decimal(12,2)) as boh_clearance_retail,
		cast(0 as decimal(12,2)) as boh_regular_cost,
		cast(0 as decimal(10,0)) as boh_regular_units,
		cast(0 as decimal(12,2)) as boh_regular_retail,
	  cast(0 as decimal(12,2)) as eoh_total_cost,
	  cast(0 as decimal(10,0)) as eoh_total_units,
		cast(0 as decimal(12,2)) as eoh_total_retail,
	  cast(0 as decimal(12,2)) as eoh_clearance_cost,
	  cast(0 as decimal(10,0)) as eoh_clearance_units,
		cast(0 as decimal(12,2)) as eoh_clearance_retail,
	  cast(0 as decimal(12,2)) as eoh_regular_cost,
	  cast(0 as decimal(10,0)) as eoh_regular_units,
		cast(0 as decimal(12,2)) as eoh_regular_retail,
		sum(coalesce(mos.mos_adjusted_total_cost,0)) as mos_total_cost_amt,
		sum(coalesce(mos.mos_adjusted_total_units,0)) as mos_total_units,
		sum(coalesce(mos.mos_adjusted_total_retail,0)) as mos_total_retail_amt,
		cast(0 as decimal(12,2)) as rtv_total_cost,
		cast(0 as decimal(12,2)) as rtv_total_retail,
		cast(0 as decimal(10,0)) as rtv_total_units
	from prd_nap_base_vws.merch_sp_mos_adjusted_sku_store_week_agg_fact mos
	inner join date_lkup as dt
		on mos.week_num = dt.week_idnt_true
	inner join store_lkup as store
		on mos.store_num = store.store_num
	inner join sku_lkup as sku
		on mos.rms_sku_num = sku.rms_sku_num
  where store.channel_num <> 920
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on mos_final;

create volatile multiset table rtv_final
as (
	select
		dt.ty_ly_ind,
		dt.week_idnt,
		coalesce(store.business_unit_desc,'N/A') as business_unit_desc,
		coalesce(store.channel_num,-1) as channel_num,
		coalesce(store.channel_desc,'N/A') as channel_desc,
		cast(coalesce(store.store_country_code,'N/A') as varchar(3)) as country_code,
		cast(coalesce(store.banner,'N/A') as char(3)) as banner,
		sku.div_label,
    sku.sdiv_label,
   	sku.dept_label,
    sku.dept_idnt,
    sku.supplier_number,
    sku.brand_name,
	  sku.brand_label,
	  sku.brand_label_num,
	  sku.npg_ind,
	  sku.supplier,
	  cast(0 as decimal(12,2)) as net_sales_tot_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_retl ,
		cast(0 as decimal(12,2)) as net_sales_tot_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_regular_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_promo_cost ,
		cast(0 as decimal(12,2)) as net_sales_tot_clearance_cost ,
		cast(0 as decimal(10,0)) as net_sales_tot_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_regular_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_promo_units ,
		cast(0 as decimal(10,0)) as net_sales_tot_clearance_units ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_retl  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_regular_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_promo_cost  ,
      cast(0 as decimal(18,2)) as net_sales_nx_adj_tot_clearance_cost  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_regular_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_promo_units  ,
      cast(0 as decimal(18,0)) as net_sales_nx_adj_tot_clearance_units  ,
      cast(0 as decimal(12,2)) as returns_tot_retl ,
      cast(0 as decimal(12,2)) as returns_tot_regular_retl ,
      cast(0 as decimal(12,2)) as returns_tot_promo_retl ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_retl ,
      cast(0 as decimal(12,2)) as returns_tot_cost ,
      cast(0 as decimal(12,2)) as returns_tot_regular_cost ,
      cast(0 as decimal(12,2)) as returns_tot_promo_cost ,
      cast(0 as decimal(12,2)) as returns_tot_clearance_cost ,
      cast(0 as decimal(10,0)) as returns_tot_units ,
      cast(0 as decimal(10,0)) as returns_tot_regular_units ,
      cast(0 as decimal(10,0)) as returns_tot_promo_units ,
      cast(0 as decimal(10,0)) as returns_tot_clearance_units,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_retl ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_cost ,
    cast(0 as decimal(10,0)) as dropship_net_sales_tot_units,
		cast(0 as decimal(12,2)) as demand_tot_amt ,
		cast(0 as decimal(12,2)) as demand_regular_amt ,
		cast(0 as decimal(12,2)) as demand_promo_amt ,
		cast(0 as decimal(12,2)) as demand_clearence_amt ,
		cast(0 as decimal(12,2)) as demand_unknown_amt ,
		cast(0 as decimal(10,0)) as demand_tot_qty ,
		cast(0 as decimal(10,0)) as demand_regular_qty ,
		cast(0 as decimal(10,0)) as demand_promo_qty ,
		cast(0 as decimal(10,0)) as demand_clearence_qty ,
		cast(0 as decimal(10,0)) as demand_unknown_qty ,
		cast(0 as decimal(10,0)) as total_receipts_units ,
		cast(0 as decimal(12,2)) as total_receipts_cost ,
		cast(0 as decimal(12,2)) as total_receipts_retail ,
		cast(0 as decimal(10,0)) as dropship_receipts_units ,
		cast(0 as decimal(12,2)) as dropship_receipts_cost ,
		cast(0 as decimal(12,2)) as dropship_receipts_retail ,
		cast(0 as decimal(10,0)) as packandhold_transfer_in_units ,
		cast(0 as decimal(10,0)) as packandhold_transfer_out_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_in_units ,
		cast(0 as decimal(10,0)) as reservestock_transfer_out_units ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_retail ,
		cast(0 as decimal(12,2)) as packandhold_transfer_in_cost ,
		cast(0 as decimal(12,2)) as packandhold_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_cost ,
		cast(0 as decimal(12,2)) as reservestock_transfer_in_retail ,
		cast(0 as decimal(12,2)) as reservestock_transfer_out_retail ,
	  cast(0 as decimal(12,2)) as disc_terms_cost,
		cast(0 as decimal(12,2)) as vendor_funds_retail,
		cast(0 as decimal(12,2)) as vendor_funds_cost,
		cast(0 as decimal(12,2)) as boh_total_cost,
		cast(0 as decimal(10,0)) as boh_total_units,
		cast(0 as decimal(12,2)) as boh_total_retail,
		cast(0 as decimal(12,2)) as boh_clearance_cost,
		cast(0 as decimal(10,0)) as boh_clearance_units,
		cast(0 as decimal(12,2)) as boh_clearance_retail,
		cast(0 as decimal(12,2)) as boh_regular_cost,
		cast(0 as decimal(10,0)) as boh_regular_units,
		cast(0 as decimal(12,2)) as boh_regular_retail,
	  cast(0 as decimal(12,2)) as eoh_total_cost,
	  cast(0 as decimal(10,0)) as eoh_total_units,
		cast(0 as decimal(12,2)) as eoh_total_retail,
	  cast(0 as decimal(12,2)) as eoh_clearance_cost,
	  cast(0 as decimal(10,0)) as eoh_clearance_units,
		cast(0 as decimal(12,2)) as eoh_clearance_retail,
	  cast(0 as decimal(12,2)) as eoh_regular_cost,
	  cast(0 as decimal(10,0)) as eoh_regular_units,
		cast(0 as decimal(12,2)) as eoh_regular_retail,
	  cast(0 as decimal(12,2)) as mos_total_cost_amt,
		cast(0 as decimal(10,0)) as mos_total_units,
		cast(0 as decimal(12,2)) as mos_total_retail_amt,
		sum(coalesce(rtv_total_cost,0)) as rtv_total_cost,
	  sum(coalesce(rtv_total_retail,0)) as rtv_total_retail,
		sum(coalesce(rtv_total_units,0)) as rtv_total_units
	from prd_nap_base_vws.merch_return_to_vendor_sku_loc_week_agg_fact_vw as rtv
	inner join date_lkup as dt
		on rtv.week_num = dt.week_idnt_true
	inner join store_lkup as store
		on rtv.store_num = store.store_num
	inner join sku_lkup as sku
		on rtv.rms_sku_num = sku.rms_sku_num
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
) with data primary index (week_idnt, channel_num, supplier_number, dept_idnt) on commit preserve rows;

collect stats primary index(week_idnt, channel_num, supplier_number, dept_idnt),
	column(supplier_number),
	column(week_idnt),
	column(dept_idnt),
	column(channel_num) on rtv_final;


delete from {dsa_ai_secure_schema}.supplier_profitability_expectation_channel_week_dept_agg
;

insert into {dsa_ai_secure_schema}.supplier_profitability_expectation_channel_week_dept_agg
	select
		base.ty_ly_ind,
		base.week_idnt,
		d.fiscal_year_num ,
		d.fiscal_halfyear_num ,
		d.quarter_abrv ,
		d.quarter_label ,
		d.month_abrv ,
		d.month_idnt ,
		d.month_label ,
		d.week_label ,
		d.week_desc ,
		d.fiscal_week_num ,
		d.week_start_day_date ,
		d.week_end_day_date ,
		base.business_unit_desc,
		base.channel_num,
		base.channel_desc,
		base.country_code,
		base.banner,
		base.div_label,
	  base.sdiv_label,
	  base.dept_label,
	  base.dept_idnt,
	  base.supplier_number,
	  base.brand_name,
	  base.brand_label,
	  base.brand_label_num,
	  base.npg_ind,
    coalesce(sb.sb_ind, 'N') as sb_ind,
	  base.supplier,
	  coalesce(supp.supplier_group, 'N/A') as supplier_group,
		sum(net_sales_tot_retl) as net_sales_tot_retl ,
		sum(net_sales_tot_regular_retl) as net_sales_tot_regular_retl ,
		sum(net_sales_tot_promo_retl) as net_sales_tot_promo_retl ,
		sum(net_sales_tot_clearance_retl) as net_sales_tot_clearance_retl ,
		sum(net_sales_tot_cost) as net_sales_tot_cost ,
		sum(net_sales_tot_regular_cost) as net_sales_tot_regular_cost ,
		sum(net_sales_tot_promo_cost) as net_sales_tot_promo_cost ,
		sum(net_sales_tot_clearance_cost) as net_sales_tot_clearance_cost ,
		sum(net_sales_tot_units) as net_sales_tot_units ,
		sum(net_sales_tot_regular_units) as net_sales_tot_regular_units ,
		sum(net_sales_tot_promo_units) as net_sales_tot_promo_units ,
		sum(net_sales_tot_clearance_units) as net_sales_tot_clearance_units ,
		sum(base.net_sales_nx_adj_tot_retl ) as net_sales_nx_adj_tot_retl  ,
		sum(base.net_sales_nx_adj_tot_regular_retl ) as net_sales_nx_adj_tot_regular_retl  ,
		sum(base.net_sales_nx_adj_tot_promo_retl ) as net_sales_nx_adj_tot_promo_retl  ,
		sum(base.net_sales_nx_adj_tot_clearance_retl ) as net_sales_nx_adj_tot_clearance_retl  ,
		sum(base.net_sales_nx_adj_tot_cost ) as net_sales_nx_adj_tot_cost  ,
		sum(base.net_sales_nx_adj_tot_regular_cost ) as net_sales_nx_adj_tot_regular_cost  ,
		sum(base.net_sales_nx_adj_tot_promo_cost ) as net_sales_nx_adj_tot_promo_cost  ,
		sum(base.net_sales_nx_adj_tot_clearance_cost ) as net_sales_nx_adj_tot_clearance_cost  ,
		sum(base.net_sales_nx_adj_tot_units ) as net_sales_nx_adj_tot_units  ,
		sum(base.net_sales_nx_adj_tot_regular_units ) as net_sales_nx_adj_tot_regular_units  ,
		sum(base.net_sales_nx_adj_tot_promo_units ) as net_sales_nx_adj_tot_promo_units  ,
		sum(base.net_sales_nx_adj_tot_clearance_units ) as net_sales_nx_adj_tot_clearance_units  ,
      sum(returns_tot_retl) as returns_tot_retl ,
      sum(returns_tot_regular_retl) as returns_tot_regular_retl ,
      sum(returns_tot_promo_retl) as returns_tot_promo_retl ,
      sum(returns_tot_clearance_retl) as returns_tot_clearance_retl ,
      sum(returns_tot_cost) as returns_tot_cost ,
      sum(returns_tot_regular_cost) as returns_tot_regular_cost ,
      sum(returns_tot_promo_cost) as returns_tot_promo_cost ,
      sum(returns_tot_clearance_cost) as returns_tot_clearance_cost ,
      sum(returns_tot_units) as returns_tot_units ,
      sum(returns_tot_regular_units) as returns_tot_regular_units ,
      sum(returns_tot_promo_units) as returns_tot_promo_units ,
      sum(returns_tot_clearance_units) as returns_tot_clearance_units ,
    sum(dropship_net_sales_tot_retl) as dropship_net_sales_tot_retl,
    sum(dropship_net_sales_tot_cost) as dropship_net_sales_tot_cost,
    sum(dropship_net_sales_tot_units) as dropship_net_sales_tot_units,
		sum(demand_tot_amt) as demand_tot_amt ,
		sum(demand_regular_amt) as demand_regular_amt ,
		sum(demand_promo_amt) as demand_promo_amt ,
		sum(demand_clearence_amt) as demand_clearence_amt ,
		sum(demand_unknown_amt) as demand_unknown_amt ,
		sum(demand_tot_qty) as demand_tot_qty ,
		sum(demand_regular_qty) as demand_regular_qty ,
		sum(demand_promo_qty) as demand_promo_qty ,
		sum(demand_clearence_qty) as demand_clearence_qty ,
		sum(demand_unknown_qty) as demand_unknown_qty ,
		sum(base.total_receipts_units) as total_receipts_units,
		sum(base.total_receipts_cost) as total_receipts_cost,
		sum(base.total_receipts_retail) as total_receipts_retail,
		sum(dropship_receipts_units) as dropship_receipts_units,
    sum(dropship_receipts_cost) as dropship_receipts_cost,
    sum(dropship_receipts_retail) as dropship_receipts_retail,
		sum(base.packandhold_transfer_in_units) as packandhold_transfer_in_units,
		sum(base.packandhold_transfer_out_units) as packandhold_transfer_out_units,
		sum(base.reservestock_transfer_in_units) as reservestock_transfer_in_units,
		sum(base.reservestock_transfer_out_units) as reservestock_transfer_out_units,
		sum(base.packandhold_transfer_in_retail) as packandhold_transfer_in_retail,
		sum(base.packandhold_transfer_out_retail) as packandhold_transfer_out_retail,
		sum(base.packandhold_transfer_in_cost) as packandhold_transfer_in_cost,
		sum(base.packandhold_transfer_out_cost) as packandhold_transfer_out_cost,
		sum(base.reservestock_transfer_in_cost) as reservestock_transfer_in_cost,
		sum(base.reservestock_transfer_out_cost) as reservestock_transfer_out_cost,
		sum(base.reservestock_transfer_in_retail) as reservestock_transfer_in_retail,
		sum(base.reservestock_transfer_out_retail) as reservestock_transfer_out_retail,
		sum(base.disc_terms_cost) as disc_terms_cost,
		sum(vendor_funds_retail) as vendor_funds_retail,
		sum(vendor_funds_cost) as vendor_funds_cost,
	  sum(base.boh_total_cost) as boh_total_cost ,
		sum(base.boh_total_units) as boh_total_units ,
		sum(base.boh_total_retail) as boh_total_retail ,
		sum(base.boh_clearance_cost) as boh_clearance_cost ,
		sum(base.boh_clearance_units) as boh_clearance_units ,
		sum(base.boh_clearance_retail) as boh_clearance_retail ,
	  sum(base.boh_regular_cost) as boh_regular_cost ,
		sum(base.boh_regular_units) as boh_regular_units ,
		sum(base.boh_regular_retail) as boh_regular_retail ,
		sum(base.eoh_total_cost) as eoh_total_cost ,
		sum(base.eoh_total_units) as eoh_total_units ,
		sum(base.eoh_total_retail) as eoh_total_retail ,
		sum(base.eoh_clearance_cost) as eoh_clearance_cost ,
		sum(base.eoh_clearance_units) as eoh_clearance_units ,
		sum(base.eoh_clearance_retail) as eoh_clearance_retail ,
		sum(base.eoh_regular_cost) as eoh_regular_cost ,
		sum(base.eoh_regular_units) as eoh_regular_units ,
		sum(base.eoh_regular_retail) as eoh_regular_retail ,
		sum(base.mos_total_cost_amt) as mos_total_cost_amt ,
		sum(base.mos_total_units) as mos_total_units ,
		sum(base.mos_total_retail_amt) as mos_total_retail_amt ,
		sum(base.rtv_total_cost) as rtv_total_cost ,
		sum(base.rtv_total_retail) as rtv_total_retail ,
		sum(base.rtv_total_units) as rtv_total_units ,
  	max(coalesce(s.ty_profitability_expectation, 0)) as profitability_expectation ,
    max(coalesce(s.ty_reg_sales_expectation, 0)) as reg_sales_expectation ,
    max(coalesce(s.ty_initial_markup_expectation, 0)) as initial_markup_expectation ,
    current_timestamp as process_tmstp
	from (
		select	*
		from sales_final
		union all
		select *
		from demand_final
		union all
		select *
		from inventory_final
		union all
		select *
		from receipts_final
		union all
		select *
		from transfer_final
		union all
		select *
		from vendor_terms_final
		union all
		select *
		from mos_final
		union all
		select *
		from vfm_final
		union all
		select *
		from rtv_final) base
	join date_lkup as d
		on base.week_idnt = d.week_idnt
	left join supplier_lkup supp
		on base.supplier_number = supp.supplier_num
		and base.dept_idnt = supp.dept_num
		and base.banner = supp.banner
  left join supp_prof s
  	on base.dept_idnt = s.dept_num
		and supp.supplier_group = s.supplier_group
		and base.banner = s.banner
		and d.fiscal_year_num = s.fiscal_year_num
  left join sb_lkup sb
    on base.banner = sb.banner
    and supp.supplier_group = sb.supplier_group
    and base.supplier_number = sb.supplier_num
    and base.dept_idnt = sb.dept_num
    and base.brand_name = sb.vendor_brand_name
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 
	;

-- stats defined on DDL so will be auto-collected by Teradata
