/*
Name: Daily Sales and Profitability
Author: Meghan Hickey
Modified By: Asiyah Fox, Alli Moore, Tanner Moxcey
Date Created:5/3/23
Date Last Updated: 6/5/24  -- updated to add D990 for Plan Data
*/

/* 1. Create Date Lookup for rolling 5 fiscal weeks + current week */
create multiset volatile table date_filter as (
    select
        a.day_idnt
        , a.fiscal_year_num
        , a.fiscal_day_num
        , a.day_num_of_fiscal_week
        , a.day_date
        , a.day_desc
        , a.fiscal_week_num
        , a.week_desc
        , a.week_idnt
        , a.fiscal_month_num
        , a.month_idnt
        , a.month_desc
        , a.fiscal_quarter_num
        , a.quarter_idnt
        , a.ty_ly_lly_ind as ty_ly_ind
        , case when a.week_idnt = (select week_idnt from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE - 1) and CURRENT_DATE < a.week_end_day_date then 0 else 1 end as elapsed_week_ind
        , case when a.day_date > CURRENT_DATE - 1 then 0 else 1 end as elapsed_day_ind
        , case when a.fiscal_week_num = (select fiscal_week_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE - 1) then 1 else 0 end as wtd
        , case when a.fiscal_month_num = (select fiscal_month_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE - 1) then 1 else 0 end as mtd
        , case when a.fiscal_day_num = (select fiscal_day_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE - 1) then 1 else 0 end as max_day_elapsed
        , case when a.fiscal_day_num = (select fiscal_day_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE - 1) or a.day_date = a.week_end_day_date then 1 else 0 end as max_day_elapsed_week
        , case when a.fiscal_day_num = (select fiscal_day_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE - 1) or a.day_date = a.month_end_day_date then 1 else 0 end as max_day_elapsed_month
        , case when a.fiscal_day_num = (select fiscal_day_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE - 1) or a.day_date = a.quarter_end_day_date then 1 else 0 end as max_day_elapsed_quarter
        , b.day_idnt as true_day_idnt
    from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW a
    left join prd_nap_usr_vws.day_cal_454_dim b
      on b.day_date = a.day_date
    where a.ty_ly_lly_ind in ('TY', 'LY') 
      and (a.fiscal_month_num = (select distinct fiscal_month_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 7) 
          or a.fiscal_month_num = (select distinct fiscal_month_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = current_date - 1)
        )
      and a.fiscal_day_num <= (select fiscal_day_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE-1) --remove to show full current week even if hasn't elapsed
) with data primary index (day_idnt, day_date, week_idnt) on commit preserve rows
;

collect stats primary index(day_idnt, day_date, week_idnt) on date_filter;


/* 2.a. Create Store Lookup for Channels Needed*/
create multiset volatile table store_lkup as (
    select
        distinct store_num
        , channel_num
        , channel_desc
        , store_country_code
        , case when channel_num in (110, 120, 310) then 'NORDSTROM'
              when channel_num in (210, 240, 250, 260) then 'NORDSTROM RACK'
         	  end as banner
    from prd_nap_usr_vws.store_dim
    where channel_num in (110, 120, 210, 250, 260, 310)
) with data primary index (store_num) on commit preserve rows
;

collect stats primary index(store_num) on store_lkup;

/* 2.b. Create Channel Lookup*/
create multiset volatile table channel_lkup as (
    select
        distinct channel_num
        , trim(channel_num || ', ' || channel_desc) as channel
        , case when channel_num in (110, 120, 310) then 'NORDSTROM'
              when channel_num in (210, 240, 250, 260) then 'NORDSTROM RACK'
         	  end as banner
        , store_country_code
    from prd_nap_usr_vws.store_dim
    where channel_num in (110, 120, 210, 250, 260, 310)
) with data primary index (channel_num) on commit preserve rows
;

collect stats primary index(channel_num) on channel_lkup;

/* 2.c. Create Department Lookup*/
create multiset volatile table dept_lkup as (
    select
        distinct dept_num
        , trim(division_num || ', ' || division_name) as division
        , trim(subdivision_num || ', ' || subdivision_name) as subdivision
        , trim(dept_num || ', ' || dept_name) as department
    from PRD_NAP_USR_VWS.DEPARTMENT_DIM
) with data primary index (dept_num) on commit preserve rows
;

collect stats primary index(dept_num) on dept_lkup;


/* 3. AOR Lookup */
--DROP TABLE aor;
CREATE MULTISET VOLATILE TABLE aor AS (
SELECT DISTINCT
	REGEXP_REPLACE(channel_brand,'_',' ') AS banner
	,dept_num
	,general_merch_manager_executive_vice_president
	,div_merch_manager_senior_vice_president
	,div_merch_manager_vice_president
	,merch_director
	,buyer
	,merch_planning_executive_vice_president
	,merch_planning_senior_vice_president
	,merch_planning_vice_president
	,merch_planning_director_manager
	,assortment_planner
FROM prd_nap_usr_vws.area_of_responsibility_dim
QUALIFY ROW_NUMBER() OVER (PARTITION BY banner, dept_num ORDER BY 3,4,5,6,7,8,9,10,11,12) = 1
) WITH DATA
	PRIMARY INDEX (banner,dept_num)
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (banner,dept_num)
		ON aor
;

/* 4. EOH by Day */
create multiset volatile table eoh as (
    select
        day_idnt as day_num
        , fiscal_day_num
        , ty_ly_ind
        , sku_idnt as rms_sku_num
        , loc_idnt as store_num
        , 0 AS net_sales_r
        , 0 AS net_sales_c
        , 0 AS net_sales_u
        , 0 AS net_sales_reg_r
        , 0 AS net_sales_reg_c
        , 0 AS net_sales_reg_u
        , 0 AS net_sales_pro_r
        , 0 AS net_sales_pro_c
        , 0 AS net_sales_pro_u
        , 0 AS net_sales_clear_r
        , 0 AS net_sales_clear_c
        , 0 AS net_sales_clear_u
        , cast(0 as DECIMAL(12,2)) as demand
        , cast(0 as DECIMAL(10,0)) as demand_u
        , sum(eoh_r) as eoh_r
        , sum(eoh_c) as eoh_c
        , sum(eoh_u) as eoh_u
        , cast(0 as DECIMAL(12,2)) as in_transit_r
        , cast(0 as DECIMAL(12,2)) as in_transit_c
        , cast(0 as DECIMAL(10,0)) as in_transit_u
    from (
    select
        day_dt
        , sku_idnt
        , loc_idnt
        , (eoh_dollars) as eoh_r
        , (eoh_units * weighted_average_cost) as eoh_c
        , (eoh_units) as eoh_u
    from t2dl_das_ace_mfp.sku_loc_pricetype_day a
    where (a.day_dt between (select min(day_date) from date_filter where ty_ly_ind = 'LY')
            and (select max(day_date) from date_filter where ty_ly_ind = 'LY')
      or a.day_dt between (select min(day_date) from date_filter where ty_ly_ind = 'TY')
            and (select max(day_date) from date_filter where ty_ly_ind = 'TY')
      ) and loc_idnt < 1000
      and eoh_units > 0
    ) a
    inner join date_filter b
      on a.day_dt = b.day_date
    inner join store_lkup st
    	on a.loc_idnt = st.store_num
    	and st.store_country_code = 'US'
    group by 1,2,3,4,5
) with data primary index (day_num, rms_sku_num, store_num) on commit preserve rows
;

collect stats primary index(day_num, rms_sku_num, store_num) on eoh;


/* 5. In transit Units -> Retail -> Cost */
create multiset volatile table it_base as (
    select
        rms_sku_id as rms_sku_num
      	, snapshot_date
      	, location_id as store_num
      	, coalesce(in_transit_qty,0) as in_transit_u
      	, price_store_num
        , cast(a.snapshot_date as timestamp) as snapshot_time
    from prd_nap_usr_vws.inventory_stock_quantity_by_day_fact a
    inner join store_lkup b
      on a.location_id = b.store_num
    inner join prd_nap_usr_vws.price_store_dim_vw c
      on b.store_num = c.store_num
    where (snapshot_date between (select min(day_date) from date_filter where ty_ly_ind = 'LY')
            and (select max(day_date) from date_filter where ty_ly_ind = 'LY')
      or snapshot_date between (select min(day_date) from date_filter where ty_ly_ind = 'TY')
            and (select max(day_date) from date_filter where ty_ly_ind = 'TY')
    ) --and location_type not in ('DS', 'DS_OP')
      and in_transit_qty > 0
) with data primary index (snapshot_date, rms_sku_num, store_num, price_store_num, snapshot_time) on commit preserve rows
;

collect stats primary index(snapshot_date, rms_sku_num, store_num, price_store_num, snapshot_time) on it_base;


create multiset volatile table it_price as (
    select
        a.*
      	, (in_transit_u * b.ownership_retail_price_amt) in_transit_r
    from it_base a
    left join prd_nap_usr_vws.product_price_timeline_dim b
			on a.rms_sku_num = b.rms_sku_num
				and a.price_store_num = b.store_num
				and a.snapshot_time between b.EFF_BEGIN_TMSTP AND b.EFF_END_TMSTP - interval '0.001' second
) with data primary index (snapshot_date, rms_sku_num, store_num) on commit preserve rows
;

collect stats primary index(snapshot_date, rms_sku_num, store_num) on it_price;


create multiset volatile table it_cost as (
    select
        day_idnt as day_num
        , fiscal_day_num
        , ty_ly_ind
        , rms_sku_num
        , cast(store_num as int) as store_num
        , 0 AS net_sales_r
        , 0 AS net_sales_c
        , 0 AS net_sales_u
        , 0 AS net_sales_reg_r
        , 0 AS net_sales_reg_c
        , 0 AS net_sales_reg_u
        , 0 AS net_sales_pro_r
        , 0 AS net_sales_pro_c
        , 0 AS net_sales_pro_u
        , 0 AS net_sales_clear_r
        , 0 AS net_sales_clear_c
        , 0 AS net_sales_clear_u
        , cast(0 as DECIMAL(12,2)) as demand
        , cast(0 as DECIMAL(10,0)) as demand_u
        , 0 as eoh_r
        , 0 as eoh_c
        , 0 as eoh_u
        , sum(in_transit_r) as in_transit_r
        , sum(in_transit_c) as in_transit_c
        , sum(in_transit_u) as in_transit_u
    from (
        select
            a.rms_sku_num
            , a.snapshot_date
            , a.store_num
            , a.in_transit_u
            , a.in_transit_r
          	, (a.in_transit_u * b.weighted_average_cost) in_transit_c
        from it_price a
        left join PRD_NAP_USR_VWS.WEIGHTED_AVERAGE_COST_DATE_DIM b
    			on a.rms_sku_num = b.sku_num
    				and a.store_num = b.location_num
    				and a.snapshot_date between b.eff_begin_dt and eff_end_dt - 1
    ) a
    inner join date_filter c
        on a.snapshot_date = c.day_date
    group by 1, 2, 3, 4, 5
) with data primary index (day_num, rms_sku_num, store_num) on commit preserve rows
;

collect stats primary index(day_num, rms_sku_num, store_num) on it_cost;


/* 6. Sales */
create multiset volatile table sales as (
    select
        a.day_num
        , fiscal_day_num
        , ty_ly_ind
        , a.rms_sku_num
        , a.store_num
        , jwn_operational_gmv_total_retail_amt AS net_sales_r
        , jwn_operational_gmv_total_cost_amt AS net_sales_c
        , jwn_operational_gmv_total_units AS net_sales_u
        , jwn_operational_gmv_regular_retail_amt AS net_sales_reg_r
        , jwn_operational_gmv_regular_cost_amt AS net_sales_reg_c
        , jwn_operational_gmv_regular_units AS net_sales_reg_u
        , jwn_operational_gmv_promo_retail_amt AS net_sales_pro_r
        , jwn_operational_gmv_promo_cost_amt AS net_sales_pro_c
        , jwn_operational_gmv_promo_units AS net_sales_pro_u
        , jwn_operational_gmv_clearance_retail_amt AS net_sales_clear_r
        , jwn_operational_gmv_clearance_cost_amt AS net_sales_clear_c
        , jwn_operational_gmv_clearance_units AS net_sales_clear_u
        , cast(0 as DECIMAL(12,2)) as demand
        , cast(0 as DECIMAL(10,0)) as demand_u
        , cast(0 as DECIMAL(12,2)) as eoh_r
        , cast(0 as DECIMAL(12,2)) as eoh_c
        , cast(0 as DECIMAL(10,0)) as eoh_u
        , cast(0 as DECIMAL(12,2)) as in_transit_r
        , cast(0 as DECIMAL(12,2)) as in_transit_c
        , cast(0 as DECIMAL(10,0)) as in_transit_u
    from PRD_NAP_USR_VWS.MERCH_JWN_SALE_RETURN_SKU_STORE_DAY_FACT_VW a
    inner join date_filter b
      on a.day_num = b.true_day_idnt
    inner join store_lkup st
    	on a.store_num = st.store_num
    	and st.store_country_code = 'US'
) with data primary index (day_num, rms_sku_num, store_num) on commit preserve rows
;

collect stats primary index(day_num, rms_sku_num, store_num) on sales;


/* 7. Demand */
create multiset volatile table demand as (
    select
        a.day_num
        , fiscal_day_num
        , ty_ly_ind
        , a.rms_sku_num
        , a.store_num
        , 0 AS net_sales_r
        , 0 AS net_sales_c
        , 0 AS net_sales_u
        , 0 AS net_sales_reg_r
        , 0 AS net_sales_reg_c
        , 0 AS net_sales_reg_u
        , 0 AS net_sales_pro_r
        , 0 AS net_sales_pro_c
        , 0 AS net_sales_pro_u
        , 0 AS net_sales_clear_r
        , 0 AS net_sales_clear_c
        , 0 AS net_sales_clear_u
        , jwn_demand_total_retail_amt AS demand
        , jwn_demand_total_units AS demand_u
        , cast(0 as DECIMAL(12,2)) as eoh_r
        , cast(0 as DECIMAL(12,2)) as eoh_c
        , cast(0 as DECIMAL(10,0)) as eoh_u
        , cast(0 as DECIMAL(12,2)) as in_transit_r
        , cast(0 as DECIMAL(12,2)) as in_transit_c
        , cast(0 as DECIMAL(10,0)) as in_transit_u
    from PRD_NAP_USR_VWS.MERCH_JWN_DEMAND_SKU_STORE_DAY_FACT_VW a
    inner join date_filter b
      on a.day_num = b.true_day_idnt
    inner join store_lkup st
    	on a.store_num = st.store_num
    	and st.store_country_code = 'US'
) with data primary index (day_num, rms_sku_num, store_num) on commit preserve rows
;

collect stats primary index(day_num, rms_sku_num, store_num) on demand;


/* 8. Combine all actuals */
create multiset volatile table combine as (
    select * from sales
    union all
    select * from demand
    union all
    select * from eoh
    union all
    select * from it_cost
) with data primary index (day_num, fiscal_day_num, rms_sku_num, store_num) on commit preserve rows
;

collect stats primary index(day_num, fiscal_day_num, rms_sku_num, store_num) on combine;


/* 9. Aggregate actuals */
create multiset volatile table actuals as (
    select
          st.channel_num
        , sku.dept_num
        , dy.day_date
        , sum(case when ty_ly_ind = 'TY' then net_sales_r else 0 end) AS net_sales_r_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_c else 0 end) AS net_sales_c_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_u else 0 end) AS net_sales_u_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_reg_r else 0 end) AS net_sales_reg_r_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_reg_c else 0 end) AS net_sales_reg_c_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_reg_u else 0 end) AS net_sales_reg_u_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_pro_r else 0 end) AS net_sales_pro_r_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_pro_c else 0 end) AS net_sales_pro_c_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_pro_u else 0 end) AS net_sales_pro_u_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_clear_r else 0 end) AS net_sales_clear_r_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_clear_c else 0 end) AS net_sales_clear_c_ty
        , sum(case when ty_ly_ind = 'TY' then net_sales_clear_u else 0 end) AS net_sales_clear_u_ty
        , sum(case when ty_ly_ind = 'TY' then demand else 0 end) AS demand_ty
        , sum(case when ty_ly_ind = 'TY' then demand_u else 0 end) AS demand_u_ty
        , sum(case when ty_ly_ind = 'TY' then eoh_r else 0 end) + sum(case when ty_ly_ind = 'TY' then in_transit_r else 0 end) AS eoh_r_ty
        , sum(case when ty_ly_ind = 'TY' then eoh_c else 0 end) + sum(case when ty_ly_ind = 'TY' then in_transit_c else 0 end) AS eoh_c_ty
        , sum(case when ty_ly_ind = 'TY' then eoh_u else 0 end) + sum(case when ty_ly_ind = 'TY' then in_transit_u else 0 end) AS eoh_u_ty
        , sum(case when ty_ly_ind = 'LY' then net_sales_r else 0 end) AS net_sales_r_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_c else 0 end) AS net_sales_c_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_u else 0 end) AS net_sales_u_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_reg_r else 0 end) AS net_sales_reg_r_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_reg_c else 0 end) AS net_sales_reg_c_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_reg_u else 0 end) AS net_sales_reg_u_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_pro_r else 0 end) AS net_sales_pro_r_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_pro_c else 0 end) AS net_sales_pro_c_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_pro_u else 0 end) AS net_sales_pro_u_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_clear_r else 0 end) AS net_sales_clear_r_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_clear_c else 0 end) AS net_sales_clear_c_ly
        , sum(case when ty_ly_ind = 'LY' then net_sales_clear_u else 0 end) AS net_sales_clear_u_ly
        , sum(case when ty_ly_ind = 'LY' then demand else 0 end) AS demand_ly
        , sum(case when ty_ly_ind = 'LY' then demand_u else 0 end) AS demand_u_ly
        , sum(case when ty_ly_ind = 'LY' then eoh_r else 0 end) + sum(case when ty_ly_ind = 'LY' then in_transit_r else 0 end) AS eoh_r_ly
        , sum(case when ty_ly_ind = 'LY' then eoh_c else 0 end) + sum(case when ty_ly_ind = 'LY' then in_transit_c else 0 end) AS eoh_c_ly
        , sum(case when ty_ly_ind = 'LY' then eoh_u else 0 end) + sum(case when ty_ly_ind = 'LY' then in_transit_u else 0 end) AS eoh_u_ly
    from combine base
    inner join store_lkup st
    	on base.store_num = st.store_num
    	and st.store_country_code = 'US'
    inner join PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW dy
    	on base.fiscal_day_num = dy.fiscal_day_num
    	and dy.fiscal_year_num = (select fiscal_year_num from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW where day_date = CURRENT_DATE - 1)
    left join PRD_NAP_USR_VWS.PRODUCT_SKU_DIM_VW sku
    	on base.rms_sku_num = sku.rms_sku_num
    	and sku.channel_country = 'US'
    group by 1, 2, 3
) with data primary index (day_date, channel_num, dept_num) on commit preserve rows
;

collect stats primary index(day_date, channel_num, dept_num) on actuals;


/* 10. Sales/Demand Plans */
create multiset volatile table sales_demand_plan as (
	select distinct
		dt.day_date
		, sub.channel_num
		, sub.dept_num
        , coalesce(sub.net_sales_r_plan_weekly,0) * max_day_elapsed_week as net_sales_r_plan_weekly
        , coalesce(sub.net_sales_u_plan_weekly,0) * max_day_elapsed_week as net_sales_u_plan_weekly
        , coalesce(sub.net_sales_c_plan_weekly,0) * max_day_elapsed_week as net_sales_c_plan_weekly
        , coalesce(sub.net_sales_r_plan_weekly_ly,0) * max_day_elapsed_week as net_sales_r_plan_weekly_ly
        , coalesce(sub.demand_r_plan_weekly,0) * max_day_elapsed_week as demand_r_plan_weekly
        , coalesce(sub.demand_u_plan_weekly,0) * max_day_elapsed_week as demand_u_plan_weekly
        , coalesce(sub.net_sales_r_plan_monthly,0) * max_day_elapsed_month as net_sales_r_plan_monthly
        , coalesce(sub.net_sales_u_plan_monthly,0) * max_day_elapsed_month as net_sales_u_plan_monthly
        , coalesce(sub.net_sales_c_plan_monthly,0) * max_day_elapsed_month as net_sales_c_plan_monthly
        , coalesce(sub.net_sales_r_plan_monthly_ly,0) * max_day_elapsed_month as net_sales_r_plan_monthly_ly
        , coalesce(sub.demand_r_plan_monthly,0) * max_day_elapsed_month  as demand_r_plan_monthly
        , coalesce(sub.demand_u_plan_monthly,0) * max_day_elapsed_month  as demand_u_plan_monthly
	from
		(
	    select
	        b.week_idnt
	        , a.channel_num
	        , a.dept_num
	        , b.month_idnt
	        , b.quarter_idnt
	        , sum(op_net_sales_retail_amt) as net_sales_r_plan_weekly
	        , sum(op_net_sales_qty) as net_sales_u_plan_weekly
	        , sum(op_net_sales_cost_amt) as net_sales_c_plan_weekly
	        , sum(ly_net_sales_retail_amt) as net_sales_r_plan_weekly_ly
	        , sum(case when a.channel_num in (110, 210) then op_gross_sales_retail_amt else op_demand_total_retail_amt end) as demand_r_plan_weekly
	        , sum(case when a.channel_num in (110, 210) then op_gross_sales_qty else op_demand_total_qty end) as demand_u_plan_weekly
	        , sum(net_sales_r_plan_weekly) over(partition by month_idnt, channel_num, dept_num) as net_sales_r_plan_monthly
	        , sum(net_sales_u_plan_weekly) over(partition by month_idnt, channel_num, dept_num) as net_sales_u_plan_monthly
	        , sum(net_sales_c_plan_weekly) over(partition by month_idnt, channel_num, dept_num) as net_sales_c_plan_monthly
	        , sum(net_sales_r_plan_weekly_ly) over(partition by month_idnt, channel_num, dept_num) as net_sales_r_plan_monthly_ly
	        , sum(demand_r_plan_weekly)  over(partition by month_idnt, channel_num, dept_num) as demand_r_plan_monthly
	        , sum(demand_u_plan_weekly)  over(partition by month_idnt, channel_num, dept_num) as demand_u_plan_monthly
	        -- , sum(net_sales_r_plan_monthly) over(partition by quarter_idnt, channel_num, dept_num) as net_sales_r_plan_quarterly
	        -- , sum(net_sales_u_plan_monthly) over(partition by quarter_idnt, channel_num, dept_num) as net_sales_u_plan_quarterly
	        -- , sum(net_sales_c_plan_monthly) over(partition by quarter_idnt, channel_num, dept_num) as net_sales_c_plan_quarterly
	        -- , sum(demand_r_plan_monthly)  over(partition by quarter_idnt, channel_num, dept_num) as demand_r_plan_quarterly
	        -- , sum(demand_u_plan_monthly)  over(partition by quarter_idnt, channel_num, dept_num) as demand_u_plan_quarterly
	    from PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_CHANNEL_FACT a
	    inner join (
	        select
	            distinct
	            b.week_idnt as week_idnt_TRUE
	            , a.week_idnt
	            , a.month_idnt
	            , a.quarter_idnt
	            , ty_ly_lly_ind AS ty_ly_ind
	        from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW a
	        LEFT JOIN prd_nap_usr_vws.day_cal_454_dim b
	        	ON b.day_date = a.day_date
	        where a.quarter_idnt in (select quarter_idnt from date_filter)
	    ) b
	      on a.week_num = b.week_idnt_TRUE
	      and b.ty_ly_ind = 'TY'
	    where a.channel_num in (select distinct channel_num from store_lkup)
	    group by 1,2,3,4,5
	    ) sub
	left join date_filter dt
		on sub.week_idnt = dt.week_idnt
) with data primary index (day_date, channel_num, dept_num) on commit preserve rows
;

collect stats primary index(day_date, channel_num, dept_num) on sales_demand_plan;


/* 11. Inventory Plans */
create multiset volatile table eop_plan as (
	select distinct
		dt.day_date
		, sub.channel_num
		, sub.dept_num
        , coalesce(sub.eoh_c_plan_weekly,0) * max_day_elapsed_week as eoh_c_plan_weekly
        , coalesce(sub.eoh_u_plan_weekly,0) * max_day_elapsed_week as eoh_u_plan_weekly
        , coalesce(sub.eoh_c_plan_monthly,0) * max_day_elapsed_month  as eoh_c_plan_monthly
        , coalesce(sub.eoh_u_plan_monthly,0) * max_day_elapsed_month  as eoh_u_plan_monthly
	from
		(
	    select
	        b.week_idnt
	        , case when a.banner_country_num = 1 then 110
	              when a.banner_country_num = 3 then 210
	          end as channel_num
	        , a.dept_num
	        , b.month_idnt
	        , b.quarter_idnt
	        , b.wk_of_mth
	    		, sum(OP_ENDING_OF_PERIOD_ACTIVE_COST_AMT) as eoh_c_plan_weekly
	    		, sum(OP_ENDING_OF_PERIOD_ACTIVE_QTY) as eoh_u_plan_weekly
	    		, sum(case when wk_of_mth  = 1 then eoh_c_plan_weekly else 0 end) over(partition by month_idnt, channel_num, dept_num) as eoh_c_plan_monthly
	    		, sum(case when wk_of_mth  = 1 then eoh_u_plan_weekly else 0 end) over(partition by month_idnt, channel_num, dept_num) as eoh_u_plan_monthly
	    from PRD_NAP_USR_VWS.MFP_COST_PLAN_ACTUAL_BANNER_COUNTRY_FACT a
	    inner join (
	        select
	            distinct
	            b.week_idnt as week_idnt_TRUE
	            , a.week_idnt
	            , a.month_idnt
	            , a.quarter_idnt
	            , ty_ly_lly_ind AS ty_ly_ind
	            , dense_rank() over(partition by a.month_idnt order by a.week_idnt desc) as wk_of_mth
	        from PRD_NAP_VWS.REALIGNED_DATE_LKUP_VW a
	        left join prd_nap_usr_vws.day_cal_454_dim b
	        	on b.day_date = a.day_date
	        where a.quarter_idnt in (select quarter_idnt from date_filter)
	    ) b
	      on a.week_num = b.week_idnt_TRUE
	      and b.ty_ly_ind = 'TY'
	    where banner_country_num in (1,3) and dept_num not in (584, 585) --remove beauty sample depts
	    group by 1,2,3,4,5,6
	    ) sub
	left join date_filter dt
		on sub.week_idnt = dt.week_idnt
) with data primary index (day_date, channel_num, dept_num) on commit preserve rows
;

collect stats primary index(day_date, channel_num, dept_num) on eop_plan;


/* 12. Combine actuals and plans */
create multiset volatile table combine_all as (
    select
          coalesce(a.day_date, b.day_date, c.day_date) as day_date
        , coalesce(a.channel_num, b.channel_num, c.channel_num) as channel_num
        , coalesce(a.dept_num, b.dept_num, c.dept_num) as dept_num
		, sum(a.net_sales_r_ty) as net_sales_r_ty
		, sum(a.net_sales_c_ty) as net_sales_c_ty
		, sum(a.net_sales_u_ty) as net_sales_u_ty
		, sum(a.net_sales_reg_r_ty) as net_sales_reg_r_ty
		, sum(a.net_sales_reg_c_ty) as net_sales_reg_c_ty
		, sum(a.net_sales_reg_u_ty) as net_sales_reg_u_ty
		, sum(a.net_sales_pro_r_ty) as net_sales_pro_r_ty
		, sum(a.net_sales_pro_c_ty) as net_sales_pro_c_ty
		, sum(a.net_sales_pro_u_ty) as net_sales_pro_u_ty
		, sum(a.net_sales_clear_r_ty) as net_sales_clear_r_ty
		, sum(a.net_sales_clear_c_ty) as net_sales_clear_c_ty
		, sum(a.net_sales_clear_u_ty) as net_sales_clear_u_ty
		, sum(a.demand_ty) as demand_ty
		, sum(a.demand_u_ty) as demand_u_ty
		, sum(a.eoh_r_ty) as eoh_r_ty
		, sum(a.eoh_c_ty) as eoh_c_ty
		, sum(a.eoh_u_ty) as eoh_u_ty
		, sum(a.net_sales_r_ly) as net_sales_r_ly
		, sum(a.net_sales_c_ly) as net_sales_c_ly
		, sum(a.net_sales_u_ly) as net_sales_u_ly
		, sum(a.net_sales_reg_r_ly) as net_sales_reg_r_ly
		, sum(a.net_sales_reg_c_ly) as net_sales_reg_c_ly
		, sum(a.net_sales_reg_u_ly) as net_sales_reg_u_ly
		, sum(a.net_sales_pro_r_ly) as net_sales_pro_r_ly
		, sum(a.net_sales_pro_c_ly) as net_sales_pro_c_ly
		, sum(a.net_sales_pro_u_ly) as net_sales_pro_u_ly
		, sum(a.net_sales_clear_r_ly) as net_sales_clear_r_ly
		, sum(a.net_sales_clear_c_ly) as net_sales_clear_c_ly
		, sum(a.net_sales_clear_u_ly) as net_sales_clear_u_ly
		, sum(a.demand_ly) as demand_ly
		, sum(a.demand_u_ly) as demand_u_ly
		, sum(a.eoh_r_ly) as eoh_r_ly
		, sum(a.eoh_c_ly) as eoh_c_ly
		, sum(a.eoh_u_ly) as eoh_u_ly
        , sum(net_sales_r_plan_weekly) as net_sales_r_plan_weekly 
        , sum(net_sales_u_plan_weekly) as net_sales_u_plan_weekly 
        , sum(net_sales_c_plan_weekly) as net_sales_c_plan_weekly 
        , sum(net_sales_r_plan_weekly_ly) as net_sales_r_plan_weekly_ly
        , sum(demand_r_plan_weekly) as demand_r_plan_weekly
        , sum(demand_u_plan_weekly) as demand_u_plan_weekly
        , sum(eoh_c_plan_weekly) as eoh_c_plan_weekly
        , sum(eoh_u_plan_weekly) as eoh_u_plan_weekly
        , sum(net_sales_r_plan_monthly) as net_sales_r_plan_monthly
        , sum(net_sales_u_plan_monthly) as net_sales_u_plan_monthly
        , sum(net_sales_c_plan_monthly) as net_sales_c_plan_monthly
        , sum(net_sales_r_plan_monthly_ly) as net_sales_r_plan_monthly_ly
        , sum(demand_r_plan_monthly) as demand_r_plan_monthly
        , sum(demand_u_plan_monthly) as demand_u_plan_monthly
        , sum(eoh_c_plan_monthly)  as eoh_c_plan_monthly
        , sum(eoh_u_plan_monthly)  as eoh_u_plan_monthly
    from actuals a
    full outer join sales_demand_plan b
      on a.channel_num = b.channel_num
      and a.day_date = b.day_date
      and a.dept_num = b.dept_num
    full outer join eop_plan c
      on a.channel_num = c.channel_num
      and a.day_date = c.day_date
      and a.dept_num = c.dept_num
group by 1,2,3

) with data primary index (day_date, channel_num, dept_num) on commit preserve rows
;

collect stats primary index(day_date, channel_num, dept_num) on combine_all;


/* 13. Final Combination */
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'merch_daily_sales', OUT_RETURN_MSG);
create multiset table {environment_schema}.merch_daily_sales as (	
	select
		ch.banner
		,ch.channel_num
		,ch.channel
		,dep.division
		,dep.subdivision
		,dep.department
		,dep.dept_num
		,dt.fiscal_year_num
		,dt.fiscal_day_num
		,dt.day_date
		,dt.day_desc
		,dt.fiscal_week_num
		,dt.week_desc
		,dt.week_idnt
		,dt.fiscal_month_num
		,dt.month_desc
		,dt.wtd
		,dt.mtd
		,dt.elapsed_week_ind
		,dt.elapsed_day_ind
		,dt.max_day_elapsed
		,dt.max_day_elapsed_week
		,dt.max_day_elapsed_month
		,dt.max_day_elapsed_quarter
		,base.net_sales_r_ty
		,base.net_sales_c_ty
		,base.net_sales_u_ty
		,base.net_sales_reg_r_ty
		,base.net_sales_reg_c_ty
		,base.net_sales_reg_u_ty
		,base.net_sales_pro_r_ty
		,base.net_sales_pro_c_ty
		,base.net_sales_pro_u_ty
		,base.net_sales_clear_r_ty
		,base.net_sales_clear_c_ty
		,base.net_sales_clear_u_ty
		,base.demand_ty
		,base.demand_u_ty
		,base.eoh_r_ty
		,base.eoh_c_ty
		,base.eoh_u_ty
		,base.net_sales_r_ly
		,base.net_sales_c_ly
		,base.net_sales_u_ly
		,base.net_sales_reg_r_ly
		,base.net_sales_reg_c_ly
		,base.net_sales_reg_u_ly
		,base.net_sales_pro_r_ly
		,base.net_sales_pro_c_ly
		,base.net_sales_pro_u_ly
		,base.net_sales_clear_r_ly
		,base.net_sales_clear_c_ly
		,base.net_sales_clear_u_ly
		,base.demand_ly
		,base.demand_u_ly
		,base.eoh_r_ly
		,base.eoh_c_ly
		,base.eoh_u_ly
		,aor.general_merch_manager_executive_vice_president
		,aor.div_merch_manager_senior_vice_president
		,aor.div_merch_manager_vice_president
		,aor.merch_director
		,aor.buyer
		,aor.merch_planning_executive_vice_president
		,aor.merch_planning_senior_vice_president
		,aor.merch_planning_vice_president
		,aor.merch_planning_director_manager
		,aor.assortment_planner
		,base.net_sales_r_plan_weekly
		,base.net_sales_u_plan_weekly
		,base.net_sales_c_plan_weekly
		,base.net_sales_r_plan_weekly_ly
		,base.demand_r_plan_weekly
		,base.demand_u_plan_weekly
		,base.eoh_c_plan_weekly
		,base.eoh_u_plan_weekly
		,base.net_sales_r_plan_monthly
		,base.net_sales_u_plan_monthly
		,base.net_sales_c_plan_monthly
		,base.net_sales_r_plan_monthly_ly
		,base.demand_r_plan_monthly
		,base.demand_u_plan_monthly
		,base.eoh_c_plan_monthly
		,base.eoh_u_plan_monthly
		,CURRENT_TIMESTAMP as load_tmstp
	from combine_all base
	inner join date_filter dt
		on base.day_date = dt.day_date
    inner join channel_lkup ch
    	on base.channel_num = ch.channel_num
    	and ch.store_country_code = 'US'
    left join dept_lkup dep
    	on base.dept_num = dep.dept_num
	left join aor
		on ch.banner = aor.banner
		and base.dept_num = aor.dept_num
) with data primary index (day_date, channel_num, dept_num)
;


collect stats primary index(day_date, channel_num, dept_num) on {environment_schema}.merch_daily_sales;

grant select on {environment_schema}.merch_daily_sales to public;