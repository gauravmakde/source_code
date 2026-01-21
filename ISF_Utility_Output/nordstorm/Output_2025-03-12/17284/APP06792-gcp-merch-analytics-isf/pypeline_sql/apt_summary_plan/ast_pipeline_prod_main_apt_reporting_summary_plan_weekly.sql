/*
APT Plan Summary Query
Owner: Jihyun Yu
Date Created: 2/6/23

Datalab: t2dl_das_apt_cost_reporting
Creates Tables:
    - plan_summary_category_weekly
    - plan_summary_suppliergroup_weekly
*/

-- Category APT Summary Plan
delete from {environment_schema}.plan_summary_category_weekly{env_suffix} all;
insert into {environment_schema}.plan_summary_category_weekly{env_suffix}
select
  -- date
    wk.fiscal_year_num
    , wk.quarter_label
    , wk.month_idnt
    , wk.month_label
    , wk.month_454_label
    , wk.week_idnt
    , wk.week_label
    , wk.week_454_label
    , wk.week_start_day_date
    , wk.week_end_day_date
  -- filter attributes
    , base.country
    , base.currency_code
    , base.banner
    , base.chnl_idnt
    , base.fulfill_type_num
    , base.category
    , base.price_band
  -- division hierarchy
    , trim(dpt.division_num||', '|| dpt.division_name) as division_label
    , trim(dpt.subdivision_num ||', '|| dpt.subdivision_name) as subdivision_label
    , base.dept_idnt
	  , trim(dpt.dept_num ||', '|| dpt.dept_name) as dept_label
  -- category grouping
    , map1.category_planner_1
    , map1.category_planner_2
    , map1.category_group
    , map1.seasonal_designation
    , map1.rack_merch_zone
	  , map1.is_activewear
    , map1.channel_category_roles_1 as Nordstrom_CCS_Roles
    , map1.channel_category_roles_2 as Rack_CCS_Roles
    , coalesce(sl.channel_label, 'UNKNOWN') as channel_name
  -- plan sales/bop
    , base.demand_units
    , base.demand_r_dollars
    , base.gross_sls_units
    , base.gross_sls_r_dollars
    , base.return_units
    , base.return_r_dollars
    , base.net_sls_units
    , base.net_sls_r_dollars
    , base.net_sls_c_dollars
    , base.avg_inv_ttl_c
    , base.avg_inv_ttl_u
    , base.rcpt_need_r
    , base.rcpt_need_c
    , base.rcpt_need_u
    , base.rcpt_need_lr_r
    , base.rcpt_need_lr_c
    , base.rcpt_need_lr_u
    , base.plan_bop_c$
    , base.plan_bop_u
    , base.beginofmonth_bop_c
    , base.beginofmonth_bop_u
    , base.endofmonth_bop_c
    , base.endofmonth_bop_u
     -- ly & lly sales/bop/rcpt
    , base.ly_sales_c$
    , base.ly_sales_r$
    , base.ly_sales_u
    , base.ly_gross_r$
    , base.ly_gross_u
    , base.ly_demand_r$
    , base.ly_demand_u
    , base.ly_bop_c$
    , base.ly_bop_u
    , base.ly_bom_bop_c$
    , base.ly_bom_bop_u
    , base.ly_rcpt_need_c$
    , base.ly_rcpt_need_u
    -- mfp banner weekly
    , base.cp_bop_ttl_c_dollars
    , base.cp_bop_ttl_u
    , base.cp_beginofmonth_bop_c
    , base.cp_beginofmonth_bop_u
    , base.cp_eop_ttl_c_dollars
    , base.cp_eop_ttl_u
    , base.cp_eop_eom_c_dollars
    , base.cp_eop_eom_u
    , cp_rcpt_need_c_dollars
    , cp_rcpt_need_u
    , cp_rcpt_need_lr_c_dollars
    , cp_rcpt_need_lr_u
    -- mfp channel weekly
    , base.cp_demand_r_dollars
    , base.cp_demand_u
    , base.cp_gross_sls_r_dollars
    , base.cp_gross_sls_u
    , base.cp_return_r_dollars
    , base.cp_return_u
    , base.cp_net_sls_r_dollars
    , base.cp_net_sls_c_dollars
    , base.cp_net_sls_u
    , current_timestamp as update_timestamp
from (
  select
      coalesce(pl_ly.week_idnt, mb.week_idnt, mc.week_idnt) as week_idnt
      , coalesce(pl_ly.month_idnt, mb.month_idnt, mc.month_idnt) as month_idnt
      , coalesce(pl_ly.chnl_idnt, mb.channel, mc.channel) as chnl_idnt
      , coalesce(pl_ly.banner, mb.banner, mc.banner) as banner
      , coalesce(pl_ly.country, mb.channel_country, mc.channel_country) as country
      , coalesce(pl_ly.dept_idnt, mb.dept_idnt, mc.dept_idnt) as dept_idnt
      , coalesce(pl_ly.fulfill_type_num, mb.fulfill_type_num, mc.fulfill_type_num) as fulfill_type_num
      , coalesce(pl_ly.category, mb.category, mc.category) as category
      , coalesce(pl_ly.price_band, mb.price_band, mc.price_band) as price_band
      , coalesce(pl_ly.currency_code, mb.currency, mc.currency) as currency_code
    -- plan sales/bop/eop/rcpt
      , demand_units
      , demand_r_dollars
      , gross_sls_units
      , gross_sls_r_dollars
      , return_units
      , return_r_dollars
      , net_sls_units
      , net_sls_r_dollars
      , net_sls_c_dollars
      , avg_inv_ttl_c
      , avg_inv_ttl_u
      , rcpt_need_r
      , rcpt_need_c
      , rcpt_need_u
      , rcpt_need_lr_r
      , rcpt_need_lr_c
      , rcpt_need_lr_u
      , plan_bop_c$
      , plan_bop_u
      , beginofmonth_bop_c
      , beginofmonth_bop_u
      , endofmonth_bop_c
      , endofmonth_bop_u
    -- ly & lly sales/bop/rcpt
      , ly_sales_c$
      , ly_sales_r$
      , ly_sales_u
      , ly_gross_r$
      , ly_gross_u
      , ly_demand_r$
      , ly_demand_u
      , ly_bop_c$
      , ly_bop_u
      , ly_bom_bop_c$
      , ly_bom_bop_u
      , ly_rcpt_need_c$
      , ly_rcpt_need_u
    -- mfp banner weekly
      , mb.cp_bop_ttl_c_dollars
      , mb.cp_bop_ttl_u
      , mb.cp_beginofmonth_bop_c
      , mb.cp_beginofmonth_bop_u
      , mb.cp_eop_ttl_c_dollars
      , mb.cp_eop_ttl_u
      , mb.cp_eop_eom_c_dollars
      , mb.cp_eop_eom_u
      , mb.cp_rept_need_c_dollars as cp_rcpt_need_c_dollars
      , mb.cp_rept_need_u as cp_rcpt_need_u
      , mb.cp_rept_need_lr_c_dollars as cp_rcpt_need_lr_c_dollars
      , mb.cp_rept_need_lr_u as cp_rcpt_need_lr_u
    -- mfp channel weekly
      , mc.cp_demand_r_dollars
      , mc.cp_demand_u
      , mc.cp_gross_sls_r_dollars
      , mc.cp_gross_sls_u
      , mc.cp_return_r_dollars
      , mc.cp_return_u
      , mc.cp_net_sls_r_dollars
      , mc.cp_net_sls_c_dollars
      , mc.cp_net_sls_u
  from (
    select
        pl.week_idnt
        , pl.month_idnt
        , pl.chnl_idnt
        , pl.banner
        , pl.country
        , cast(pl.dept_idnt as integer) as dept_idnt
        , pl.fulfill_type_num
        , pl.category
        , pl.price_band_final as price_band
        , pl.currency_code
        , pl.demand_units
        , pl.demand_r_dollars
        , pl.gross_sls_units
        , pl.gross_sls_r_dollars
        , pl.return_units
        , pl.return_r_dollars
        , pl.net_sls_units
        , pl.net_sls_r_dollars
        , pl.net_sls_c_dollars
        , pl.next_2months_sales_run_rate
        , pl.avg_inv_ttl_c
        , pl.avg_inv_ttl_u
        , pl.rcpt_need_r
        , pl.rcpt_need_c
        , pl.rcpt_need_u
        , pl.rcpt_need_lr_r
        , pl.rcpt_need_lr_c
        , pl.rcpt_need_lr_u
        , pl.plan_bop_c_dollars as plan_bop_c$
        , pl.plan_bop_c_units as plan_bop_u
        , case when pl.week_start_day_date = pl.month_start_day_date then pl.plan_bop_c_dollars end as beginofmonth_bop_c
        , case when pl.week_start_day_date = pl.month_start_day_date then pl.plan_bop_c_units end as beginofmonth_bop_u
        , case when pl.week_end_day_date = pl.month_end_day_date then pl.plan_eop_c_dollars end as endofmonth_bop_c
        , case when pl.week_end_day_date = pl.month_end_day_date then pl.plan_eop_c_units end as endofmonth_bop_u
        , coalesce(ly.net_sls_c, lly.net_sls_c) as ly_sales_c$
        , coalesce(ly.net_sls_r, lly.net_sls_r) as ly_sales_r$
        , coalesce(ly.net_sls_units, lly.net_sls_units) as ly_sales_u
        , coalesce(ly.gross_sls_r, lly.gross_sls_r) as ly_gross_r$
        , coalesce(ly.gross_sls_units, lly.gross_sls_units) as ly_gross_u
        , coalesce(ly.demand_ttl_r, lly.demand_ttl_r) as ly_demand_r$
        , coalesce(ly.demand_ttl_units, lly.demand_ttl_units) as ly_demand_u
        , coalesce(ly.boh_ttl_c,lly.boh_ttl_c) as ly_bop_c$
        , coalesce(ly.boh_ttl_units,lly.boh_ttl_units) as ly_bop_u
        , coalesce(ly.beginofmonth_bop_c,lly.beginofmonth_bop_c) as ly_bom_bop_c$
        , coalesce(ly.beginofmonth_bop_u,lly.beginofmonth_bop_u) as ly_bom_bop_u
        , case when pl.banner = 'NORDSTROM' then coalesce(ly.ttl_porcpt_c,lly.ttl_porcpt_c)
               when pl.banner = 'NORDSTROM_RACK' then coalesce((ly.ttl_porcpt_c + ly.pah_tsfr_in_c + ly.rs_stk_tsfr_in_c),(lly.ttl_porcpt_c + lly.pah_tsfr_in_c + lly.rs_stk_tsfr_in_c))
               else 0
          end as ly_rcpt_need_c$
        , case when pl.banner = 'NORDSTROM' then coalesce(ly.ttl_porcpt_c_units,lly.ttl_porcpt_c_units)
               when pl.banner = 'NORDSTROM_RACK' then coalesce((ly.ttl_porcpt_c_units + ly.pah_tsfr_in_u + ly.rs_stk_tsfr_in_u),(lly.ttl_porcpt_c_units + lly.pah_tsfr_in_u + lly.rs_stk_tsfr_in_u))
               else 0
          end as ly_rcpt_need_u
    from (
      select p.*, 'TOTAL' as price_band_final
      from t2dl_das_apt_cost_reporting.category_channel_cost_plans_weekly p
    ) pl
    left join (
        select
            distinct a.week_idnt
            , b.week_idnt as week_idnt_ly
            , c.week_idnt as week_idnt_lly
        from PRD_NAP_USR_VWS.day_cal_454_dim a
        left join PRD_NAP_USR_VWS.day_cal_454_dim b
          on a.day_date_last_year_realigned = b.day_date
        left join PRD_NAP_USR_VWS.day_cal_454_dim c
          on b.day_date_last_year_realigned = c.day_date
        where a.day_date <= current_date + 365*3
    ) dt
    on pl.week_idnt = dt.week_idnt
      left join (
        select
            channel_num
            , week_idnt
            , department_num
            , quantrix_category
            , 'TOTAL' as price_band
            , case when dropship_ind = 'Y' then 1 else 3 end as fulfill_type_num
            , sum(net_sls_r) as net_sls_r
            , sum(net_sls_c) as net_sls_c
            , sum(net_sls_units) as net_sls_units
            , sum(gross_sls_r) as gross_sls_r
            , sum(gross_sls_units) as gross_sls_units
            , sum(demand_ttl_r) as demand_ttl_r
            , sum(demand_ttl_units) as demand_ttl_units
            , sum(eoh_ttl_c) as eoh_ttl_c
            , sum(eoh_ttl_units) as eoh_ttl_units
            , sum(boh_ttl_c) as boh_ttl_c
            , sum(boh_ttl_units) as boh_ttl_units
            , sum(beginofmonth_bop_c) as beginofmonth_bop_c
            , sum(beginofmonth_bop_u) as beginofmonth_bop_u
            , sum(ttl_porcpt_c) as ttl_porcpt_c
            , sum(ttl_porcpt_c_units) as ttl_porcpt_c_units
            , sum(pah_tsfr_in_c) as pah_tsfr_in_c
            , sum(pah_tsfr_in_u) as pah_tsfr_in_u
            , sum(rs_stk_tsfr_in_c) as rs_stk_tsfr_in_c
            , sum(rs_stk_tsfr_in_u) as rs_stk_tsfr_in_u
        from t2dl_das_apt_cost_reporting.category_priceband_cost_ly
        group by 1,2,3,4,5,6
      ) ly
        on (pl.week_idnt - 100) = ly.week_idnt -- dt.week_idnt_ly = ly.week_idnt
        and pl.chnl_idnt = ly.channel_num
        and pl.dept_idnt = ly.department_num
        and pl.category = ly.quantrix_category
        and pl.fulfill_type_num = ly.fulfill_type_num
        and pl.price_band_final = ly.price_band
      left join (
        select
            channel_num
            , week_idnt
            , department_num
            , quantrix_category
            , 'TOTAL' as price_band
            , case when dropship_ind = 'Y' then 1 else 3 end as fulfill_type_num
            , sum(net_sls_r) as net_sls_r
            , sum(net_sls_c) as net_sls_c
            , sum(net_sls_units) as net_sls_units
            , sum(gross_sls_r) as gross_sls_r
            , sum(gross_sls_units) as gross_sls_units
            , sum(demand_ttl_r) as demand_ttl_r
            , sum(demand_ttl_units) as demand_ttl_units
            , sum(eoh_ttl_c) as eoh_ttl_c
            , sum(eoh_ttl_units) as eoh_ttl_units
            , sum(boh_ttl_c) as boh_ttl_c
            , sum(boh_ttl_units) as boh_ttl_units
            , sum(beginofmonth_bop_c) as beginofmonth_bop_c
            , sum(beginofmonth_bop_u) as beginofmonth_bop_u
            , sum(ttl_porcpt_c) as ttl_porcpt_c
            , sum(ttl_porcpt_c_units) as ttl_porcpt_c_units
            , sum(pah_tsfr_in_c) as pah_tsfr_in_c
            , sum(pah_tsfr_in_u) as pah_tsfr_in_u
            , sum(rs_stk_tsfr_in_c) as rs_stk_tsfr_in_c
            , sum(rs_stk_tsfr_in_u) as rs_stk_tsfr_in_u
        from t2dl_das_apt_cost_reporting.category_priceband_cost_ly
        group by 1,2,3,4,5,6
      ) lly
        on (pl.week_idnt - 200) = lly.week_idnt -- dt.week_idnt_lly = lly.week_idnt
        and ly.week_idnt IS NULL
        and pl.chnl_idnt = lly.channel_num
        and pl.dept_idnt = lly.department_num
        and pl.category = lly.quantrix_category
        and pl.fulfill_type_num = lly.fulfill_type_num
        and pl.price_band_final = lly.price_band
  ) pl_ly
    full outer join (
      select
          case when banner = 'FP' then 'NORDSTROM'
               when banner = 'OP' then 'NORDSTROM_RACK'
          end as banner
          , week_idnt
          , month_idnt
          , dept_idnt
          , channel_country
          , 0 as channel
          , currency
          , fulfill_type_num
          , 'NOT_PLANNED' as category
          , 'TOTAL' as price_band
          , cp_bop_ttl_c_dollars
          , cp_bop_ttl_u
          , cp_beginofmonth_bop_c
          , cp_beginofmonth_bop_u
          , cp_eop_ttl_c_dollars
          , cp_eop_ttl_u
          , cp_eop_eom_c_dollars
          , cp_eop_eom_u
          , cp_rept_need_c_dollars
          , cp_rept_need_u
          , cp_rept_need_lr_c_dollars
          , cp_rept_need_lr_u
      from t2dl_das_apt_cost_reporting.mfp_cp_cost_banner_weekly
    ) mb
      on pl_ly.week_idnt = mb.week_idnt
      and pl_ly.dept_idnt = mb.dept_idnt
      and pl_ly.country = mb.channel_country
      and pl_ly.chnl_idnt = mb.channel
      and pl_ly.fulfill_type_num = mb.fulfill_type_num
      and pl_ly.banner = mb.banner
      and pl_ly.price_band = mb.price_band
      and pl_ly.category = mb.category
    full outer join (
      select
          case when chnl_idnt in (110,111,120,121,140,310,311,940,990)  then 'NORDSTROM'
               when chnl_idnt in (210,211,220,221,240,250,260,261) then 'NORDSTROM_RACK'
          end as banner
          , week_idnt
          , month_idnt
          , dept_idnt
          , channel_country
          , chnl_idnt as channel
          , currency
          , fulfill_type_num
          , 'NOT_PLANNED' as category
          , 'TOTAL' as price_band
          , cp_demand_r_dollars
          , cp_demand_u
          , cp_gross_sls_r_dollars
          , cp_gross_sls_u
          , cp_return_r_dollars
          , cp_return_u
          , cp_net_sls_r_dollars
          , cp_net_sls_c_dollars
          , cp_net_sls_u
      from t2dl_das_apt_cost_reporting.mfp_cp_cost_channel_weekly
    ) mc
      on pl_ly.week_idnt = mc.week_idnt
      and pl_ly.dept_idnt = mc.dept_idnt
      and pl_ly.country = mc.channel_country
      and pl_ly.chnl_idnt = mc.channel
      and pl_ly.fulfill_type_num = mc.fulfill_type_num
      and pl_ly.banner = mc.banner
      and pl_ly.price_band = mc.price_band
      and pl_ly.category = mc.category
) base
	left join (
		select
			  distinct dept_num
    		, category
    		, category_planner_1
    		, category_planner_2
    		, category_group
    		, seasonal_designation
    		, rack_merch_zone
    		, is_activewear
    		, channel_category_roles_1
    		, channel_category_roles_2
		from prd_nap_usr_vws.catg_subclass_map_dim
	) map1
    on base.dept_idnt = map1.dept_num
    and base.category = map1.category
	left join (
    	select
          distinct channel_country
          , channel_num
          , trim (channel_num || ', ' ||channel_desc) as channel_label
          , channel_brand as banner
	    from prd_nap_usr_vws.price_store_dim_vw
	) sl
    on base.country = sl.channel_country
	  and base.chnl_idnt = sl.channel_num
	  and base.banner = sl.banner
	left join prd_nap_usr_vws.department_dim dpt
	  on base.dept_idnt = dpt.dept_num
	inner join (
		select
        distinct week_idnt
        , week_label
        , week_454_label
        , month_idnt
        , month_label
        , month_454_label
        , week_start_day_date
        , week_end_day_date
        , quarter_label
        , fiscal_year_num
		from prd_nap_usr_vws.day_cal_454_dim
		where month_idnt >= (select month_idnt from prd_nap_usr_vws.day_cal_454_dim where day_date = current_date() - 7)
	) wk
	  on base.week_idnt = wk.week_idnt
;


-- Supplier APT Summary Plan
delete from {environment_schema}.plan_summary_suppliergroup_weekly{env_suffix} all;
insert into {environment_schema}.plan_summary_suppliergroup_weekly{env_suffix}
select
  -- date
    wk.fiscal_year_num
    , wk.quarter_label
    , wk.month_idnt
    , wk.month_label
    , wk.month_454_label
    , wk.month_start_day_date
    , wk.month_end_day_date
    , wk.week_idnt
    , wk.week_label
    , wk.week_454_label
    , wk.week_start_day_date
    , wk.week_end_day_date
  -- filter attributes
    , pl.country
    , pl.currency_code
    , pl.banner
    , pl.chnl_idnt
    , pl.fulfill_type_num
    , pl.alternate_inventory_model
    , pl.category
  -- division hierarchy
    , trim(dpt.division_num)||', '|| trim(dpt.division_name) as division_label
    , trim(dpt.subdivision_num ||', '|| dpt.subdivision_name) as subdivision_label
    , pl.dept_idnt
    , trim(dpt.dept_num ||', '|| dpt.dept_name) as dept_label
  -- supplier
    , pl.supplier_group
    , supp.buy_planner
  	, supp.preferred_partner_desc
  	, supp.areas_of_responsibility
  	, supp.is_npg
  	, supp.diversity_group
  	, supp.nord_to_rack_transfer_rate
  -- plan sales/bop
    , pl.demand_units
    , pl.demand_r_dollars
    , pl.gross_sls_units
    , pl.gross_sls_r_dollars
    , pl.return_units
    , pl.return_r_dollars
    , pl.net_sls_units
    , pl.net_sls_r_dollars
    , pl.net_sls_c_dollars
    , pl.avg_inv_ttl_c
    , pl.avg_inv_ttl_u
    , pl.plan_bop_c_dollars
    , pl.plan_bop_c_units
    , pl.plan_eop_c_dollars
    , pl.plan_eop_c_units
    , pl.rcpt_need_r
    , pl.rcpt_need_c
    , pl.rcpt_need_u
    , pl.rcpt_need_lr_r
    , pl.rcpt_need_lr_c
    , pl.rcpt_need_lr_u
    , case when pl.week_start_day_date = pl.month_start_day_date then pl.plan_bop_c_dollars end as beginofmonth_bop_c
    , case when pl.week_start_day_date = pl.month_start_day_date then pl.plan_bop_c_units end as beginofmonth_bop_u
  -- ly & lly sales/bop/rcpt
    , coalesce(ly.net_sls_c, lly.net_sls_c) as ly_sales_c$
    , coalesce(ly.net_sls_r, lly.net_sls_r) as ly_sales_r$
    , coalesce(ly.net_sls_units, lly.net_sls_units) as ly_sales_u
    , coalesce(ly.gross_sls_r, lly.gross_sls_r) as ly_gross_r$
    , coalesce(ly.gross_sls_units, lly.gross_sls_units) as ly_gross_u
    , coalesce(ly.demand_ttl_r, lly.demand_ttl_r) as ly_demand_r$
    , coalesce(ly.demand_ttl_units, lly.demand_ttl_units) as ly_demand_u
    , coalesce(ly.boh_ttl_c,lly.boh_ttl_c) as ly_bop_c$
    , coalesce(ly.boh_ttl_units,lly.boh_ttl_units) as ly_bop_u
    , coalesce(ly.beginofmonth_bop_c,lly.beginofmonth_bop_c) as ly_bom_bop_c$
    , coalesce(ly.beginofmonth_bop_u,lly.beginofmonth_bop_u) as ly_bom_bop_u
    , case when pl.banner = 'NORDSTROM' then coalesce(ly.ttl_porcpt_c,lly.ttl_porcpt_c)
           when pl.banner = 'NORDSTROM_RACK' then coalesce((ly.ttl_porcpt_c + ly.pah_tsfr_in_c + ly.rs_stk_tsfr_in_c),(lly.ttl_porcpt_c + lly.pah_tsfr_in_c + lly.rs_stk_tsfr_in_c))
    	else 0 end as ly_rcpt_need_c$
    , case when pl.banner = 'NORDSTROM' then coalesce(ly.ttl_porcpt_c_units,lly.ttl_porcpt_c_units)
      	   when pl.banner = 'NORDSTROM_RACK' then coalesce((ly.ttl_porcpt_c_units + ly.pah_tsfr_in_u + ly.rs_stk_tsfr_in_u),(lly.ttl_porcpt_c_units + lly.pah_tsfr_in_u + lly.rs_stk_tsfr_in_u))
      else 0 end as ly_rcpt_need_u
    , current_timestamp as update_timestamp
from t2dl_das_apt_cost_reporting.suppliergroup_channel_cost_plans_weekly pl
left join (
    select
        distinct a.week_idnt
        , b.week_idnt as week_idnt_ly
        , c.week_idnt as week_idnt_lly
    from PRD_NAP_USR_VWS.day_cal_454_dim a
    left join PRD_NAP_USR_VWS.day_cal_454_dim b
      on a.day_date_last_year_realigned = b.day_date
    left join PRD_NAP_USR_VWS.day_cal_454_dim c
      on b.day_date_last_year_realigned = c.day_date
    where a.day_date <= current_date + 365*3
) dt
on pl.week_idnt = dt.week_idnt
    inner join (
      select
        distinct week_idnt
        , fiscal_year_num
        , quarter_label
        , month_idnt
        , month_label
        , month_454_label
        , month_start_day_date
        , month_end_day_date
        , week_label
        , week_454_label
        , week_start_day_date
        , week_end_day_date
      from prd_nap_usr_vws.day_cal_454_dim
      where month_idnt >= (select month_idnt from prd_nap_usr_vws.day_cal_454_dim where day_date = current_date())
    ) wk
      on pl.week_idnt = wk.week_idnt
    left join (
      select
          distinct dept_num
          , supplier_group
          , case when upper(banner) = 'FP' then 'NORDSTROM' else 'NORDSTROM_RACK' end as banner
          , buy_planner
          , preferred_partner_desc
          , areas_of_responsibility
          , is_npg
          , diversity_group
          , nord_to_rack_transfer_rate
      from prd_nap_usr_vws.supp_dept_map_dim
      qualify row_number() over(partition by dept_num, supplier_group, banner order by dw_sys_updt_tmstp desc) = 1
    ) supp
      on pl.dept_idnt  = cast(supp.dept_num as integer)
      and pl.supplier_group = supp.supplier_group
      and pl.banner = supp.banner
    left join prd_nap_usr_vws.department_dim dpt
      on pl.dept_idnt = dpt.dept_num
    left join (
      select
          channel_num
          , week_idnt
          , department_num
          , quantrix_category
          , supplier_group
          , case when dropship_ind = 'Y' then 1 else 3 end as fulfill_type_num
          , sum(net_sls_r) as net_sls_r
          , sum(net_sls_c) as net_sls_c
          , sum(net_sls_units) as net_sls_units
          , sum(gross_sls_r) as gross_sls_r
          , sum(gross_sls_units) as gross_sls_units
          , sum(demand_ttl_r) as demand_ttl_r
          , sum(demand_ttl_units) as demand_ttl_units
          , sum(eoh_ttl_c) as eoh_ttl_c
          , sum(eoh_ttl_units) as eoh_ttl_units
          , sum(boh_ttl_c) as boh_ttl_c
          , sum(boh_ttl_units) as boh_ttl_units
          , sum(beginofmonth_bop_c) as beginofmonth_bop_c
          , sum(beginofmonth_bop_u) beginofmonth_bop_u
          , sum(ttl_porcpt_c) as ttl_porcpt_c
          , sum(ttl_porcpt_c_units) as ttl_porcpt_c_units
          , sum(pah_tsfr_in_c) as pah_tsfr_in_c
          , sum(pah_tsfr_in_u) as pah_tsfr_in_u
          , sum(rs_stk_tsfr_in_c) as rs_stk_tsfr_in_c
          , sum(rs_stk_tsfr_in_u) as rs_stk_tsfr_in_u
      from t2dl_das_apt_cost_reporting.category_suppliergroup_cost_ly
      group by 1,2,3,4,5,6
    ) ly
      on (pl.week_idnt - 100) = ly.week_idnt -- dt.week_idnt_ly = ly.week_idnt
      and pl.chnl_idnt = ly.channel_num
      and pl.dept_idnt = ly.department_num
      and ly.quantrix_category = pl.category
      and pl.supplier_group = ly.supplier_group
      and pl.fulfill_type_num = ly.fulfill_type_num
    left join (
      select
          channel_num
          , week_idnt
          , department_num
          , quantrix_category
          , supplier_group
          , case when dropship_ind = 'Y' then 1 else 3 end as fulfill_type_num
          , sum(net_sls_r) as net_sls_r
          , sum(net_sls_c) as net_sls_c
          , sum(net_sls_units) as net_sls_units
          , sum(gross_sls_r) as gross_sls_r
          , sum(gross_sls_units) as gross_sls_units
          , sum(demand_ttl_r) as demand_ttl_r
          , sum(demand_ttl_units) as demand_ttl_units
          , sum(eoh_ttl_c) as eoh_ttl_c
          , sum(eoh_ttl_units) as eoh_ttl_units
          , sum(boh_ttl_c) as boh_ttl_c
          , sum(boh_ttl_units) as boh_ttl_units
          , sum(beginofmonth_bop_c) as beginofmonth_bop_c
          , sum(beginofmonth_bop_u) beginofmonth_bop_u
          , sum(ttl_porcpt_c) as ttl_porcpt_c
          , sum(ttl_porcpt_c_units) as ttl_porcpt_c_units
          , sum(pah_tsfr_in_c) as pah_tsfr_in_c
          , sum(pah_tsfr_in_u) as pah_tsfr_in_u
          , sum(rs_stk_tsfr_in_c) as rs_stk_tsfr_in_c
          , sum(rs_stk_tsfr_in_u) as rs_stk_tsfr_in_u
      from t2dl_das_apt_cost_reporting.category_suppliergroup_cost_ly
      group by 1,2,3,4,5,6
    ) lly
      on (pl.week_idnt - 200) = lly.week_idnt -- dt.week_idnt_lly = lly.week_idnt
      and ly.week_idnt is null
      and pl.chnl_idnt = lly.channel_num
      and pl.dept_idnt = lly.department_num
      and lly.quantrix_category = pl.category
      and pl.supplier_group = lly.supplier_group
      and pl.fulfill_type_num = lly.fulfill_type_num
;
