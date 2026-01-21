/*------------------------------------------------------
 Margin Forecast
 Purpose: Provide visibility into margin risk or upside by price type at the division/banner/country granularity
 Data Included: (1) mfp plans, (2) supplier profitabliity results by price type, (3) price type plans
 
 Last Update: 05/12/23 Ashton Leopold
 Update: removed inactive depts, updated price type plan names to include cp for consistency
 --------------------------------------------------------*/
with cte_spd as (
select spd.week_idnt as week_idnt
	, spd.channel_num as channel_num 
	, spd.dept_idnt as dept_idnt
	--price type net sales and return actuals
	, sum(spd.net_sales_tot_retl) 				as net_sales_tot_retl
	, sum(spd.net_sales_tot_regular_retl) 		as net_sales_tot_regular_retl
	, sum(spd.net_sales_tot_promo_retl) 		as net_sales_tot_promo_retl
	, sum(spd.net_sales_tot_clearance_retl) 	as net_sales_tot_clearance_retl
	, sum(spd.net_sales_tot_cost) 				as net_sales_tot_cost
	, sum(spd.net_sales_tot_regular_cost) 		as net_sales_tot_regular_cost
	, sum(spd.net_sales_tot_promo_cost) 		as net_sales_tot_promo_cost
	, sum(spd.net_sales_tot_clearance_cost) 	as net_sales_tot_clearance_cost
	, sum(spd.net_sales_tot_units) 				as net_sales_tot_units
	, sum(spd.net_sales_tot_regular_units) 		as net_sales_tot_regular_units
	, sum(spd.net_sales_tot_promo_units) 		as net_sales_tot_promo_units
	, sum(spd.net_sales_tot_clearance_units) 	as net_sales_tot_clearance_units
	, sum(spd.returns_tot_retl) 				as returns_tot_retl
	, sum(spd.returns_tot_regular_retl) 		as returns_tot_regular_retl
	, sum(spd.returns_tot_promo_retl) 			as returns_tot_promo_retl
	, sum(spd.returns_tot_clearance_retl) 		as returns_tot_clearance_retl
	, sum(spd.returns_tot_cost) 				as returns_tot_cost
	, sum(spd.returns_tot_regular_cost) 		as returns_tot_regular_cost
	, sum(spd.returns_tot_promo_cost) 			as returns_tot_promo_cost
	, sum(spd.returns_tot_clearance_cost) 		as returns_tot_clearance_cost
	, sum(spd.returns_tot_units) 				as returns_tot_units
	, sum(spd.returns_tot_regular_units) 		as returns_tot_regular_units
	, sum(spd.returns_tot_promo_units) 			as returns_tot_promo_units
	, sum(spd.returns_tot_clearance_units) 		as returns_tot_clearance_units
	--price type eoh and boh actuals
	, sum(spd.eoh_regular_units) 	as eoh_regular_units
	, sum(spd.eoh_regular_cost) 	as eoh_regular_cost 
	, sum(spd.eoh_regular_retail) 	as eoh_regular_retail
	, sum(spd.eoh_clearance_units) 	as eoh_clearance_units
	, sum(spd.eoh_clearance_cost) 	as eoh_clearance_cost 
	, sum(spd.eoh_clearance_retail) as eoh_clearance_retail
	, sum(spd.eoh_total_units) 		as eoh_total_units 
	, sum(spd.eoh_total_cost) 		as eoh_total_cost 
	, sum(spd.eoh_total_retail) 	as eoh_total_retail 
	, sum(spd.boh_regular_units) 	as boh_regular_units
	, sum(spd.boh_regular_cost) 	as boh_regular_cost 
	, sum(spd.boh_regular_retail) 	as boh_regular_retail
	, sum(spd.boh_clearance_units) 	as boh_clearance_units
	, sum(spd.boh_clearance_cost) 	as boh_clearance_cost 
	, sum(spd.boh_clearance_retail) as boh_clearance_retail
	, sum(spd.boh_total_units) 		as boh_total_units 
	, sum(spd.boh_total_cost) 		as boh_total_cost 
	, sum(spd.boh_total_retail) 	as boh_total_retail 
from t2dl_das_strategic_brand_management_reporting.supplier_profitability_expectation_channel_week_dept spd
	--business_unit_desc duplicates channel_num
where 1=1
	and spd.country_code ='US'
group by 1, 2, 3
)
, cte_price_type_plans as (
select ptp.week_num as week_num
	, ptp.channel_num as channel_num
	, ptp.department_num as department_num
	, sum(case when ptp.price_type = 'Regular' then (ptp.gross_sls_r) end) 		as cp_reg_gross_sls_r
	, sum(case when ptp.price_type = 'Promotional' then (ptp.gross_sls_r) end) 	as cp_pro_gross_sls_r
	, sum(case when ptp.price_type = 'Clearance' then (ptp.gross_sls_r) end) 	as cp_clr_gross_sls_r
	, sum(case when ptp.price_type = 'Regular' then (ptp.gross_sls_c) end) 		as cp_reg_gross_sls_c
	, sum(case when ptp.price_type = 'Promotional' then (ptp.gross_sls_c) end) 	as cp_pro_gross_sls_c
	, sum(case when ptp.price_type = 'Clearance' then (ptp.gross_sls_c) end) 	as cp_clr_gross_sls_c
	, sum(case when ptp.price_type = 'Regular' then (ptp.gross_sls_u) end) 		as cp_reg_gross_sls_u
	, sum(case when ptp.price_type = 'Promotional' then (ptp.gross_sls_u) end) 	as cp_pro_gross_sls_u
	, sum(case when ptp.price_type = 'Clearance' then (ptp.gross_sls_u) end) 	as cp_clr_gross_sls_u
	, sum(case when ptp.price_type = 'Regular' then (ptp.rtn_r) end) 			as cp_reg_rtn_r
	, sum(case when ptp.price_type = 'Promotional' then (ptp.rtn_r) end) 		as cp_pro_rtn_r
	, sum(case when ptp.price_type = 'Clearance' then (ptp.rtn_r) end) 			as cp_clr_rtn_r
	, sum(case when ptp.price_type = 'Regular' then (ptp.rtn_c) end) 			as cp_reg_rtn_c
	, sum(case when ptp.price_type = 'Promotional' then (ptp.rtn_c) end) 		as cp_pro_rtn_c
	, sum(case when ptp.price_type = 'Clearance' then (ptp.rtn_c) end) 			as cp_clr_rtn_c
	, sum(case when ptp.price_type = 'Regular' then (ptp.rtn_u) end) 			as cp_reg_rtn_u
	, sum(case when ptp.price_type = 'Promotional' then (ptp.rtn_u) end) 		as cp_pro_rtn_u
	, sum(case when ptp.price_type = 'Clearance' then (ptp.rtn_u) end) 			as cp_clr_rtn_u
from t2dl_das_price_type_plan_outputs.price_type_plan_data ptp where 1=1
	and ptp.plan_version = 'Cp'
	and ptp.price_type <> 'Total'
group by 1, 2, 3
)
select
	--dates
	d.week_idnt 													as week_idnt	
	, d.fiscal_week_num 											as fiscal_week_num
	, mfp.week_454_label 											as week_454_label
	, trim(d.month_abrv)||', Wk '||trim(d.week_num_of_fiscal_month) as week_abrv
	, d.fiscal_month_num 											as fiscal_month_num
	, mfp."month" 													as "month"
	, d.month_abrv 													as month_abrv
	, d.fiscal_quarter_num 											as fiscal_quarter_num
	, mfp.quarter 													as quarter
	, mfp.quarter_num 												as quarter_num
	, d.quarter_abrv 												as quarter_abrv
	, case half_abrv when 'Half 1' then 1 else 2 end 				as half_num
	, mfp.half 														as half
	, left(d.half_label, 6) 										as half_abrv
	, mfp."year" 													as "year"
	, mfp.week_end_day_date 										as week_end_day_date
	, case when fy.ty_fiscal_year = d.fiscal_year_num then 'TY'
		when (fy.ty_fiscal_year-1) = d.fiscal_year_num then 'LY'
		when (fy.ty_fiscal_year-2) = d.fiscal_year_num then 'LLY'
		when (fy.ty_fiscal_year+1) = d.fiscal_year_num then 'Next Year'
	 	when (fy.ty_fiscal_year+2) = d.fiscal_year_num then 'Next Next Year'
	 	end 														as ty_ly_ind
	--location
	, mfp.banner_country 	as banner_country
	, mfp.banner 			as banner
	, mfp.country 			as country
	, mfp.channel_num 		as channel_num
	, mfp.channel 			as channel
	--aor
	, mfp.dept_num 			as dept_num
	, mfp.department_label 	as department_label
	, mfp.subdivision 		as subdivion
	, mfp.division 			as division
	--price type net sales and return actuals
	, spd.net_sales_tot_retl 			as net_sales_tot_retl
	, spd.net_sales_tot_regular_retl 	as net_sales_tot_regular_retl
	, spd.net_sales_tot_promo_retl 		as net_sales_tot_promo_retl
	, spd.net_sales_tot_clearance_retl 	as net_sales_tot_clearance_retl
	, spd.net_sales_tot_cost 			as net_sales_tot_cost
	, spd.net_sales_tot_regular_cost 	as net_sales_tot_regular_cost
	, spd.net_sales_tot_promo_cost 		as net_sales_tot_promo_cost
	, spd.net_sales_tot_clearance_cost 	as net_sales_tot_clearance_cost
	, spd.net_sales_tot_units 			as net_sales_tot_units
	, spd.net_sales_tot_regular_units 	as net_sales_tot_regular_units
	, spd.net_sales_tot_promo_units 	as net_sales_tot_promo_units
	, spd.net_sales_tot_clearance_units as net_sales_tot_clearance_units
	, spd.returns_tot_retl 				as returns_tot_retl
	, spd.returns_tot_regular_retl 		as returns_tot_regular_retl
	, spd.returns_tot_promo_retl 		as returns_tot_promo_retl
	, spd.returns_tot_clearance_retl 	as returns_tot_clearance_retl
	, spd.returns_tot_cost 				as returns_tot_cost
	, spd.returns_tot_regular_cost 		as returns_tot_regular_cost
	, spd.returns_tot_promo_cost 		as returns_tot_promo_cost
	, spd.returns_tot_clearance_cost 	as returns_tot_clearance_cost
	, spd.returns_tot_units 			as returns_tot_units
	, spd.returns_tot_regular_units 	as returns_tot_regular_units
	, spd.returns_tot_promo_units 		as returns_tot_promo_units
	, spd.returns_tot_clearance_units 	as returns_tot_clearance_units
	--price type eoh and boh actuals
	, spd.eoh_regular_units 	as eoh_regular_units
	, spd.eoh_regular_cost 		as eoh_regular_cost 
	, spd.eoh_regular_retail 	as eoh_regular_retail
	, spd.eoh_clearance_units 	as eoh_clearance_units
	, spd.eoh_clearance_cost 	as eoh_clearance_cost 
	, spd.eoh_clearance_retail 	as eoh_clearance_retail
	, spd.eoh_total_units 		as eoh_total_units 
	, spd.eoh_total_cost 		as eoh_total_cost 
	, spd.eoh_total_retail 		as eoh_total_retail 
	, spd.boh_regular_units 	as boh_regular_units
	, spd.boh_regular_cost 		as boh_regular_cost 
	, spd.boh_regular_retail 	as boh_regular_retail
	, spd.boh_clearance_units 	as boh_clearance_units
	, spd.boh_clearance_cost 	as boh_clearance_cost 
	, spd.boh_clearance_retail 	as boh_clearance_retail
	, spd.boh_total_units 		as boh_total_units 
	, spd.boh_total_cost 		as boh_total_cost 
	, spd.boh_total_retail 		as boh_total_retail 
	--price type plans
	, ptp.reg_gross_sls_r_plan as reg_gross_sls_r_plan
	, ptp.pro_gross_sls_r_plan as pro_gross_sls_r_plan
	, ptp.clr_gross_sls_r_plan as clr_gross_sls_r_plan
	, ptp.reg_gross_sls_c_plan as reg_gross_sls_c_plan
	, ptp.pro_gross_sls_c_plan as pro_gross_sls_c_plan
	, ptp.clr_gross_sls_c_plan as clr_gross_sls_c_plan
	, ptp.reg_gross_sls_u_plan as reg_gross_sls_u_plan
	, ptp.pro_gross_sls_u_plan as pro_gross_sls_u_plan
	, ptp.clr_gross_sls_u_plan as clr_gross_sls_u_plan
	, ptp.reg_rtn_r_plan as reg_rtn_r_plan
	, ptp.pro_rtn_r_plan as pro_rtn_r_plan
	, ptp.clr_rtn_r_plan as clr_rtn_r_plan
	, ptp.reg_rtn_c_plan as reg_rtn_c_plan
	, ptp.pro_rtn_c_plan as pro_rtn_c_plan
	, ptp.clr_rtn_c_plan as clr_rtn_c_plan
	, ptp.reg_rtn_u_plan as reg_rtn_u_plan
	, ptp.pro_rtn_u_plan as pro_rtn_u_plan
	, ptp.clr_rtn_u_plan as clr_rtn_u_plan
	--mfp plans 
	, sum(mfp.cp_gross_sales_retail_amt) 	as cp_gross_sales_retail_amt
	, sum(mfp.cp_gross_sales_units) 		as cp_gross_sales_units
	, sum(mfp.cp_net_sales_retail_amt)	 	as cp_net_sales_retail_amt
	, sum(mfp.cp_net_sales_cost_amt) 		as cp_net_sales_cost_amt
	, sum(mfp.cp_net_sales_units) 			as cp_net_sales_units
	, sum(mfp.cp_returns_retail_amt) 		as cp_returns_retail_amt
	, sum(mfp.cp_returns_units) 			as cp_returns_units
	, sum(mfp.op_gross_sales_retail_amt) 	as op_gross_sales_retail_amt
	, sum(mfp.op_gross_sales_units) 		as op_gross_sales_units
	, sum(mfp.op_net_sales_retail_amt) 		as op_net_sales_retail_amt
	, sum(mfp.op_net_sales_cost_amt) 		as op_net_sales_cost_amt
	, sum(mfp.op_net_sales_units) 			as op_net_sales_units
	, sum(mfp.op_returns_retail_amt) 		as op_returns_retail_amt
	, sum(mfp.op_returns_units) 			as op_returns_units
from t2dl_das_ace_mfp.mfp_banner_country_channel_stg mfp
	--fulfill_type_num duplicates channel_num
left join cte_spd spd on 1=1
	and spd.dept_idnt = mfp.dept_num 
	and spd.channel_num = mfp.channel_num 
	and spd.week_idnt = mfp.week_num
left join cte_price_type_plans ptp on 1=1
	and ptp.department_num = mfp.dept_num
	and ptp.channel_num = mfp.channel_num 
	and ptp.week_num = mfp.week_num 
left join prd_nap_usr_vws.day_cal_454_dim d on 1=1
	and d.day_date = mfp.week_end_day_date 
left join (select distinct fiscal_year_num as ty_fiscal_year from prd_nap_usr_vws.day_cal_454_dim d where d.day_date = current_date()) fy on 1=1
where 1=1
	and mfp.country = 'US'
	and mfp.division not like '%INACTIVE%'
	--data validation
	--and mfp.dept_num = 837
	--and mfp."MONTH" = '2023 FEB'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
	, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40
	, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60
	, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80
	, 81, 82, 83, 84, 85, 86