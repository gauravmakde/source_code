TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.current_month;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.current_month
(SELECT DISTINCT month_idnt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
 WHERE day_date = CURRENT_DATE('PST8PDT'));


CREATE TEMPORARY TABLE IF NOT EXISTS dates_lkup_ccp
--CLUSTER BY month_idnt, week_idnt
AS
SELECT DISTINCT week_idnt,
 week_start_day_date,
 week_end_day_date,
 month_idnt,
 week_num_of_fiscal_month,
 MAX(week_num_of_fiscal_month) OVER (PARTITION BY month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
 AS weeks_in_month,
  CASE
  WHEN week_start_day_idnt = month_start_day_idnt
  THEN 1
  ELSE 0
  END AS month_week_start_ind,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE month_idnt IN (SELECT fiscal_month_idnt
   FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_category_cluster_plan_fact_vw);


-- demand retail


-- demand units


-- Gross sales retail


-- Gross sales units


-- returns retail


-- returns units


-- net sales retail


-- net sales cost


-- net sales untis


CREATE TEMPORARY TABLE IF NOT EXISTS mfpc_channel_weekly_ratios
--CLUSTER BY month_idnt, chnl_idnt, dept_idnt
AS
SELECT DISTINCT dc.month_idnt,
 mfpc.week_num AS week_idnt,
 dc.week_start_day_date,
 dc.week_end_day_date,
 dc.weeks_in_month,
 mfpc.fulfill_type_num,
 mfpc.channel_num AS chnl_idnt,
 mfpc.dept_num AS dept_idnt,
 SUM(mfpc.sp_demand_total_retail_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_r_week,
 SUM(mfpc.sp_demand_total_retail_amt) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_r_month,
 COALESCE((SUM(mfpc.sp_demand_total_retail_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num
       , mfpc.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_demand_total_retail_amt
     ) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS demand_mfp_sp_r_month_wk_pct,
 SUM(mfpc.sp_demand_total_qty) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_u_week,
 SUM(mfpc.sp_demand_total_qty) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS demand_u_month,
 COALESCE((SUM(mfpc.sp_demand_total_qty) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc
       .dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_demand_total_qty) OVER
     (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS demand_mfp_sp_u_month_wk_pct,
 SUM(mfpc.sp_gross_sales_retail_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gross_sls_r_week,
 SUM(mfpc.sp_gross_sales_retail_amt) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gross_sls_r_month,
 COALESCE((SUM(mfpc.sp_gross_sales_retail_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num
       , mfpc.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_gross_sales_retail_amt
     ) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS gross_sls_r_mfp_sp_month_wk_pct,
 SUM(mfpc.sp_gross_sales_qty) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gross_sls_u_week,
 SUM(mfpc.sp_gross_sales_qty) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gross_sls_u_month,
 COALESCE((SUM(mfpc.sp_gross_sales_qty) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_gross_sales_qty) OVER (PARTITION BY
       dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND
     UNBOUNDED FOLLOWING), 0), 0) AS gross_sls_u_mfp_sp_month_wk_pct,
 SUM(mfpc.sp_returns_retail_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS return_r_week,
 SUM(mfpc.sp_returns_retail_amt) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS return_r_month,
 COALESCE((SUM(mfpc.sp_returns_retail_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num,
       mfpc.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_returns_retail_amt
     ) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS return_mfp_sp_r_month_wk_pct,
 SUM(mfpc.sp_returns_qty) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS return_u_week,
 SUM(mfpc.sp_returns_qty) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS return_u_month,
 COALESCE((SUM(mfpc.sp_returns_qty) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_returns_qty) OVER (PARTITION BY
       dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND
     UNBOUNDED FOLLOWING), 0), 0) AS return_mfp_sp_u_month_wk_pct,
 SUM(mfpc.sp_net_sales_retail_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sls_r_week,
 SUM(mfpc.sp_net_sales_retail_amt) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sls_r_month,
 COALESCE((SUM(mfpc.sp_net_sales_retail_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num,
       mfpc.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_net_sales_retail_amt
     ) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS net_sls_r_mfp_sp_month_wk_pct,
 SUM(mfpc.sp_net_sales_cost_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sls_c_week,
 SUM(mfpc.sp_net_sales_cost_amt) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sls_c_month,
 COALESCE((SUM(mfpc.sp_net_sales_cost_amt) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num,
       mfpc.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_net_sales_cost_amt
     ) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN
     UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS net_sls_c_mfp_sp_month_wk_pct,
 SUM(mfpc.sp_net_sales_qty) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sls_u_week,
 SUM(mfpc.sp_net_sales_qty) OVER (PARTITION BY dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS net_sls_u_month,
 COALESCE((SUM(mfpc.sp_net_sales_qty) OVER (PARTITION BY dc.week_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num
       RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(mfpc.sp_net_sales_qty) OVER (PARTITION BY
       dc.month_idnt, mfpc.fulfill_type_num, mfpc.channel_num, mfpc.dept_num RANGE BETWEEN UNBOUNDED PRECEDING AND
     UNBOUNDED FOLLOWING), 0), 0) AS net_sls_u_mfp_sp_month_wk_pct
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_channel_fact AS mfpc
 INNER JOIN dates_lkup_ccp AS dc ON mfpc.week_num = dc.week_idnt
WHERE mfpc.fulfill_type_num IN (1, 3);


-- DROP TABLE mfp_banner_weekly_ratios;


--from Jeff Akerman - APT dont plan Inactive Receipts, so in order to tie back to MFP Cp, 


--need to remove inactive receipt from total receipt calculation


-- avg inv cost


-- receipt cost


-- receipt units


-- receitps less reserve cost


-- receitps less reserve units


-- EOH C


-- EOH U


CREATE TEMPORARY TABLE IF NOT EXISTS mfp_banner_weekly_ratios AS WITH mfp_sp AS (SELECT dc.month_idnt,
   mfpc.week_num AS week_idnt,
   dc.week_start_day_date,
   dc.week_end_day_date,
   dc.week_num_of_fiscal_month,
   dc.weeks_in_month,
    CASE
    WHEN mfpc.banner_country_num IN (1, 3)
    THEN 'US'
    WHEN mfpc.banner_country_num IN (2, 4)
    THEN 'CA'
    ELSE NULL
    END AS channel_country,
    CASE
    WHEN mfpc.banner_country_num IN (1, 2)
    THEN 'NORDSTROM'
    WHEN mfpc.banner_country_num IN (3, 4)
    THEN 'NORDSTROM_RACK'
    ELSE NULL
    END AS banner,
   mfpc.fulfill_type_num,
   mfpc.dept_num AS dept_idnt,
   SUM(mfpc.sp_beginning_of_period_active_cost_amt + mfpc.sp_beginning_of_period_inactive_cost_amt) AS
   sp_bop_ttl_c_dollars,
   SUM(mfpc.sp_ending_of_period_active_cost_amt + mfpc.sp_ending_of_period_inactive_cost_amt) AS sp_eop_ttl_c_dollars,
   SUM(mfpc.sp_ending_of_period_active_qty + mfpc.sp_ending_of_period_inactive_qty) AS sp_eop_ttl_units,
   SUM(mfpc.sp_receipts_active_cost_amt) AS sp_rept_need_c_dollars,
   SUM(mfpc.sp_receipts_active_qty) AS sp_rept_need_u_dollars,
   SUM(mfpc.sp_receipts_active_cost_amt - mfpc.sp_receipts_reserve_cost_amt) AS sp_rept_need_lr_c_dollars,
   SUM(mfpc.sp_receipts_active_qty - mfpc.sp_receipts_reserve_qty) AS sp_rept_need_lr_u_dollars
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS mfpc
   INNER JOIN dates_lkup_ccp AS dc ON mfpc.week_num = dc.week_idnt
  WHERE mfpc.fulfill_type_num IN (1, 3)
  GROUP BY dc.month_idnt,
   week_idnt,
   dc.week_start_day_date,
   dc.week_end_day_date,
   dc.week_num_of_fiscal_month,
   dc.weeks_in_month,
   channel_country,
   banner,
   mfpc.fulfill_type_num,
   dept_idnt) (SELECT month_idnt,
   week_idnt,
   week_start_day_date,
   week_end_day_date,
   week_num_of_fiscal_month,
   weeks_in_month,
   fulfill_type_num,
   channel_country,
   banner,
   dept_idnt,
     (sp_bop_ttl_c_dollars + sp_eop_ttl_c_dollars) / 2 AS avg_inv_cost_week,
   SUM((sp_bop_ttl_c_dollars + sp_eop_ttl_c_dollars) / 2) OVER (PARTITION BY month_idnt, fulfill_type_num,
      channel_country, banner, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS
   avg_inv_cost_month,
   COALESCE((sp_bop_ttl_c_dollars + sp_eop_ttl_c_dollars) / 2 / NULLIF(SUM((sp_bop_ttl_c_dollars + sp_eop_ttl_c_dollars
         ) / 2) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS inv_mfp_sp_c_month_wk_pct,
   sp_rept_need_c_dollars AS rept_cost_week,
   SUM(sp_rept_need_c_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rept_cost_month,
   COALESCE(sp_rept_need_c_dollars / NULLIF(SUM(sp_rept_need_c_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num
         , channel_country, banner, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS
   rept_mfp_sp_c_month_wk_pct,
   sp_rept_need_u_dollars AS rept_units_week,
   SUM(sp_rept_need_u_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rept_units_month,
   COALESCE(sp_rept_need_u_dollars / NULLIF(SUM(sp_rept_need_u_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num
         , channel_country, banner, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS
   rept_mfp_sp_u_month_wk_pct,
   sp_rept_need_lr_c_dollars AS rept_lr_cost_week,
   SUM(sp_rept_need_lr_c_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rept_lr_cost_month,
   COALESCE(sp_rept_need_lr_c_dollars / NULLIF(SUM(sp_rept_need_lr_c_dollars) OVER (PARTITION BY month_idnt,
         fulfill_type_num, channel_country, banner, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ), 0), 0) AS rept_lr_mfp_sp_c_month_wk_pct,
   sp_rept_need_lr_u_dollars AS rept_lr_units_week,
   SUM(sp_rept_need_lr_u_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS rept_lr_units_month,
   COALESCE(sp_rept_need_lr_u_dollars / NULLIF(SUM(sp_rept_need_lr_u_dollars) OVER (PARTITION BY month_idnt,
         fulfill_type_num, channel_country, banner, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ), 0), 0) AS rept_lr_mfp_sp_u_month_wk_pct,
   sp_eop_ttl_c_dollars AS eop_cost_week,
   SUM(CASE
     WHEN week_num_of_fiscal_month = weeks_in_month
     THEN sp_eop_ttl_c_dollars
     ELSE NULL
     END) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS eop_cost_month,
   COALESCE(sp_eop_ttl_c_dollars / NULLIF(SUM(CASE
        WHEN week_num_of_fiscal_month = weeks_in_month
        THEN sp_eop_ttl_c_dollars
        ELSE NULL
        END) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS eop_mfp_sp_c_month_wk_pct,
   sp_eop_ttl_units AS eop_units_week,
   SUM(CASE
     WHEN week_num_of_fiscal_month = weeks_in_month
     THEN sp_eop_ttl_units
     ELSE NULL
     END) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS eop_units_month,
   COALESCE(sp_eop_ttl_units / NULLIF(SUM(CASE
        WHEN week_num_of_fiscal_month = weeks_in_month
        THEN sp_eop_ttl_units
        ELSE NULL
        END) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt RANGE BETWEEN
       UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 0) AS eop_mfp_sp_u_month_wk_pct
  FROM mfp_sp);


-- DROP TABLE category_cluster_sales_plans;


--,net_sales_cost_currency_code AS currency_code


CREATE TEMPORARY TABLE IF NOT EXISTS category_sales_plans_monthly
--CLUSTER BY month_idnt, chnl_idnt, dept_idnt
AS
SELECT m.month_idnt,
 p.fiscal_month_idnt,
 m.month_start_day_date,
 m.month_end_day_date,
  CASE
  WHEN LOWER(p.cluster_name) = LOWER('NORDSTROM_CANADA_STORES')
  THEN 111
  WHEN LOWER(p.cluster_name) = LOWER('NORDSTROM_CANADA_ONLINE')
  THEN 121
  WHEN LOWER(p.cluster_name) = LOWER('NORDSTROM_STORES')
  THEN 110
  WHEN LOWER(p.cluster_name) = LOWER('NORDSTROM_ONLINE')
  THEN 120
  WHEN LOWER(p.cluster_name) = LOWER('RACK_ONLINE')
  THEN 250
  WHEN LOWER(p.cluster_name) = LOWER('RACK_CANADA_STORES')
  THEN 211
  WHEN LOWER(p.cluster_name) IN (LOWER('RACK STORES'), LOWER('PRICE'), LOWER('HYBRID'), LOWER('BRAND'))
  THEN 210
  WHEN LOWER(p.cluster_name) = LOWER('NORD CA RSWH')
  THEN 311
  WHEN LOWER(p.cluster_name) = LOWER('NORD US RSWH')
  THEN 310
  WHEN LOWER(p.cluster_name) = LOWER('RACK CA RSWH')
  THEN 261
  WHEN LOWER(p.cluster_name) = LOWER('RACK US RSWH')
  THEN 260
  ELSE NULL
  END AS chnl_idnt,
 p.banner,
 p.country,
 p.channel,
 p.category,
 p.price_band,
 p.dept_idnt,
 p.alternate_inventory_model,
  CASE
  WHEN LOWER(p.alternate_inventory_model) = LOWER('OWN')
  THEN 3
  WHEN LOWER(p.alternate_inventory_model) = LOWER('DROPSHIP')
  THEN 1
  ELSE NULL
  END AS fulfill_type_num,
  net_sales_cost_currency_code AS currency_code,
 SUM(p.demand_units) AS total_demand_units,
 SUM(p.demand_dollar_amount) AS total_demand_retail_dollars,
 SUM(p.gross_sales_units) AS total_gross_sales_units,
 SUM(p.gross_sales_dollar_amount) AS total_gross_sales_retail_dollars,
 SUM(p.returns_units) AS total_return_units,
 SUM(p.returns_dollar_amount) AS total_return_retail_dollars,
 SUM(p.net_sales_units) AS total_net_sales_units,
 SUM(p.net_sales_retail_amount) AS total_net_sales_retail_dollars,
 SUM(p.net_sales_cost_amount) AS total_net_sales_cost_dollars,
 AVG(p.sales_next_two_month_run_rate) AS next_2months_sales_run_rate,
 MAX(p.dw_sys_load_tmstp) AS qntrx_update_timestamp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_timestamp
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_category_cluster_plan_fact_vw AS p
 INNER JOIN (SELECT DISTINCT month_idnt,
   month_start_day_date,
   month_end_day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS m ON p.fiscal_month_idnt = m.month_idnt
GROUP BY m.month_idnt,
 p.fiscal_month_idnt,
 m.month_start_day_date,
 m.month_end_day_date,
 chnl_idnt,
 p.banner,
 p.country,
 p.channel,
 p.category,
 p.price_band,
 p.dept_idnt,
 p.alternate_inventory_model,
 fulfill_type_num,
 currency_code;


--,ccp.currency_code


CREATE TEMPORARY TABLE IF NOT EXISTS category_sales_plans_weekly
--CLUSTER BY month_idnt, chnl_idnt, dept_idnt
AS
SELECT ccp.month_idnt,
 ccp.month_start_day_date,
 ccp.month_end_day_date,
 mfp.week_idnt,
 mfp.week_start_day_date,
 mfp.week_end_day_date,
 ccp.chnl_idnt,
 ccp.banner,
 ccp.country,
 ccp.channel,
 ccp.dept_idnt,
 ccp.alternate_inventory_model,
 ccp.fulfill_type_num,
 ccp.category,
 ccp.price_band,
 ccp.currency_code,
  ccp.total_demand_units * COALESCE(mfp.demand_mfp_sp_u_month_wk_pct, 1.00 / mfp.weeks_in_month) AS demand_units,
  ccp.total_demand_retail_dollars * COALESCE(mfp.demand_mfp_sp_r_month_wk_pct, 1.00 / mfp.weeks_in_month) AS
 demand_r_dollars,
  ccp.total_gross_sales_units * COALESCE(mfp.gross_sls_u_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) AS
 gross_sls_units,
  ccp.total_gross_sales_retail_dollars * COALESCE(mfp.gross_sls_r_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) AS
 gross_sls_r_dollars,
  ccp.total_return_units * COALESCE(mfp.return_mfp_sp_u_month_wk_pct, 1.00 / mfp.weeks_in_month) AS return_units,
  ccp.total_return_retail_dollars * COALESCE(mfp.return_mfp_sp_r_month_wk_pct, 1.00 / mfp.weeks_in_month) AS
 return_r_dollars,
  ccp.total_net_sales_units * COALESCE(mfp.net_sls_u_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) AS net_sls_units,
  ccp.total_net_sales_retail_dollars * COALESCE(mfp.net_sls_r_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) AS
 net_sls_r_dollars,
  ccp.total_net_sales_cost_dollars * COALESCE(mfp.net_sls_c_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) AS
 net_sls_c_dollars,
 ccp.next_2months_sales_run_rate,
 ccp.qntrx_update_timestamp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_timestamp
FROM category_sales_plans_monthly AS ccp
 LEFT JOIN mfpc_channel_weekly_ratios AS mfp ON ccp.month_idnt = mfp.month_idnt AND ccp.chnl_idnt = mfp.chnl_idnt AND CAST(ccp.dept_idnt AS FLOAT64)
    = mfp.dept_idnt AND ccp.fulfill_type_num = mfp.fulfill_type_num;


-- DROP TABLE category_priceband_banner_cost_plan_monthly;


--,average_inventory_cost_currency_code AS currency_code


CREATE TEMPORARY TABLE IF NOT EXISTS category_inventory_plans_monthly
--CLUSTER BY month_idnt, banner, country, dept_idnt
AS
SELECT m.month_idnt,
 p.fiscal_month_idnt,
 m.month_start_day_date,
 m.month_end_day_date,
 p.alternate_inventory_model,
  CASE
  WHEN LOWER(p.alternate_inventory_model) = LOWER('OWN')
  THEN 3
  WHEN LOWER(p.alternate_inventory_model) = LOWER('DROPSHIP')
  THEN 1
  ELSE NULL
  END AS fulfill_type_num,
  CASE
  WHEN LOWER(p.selling_country) = LOWER('US') AND LOWER(p.selling_brand) = LOWER('NORDSTROM')
  THEN 110
  WHEN LOWER(p.selling_country) = LOWER('CA') AND LOWER(p.selling_brand) = LOWER('NORDSTROM')
  THEN 111
  WHEN LOWER(p.selling_country) = LOWER('US') AND LOWER(p.selling_brand) = LOWER('NORDSTROM_RACK')
  THEN 210
  WHEN LOWER(p.selling_country) = LOWER('CA') AND LOWER(p.selling_brand) = LOWER('NORDSTROM_RACK')
  THEN 211
  ELSE NULL
  END AS chnl_idnt,
 p.selling_country AS country,
 p.selling_brand AS banner,
 p.dept_idnt,
 p.category,
 p.price_band,
 average_inventory_cost_currency_code AS currency_code,
 SUM(p.average_inventory_cost_amount) AS plan_avg_inv_c_dollars,
 SUM(p.average_inventory_retail_amount) AS plan_avg_inv_r_dollars,
 SUM(p.average_inventory_units) AS plan_avg_inv_units,
 SUM(p.beginning_of_period_inventory_cost_amount) AS plan_bop_c_dollars,
 SUM(p.beginning_of_period_inventory_retail_amount) AS plan_bop_r_dollars,
 SUM(p.beginning_of_period_inventory_units) AS plan_bop_c_units,
 SUM(p.average_inventory_cost_amount * 2 - p.beginning_of_period_inventory_cost_amount) AS plan_eop_c_dollars,
 SUM(p.average_inventory_units * 2 - p.beginning_of_period_inventory_units) AS plan_eop_c_units,
 SUM(p.receipts_cost_amount) AS plan_rcpt_c_dollars,
 SUM(p.receipts_retail_amount) AS plan_rcpt_r_dollars,
 SUM(p.receipts_units) AS plan_rcpt_c_units,
 SUM(p.receipts_less_reserve_cost_amount) AS rept_l_rsv_c_dollars,
 SUM(p.receipts_less_reserve_retail_amount) AS rept_l_rsv_r_dollars,
 SUM(p.receipts_less_reserve_units) AS rept_l_rsv_c_units,
 MAX(p.quantrix_update) AS qntrx_update_timestamp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_timestamp
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_category_country_plan_fact_vw AS p
 INNER JOIN (SELECT DISTINCT month_idnt,
   month_start_day_date,
   month_end_day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS m ON p.fiscal_month_idnt = m.month_idnt
GROUP BY m.month_idnt,
 p.fiscal_month_idnt,
 m.month_start_day_date,
 m.month_end_day_date,
 p.alternate_inventory_model,
 fulfill_type_num,
 chnl_idnt,
 country,
 banner,
 p.dept_idnt,
 p.category,
 p.price_band,
 currency_code;


-- DROP TABLE category_inventory_plans_weekly;


--,ccp.currency_code


CREATE TEMPORARY TABLE IF NOT EXISTS category_inventory_plans_weekly
--CLUSTER BY month_idnt, banner, country, dept_idnt
AS
SELECT ccp.month_idnt,
 ccp.month_start_day_date,
 ccp.month_end_day_date,
 mfpcw.week_idnt,
 mfpcw.week_start_day_date,
 mfpcw.week_end_day_date,
 ccp.chnl_idnt,
 ccp.banner,
 ccp.country,
 'STORE' AS channel,
 ccp.dept_idnt,
 ccp.alternate_inventory_model,
 ccp.fulfill_type_num,
 ccp.category,
 ccp.price_band,
 ccp.currency_code,
  ccp.plan_avg_inv_c_dollars * COALESCE(mfpcw.inv_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS avg_inv_ttl_c,
  ccp.plan_avg_inv_units * COALESCE(mfpcw.inv_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS avg_inv_ttl_u,
  CASE
  WHEN mfpcw.week_num_of_fiscal_month = 1
  THEN ccp.plan_bop_c_dollars
  ELSE NULL
  END AS plan_bop_c_dollars,
  CASE
  WHEN mfpcw.week_num_of_fiscal_month = 1
  THEN ccp.plan_bop_c_units
  ELSE NULL
  END AS plan_bop_c_units,
  ccp.plan_eop_c_dollars * COALESCE(mfpcw.eop_mfp_sp_c_month_wk_pct, 1.00) AS plan_eop_c_dollars,
  ccp.plan_eop_c_units * COALESCE(mfpcw.eop_mfp_sp_u_month_wk_pct, 1.00) AS plan_eop_c_units,
  ccp.plan_rcpt_r_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS rcpt_need_r,
  ccp.plan_rcpt_c_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS rcpt_need_c,
  ccp.plan_rcpt_c_units * COALESCE(mfpcw.rept_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS rcpt_need_u,
  ccp.rept_l_rsv_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS
 rcpt_need_lr_r,
  ccp.rept_l_rsv_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS
 rcpt_need_lr_c,
  ccp.rept_l_rsv_c_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS rcpt_need_lr_u
FROM category_inventory_plans_monthly AS ccp
 LEFT JOIN mfp_banner_weekly_ratios AS mfpcw ON ccp.month_idnt = mfpcw.month_idnt AND CAST(ccp.dept_idnt AS FLOAT64) =
      mfpcw.dept_idnt AND ccp.fulfill_type_num = mfpcw.fulfill_type_num AND LOWER(ccp.country) = LOWER(mfpcw.channel_country
     ) AND LOWER(ccp.banner) = LOWER(mfpcw.banner)
WHERE ccp.month_idnt IN (SELECT fiscal_month_idnt
   FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_category_cluster_plan_fact_vw);


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.category_channel_cost_plans_weekly;


--,COALESCE(sls.currency_code, inv.currency_code) AS currency_code


--AND sls.currency_code = inv.currency_code


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.category_channel_cost_plans_weekly
(SELECT COALESCE(sls.month_idnt, inv.month_idnt) AS month_idnt,
  COALESCE(sls.month_start_day_date, inv.month_start_day_date) AS month_start_day_date,
  COALESCE(sls.month_end_day_date, inv.month_end_day_date) AS month_end_day_date,
  COALESCE(sls.week_idnt, inv.week_idnt) AS week_idnt,
  COALESCE(sls.week_start_day_date, inv.week_start_day_date) AS week_start_day_date,
  COALESCE(sls.week_end_day_date, inv.week_end_day_date) AS week_end_day_date,
  COALESCE(sls.chnl_idnt, inv.chnl_idnt) AS chnl_idnt,
  COALESCE(sls.banner, inv.banner) AS banner,
  COALESCE(sls.country, inv.country) AS country,
  COALESCE(sls.channel, inv.channel) AS channel,
  COALESCE(sls.dept_idnt, inv.dept_idnt) AS dept_idnt,
  COALESCE(sls.alternate_inventory_model, inv.alternate_inventory_model) AS alternate_inventory_model,
  COALESCE(sls.fulfill_type_num, inv.fulfill_type_num) AS fulfill_type_num,
  COALESCE(sls.category, inv.category) AS category,
  COALESCE(sls.price_band, inv.price_band) AS price_band,
  COALESCE(sls.currency_code, inv.currency_code) AS currency_code,
  CAST(sls.demand_units AS BIGNUMERIC) AS demand_units,
  cast(sls.demand_r_dollars AS BIGNUMERIC) AS demand_r_dollars,
  cast(sls.gross_sls_units AS BIGNUMERIC) AS gross_sls_units,
  cast(sls.gross_sls_r_dollars AS BIGNUMERIC) AS gross_sls_r_dollars,
  cast(sls.return_units AS BIGNUMERIC) AS return_units,
  cast(sls.return_r_dollars AS BIGNUMERIC) AS return_r_dollars,
  cast(sls.net_sls_units AS BIGNUMERIC) AS net_sls_units,
  cast(sls.net_sls_r_dollars AS BIGNUMERIC) AS net_sls_r_dollars,
  cast(sls.net_sls_c_dollars AS BIGNUMERIC) AS net_sls_c_dollars,
  sls.next_2months_sales_run_rate,
  CAST(inv.avg_inv_ttl_c AS BIGNUMERIC) AS avg_inv_ttl_c,
  CAST(inv.avg_inv_ttl_u AS BIGNUMERIC) avg_inv_ttl_u,
  inv.plan_bop_c_dollars,
  inv.plan_bop_c_units,
  inv.plan_eop_c_dollars,
  inv.plan_eop_c_units,
  cast(inv.rcpt_need_r as BIGNUMERIC) as rcpt_need_r,
  cast(inv.rcpt_need_c as BIGNUMERIC) as rcpt_need_c,
  cast(inv.rcpt_need_u as BIGNUMERIC) as rcpt_need_u,
  cast(inv.rcpt_need_lr_r as BIGNUMERIC) as rcpt_need_lr_r,
  cast(inv.rcpt_need_lr_c as BIGNUMERIC) as rcpt_need_lr_c,
  cast(inv.rcpt_need_lr_u as BIGNUMERIC) as rcpt_need_lr_u,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS update_timestamp_tz
 FROM category_sales_plans_weekly AS sls
  FULL JOIN category_inventory_plans_weekly AS inv ON sls.month_idnt = inv.month_idnt AND sls.month_start_day_date = inv
                 .month_start_day_date AND sls.month_end_day_date = inv.month_end_day_date AND sls.week_idnt = inv.week_idnt
                AND sls.week_start_day_date = inv.week_start_day_date AND sls.week_end_day_date = inv.week_end_day_date
           AND sls.chnl_idnt = inv.chnl_idnt AND LOWER(sls.banner) = LOWER(inv.banner) AND LOWER(sls.country) = LOWER(inv
           .country) AND LOWER(sls.channel) = LOWER(inv.channel) AND LOWER(sls.dept_idnt) = LOWER(inv.dept_idnt) AND
       LOWER(sls.alternate_inventory_model) = LOWER(inv.alternate_inventory_model) AND sls.fulfill_type_num = inv.fulfill_type_num
       AND LOWER(sls.category) = LOWER(inv.category) AND LOWER(sls.price_band) = LOWER(inv.price_band)
	   AND sls.currency_code = inv.currency_code);



-- DROP TABLE suppliergroup_channel_cost_plans_monthly;


--,net_sales_cost_currency_code AS currency_code


CREATE TEMPORARY TABLE IF NOT EXISTS suppliergroup_channel_cost_plans_monthly
--CLUSTER BY month_idnt, chnl_idnt, dept_idnt, category
AS
SELECT m.month_idnt,
 p.fiscal_month_idnt,
 m.month_start_day_date,
 m.month_end_day_date,
 p.alternate_inventory_model,
  CASE
  WHEN LOWER(p.alternate_inventory_model) = LOWER('OWN')
  THEN 3
  WHEN LOWER(p.alternate_inventory_model) = LOWER('DROPSHIP')
  THEN 1
  ELSE NULL
  END AS fulfill_type_num,
  CASE
  WHEN LOWER(p.cluster_name) = LOWER('NORDSTROM_CANADA_STORES')
  THEN 111
  WHEN LOWER(p.cluster_name) = LOWER('NORDSTROM_CANADA_ONLINE')
  THEN 121
  WHEN LOWER(p.cluster_name) = LOWER('NORDSTROM_STORES')
  THEN 110
  WHEN LOWER(p.cluster_name) = LOWER('NORDSTROM_ONLINE')
  THEN 120
  WHEN LOWER(p.cluster_name) = LOWER('RACK_ONLINE')
  THEN 250
  WHEN LOWER(p.cluster_name) = LOWER('RACK_CANADA_STORES')
  THEN 211
  WHEN LOWER(p.cluster_name) IN (LOWER('RACK STORES'), LOWER('PRICE'), LOWER('HYBRID'), LOWER('BRAND'))
  THEN 210
  WHEN LOWER(p.cluster_name) = LOWER('NORD CA RSWH')
  THEN 311
  WHEN LOWER(p.cluster_name) = LOWER('NORD US RSWH')
  THEN 310
  WHEN LOWER(p.cluster_name) = LOWER('RACK CA RSWH')
  THEN 261
  WHEN LOWER(p.cluster_name) = LOWER('RACK US RSWH')
  THEN 260
  ELSE NULL
  END AS chnl_idnt,
 p.selling_brand AS banner,
 p.selling_country AS country,
 p.dept_idnt,
 p.category,
 p.supplier_group,
 net_sales_cost_currency_code AS currency_code,
 SUM(p.demand_units) AS total_demand_units,
 SUM(p.demand_dollar_amount) AS total_demand_retail_dollars,
 SUM(p.gross_sales_units) AS total_gross_sales_units,
 SUM(p.gross_sales_dollar_amount) AS total_gross_sales_retail_dollars,
 SUM(p.returns_units) AS total_return_units,
 SUM(p.returns_dollar_amount) AS total_return_retail_dollars,
 SUM(p.net_sales_units) AS total_net_sales_units,
 SUM(p.net_sales_retail_amount) AS total_net_sales_retail_dollars,
 SUM(p.net_sales_cost_amount) AS total_net_sales_cost_dollars,
 AVG(p.sales_next_two_month_run_rate) AS next_2months_sales_run_rate,
 SUM(p.average_inventory_cost_amount) AS plan_avg_inv_c_dollars,
 SUM(p.average_inventory_retail_amount) AS plan_avg_inv_r_dollars,
 SUM(p.average_inventory_units) AS plan_avg_inv_units,
 SUM(p.beginning_of_period_inventory_cost_amount) AS plan_bop_c_dollars,
 SUM(p.beginning_of_period_inventory_retail_amount) AS plan_bop_r_dollars,
 SUM(p.beginning_of_period_inventory_units) AS plan_bop_c_units,
 SUM(p.average_inventory_cost_amount * 2 - p.beginning_of_period_inventory_cost_amount) AS plan_eop_c_dollars,
 SUM(p.average_inventory_units * 2 - p.beginning_of_period_inventory_units) AS plan_eop_c_units,
 SUM(p.replenishment_receipts_cost_amount) AS plan_rp_rcpt_c_dollars,
 SUM(p.replenishment_receipts_retail_amount) AS plan_rp_rcpt_r_dollars,
 SUM(p.replenishment_receipts_units) AS plan_rp_rcpt_units,
 SUM(p.replenishment_receipts_less_reserve_cost_amount) AS plan_rp_rcpt_lr_c_dollars,
 SUM(p.replenishment_receipts_less_reserve_retail_amount) AS plan_rp_rcpt_lr_r_dollars,
 SUM(p.replenishment_receipts_less_reserve_units) AS plan_rp_rcpt_lr_units,
 SUM(p.nonreplenishment_receipts_cost_amount) AS plan_nrp_rcpt_c_dollars,
 SUM(p.nonreplenishment_receipts_retail_amount) AS plan_nrp_rcpt_r_dollars,
 SUM(p.nonreplenishment_receipts_units) AS plan_nrp_rcpt_units,
 SUM(p.nonreplenishment_receipts_less_reserve_cost_amount) AS plan_nrp_rcpt_lr_c_dollars,
 SUM(p.nonreplenishment_receipts_less_reserve_retail_amount) AS plan_nrp_rcpt_lr_r_dollars,
 SUM(p.nonreplenishment_receipts_less_reserve_units) AS plan_nrp_rcpt_lr_units,
 SUM(p.active_inventory_in_cost_amount) AS plan_pah_in_c_dollars,
 SUM(p.active_inventory_in_retail_amount) AS plan_pah_in_r_dollars,
 SUM(p.active_inventory_in_units) AS plan_pah_in_units,
 SUM(p.plannable_inventory_cost_amount) AS plan_rcpt_need_c_dollars,
 SUM(p.plannable_inventory_retail_amount) AS plan_rcpt_need_r_dollars,
 SUM(p.plannable_inventory_units) AS plan_rcpt_need_c_units,
 SUM(p.plannable_inventory_receipt_less_reserve_cost_amount) AS plan_rcpt_l_rs_c_dollars,
 SUM(p.plannable_inventory_receipt_less_reserve_retail_amount) AS plan_rcpt_l_rs_r_dollars,
 SUM(p.plannable_inventory_receipt_less_reserve_units) AS plan_rcpt_l_rs_c_units
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_fact_vw AS p
 INNER JOIN (SELECT DISTINCT month_idnt,
   month_start_day_date,
   month_end_day_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS m ON p.fiscal_month_idnt = m.month_idnt
GROUP BY m.month_idnt,
 p.fiscal_month_idnt,
 m.month_start_day_date,
 m.month_end_day_date,
 p.alternate_inventory_model,
 fulfill_type_num,
 chnl_idnt,
 banner,
 country,
 p.dept_idnt,
 p.category,
 p.supplier_group,
 currency_code;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.suppliergroup_channel_cost_plans_weekly;


--,supp.currency_code


-- sales


-- inventory


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.suppliergroup_channel_cost_plans_weekly
(SELECT supp.month_idnt,
  supp.month_start_day_date,
  supp.month_end_day_date,
  COALESCE(mfp.week_idnt, mfpcw.week_idnt) AS week_idnt,
  COALESCE(mfp.week_start_day_date, mfpcw.week_start_day_date) AS week_start_day_date,
  COALESCE(mfp.week_end_day_date, mfpcw.week_end_day_date) AS week_end_day_date,
  supp.chnl_idnt,
  supp.banner,
  supp.country,
  supp.dept_idnt,
  supp.alternate_inventory_model,
  supp.fulfill_type_num,
  supp.category,
  supp.supplier_group,
  supp.currency_code,
  CAST(supp.total_demand_units * COALESCE(mfp.demand_mfp_sp_u_month_wk_pct, 1.00 / mfp.weeks_in_month) AS BIGNUMERIC) AS
  demand_units,
   cast(supp.total_demand_retail_dollars * COALESCE(mfp.demand_mfp_sp_r_month_wk_pct, 1.00 / mfp.weeks_in_month) as bignumeric) AS
  demand_r_dollars,
   cast(supp.total_gross_sales_units * COALESCE(mfp.gross_sls_u_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) as bignumeric) AS
  gross_sls_units,
   cast(supp.total_gross_sales_retail_dollars * COALESCE(mfp.gross_sls_r_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) as bignumeric) AS
  gross_sls_r_dollars,
   cast(supp.total_return_units * COALESCE(mfp.return_mfp_sp_u_month_wk_pct, 1.00 / mfp.weeks_in_month) as bignumeric) AS return_units,
   cast(supp.total_return_retail_dollars * COALESCE(mfp.return_mfp_sp_r_month_wk_pct, 1.00 / mfp.weeks_in_month) as bignumeric) AS
  return_r_dollars,
   cast(supp.total_net_sales_units * COALESCE(mfp.net_sls_u_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) as bignumeric) AS net_sls_units
  ,
   cast( supp.total_net_sales_retail_dollars * COALESCE(mfp.net_sls_r_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) as bignumeric) AS
  net_sls_r_dollars,
   cast(supp.total_net_sales_cost_dollars * COALESCE(mfp.net_sls_c_mfp_sp_month_wk_pct, 1.00 / mfp.weeks_in_month) as bignumeric) AS
  net_sls_c_dollars,
  supp.next_2months_sales_run_rate,
  CAST(supp.plan_avg_inv_c_dollars * COALESCE(mfpcw.inv_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) AS bignumeric)
  AS avg_inv_ttl_c,
   cast(supp.plan_avg_inv_units * COALESCE(mfpcw.inv_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS avg_inv_ttl_u,
   CASE
   WHEN mfpcw.week_num_of_fiscal_month = 1
   THEN supp.plan_bop_c_dollars
   ELSE NULL
   END AS plan_bop_c_dollars,
   CASE
   WHEN mfpcw.week_num_of_fiscal_month = 1
   THEN supp.plan_bop_c_units
   ELSE NULL
   END AS plan_bop_c_units,
   supp.plan_eop_c_dollars * COALESCE(mfpcw.eop_mfp_sp_c_month_wk_pct, 1.00) AS plan_eop_c_dollars,
   supp.plan_eop_c_units * COALESCE(mfpcw.eop_mfp_sp_u_month_wk_pct, 1.00) AS plan_eop_c_units,
   cast(supp.plan_rp_rcpt_r_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_rp_rcpt_r,
   cast(supp.plan_rp_rcpt_c_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_rp_rcpt_c,
   cast(supp.plan_rp_rcpt_units * COALESCE(mfpcw.rept_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS plan_rp_rcpt_u,
   cast(supp.plan_rp_rcpt_lr_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_rp_rcpt_lr_r,
   cast(supp.plan_rp_rcpt_lr_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_rp_rcpt_lr_c,
   cast(supp.plan_rp_rcpt_lr_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_rp_rcpt_lr_u,
   cast(supp.plan_nrp_rcpt_r_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_nrp_rcpt_r,
   cast(supp.plan_nrp_rcpt_c_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_nrp_rcpt_c,
   cast(supp.plan_nrp_rcpt_units * COALESCE(mfpcw.rept_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS plan_nrp_rcpt_u
  ,
   cast(supp.plan_nrp_rcpt_lr_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_nrp_rcpt_lr_r,
   cast(supp.plan_nrp_rcpt_lr_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_nrp_rcpt_lr_c,
   cast(supp.plan_nrp_rcpt_lr_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_nrp_rcpt_lr_u,
   cast(supp.plan_rcpt_need_r_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  rcpt_need_r,
   cast(supp.plan_rcpt_need_c_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  rcpt_need_c,
   cast(supp.plan_rcpt_need_c_units * COALESCE(mfpcw.rept_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS rcpt_need_u
  ,
   cast(supp.plan_rcpt_l_rs_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  rcpt_need_lr_r,
   cast(supp.plan_rcpt_l_rs_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  rcpt_need_lr_c,
   cast(supp.plan_rcpt_l_rs_c_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  rcpt_need_lr_u,
   cast(supp.plan_pah_in_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_pah_in_r,
   cast(supp.plan_pah_in_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS
  plan_pah_in_c,
   cast(supp.plan_pah_in_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct, 1.00 / mfpcw.weeks_in_month) as bignumeric) AS plan_pah_in_u
  ,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP)  AS update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS update_timestamp_tz
 FROM suppliergroup_channel_cost_plans_monthly AS supp
  LEFT JOIN mfpc_channel_weekly_ratios AS mfp ON supp.month_idnt = mfp.month_idnt AND supp.chnl_idnt = mfp.chnl_idnt AND CAST(supp.dept_idnt AS FLOAT64)
     = mfp.dept_idnt AND supp.fulfill_type_num = mfp.fulfill_type_num
  LEFT JOIN mfp_banner_weekly_ratios AS mfpcw ON supp.month_idnt = mfpcw.month_idnt AND CAST(supp.dept_idnt AS FLOAT64)
        = mfpcw.dept_idnt AND supp.fulfill_type_num = mfpcw.fulfill_type_num AND LOWER(supp.country) = LOWER(mfpcw.channel_country
       ) AND LOWER(supp.banner) = LOWER(mfpcw.banner) AND mfp.week_idnt = mfpcw.week_idnt);