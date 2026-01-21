/*
APT Cost Plan Weekly Main
Author: Sara Riker
Date Created: 1/19/22
Date Updated: 7/6/23

Datalab: t2dl_das_apt_cost_reporting
Creates Tables:
    - category_channel_cost_plans_weekly
    - suppliergroup_channel_cost_plans_weekly
*/

-- for views to pull in correct current month data
DELETE FROM {environment_schema}.current_month ALL;
INSERT INTO {environment_schema}.current_month
    SELECT DISTINCT
        month_idnt 
    FROM prd_nap_usr_vws.day_cal_454_dim 
    WHERE day_date = current_date
;

CREATE MULTISET VOLATILE TABLE dates_lkup_ccp AS (
SELECT DISTINCT
     week_idnt
    ,week_start_day_date
    ,week_end_day_date
    ,month_idnt
    ,week_num_of_fiscal_month
    ,MAX(week_num_of_fiscal_month) OVER (PARTITION BY month_idnt) AS weeks_in_month
    ,CASE WHEN week_start_day_idnt = month_start_day_idnt THEN 1 ELSE 0 END as month_week_start_ind
    ,fiscal_year_num
FROM prd_nap_usr_vws.day_cal_454_dim
WHERE month_idnt in (SELECT fiscal_month_idnt FROM t2dl_das_apt_cost_reporting.merch_assortment_category_cluster_plan_fact_vw)
)WITH DATA
PRIMARY INDEX(month_idnt, week_idnt) ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE mfpc_channel_weekly_ratios AS (
SELECT DISTINCT
     dc.month_idnt
    ,mfpc.week_num AS week_idnt
    ,dc.week_start_day_date
    ,dc.week_end_day_date
    ,dc.weeks_in_month
    ,mfpc.fulfill_type_num
    ,channel_num AS chnl_idnt
    ,dept_num AS dept_idnt
    -- demand retail
    ,SUM(sp_demand_total_retail_amt) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num)  AS demand_r_week
    ,SUM(sp_demand_total_retail_amt) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS demand_r_month
    ,COALESCE(demand_r_week / NULLIF(demand_r_month, 0), 0) AS demand_mfp_sp_r_month_wk_pct
    -- demand units
    ,SUM(sp_demand_total_qty) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num)  AS demand_u_week
    ,SUM(sp_demand_total_qty) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS demand_u_month
    ,COALESCE(demand_u_week / NULLIF(demand_u_month, 0), 0) AS demand_mfp_sp_u_month_wk_pct
    -- Gross sales retail
    ,SUM(sp_gross_sales_retail_amt) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num)  AS gross_sls_r_week
    ,SUM(sp_gross_sales_retail_amt) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS gross_sls_r_month
    ,COALESCE(gross_sls_r_week / NULLIF(gross_sls_r_month, 0), 0) AS gross_sls_r_mfp_sp_month_wk_pct
    -- Gross sales units
    ,SUM(sp_gross_sales_qty) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num)  AS gross_sls_u_week
    ,SUM(sp_gross_sales_qty) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS gross_sls_u_month
    ,COALESCE(gross_sls_u_week / NULLIF(gross_sls_u_month, 0), 0) AS gross_sls_u_mfp_sp_month_wk_pct
    -- returns retail
    ,SUM(sp_returns_retail_amt) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num)  AS return_r_week
    ,SUM(sp_returns_retail_amt) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS return_r_month
    ,COALESCE(return_r_week / NULLIF(return_r_month, 0), 0) AS return_mfp_sp_r_month_wk_pct
    -- returns units
    ,SUM(sp_returns_qty) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num)  AS return_u_week
    ,SUM(sp_returns_qty) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS return_u_month
    ,COALESCE(return_u_week / NULLIF(return_u_month, 0), 0) AS return_mfp_sp_u_month_wk_pct
    -- net sales retail
    ,SUM(sp_net_sales_retail_amt) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num) AS net_sls_r_week
    ,SUM(sp_net_sales_retail_amt) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS net_sls_r_month
    ,COALESCE(net_sls_r_week / NULLIF(net_sls_r_month, 0), 0) AS net_sls_r_mfp_sp_month_wk_pct
    -- net sales cost
    ,SUM(sp_net_sales_cost_amt) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num) AS net_sls_c_week
    ,SUM(sp_net_sales_cost_amt) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS net_sls_c_month
    ,COALESCE(net_sls_c_week / NULLIF(net_sls_c_month, 0), 0) AS net_sls_c_mfp_sp_month_wk_pct
    -- net sales untis
    ,SUM(sp_net_sales_qty) OVER (PARTITION BY dc.week_idnt, fulfill_type_num, channel_num, dept_num) AS net_sls_u_week
    ,SUM(sp_net_sales_qty) OVER (PARTITION BY dc.month_idnt, fulfill_type_num, channel_num, dept_num) AS net_sls_u_month
    ,COALESCE(net_sls_u_week / NULLIF(net_sls_u_month, 0), 0) AS net_sls_u_mfp_sp_month_wk_pct
FROM prd_nap_vws.mfp_cost_plan_actual_channel_fact mfpc
JOIN dates_lkup_ccp dc
  ON mfpc.week_num = dc.week_idnt
WHERE fulfill_type_num IN (1,3)
) WITH DATA
PRIMARY INDEX (month_idnt, chnl_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;


-- DROP TABLE mfp_banner_weekly_ratios;
CREATE MULTISET VOLATILE TABLE mfp_banner_weekly_ratios AS (
WITH mfp_sp AS (
    SELECT
         dc.month_idnt
        ,mfpc.week_num as week_idnt
        ,dc.week_start_day_date
        ,dc.week_end_day_date
        ,week_num_of_fiscal_month
        ,weeks_in_month
        ,CASE WHEN (banner_country_num = 1 OR banner_country_num  = 3) THEN 'US'
              WHEN (banner_country_num = 2 OR banner_country_num  = 4) THEN 'CA'
            END AS channel_country
        ,CASE WHEN banner_country_num = 1 OR banner_country_num  = 2 THEN 'NORDSTROM'
              WHEN banner_country_num = 3 OR banner_country_num  = 4 THEN 'NORDSTROM_RACK'
            END AS banner
        ,fulfill_type_num
        ,mfpc.dept_num AS dept_idnt
        ,SUM(sp_beginning_of_period_active_cost_amt + sp_beginning_of_period_inactive_cost_amt) AS sp_bop_ttl_c_dollars
        ,SUM(sp_ending_of_period_active_cost_amt + sp_ending_of_period_inactive_cost_amt) AS sp_eop_ttl_c_dollars
        ,SUM(sp_ending_of_period_active_qty + sp_ending_of_period_inactive_qty) AS sp_eop_ttl_units
        ----from Jeff Akerman - APT don't plan Inactive Receipts, so in order to tie back to MFP Cp, need to remove inactive receipt from total receipt calculation
        ,SUM(sp_receipts_active_cost_amt) AS sp_rept_need_c_dollars
        ,SUM(sp_receipts_active_qty) AS sp_rept_need_u_dollars
        ,SUM(sp_receipts_active_cost_amt - sp_receipts_reserve_cost_amt) AS sp_rept_need_lr_c_dollars
        ,SUM(sp_receipts_active_qty - sp_receipts_reserve_qty) AS sp_rept_need_lr_u_dollars
    FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact mfpc
    JOIN dates_lkup_ccp dc
      ON mfpc.week_num = dc.week_idnt
    WHERE fulfill_type_num IN (1,3)
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT DISTINCT 
     month_idnt
    ,week_idnt
    ,week_start_day_date
    ,week_end_day_date
    ,week_num_of_fiscal_month
    ,weeks_in_month
    ,fulfill_type_num
    ,channel_country
    ,banner
    ,dept_idnt
    -- avg inv cost
    ,(sp_bop_ttl_c_dollars + sp_eop_ttl_c_dollars)/2 AS avg_inv_cost_week
    ,SUM(avg_inv_cost_week) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt) AS avg_inv_cost_month
    ,COALESCE(avg_inv_cost_week / NULLIF(avg_inv_cost_month, 0), 0) AS inv_mfp_sp_c_month_wk_pct
    -- receipt cost
    ,sp_rept_need_c_dollars as rept_cost_week
    ,SUM(sp_rept_need_c_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt) AS rept_cost_month
    ,COALESCE(rept_cost_week / NULLIF(rept_cost_month, 0), 0) AS rept_mfp_sp_c_month_wk_pct
    -- receipt units
    ,sp_rept_need_u_dollars as rept_units_week
    ,SUM(sp_rept_need_u_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt) AS rept_units_month
    ,COALESCE(rept_units_week / NULLIF(rept_units_month, 0), 0) AS rept_mfp_sp_u_month_wk_pct
    -- receitps less reserve cost
    ,sp_rept_need_lr_c_dollars as rept_lr_cost_week
    ,SUM(sp_rept_need_lr_c_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt) AS rept_lr_cost_month
    ,COALESCE(rept_lr_cost_week / NULLIF(rept_lr_cost_month, 0), 0) AS rept_lr_mfp_sp_c_month_wk_pct
    -- receitps less reserve units
    ,sp_rept_need_lr_u_dollars as rept_lr_units_week
    ,SUM(sp_rept_need_lr_u_dollars) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt) AS rept_lr_units_month
    ,COALESCE(rept_lr_units_week / NULLIF(rept_lr_units_month, 0), 0) AS rept_lr_mfp_sp_u_month_wk_pct
    -- EOH C
    ,sp_eop_ttl_c_dollars as eop_cost_week
    ,SUM(CASE WHEN week_num_of_fiscal_month = weeks_in_month THEN sp_eop_ttl_c_dollars END) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt) AS eop_cost_month
    ,COALESCE(eop_cost_week / NULLIF(eop_cost_month, 0), 0) AS eop_mfp_sp_c_month_wk_pct
    -- EOH U
    ,sp_eop_ttl_units as eop_units_week
    ,SUM(CASE WHEN week_num_of_fiscal_month = weeks_in_month THEN sp_eop_ttl_units END) OVER (PARTITION BY month_idnt, fulfill_type_num, channel_country, banner, dept_idnt) AS eop_units_month
    ,COALESCE(eop_units_week / NULLIF(eop_units_month, 0), 0) AS eop_mfp_sp_u_month_wk_pct
FROM mfp_sp
)WITH DATA
PRIMARY INDEX (week_idnt, month_idnt, banner, dept_idnt) 
ON COMMIT PRESERVE ROWS;



-- DROP TABLE category_cluster_sales_plans;
CREATE MULTISET VOLATILE TABLE category_sales_plans_monthly AS (
SELECT
        m.month_idnt
    ,m.month_start_day_date
    ,m.month_end_day_date
    ,CASE WHEN cluster_name = 'NORDSTROM_CANADA_STORES' THEN 111
          WHEN cluster_name = 'NORDSTROM_CANADA_ONLINE' THEN 121
          WHEN cluster_name = 'NORDSTROM_STORES' THEN 110
          WHEN cluster_name = 'NORDSTROM_ONLINE' THEN 120
          WHEN cluster_name = 'RACK_ONLINE' THEN 250
          WHEN cluster_name = 'RACK_CANADA_STORES' THEN 211
          WHEN cluster_name IN ('RACK STORES', 'PRICE','HYBRID','BRAND') THEN 210
          WHEN cluster_name = 'NORD CA RSWH' THEN 311
          WHEN cluster_name = 'NORD US RSWH' THEN 310
          WHEN cluster_name = 'RACK CA RSWH' THEN 261
          WHEN cluster_name = 'RACK US RSWH' THEN 260
        END AS chnl_idnt    
    ,banner
    ,country
    ,channel
    ,category
    ,price_band
    ,dept_idnt
    ,alternate_inventory_model
    ,CASE WHEN alternate_inventory_model = 'OWN' THEN 3
          WHEN alternate_inventory_model = 'DROPSHIP' THEN 1
        END AS fulfill_type_num
    ,net_sales_cost_currency_code AS currency_code
    ,SUM(p.demand_units) AS total_demand_units
    ,SUM(p.demand_dollar_amount) AS total_demand_retail_dollars
    ,SUM(p.gross_sales_units) AS total_gross_sales_units
    ,SUM(p.gross_sales_dollar_amount) AS total_gross_sales_retail_dollars
    ,SUM(p.returns_units) as total_return_units
    ,SUM(p.returns_dollar_amount) AS total_return_retail_dollars
    ,SUM(p.net_sales_units) AS total_net_sales_units
    ,SUM(p.net_sales_retail_amount) AS total_net_sales_retail_dollars
    ,SUM(p.net_sales_cost_amount) AS total_net_sales_cost_dollars
    ,AVG(p.sales_next_two_month_run_rate) AS next_2months_sales_run_rate
    ,MAX(p.dw_sys_load_tmstp) AS qntrx_update_timestamp
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM t2dl_das_apt_cost_reporting.merch_assortment_category_cluster_plan_fact_vw p
JOIN (
        SELECT DISTINCT
             month_idnt
            ,month_start_day_date
            ,month_end_day_date
        FROM prd_nap_usr_vws.day_cal_454_dim
     ) m
  ON p.fiscal_month_idnt = m.month_idnt
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) WITH DATA
PRIMARY INDEX (month_idnt, chnl_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE category_sales_plans_weekly AS (
SELECT 
     ccp.month_idnt
    ,ccp.month_start_day_date
    ,ccp.month_end_day_date
    ,mfp.week_idnt
    ,mfp.week_start_day_date
    ,mfp.week_end_day_date
    ,ccp.chnl_idnt    
    ,ccp.banner
    ,ccp.country
    ,ccp.channel
    ,ccp.dept_idnt
    ,ccp.alternate_inventory_model
    ,ccp.fulfill_type_num
    ,ccp.category
    ,ccp.price_band
    ,ccp.currency_code
    ,ccp.total_demand_units * COALESCE(mfp.demand_mfp_sp_u_month_wk_pct, 1.00/weeks_in_month) AS demand_units
    ,ccp.total_demand_retail_dollars * COALESCE(mfp.demand_mfp_sp_r_month_wk_pct, 1.00/weeks_in_month) AS demand_r_dollars
    ,ccp.total_gross_sales_units * COALESCE(mfp.gross_sls_u_mfp_sp_month_wk_pct, 1.00/weeks_in_month) AS gross_sls_units
    ,ccp.total_gross_sales_retail_dollars * COALESCE(mfp.gross_sls_r_mfp_sp_month_wk_pct, 1.00/weeks_in_month) AS gross_sls_r_dollars
    ,ccp.total_return_units * COALESCE(mfp.return_mfp_sp_u_month_wk_pct, 1.00/weeks_in_month) AS return_units
    ,ccp.total_return_retail_dollars * COALESCE(mfp.return_mfp_sp_r_month_wk_pct, 1.00/weeks_in_month) AS return_r_dollars
    ,ccp.total_net_sales_units * COALESCE(mfp.net_sls_u_mfp_sp_month_wk_pct, 1.00/weeks_in_month) AS net_sls_units
    ,ccp.total_net_sales_retail_dollars * COALESCE(mfp.net_sls_r_mfp_sp_month_wk_pct, 1.00/weeks_in_month) AS net_sls_r_dollars
    ,ccp.total_net_sales_cost_dollars * COALESCE(mfp.net_sls_c_mfp_sp_month_wk_pct, 1.00/weeks_in_month) AS net_sls_c_dollars
    ,ccp.next_2months_sales_run_rate
    ,ccp.qntrx_update_timestamp
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM category_sales_plans_monthly ccp
LEFT JOIN mfpc_channel_weekly_ratios mfp
  ON ccp.month_idnt = mfp.month_idnt
 AND ccp.chnl_idnt = mfp.chnl_idnt
 AND ccp.dept_idnt = mfp.dept_idnt
 AND ccp.fulfill_type_num = mfp.fulfill_type_num
) WITH DATA 
PRIMARY INDEX (month_idnt, chnl_idnt, dept_idnt) ON COMMIT PRESERVE ROWS
;

-- DROP TABLE category_priceband_banner_cost_plan_monthly;
CREATE MULTISET VOLATILE TABLE category_inventory_plans_monthly AS (
SELECT
     m.month_idnt
    ,m.month_start_day_date
    ,m.month_end_day_date
    ,alternate_inventory_model
    ,CASE WHEN alternate_inventory_model = 'OWN' THEN 3
    WHEN alternate_inventory_model = 'DROPSHIP' THEN 1
    END AS fulfill_type_num
    ,CASE WHEN selling_country = 'US' AND selling_brand = 'NORDSTROM' THEN 110
          WHEN selling_country = 'CA' AND selling_brand = 'NORDSTROM' THEN 111
          WHEN selling_country = 'US' AND selling_brand = 'NORDSTROM_RACK' THEN 210
          WHEN selling_country = 'CA' AND selling_brand = 'NORDSTROM_RACK' THEN 211
        END AS chnl_idnt 
    ,selling_country AS country
    ,p.selling_brand AS banner
    ,dept_idnt
    ,p.category AS category
    ,p.price_band AS price_band
    ,average_inventory_cost_currency_code AS currency_code
    ,SUM(p.average_inventory_cost_amount) AS plan_avg_inv_c_dollars
    ,SUM(P.average_inventory_retail_amount) AS plan_avg_inv_r_dollars
    ,SUM(p.average_inventory_units) AS plan_avg_inv_units
    ,SUM(p.beginning_of_period_inventory_cost_amount) AS plan_bop_c_dollars
    ,SUM(P.beginning_of_period_inventory_retail_amount) AS plan_bop_r_dollars
    ,SUM(p.beginning_of_period_inventory_units) AS plan_bop_c_units
    ,SUM(p.average_inventory_cost_amount*2 - p.beginning_of_period_inventory_cost_amount) AS plan_eop_c_dollars
    ,SUM(p.average_inventory_units*2 - p.beginning_of_period_inventory_units) AS plan_eop_c_units
    ,SUM(p.receipts_cost_amount) AS plan_rcpt_c_dollars
    ,SUM(P.receipts_retail_amount) AS plan_rcpt_r_dollars
    ,SUM(p.receipts_units) AS plan_rcpt_c_units
    ,SUM(p.receipts_less_reserve_cost_amount) AS rept_l_rsv_c_dollars
    ,SUM(p.receipts_less_reserve_retail_amount) AS rept_l_rsv_r_dollars
    ,SUM(p.receipts_less_reserve_units) AS rept_l_rsv_c_units
    ,MAX(p.quantrix_update) AS qntrx_update_timestamp
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM t2dl_das_apt_cost_reporting.merch_assortment_category_country_plan_fact_vw p
JOIN (
        SELECT DISTINCT
             month_idnt
            ,month_start_day_date
            ,month_end_day_date
        FROM prd_nap_usr_vws.day_cal_454_dim
     ) m
  ON p.fiscal_month_idnt = m.month_idnt
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
WITH DATA
PRIMARY INDEX (month_idnt, banner, country, dept_idnt, category,price_band, fulfill_type_num) ON COMMIT PRESERVE ROWS;


-- DROP TABLE category_inventory_plans_weekly;
CREATE MULTISET VOLATILE TABLE category_inventory_plans_weekly AS (
SELECT
     ccp.month_idnt
    ,ccp.month_start_day_date
    ,ccp.month_end_day_date
    ,mfpcw.week_idnt
    ,mfpcw.week_start_day_date
    ,mfpcw.week_end_day_date
    ,ccp.chnl_idnt    
    ,ccp.banner
    ,ccp.country
    ,'STORE' AS channel
    ,ccp.dept_idnt
    ,ccp.alternate_inventory_model
    ,ccp.fulfill_type_num
    ,ccp.category
    ,ccp.price_band
    ,ccp.currency_code
    ,plan_avg_inv_c_dollars * COALESCE(mfpcw.inv_mfp_sp_c_month_wk_pct, 1.00/weeks_in_month) AS avg_inv_ttl_c
    ,plan_avg_inv_units *COALESCE( mfpcw.inv_mfp_sp_c_month_wk_pct, 1.00/weeks_in_month) AS avg_inv_ttl_u
    ,CASE WHEN week_num_of_fiscal_month = 1 THEN ccp.plan_bop_c_dollars END AS plan_bop_c_dollars
    ,CASE WHEN week_num_of_fiscal_month = 1 THEN ccp.plan_bop_c_units END AS plan_bop_c_units
    ,ccp.plan_eop_c_dollars * COALESCE(mfpcw.eop_mfp_sp_c_month_wk_pct,1.00) AS plan_eop_c_dollars
    ,ccp.plan_eop_c_units * COALESCE(mfpcw.eop_mfp_sp_u_month_wk_pct,1.00) AS plan_eop_c_units
    ,plan_rcpt_r_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00/weeks_in_month) AS rcpt_need_r
    ,plan_rcpt_c_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct, 1.00/weeks_in_month) AS rcpt_need_c
    ,plan_rcpt_c_units * COALESCE(mfpcw.rept_mfp_sp_u_month_wk_pct, 1.00/weeks_in_month) AS rcpt_need_u
    ,rept_l_rsv_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00/weeks_in_month) AS rcpt_need_lr_r
    ,rept_l_rsv_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct, 1.00/weeks_in_month) AS rcpt_need_lr_c
    ,rept_l_rsv_c_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct, 1.00/weeks_in_month) AS rcpt_need_lr_u
FROM category_inventory_plans_monthly ccp
LEFT JOIN mfp_banner_weekly_ratios mfpcw
  ON ccp.month_idnt = mfpcw.month_idnt
 AND ccp.dept_idnt = mfpcw.dept_idnt
 AND ccp.fulfill_type_num = mfpcw.fulfill_type_num
 AND ccp.country = mfpcw.channel_country 
 AND ccp.banner = mfpcw.banner
WHERE ccp.month_idnt in (SELECT fiscal_month_idnt FROM t2dl_das_apt_cost_reporting.merch_assortment_category_cluster_plan_fact_vw)
) 
WITH DATA
PRIMARY INDEX (month_idnt, banner, country, dept_idnt, category,price_band, fulfill_type_num) ON COMMIT PRESERVE ROWS;

DELETE FROM {environment_schema}.category_channel_cost_plans_weekly ALL;
INSERT INTO {environment_schema}.category_channel_cost_plans_weekly 
SELECT 
     COALESCE(sls.month_idnt, inv.month_idnt) AS month_idnt                                                                
    ,COALESCE(sls.month_start_day_date, inv.month_start_day_date) AS month_start_day_date                                                      
    ,COALESCE(sls.month_end_day_date, inv.month_end_day_date) AS month_end_day_date                                                        
    ,COALESCE(sls.week_idnt, inv.week_idnt) AS week_idnt                                                                 
    ,COALESCE(sls.week_start_day_date, inv.week_start_day_date) AS week_start_day_date                                                       
    ,COALESCE(sls.week_end_day_date, inv.week_end_day_date) AS week_end_day_date                                                         
    ,COALESCE(sls.chnl_idnt, inv.chnl_idnt) AS chnl_idnt                                                                 
    ,COALESCE(sls.banner, inv.banner) AS banner                                                                    
    ,COALESCE(sls.country, inv.country) AS country                                                                   
    ,COALESCE(sls.channel, inv.channel) AS channel                                                                   
    ,COALESCE(sls.dept_idnt, inv.dept_idnt) AS dept_idnt                                                                 
    ,COALESCE(sls.alternate_inventory_model , inv.alternate_inventory_model ) AS alternate_inventory_model                                                 
    ,COALESCE(sls.fulfill_type_num, inv.fulfill_type_num) AS fulfill_type_num                                                          
    ,COALESCE(sls.category, inv.category) AS category                                                                  
    ,COALESCE(sls.price_band, inv.price_band) AS price_band                                                                
    ,COALESCE(sls.currency_code, inv.currency_code) AS currency_code
    ,sls.demand_units
    ,sls.demand_r_dollars
    ,sls.gross_sls_units
    ,sls.gross_sls_r_dollars
    ,sls.return_units
    ,sls.return_r_dollars
    ,sls.net_sls_units
    ,sls.net_sls_r_dollars
    ,sls.net_sls_c_dollars
    ,sls.next_2months_sales_run_rate
    ,inv.avg_inv_ttl_c
    ,inv.avg_inv_ttl_u
    ,inv.plan_bop_c_dollars
    ,inv.plan_bop_c_units
    ,inv.plan_eop_c_dollars
    ,inv.plan_eop_c_units
    ,inv.rcpt_need_r
    ,inv.rcpt_need_c
    ,inv.rcpt_need_u
    ,inv.rcpt_need_lr_r
    ,inv.rcpt_need_lr_c
    ,inv.rcpt_need_lr_u
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM category_sales_plans_weekly sls
FULL OUTER JOIN category_inventory_plans_weekly inv
  ON sls.month_idnt = inv.month_idnt
 AND sls.month_start_day_date = inv.month_start_day_date
 AND sls.month_end_day_date = inv.month_end_day_date
 AND sls.week_idnt = inv.week_idnt
 AND sls.week_start_day_date = inv.week_start_day_date
 AND sls.week_end_day_date = inv.week_end_day_date
 AND sls.chnl_idnt = inv.chnl_idnt    
 AND sls.banner = inv.banner
 AND sls.country = inv.country
 AND sls.channel = inv.channel
 AND sls.dept_idnt = inv.dept_idnt
 AND sls.alternate_inventory_model = inv.alternate_inventory_model
 AND sls.fulfill_type_num = inv.fulfill_type_num
 AND sls.category = inv.category
 AND sls.price_band = inv.price_band
 AND sls.currency_code = inv.currency_code
;

COLLECT STATS
  COLUMN (chnl_idnt, banner, country)
  , COLUMN (week_idnt, chnl_idnt, dept_idnt, fulfill_type_num, category, price_band)
  , COLUMN (week_idnt, chnl_idnt, banner, country, dept_idnt, fulfill_type_num, category, price_band)
  , COLUMN (chnl_idnt)
    ON {environment_schema}.category_channel_cost_plans_weekly;

-- DROP TABLE suppliergroup_channel_cost_plans_monthly;
CREATE MULTISET VOLATILE TABLE suppliergroup_channel_cost_plans_monthly AS (
SELECT
     m.month_idnt
    ,m.month_start_day_date
    ,m.month_end_day_date
    ,alternate_inventory_model
    ,CASE WHEN alternate_inventory_model = 'OWN' THEN 3
          WHEN alternate_inventory_model = 'DROPSHIP' THEN 1
        END AS fulfill_type_num
    ,CASE WHEN cluster_name = 'NORDSTROM_CANADA_STORES' THEN 111
          WHEN cluster_name = 'NORDSTROM_CANADA_ONLINE' THEN 121
          WHEN cluster_name = 'NORDSTROM_STORES' THEN 110
          WHEN cluster_name = 'NORDSTROM_ONLINE' THEN 120
          WHEN cluster_name = 'RACK_ONLINE' THEN 250
          WHEN cluster_name = 'RACK_CANADA_STORES' THEN 211
          WHEN cluster_name IN ('RACK STORES', 'PRICE','HYBRID','BRAND') THEN 210
          WHEN cluster_name = 'NORD CA RSWH' THEN 311
          WHEN cluster_name = 'NORD US RSWH' THEN 310
          WHEN cluster_name = 'RACK CA RSWH' THEN 261
          WHEN cluster_name = 'RACK US RSWH' THEN 260
      END AS chnl_idnt  
    ,selling_brand AS banner
    ,selling_country AS country
    ,p.dept_idnt
    ,p.category
    ,p.supplier_group
    ,net_sales_cost_currency_code AS currency_code
    ,SUM(p.demand_units) AS total_demand_units
    ,SUM(p.demand_dollar_amount) AS total_demand_retail_dollars
    ,SUM(p.gross_sales_units) AS total_gross_sales_units
    ,SUM(p.gross_sales_dollar_amount) AS total_gross_sales_retail_dollars
    ,SUM(p.returns_units) as total_return_units
    ,SUM(p.returns_dollar_amount) AS total_return_retail_dollars
    ,SUM(p.net_sales_units) AS total_net_sales_units
    ,SUM(p.net_sales_retail_amount) AS total_net_sales_retail_dollars
    ,SUM(p.net_sales_cost_amount) AS total_net_sales_cost_dollars
    ,AVG(p.sales_next_two_month_run_rate) AS next_2months_sales_run_rate
    ,SUM(p.average_inventory_cost_amount) AS plan_avg_inv_c_dollars
    ,SUM(p.average_inventory_retail_amount) AS plan_avg_inv_r_dollars
    ,SUM(p.average_inventory_units) AS plan_avg_inv_units
    ,SUM(p.beginning_of_period_inventory_cost_amount) AS plan_bop_c_dollars
    ,SUM(p.beginning_of_period_inventory_retail_amount) AS plan_bop_r_dollars
    ,SUM(p.beginning_of_period_inventory_units) AS plan_bop_c_units
    ,SUM(p.average_inventory_cost_amount*2 - p.beginning_of_period_inventory_cost_amount) AS plan_eop_c_dollars
    ,SUM(p.average_inventory_units*2 - p.beginning_of_period_inventory_units) AS plan_eop_c_units
    ,SUM(P.replenishment_receipts_cost_amount) AS plan_rp_rcpt_c_dollars
    ,SUM(P.replenishment_receipts_retail_amount) AS plan_rp_rcpt_r_dollars
    ,SUM(P.replenishment_receipts_units) AS plan_rp_rcpt_units
    ,SUM(P.replenishment_receipts_less_reserve_cost_amount) AS plan_rp_rcpt_lr_c_dollars
    ,SUM(P.replenishment_receipts_less_reserve_retail_amount) AS plan_rp_rcpt_lr_r_dollars
    ,SUM(P.replenishment_receipts_less_reserve_units) AS plan_rp_rcpt_lr_units
    ,SUM(P.nonreplenishment_receipts_cost_amount) AS plan_nrp_rcpt_c_dollars
    ,SUM(P.nonreplenishment_receipts_retail_amount) AS plan_nrp_rcpt_r_dollars
    ,SUM(P.nonreplenishment_receipts_units) AS plan_nrp_rcpt_units
    ,SUM(P.nonreplenishment_receipts_less_reserve_cost_amount) AS plan_nrp_rcpt_lr_c_dollars
    ,SUM(P.nonreplenishment_receipts_less_reserve_retail_amount) AS plan_nrp_rcpt_lr_r_dollars
    ,SUM(P.nonreplenishment_receipts_less_reserve_units) AS plan_nrp_rcpt_lr_units
    ,SUM(P.active_inventory_in_cost_amount) AS plan_pah_in_c_dollars
    ,SUM(P.active_inventory_in_retail_amount) AS plan_pah_in_r_dollars
    ,SUM(P.active_inventory_in_units) AS plan_pah_in_units
    ,SUM(P.plannable_inventory_cost_amount) AS plan_rcpt_need_c_dollars
    ,SUM(P.plannable_inventory_retail_amount) AS plan_rcpt_need_r_dollars
    ,SUM(P.plannable_inventory_units) AS plan_rcpt_need_c_units
    ,SUM(P.plannable_inventory_receipt_less_reserve_cost_amount) AS plan_rcpt_l_rs_c_dollars
    ,SUM(P.plannable_inventory_receipt_less_reserve_retail_amount) AS plan_rcpt_l_rs_r_dollars
    ,SUM(P.plannable_inventory_receipt_less_reserve_units) AS plan_rcpt_l_rs_c_units
FROM t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_fact_vw p
JOIN (
        SELECT DISTINCT
             month_idnt
            ,month_start_day_date
            ,month_end_day_date
        FROM prd_nap_usr_vws.day_cal_454_dim
     ) m
  ON p.fiscal_month_idnt = m.month_idnt
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
) WITH DATA
PRIMARY INDEX (month_idnt, chnl_idnt, dept_idnt, category, supplier_group, fulfill_type_num) 
ON COMMIT PRESERVE ROWS
;

DELETE FROM {environment_schema}.suppliergroup_channel_cost_plans_weekly ALL;
INSERT INTO {environment_schema}.suppliergroup_channel_cost_plans_weekly 
SELECT 
     supp.month_idnt                                                                
    ,supp.month_start_day_date
    ,supp.month_end_day_date
    ,COALESCE(mfp.week_idnt, mfpcw.week_idnt) AS week_idnt
    ,COALESCE(mfp.week_start_day_date, mfpcw.week_start_day_date) AS week_start_day_date
    ,COALESCE(mfp.week_end_day_date, mfpcw.week_end_day_date) AS week_end_day_date
    ,supp.chnl_idnt
    ,supp.banner
    ,supp.country
    ,supp.dept_idnt                                                                 
    ,supp.alternate_inventory_model
    ,supp.fulfill_type_num
    ,supp.category
    ,supp.supplier_group
    ,supp.currency_code
    -- sales
    ,supp.total_demand_units * COALESCE(mfp.demand_mfp_sp_u_month_wk_pct,1.00/mfp.weeks_in_month) AS demand_units
    ,supp.total_demand_retail_dollars * COALESCE(mfp.demand_mfp_sp_r_month_wk_pct,1.00/mfp.weeks_in_month) AS demand_r_dollars
    ,supp.total_gross_sales_units * COALESCE(mfp.gross_sls_u_mfp_sp_month_wk_pct,1.00/mfp.weeks_in_month) AS gross_sls_units
    ,supp.total_gross_sales_retail_dollars * COALESCE(mfp.gross_sls_r_mfp_sp_month_wk_pct,1.00/mfp.weeks_in_month) AS gross_sls_r_dollars
    ,supp.total_return_units * COALESCE(mfp.return_mfp_sp_u_month_wk_pct,1.00/mfp.weeks_in_month) AS return_units
    ,supp.total_return_retail_dollars * COALESCE(mfp.return_mfp_sp_r_month_wk_pct,1.00/mfp.weeks_in_month) AS return_r_dollars
    ,supp.total_net_sales_units * COALESCE(mfp.net_sls_u_mfp_sp_month_wk_pct,1.00/mfp.weeks_in_month) AS net_sls_units
    ,supp.total_net_sales_retail_dollars * COALESCE(mfp.net_sls_r_mfp_sp_month_wk_pct,1.00/mfp.weeks_in_month) AS net_sls_r_dollars
    ,supp.total_net_sales_cost_dollars * COALESCE(mfp.net_sls_c_mfp_sp_month_wk_pct,1.00/mfp.weeks_in_month) AS net_sls_c_dollars
    ,supp.next_2months_sales_run_rate
    -- inventory
    ,plan_avg_inv_c_dollars * COALESCE(mfpcw.inv_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS avg_inv_ttl_c
    ,plan_avg_inv_units * COALESCE(mfpcw.inv_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS avg_inv_ttl_u
    ,CASE WHEN mfpcw.week_num_of_fiscal_month = 1 THEN supp.plan_bop_c_dollars END AS plan_bop_c_dollars
    ,CASE WHEN mfpcw.week_num_of_fiscal_month = 1 THEN supp.plan_bop_c_units END AS plan_bop_c_units
    ,supp.plan_eop_c_dollars * COALESCE(mfpcw.eop_mfp_sp_c_month_wk_pct,1.00) AS plan_eop_c_dollars
    ,supp.plan_eop_c_units * COALESCE(mfpcw.eop_mfp_sp_u_month_wk_pct,1.00) AS plan_eop_c_units
    ,supp.plan_rp_rcpt_r_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_rp_rcpt_r
    ,supp.plan_rp_rcpt_c_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_rp_rcpt_c
    ,supp.plan_rp_rcpt_units * COALESCE(mfpcw.rept_mfp_sp_u_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_rp_rcpt_u    
    ,supp.plan_rp_rcpt_lr_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_rp_rcpt_lr_r
    ,supp.plan_rp_rcpt_lr_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_rp_rcpt_lr_c
    ,supp.plan_rp_rcpt_lr_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_rp_rcpt_lr_u
    ,supp.plan_nrp_rcpt_r_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_nrp_rcpt_r
    ,supp.plan_nrp_rcpt_c_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_nrp_rcpt_c
    ,supp.plan_nrp_rcpt_units * COALESCE(mfpcw.rept_mfp_sp_u_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_nrp_rcpt_u    
    ,supp.plan_nrp_rcpt_lr_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_nrp_rcpt_lr_r
    ,supp.plan_nrp_rcpt_lr_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_nrp_rcpt_lr_c
    ,supp.plan_nrp_rcpt_lr_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_nrp_rcpt_lr_u
    ,supp.plan_rcpt_need_r_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS rcpt_need_r
    ,supp.plan_rcpt_need_c_dollars * COALESCE(mfpcw.rept_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS rcpt_need_c
    ,supp.plan_rcpt_need_c_units * COALESCE(mfpcw.rept_mfp_sp_u_month_wk_pct,1.00/mfpcw.weeks_in_month) AS rcpt_need_u
    ,supp.plan_rcpt_l_rs_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS rcpt_need_lr_r
    ,supp.plan_rcpt_l_rs_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS rcpt_need_lr_c
    ,supp.plan_rcpt_l_rs_c_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct,1.00/mfpcw.weeks_in_month) AS rcpt_need_lr_u
    ,supp.plan_pah_in_r_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_pah_in_r
    ,supp.plan_pah_in_c_dollars * COALESCE(mfpcw.rept_lr_mfp_sp_c_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_pah_in_c
    ,supp.plan_pah_in_units * COALESCE(mfpcw.rept_lr_mfp_sp_u_month_wk_pct,1.00/mfpcw.weeks_in_month) AS plan_pah_in_u
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM suppliergroup_channel_cost_plans_monthly supp
LEFT JOIN mfpc_channel_weekly_ratios mfp
  ON supp.month_idnt = mfp.month_idnt
 AND supp.chnl_idnt = mfp.chnl_idnt
 AND supp.dept_idnt = mfp.dept_idnt
 AND supp.fulfill_type_num = mfp.fulfill_type_num
LEFT JOIN mfp_banner_weekly_ratios mfpcw
  ON supp.month_idnt = mfpcw.month_idnt
 AND supp.dept_idnt = mfpcw.dept_idnt
 AND supp.fulfill_type_num = mfpcw.fulfill_type_num
 AND supp.country = mfpcw.channel_country 
 AND supp.banner = mfpcw.banner
 AND mfp.week_idnt = mfpcw.week_idnt
;