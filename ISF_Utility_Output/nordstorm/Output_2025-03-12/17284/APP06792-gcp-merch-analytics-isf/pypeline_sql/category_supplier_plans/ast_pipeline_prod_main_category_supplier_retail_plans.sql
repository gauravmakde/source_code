
/*
Category Supplier Retail Plan DDL
Author: Sara Riker
8/11/22: Create DDL

Updates Tables:
    {environment_schema}.category_level_retail_plans{env_suffix}
    {environment_schema}.supplier_group_retail_plans{env_suffix}
*/

CREATE MULTISET VOLATILE TABLE months AS (
    SELECT DISTINCT
         m.month_id
        ,c.month_idnt
        ,c.month_start_day_date
    FROM (
        SELECT DISTINCT
            CASE WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'FEB' THEN 1
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'MAR' THEN 2
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'APR' THEN 3
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'MAY' THEN 4
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'JUN' THEN 5
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'JUL' THEN 6
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'AUG' THEN 7
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'SEP' THEN 8
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'OCT' THEN 9
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'NOV' THEN 10
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'DEC' THEN 11
                WHEN UPPER(SUBSTRING(a.month_id, 1, 3)) = 'JAN' THEN 12
                END AS mth_454
            ,CONCAT(20, SUBSTRING(a.month_id, 8,8)) AS yr_454
            ,month_id
        FROM prd_nap_usr_vws.merch_assortment_category_level_plan_fact a
        ) m
    JOIN (
        SELECT DISTINCT 
            month_idnt
            ,month_start_day_date
            ,fiscal_month_num
            ,fiscal_year_num 
        FROM prd_nap_usr_vws.day_cal_454_dim
        ) c
    ON c.fiscal_month_num = m.mth_454
    AND c.fiscal_year_num = m.yr_454
) WITH DATA 
PRIMARY INDEX(month_idnt) ON COMMIT PRESERVE ROWS;
;

CREATE MULTISET VOLATILE TABLE cluster_map AS (
    SELECT DISTINCT 
       cluster_name
      ,CASE WHEN cluster_name = 'NORDSTROM_CANADA_STORES' THEN 111
            WHEN cluster_name = 'NORDSTROM_CANADA_ONLINE' THEN 121
            WHEN cluster_name = 'NORDSTROM_STORES' THEN 110
            WHEN cluster_name = 'NORDSTROM_ONLINE' THEN 120
            WHEN cluster_name = 'RACK_ONLINE' THEN 250
            WHEN cluster_name = 'RACK_CANADA_STORES' THEN 211
            WHEN cluster_name IN ('BRAND', 'HYBRID', 'PRICE') THEN 210 -- Rack stores
            END AS chnl_idnt
  FROM prd_nap_usr_vws.merch_assortment_category_level_plan_fact
) WITH DATA 
PRIMARY INDEX(cluster_name) ON COMMIT PRESERVE ROWS
;

-- DROP TABLE sup_plans;
CREATE MULTISET VOLATILE TABLE sup_plans AS (
SELECT 
     m.month_idnt
    ,m.month_start_day_date
    ,(SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date) AS plan_snapshot_month_idnt
    ,c.chnl_idnt
    ,p.department_number AS dept_idnt
    ,p.supplier_group
    ,p.category
    ,total_sales_retail_currency_code AS currency_code
    ,SUM(p.total_sales_units) AS total_sales_units
    ,SUM(p.total_sales_retail_amount) AS total_sales_retail_dollars
    ,SUM(p.total_demand_units) AS total_demand_units
    ,SUM(p.total_demand_retail_amount) AS total_demand_retail_dollars
    ,SUM(p.beginning_of_period_inventory_units) AS bop_inventory_units
    ,SUM(p.beginning_of_period_inventory_retail_amount) AS bop_inventory_retail_dollars
    ,SUM(p.receipt_need_units) AS receipt_need_units
    ,SUM(p.receipt_need_retail_amount) AS receipt_need_retail_dollars
    ,AVG(p.next_2months_sales_run_rate) AS next_2months_sales_run_rate
    ,SUM(p.receipt_need_less_reserve_units) AS receipt_need_less_reserve_units
    ,SUM(p.receipt_need_less_reserve_retail_amount) AS receipt_need_less_reserve_retail_dollars
    ,SUM(p.rack_transfer_units) AS rack_transfer_units
    ,SUM(p.rack_transfer_retail_amount) AS rack_transfer_retail_dollars
    ,MAX(dw_sys_updt_tmstp) AS qntrx_update_timestamp
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM prd_nap_usr_vws.merch_assortment_supplier_group_plan_fact p
JOIN cluster_map c
  ON p.cluster_name = c.cluster_name
JOIN months m
  ON p.month_id = m.month_id
GROUP BY 1,2,3,4,5,6,7,8
) WITH DATA 
PRIMARY INDEX(month_idnt, chnl_idnt, dept_idnt, supplier_group, category) ON COMMIT PRESERVE ROWS
;


MERGE INTO {environment_schema}.supplier_group_retail_plans{env_suffix} AS a
USING sup_plans AS b
   ON a.month_idnt = b.month_idnt
  AND a.month_start_day_date = b.month_start_day_date
  AND a.plan_snapshot_month_idnt = b.plan_snapshot_month_idnt
  AND a.chnl_idnt = b.chnl_idnt
  AND a.dept_idnt = b.dept_idnt
  AND a.supplier_group = b.supplier_group
  AND a.category = b.category
WHEN MATCHED
THEN UPDATE
	SET
         currency_code = b.currency_code
        ,total_sales_units = b.total_sales_units
        ,total_sales_retail_dollars = b.total_sales_retail_dollars
        ,total_demand_units = b.total_demand_units
        ,total_demand_retail_dollars = b.total_demand_retail_dollars
        ,bop_inventory_units = b.bop_inventory_units
        ,bop_inventory_retail_dollars = b.bop_inventory_retail_dollars
        ,receipt_need_units = b.receipt_need_units
        ,receipt_need_retail_dollars = b.receipt_need_retail_dollars
        ,next_2months_sales_run_rate = b.next_2months_sales_run_rate
        ,receipt_need_less_reserve_units = b.receipt_need_less_reserve_units
        ,receipt_need_less_reserve_retail_dollars = b.receipt_need_less_reserve_retail_dollars
        ,rack_transfer_units = b.rack_transfer_units
        ,rack_transfer_retail_dollars = b.rack_transfer_retail_dollars
        ,qntrx_update_timestamp = b.qntrx_update_timestamp
        ,update_timestamp = b.update_timestamp
WHEN NOT MATCHED
THEN INSERT
	VALUES (
         b.month_idnt
        ,b.month_start_day_date
        ,b.plan_snapshot_month_idnt
        ,b.chnl_idnt
        ,b.dept_idnt
        ,b.supplier_group
        ,b.category
        ,b.currency_code
        ,b.total_sales_units
        ,b.total_sales_retail_dollars
        ,b.total_demand_units
        ,b.total_demand_retail_dollars
        ,b.bop_inventory_units
        ,b.bop_inventory_retail_dollars
        ,b.receipt_need_units
        ,b.receipt_need_retail_dollars
        ,b.next_2months_sales_run_rate
        ,b.receipt_need_less_reserve_units
        ,b.receipt_need_less_reserve_retail_dollars
        ,b.rack_transfer_units
        ,b.rack_transfer_retail_dollars
        ,b.qntrx_update_timestamp
        ,b.update_timestamp
    )
;
-- DROP TABLE dates;
CREATE MULTISET VOLATILE TABLE dates AS (
SELECT DISTINCT
     week_idnt
    ,week_start_day_date
    ,month_idnt
    ,week_num_of_fiscal_month
    ,MAX(week_num_of_fiscal_month) OVER (PARTITION BY month_idnt) AS weeks_in_month
FROM prd_nap_usr_vws.day_cal_454_dim 
WHERE month_idnt IN (SELECT DISTINCT month_idnt FROM {environment_schema}.category_level_retail_plans{env_suffix})
) WITH DATA 
PRIMARY INDEX(month_idnt) ON COMMIT PRESERVE ROWS;

-- DROP TABLE snap_plans;
CREATE MULTISET VOLATILE TABLE snap_plans AS (
with plans AS (
    SELECT 
         sub.chnl_idnt
        ,sub.week_idnt
        ,sub.week_start_day_date
        ,sub.month_idnt
        ,sub.dept_idnt    
        ,SUM(sub.sales_plan_sp) AS sales_plan_sp  
        ,SUM(sub.demand_plan_sp) AS demand_plan_sp
        ,SUM(sub.receipts_plan_sp) AS receipts_plan_sp
        ,SUM(sub.eoh_plan_sp) AS eoh_plan_sp    
        ,SUM(sub.eoh_plan_sp_eom) AS eoh_plan_sp_eom    
    FROM
        (
        SELECT
             pl.chnl_idnt
            ,dt.week_idnt
            ,dt.week_start_day_date
            ,dt.month_idnt
            ,pl.dept_idnt
            ,SUM(pl.sale_ttl_retl$_sp) AS sales_plan_sp
            ,SUM(pl.dmnd_ttl_retl$_op) AS demand_plan_sp
            ,SUM(pl.rcpt_tot_adj_retl$_sp) AS receipts_plan_sp
            ,SUM(pl.inv_eoh_ttl_retl$_sp) AS eoh_plan_sp	
            ,SUM(CASE WHEN dt.week_num_of_fiscal_month = dt.weeks_in_month
            	THEN pl.inv_eoh_ttl_retl$_sp END) AS eoh_plan_sp_eom	
        FROM prd_ma_bado.mfp_fp_mlp_dept_cw_cvw pl
        INNER JOIN (SELECT DISTINCT chnl_idnt FROM {environment_schema}.category_level_retail_plans{env_suffix}) loc
            ON pl.chnl_idnt = loc.chnl_idnt
        INNER JOIN dates dt
            ON pl.wk_idnt = dt.week_idnt 
        GROUP BY 1,2,3,4,5  
    UNION ALL	        
        SELECT
             pl.chnl_idnt
            ,dt.week_idnt
            ,dt.week_start_day_date
            ,dt.month_idnt
            ,pl.dept_idnt
            ,SUM(pl.sale_ttl_retl$_sp) AS sales_plan_sp
            ,SUM(pl.dmnd_ttl_retl$_sp) AS demand_plan_sp
            ,SUM(pl.rcpt_po_wtsfr_retl$_sp) AS receipts_plan_sp
            ,SUM(pl.inv_eoh_ttl_retl$_sp) AS eoh_plan_sp	
            ,SUM(CASE WHEN dt.week_num_of_fiscal_month = dt.weeks_in_month
            	THEN pl.inv_eoh_ttl_retl$_sp END) AS eoh_plan_sp_eom	
        FROM prd_ma_bado.mfp_op_mlp_dept_cw_cvw pl
        INNER JOIN (SELECT DISTINCT chnl_idnt FROM {environment_schema}.category_level_retail_plans{env_suffix}) loc
            ON pl.chnl_idnt = loc.chnl_idnt
        INNER JOIN dates dt
            ON pl.wk_idnt = dt.week_idnt 
        GROUP BY 1,2,3,4,5
        ) sub
    GROUP BY 1,2,3,4,5
)
SELECT
     month_idnt
    ,(SELECT DISTINCT b.month_idnt FROM prd_ma_bado.mfp_op_mlp_dept_cw_cvw pl
                          INNER JOIN dates dt
                             ON pl.wk_idnt = dt.week_idnt 
                          LEFT JOIN prd_nap_usr_vws.day_cal_454_dim b
                            ON pl.rcd_load_dt = b.day_date) AS plan_snapshot_month_idnt
    ,week_idnt
    ,week_start_day_date
    ,chnl_idnt
    ,dept_idnt
	--SALES
    ,sales_plan_sp
    ,SUM(sales_plan_sp) OVER (PARTITION BY month_idnt, chnl_idnt, dept_idnt) AS sales_plan_sp_month
    ,COALESCE(sales_plan_sp / NULLIF(sales_plan_sp_month, 0), 0) AS sales_plan_sp_month_wk_pct
	--DEMAND
    ,demand_plan_sp
    ,SUM(demand_plan_sp) OVER (PARTITION BY month_idnt, chnl_idnt, dept_idnt) AS demand_plan_sp_month
    ,COALESCE(demand_plan_sp / NULLIF(demand_plan_sp_month, 0), 0) AS demand_plan_sp_month_wk_pct
	--RECEIPTS    
    ,receipts_plan_sp
    ,SUM(receipts_plan_sp) OVER (PARTITION BY month_idnt, chnl_idnt, dept_idnt) AS receipts_plan_sp_month
    ,COALESCE(receipts_plan_sp / NULLIF(receipts_plan_sp_month, 0), 0) AS receipts_plan_sp_month_wk_pct
	--EOH
    ,eoh_plan_sp
    ,MAX(eoh_plan_sp_eom) OVER (PARTITION BY month_idnt, chnl_idnt, dept_idnt) AS eoh_plan_sp_month
    ,COALESCE(eoh_plan_sp / NULLIF(eoh_plan_sp_month, 0), 0) AS eoh_plan_sp_month_wk_pct  
FROM plans p
) WITH DATA PRIMARY INDEX(chnl_idnt, month_idnt, week_idnt, dept_idnt) ON COMMIT PRESERVE ROWS;

COLLECT STATS 
	PRIMARY INDEX (chnl_idnt, month_idnt, week_idnt, dept_idnt)
		ON snap_plans;
	

;
MERGE INTO {environment_schema}.mfp_weekly_percents{env_suffix} AS a
USING snap_plans AS b
   ON a.month_idnt = b.month_idnt
  AND a.plan_snapshot_month_idnt = b.plan_snapshot_month_idnt
  AND a.week_idnt = b.week_idnt
  AND a.week_start_day_date = b.week_start_day_date  
  AND a.chnl_idnt = b.chnl_idnt
  AND a.dept_idnt = b.dept_idnt
WHEN MATCHED
THEN UPDATE
	SET
         sales_plan_sp = b.sales_plan_sp
        ,sales_plan_sp_month = b.sales_plan_sp_month
        ,sales_plan_sp_month_wk_pct = b.sales_plan_sp_month_wk_pct
        ,demand_plan_sp = b.demand_plan_sp
        ,demand_plan_sp_month = b.demand_plan_sp_month
        ,demand_plan_sp_month_wk_pct = b.demand_plan_sp_month_wk_pct
        ,receipts_plan_sp = b.receipts_plan_sp
        ,receipts_plan_sp_month = b.receipts_plan_sp_month
        ,receipts_plan_sp_month_wk_pct = b.receipts_plan_sp_month_wk_pct
        ,eoh_plan_sp = b.eoh_plan_sp
        ,eoh_plan_sp_month = b.eoh_plan_sp_month
        ,eoh_plan_sp_month_wk_pct = b.eoh_plan_sp_month_wk_pct
        ,update_timestamp = current_timestamp 
WHEN NOT MATCHED
THEN INSERT
	VALUES (
           b.month_idnt  
          ,b.plan_snapshot_month_idnt  
          ,b.week_idnt 
          ,b.week_start_day_date 
          ,b.chnl_idnt 
          ,b.dept_idnt 
          ,b.sales_plan_sp
          ,b.sales_plan_sp_month 
          ,b.sales_plan_sp_month_wk_pct 
          ,b.demand_plan_sp
          ,b.demand_plan_sp_month 
          ,b.demand_plan_sp_month_wk_pct 
          ,b.receipts_plan_sp 
          ,b.receipts_plan_sp_month
          ,b.receipts_plan_sp_month_wk_pct 
          ,b.eoh_plan_sp 
          ,b.eoh_plan_sp_month 
          ,b.eoh_plan_sp_month_wk_pct 
          ,current_timestamp
    )
;

CREATE MULTISET VOLATILE TABLE weekly_sup_plans AS (
    WITH snapshot_rank AS (
    SELECT DISTINCT
         plan_snapshot_month_idnt 
        ,DENSE_RANK() OVER (ORDER BY plan_snapshot_month_idnt DESC) AS snap_rank
    FROM {environment_schema}.mfp_weekly_percents{env_suffix}
    ),
    snap_plan_month AS (
    SELECT
        COALESCE((SELECT DISTINCT plan_snapshot_month_idnt FROM snapshot_rank WHERE snap_rank = 2), (SELECT DISTINCT plan_snapshot_month_idnt FROM snapshot_rank WHERE snap_rank = 1)) AS plan_snapshot_month_idnt
    FROM snapshot_rank
    )
    SELECT
         c.month_idnt
        ,c.plan_snapshot_month_idnt
        ,w.week_idnt 
        ,w.week_start_day_date
        ,c.chnl_idnt
        ,c.dept_idnt
        ,c.supplier_group
        ,c.category
        ,c.currency_code
        ,w.sales_plan_sp_month_wk_pct
        ,w.demand_plan_sp_month_wk_pct
        ,w.receipts_plan_sp_month_wk_pct
        ,w.eoh_plan_sp_month_wk_pct
        -- sales plans
        ,c.total_sales_units * w.sales_plan_sp_month_wk_pct AS sales_unit
        ,c.total_sales_retail_dollars * w.sales_plan_sp_month_wk_pct AS sales_retail_dollars
        -- demand
        ,c.total_demand_units * w.demand_plan_sp_month_wk_pct AS demand_units
        ,c.total_demand_retail_dollars * w.demand_plan_sp_month_wk_pct AS demand_retail_dollars
        -- receipts
        ,c.receipt_need_less_reserve_units * w.receipts_plan_sp_month_wk_pct AS receipt_units
        ,c.receipt_need_less_reserve_retail_dollars * w.receipts_plan_sp_month_wk_pct AS receipt_retail_dollars
        -- eoh
        ,c.bop_inventory_units * w.eoh_plan_sp_month_wk_pct AS eoh_units
        ,c.bop_inventory_retail_dollars * w.eoh_plan_sp_month_wk_pct AS eoh_retail_dollars
        ,c.qntrx_update_timestamp
        ,CURRENT_TIMESTAMP AS update_timestamp
    FROM {environment_schema}.supplier_group_retail_plans{env_suffix} c
    JOIN {environment_schema}.mfp_weekly_percents{env_suffix} w
      ON c.month_idnt = w.month_idnt
     AND c.chnl_idnt = w.chnl_idnt
     AND c.dept_idnt = w.dept_idnt
    WHERE c.plan_snapshot_month_idnt = (SELECT MAX(plan_snapshot_month_idnt) FROM {environment_schema}.supplier_group_retail_plans{env_suffix})
      AND w.plan_snapshot_month_idnt = (SELECT DISTINCT plan_snapshot_month_idnt FROM snap_plan_month) -- want last month's snapshot
) WITH DATA 
PRIMARY INDEX(chnl_idnt, month_idnt, week_idnt, dept_idnt) 
ON COMMIT PRESERVE ROWS
;

MERGE INTO {environment_schema}.supplier_group_retail_plans_weekly{env_suffix} AS a
USING weekly_sup_plans AS b
   ON a.month_idnt = b.month_idnt
  AND a.plan_snapshot_month_idnt = b.plan_snapshot_month_idnt
  AND a.week_idnt = b.week_idnt
  AND a.week_start_day_date = b.week_start_day_date  
  AND a.chnl_idnt = b.chnl_idnt
  AND a.dept_idnt = b.dept_idnt
  AND a.supplier_group = b.supplier_group
  AND a.category = b.category
WHEN MATCHED
THEN UPDATE
	SET
         sales_plan_sp_month_wk_pct = b.sales_plan_sp_month_wk_pct
        ,demand_plan_sp_month_wk_pct = b.demand_plan_sp_month_wk_pct
        ,receipts_plan_sp_month_wk_pct = b.receipts_plan_sp_month_wk_pct
        ,eoh_plan_sp_month_wk_pct = b.eoh_plan_sp_month_wk_pct
        ,sales_unit = b.sales_unit
        ,sales_retail_dollars = b.sales_retail_dollars
        ,demand_units = b.demand_units
        ,demand_retail_dollars = b.demand_retail_dollars
        ,receipt_units = b.receipt_units
        ,receipt_retail_dollars = b.receipt_retail_dollars
        ,eoh_units = b.eoh_units
        ,eoh_retail_dollars = b.eoh_retail_dollars
        ,qntrx_update_timestamp = b.qntrx_update_timestamp
        ,update_timestamp = current_timestamp 
WHEN NOT MATCHED
THEN INSERT
	VALUES (
         b.month_idnt  
        ,b.plan_snapshot_month_idnt  
        ,b.week_idnt 
        ,b.week_start_day_date 
        ,b.chnl_idnt 
        ,b.dept_idnt 
        ,b.supplier_group
        ,b.category
        ,b.currency_code
        ,b.sales_plan_sp_month_wk_pct
        ,b.demand_plan_sp_month_wk_pct
        ,b.receipts_plan_sp_month_wk_pct 
        ,b.eoh_plan_sp_month_wk_pct 
        ,b.sales_unit
        ,b.sales_retail_dollars
        ,b.demand_units
        ,b.demand_retail_dollars 
        ,b.receipt_units 
        ,b.receipt_retail_dollars
        ,b.eoh_units 
        ,b.eoh_retail_dollars 
        ,b.qntrx_update_timestamp
        ,b.update_timestamp 
    )
;