/*
APT Cost Plan History DDL
Author: Sara Riker
Date Created: 9/29/22
Date Last Updated: 9/29/22

Datalab: t2dl_das_apt_cost_reporting
Updates Tables:
    - merch_assortment_supplier_cluster_plan_history_weekly
*/

-- Supplier Cluster Plans

DROP TABLE supp_cluster_plans;
CREATE MULTISET VOLATILE TABLE supp_cluster_plans AS (
SELECT
      cal.month_idnt AS snapshot_plan_month_idnt
     ,cal.week_idnt AS snapshot_plan_week_idnt
     ,cal.week_start_day_date AS snapshot_start_day_date
     ,CASE WHEN week_num_of_fiscal_month = 3 THEN 1 
           WHEN month_idnt = 202212 AND week_num_of_fiscal_month = 4 THEN 1
           ELSE 0 END AS replan_week
     ,CAST(CONCAT('20',LEFT(strtok(month_id,'FY',2),2),
           CASE WHEN LEFT(month_id,3)= 'FEB' THEN '01'
                WHEN LEFT(month_id,3)= 'MAR' THEN '02'
                WHEN LEFT(month_id,3)= 'APR' THEN '03'
                WHEN LEFT(month_id,3)= 'MAY' THEN '04'
                WHEN LEFT(month_id,3)= 'JUN' THEN '05'
                WHEN LEFT(month_id,3)= 'JUL' THEN '06'
                WHEN LEFT(month_id,3)= 'AUG' THEN '07'
                WHEN LEFT(month_id,3)= 'SEP' THEN '08'
                WHEN LEFT(month_id,3)= 'OCT' THEN '09'
                WHEN LEFT(month_id,3)= 'NOV' THEN '10'
                WHEN LEFT(month_id,3)= 'DEC' THEN '11'
                WHEN LEFT(month_id,3)= 'JAN' THEN '12'
              END) AS INTEGER) AS fiscal_month_idnt 
    ,event_time
    ,selling_country
    ,selling_brand
    ,cluster_name
    ,category
    ,supplier_group
    ,department_number AS dept_idnt
    ,month_id
    ,alternate_inventory_model
    ,demand_dollar_currency_code
    ,demand_dollar_amount
    ,demand_units
    ,gross_sales_dollar_currency_code
    ,gross_sales_dollar_amount
    ,gross_sales_units
    ,returns_dollar_currency_code
    ,returns_dollar_amount
    ,returns_units
    ,net_sales_units
    ,net_sales_retail_currency_code
    ,net_sales_retail_amount
    ,net_sales_cost_currency_code
    ,net_sales_cost_amount
    ,product_margin_retail_currency_code
    ,product_margin_retail_amount
    ,demand_next_two_month_run_rate
    ,sales_next_two_month_run_rate
    ,replenishment_receipts_units
    ,replenishment_receipts_retail_currency_code
    ,replenishment_receipts_retail_amount
    ,replenishment_receipts_cost_currency_code
    ,replenishment_receipts_cost_amount
    ,replenishment_receipts_less_reserve_units
    ,replenishment_receipts_less_reserve_retail_currency_code
    ,replenishment_receipts_less_reserve_retail_amount
    ,replenishment_receipts_less_reserve_cost_currency_code
    ,replenishment_receipts_less_reserve_cost_amount
    ,nonreplenishment_receipts_units
    ,nonreplenishment_receipts_retail_currency_code
    ,nonreplenishment_receipts_retail_amount
    ,nonreplenishment_receipts_cost_currency_code
    ,nonreplenishment_receipts_cost_amount
    ,nonreplenishment_receipts_less_reserve_units
    ,nonreplenishment_receipts_less_reserve_retail_currency_code
    ,nonreplenishment_receipts_less_reserve_retail_amount
    ,nonreplenishment_receipts_less_reserve_cost_currency_code
    ,nonreplenishment_receipts_less_reserve_cost_amount
    ,dropship_receipt_units
    ,dropship_receipt_retail_currency_code
    ,dropship_receipt_retail_amount
    ,dropship_receipt_cost_currency_code
    ,dropship_receipt_cost_amount
    ,average_inventory_units
    ,average_inventory_retail_currency_code
    ,average_inventory_retail_amount
    ,average_inventory_cost_currency_code
    ,average_inventory_cost_amount
    ,beginning_of_period_inventory_units
    ,beginning_of_period_inventory_retail_currency_code
    ,beginning_of_period_inventory_retail_amount
    ,beginning_of_period_inventory_cost_currency_code
    ,beginning_of_period_inventory_cost_amount
    ,beginning_of_period_inventory_target_units
    ,beginning_of_period_inventory_target_retail_currency_code
    ,beginning_of_period_inventory_target_retail_amount
    ,beginning_of_period_inventory_target_cost_currency_code
    ,beginning_of_period_inventory_target_cost_amount
    ,return_to_vendor_units
    ,return_to_vendor_retail_currency_code
    ,return_to_vendor_retail_amount
    ,return_to_vendor_cost_currency_code
    ,return_to_vendor_cost_amount
    ,rack_transfer_units
    ,rack_transfer_retail_currency_code
    ,rack_transfer_retail_amount
    ,rack_transfer_cost_currency_code
    ,rack_transfer_cost_amount
    ,active_inventory_in_units
    ,active_inventory_in_retail_currency_code
    ,active_inventory_in_retail_amount
    ,active_inventory_in_cost_currency_code
    ,active_inventory_in_cost_amount
    ,plannable_inventory_units
    ,plannable_inventory_retail_currency_code
    ,plannable_inventory_retail_amount
    ,plannable_inventory_cost_currency_code
    ,plannable_inventory_cost_amount
    ,plannable_inventory_receipt_less_reserve_units
    ,plannable_inventory_receipt_less_reserve_retail_currency_code
    ,plannable_inventory_receipt_less_reserve_retail_amount
    ,plannable_inventory_receipt_less_reserve_cost_currency_code
    ,plannable_inventory_receipt_less_reserve_cost_amount
    ,shrink_units
    ,shrink_retail_currency_code
    ,shrink_retail_amount
    ,shrink_cost_currency_code
    ,shrink_cost_amount
    ,dw_sys_load_tmstp AS quantrix_update
    ,current_timestamp AS rcd_update_timestamp
FROM t2dl_das_apt_reporting.merch_assortment_supplier_cluster_plan_fact p
JOIN prd_nap_usr_vws.day_cal_454_dim cal
  ON current_date = cal.day_date
WHERE CAST(CONCAT('20',LEFT(strtok(month_id,'FY',2),2),
           CASE WHEN LEFT(month_id,3)= 'FEB' THEN '01'
                WHEN LEFT(month_id,3)= 'MAR' THEN '02'
                WHEN LEFT(month_id,3)= 'APR' THEN '03'
                WHEN LEFT(month_id,3)= 'MAY' THEN '04'
                WHEN LEFT(month_id,3)= 'JUN' THEN '05'
                WHEN LEFT(month_id,3)= 'JUL' THEN '06'
                WHEN LEFT(month_id,3)= 'AUG' THEN '07'
                WHEN LEFT(month_id,3)= 'SEP' THEN '08'
                WHEN LEFT(month_id,3)= 'OCT' THEN '09'
                WHEN LEFT(month_id,3)= 'NOV' THEN '10'
                WHEN LEFT(month_id,3)= 'DEC' THEN '11'
                WHEN LEFT(month_id,3)= 'JAN' THEN '12'
              END) AS INTEGER)  >= (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
)
WITH DATA
PRIMARY INDEX (selling_brand, dept_idnt, category, supplier_group)
ON COMMIT PRESERVE ROWS
;

DELETE FROM {environment_schema}.merch_assortment_supplier_cluster_plan_history_weekly WHERE snapshot_plan_week_idnt = (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date);

INSERT INTO {environment_schema}.merch_assortment_supplier_cluster_plan_history_weekly
SELECT * 
FROM supp_cluster_plans
WHERE snapshot_plan_week_idnt = (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
;
