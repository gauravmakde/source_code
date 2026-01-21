/*
Contribution Profit - Labor
Authors: Sara Riker and KP Tryon
Datalab: t2dl_das_contribution_margin

2022-01-31: Create main scripts for commssion rates, transaction base, selling labor, marketing email costs

Updates Tables:
    complete:
        - t2dl_das_contribution_margin.commission_rates

    in progress:
        - t2dl_das_contribution_margin.transaction_base
        - t2dl_das_contribution_margin.cost_selling_store_labor
        - t2dl_das_contribution_margin.cost_marketing
        - t2dl_das_contribution_margin.cost_loyalty
        - t2dl_das_contribution_margin.cost_distribution_fulfillment
        - t2dl_das_contribution_margin.cost_shipping_revenue
        - t2dl_das_contribution_margin.cost_contra_cogs
        - t2dl_das_contribution_margin.cost_base
*/

-- 2022-01-31: Commission Rates only needs to be updated once
/*
DELETE FROM t2dl_das_contribution_margin.commission_rates ALL;
INSERT INTO t2dl_das_contribution_margin.commission_rates 
SELECT
     dept_idnt
    ,commission_rate
    ,current_timestamp AS update_timestamp
FROM t3dl_ace_mch.commission_rates
;
*/


CREATE MULTISET VOLATILE TABLE dates AS (
    SELECT
         current_date - 7 AS start_date -- daily update
        --  '2021-02-01' AS start_date -- backfill
        ,current_date AS end_date 
)
WITH DATA
ON COMMIT PRESERVE ROWS
;

-- 2022-01-31: Create base transaction table

DELETE FROM t2dl_das_contribution_margin.transaction_base WHERE business_day_date BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates);
INSERT INTO t2dl_das_contribution_margin.transaction_base
SELECT 
     business_day_date
    ,tran_date
    ,tran_time
    ,order_num
    ,global_tran_id
    ,line_item_seq_num
    ,line_item_activity_type_code AS tran_type_code
    ,line_item_activity_type_desc AS tran_type
    ,line_item_order_type
    ,line_item_fulfillment_type
    ,ringing_store_num
    ,fulfilling_store_num
    ,intent_store_num
    ,business_unit_desc
    ,commission_slsprsn_num AS worker_number
    ,CASE WHEN business_unit_desc IN ('FULL LINE', 'FULL LINE CANADA')      -- Full Line Stores Only
           AND commission_slsprsn_num <> 2079333                            -- N.com dummy employee number
           AND line_item_merch_nonmerch_ind = 'MERCH'                       -- Merchandise only
          THEN 1 
          ELSE 0 
          END AS commissionable_flag
    ,CASE WHEN line_item_fulfillment_type = 'StoreTake' 
            OR line_item_fulfillment_type IS NULL 
          THEN 0 
          ELSE 1 
          END AS shipping_flag
    ,CASE WHEN line_item_merch_nonmerch_ind = 'MERCH' THEN 1 
          ELSE 0 
          END AS merch_ind
    ,nonmerch_fee_code
    ,merch_dept_num
    ,sku_num
    ,line_net_usd_amt
    ,line_net_amt 
    ,line_item_net_amt_currency_code
    ,line_item_regular_price
    ,line_item_quantity
    ,price_adj_code
    ,commission_rate
    ,current_timestamp AS update_timestamp
FROM prd_nap_usr_vws.retail_tran_detail_fact_vw trn
LEFT JOIN t2dl_das_contribution_margin.commission_rates cr
  ON trn.merch_dept_num = cr.dept_idnt
JOIN prd_nap_usr_vws.store st
  ON trn.intent_store_num = st.store_num 
WHERE business_day_date BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates)
;

COLLECT STATS 
     PRIMARY INDEX (business_day_date, global_tran_id, line_item_seq_num)
    ,COLUMN (business_day_date, global_tran_id, line_item_seq_num)
    ,COLUMN (business_day_date)
    ,COLUMN (tran_date)
    ,COLUMN (tran_time)
    ,COLUMN (worker_number)
ON t2dl_das_contribution_margin.transaction_base;

/*
=======================
SELLING AND LABOR COSTS
Author: Sara Riker
=======================
*/

-- 2022-01-31: Get commission plan information from nap hr database

-- DROP TABLE worker_commission;
CREATE MULTISET VOLATILE TABLE worker_commission AS (
SELECT DISTINCT
     worker_number
    ,beauty_line_assignment
    ,cosmetic_line_assignment
    ,other_line_assignment
    ,line_assignment
    ,job_family
    ,job_family_group
    ,job_title
    ,CASE WHEN commission_plan_name = 'N/A' THEN NULL ELSE commission_plan_name END AS commission_plan_name
    ,pay_rate_type
    ,cost_center
    ,cost_center_name
    ,payroll_department
    ,payroll_store
    ,hire_date
    ,CASE WHEN commission_plan_name IN ('Entry Comm RD') THEN -0.01     -- Entry-level Salespeople receive 1.00% less
          WHEN payroll_department = 1196 THEN 0.02
          WHEN job_title = 'Personal Stylist' THEN 0.01                 -- Personal Stylist receive 1.00% more
          ELSE 0 END AS altered_commission
    ,eff_begin_date AS begin_date
    ,eff_end_date AS end_date
FROM prd_nap_hr_usr_vws.hr_worker_dim
) WITH DATA
PRIMARY INDEX (worker_number, begin_date, end_date) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATS 
    PRIMARY INDEX (worker_number, begin_date, end_date)
    ON worker_commission;

/*
=======================================================
2022-01-31: FULFILLMENT LABOR COSTS NEEDS MORE RESEARCH
=======================================================

CREATE MULTISET VOLATILE TABLE fulfillment_activity AS (
SELECT 
     order_num AS order_num
    ,order_line_num AS order_line_num
    ,order_date_pacific AS order_date_pacific
    ,event_date_pacific AS event_date_pacific
    ,event_code AS event_code
    ,rms_sku_num AS sku_idnt
    ,action_employee_num AS action_employee_num
FROM prd_nap_usr_vws.order_line_lifecycle_fact
WHERE order_date_pacific BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates)
  AND action_employee_num IS NOT NULL
) WITH DATA
PRIMARY INDEX (order_num, order_line_num, order_date_pacific) 
ON COMMIT PRESERVE ROWS
;
*/

-- 2022-01-31: Insert into selling and labor cost table

DELETE FROM t2dl_das_contribution_margin.cost_selling_store_labor WHERE business_day_date BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates);
INSERT INTO t2dl_das_contribution_margin.cost_selling_store_labor
SELECT 
     t.business_day_date
    ,t.tran_date 
    ,t.order_num 
    ,t.global_tran_id 
    ,t.line_item_seq_num 
    ,t.commissionable_flag
    ,t.shipping_flag
    ,CASE WHEN LOWER(commission_plan_name) LIKE '%cosm%' OR LOWER(commission_plan_name) LIKE '%spa%' THEN 0.03        -- Beauty receive 3% regardless of department
          WHEN commission_plan_name LIKE 'NC%' THEN 0
          WHEN commission_plan_name IS NOT NULL AND commissionable_flag = 1 THEN altered_commission + commission_rate -- Updated commission rate base on employee's commission plan
          ELSE 0
          END AS employee_commission_rate
    ,CASE WHEN commissionable_flag = 1 AND commission_plan_name IS NOT NULL THEN line_net_usd_amt * employee_commission_rate
          ELSE 0 
          END AS commission_dollars
    ,NULL AS non_sell_lobar_dollars                                                                                   -- Temporary NULL untill pay rates are in NAP
    ,current_timestamp AS update_timestamp
FROM t2dl_das_contribution_margin.transaction_base t
LEFT JOIN worker_commission w
  ON t.worker_number = w.worker_number
 AND t.tran_time BETWEEN w.begin_date AND w.end_date
WHERE business_day_date BETWEEN (SELECT start_date FROM dates) AND (SELECT end_date FROM dates)
;

COLLECT STATS 
    PRIMARY INDEX (business_day_date, global_tran_id, line_item_seq_num)
    ON t2dl_das_contribution_margin.cost_selling_store_labor;