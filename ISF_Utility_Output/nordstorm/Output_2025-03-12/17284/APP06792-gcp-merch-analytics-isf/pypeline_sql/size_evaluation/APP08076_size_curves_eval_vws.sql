/*
Name: Size Curves Evaluation Views
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    {environment_schema} T2DL_DAS_SIZE
                {env_suffix} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_eval_vws
Author(s): Sara Riker
Date Created: 5/29/24

Creates: 
    - adoption_metrics_vw
    - eval_adoption_vw
    - size_eval_cal_week_vw
    - size_eval_cal_month_vw
    - elegible_classframe_vw

Dependancies: 
    - supp_size_hierarchy
    - adoption_metrics
    - size_eval_cal
    - size_actuals
    - size_bus_impact
    - location_model_actuals_vw
*/


REPLACE VIEW {environment_schema}.adoption_metrics_vw AS
LOCK ROW FOR ACCESS
WITH adoption AS (
    SELECT
         a.fiscal_month
        ,banner
        ,channel_id AS channel_num
        ,dept_id AS dept_idnt
        ,size_profile
        ,SUM(rcpt_units) AS rcpt_units
    FROM t2dl_das_size.adoption_metrics a
    GROUP BY 1,2,3,4,5
)
SELECT
     a.fiscal_month AS month_idnt
    ,a.banner
    ,a.channel_num
    ,a.dept_idnt
    ,COALESCE(SUM(CASE WHEN size_profile = 'ARTS' THEN rcpt_units END), 0) AS arts_curecs_rcpt_u
    ,COALESCE(SUM(CASE WHEN size_profile = 'EXISTING CURVE' THEN rcpt_units END), 0) AS existing_curves_rcpt_u
    ,COALESCE(SUM(CASE WHEN size_profile = 'NONE' THEN rcpt_units end), 0) AS none_curves_rcpt_u
    ,COALESCE(SUM(CASE WHEN size_profile = 'OTHER' THEN rcpt_units end), 0) AS other_curves_rcpt_u
    ,COALESCE(SUM(CASE WHEN size_profile = 'DSA' THEN rcpt_units END), 0) AS dsa_curves_rcpt_u
    ,SUM(rcpt_unitS) AS curves_rcpt_u
    ,COALESCE(dsa_curves_rcpt_u / NULLIFZERO(curves_rcpt_u * 1.0000), 0) AS dsa_adoption_rate
FROM adoption a
JOIN (
        SELECT DISTINCT
            dept_idnt
        FROM {environment_schema}.supp_size_hierarchy
    ) d 
  ON a.dept_idnt = d.dept_idnt
GROUP BY 1,2,3,4
;

REPLACE VIEW {environment_schema}.size_eval_cal_week_vw AS
LOCK ROW FOR ACCESS
SELECT
     week_idnt
    ,week_label
    ,ly_week_num_realigned
    ,month_idnt
    ,month_label
    ,month_end_week_idnt
    ,quarter_idnt
    ,quarter_label
    ,quarter_end_week_idnt
    ,quarter_end_month_idnt
    ,half_idnt
    ,half_label
    ,half_end_quarter_idnt
    ,fiscal_year_num
    ,hist_ind
    ,current_month
    ,last_completed_week
    ,last_completed_month
    ,last_completed_3_months
FROM {environment_schema}.size_eval_cal
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
;

REPLACE VIEW {environment_schema}.size_eval_cal_month_vw AS
LOCK ROW FOR ACCESS
SELECT
     month_idnt
    ,month_label
    ,quarter_idnt
    ,quarter_label
    ,quarter_end_month_idnt
    ,half_idnt
    ,half_label
    ,half_end_quarter_idnt
    ,fiscal_year_num
    ,MAX(hist_ind) AS hist_ind
    ,MAX(current_month) AS current_month
    ,MAX(last_completed_month) AS last_completed_month
    ,MAX(last_completed_3_months) AS last_completed_3_months
    ,MAX(next_3_months) AS next_3_months
FROM {environment_schema}.size_eval_cal
GROUP BY 1,2,3,4,5,6,7,8,9
;

REPLACE VIEW {environment_schema}.eval_adoption_vw AS
LOCK ROW FOR ACCESS
WITH adoption AS (
    SELECT
         a.fiscal_month
        ,banner
        ,channel_id AS channel_num
        ,dept_id AS dept_idnt
        ,size_profile
        ,SUM(rcpt_units) AS rcpt_units
    FROM t2dl_das_size.adoption_metrics a
    GROUP BY 1,2,3,4,5
)
SELECT
     a.fiscal_month AS month_idnt
    ,c.hist_ind
    ,c.current_month
    ,c.last_completed_3_months
    ,c.next_3_months
    ,CASE WHEN a.channel_num IN (110, 120) THEN 'NORDSTROM' ELSE 'NORDSTROM_RACK' END AS banner
    ,CASE WHEN a.channel_num IN (110, 210) THEN 'STORE' ELSE 'ONLINE' END AS selling_channel
    ,a.channel_num
    ,d.division
    ,d.subdivision
    ,d.department
    ,COALESCE(SUM(CASE WHEN size_profile = 'ARTS' THEN rcpt_units END), 0) AS arts_curecs_rcpt_u
    ,COALESCE(SUM(CASE WHEN size_profile = 'EXISTING CURVE' THEN rcpt_units END), 0) AS existing_curves_rcpt_u
    ,COALESCE(SUM(CASE WHEN size_profile = 'NONE' THEN rcpt_units end), 0) AS none_curves_rcpt_u
    ,COALESCE(SUM(CASE WHEN size_profile = 'OTHER' THEN rcpt_units end), 0) AS other_curves_rcpt_u
    ,COALESCE(SUM(CASE WHEN size_profile = 'DSA' THEN rcpt_units END), 0) AS dsa_curves_rcpt_u
    ,SUM(rcpt_unitS) AS curves_rcpt_u
    ,COALESCE(dsa_curves_rcpt_u / NULLIFZERO(curves_rcpt_u * 1.0000), 0) AS dsa_adoption_rate
FROM adoption a
JOIN (
        SELECT  
             month_idnt
            ,MAX(hist_ind) AS hist_ind
            ,MAX(current_month) AS current_month
            ,MAX(last_completed_3_months) AS last_completed_3_months
            ,MAX(next_3_months) AS next_3_months
        FROM  {environment_schema}.size_eval_cal_month_vw
        GROUP BY 1
    ) c
  ON a.fiscal_month = c.month_idnt
JOIN (
        SELECT 
             TRIM(div_idnt) || ': ' || div_desc AS division
            ,TRIM(grp_idnt) || ': ' || grp_desc AS subdivision
            ,TRIM(h.dept_idnt) || ': ' || dept_desc AS department
            ,h.dept_idnt
        FROM {environment_schema}.supp_size_hierarchy h
        GROUP BY 1,2,3,4
    ) d 
  ON a.dept_idnt = d.dept_idnt
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
;



REPLACE VIEW {environment_schema}.elegible_classframe_vw AS
LOCK ROW FOR ACCESS
SELECT 
     a.store_num
    ,a.dept_idnt
    ,a.class_frame
    ,a.supplier_frame 
    ,SUM(eoh_u) AS total_eoh_u
    ,SUM(net_sales_u) AS total_net_sales_u
    ,COUNT(DISTINCT size_1_id) sizes_per_frame
FROM {environment_schema}.size_actuals a 
WHERE ly_ty = 'TY'
GROUP BY 1,2,3,4
HAVING sizes_per_frame > 1 
    AND (total_eoh_u > 1 
     OR total_net_sales_u > 1)
;

REPLACE VIEW {environment_schema}.current_adoption_vw AS
LOCK ROW FOR ACCESS
SELECT  
     channel_id AS channel_num
    ,dept_id AS dept_idnt
    -- DSA adoption rate from AB
    ,SUM(CASE WHEN size_profile = 'DSA' THEN rcpt_units ELSE 0 END) AS dsa_curves_rcpt_u
    ,SUM(rcpt_units) AS ttl_ab_rcpt_units
    ,dsa_curves_rcpt_u / NULLIFZERO(ttl_ab_rcpt_units * 1.0000) AS dsa_adoption_rate
    -- target adoption (% of Receipts that are controllable) 
    ,SUM(controllable_receipts) AS controllable_rcpts_u
    ,SUM(total_receipts) AS total_receipts_u
    ,COALESCE(controllable_rcpts_u / NULLIF(total_receipts_u * 1.0000,0), 0) AS target_adoption
    -- normalize adoption rate with target
    ,dsa_adoption_rate * target_adoption AS weighted_adoption
    ,(target_adoption - dsa_adoption_rate) / target_adoption AS pct_to_target
    ,CASE WHEN pct_to_target < 0.05 THEN 'High Adopting'
          WHEN pct_to_target BETWEEN 0 AND .2 THEN 'Mid Adopting'
          WHEN pct_to_target > .2 THEN 'Low Adopting'
          END AS adoption_bucket
FROM t2dl_das_size.adoption_metrics a
JOIN t2dl_das_size.size_eval_cal_month_vw m
  ON a.fiscal_month = m.month_idnt
 AND m.last_completed_3_months = 1
JOIN (
        SELECT 
             a.channel_num
            ,a.dept_idnt
            ,SUM(CASE WHEN sized_receipt_u < 0 THEN 0 ELSE sized_receipt_u END + CASE WHEN unknown_receipt_u < 0 THEN 0 ELSE unknown_receipt_u END) AS controllable_receipts
            ,SUM(receipts_u) AS total_receipts
            ,COALESCE(controllable_receipts / NULLIF(total_receipts * 1.0000,0), 0) AS pct_controllable_rcts
        FROM t2dl_das_size.size_actuals a 
        WHERE ly_ty = 'TY'
        GROUP BY 1,2
        HAVING controllable_receipts > 0
    ) t
  ON a.channel_id = t.channel_num
 AND a.dept_id = t.dept_idnt
GROUP BY 1,2
;

REPLACE VIEW {environment_schema}.size_bus_impact_vw AS
LOCK ROW FOR ACCESS
SELECT
     a.channel_num
    ,CASE WHEN a.channel_num IN (110, 120) THEN 'NORDSTROM' ELSE 'NORDSTROM_RACK' END AS banner
    ,CASE WHEN a.channel_num IN (110, 210) THEN 'STORE' ELSE 'ONLINE' END AS selling_channel
    ,a.dept_idnt
    ,d.division
    ,d.subdivision
    ,d.department
    ,COALESCE(ad.adoption_bucket, 'Low Adopting') AS adoption
    -- TY
    ,COALESCE(ty_net_sales_r, 0) AS ty_net_sales_r
    ,COALESCE(ty_net_sales_u, 0) AS ty_net_sales_u
    ,COALESCE(ty_reg_net_sales_r, 0) AS ty_reg_net_sales_r
    ,COALESCE(ty_reg_net_sales_u, 0) AS ty_reg_net_sales_u
    ,COALESCE(ty_product_margin, 0) AS ty_product_margin
    ,COALESCE(ty_eoh_u, 0) AS ty_eoh_u
    ,COALESCE(ty_reg_eoh_u, 0) AS ty_reg_eoh_u
    -- LY
    ,COALESCE(ly_net_sales_r, 0) AS ly_net_sales_r
    ,COALESCE(ly_net_sales_u, 0) AS ly_net_sales_u
    ,COALESCE(ly_reg_net_sales_r, 0) AS ly_reg_net_sales_r
    ,COALESCE(ly_reg_net_sales_u, 0) AS ly_reg_net_sales_u
    ,COALESCE(ly_product_margin, 0) AS ly_product_margin
    ,COALESCE(ly_eoh_u, 0) AS ly_eoh_u
    ,COALESCE(ly_reg_eoh_u, 0) AS ly_reg_eoh_u
FROM (
        SELECT
             channel_num
            ,a.dept_idnt
             -- TY
            ,SUM(CASE WHEN a.ly_ty = 'TY' THEN net_sales_r ELSE 0 END) AS ty_net_sales_r
            ,SUM(CASE WHEN a.ly_ty = 'TY' THEN net_sales_u ELSE 0 END) AS ty_net_sales_u
            ,SUM(CASE WHEN a.ly_ty = 'TY' THEN reg_net_sales_r ELSE 0 END) AS ty_reg_net_sales_r
            ,SUM(CASE WHEN a.ly_ty = 'TY' THEN reg_net_sales_u ELSE 0 END) AS ty_reg_net_sales_u
            ,SUM(CASE WHEN a.ly_ty = 'TY' THEN product_margin ELSE 0 END) AS ty_product_margin
            ,SUM(CASE WHEN a.ly_ty = 'TY' THEN eoh_u ELSE 0 END) AS ty_eoh_u
            ,SUM(CASE WHEN a.ly_ty = 'TY' THEN reg_eoh_u ELSE 0 END) AS ty_reg_eoh_u
            -- LY
            ,SUM(CASE WHEN a.ly_ty = 'LY' THEN net_sales_r ELSE 0 END) AS ly_net_sales_r
            ,SUM(CASE WHEN a.ly_ty = 'LY' THEN net_sales_u ELSE 0 END) AS ly_net_sales_u
            ,SUM(CASE WHEN a.ly_ty = 'LY' THEN reg_net_sales_r ELSE 0 END) AS ly_reg_net_sales_r
            ,SUM(CASE WHEN a.ly_ty = 'LY' THEN reg_net_sales_u ELSE 0 END) AS ly_reg_net_sales_u
            ,SUM(CASE WHEN a.ly_ty = 'LY' THEN product_margin ELSE 0 END) AS ly_product_margin
            ,SUM(CASE WHEN a.ly_ty = 'LY' THEN eoh_u ELSE 0 END) AS ly_eoh_u
            ,SUM(CASE WHEN a.ly_ty = 'LY' THEN reg_eoh_u ELSE 0 END) AS ly_reg_eoh_u
        FROM {environment_schema}.size_actuals a
        JOIN {environment_schema}.elegible_classframe_vw e
          ON a.store_num = e.store_num
         AND a.dept_idnt = e.dept_idnt
         AND a.class_frame = e.class_frame
         AND a.supplier_frame = e.supplier_frame
        GROUP BY 1,2
    ) a
LEFT JOIN {environment_schema}.current_adoption_vw ad
  ON a.channel_num = ad.channel_num
 AND a.dept_idnt = ad.dept_idnt
LEFT JOIN (
       SELECT 
             TRIM(div_idnt) || ': ' || div_desc AS division
            ,TRIM(grp_idnt) || ': ' || grp_desc AS subdivision
            ,TRIM(h.dept_idnt) || ': ' || dept_desc AS department
            ,d.dept_idnt
        FROM {environment_schema}.supp_size_hierarchy h
        JOIN (SELECT DISTINCT dept_idnt FROM {environment_schema}.supp_size_hierarchy) d
          ON h.dept_idnt = d.dept_idnt
        GROUP BY 1,2,3,4
    ) d
  ON d.dept_idnt = a.dept_idnt
;


REPLACE VIEW {environment_schema}.location_model_actuals_vw AS
LOCK ROW FOR ACCESS
WITH actuals AS (
    SELECT a.* 
    FROM {environment_schema}.size_actuals a
    JOIN {environment_schema}.elegible_classframe_vw e
      ON a.store_num = e.store_num
     AND a.dept_idnt = e.dept_idnt
     AND a.class_frame = e.class_frame
     AND a.supplier_frame = e.supplier_frame
    WHERE ly_ty = 'TY'
),
models AS (
    SELECT 
         a.*
    FROM {environment_schema}.size_curves_model_rec a
    JOIN {environment_schema}.elegible_classframe_vw e
      ON a.store_num = e.store_num
     AND a.dept_idnt = e.dept_idnt
     AND a.class_frame = e.class_frame
     AND a.supplier_frame = e.supplier_frame
    WHERE a.half_idnt = (SELECT MIN(DISTINCT half_idnt) FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1)
) 
SELECT
     l.*
    ,COALESCE(m.cluster_id, c.cluster_id, 10) AS cluster_id
    ,COALESCE(m.cluster_name, c.cluster_name, 'None') AS cluster_name
    ,COALESCE(a.npg_ind,'N') AS npg_ind
    ,COALESCE(a.dept_idnt, m.dept_idnt) AS dept_idnt
    ,d.division
    ,d.subdivision
    ,d.department
    ,COALESCE(cl.class_desc, a.class_frame , m.class_frame) AS class_desc 
    ,COALESCE(a.class_frame, m.class_frame) AS class_frame
    ,COALESCE(a.supplier_frame, m.supplier_frame) AS supplier_frame
    ,COALESCE(s.supplier, a.supplier_frame, m.supplier_frame) AS supplier 
    ,COALESCE(a.size_1_frame, m.frame) AS size_1_frame
    ,COALESCE(a.size_1_rank, m.size_1_rank) AS size_1_rank
    ,COALESCE(a.size_1_id, m.size_1_id) AS size_1_id
    ,COALESCE(rp_ind, 'N') AS rp_ind
    ,m.bulk_ratio AS bulk_ratio
    ,m.cluster_ratio AS cluster_ratio
    -- Net Sales
      -- units
    ,COALESCE(net_sales_u, 0) AS net_sales_u
    ,COALESCE(ds_net_sales_u, 0) AS ds_net_sales_u
    ,COALESCE(reg_net_sales_u, 0) AS reg_net_sales_u
    ,COALESCE(pro_net_sales_u, 0) AS pro_net_sales_u
    ,COALESCE(clr_net_sales_u, 0) AS clr_net_sales_u
      --retail
    ,COALESCE(net_sales_r, 0) AS net_sales_r
    ,COALESCE(ds_net_sales_r, 0) AS ds_net_sales_r
    ,COALESCE(reg_net_sales_r, 0) AS reg_net_sales_r
    ,COALESCE(pro_net_sales_r, 0) AS pro_net_sales_r
    ,COALESCE(clr_net_sales_r, 0) AS clr_net_sales_r
      --cost
    ,COALESCE(net_sales_c, 0) AS net_sales_c
    ,COALESCE(ds_net_sales_c, 0) AS ds_net_sales_c
    ,COALESCE(reg_net_sales_c, 0) AS reg_net_sales_c
    ,COALESCE(pro_net_sales_c, 0) AS pro_net_sales_c
    ,COALESCE(clr_net_sales_c, 0) AS clr_net_sales_c
    --returns
      --unit
    ,COALESCE(returns_u, 0) AS returns_u
    ,COALESCE(ds_returns_u, 0) AS ds_returns_u
    ,COALESCE(reg_returns_u, 0) AS reg_returns_u
    ,COALESCE(pro_returns_u, 0) AS pro_returns_u
    ,COALESCE(clr_returns_u, 0) AS clr_returns_u
      --retail
    ,COALESCE(returns_r, 0) AS returns_r
    ,COALESCE(ds_returns_r, 0) AS ds_returns_r
    ,COALESCE(reg_returns_r, 0) AS reg_returns_r
    ,COALESCE(pro_returns_r, 0) AS pro_returns_r
    ,COALESCE(clr_returns_r, 0) AS clr_returns_r
      --cost
    ,COALESCE(returns_c, 0) AS returns_c
    ,COALESCE(ds_returns_c, 0) AS ds_returns_c
    ,COALESCE(reg_returns_c, 0) AS reg_returns_c
    ,COALESCE(pro_returns_c, 0) AS pro_returns_c
    ,COALESCE(clr_returns_c, 0) AS clr_returns_c
    --gross sales
      --unit
    ,COALESCE(gross_sales_u, 0) AS gross_sales_u
    ,COALESCE(ds_gross_sales_u, 0) AS ds_gross_sales_u
    ,COALESCE(reg_gross_sales_u, 0) AS reg_gross_sales_u
    ,COALESCE(pro_gross_sales_u, 0) AS pro_gross_sales_u
    ,COALESCE(clr_gross_sales_u, 0) AS clr_gross_sales_u
      --retail
    ,COALESCE(gross_sales_r, 0) AS gross_sales_r
    ,COALESCE(ds_gross_sales_r, 0) AS ds_gross_sales_r
    ,COALESCE(reg_gross_sales_r, 0) AS reg_gross_sales_r
    ,COALESCE(pro_gross_sales_r, 0) AS pro_gross_sales_r
    ,COALESCE(clr_gross_sales_r, 0) AS clr_gross_sales_r
      --cost
    ,COALESCE(gross_sales_c, 0) AS gross_sales_c
    ,COALESCE(ds_gross_sales_c, 0) AS ds_gross_sales_c
    ,COALESCE(reg_gross_sales_c, 0) AS reg_gross_sales_c
    ,COALESCE(pro_gross_sales_c, 0) AS pro_gross_sales_c
    ,COALESCE(clr_gross_sales_c, 0) AS clr_gross_sales_c
    --product margin
    ,COALESCE(product_margin, 0) AS product_margin
    ,COALESCE(ds_product_margin, 0) AS ds_product_margin
    ,COALESCE(reg_product_margin, 0) AS reg_product_margin
    ,COALESCE(pro_product_margin, 0)AS pro_product_margin    
    ,COALESCE(clr_product_margin, 0) AS clr_product_margin
    -- inventory
    ,COALESCE(eoh_u, 0) AS eoh_u
    ,COALESCE(reg_eoh_u, 0) AS reg_eoh_u
    ,COALESCE(cl_eoh_u, 0) AS cl_eoh_u
    ,COALESCE(eoh_r, 0) AS eoh_r
    ,COALESCE(reg_eoh_r, 0) AS reg_eoh_r
    ,COALESCE(cl_eoh_r, 0) AS cl_eoh_r
    ,COALESCE(eoh_c, 0) AS eoh_c
    ,COALESCE(reg_eoh_c, 0) AS reg_eoh_c
    ,COALESCE(cl_eoh_c, 0) AS cl_eoh_c
    -- receipts
    ,COALESCE(receipts_u, 0) AS receipts_u
    ,COALESCE(receipts_c, 0) AS receipts_c
    ,COALESCE(receipts_r, 0) AS receipts_r
    -- DS reciepts 
    ,COALESCE(ds_receipts_u, 0) AS ds_receipts_u
    ,COALESCE(ds_receipts_c, 0) AS ds_receipts_c
    ,COALESCE(ds_receipts_r, 0) AS ds_receipts_r
    -- close outs
    ,COALESCE(close_out_receipt_u, 0) AS close_out_receipt_u
    ,COALESCE(close_out_receipt_c, 0) AS close_out_receipt_c
    ,COALESCE(close_out_receipt_r, 0) AS close_out_receipt_r
    -- sized close outs
    ,COALESCE(close_out_sized_receipt_u, 0) AS close_out_sized_receipt_u
    ,COALESCE(close_out_sized_receipt_c, 0) AS close_out_sized_receipt_c
    ,COALESCE(close_out_sized_receipt_r, 0) AS close_out_sized_receipt_r
    -- casepacks
    ,COALESCE(casepack_receipt_u, 0) AS casepack_receipt_u
    ,COALESCE(casepack_receipt_c, 0)AS casepack_receipt_c
    ,COALESCE(casepack_receipt_r, 0) AS casepack_receipt_r
    -- prepacks
    ,COALESCE(prepack_receipt_u, 0) AS prepack_receipt_u
    ,COALESCE(prepack_receipt_c, 0) AS prepack_receipt_c
    ,COALESCE(prepack_receipt_r, 0) AS prepack_receipt_r
    -- Item
    ,COALESCE(sized_receipt_u, 0) AS sized_receipt_u
    ,COALESCE(sized_receipt_c, 0) AS sized_receipt_c
    ,COALESCE(sized_receipt_r, 0) AS sized_receipt_r
    -- unknown
    ,COALESCE(unknown_receipt_u, 0) AS unknown_receipt_u
    ,COALESCE(unknown_receipt_c, 0) AS unknown_receipt_c
    ,COALESCE(unknown_receipt_r, 0) AS unknown_receipt_r
    -- transfers
    ,COALESCE(transfer_out_u , 0) AS transfer_out_u
    ,COALESCE(transfer_in_u, 0) AS transfer_in_u
    ,COALESCE(racking_last_chance_out_u, 0) AS racking_last_chance_out_u
    ,COALESCE(flx_in_u, 0) AS flx_in_u
    ,COALESCE(stock_balance_out_u, 0) AS stock_balance_out_u
    ,COALESCE(stock_balance_in_u, 0) AS stock_balance_in_u
    ,COALESCE(reserve_stock_in_u, 0) AS reserve_stock_in_u
    ,COALESCE(pah_in_u, 0) AS pah_in_u
    ,COALESCE(cust_return_out_u, 0) AS cust_return_out_u
    ,COALESCE(cust_return_in_u, 0) AS cust_return_in_u
    ,COALESCE(cust_transfer_out_u, 0) AS cust_transfer_out_u
    ,COALESCE(cust_transfer_in_u , 0) AS cust_transfer_in_u
FROM actuals a
FULL OUTER JOIN models m
  ON a.channel_num = m.channel_num
 AND a.store_num = m.store_num
 AND a.dept_idnt = m.dept_idnt
 AND a.class_frame = m.class_frame
 AND a.supplier_frame = m.supplier_frame
 AND a.size_1_frame = m.frame
 AND a.size_1_id = m.size_1_id
LEFT JOIN (
        SELECT
             store_num
            ,cluster_id
            ,cluster_name
            ,dept_idnt
            ,class_frame
            ,supplier_frame
            ,frame
        FROM models
        WHERE cluster_id IS NOT NULL
        GROUP BY 1,2,3,4,5,6,7
    ) c
  ON a.store_num = c.store_num
 AND a.dept_idnt = c.dept_idnt
 AND a.class_frame = c.class_frame
 AND a.supplier_frame = c.supplier_frame
 AND a.size_1_frame = c.frame
JOIN {environment_schema}.locations l
  ON COALESCE(a.store_num, m.store_num) = l.store_num
JOIN (
        SELECT 
             TRIM(div_idnt) || ': ' || div_desc AS division
            ,TRIM(grp_idnt) || ': ' || grp_desc AS subdivision
            ,TRIM(h.dept_idnt) || ': ' || dept_desc AS department
            ,dept_idnt
        FROM {environment_schema}.supp_size_hierarchy h
        GROUP BY 1,2,3,4
    ) d 
  ON COALESCE(a.dept_idnt, m.dept_idnt) = d.dept_idnt
LEFT JOIN (
        SELECT 
             TRIM(class_idnt) || ': ' || class_desc AS class_desc
            ,dept_idnt
            ,class_idnt
        FROM {environment_schema}.supp_size_hierarchy h
        GROUP BY 1,2,3
    ) cl
  ON COALESCE(a.dept_idnt, m.dept_idnt) = cl.dept_idnt
 AND STRTOK(COALESCE(a.class_frame, m.class_frame), '~~', 2)= cl.class_idnt
LEFT JOIN (
        SELECT 
             TRIM(supplier_idnt) || ': ' || supplier_name AS supplier
            ,dept_idnt
            ,supplier_idnt
        FROM {environment_schema}.supp_size_hierarchy h
        GROUP BY 1,2,3
    ) s
  ON COALESCE(a.dept_idnt, m.dept_idnt) = s.dept_idnt
 AND COALESCE(a.supplier_frame, m.supplier_frame) = s.supplier_idnt
;
