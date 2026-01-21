/*
Name: Size Curves Evaluation Accuracy 
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    {{environment_schema}} T2DL_DAS_SIZE
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_eval_vws
Author(s): Sara Riker
Date Created: 6/12/24
Date Updated: 7/15/24

Creates: 
    - accuracy

Dependancies: 
    - location_model_actuals_vw
    - supp_size_hierarchy
    - locations
    - size_eval_cal_month_vw
*/

-- baseline bulk
CREATE MULTISET VOLATILE TABLE baseline_bulk_ratios AS (
SELECT
     a.channel_num
    ,a.dept_idnt
    ,a.class_frame
    ,a.supplier_frame
    ,a.size_1_frame
    ,a.size_1_id
    -- baseline
    ,SUM(CASE WHEN ly_ty = 'LY' THEN CASE WHEN a.reg_net_sales_u < 0 THEN 0 ELSE a.reg_net_sales_u END ELSE 0 END) AS ly_size_sales_u
    ,SUM(ly_size_sales_u) OVER (PARTITION BY channel_num, a.dept_idnt, a.class_frame, a.supplier_frame, a.size_1_frame) AS ly_sales_frame_u
    ,COALESCE(ly_size_sales_u / NULLIF(ly_sales_frame_u * 1.0000, 0), 0) AS baseline_bulk_ratio
    -- Actual sales
    ,SUM(CASE WHEN ly_ty = 'TY' THEN CASE WHEN a.reg_net_sales_u < 0 THEN 0 ELSE a.reg_net_sales_u END ELSE 0 END) AS ty_size_sales_u
    ,SUM(ty_size_sales_u) OVER (PARTITION BY channel_num, a.dept_idnt, a.class_frame, a.supplier_frame, a.size_1_frame) AS ty_sales_frame_u
    ,COALESCE(ty_size_sales_u / NULLIF(ty_sales_frame_u * 1.0000, 0), 0) AS sales_baseline_bulk_ratio
    -- Actual Receipts
    ,SUM(CASE WHEN ly_ty = 'TY' THEN CASE WHEN a.receipts_u < 0 THEN 0 ELSE a.receipts_u END ELSE 0 END) AS ty_size_rcpt_u
    ,SUM(ty_size_rcpt_u) OVER (PARTITION BY channel_num, a.dept_idnt, a.class_frame, a.supplier_frame, a.size_1_frame) AS ty_rcpt_frame_u
    ,COALESCE(ty_size_rcpt_u / NULLIF(ty_rcpt_frame_u * 1.0000, 0), 0) AS rcpt_baseline_bulk_ratio
FROM t2dl_das_size.size_actuals a
JOIN t2dl_das_size.elegible_classframe_vw e
  ON a.store_num = e.store_num
 AND a.dept_idnt = e.dept_idnt
 AND a.class_frame = e.class_frame
 AND a.supplier_frame = e.supplier_frame
GROUP BY 1,2,3,4,5,6
) WITH DATA
PRIMARY INDEX(channel_num, dept_idnt, class_frame, supplier_frame, size_1_id)
ON COMMIT PRESERVE ROWS;

-- DROP TABLE baseline_bulk_wmae;
CREATE MULTISET VOLATILE TABLE baseline_bulk_wmae AS (
WITH baseline_bulk_absolute_errors AS (
SELECT
     channel_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    ,size_1_id
    ,baseline_bulk_ratio
    -- FRAME
    ,ty_sales_frame_u
    ,ty_rcpt_frame_u
    -- RATIOS
    ,sales_baseline_bulk_ratio
    ,rcpt_baseline_bulk_ratio
    
    -- baseline WMAE
    -- sales v baseline
    ,ABS(sales_baseline_bulk_ratio - baseline_bulk_ratio) AS abs_err_sales_baseline_bulk
    ,abs_err_sales_baseline_bulk * sales_baseline_bulk_ratio AS w_error_sales_baseline_bulk
    -- receipts v baseline 
    ,ABS(rcpt_baseline_bulk_ratio - baseline_bulk_ratio) AS abs_err_rcpt_baseline_bulk
    ,abs_err_rcpt_baseline_bulk * rcpt_baseline_bulk_ratio AS w_error_rcpt_baseline_bulk
FROM baseline_bulk_ratios
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
,baseline_bulk_aggregated_errors AS (
SELECT
     channel_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    -- NUMERATOR
    ,ty_sales_frame_u
    ,ty_rcpt_frame_u

    -- DENOMINATOR
    ,SUM(ty_sales_frame_u) OVER (PARTITION BY channel_num, dept_idnt) AS sales_dept
    ,SUM(ty_rcpt_frame_u) OVER (PARTITION BY channel_num, dept_idnt) AS receipts_dept
    -- WMAE WEIGHTS
    ,ty_sales_frame_u / NULLIFZERO(sales_dept * 1.0000) AS sales_weight
    ,ty_rcpt_frame_u / NULLIFZERO(receipts_dept * 1.0000) AS receipts_weight
    -- BASELINE WMAE
    ,SUM(w_error_sales_baseline_bulk) AS wmae_sales_baseline_bulk
    ,SUM(w_error_rcpt_baseline_bulk) AS wmae_receipts_baseline_bulk
FROM baseline_bulk_absolute_errors
GROUP BY 1,2,3,4,5,6,7
)
SELECT
     channel_num
    ,dept_idnt
    ,sales_dept
    ,receipts_dept
    ,SUM(wmae_sales_baseline_bulk * sales_weight) AS wmae_baseline_sls_bulk
    ,SUM(wmae_receipts_baseline_bulk * receipts_weight) AS wmae_baseline_rcpt_bulk
FROM baseline_bulk_aggregated_errors
GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX(channel_num, dept_idnt)
ON COMMIT PRESERVE ROWS;

-- baseline cluster
-- DROP TABLE baseline_loc_ratios;
CREATE MULTISET VOLATILE TABLE baseline_loc_ratios AS (
SELECT
     a.channel_num
    ,a.store_num
    ,a.dept_idnt
    ,a.class_frame
    ,a.supplier_frame
    ,a.size_1_frame
    ,a.size_1_id
    -- baseline
    ,SUM(CASE WHEN ly_ty = 'LY' THEN CASE WHEN a.reg_net_sales_u < 0 THEN 0 ELSE a.reg_net_sales_u END ELSE 0 END) AS ly_size_sales_u
    ,SUM(ly_size_sales_u) OVER (PARTITION BY channel_num, a.store_num, a.dept_idnt, a.class_frame, a.supplier_frame, a.size_1_frame) AS ly_sales_frame_u
    ,COALESCE(ly_size_sales_u / NULLIF(ly_sales_frame_u * 1.0000, 0), 0) AS baseline_loc_ratio
    -- Actual sales
    ,SUM(CASE WHEN ly_ty = 'TY' THEN CASE WHEN a.reg_net_sales_u < 0 THEN 0 ELSE a.reg_net_sales_u END ELSE 0 END) AS ty_size_sales_u
    ,SUM(ty_size_sales_u) OVER (PARTITION BY channel_num, a.store_num, a.dept_idnt, a.class_frame, a.supplier_frame, a.size_1_frame) AS ty_sales_frame_u
    ,COALESCE(ty_size_sales_u / NULLIF(ty_sales_frame_u * 1.0000, 0), 0) AS sales_baseline_loc_ratio
    -- Actual Receipts
    ,SUM(CASE WHEN ly_ty = 'TY' THEN CASE WHEN a.receipts_u < 0 THEN 0 ELSE a.receipts_u END ELSE 0 END) AS ty_size_rcpt_u
    ,SUM(ty_size_rcpt_u) OVER (PARTITION BY channel_num, a.store_num, a.dept_idnt, a.class_frame, a.supplier_frame, a.size_1_frame) AS ty_rcpt_frame_u
    ,COALESCE(ty_size_rcpt_u / NULLIF(ty_rcpt_frame_u * 1.0000, 0), 0) AS rcpt_baseline_loc_ratio
FROM t2dl_das_size.size_actuals a
JOIN t2dl_das_size.elegible_classframe_vw e
  ON a.store_num = e.store_num
 AND a.dept_idnt = e.dept_idnt
 AND a.class_frame = e.class_frame
 AND a.supplier_frame = e.supplier_frame
GROUP BY 1,2,3,4,5,6,7
) WITH DATA
PRIMARY INDEX(channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_id)
ON COMMIT PRESERVE ROWS;

-- DROP TABLE baseline_loc_wmae;
CREATE MULTISET VOLATILE TABLE baseline_loc_wmae AS (
WITH baseline_loc_absolute_errors AS (
SELECT
     channel_num
    ,store_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    ,size_1_id
    ,baseline_loc_ratio
    -- FRAME
    ,ty_sales_frame_u
    ,ty_rcpt_frame_u
    -- RATIOS
    ,sales_baseline_loc_ratio
    ,rcpt_baseline_loc_ratio
    
    -- baseline WMAE
    -- sales v baseline
    ,ABS(sales_baseline_loc_ratio - baseline_loc_ratio) AS abs_err_sales_baseline_loc
    ,abs_err_sales_baseline_loc * sales_baseline_loc_ratio AS w_error_sales_baseline_loc
    -- receipts v baseline 
    ,ABS(rcpt_baseline_loc_ratio - baseline_loc_ratio) AS abs_err_rcpt_baseline_loc
    ,abs_err_rcpt_baseline_loc * rcpt_baseline_loc_ratio AS w_error_rcpt_baseline_loc
FROM baseline_loc_ratios
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
,baseline_loc_aggregated_errors AS (
SELECT
     channel_num
    ,store_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    -- NUMERATOR
    ,ty_sales_frame_u
    ,ty_rcpt_frame_u

    -- DENOMINATOR
    ,SUM(ty_sales_frame_u) OVER (PARTITION BY channel_num, store_num, dept_idnt) AS sales_dept
    ,SUM(ty_rcpt_frame_u) OVER (PARTITION BY channel_num, store_num, dept_idnt) AS receipts_dept
    -- WMAE WEIGHTS
    ,ty_sales_frame_u / NULLIFZERO(sales_dept * 1.0000) AS sales_weight
    ,ty_rcpt_frame_u / NULLIFZERO(receipts_dept * 1.0000) AS receipts_weight
    -- BASELINE WMAE
    ,SUM(w_error_sales_baseline_loc) AS wmae_sales_baseline_loc
    ,SUM(w_error_rcpt_baseline_loc) AS wmae_receipts_baseline_loc
FROM baseline_loc_absolute_errors
GROUP BY 1,2,3,4,5,6,7,8
)
SELECT
     channel_num
    ,store_num
    ,dept_idnt
    ,sales_dept
    ,receipts_dept
    ,SUM(wmae_sales_baseline_loc * sales_weight) AS wmae_baseline_sls_loc
    ,SUM(wmae_receipts_baseline_loc * receipts_weight) AS wmae_baseline_rcpt_loc
FROM baseline_loc_aggregated_errors
GROUP BY 1,2,3,4,5
) WITH DATA
PRIMARY INDEX(channel_num, dept_idnt)
ON COMMIT PRESERVE ROWS;

-- combine baseline wmae
CREATE MULTISET VOLATILE TABLE baseline_wmae AS (
SELECT 
     l.* 
    ,b.wmae_baseline_sls_bulk
    ,b.wmae_baseline_rcpt_bulk
FROM baseline_loc_wmae l
JOIN baseline_bulk_wmae b
  ON l.channel_num = b.channel_num
 AND l.dept_idnt = b.dept_idnt
) WITH DATA
PRIMARY INDEX(channel_num, store_num, dept_idnt)
ON COMMIT PRESERVE ROWS;
;

-- DROP TABLE bulk_ratios;
CREATE MULTISET VOLATILE TABLE bulk_ratios AS (
SELECT 
     channel_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    ,size_1_id
    ,MAX(bulk_ratio) AS bulk_ratio
    -- NUMERATOR
    ,SUM(CASE WHEN net_sales_u < 0 THEN 0 ELSE net_sales_u END) AS sales_size
    ,SUM(net_sales_r) AS sales_size_r
    ,SUM(CASE WHEN reg_net_sales_u < 0 THEN 0 ELSE reg_net_sales_u END) AS reg_sales_size
    ,SUM(eoh_u) AS eoh_size
    ,SUM(reg_eoh_u) AS reg_eoh_size
    ,SUM(sized_receipt_u + unknown_receipt_u) AS receipts_size
    -- DENOMINATOR
    ,SUM(sales_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS sales_frame
    ,SUM(sales_size_r) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS sales_frame_r
    ,SUM(reg_sales_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS reg_sales_frame
    ,SUM(eoh_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS eoh_frame
    ,SUM(reg_eoh_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS reg_eoh_frame
    ,SUM(receipts_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS receipts_frame
    -- SIZE RATIOS
    ,sales_size / NULLIFZERO(sales_frame * 1.0000) AS sales_ratio
    ,reg_sales_size / NULLIFZERO(reg_sales_frame * 1.0000) AS reg_sales_ratio
    ,eoh_size / NULLIFZERO(eoh_frame * 1.0000) AS eoh_ratio
    ,reg_eoh_size / NULLIFZERO(reg_eoh_frame * 1.0000) AS reg_eoh_ratio
    ,receipts_size / NULLIFZERO(receipts_frame * 1.0000) AS receipts_ratio
FROM {environment_schema}.location_model_actuals_vw --{environment_schema}
GROUP BY 1,2,3,4,5,6
) WITH DATA
PRIMARY INDEX(channel_num, dept_idnt, class_frame, supplier_frame, size_1_id)
ON COMMIT PRESERVE ROWS;

-- DROP TABLE bulk_wmae;
CREATE MULTISET VOLATILE TABLE bulk_wmae AS (
WITH bulk_absolute_errors AS (
SELECT
     channel_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    ,size_1_id
    ,bulk_ratio
    -- FRAME
    ,sales_frame
    ,sales_frame_r
    ,reg_sales_frame
    ,eoh_frame
    ,reg_eoh_frame
    ,receipts_frame
    -- RATIOS
    ,sales_ratio
    ,reg_sales_ratio
    ,eoh_ratio
    ,reg_eoh_ratio
    ,receipts_ratio
    -- MODEL WMAE
    -- sales v model
    ,ABS(sales_ratio - bulk_ratio) AS abs_err_sales_bulk
    ,abs_err_sales_bulk * sales_ratio AS w_error_sales_bulk
    -- reg sales v model
    ,ABS(reg_sales_ratio - bulk_ratio) AS abs_err_reg_sales_bulk
    ,abs_err_reg_sales_bulk * reg_sales_ratio AS w_error_reg_sales_bulk
    -- eoh v model
    ,ABS(eoh_ratio - bulk_ratio) AS abs_err_eoh_bulk
    ,abs_err_eoh_bulk * eoh_ratio AS w_error_eoh_bulk
    -- reg eoh v model
    ,ABS(reg_eoh_ratio - bulk_ratio) AS abs_err_reg_eoh_bulk
    ,abs_err_reg_eoh_bulk * reg_eoh_ratio AS w_error_reg_eoh_bulk
    -- receipts v model 
    ,ABS(receipts_ratio - bulk_ratio) AS abs_err_rcpt_bulk
    ,abs_err_rcpt_bulk * receipts_ratio AS w_error_rcpt_bulk
    -- ACTUAL WMAE
    -- sales v receipts 
    ,ABS(sales_ratio - receipts_ratio) AS abs_err_sales_rcpts_bulk
    ,abs_err_sales_rcpts_bulk * sales_ratio AS w_error_sales_rcpts_bulk
    -- sales v eoh
    ,ABS(sales_ratio - eoh_ratio) AS abs_err_sales_eoh_bulk
    ,abs_err_sales_eoh_bulk * sales_ratio AS w_error_sales_eoh_bulk
    -- reg sales v reg eoh
    ,ABS(reg_sales_ratio - reg_eoh_ratio) AS abs_err_reg_sales_eoh_bulk
    ,abs_err_reg_sales_eoh_bulk * reg_sales_ratio AS w_error_reg_sales_eoh_bulk
    -- reg sales v receipts
    ,ABS(reg_sales_ratio - receipts_ratio) AS abs_err_reg_sales_rcpt_bulk
    ,abs_err_reg_sales_rcpt_bulk * reg_sales_ratio AS w_error_reg_sales_rcpts_bulk
FROM bulk_ratios
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
,bulk_aggregated_errors AS (
SELECT
     channel_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    -- NUMERATOR
    ,sales_frame
    ,sales_frame_r
    ,reg_sales_frame
    ,eoh_frame
    ,reg_eoh_frame
    ,receipts_frame
    -- DENOMINATOR
    ,SUM(sales_frame) OVER (PARTITION BY channel_num, dept_idnt) AS sales_dept
    ,SUM(sales_frame_r) OVER (PARTITION BY channel_num, dept_idnt) AS sales_dept_r
    ,SUM(reg_sales_frame) OVER (PARTITION BY channel_num, dept_idnt) AS reg_sales_dept
    ,SUM(eoh_frame) OVER (PARTITION BY channel_num, dept_idnt) AS eoh_dept
    ,SUM(reg_eoh_frame) OVER (PARTITION BY channel_num, dept_idnt) AS reg_eoh_dept
    ,SUM(receipts_frame) OVER (PARTITION BY channel_num, dept_idnt) AS receipts_dept
    -- WMAE WEIGHTS
    ,sales_frame / NULLIFZERO(sales_dept * 1.0000) AS sales_weight
    ,reg_sales_frame / NULLIFZERO(reg_sales_dept * 1.0000) AS reg_sales_weight
    ,eoh_frame / NULLIFZERO(eoh_dept * 1.0000) AS eoh_weight
    ,reg_eoh_frame / NULLIFZERO(reg_eoh_dept * 1.0000) AS reg_eoh_weight
    ,receipts_frame / NULLIFZERO(receipts_dept * 1.0000) AS receipts_weight
    -- MODEL WMAE
    ,SUM(w_error_sales_bulk) AS wmae_sales_bulk
    ,SUM(w_error_reg_sales_bulk) AS wmae_reg_sales_bulk
    ,SUM(w_error_eoh_bulk) AS wmae_eoh_bulk
    ,SUM(w_error_reg_eoh_bulk) AS wmae_reg_eoh_bulk
    ,SUM(w_error_rcpt_bulk) AS wmae_receipts_bulk
    -- ACTUAL WMAE
    ,SUM(w_error_sales_rcpts_bulk) AS wmae_sales_rcpts_bulk
    ,SUM(w_error_reg_sales_rcpts_bulk) AS wmae_reg_sales_rcpts_bulk
    ,SUM(w_error_sales_eoh_bulk) AS wmae_sales_eoh_bulk
    ,SUM(w_error_reg_sales_eoh_bulk) AS wmae_reg_sales_eoh_bulk
FROM bulk_absolute_errors
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
SELECT
     channel_num
    ,dept_idnt
    ,sales_dept
    ,sales_dept_r
    ,eoh_dept
    ,receipts_dept
    ,reg_sales_dept
    ,reg_eoh_dept
    ,SUM(wmae_sales_bulk * sales_weight) AS wmae_sales_bulk
    ,SUM(wmae_reg_sales_bulk * reg_sales_weight) AS wmae_reg_sales_bulk
    ,SUM(wmae_eoh_bulk * eoh_weight) AS wmae_eoh_bulk
    ,SUM(wmae_reg_eoh_bulk * reg_eoh_weight) AS wmae_reg_eoh_bulk
    ,SUM(wmae_receipts_bulk * receipts_weight) AS wmae_receipts_bulk
    ,SUM(wmae_sales_rcpts_bulk  * sales_weight) AS wmae_sales_rcpts_bulk
    ,SUM(wmae_reg_sales_rcpts_bulk  * reg_sales_weight) AS wmae_reg_sales_rcpts_bulk
    ,SUM(wmae_sales_eoh_bulk * sales_weight) AS wmae_sales_eoh_bulk
    ,SUM(wmae_reg_sales_eoh_bulk * reg_sales_weight) AS wmae_reg_sales_eoh_bulk
FROM bulk_aggregated_errors
GROUP BY 1,2,3,4,5,6,7,8
) WITH DATA
PRIMARY INDEX(channel_num, dept_idnt)
ON COMMIT PRESERVE ROWS;

-- DROP TABLE cluster_ratios;
CREATE MULTISET VOLATILE TABLE cluster_ratios AS (
SELECT 
     channel_num
    ,store_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    ,size_1_id
    ,MAX(cluster_ratio) AS cluster_ratio
    -- NUMERATOR
    ,SUM(CASE WHEN net_sales_u < 0 THEN 0 ELSE net_sales_u END) AS sales_size
    ,SUM(net_sales_r) AS sales_size_r
    ,SUM(CASE WHEN reg_net_sales_u < 0 THEN 0 ELSE reg_net_sales_u END) AS reg_sales_size
    ,SUM(eoh_u) AS eoh_size
    ,SUM(reg_eoh_u) AS reg_eoh_size
    ,SUM(sized_receipt_u + unknown_receipt_u) AS receipts_size
    -- DENOMINATOR
    ,SUM(sales_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS sales_frame
    ,SUM(sales_size_r) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS sales_frame_r
    ,SUM(reg_sales_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS reg_sales_frame
    ,SUM(eoh_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS eoh_frame
    ,SUM(reg_eoh_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS reg_eoh_frame
    ,SUM(receipts_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame) AS receipts_frame
    -- SIZE RATIOS
    ,sales_size / NULLIFZERO(sales_frame * 1.0000) AS sales_ratio
    ,reg_sales_size / NULLIFZERO(reg_sales_frame * 1.0000) AS reg_sales_ratio
    ,eoh_size / NULLIFZERO(eoh_frame * 1.0000) AS eoh_ratio
    ,reg_eoh_size / NULLIFZERO(reg_eoh_frame * 1.0000) AS reg_eoh_ratio
    ,receipts_size / NULLIFZERO(receipts_frame * 1.0000) AS receipts_ratio
FROM {environment_schema}.location_model_actuals_vw --{environment_schema}
GROUP BY 1,2,3,4,5,6,7
) WITH DATA
PRIMARY INDEX(channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_id)
ON COMMIT PRESERVE ROWS;
-- DROP TABLE cluster_wmae;
CREATE MULTISET VOLATILE TABLE cluster_wmae AS (
WITH cluster_absolute_errors AS (
SELECT
     channel_num
    ,store_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    ,size_1_id
    ,cluster_ratio
    -- FRAME
    ,sales_frame
    ,sales_frame_r
    ,reg_sales_frame
    ,eoh_frame
    ,reg_eoh_frame
    ,receipts_frame
    -- RATIOS
    ,sales_ratio
    ,reg_sales_ratio
    ,eoh_ratio
    ,reg_eoh_ratio
    ,receipts_ratio
    -- MODEL WMAE
    -- sales v model
    ,ABS(sales_ratio - cluster_ratio) AS abs_err_sales_cluster
    ,abs_err_sales_cluster * sales_ratio AS w_error_sales_cluster
    -- reg sales v model
    ,ABS(reg_sales_ratio - cluster_ratio) AS abs_err_reg_sales_cluster
    ,abs_err_reg_sales_cluster * reg_sales_ratio AS w_error_reg_sales_cluster
    -- eoh v model
    ,ABS(eoh_ratio - cluster_ratio) AS abs_err_eoh_cluster
    ,abs_err_eoh_cluster * eoh_ratio AS w_error_eoh_cluster
    -- reg eoh v model
    ,ABS(reg_eoh_ratio - cluster_ratio) AS abs_err_reg_eoh_cluster
    ,abs_err_reg_eoh_cluster * reg_eoh_ratio AS w_error_reg_eoh_cluster
    -- receipts v model 
    ,ABS(receipts_ratio - cluster_ratio) AS abs_err_rcpt_cluster
    ,abs_err_rcpt_cluster * receipts_ratio AS w_error_rcpt_cluster
    -- ACTUAL WMAE
    -- sales v receipts 
    ,ABS(sales_ratio - receipts_ratio) AS abs_err_sales_rcpts_cluster
    ,abs_err_sales_rcpts_cluster * sales_ratio AS w_error_sales_rcpts_cluster
    -- sales v eoh
    ,ABS(sales_ratio - eoh_ratio) AS abs_err_sales_eoh_cluster
    ,abs_err_sales_eoh_cluster * sales_ratio AS w_error_sales_eoh_cluster
    -- reg sales v reg eoh
    ,ABS(reg_sales_ratio - reg_eoh_ratio) AS abs_err_reg_sales_eoh_cluster
    ,abs_err_reg_sales_eoh_cluster * reg_sales_ratio AS w_error_reg_sales_eoh_cluster
    -- reg sales v receipts
    ,ABS(reg_sales_ratio - receipts_ratio) AS abs_err_reg_sales_rcpt_cluster
    ,abs_err_reg_sales_rcpt_cluster * reg_sales_ratio AS w_error_reg_sales_rcpts_cluster
FROM cluster_ratios
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)
,cluster_aggregated_errors AS (
SELECT
     channel_num
    ,store_num
    ,dept_idnt
    ,class_frame
    ,supplier_frame
    ,size_1_frame
    -- NUMERATOR
    ,sales_frame
    ,sales_frame_r
    ,reg_sales_frame
    ,eoh_frame
    ,reg_eoh_frame
    ,receipts_frame
    -- DENOMINATOR
    ,SUM(sales_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt) AS sales_dept
    ,SUM(sales_frame_r) OVER (PARTITION BY channel_num, store_num, dept_idnt) AS sales_dept_r
    ,SUM(reg_sales_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt) AS reg_sales_dept
    ,SUM(eoh_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt) AS eoh_dept
    ,SUM(reg_eoh_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt) AS reg_eoh_dept
    ,SUM(receipts_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt) AS receipts_dept
    -- WMAE WEIGHTS
    ,sales_frame / NULLIFZERO(sales_dept * 1.0000) AS sales_weight
    ,reg_sales_frame / NULLIFZERO(reg_sales_dept * 1.0000) AS reg_sales_weight
    ,eoh_frame / NULLIFZERO(eoh_dept * 1.0000) AS eoh_weight
    ,reg_eoh_frame / NULLIFZERO(reg_eoh_dept * 1.0000) AS reg_eoh_weight
    ,receipts_frame / NULLIFZERO(receipts_dept * 1.0000) AS receipts_weight
    -- MODEL WMAE
    ,SUM(w_error_sales_cluster) AS wmae_sales_cluster
    ,SUM(w_error_reg_sales_cluster) AS wmae_reg_sales_cluster
    ,SUM(w_error_eoh_cluster) AS wmae_eoh_cluster
    ,SUM(w_error_reg_eoh_cluster) AS wmae_reg_eoh_cluster
    ,SUM(w_error_rcpt_cluster) AS wmae_receipts_cluster
    -- ACTUAL WMAE
    ,SUM(w_error_sales_rcpts_cluster) AS wmae_sales_rcpts_cluster
    ,SUM(w_error_reg_sales_rcpts_cluster) AS wmae_reg_sales_rcpts_cluster
    ,SUM(w_error_sales_eoh_cluster) AS wmae_sales_eoh_cluster
    ,SUM(w_error_reg_sales_eoh_cluster) AS wmae_reg_sales_eoh_cluster
FROM cluster_absolute_errors
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
SELECT
     channel_num
    ,store_num
    ,dept_idnt
    ,sales_dept
    ,sales_dept_r
    ,eoh_dept
    ,receipts_dept
    ,reg_sales_dept
    ,reg_eoh_dept
    ,SUM(wmae_sales_cluster * sales_weight) AS wmae_sales_loc
    ,SUM(wmae_reg_sales_cluster * reg_sales_weight) AS wmae_reg_sales_loc
    ,SUM(wmae_eoh_cluster * eoh_weight) AS wmae_eoh_loc
    ,SUM(wmae_reg_eoh_cluster * reg_eoh_weight) AS wmae_reg_eoh_loc
    ,SUM(wmae_receipts_cluster * receipts_weight) AS wmae_receipts_loc
    ,SUM(wmae_sales_rcpts_cluster  * sales_weight) AS wmae_sales_rcpts_loc
    ,SUM(wmae_reg_sales_rcpts_cluster  * reg_sales_weight) AS wmae_reg_sales_rcpts_loc
    ,SUM(wmae_sales_eoh_cluster * sales_weight) AS wmae_sales_eoh_loc
    ,SUM(wmae_reg_sales_eoh_cluster * reg_sales_weight) AS wmae_reg_sales_eoh_loc
FROM cluster_aggregated_errors
GROUP BY 1,2,3,4,5,6,7,8,9
) WITH DATA
PRIMARY INDEX(channel_num, store_num, dept_idnt)
ON COMMIT PRESERVE ROWS;

-- DROP TABLE wmae;
CREATE MULTISET VOLATILE TABLE wmae AS (
SELECT c.*
    ,b.sales_dept AS sales_dept_bulk
    ,b.sales_dept_r AS sales_dept_bulk_r
    ,b.eoh_dept AS eoh_dept_bulk
    ,b.receipts_dept AS receipts_dept_bulk
    ,b.reg_sales_dept AS reg_sales_dept_bulk
    ,b.reg_eoh_dept AS reg_eoh_dept_bulk
    ,b.wmae_sales_bulk
    ,b.wmae_reg_sales_bulk
    ,b.wmae_eoh_bulk
    ,b.wmae_reg_eoh_bulk
    ,b.wmae_receipts_bulk
    ,b.wmae_sales_rcpts_bulk
    ,b.wmae_reg_sales_rcpts_bulk
    ,b.wmae_sales_eoh_bulk
    ,b.wmae_reg_sales_eoh_bulk
    ,bl.wmae_baseline_rcpt_bulk
    ,bl.wmae_baseline_sls_bulk
    ,bl.wmae_baseline_rcpt_loc
    ,bl.wmae_baseline_sls_loc
FROM cluster_wmae AS c
JOIN bulk_wmae AS b
  ON b.channel_num = c.channel_num
 AND b.dept_idnt = c.dept_idnt
JOIN baseline_wmae AS bl
  ON bl.channel_num = c.channel_num
 AND bl.dept_idnt = c.dept_idnt
 AND bl.store_num = c.store_num 
) WITH DATA
PRIMARY INDEX(channel_num, store_num, dept_idnt)
ON COMMIT PRESERVE ROWS;

DELETE FROM {environment_schema}.accuracy WHERE eval_month_idnt = (SELECT month_idnt FROM {environment_schema}.size_eval_cal_month_vw WHERE current_month = 1);
INSERT INTO {environment_schema}.accuracy 
SELECT 
     (SELECT month_idnt FROM {environment_schema}.size_eval_cal_month_vw WHERE current_month = 1 GROUP BY 1) AS eval_month_idnt 
    ,st.store_num
    ,st.store_name
    ,st.channel_num
    ,st.banner
    ,st.selling_channel
    ,st.store_address_state
    ,st.store_dma_desc
    ,st.store_location_latitude
    ,st.store_location_longitude
    ,st.region_desc
    ,st.region_short_desc
    ,d.division
    ,d.subdivision
    ,d.department
    ,w.sales_dept
    ,w.sales_dept_r
    ,w.eoh_dept
    ,w.receipts_dept
    ,w.reg_sales_dept
    ,w.reg_eoh_dept
    ,w.sales_dept_bulk
    ,w.sales_dept_bulk_r
    ,w.eoh_dept_bulk
    ,w.receipts_dept_bulk
    ,w.reg_sales_dept_bulk
    ,w.reg_eoh_dept_bulk
    ,w.wmae_baseline_sls_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_baseline_sls_bulk ELSE w.wmae_baseline_sls_loc END AS wmae_baseline_sls_loc
    ,w.wmae_baseline_rcpt_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_baseline_rcpt_bulk ELSE w.wmae_baseline_rcpt_loc END AS wmae_baseline_rcpt_loc
    ,w.wmae_sales_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_sales_bulk ELSE w.wmae_sales_loc END AS wmae_sales_loc
    ,w.wmae_reg_sales_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_reg_sales_bulk ELSE w.wmae_reg_sales_loc END AS wmae_reg_sales_loc
    ,w.wmae_eoh_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_eoh_bulk ELSE w.wmae_eoh_loc END AS wmae_eoh_loc
    ,w.wmae_receipts_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_receipts_bulk ELSE w.wmae_receipts_loc END wmae_receipts_loc
    ,w.wmae_sales_rcpts_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_sales_rcpts_bulk ELSE w.wmae_sales_rcpts_loc END AS wmae_sales_rcpts_loc
    ,w.wmae_reg_sales_rcpts_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_reg_sales_rcpts_bulk ELSE w.wmae_reg_sales_rcpts_loc END AS wmae_reg_sales_rcpts_loc
    ,w.wmae_sales_eoh_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_sales_eoh_bulk ELSE w.wmae_sales_eoh_loc END AS wmae_sales_eoh_loc
    ,w.wmae_reg_sales_eoh_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_reg_sales_eoh_bulk ELSE w.wmae_reg_sales_eoh_loc END AS wmae_reg_sales_eoh_loc
    ,w.wmae_reg_eoh_bulk
    ,CASE WHEN selling_channel = 'ONLINE' THEN w.wmae_reg_eoh_bulk ELSE w.wmae_reg_eoh_loc END AS wmae_reg_eoh_loc
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM wmae w
JOIN {environment_schema}.locations st
  ON w.store_num = st.store_num
JOIN (
       SELECT 
             TRIM(div_idnt) || ': ' || div_desc AS division
            ,TRIM(grp_idnt) || ': ' || grp_desc AS subdivision
            ,TRIM(dept_idnt) || ': ' || dept_desc AS department
            ,d.dept_idnt
        FROM {environment_schema}.supp_size_hierarchy d
        GROUP BY 1,2,3,4
    ) d
  ON d.dept_idnt = w.dept_idnt
;