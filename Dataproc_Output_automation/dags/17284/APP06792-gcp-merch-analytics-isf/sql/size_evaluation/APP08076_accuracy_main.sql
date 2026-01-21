/*
Name: Size Curves Evaluation Accuracy 
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    {`{{params.gcp_project_id}}`.{{params.environment_schema}}} `{{params.gcp_project_id}}`.{{params.environment_schema}}
                env_suffix '' or '_dev' tablesuffix for prod testing

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



CREATE TEMPORARY TABLE IF NOT EXISTS baseline_bulk_ratios
CLUSTER BY channel_num, dept_idnt, class_frame, supplier_frame
AS
SELECT t0.channel_num,
 t0.dept_idnt,
 t0.class_frame,
 t0.supplier_frame,
 t0.size_1_frame,
 t0.size_1_id,
 t0.ly_size_sales_u,
 SUM(t0.ly_size_sales_u) OVER (PARTITION BY t0.channel_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ly_sales_frame_u,
 COALESCE(t0.ly_size_sales_u / NULLIF(CAST(SUM(t0.ly_size_sales_u) OVER (PARTITION BY t0.channel_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    , 0), 0) AS baseline_bulk_ratio,
 t0.ty_size_sales_u,
 SUM(t0.ty_size_sales_u) OVER (PARTITION BY t0.channel_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ty_sales_frame_u,
 COALESCE(t0.ty_size_sales_u / NULLIF(CAST(SUM(t0.ty_size_sales_u) OVER (PARTITION BY t0.channel_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    , 0), 0) AS sales_baseline_bulk_ratio,
 t0.ty_size_rcpt_u,
 SUM(t0.ty_size_rcpt_u) OVER (PARTITION BY t0.channel_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ty_rcpt_frame_u,
 COALESCE(t0.ty_size_rcpt_u / NULLIF(CAST(SUM(t0.ty_size_rcpt_u) OVER (PARTITION BY t0.channel_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    , 0), 0) AS rcpt_baseline_bulk_ratio
FROM (SELECT a.channel_num,
   a.dept_idnt,
   a.class_frame,
   a.supplier_frame,
   a.size_1_frame,
   a.size_1_id,
   a.reg_net_sales_u,
   a.ly_ty,
   a.receipts_u,
   SUM(CASE
     WHEN LOWER(a.ly_ty) = LOWER('LY')
     THEN CASE
      WHEN a.reg_net_sales_u < 0
      THEN 0
      ELSE a.reg_net_sales_u
      END
     ELSE 0
     END) AS ly_size_sales_u,
   SUM(CASE
     WHEN LOWER(a.ly_ty) = LOWER('TY')
     THEN CASE
      WHEN a.reg_net_sales_u < 0
      THEN 0
      ELSE a.reg_net_sales_u
      END
     ELSE 0
     END) AS ty_size_sales_u,
   SUM(CASE
     WHEN LOWER(a.ly_ty) = LOWER('TY')
     THEN CASE
      WHEN a.receipts_u < 0
      THEN 0
      ELSE a.receipts_u
      END
     ELSE 0
     END) AS ty_size_rcpt_u
  FROM `{{params.gcp_project_id}}`.t2dl_das_size.size_actuals AS a
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_size.elegible_classframe_vw AS e ON a.store_num = e.store_num AND a.dept_idnt = e.dept_idnt AND
      LOWER(a.class_frame) = LOWER(e.class_frame) AND LOWER(a.supplier_frame) = LOWER(e.supplier_frame)
  GROUP BY a.channel_num,
   a.dept_idnt,
   a.class_frame,
   a.supplier_frame,
   a.size_1_frame,
   a.size_1_id,
   a.reg_net_sales_u,
   a.ly_ty,
   a.receipts_u) AS t0;


CREATE TEMPORARY TABLE IF NOT EXISTS baseline_bulk_wmae AS 
WITH baseline_bulk_absolute_errors AS (SELECT channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   baseline_bulk_ratio,
   ty_sales_frame_u,
   ty_rcpt_frame_u,
   sales_baseline_bulk_ratio,
   rcpt_baseline_bulk_ratio,
   ABS(sales_baseline_bulk_ratio - baseline_bulk_ratio) AS abs_err_sales_baseline_bulk,
    ABS(sales_baseline_bulk_ratio - baseline_bulk_ratio) * sales_baseline_bulk_ratio AS w_error_sales_baseline_bulk,
   ABS(rcpt_baseline_bulk_ratio - baseline_bulk_ratio) AS abs_err_rcpt_baseline_bulk,
    ABS(rcpt_baseline_bulk_ratio - baseline_bulk_ratio) * rcpt_baseline_bulk_ratio AS w_error_rcpt_baseline_bulk
  FROM baseline_bulk_ratios
  GROUP BY channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   baseline_bulk_ratio,
   ty_sales_frame_u,
   sales_baseline_bulk_ratio,
   ty_rcpt_frame_u,
   rcpt_baseline_bulk_ratio), baseline_bulk_aggregated_errors AS (SELECT channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   ty_sales_frame_u,
   ty_rcpt_frame_u,
   SUM(ty_sales_frame_u) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS sales_dept,
   SUM(ty_rcpt_frame_u) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS receipts_dept,
    ty_sales_frame_u / IF(CAST(SUM(ty_sales_frame_u) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(ty_sales_frame_u) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS sales_weight,
    ty_rcpt_frame_u / IF(CAST(SUM(ty_rcpt_frame_u) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(ty_rcpt_frame_u) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS receipts_weight,
   SUM(w_error_sales_baseline_bulk) AS wmae_sales_baseline_bulk,
   SUM(w_error_rcpt_baseline_bulk) AS wmae_receipts_baseline_bulk
  FROM baseline_bulk_absolute_errors
  GROUP BY channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   ty_sales_frame_u,
   ty_rcpt_frame_u,
   w_error_sales_baseline_bulk,
   w_error_rcpt_baseline_bulk) (SELECT channel_num,
   dept_idnt,
   sales_dept,
   receipts_dept,
   SUM(wmae_sales_baseline_bulk * sales_weight) AS wmae_baseline_sls_bulk,
   SUM(wmae_receipts_baseline_bulk * receipts_weight) AS wmae_baseline_rcpt_bulk
  FROM baseline_bulk_aggregated_errors
  GROUP BY channel_num,
   dept_idnt,
   sales_dept,
   receipts_dept);


CREATE TEMPORARY TABLE IF NOT EXISTS baseline_loc_ratios
CLUSTER BY channel_num, store_num, dept_idnt, class_frame
AS
SELECT t0.channel_num,
 t0.store_num,
 t0.dept_idnt,
 t0.class_frame,
 t0.supplier_frame,
 t0.size_1_frame,
 t0.size_1_id,
 t0.ly_size_sales_u,
 SUM(t0.ly_size_sales_u) OVER (PARTITION BY t0.channel_num, t0.store_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame
    , t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ly_sales_frame_u,
 COALESCE(t0.ly_size_sales_u / NULLIF(CAST(SUM(t0.ly_size_sales_u) OVER (PARTITION BY t0.channel_num, t0.store_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    , 0), 0) AS baseline_loc_ratio,
 t0.ty_size_sales_u,
 SUM(t0.ty_size_sales_u) OVER (PARTITION BY t0.channel_num, t0.store_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame
    , t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ty_sales_frame_u,
 COALESCE(t0.ty_size_sales_u / NULLIF(CAST(SUM(t0.ty_size_sales_u) OVER (PARTITION BY t0.channel_num, t0.store_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    , 0), 0) AS sales_baseline_loc_ratio,
 t0.ty_size_rcpt_u,
 SUM(t0.ty_size_rcpt_u) OVER (PARTITION BY t0.channel_num, t0.store_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame
    , t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ty_rcpt_frame_u,
 COALESCE(t0.ty_size_rcpt_u / NULLIF(CAST(SUM(t0.ty_size_rcpt_u) OVER (PARTITION BY t0.channel_num, t0.store_num, t0.dept_idnt, t0.class_frame, t0.supplier_frame, t0.size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    , 0), 0) AS rcpt_baseline_loc_ratio
FROM (SELECT a.channel_num,
   a.store_num,
   a.dept_idnt,
   a.class_frame,
   a.supplier_frame,
   a.size_1_frame,
   a.size_1_id,
   a.reg_net_sales_u,
   a.ly_ty,
   a.receipts_u,
   SUM(CASE
     WHEN LOWER(a.ly_ty) = LOWER('LY')
     THEN CASE
      WHEN a.reg_net_sales_u < 0
      THEN 0
      ELSE a.reg_net_sales_u
      END
     ELSE 0
     END) AS ly_size_sales_u,
   SUM(CASE
     WHEN LOWER(a.ly_ty) = LOWER('TY')
     THEN CASE
      WHEN a.reg_net_sales_u < 0
      THEN 0
      ELSE a.reg_net_sales_u
      END
     ELSE 0
     END) AS ty_size_sales_u,
   SUM(CASE
     WHEN LOWER(a.ly_ty) = LOWER('TY')
     THEN CASE
      WHEN a.receipts_u < 0
      THEN 0
      ELSE a.receipts_u
      END
     ELSE 0
     END) AS ty_size_rcpt_u
  FROM `{{params.gcp_project_id}}`.t2dl_das_size.size_actuals AS a
   INNER JOIN `{{params.gcp_project_id}}`.t2dl_das_size.elegible_classframe_vw AS e ON a.store_num = e.store_num AND a.dept_idnt = e.dept_idnt AND
      LOWER(a.class_frame) = LOWER(e.class_frame) AND LOWER(a.supplier_frame) = LOWER(e.supplier_frame)
  GROUP BY a.channel_num,
   a.store_num,
   a.dept_idnt,
   a.class_frame,
   a.supplier_frame,
   a.size_1_frame,
   a.size_1_id,
   a.reg_net_sales_u,
   a.ly_ty,
   a.receipts_u) AS t0;


CREATE TEMPORARY TABLE IF NOT EXISTS baseline_loc_wmae AS WITH baseline_loc_absolute_errors AS (SELECT channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   baseline_loc_ratio,
   ty_sales_frame_u,
   ty_rcpt_frame_u,
   sales_baseline_loc_ratio,
   rcpt_baseline_loc_ratio,
   ABS(sales_baseline_loc_ratio - baseline_loc_ratio) AS abs_err_sales_baseline_loc,
    ABS(sales_baseline_loc_ratio - baseline_loc_ratio) * sales_baseline_loc_ratio AS w_error_sales_baseline_loc,
   ABS(rcpt_baseline_loc_ratio - baseline_loc_ratio) AS abs_err_rcpt_baseline_loc,
    ABS(rcpt_baseline_loc_ratio - baseline_loc_ratio) * rcpt_baseline_loc_ratio AS w_error_rcpt_baseline_loc
  FROM baseline_loc_ratios
  GROUP BY channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   baseline_loc_ratio,
   ty_sales_frame_u,
   sales_baseline_loc_ratio,
   ty_rcpt_frame_u,
   rcpt_baseline_loc_ratio), baseline_loc_aggregated_errors AS (SELECT channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   ty_sales_frame_u,
   ty_rcpt_frame_u,
   SUM(ty_sales_frame_u) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS sales_dept,
   SUM(ty_rcpt_frame_u) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS receipts_dept,
    ty_sales_frame_u / IF(CAST(SUM(ty_sales_frame_u) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(ty_sales_frame_u) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS sales_weight,
    ty_rcpt_frame_u / IF(CAST(SUM(ty_rcpt_frame_u) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(ty_rcpt_frame_u) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS receipts_weight,
   SUM(w_error_sales_baseline_loc) AS wmae_sales_baseline_loc,
   SUM(w_error_rcpt_baseline_loc) AS wmae_receipts_baseline_loc
  FROM baseline_loc_absolute_errors
  GROUP BY channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   ty_sales_frame_u,
   ty_rcpt_frame_u,
   w_error_sales_baseline_loc,
   w_error_rcpt_baseline_loc) (SELECT channel_num,
   store_num,
   dept_idnt,
   sales_dept,
   receipts_dept,
   SUM(wmae_sales_baseline_loc * sales_weight) AS wmae_baseline_sls_loc,
   SUM(wmae_receipts_baseline_loc * receipts_weight) AS wmae_baseline_rcpt_loc
  FROM baseline_loc_aggregated_errors
  GROUP BY channel_num,
   store_num,
   dept_idnt,
   sales_dept,
   receipts_dept);


CREATE TEMPORARY TABLE IF NOT EXISTS baseline_wmae
CLUSTER BY channel_num, store_num, dept_idnt
AS
SELECT l.channel_num,
 l.store_num,
 l.dept_idnt,
 l.sales_dept,
 l.receipts_dept,
 l.wmae_baseline_sls_loc,
 l.wmae_baseline_rcpt_loc,
 b.wmae_baseline_sls_bulk,
 b.wmae_baseline_rcpt_bulk
FROM baseline_loc_wmae AS l
 INNER JOIN baseline_bulk_wmae AS b ON l.channel_num = b.channel_num AND l.dept_idnt = b.dept_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS bulk_ratios
CLUSTER BY channel_num, dept_idnt, class_frame, supplier_frame
AS
SELECT channel_num,
 dept_idnt,
 class_frame,
 supplier_frame,
 size_1_frame,
 size_1_id,
 bulk_ratio,
 sales_size,
 sales_size_r,
 reg_sales_size,
 eoh_size,
 reg_eoh_size,
 receipts_size,
 SUM(sales_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_frame,
 SUM(sales_size_r) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_frame_r,
 SUM(reg_sales_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS reg_sales_frame,
 SUM(eoh_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS eoh_frame,
 SUM(reg_eoh_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS reg_eoh_frame,
 SUM(receipts_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN
  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS receipts_frame,
  sales_size / IF(CAST(SUM(sales_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(sales_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS sales_ratio,
  reg_sales_size / IF(CAST(SUM(reg_sales_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(reg_sales_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS reg_sales_ratio,
  eoh_size / IF(CAST(SUM(eoh_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(eoh_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS eoh_ratio,
  reg_eoh_size / IF(CAST(SUM(reg_eoh_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(reg_eoh_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS reg_eoh_ratio,
  receipts_size / IF(CAST(SUM(receipts_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(receipts_size) OVER (PARTITION BY channel_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS receipts_ratio
FROM (SELECT channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   net_sales_u,
   net_sales_r,
   reg_net_sales_u,
   eoh_u,
   reg_eoh_u,
   sized_receipt_u,
   unknown_receipt_u,
   MAX(bulk_ratio) AS bulk_ratio,
   SUM(CASE
     WHEN net_sales_u < 0
     THEN 0
     ELSE net_sales_u
     END) AS sales_size,
   SUM(net_sales_r) AS sales_size_r,
   SUM(CASE
     WHEN reg_net_sales_u < 0
     THEN 0
     ELSE reg_net_sales_u
     END) AS reg_sales_size,
   SUM(eoh_u) AS eoh_size,
   SUM(reg_eoh_u) AS reg_eoh_size,
   SUM(sized_receipt_u + unknown_receipt_u) AS receipts_size
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.location_model_actuals_vw
  GROUP BY channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   net_sales_u,
   net_sales_r,
   reg_net_sales_u,
   eoh_u,
   reg_eoh_u,
   sized_receipt_u,
   unknown_receipt_u) AS t0;


CREATE TEMPORARY TABLE IF NOT EXISTS bulk_wmae AS WITH bulk_absolute_errors AS (SELECT channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   bulk_ratio,
   sales_frame,
   sales_frame_r,
   reg_sales_frame,
   eoh_frame,
   reg_eoh_frame,
   receipts_frame,
   sales_ratio,
   reg_sales_ratio,
   eoh_ratio,
   reg_eoh_ratio,
   receipts_ratio,
   ABS(sales_ratio - bulk_ratio) AS abs_err_sales_bulk,
    ABS(sales_ratio - bulk_ratio) * sales_ratio AS w_error_sales_bulk,
   ABS(reg_sales_ratio - bulk_ratio) AS abs_err_reg_sales_bulk,
    ABS(reg_sales_ratio - bulk_ratio) * reg_sales_ratio AS w_error_reg_sales_bulk,
   ABS(eoh_ratio - bulk_ratio) AS abs_err_eoh_bulk,
    ABS(eoh_ratio - bulk_ratio) * eoh_ratio AS w_error_eoh_bulk,
   ABS(reg_eoh_ratio - bulk_ratio) AS abs_err_reg_eoh_bulk,
    ABS(reg_eoh_ratio - bulk_ratio) * reg_eoh_ratio AS w_error_reg_eoh_bulk,
   ABS(receipts_ratio - bulk_ratio) AS abs_err_rcpt_bulk,
    ABS(receipts_ratio - bulk_ratio) * receipts_ratio AS w_error_rcpt_bulk,
   ABS(sales_ratio - receipts_ratio) AS abs_err_sales_rcpts_bulk,
    ABS(sales_ratio - receipts_ratio) * sales_ratio AS w_error_sales_rcpts_bulk,
   ABS(sales_ratio - eoh_ratio) AS abs_err_sales_eoh_bulk,
    ABS(sales_ratio - eoh_ratio) * sales_ratio AS w_error_sales_eoh_bulk,
   ABS(reg_sales_ratio - reg_eoh_ratio) AS abs_err_reg_sales_eoh_bulk,
    ABS(reg_sales_ratio - reg_eoh_ratio) * reg_sales_ratio AS w_error_reg_sales_eoh_bulk,
   ABS(reg_sales_ratio - receipts_ratio) AS abs_err_reg_sales_rcpt_bulk,
    ABS(reg_sales_ratio - receipts_ratio) * reg_sales_ratio AS w_error_reg_sales_rcpts_bulk
  FROM bulk_ratios
  GROUP BY channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   bulk_ratio,
   sales_frame,
   sales_frame_r,
   reg_sales_frame,
   eoh_frame,
   reg_eoh_frame,
   receipts_frame,
   sales_ratio,
   reg_sales_ratio,
   eoh_ratio,
   reg_eoh_ratio,
   receipts_ratio), bulk_aggregated_errors AS (SELECT channel_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   sales_frame,
   sales_frame_r,
   reg_sales_frame,
   eoh_frame,
   reg_eoh_frame,
   receipts_frame,
   SUM(sales_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS sales_dept,
   SUM(sales_frame_r) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS sales_dept_r,
   SUM(reg_sales_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS reg_sales_dept,
   SUM(eoh_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
   AS eoh_dept,
   SUM(reg_eoh_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS reg_eoh_dept,
   SUM(receipts_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS receipts_dept,
    sales_frame / IF(CAST(SUM(sales_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(sales_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS sales_weight,
    reg_sales_frame / IF(CAST(SUM(reg_sales_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(reg_sales_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS reg_sales_weight,
    eoh_frame / IF(CAST(SUM(eoh_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(eoh_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS eoh_weight,
    reg_eoh_frame / IF(CAST(SUM(reg_eoh_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(reg_eoh_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS reg_eoh_weight,
    receipts_frame / IF(CAST(SUM(receipts_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(receipts_frame) OVER (PARTITION BY channel_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS receipts_weight,
   wmae_sales_bulk,
   wmae_reg_sales_bulk,
   wmae_eoh_bulk,
   wmae_reg_eoh_bulk,
   wmae_receipts_bulk,
   wmae_sales_rcpts_bulk,
   wmae_reg_sales_rcpts_bulk,
   wmae_sales_eoh_bulk,
   wmae_reg_sales_eoh_bulk
  FROM (SELECT channel_num,
     dept_idnt,
     class_frame,
     supplier_frame,
     size_1_frame,
     sales_frame,
     sales_frame_r,
     reg_sales_frame,
     eoh_frame,
     reg_eoh_frame,
     receipts_frame,
     w_error_sales_bulk,
     w_error_reg_sales_bulk,
     w_error_eoh_bulk,
     w_error_reg_eoh_bulk,
     w_error_rcpt_bulk,
     w_error_sales_rcpts_bulk,
     w_error_reg_sales_rcpts_bulk,
     w_error_sales_eoh_bulk,
     w_error_reg_sales_eoh_bulk,
     SUM(w_error_sales_bulk) AS wmae_sales_bulk,
     SUM(w_error_reg_sales_bulk) AS wmae_reg_sales_bulk,
     SUM(w_error_eoh_bulk) AS wmae_eoh_bulk,
     SUM(w_error_reg_eoh_bulk) AS wmae_reg_eoh_bulk,
     SUM(w_error_rcpt_bulk) AS wmae_receipts_bulk,
     SUM(w_error_sales_rcpts_bulk) AS wmae_sales_rcpts_bulk,
     SUM(w_error_reg_sales_rcpts_bulk) AS wmae_reg_sales_rcpts_bulk,
     SUM(w_error_sales_eoh_bulk) AS wmae_sales_eoh_bulk,
     SUM(w_error_reg_sales_eoh_bulk) AS wmae_reg_sales_eoh_bulk
    FROM bulk_absolute_errors
    GROUP BY channel_num,
     dept_idnt,
     class_frame,
     supplier_frame,
     size_1_frame,
     sales_frame,
     sales_frame_r,
     reg_sales_frame,
     eoh_frame,
     reg_eoh_frame,
     receipts_frame,
     w_error_sales_bulk,
     w_error_reg_sales_bulk,
     w_error_eoh_bulk,
     w_error_reg_eoh_bulk,
     w_error_rcpt_bulk,
     w_error_sales_rcpts_bulk,
     w_error_sales_eoh_bulk,
     w_error_reg_sales_eoh_bulk,
     w_error_reg_sales_rcpts_bulk) AS t3) (SELECT channel_num,
   dept_idnt,
   sales_dept,
   sales_dept_r,
   eoh_dept,
   receipts_dept,
   reg_sales_dept,
   reg_eoh_dept,
   SUM(wmae_sales_bulk * sales_weight) AS wmae_sales_bulk,
   SUM(wmae_reg_sales_bulk * reg_sales_weight) AS wmae_reg_sales_bulk,
   SUM(wmae_eoh_bulk * eoh_weight) AS wmae_eoh_bulk,
   SUM(wmae_reg_eoh_bulk * reg_eoh_weight) AS wmae_reg_eoh_bulk,
   SUM(wmae_receipts_bulk * receipts_weight) AS wmae_receipts_bulk,
   SUM(wmae_sales_rcpts_bulk * sales_weight) AS wmae_sales_rcpts_bulk,
   SUM(wmae_reg_sales_rcpts_bulk * reg_sales_weight) AS wmae_reg_sales_rcpts_bulk,
   SUM(wmae_sales_eoh_bulk * sales_weight) AS wmae_sales_eoh_bulk,
   SUM(wmae_reg_sales_eoh_bulk * reg_sales_weight) AS wmae_reg_sales_eoh_bulk
  FROM bulk_aggregated_errors
  GROUP BY channel_num,
   dept_idnt,
   sales_dept,
   sales_dept_r,
   eoh_dept,
   receipts_dept,
   reg_sales_dept,
   reg_eoh_dept);


CREATE TEMPORARY TABLE IF NOT EXISTS cluster_ratios
CLUSTER BY channel_num, store_num, dept_idnt, class_frame
AS
SELECT channel_num,
 store_num,
 dept_idnt,
 class_frame,
 supplier_frame,
 size_1_frame,
 size_1_id,
 cluster_ratio,
 sales_size,
 sales_size_r,
 reg_sales_size,
 eoh_size,
 reg_eoh_size,
 receipts_size,
 SUM(sales_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_frame,
 SUM(sales_size_r) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_frame_r,
 SUM(reg_sales_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS reg_sales_frame,
 SUM(eoh_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS eoh_frame,
 SUM(reg_eoh_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS reg_eoh_frame,
 SUM(receipts_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS receipts_frame,
  sales_size / IF(CAST(SUM(sales_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(sales_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS sales_ratio,
  reg_sales_size / IF(CAST(SUM(reg_sales_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(reg_sales_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS reg_sales_ratio,
  eoh_size / IF(CAST(SUM(eoh_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(eoh_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS eoh_ratio,
  reg_eoh_size / IF(CAST(SUM(reg_eoh_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(reg_eoh_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS reg_eoh_ratio,
  receipts_size / IF(CAST(SUM(receipts_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
    = 0, NULL, CAST(SUM(receipts_size) OVER (PARTITION BY channel_num, store_num, dept_idnt, class_frame, supplier_frame, size_1_frame RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
   ) AS receipts_ratio
FROM (SELECT channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   net_sales_u,
   net_sales_r,
   reg_net_sales_u,
   eoh_u,
   reg_eoh_u,
   unknown_receipt_u,
   sized_receipt_u,
   MAX(cluster_ratio) AS cluster_ratio,
   SUM(CASE
     WHEN net_sales_u < 0
     THEN 0
     ELSE net_sales_u
     END) AS sales_size,
   SUM(net_sales_r) AS sales_size_r,
   SUM(CASE
     WHEN reg_net_sales_u < 0
     THEN 0
     ELSE reg_net_sales_u
     END) AS reg_sales_size,
   SUM(eoh_u) AS eoh_size,
   SUM(reg_eoh_u) AS reg_eoh_size,
   SUM(sized_receipt_u + unknown_receipt_u) AS receipts_size
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.location_model_actuals_vw
  GROUP BY channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   net_sales_u,
   net_sales_r,
   reg_net_sales_u,
   eoh_u,
   reg_eoh_u,
   unknown_receipt_u,
   sized_receipt_u) AS t0;


CREATE TEMPORARY TABLE IF NOT EXISTS cluster_wmae AS WITH cluster_absolute_errors AS (SELECT channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   cluster_ratio,
   sales_frame,
   sales_frame_r,
   reg_sales_frame,
   eoh_frame,
   reg_eoh_frame,
   receipts_frame,
   sales_ratio,
   reg_sales_ratio,
   eoh_ratio,
   reg_eoh_ratio,
   receipts_ratio,
   ABS(sales_ratio - cluster_ratio) AS abs_err_sales_cluster,
    ABS(sales_ratio - cluster_ratio) * sales_ratio AS w_error_sales_cluster,
   ABS(reg_sales_ratio - cluster_ratio) AS abs_err_reg_sales_cluster,
    ABS(reg_sales_ratio - cluster_ratio) * reg_sales_ratio AS w_error_reg_sales_cluster,
   ABS(eoh_ratio - cluster_ratio) AS abs_err_eoh_cluster,
    ABS(eoh_ratio - cluster_ratio) * eoh_ratio AS w_error_eoh_cluster,
   ABS(reg_eoh_ratio - cluster_ratio) AS abs_err_reg_eoh_cluster,
    ABS(reg_eoh_ratio - cluster_ratio) * reg_eoh_ratio AS w_error_reg_eoh_cluster,
   ABS(receipts_ratio - cluster_ratio) AS abs_err_rcpt_cluster,
    ABS(receipts_ratio - cluster_ratio) * receipts_ratio AS w_error_rcpt_cluster,
   ABS(sales_ratio - receipts_ratio) AS abs_err_sales_rcpts_cluster,
    ABS(sales_ratio - receipts_ratio) * sales_ratio AS w_error_sales_rcpts_cluster,
   ABS(sales_ratio - eoh_ratio) AS abs_err_sales_eoh_cluster,
    ABS(sales_ratio - eoh_ratio) * sales_ratio AS w_error_sales_eoh_cluster,
   ABS(reg_sales_ratio - reg_eoh_ratio) AS abs_err_reg_sales_eoh_cluster,
    ABS(reg_sales_ratio - reg_eoh_ratio) * reg_sales_ratio AS w_error_reg_sales_eoh_cluster,
   ABS(reg_sales_ratio - receipts_ratio) AS abs_err_reg_sales_rcpt_cluster,
    ABS(reg_sales_ratio - receipts_ratio) * reg_sales_ratio AS w_error_reg_sales_rcpts_cluster
  FROM cluster_ratios
  GROUP BY channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   size_1_id,
   cluster_ratio,
   sales_frame,
   sales_frame_r,
   reg_sales_frame,
   eoh_frame,
   reg_eoh_frame,
   receipts_frame,
   sales_ratio,
   reg_sales_ratio,
   eoh_ratio,
   reg_eoh_ratio,
   receipts_ratio), cluster_aggregated_errors AS (SELECT channel_num,
   store_num,
   dept_idnt,
   class_frame,
   supplier_frame,
   size_1_frame,
   sales_frame,
   sales_frame_r,
   reg_sales_frame,
   eoh_frame,
   reg_eoh_frame,
   receipts_frame,
   SUM(sales_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS sales_dept,
   SUM(sales_frame_r) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS sales_dept_r,
   SUM(reg_sales_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS reg_sales_dept,
   SUM(eoh_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS eoh_dept,
   SUM(reg_eoh_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS reg_eoh_dept,
   SUM(receipts_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS receipts_dept,
    sales_frame / IF(CAST(SUM(sales_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(sales_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS sales_weight,
    reg_sales_frame / IF(CAST(SUM(reg_sales_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(reg_sales_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS reg_sales_weight,
    eoh_frame / IF(CAST(SUM(eoh_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(eoh_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS eoh_weight,
    reg_eoh_frame / IF(CAST(SUM(reg_eoh_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(reg_eoh_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS reg_eoh_weight,
    receipts_frame / IF(CAST(SUM(receipts_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
      = 0, NULL, CAST(SUM(receipts_frame) OVER (PARTITION BY channel_num, store_num, dept_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS NUMERIC)
     ) AS receipts_weight,
   wmae_sales_cluster,
   wmae_reg_sales_cluster,
   wmae_eoh_cluster,
   wmae_reg_eoh_cluster,
   wmae_receipts_cluster,
   wmae_sales_rcpts_cluster,
   wmae_reg_sales_rcpts_cluster,
   wmae_sales_eoh_cluster,
   wmae_reg_sales_eoh_cluster
  FROM (SELECT channel_num,
     store_num,
     dept_idnt,
     class_frame,
     supplier_frame,
     size_1_frame,
     sales_frame,
     sales_frame_r,
     reg_sales_frame,
     eoh_frame,
     reg_eoh_frame,
     receipts_frame,
     w_error_sales_cluster,
     w_error_reg_sales_cluster,
     w_error_eoh_cluster,
     w_error_reg_eoh_cluster,
     w_error_rcpt_cluster,
     w_error_sales_rcpts_cluster,
     w_error_reg_sales_rcpts_cluster,
     w_error_sales_eoh_cluster,
     w_error_reg_sales_eoh_cluster,
     SUM(w_error_sales_cluster) AS wmae_sales_cluster,
     SUM(w_error_reg_sales_cluster) AS wmae_reg_sales_cluster,
     SUM(w_error_eoh_cluster) AS wmae_eoh_cluster,
     SUM(w_error_reg_eoh_cluster) AS wmae_reg_eoh_cluster,
     SUM(w_error_rcpt_cluster) AS wmae_receipts_cluster,
     SUM(w_error_sales_rcpts_cluster) AS wmae_sales_rcpts_cluster,
     SUM(w_error_reg_sales_rcpts_cluster) AS wmae_reg_sales_rcpts_cluster,
     SUM(w_error_sales_eoh_cluster) AS wmae_sales_eoh_cluster,
     SUM(w_error_reg_sales_eoh_cluster) AS wmae_reg_sales_eoh_cluster
    FROM cluster_absolute_errors
    GROUP BY channel_num,
     store_num,
     dept_idnt,
     class_frame,
     supplier_frame,
     size_1_frame,
     sales_frame,
     sales_frame_r,
     reg_sales_frame,
     eoh_frame,
     reg_eoh_frame,
     receipts_frame,
     w_error_sales_cluster,
     w_error_reg_sales_cluster,
     w_error_eoh_cluster,
     w_error_reg_eoh_cluster,
     w_error_rcpt_cluster,
     w_error_sales_rcpts_cluster,
     w_error_sales_eoh_cluster,
     w_error_reg_sales_eoh_cluster,
     w_error_reg_sales_rcpts_cluster) AS t3) (SELECT channel_num,
   store_num,
   dept_idnt,
   sales_dept,
   sales_dept_r,
   eoh_dept,
   receipts_dept,
   reg_sales_dept,
   reg_eoh_dept,
   SUM(wmae_sales_cluster * sales_weight) AS wmae_sales_loc,
   SUM(wmae_reg_sales_cluster * reg_sales_weight) AS wmae_reg_sales_loc,
   SUM(wmae_eoh_cluster * eoh_weight) AS wmae_eoh_loc,
   SUM(wmae_reg_eoh_cluster * reg_eoh_weight) AS wmae_reg_eoh_loc,
   SUM(wmae_receipts_cluster * receipts_weight) AS wmae_receipts_loc,
   SUM(wmae_sales_rcpts_cluster * sales_weight) AS wmae_sales_rcpts_loc,
   SUM(wmae_reg_sales_rcpts_cluster * reg_sales_weight) AS wmae_reg_sales_rcpts_loc,
   SUM(wmae_sales_eoh_cluster * sales_weight) AS wmae_sales_eoh_loc,
   SUM(wmae_reg_sales_eoh_cluster * reg_sales_weight) AS wmae_reg_sales_eoh_loc
  FROM cluster_aggregated_errors
  GROUP BY channel_num,
   store_num,
   dept_idnt,
   sales_dept,
   sales_dept_r,
   eoh_dept,
   receipts_dept,
   reg_sales_dept,
   reg_eoh_dept);


CREATE TEMPORARY TABLE IF NOT EXISTS wmae
CLUSTER BY channel_num, store_num, dept_idnt
AS
SELECT c.channel_num,
 c.store_num,
 c.dept_idnt,
 c.sales_dept,
 c.sales_dept_r,
 c.eoh_dept,
 c.receipts_dept,
 c.reg_sales_dept,
 c.reg_eoh_dept,
 c.wmae_sales_loc,
 c.wmae_reg_sales_loc,
 c.wmae_eoh_loc,
 c.wmae_reg_eoh_loc,
 c.wmae_receipts_loc,
 c.wmae_sales_rcpts_loc,
 c.wmae_reg_sales_rcpts_loc,
 c.wmae_sales_eoh_loc,
 c.wmae_reg_sales_eoh_loc,
 b.sales_dept AS sales_dept_bulk,
 b.sales_dept_r AS sales_dept_bulk_r,
 b.eoh_dept AS eoh_dept_bulk,
 b.receipts_dept AS receipts_dept_bulk,
 b.reg_sales_dept AS reg_sales_dept_bulk,
 b.reg_eoh_dept AS reg_eoh_dept_bulk,
 b.wmae_sales_bulk,
 b.wmae_reg_sales_bulk,
 b.wmae_eoh_bulk,
 b.wmae_reg_eoh_bulk,
 b.wmae_receipts_bulk,
 b.wmae_sales_rcpts_bulk,
 b.wmae_reg_sales_rcpts_bulk,
 b.wmae_sales_eoh_bulk,
 b.wmae_reg_sales_eoh_bulk,
 bl.wmae_baseline_rcpt_bulk,
 bl.wmae_baseline_sls_bulk,
 bl.wmae_baseline_rcpt_loc,
 bl.wmae_baseline_sls_loc
FROM cluster_wmae AS c
 INNER JOIN bulk_wmae AS b ON c.channel_num = b.channel_num AND c.dept_idnt = b.dept_idnt
 INNER JOIN baseline_wmae AS bl ON c.channel_num = bl.channel_num AND c.dept_idnt = bl.dept_idnt AND c.store_num = bl.store_num;


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.accuracy
WHERE eval_month_idnt = (SELECT month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_month_vw
  WHERE current_month = 1);


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.accuracy
SELECT DISTINCT (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_month_vw
   WHERE current_month = 1
   GROUP BY month_idnt) AS eval_month_idnt,
  st.store_num,
  st.store_name,
  st.channel_num,
  st.banner,
  st.selling_channel,
  st.store_address_state,
  st.store_dma_desc,
  st.store_location_latitude,
  st.store_location_longitude,
  st.region_desc,
  st.region_short_desc,
  d.division,
  d.subdivision,
  d.department,
  w.sales_dept,
  CAST(TRUNC(w.sales_dept_r) AS INTEGER) AS sales_dept_r,
  w.eoh_dept,
  w.receipts_dept,
  w.reg_sales_dept,
  w.reg_eoh_dept,
  w.sales_dept_bulk,
  CAST(TRUNC(w.sales_dept_bulk_r) AS INTEGER) AS sales_dept_bulk_r,
  w.eoh_dept_bulk,
  w.receipts_dept_bulk,
  w.reg_sales_dept_bulk,
  w.reg_eoh_dept_bulk,
  CAST(w.wmae_baseline_sls_bulk AS FLOAT64) AS wmae_baseline_sls_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_baseline_sls_bulk
    ELSE w.wmae_baseline_sls_loc
    END AS FLOAT64) AS wmae_baseline_sls_loc,
  CAST(w.wmae_baseline_rcpt_bulk AS FLOAT64) AS wmae_baseline_rcpt_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_baseline_rcpt_bulk
    ELSE w.wmae_baseline_rcpt_loc
    END AS FLOAT64) AS wmae_baseline_rcpt_loc,
  CAST(w.wmae_sales_bulk AS FLOAT64) AS wmae_sales_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_sales_bulk
    ELSE w.wmae_sales_loc
    END AS FLOAT64) AS wmae_sales_loc,
  CAST(w.wmae_reg_sales_bulk AS FLOAT64) AS wmae_reg_sales_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_reg_sales_bulk
    ELSE w.wmae_reg_sales_loc
    END AS FLOAT64) AS wmae_reg_sales_loc,
  CAST(w.wmae_eoh_bulk AS FLOAT64) AS wmae_eoh_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_eoh_bulk
    ELSE w.wmae_eoh_loc
    END AS FLOAT64) AS wmae_eoh_loc,
  CAST(w.wmae_receipts_bulk AS FLOAT64) AS wmae_receipts_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_receipts_bulk
    ELSE w.wmae_receipts_loc
    END AS FLOAT64) AS wmae_receipts_loc,
  CAST(w.wmae_sales_rcpts_bulk AS FLOAT64) AS wmae_sales_rcpts_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_sales_rcpts_bulk
    ELSE w.wmae_sales_rcpts_loc
    END AS FLOAT64) AS wmae_sales_rcpts_loc,
  CAST(w.wmae_reg_sales_rcpts_bulk AS FLOAT64) AS wmae_reg_sales_rcpts_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_reg_sales_rcpts_bulk
    ELSE w.wmae_reg_sales_rcpts_loc
    END AS FLOAT64) AS wmae_reg_sales_rcpts_loc,
  CAST(w.wmae_sales_eoh_bulk AS FLOAT64) AS wmae_sales_eoh_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_sales_eoh_bulk
    ELSE w.wmae_sales_eoh_loc
    END AS FLOAT64) AS wmae_sales_eoh_loc,
  CAST(w.wmae_reg_sales_eoh_bulk AS FLOAT64) AS wmae_reg_sales_eoh_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_reg_sales_eoh_bulk
    ELSE w.wmae_reg_sales_eoh_loc
    END AS FLOAT64) AS wmae_reg_sales_eoh_loc,
  CAST(w.wmae_reg_eoh_bulk AS FLOAT64) AS wmae_reg_eoh_bulk,
  CAST(CASE
    WHEN LOWER(st.selling_channel) = LOWER('ONLINE')
    THEN w.wmae_reg_eoh_bulk
    ELSE w.wmae_reg_eoh_loc
    END AS FLOAT64) AS wmae_reg_eoh_loc,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  update_timestamp,
   `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as update_timestamp_tz
 FROM wmae AS w
  INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.locations AS st ON w.store_num = st.store_num
  INNER JOIN (SELECT TRIM(FORMAT('%11d', div_idnt)) || ': ' || div_desc AS division,
      TRIM(FORMAT('%11d', grp_idnt)) || ': ' || grp_desc AS subdivision,
      TRIM(FORMAT('%11d', dept_idnt)) || ': ' || dept_desc AS department,
    dept_idnt
   FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy AS d
   GROUP BY division,
    subdivision,
    department,
    dept_idnt) AS d ON w.dept_idnt = d.dept_idnt
 EXCEPT DISTINCT
 SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.accuracy;