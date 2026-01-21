/*
Name: Size Curves Evaluation Actuals
APPID-Name: APP08076 Data Driven Size Curves
Purpose: 
    - views for evaluation & monitoring dashboard
Variable(s):    t2dl_das_size T2DL_DAS_SIZE
                env_suffix '' or '_dev' tablesuffix for prod testing

DAG: APP08076_size_curves_eval_vws
Author(s): Sara Riker
Date Created: 6/12/24
Date Updated: 7/15/24

Creates: 
    - size_actuals

Dependancies: 
    - size_eval_cal_week_vw
    - supp_size_hierarchy
    - size_curves_model_rec
*/


CREATE TEMPORARY TABLE IF NOT EXISTS sales_sku_base
AS
SELECT s.channel_num,
 s.store_num,
 s.week_num AS week_idnt,
 w.month_idnt,
  CASE
  WHEN s.week_num = w.ly_week_num_realigned
  THEN 'LY'
  ELSE 'TY'
  END AS ly_ty,
 w.half_idnt,
 s.dropship_ind,
 s.rp_ind,
 s.npg_ind,
 s.rms_sku_num,
 s.net_sales_tot_units AS net_sales_u,
 s.net_sales_tot_regular_units AS reg_net_sales_u,
 s.net_sales_tot_promo_units AS pro_net_sales_u,
 s.net_sales_tot_clearance_units AS clr_net_sales_u,
 s.net_sales_tot_retl AS net_sales_r,
 s.net_sales_tot_regular_retl AS reg_net_sales_r,
 s.net_sales_tot_promo_retl AS pro_net_sales_r,
 s.net_sales_tot_clearance_retl AS clr_net_sales_r,
 s.net_sales_tot_cost AS net_sales_c,
 s.net_sales_tot_regular_cost AS reg_net_sales_c,
 s.net_sales_tot_promo_cost AS pro_net_sales_c,
 s.net_sales_tot_clearance_cost AS clr_net_sales_c,
 s.returns_tot_units AS returns_u,
 s.returns_tot_regular_units AS reg_returns_u,
 s.returns_tot_promo_units AS pro_returns_u,
 s.returns_tot_clearance_units AS clr_returns_u,
 s.returns_tot_retl AS returns_r,
 s.returns_tot_regular_retl AS reg_returns_r,
 s.returns_tot_promo_retl AS pro_returns_r,
 s.returns_tot_clearance_retl AS clr_returns_r,
 s.returns_tot_cost AS returns_c,
 s.returns_tot_regular_cost AS reg_returns_c,
 s.returns_tot_promo_cost AS pro_returns_c,
 s.returns_tot_clearance_cost AS clr_returns_c,
 s.gross_sales_tot_units AS gross_sales_u,
 s.gross_sales_tot_regular_units AS reg_gross_sales_u,
 s.gross_sales_tot_promo_units AS pro_gross_sales_u,
 s.gross_sales_tot_clearance_units AS clr_gross_sales_u,
 s.gross_sales_tot_retl AS gross_sales_r,
 s.gross_sales_tot_regular_retl AS reg_gross_sales_r,
 s.gross_sales_tot_promo_retl AS pro_gross_sales_r,
 s.gross_sales_tot_clearance_retl AS clr_gross_sales_r,
 s.gross_sales_tot_cost AS gross_sales_c,
 s.gross_sales_tot_regular_cost AS reg_gross_sales_c,
 s.gross_sales_tot_promo_cost AS pro_gross_sales_c,
 s.gross_sales_tot_clearance_cost AS clr_gross_sales_c
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw AS w 
 ON (s.week_num = w.week_idnt 
 OR s.week_num = w.ly_week_num_realigned
     ) AND w.last_completed_3_months = 1
WHERE s.channel_num IN (110, 210, 250, 120)
 AND s.division_num IN (351, 345, 310)
 AND (s.week_num IN (SELECT week_idnt
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
     WHERE last_completed_3_months = 1
     GROUP BY week_idnt) 
	 OR s.week_num IN (SELECT ly_week_num_realigned
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
     WHERE last_completed_3_months = 1
     GROUP BY ly_week_num_realigned))
 AND (s.net_sales_tot_units > 0 
 OR s.returns_tot_units > 0 
 OR s.gross_sales_tot_units > 0);


CREATE TEMPORARY TABLE IF NOT EXISTS sales_sku
AS
SELECT s.channel_num,
 s.store_num,
 s.month_idnt,
 s.half_idnt,
 s.week_idnt,
 s.ly_ty,
 s.dropship_ind,
 s.rp_ind,
 s.npg_ind,
 s.rms_sku_num,
 h.dept_idnt,
 h.class_idnt,
 h.supplier_idnt,
 h.size_1_frame,
 h.size_1_rank,
 h.size_1_id,
 s.net_sales_u,
 s.reg_net_sales_u,
 s.pro_net_sales_u,
 s.clr_net_sales_u,
 s.net_sales_r,
 s.reg_net_sales_r,
 s.pro_net_sales_r,
 s.clr_net_sales_r,
 s.net_sales_c,
 s.reg_net_sales_c,
 s.pro_net_sales_c,
 s.clr_net_sales_c,
 s.returns_u,
 s.reg_returns_u,
 s.pro_returns_u,
 s.clr_returns_u,
 s.returns_r,
 s.reg_returns_r,
 s.pro_returns_r,
 s.clr_returns_r,
 s.returns_c,
 s.reg_returns_c,
 s.pro_returns_c,
 s.clr_returns_c,
 s.gross_sales_u,
 s.reg_gross_sales_u,
 s.pro_gross_sales_u,
 s.clr_gross_sales_u,
 s.gross_sales_r,
 s.reg_gross_sales_r,
 s.pro_gross_sales_r,
 s.clr_gross_sales_r,
 s.gross_sales_c,
 s.reg_gross_sales_c,
 s.pro_gross_sales_c,
 s.clr_gross_sales_c
FROM sales_sku_base AS s
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy AS h 
 ON LOWER(s.rms_sku_num) = LOWER(h.sku_idnt);


CREATE TEMPORARY TABLE IF NOT EXISTS sales_size
AS
SELECT channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id,
 MAX(rp_ind) AS rp_ind,
 SUM(net_sales_u) AS net_sales_u,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN net_sales_u
   ELSE 0
   END) AS ds_net_sales_u,
 SUM(reg_net_sales_u) AS reg_net_sales_u,
 SUM(pro_net_sales_u) AS pro_net_sales_u,
 SUM(clr_net_sales_u) AS clr_net_sales_u,
 SUM(net_sales_r) AS net_sales_r,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN net_sales_r
   ELSE 0
   END) AS ds_net_sales_r,
 SUM(reg_net_sales_r) AS reg_net_sales_r,
 SUM(pro_net_sales_r) AS pro_net_sales_r,
 SUM(clr_net_sales_r) AS clr_net_sales_r,
 SUM(net_sales_c) AS net_sales_c,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN net_sales_c
   ELSE 0
   END) AS ds_net_sales_c,
 SUM(reg_net_sales_c) AS reg_net_sales_c,
 SUM(pro_net_sales_c) AS pro_net_sales_c,
 SUM(clr_net_sales_c) AS clr_net_sales_c,
 SUM(returns_u) AS returns_u,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN returns_u
   ELSE 0
   END) AS ds_returns_u,
 SUM(reg_returns_u) AS reg_returns_u,
 SUM(pro_returns_u) AS pro_returns_u,
 SUM(clr_returns_u) AS clr_returns_u,
 SUM(returns_r) AS returns_r,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN returns_r
   ELSE 0
   END) AS ds_returns_r,
 SUM(reg_returns_r) AS reg_returns_r,
 SUM(pro_returns_r) AS pro_returns_r,
 SUM(clr_returns_r) AS clr_returns_r,
 SUM(returns_c) AS returns_c,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN returns_c
   ELSE 0
   END) AS ds_returns_c,
 SUM(reg_returns_c) AS reg_returns_c,
 SUM(pro_returns_c) AS pro_returns_c,
 SUM(clr_returns_c) AS clr_returns_c,
 SUM(gross_sales_u) AS gross_sales_u,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN gross_sales_u
   ELSE 0
   END) AS ds_gross_sales_u,
 SUM(reg_gross_sales_u) AS reg_gross_sales_u,
 SUM(pro_gross_sales_u) AS pro_gross_sales_u,
 SUM(clr_gross_sales_u) AS clr_gross_sales_u,
 SUM(gross_sales_r) AS gross_sales_r,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN gross_sales_r
   ELSE 0
   END) AS ds_gross_sales_r,
 SUM(reg_gross_sales_r) AS reg_gross_sales_r,
 SUM(pro_gross_sales_r) AS pro_gross_sales_r,
 SUM(clr_gross_sales_r) AS clr_gross_sales_r,
 SUM(gross_sales_c) AS gross_sales_c,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN gross_sales_c
   ELSE 0
   END) AS ds_gross_sales_c,
 SUM(reg_gross_sales_c) AS reg_gross_sales_c,
 SUM(pro_gross_sales_c) AS pro_gross_sales_c,
 SUM(clr_gross_sales_c) AS clr_gross_sales_c,
  SUM(net_sales_r) - SUM(net_sales_c) AS product_margin,
  SUM(CASE
    WHEN LOWER(dropship_ind) = LOWER('Y')
    THEN net_sales_r
    ELSE 0
    END) - SUM(CASE
    WHEN LOWER(dropship_ind) = LOWER('Y')
    THEN net_sales_c
    ELSE 0
    END) AS ds_product_margin,
  SUM(reg_net_sales_r) - SUM(reg_net_sales_c) AS reg_product_margin,
  SUM(pro_net_sales_r) - SUM(pro_net_sales_c) AS pro_product_margin,
  SUM(clr_net_sales_r) - SUM(clr_net_sales_c) AS clr_product_margin,
 0 AS eoh_u,
 0 AS reg_eoh_u,
 0 AS cl_eoh_u,
 0 AS eoh_r,
 0 AS reg_eoh_r,
 0 AS cl_eoh_r,
 0 AS eoh_c,
 0 AS reg_eoh_c,
 0 AS cl_eoh_c
FROM sales_sku AS s
GROUP BY channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id;


--COLLECT STATISTICS       COLUMN(store_num)      ,COLUMN(channel_num ,store_num, ly_ty, npg_ind ,dept_idnt ,class_idnt, supplier_idnt ,size_1_frame, size_1_rank, size_1_id)      ON sales_size


-- -- DROP TABLE inv_sku_base;


CREATE TEMPORARY TABLE IF NOT EXISTS inv_sku_base
AS
SELECT i.channel_num,
 i.store_num,
 i.week_num AS week_idnt,
 w.month_idnt,
 w.half_idnt,
  CASE
  WHEN i.week_num = w.ly_week_num_realigned
  THEN 'LY'
  ELSE 'TY'
  END AS ly_ty,
 i.rp_ind,
 i.npg_ind,
 i.rms_sku_num,
 i.eoh_total_units,
 i.eoh_regular_units,
 i.eoh_clearance_units,
 i.eoh_total_retail,
 i.eoh_regular_retail,
 i.eoh_clearance_retail,
 i.eoh_total_cost,
 i.eoh_regular_cost,
 i.eoh_clearance_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS i
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw AS w ON (i.week_num = w.week_idnt OR i.week_num = w.ly_week_num_realigned
     ) AND w.last_completed_3_months = 1
WHERE i.channel_num IN (110, 210, 250, 120)
 AND i.division_num IN (351, 345, 310)
 AND (i.week_num IN (SELECT MAX(week_idnt) AS A928891489
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
     WHERE last_completed_3_months = 1) OR i.week_num IN (SELECT MAX(ly_week_num_realigned) AS A572756494
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
     WHERE last_completed_3_months = 1))
 AND i.eoh_total_units > 0;



CREATE TEMPORARY TABLE IF NOT EXISTS inv_sku
AS
SELECT s.channel_num,
 CAST(TRUNC(CAST(CASE
   WHEN s.store_num = ''
   THEN '0'
   ELSE s.store_num
   END AS FLOAT64)) AS INTEGER) AS store_num,
 s.week_idnt,
 s.month_idnt,
 s.half_idnt,
 s.ly_ty,
 s.rp_ind,
 s.npg_ind,
 h.dept_idnt,
 h.class_idnt,
 h.supplier_idnt,
 h.size_1_frame,
 h.size_1_rank,
 h.size_1_id,
 s.eoh_total_units,
 s.eoh_regular_units,
 s.eoh_clearance_units,
 s.eoh_total_retail,
 s.eoh_regular_retail,
 s.eoh_clearance_retail,
 s.eoh_total_cost,
 s.eoh_regular_cost,
 s.eoh_clearance_cost
FROM inv_sku_base AS s
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy AS h ON LOWER(s.rms_sku_num) = LOWER(h.sku_idnt); 


CREATE TEMPORARY TABLE IF NOT EXISTS inv_size
AS
SELECT channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id,
 MAX(rp_ind) AS rp_ind,
 0 AS net_sales_u,
 0 AS ds_net_sales_u,
 0 AS reg_net_sales_u,
 0 AS pro_net_sales_u,
 0 AS clr_net_sales_u,
 0 AS net_sales_r,
 0 AS ds_net_sales_r,
 0 AS reg_net_sales_r,
 0 AS pro_net_sales_r,
 0 AS clr_net_sales_r,
 0 AS net_sales_c,
 0 AS ds_net_sales_c,
 0 AS reg_net_sales_c,
 0 AS pro_net_sales_c,
 0 AS clr_net_sales_c,
 0 AS returns_u,
 0 AS ds_returns_u,
 0 AS reg_returns_u,
 0 AS pro_returns_u,
 0 AS clr_returns_u,
 0 AS returns_r,
 0 AS ds_returns_r,
 0 AS reg_returns_r,
 0 AS pro_returns_r,
 0 AS clr_returns_r,
 0 AS returns_c,
 0 AS ds_returns_c,
 0 AS reg_returns_c,
 0 AS pro_returns_c,
 0 AS clr_returns_c,
 0 AS gross_sales_u,
 0 AS ds_gross_sales_u,
 0 AS reg_gross_sales_u,
 0 AS pro_gross_sales_u,
 0 AS clr_gross_sales_u,
 0 AS gross_sales_r,
 0 AS ds_gross_sales_r,
 0 AS reg_gross_sales_r,
 0 AS pro_gross_sales_r,
 0 AS clr_gross_sales_r,
 0 AS gross_sales_c,
 0 AS ds_gross_sales_c,
 0 AS reg_gross_sales_c,
 0 AS pro_gross_sales_c,
 0 AS clr_gross_sales_c,
 0 AS product_margin,
 0 AS ds_product_margin,
 0 AS reg_product_margin,
 0 AS pro_product_margin,
 0 AS clr_product_margin,
 SUM(eoh_total_units) AS eoh_u,
 SUM(eoh_regular_units) AS reg_eoh_u,
 SUM(eoh_clearance_units) AS cl_eoh_u,
 SUM(eoh_total_retail) AS eoh_r,
 SUM(eoh_regular_retail) AS reg_eoh_r,
 SUM(eoh_clearance_retail) AS cl_eoh_r,
 SUM(eoh_total_cost) AS eoh_c,
 SUM(eoh_regular_cost) AS reg_eoh_c,
 SUM(eoh_clearance_cost) AS cl_eoh_c
FROM inv_sku
GROUP BY channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id;


--COLLECT STATISTICS       COLUMN(store_num)     ,COLUMN(channel_num, store_num, ly_ty, npg_ind ,dept_idnt ,class_idnt, supplier_idnt,size_1_frame, size_1_rank, size_1_id)      ON inv_size


-- -- DROP TABLE actuals_size;


CREATE TEMPORARY TABLE IF NOT EXISTS actuals_size
AS
SELECT *
FROM sales_size AS s
UNION ALL
SELECT *
FROM inv_size AS i;


CREATE TEMPORARY TABLE IF NOT EXISTS inv_sales
AS
SELECT channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id,
 MAX(rp_ind) AS rp_ind,
 SUM(COALESCE(net_sales_u, 0)) AS net_sales_u,
 SUM(COALESCE(ds_net_sales_u, 0)) AS ds_net_sales_u,
 SUM(COALESCE(reg_net_sales_u, 0)) AS reg_net_sales_u,
 SUM(COALESCE(pro_net_sales_u, 0)) AS pro_net_sales_u,
 SUM(COALESCE(clr_net_sales_u, 0)) AS clr_net_sales_u,
 SUM(COALESCE(net_sales_r, 0)) AS net_sales_r,
 SUM(COALESCE(ds_net_sales_r, 0)) AS ds_net_sales_r,
 SUM(COALESCE(reg_net_sales_r, 0)) AS reg_net_sales_r,
 SUM(COALESCE(pro_net_sales_r, 0)) AS pro_net_sales_r,
 SUM(COALESCE(clr_net_sales_r, 0)) AS clr_net_sales_r,
 SUM(COALESCE(net_sales_c, 0)) AS net_sales_c,
 SUM(COALESCE(ds_net_sales_c, 0)) AS ds_net_sales_c,
 SUM(COALESCE(reg_net_sales_c, 0)) AS reg_net_sales_c,
 SUM(COALESCE(pro_net_sales_c, 0)) AS pro_net_sales_c,
 SUM(COALESCE(clr_net_sales_c, 0)) AS clr_net_sales_c,
 SUM(COALESCE(returns_u, 0)) AS returns_u,
 SUM(COALESCE(ds_returns_u, 0)) AS ds_returns_u,
 SUM(COALESCE(reg_returns_u, 0)) AS reg_returns_u,
 SUM(COALESCE(pro_returns_u, 0)) AS pro_returns_u,
 SUM(COALESCE(clr_returns_u, 0)) AS clr_returns_u,
 SUM(COALESCE(returns_r, 0)) AS returns_r,
 SUM(COALESCE(ds_returns_r, 0)) AS ds_returns_r,
 SUM(COALESCE(reg_returns_r, 0)) AS reg_returns_r,
 SUM(COALESCE(pro_returns_r, 0)) AS pro_returns_r,
 SUM(COALESCE(clr_returns_r, 0)) AS clr_returns_r,
 SUM(COALESCE(returns_c, 0)) AS returns_c,
 SUM(COALESCE(ds_returns_c, 0)) AS ds_returns_c,
 SUM(COALESCE(reg_returns_c, 0)) AS reg_returns_c,
 SUM(COALESCE(pro_returns_c, 0)) AS pro_returns_c,
 SUM(COALESCE(clr_returns_c, 0)) AS clr_returns_c,
 SUM(COALESCE(gross_sales_u, 0)) AS gross_sales_u,
 SUM(COALESCE(ds_gross_sales_u, 0)) AS ds_gross_sales_u,
 SUM(COALESCE(reg_gross_sales_u, 0)) AS reg_gross_sales_u,
 SUM(COALESCE(pro_gross_sales_u, 0)) AS pro_gross_sales_u,
 SUM(COALESCE(clr_gross_sales_u, 0)) AS clr_gross_sales_u,
 SUM(COALESCE(gross_sales_r, 0)) AS gross_sales_r,
 SUM(COALESCE(ds_gross_sales_r, 0)) AS ds_gross_sales_r,
 SUM(COALESCE(reg_gross_sales_r, 0)) AS reg_gross_sales_r,
 SUM(COALESCE(pro_gross_sales_r, 0)) AS pro_gross_sales_r,
 SUM(COALESCE(clr_gross_sales_r, 0)) AS clr_gross_sales_r,
 SUM(COALESCE(gross_sales_c, 0)) AS gross_sales_c,
 SUM(COALESCE(ds_gross_sales_c, 0)) AS ds_gross_sales_c,
 SUM(COALESCE(reg_gross_sales_c, 0)) AS reg_gross_sales_c,
 SUM(COALESCE(pro_gross_sales_c, 0)) AS pro_gross_sales_c,
 SUM(COALESCE(clr_gross_sales_c, 0)) AS clr_gross_sales_c,
 SUM(COALESCE(product_margin, 0)) AS product_margin,
 SUM(COALESCE(ds_product_margin, 0)) AS ds_product_margin,
 SUM(COALESCE(reg_product_margin, 0)) AS reg_product_margin,
 SUM(COALESCE(pro_product_margin, 0)) AS pro_product_margin,
 SUM(COALESCE(clr_product_margin, 0)) AS clr_product_margin,
 SUM(COALESCE(eoh_u, 0)) AS eoh_u,
 SUM(COALESCE(reg_eoh_u, 0)) AS reg_eoh_u,
 SUM(COALESCE(cl_eoh_u, 0)) AS cl_eoh_u,
 SUM(COALESCE(eoh_r, 0)) AS eoh_r,
 SUM(COALESCE(reg_eoh_r, 0)) AS reg_eoh_r,
 SUM(COALESCE(cl_eoh_r, 0)) AS cl_eoh_r,
 SUM(COALESCE(eoh_c, 0)) AS eoh_c,
 SUM(COALESCE(reg_eoh_c, 0)) AS reg_eoh_c,
 SUM(COALESCE(cl_eoh_c, 0)) AS cl_eoh_c,
 0 AS receipts_u,
 0 AS receipts_c,
 0 AS receipts_r,
 0 AS ds_receipts_u,
 0 AS ds_receipts_c,
 0 AS ds_receipts_r,
 0 AS close_out_receipt_u,
 0 AS close_out_receipt_c,
 0 AS close_out_receipt_r,
 0 AS close_out_sized_receipt_u,
 0 AS close_out_sized_receipt_c,
 0 AS close_out_sized_receipt_r,
 0 AS casepack_receipt_u,
 0 AS casepack_receipt_c,
 0 AS casepack_receipt_r,
 0 AS prepack_receipt_u,
 0 AS prepack_receipt_c,
 0 AS prepack_receipt_r,
 0 AS sized_receipt_u,
 0 AS sized_receipt_c,
 0 AS sized_receipt_r,
 0 AS unknown_receipt_u,
 0 AS unknown_receipt_c,
 0 AS unknown_receipt_r,
 0 AS transfer_out_u,
 0 AS transfer_in_u,
 0 AS racking_last_chance_out_u,
 0 AS flx_in_u,
 0 AS stock_balance_out_u,
 0 AS stock_balance_in_u,
 0 AS reserve_stock_in_u,
 0 AS pah_in_u,
 0 AS cust_return_out_u,
 0 AS cust_return_in_u,
 0 AS cust_transfer_out_u,
 0 AS cust_transfer_in_u
FROM actuals_size AS a
GROUP BY channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id;



CREATE TEMPORARY TABLE IF NOT EXISTS po_base AS 
WITH allocation AS (SELECT DISTINCT operation_id AS purchase_order_number,
   purchase_order_type,
   item_type
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.allocation_item_fact_vw AS po_fct
  WHERE po_end_ship_date >= DATE '2023-01-01'
   AND LOWER(operation_type) = LOWER('PURCHASE_ORDER')
   AND operation_id IS NOT NULL
  QUALIFY (DENSE_RANK() OVER (PARTITION BY operation_id ORDER BY source_system)) = 1) 

  (SELECT po_fct.purchase_order_num,
   po_fct.status,
   po_fct.order_type,
    CASE
    WHEN LOWER(po_hdr.order_type) IN (LOWER('AUTOMATIC_REORDER'), LOWER('BUYER_REORDER'))
    THEN 'Y'
    ELSE 'N'
    END AS rp_ind,
   COALESCE(po_fct.purchaseorder_type, po_hdr.po_type, a.purchase_order_type) AS purchase_order_type,
   po_hdr.internal_po_ind,
   po_hdr.npg_ind,
   po_fct.dropship_ind,
   po_hdr.merchandise_source,
   a.item_type
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_fact AS po_fct
   LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS po_hdr ON LOWER(po_fct.purchase_order_num) = LOWER(po_hdr.purchase_order_number
     )
   LEFT JOIN allocation AS a ON LOWER(po_fct.purchase_order_num) = LOWER(a.purchase_order_number)
   INNER JOIN (SELECT DISTINCT dept_idnt
    FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy) AS d ON po_fct.department_id = d.dept_idnt
  WHERE po_fct.open_to_buy_endofweek_date >= DATE '2023-01-01');



CREATE TEMPORARY TABLE IF NOT EXISTS receipts
AS
SELECT CASE
  WHEN w.ly_week_num_realigned = cal.week_idnt
  THEN 'LY'
  ELSE 'TY'
  END AS ly_ty,
 rcpt.store_num,
 st.channel_num,
 rcpt.dropship_ind,
 rcpt.rp_ind,
 h.npg_ind,
 h.dept_idnt,
 h.class_idnt,
 h.supplier_idnt,
 h.size_1_frame,
 h.size_1_rank,
 h.size_1_id,
  CASE
  WHEN pot.po_type_code IS NULL AND LOWER(rcpt.dropship_ind) = LOWER('Y')
  THEN 'DR'
  WHEN pot.po_type_code IS NULL AND LOWER(rcpt.rp_ind) = LOWER('Y')
  THEN 'RP'
  WHEN po.purchase_order_type IS NOT NULL
  THEN po.purchase_order_type
  ELSE 'NONE'
  END AS po_type_code,
  CASE
  WHEN pot.po_type_code IS NULL AND LOWER(rcpt.dropship_ind) = LOWER('Y')
  THEN 'Dropship'
  WHEN pot.po_type_code IS NULL AND LOWER(rcpt.rp_ind) = LOWER('Y')
  THEN 'Replenishment'
  WHEN pot.po_type_desc IS NOT NULL
  THEN pot.po_type_desc
  ELSE 'NONE'
  END AS po_type_desc,
  CASE
  WHEN LOWER(pot.po_type_code) IN (LOWER('HF'), LOWER('HL'), LOWER('PM'), LOWER('RS'), LOWER('RY'), LOWER('RZ'), LOWER('DN'
     ), LOWER('OL'), LOWER('CN'), LOWER('CO'), LOWER('NQ'), LOWER('TS'), LOWER('DB'), LOWER('DS'), LOWER('BK'), LOWER('IN'
     ), LOWER('PU'), LOWER('XR'))
  THEN 'N'
  WHEN pot.po_type_code IS NULL AND LOWER(rcpt.dropship_ind) = LOWER('Y')
  THEN 'N'
  WHEN pot.po_type_code IS NULL AND LOWER(rcpt.rp_ind) = LOWER('Y')
  THEN 'N'
  WHEN LOWER(po.item_type) IN (LOWER('CASEPACK'), LOWER('PREPACK'))
  THEN 'N'
  ELSE 'Y'
  END AS jda,
  CASE
  WHEN LOWER(po.merchandise_source) = LOWER('CLOSE OUT') OR LOWER(po.purchase_order_type) = LOWER('GE')
  THEN 'Y'
  ELSE 'N'
  END AS close_out,
  CASE
  WHEN po.item_type IS NULL AND LOWER(rcpt.dropship_ind) = LOWER('Y')
  THEN 'DS'
  WHEN po.item_type IS NULL AND LOWER(rcpt.rp_ind) = LOWER('Y')
  THEN 'RP'
  WHEN po.item_type IS NOT NULL
  THEN po.item_type
  ELSE 'UNKNOWN'
  END AS item_type,
 SUM(rcpt.receipts_units + rcpt.receipts_crossdock_units) AS receipts_u,
 SUM(rcpt.receipts_cost + rcpt.receipts_crossdock_cost) AS receipts_c,
 SUM(rcpt.receipts_retail + rcpt.receipts_crossdock_retail) AS receipts_r
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS rcpt
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal AS cal ON rcpt.tran_date = cal.day_date AND cal.hist_ind = 1
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st ON rcpt.store_num = st.store_num
 INNER JOIN po_base AS po ON LOWER(rcpt.poreceipt_order_number) = LOWER(po.purchase_order_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw AS w ON (cal.week_idnt = w.week_idnt OR cal.week_idnt = w.ly_week_num_realigned
     ) AND w.last_completed_3_months = 1
 LEFT JOIN (SELECT DISTINCT *
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.po_types) AS pot ON LOWER(po.purchase_order_type) = LOWER(pot.po_type_code)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy AS h ON LOWER(h.sku_idnt) = LOWER(rcpt.sku_num)
WHERE (cal.week_idnt IN (SELECT week_idnt
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
     WHERE last_completed_3_months = 1
     GROUP BY week_idnt) OR cal.week_idnt IN (SELECT ly_week_num_realigned
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
     WHERE last_completed_3_months = 1
     GROUP BY ly_week_num_realigned))
 AND rcpt.tran_code = 20
 AND st.channel_num IN (110, 210, 250, 120)
GROUP BY ly_ty,
 rcpt.store_num,
 st.channel_num,
 rcpt.dropship_ind,
 rcpt.rp_ind,
 h.npg_ind,
 h.dept_idnt,
 h.class_idnt,
 h.supplier_idnt,
 h.size_1_frame,
 h.size_1_rank,
 h.size_1_id,
 po_type_code,
 po_type_desc,
 jda,
 close_out,
 item_type;



CREATE TEMPORARY TABLE IF NOT EXISTS trans_base
AS
SELECT a.purchase_order_num,
 rcpt.sku_num,
 rcpt.store_num,
 st.channel_num,
 cal.week_idnt,
 rcpt.dropship_ind,
 rcpt.rp_ind,
 SUM(rcpt.receipts_units + rcpt.receipts_crossdock_units) AS receipts_u,
 SUM(rcpt.receipts_cost + rcpt.receipts_crossdock_cost) AS receipts_c,
 SUM(rcpt.receipts_retail + rcpt.receipts_crossdock_retail) AS receipts_r
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_fact_vw AS rcpt
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_item_distributelocation_fact AS a ON LOWER(SUBSTR(CAST(a.external_distribution_id AS STRING)
      , 1, 100)) = LOWER(rcpt.poreceipt_order_number) AND LOWER(a.rms_sku_num) = LOWER(rcpt.sku_num) AND rcpt.store_num
   = a.distribute_location_id
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal AS cal ON rcpt.tran_date = cal.day_date AND cal.hist_ind = 1
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy AS h ON LOWER(h.sku_idnt) = LOWER(rcpt.sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st ON rcpt.store_num = st.store_num
WHERE rcpt.tran_code = 30
 AND st.channel_num IN (110, 210, 250, 120)
GROUP BY a.purchase_order_num,
 rcpt.sku_num,
 rcpt.store_num,
 st.channel_num,
 cal.week_idnt,
 rcpt.dropship_ind,
 rcpt.rp_ind;


CREATE TEMPORARY TABLE IF NOT EXISTS trans
AS
SELECT CASE
  WHEN w.ly_week_num_realigned = t.week_idnt
  THEN 'LY'
  ELSE 'TY'
  END AS ly_ty,
 t.store_num,
 t.channel_num,
 t.dropship_ind,
 t.rp_ind,
 h.npg_ind,
 h.dept_idnt,
 h.class_idnt,
 h.supplier_idnt,
 h.size_1_frame,
 h.size_1_rank,
 h.size_1_id,
  CASE
  WHEN pot.po_type_code IS NULL AND LOWER(t.dropship_ind) = LOWER('Y')
  THEN 'DR'
  WHEN pot.po_type_code IS NULL AND LOWER(t.rp_ind) = LOWER('Y')
  THEN 'RP'
  WHEN po.purchase_order_type IS NOT NULL
  THEN po.purchase_order_type
  ELSE 'NONE'
  END AS po_type_code,
  CASE
  WHEN pot.po_type_code IS NULL AND LOWER(t.dropship_ind) = LOWER('Y')
  THEN 'Dropship'
  WHEN pot.po_type_code IS NULL AND LOWER(t.rp_ind) = LOWER('Y')
  THEN 'Replenishment'
  WHEN pot.po_type_desc IS NOT NULL
  THEN pot.po_type_desc
  ELSE 'NONE'
  END AS po_type_desc,
  CASE
  WHEN LOWER(pot.po_type_code) IN (LOWER('HF'), LOWER('HL'), LOWER('PM'), LOWER('RS'), LOWER('RY'), LOWER('RZ'), LOWER('DN'
     ), LOWER('OL'), LOWER('CN'), LOWER('CO'), LOWER('NQ'), LOWER('TS'), LOWER('DB'), LOWER('DS'), LOWER('BK'), LOWER('IN'
     ), LOWER('PU'), LOWER('XR'))
  THEN 'N'
  WHEN pot.po_type_code IS NULL AND LOWER(t.dropship_ind) = LOWER('Y')
  THEN 'N'
  WHEN pot.po_type_code IS NULL AND LOWER(t.rp_ind) = LOWER('Y')
  THEN 'N'
  WHEN LOWER(po.item_type) IN (LOWER('CASEPACK'), LOWER('PREPACK'))
  THEN 'N'
  ELSE 'Y'
  END AS jda,
  CASE
  WHEN LOWER(po.merchandise_source) = LOWER('CLOSE OUT') OR LOWER(po.purchase_order_type) = LOWER('GE')
  THEN 'Y'
  ELSE 'N'
  END AS close_out,
  CASE
  WHEN po.item_type IS NULL AND LOWER(t.dropship_ind) = LOWER('Y')
  THEN 'DS'
  WHEN po.item_type IS NULL AND LOWER(t.rp_ind) = LOWER('Y')
  THEN 'RP'
  WHEN po.item_type IS NOT NULL
  THEN po.item_type
  ELSE 'UNKNOWN'
  END AS item_type,
 SUM(t.receipts_u) AS receipts_u,
 SUM(t.receipts_c) AS receipts_c,
 SUM(t.receipts_r) AS receipts_r
FROM trans_base AS t
 INNER JOIN po_base AS po ON LOWER(t.purchase_order_num) = LOWER(po.purchase_order_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw AS w ON (t.week_idnt = w.week_idnt OR t.week_idnt = w.ly_week_num_realigned
     ) AND w.last_completed_3_months = 1
 LEFT JOIN (SELECT DISTINCT *
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.po_types) AS pot ON LOWER(po.purchase_order_type) = LOWER(pot.po_type_code)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy AS h ON LOWER(h.sku_idnt) = LOWER(t.sku_num)
WHERE (t.week_idnt IN (SELECT week_idnt
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
     WHERE last_completed_3_months = 1
     GROUP BY week_idnt) OR t.week_idnt IN (SELECT ly_week_num_realigned
     FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
     WHERE last_completed_3_months = 1
     GROUP BY ly_week_num_realigned))
 AND t.channel_num IN (110, 210, 250, 120)
GROUP BY ly_ty,
 t.store_num,
 t.channel_num,
 t.dropship_ind,
 t.rp_ind,
 h.npg_ind,
 h.dept_idnt,
 h.class_idnt,
 h.supplier_idnt,
 h.size_1_frame,
 h.size_1_rank,
 h.size_1_id,
 po_type_code,
 po_type_desc,
 jda,
 close_out,
 item_type;



CREATE TEMPORARY TABLE IF NOT EXISTS receipts_trans
AS
SELECT channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id,
 MAX(rp_ind) AS rp_ind,
 0 AS net_sales_u,
 0 AS ds_net_sales_u,
 0 AS reg_net_sales_u,
 0 AS pro_net_sales_u,
 0 AS clr_net_sales_u,
 0 AS net_sales_r,
 0 AS ds_net_sales_r,
 0 AS reg_net_sales_r,
 0 AS pro_net_sales_r,
 0 AS clr_net_sales_r,
 0 AS net_sales_c,
 0 AS ds_net_sales_c,
 0 AS reg_net_sales_c,
 0 AS pro_net_sales_c,
 0 AS clr_net_sales_c,
 0 AS returns_u,
 0 AS ds_returns_u,
 0 AS reg_returns_u,
 0 AS pro_returns_u,
 0 AS clr_returns_u,
 0 AS returns_r,
 0 AS ds_returns_r,
 0 AS reg_returns_r,
 0 AS pro_returns_r,
 0 AS clr_returns_r,
 0 AS returns_c,
 0 AS ds_returns_c,
 0 AS reg_returns_c,
 0 AS pro_returns_c,
 0 AS clr_returns_c,
 0 AS gross_sales_u,
 0 AS ds_gross_sales_u,
 0 AS reg_gross_sales_u,
 0 AS pro_gross_sales_u,
 0 AS clr_gross_sales_u,
 0 AS gross_sales_r,
 0 AS ds_gross_sales_r,
 0 AS reg_gross_sales_r,
 0 AS pro_gross_sales_r,
 0 AS clr_gross_sales_r,
 0 AS gross_sales_c,
 0 AS ds_gross_sales_c,
 0 AS reg_gross_sales_c,
 0 AS pro_gross_sales_c,
 0 AS clr_gross_sales_c,
 0 AS product_margin,
 0 AS ds_product_margin,
 0 AS reg_product_margin,
 0 AS pro_product_margin,
 0 AS clr_product_margin,
 0 AS eoh_u,
 0 AS reg_eoh_u,
 0 AS cl_eoh_u,
 0 AS eoh_r,
 0 AS reg_eoh_r,
 0 AS cl_eoh_r,
 0 AS eoh_c,
 0 AS reg_eoh_c,
 0 AS cl_eoh_c,
 SUM(receipts_u) AS receipts_u,
 SUM(receipts_c) AS receipts_c,
 SUM(receipts_r) AS receipts_r,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN receipts_u
   ELSE 0
   END) AS ds_receipts_u,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN receipts_c
   ELSE 0
   END) AS ds_receipts_c,
 SUM(CASE
   WHEN LOWER(dropship_ind) = LOWER('Y')
   THEN receipts_r
   ELSE 0
   END) AS ds_receipts_r,
 SUM(CASE
   WHEN LOWER(close_out) = LOWER('Y')
   THEN receipts_u
   ELSE 0
   END) AS close_out_receipt_u,
 SUM(CASE
   WHEN LOWER(close_out) = LOWER('Y')
   THEN receipts_c
   ELSE 0
   END) AS close_out_receipt_c,
 SUM(CASE
   WHEN LOWER(close_out) = LOWER('Y')
   THEN receipts_r
   ELSE 0
   END) AS close_out_receipt_r,
 SUM(CASE
   WHEN LOWER(close_out) = LOWER('Y') AND LOWER(item_type) = LOWER('ITEM')
   THEN receipts_u
   ELSE 0
   END) AS close_out_sized_receipt_u,
 SUM(CASE
   WHEN LOWER(close_out) = LOWER('Y') AND LOWER(item_type) = LOWER('ITEM')
   THEN receipts_c
   ELSE 0
   END) AS close_out_sized_receipt_c,
 SUM(CASE
   WHEN LOWER(close_out) = LOWER('Y') AND LOWER(item_type) = LOWER('ITEM')
   THEN receipts_r
   ELSE 0
   END) AS close_out_sized_receipt_r,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('CASEPACK')
   THEN receipts_u
   ELSE 0
   END) AS casepack_receipt_u,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('CASEPACK')
   THEN receipts_c
   ELSE 0
   END) AS casepack_receipt_c,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('CASEPACK')
   THEN receipts_r
   ELSE 0
   END) AS casepack_receipt_r,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('PREPACK')
   THEN receipts_u
   ELSE 0
   END) AS prepack_receipt_u,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('PREPACK')
   THEN receipts_c
   ELSE 0
   END) AS prepack_receipt_c,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('PREPACK')
   THEN receipts_r
   ELSE 0
   END) AS prepack_receipt_r,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('ITEM')
   THEN receipts_u
   ELSE 0
   END) AS sized_receipt_u,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('ITEM')
   THEN receipts_c
   ELSE 0
   END) AS sized_receipt_c,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('ITEM')
   THEN receipts_r
   ELSE 0
   END) AS sized_receipt_r,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('UNKNOWN')
   THEN receipts_u
   ELSE 0
   END) AS unknown_receipt_u,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('UNKNOWN')
   THEN receipts_c
   ELSE 0
   END) AS unknown_receipt_c,
 SUM(CASE
   WHEN LOWER(item_type) = LOWER('UNKNOWN')
   THEN receipts_r
   ELSE 0
   END) AS unknown_receipt_r,
 0 AS transfer_out_u,
 0 AS transfer_in_u,
 0 AS racking_last_chance_out_u,
 0 AS flx_in_u,
 0 AS stock_balance_out_u,
 0 AS stock_balance_in_u,
 0 AS reserve_stock_in_u,
 0 AS pah_in_u,
 0 AS cust_return_out_u,
 0 AS cust_return_in_u,
 0 AS cust_transfer_out_u,
 0 AS cust_transfer_in_u
FROM (SELECT *
   FROM receipts
   UNION ALL
   SELECT *
   FROM trans) AS r
GROUP BY channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id;



CREATE TEMPORARY TABLE IF NOT EXISTS transfer_type
AS
SELECT tr.transfer_context_value,
 fct.transfer_context_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rms14_transfer_created_fact AS tr
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rms_cost_transfers_fact AS fct ON LOWER(tr.operation_num) = LOWER(fct.transfer_num) AND
   LOWER(tr.rms_sku_num) = LOWER(fct.rms_sku_num)
WHERE LOWER(fct.transfer_context_type) IN (LOWER('RACKING'), LOWER('LAST CHANCE'), LOWER('STOCK_BALANCE'), LOWER('RESERVE_STOCK_TRANSFER'
    ), LOWER('RACK_PACK_AND_HOLD'), LOWER('CUSTOMER_RETURN'), LOWER('CUSTOMER_TRANSFER'))
GROUP BY tr.transfer_context_value,
 fct.transfer_context_type;



CREATE TEMPORARY TABLE IF NOT EXISTS transfer_base
AS
SELECT tr.operation_num,
 tr.rms_sku_num,
 sh.receipt_date,
 w.week_idnt,
 w.month_idnt,
 w.half_idnt,
  CASE
  WHEN w.ly_week_num_realigned = cal.week_idnt
  THEN 'LY'
  ELSE 'TY'
  END AS ly_ty,
 tr.transfer_context_value,
 tt.transfer_context_type,
 tr.from_location_id,
 tr.to_location_id,
 tr.transfer_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rms14_transfer_created_fact AS tr
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rms14_transfer_shipment_receipt_fact AS sh ON LOWER(sh.operation_num) = LOWER(tr.operation_num
    ) AND LOWER(sh.rms_sku_num) = LOWER(tr.rms_sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal AS cal ON sh.receipt_date = cal.day_date AND cal.hist_ind = 1
 INNER JOIN (SELECT *
  FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
  WHERE last_completed_3_months = 1) AS w ON cal.week_idnt = w.week_idnt OR cal.week_idnt = w.ly_week_num_realigned
 LEFT JOIN transfer_type AS tt ON LOWER(tr.transfer_context_value) = LOWER(tt.transfer_context_value)
WHERE LOWER(tr.transfer_context_value) NOT IN (LOWER('GT'), LOWER('GTR'), LOWER('HL'), LOWER('ICR'), LOWER('NXTR'),
   LOWER('NULL'))
GROUP BY tr.operation_num,
 tr.rms_sku_num,
 sh.receipt_date,
 w.week_idnt,
 w.month_idnt,
 w.half_idnt,
 ly_ty,
 tr.transfer_context_value,
 tt.transfer_context_type,
 tr.from_location_id,
 tr.to_location_id,
 tr.transfer_qty;


CREATE TEMPORARY TABLE IF NOT EXISTS transfers
AS
SELECT a.operation_num,
 a.rms_sku_num,
 a.receipt_date,
 a.week_idnt,
 a.month_idnt,
 a.half_idnt,
 a.ly_ty,
 a.transfer_context_value,
 a.transfer_context_type,
 a.from_location_id AS store_num,
 st.channel_num,
 h.dept_idnt,
 h.class_idnt,
 h.supplier_idnt,
 h.npg_ind,
 h.size_1_frame,
 h.size_1_rank,
 h.size_1_id,
 a.transfer_qty AS transfer_out_qty,
 0 AS transfer_in_qty
FROM transfer_base AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st ON a.from_location_id = st.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy AS h ON LOWER(h.sku_idnt) = LOWER(a.rms_sku_num)
WHERE st.channel_num IN (110, 210, 250, 120)
UNION ALL
SELECT a0.operation_num,
 a0.rms_sku_num,
 a0.receipt_date,
 a0.week_idnt,
 a0.month_idnt,
 a0.half_idnt,
 a0.ly_ty,
 a0.transfer_context_value,
 a0.transfer_context_type,
 a0.to_location_id AS store_num,
 st0.channel_num,
 h0.dept_idnt,
 h0.class_idnt,
 h0.supplier_idnt,
 h0.npg_ind,
 h0.size_1_frame,
 h0.size_1_rank,
 h0.size_1_id,
 0 AS transfer_out_qty,
 a0.transfer_qty AS transfer_in_qty
FROM transfer_base AS a0
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw AS st0 ON a0.to_location_id = st0.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.environment_schema}}.supp_size_hierarchy AS h0 ON LOWER(h0.sku_idnt) = LOWER(a0.rms_sku_num)
WHERE st0.channel_num IN (110, 210, 250, 120);




CREATE TEMPORARY TABLE IF NOT EXISTS transfers_in_out
AS
SELECT channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id,
 MAX('N') AS rp_ind,
 0 AS net_sales_u,
 0 AS ds_net_sales_u,
 0 AS reg_net_sales_u,
 0 AS pro_net_sales_u,
 0 AS clr_net_sales_u,
 0 AS net_sales_r,
 0 AS ds_net_sales_r,
 0 AS reg_net_sales_r,
 0 AS pro_net_sales_r,
 0 AS clr_net_sales_r,
 0 AS net_sales_c,
 0 AS ds_net_sales_c,
 0 AS reg_net_sales_c,
 0 AS pro_net_sales_c,
 0 AS clr_net_sales_c,
 0 AS returns_u,
 0 AS ds_returns_u,
 0 AS reg_returns_u,
 0 AS pro_returns_u,
 0 AS clr_returns_u,
 0 AS returns_r,
 0 AS ds_returns_r,
 0 AS reg_returns_r,
 0 AS pro_returns_r,
 0 AS clr_returns_r,
 0 AS returns_c,
 0 AS ds_returns_c,
 0 AS reg_returns_c,
 0 AS pro_returns_c,
 0 AS clr_returns_c,
 0 AS gross_sales_u,
 0 AS ds_gross_sales_u,
 0 AS reg_gross_sales_u,
 0 AS pro_gross_sales_u,
 0 AS clr_gross_sales_u,
 0 AS gross_sales_r,
 0 AS ds_gross_sales_r,
 0 AS reg_gross_sales_r,
 0 AS pro_gross_sales_r,
 0 AS clr_gross_sales_r,
 0 AS gross_sales_c,
 0 AS ds_gross_sales_c,
 0 AS reg_gross_sales_c,
 0 AS pro_gross_sales_c,
 0 AS clr_gross_sales_c,
 0 AS product_margin,
 0 AS ds_product_margin,
 0 AS reg_product_margin,
 0 AS pro_product_margin,
 0 AS clr_product_margin,
 0 AS eoh_u,
 0 AS reg_eoh_u,
 0 AS cl_eoh_u,
 0 AS eoh_r,
 0 AS reg_eoh_r,
 0 AS cl_eoh_r,
 0 AS eoh_c,
 0 AS reg_eoh_c,
 0 AS cl_eoh_c,
 0 AS receipts_u,
 0 AS receipts_c,
 0 AS receipts_r,
 0 AS ds_receipts_u,
 0 AS ds_receipts_c,
 0 AS ds_receipts_r,
 0 AS close_out_receipt_u,
 0 AS close_out_receipt_c,
 0 AS close_out_receipt_r,
 0 AS close_out_sized_receipt_u,
 0 AS close_out_sized_receipt_c,
 0 AS close_out_sized_receipt_r,
 0 AS casepack_receipt_u,
 0 AS casepack_receipt_c,
 0 AS casepack_receipt_r,
 0 AS prepack_receipt_u,
 0 AS prepack_receipt_c,
 0 AS prepack_receipt_r,
 0 AS sized_receipt_u,
 0 AS sized_receipt_c,
 0 AS sized_receipt_r,
 0 AS unknown_receipt_u,
 0 AS unknown_receipt_c,
 0 AS unknown_receipt_r,
 SUM(transfer_out_qty) AS transfer_out_u,
 SUM(transfer_in_qty) AS transfer_in_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) IN (LOWER('RACKING'), LOWER('LAST CHANCE'))
   THEN transfer_out_qty
   ELSE NULL
   END) AS racking_last_chance_out_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('RACKING')
   THEN transfer_in_qty
   ELSE NULL
   END) AS flx_in_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('STOCK_BALANCE')
   THEN transfer_out_qty
   ELSE NULL
   END) AS stock_balance_out_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('STOCK_BALANCE')
   THEN transfer_in_qty
   ELSE NULL
   END) AS stock_balance_in_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('RESERVE_STOCK_TRANSFER')
   THEN transfer_in_qty
   ELSE NULL
   END) AS reserve_stock_in_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('RACK_PACK_AND_HOLD')
   THEN transfer_in_qty
   ELSE NULL
   END) AS pah_in_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('CUSTOMER_RETURN')
   THEN transfer_out_qty
   ELSE NULL
   END) AS cust_return_out_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('CUSTOMER_RETURN')
   THEN transfer_in_qty
   ELSE NULL
   END) AS cust_return_in_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('CUSTOMER_TRANSFER')
   THEN transfer_out_qty
   ELSE NULL
   END) AS cust_transfer_out_u,
 SUM(CASE
   WHEN LOWER(transfer_context_type) = LOWER('CUSTOMER_TRANSFER')
   THEN transfer_in_qty
   ELSE NULL
   END) AS cust_transfer_in_u
FROM transfers
GROUP BY channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id;



CREATE TEMPORARY TABLE IF NOT EXISTS frames
AS
SELECT half_idnt,
 channel_num,
 dept_idnt,
 class_frame,
 frame,
 supplier_frame
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curves_model_rec
WHERE LOWER(class_frame) <> LOWER('NA')
 AND half_idnt = (SELECT MIN(half_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
   WHERE last_completed_3_months = 1)
 AND store_num IS NOT NULL
GROUP BY half_idnt,
 channel_num,
 dept_idnt,
 class_frame,
 frame,
 supplier_frame;



CREATE TEMPORARY TABLE IF NOT EXISTS frames2
AS
SELECT half_idnt,
 channel_num,
 dept_idnt,
 frame,
 class_frame
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_curves_model_rec
WHERE half_idnt = (SELECT MIN(half_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_eval_cal_week_vw
   WHERE last_completed_3_months = 1)
 AND store_num IS NOT NULL
GROUP BY half_idnt,
 channel_num,
 dept_idnt,
 frame,
 class_frame;


CREATE TEMPORARY TABLE IF NOT EXISTS combine
AS
SELECT channel_num,
 store_num,
 ly_ty,
 npg_ind,
 dept_idnt,
 class_idnt,
 supplier_idnt,
 size_1_frame,
 size_1_rank,
 size_1_id,
 rp_ind,
 net_sales_u,
 ds_net_sales_u,
 reg_net_sales_u,
 pro_net_sales_u,
 clr_net_sales_u,
 net_sales_r,
 ds_net_sales_r,
 reg_net_sales_r,
 pro_net_sales_r,
 clr_net_sales_r,
 net_sales_c,
 ds_net_sales_c,
 reg_net_sales_c,
 pro_net_sales_c,
 clr_net_sales_c,
 returns_u,
 ds_returns_u,
 reg_returns_u,
 pro_returns_u,
 clr_returns_u,
 returns_r,
 ds_returns_r,
 reg_returns_r,
 pro_returns_r,
 clr_returns_r,
 returns_c,
 ds_returns_c,
 reg_returns_c,
 pro_returns_c,
 clr_returns_c,
 gross_sales_u,
 ds_gross_sales_u,
 reg_gross_sales_u,
 pro_gross_sales_u,
 clr_gross_sales_u,
 gross_sales_r,
 ds_gross_sales_r,
 reg_gross_sales_r,
 pro_gross_sales_r,
 clr_gross_sales_r,
 gross_sales_c,
 ds_gross_sales_c,
 reg_gross_sales_c,
 pro_gross_sales_c,
 clr_gross_sales_c,
 product_margin,
 ds_product_margin,
 reg_product_margin,
 pro_product_margin,
 clr_product_margin,
 eoh_u,
 reg_eoh_u,
 cl_eoh_u,
 eoh_r,
 reg_eoh_r,
 cl_eoh_r,
 eoh_c,
 reg_eoh_c,
 cl_eoh_c,
 receipts_u,
 receipts_c,
 receipts_r,
 ds_receipts_u,
 ds_receipts_c,
 ds_receipts_r,
 close_out_receipt_u,
 close_out_receipt_c,
 close_out_receipt_r,
 close_out_sized_receipt_u,
 close_out_sized_receipt_c,
 close_out_sized_receipt_r,
 casepack_receipt_u,
 casepack_receipt_c,
 casepack_receipt_r,
 prepack_receipt_u,
 prepack_receipt_c,
 prepack_receipt_r,
 sized_receipt_u,
 sized_receipt_c,
 sized_receipt_r,
 unknown_receipt_u,
 unknown_receipt_c,
 unknown_receipt_r,
 transfer_out_u,
 transfer_in_u,
 racking_last_chance_out_u,
 flx_in_u,
 stock_balance_out_u,
 stock_balance_in_u,
 reserve_stock_in_u,
 pah_in_u,
 cust_return_out_u,
 cust_return_in_u,
 cust_transfer_out_u,
 cust_transfer_in_u
FROM (SELECT *
   FROM inv_sales
   UNION ALL
   SELECT *
   FROM receipts_trans
   UNION ALL
   SELECT *
   FROM transfers_in_out) AS a;



CREATE TEMPORARY TABLE IF NOT EXISTS combine_2
AS
SELECT a.channel_num,
 a.store_num,
 a.ly_ty,
 a.npg_ind,
 a.dept_idnt,
 a.class_idnt,
 a.supplier_idnt,
 a.size_1_frame,
 a.size_1_rank,
 a.size_1_id,
 a.rp_ind,
 a.net_sales_u,
 a.ds_net_sales_u,
 a.reg_net_sales_u,
 a.pro_net_sales_u,
 a.clr_net_sales_u,
 a.net_sales_r,
 a.ds_net_sales_r,
 a.reg_net_sales_r,
 a.pro_net_sales_r,
 a.clr_net_sales_r,
 a.net_sales_c,
 a.ds_net_sales_c,
 a.reg_net_sales_c,
 a.pro_net_sales_c,
 a.clr_net_sales_c,
 a.returns_u,
 a.ds_returns_u,
 a.reg_returns_u,
 a.pro_returns_u,
 a.clr_returns_u,
 a.returns_r,
 a.ds_returns_r,
 a.reg_returns_r,
 a.pro_returns_r,
 a.clr_returns_r,
 a.returns_c,
 a.ds_returns_c,
 a.reg_returns_c,
 a.pro_returns_c,
 a.clr_returns_c,
 a.gross_sales_u,
 a.ds_gross_sales_u,
 a.reg_gross_sales_u,
 a.pro_gross_sales_u,
 a.clr_gross_sales_u,
 a.gross_sales_r,
 a.ds_gross_sales_r,
 a.reg_gross_sales_r,
 a.pro_gross_sales_r,
 a.clr_gross_sales_r,
 a.gross_sales_c,
 a.ds_gross_sales_c,
 a.reg_gross_sales_c,
 a.pro_gross_sales_c,
 a.clr_gross_sales_c,
 a.product_margin,
 a.ds_product_margin,
 a.reg_product_margin,
 a.pro_product_margin,
 a.clr_product_margin,
 a.eoh_u,
 a.reg_eoh_u,
 a.cl_eoh_u,
 a.eoh_r,
 a.reg_eoh_r,
 a.cl_eoh_r,
 a.eoh_c,
 a.reg_eoh_c,
 a.cl_eoh_c,
 a.receipts_u,
 a.receipts_c,
 a.receipts_r,
 a.ds_receipts_u,
 a.ds_receipts_c,
 a.ds_receipts_r,
 a.close_out_receipt_u,
 a.close_out_receipt_c,
 a.close_out_receipt_r,
 a.close_out_sized_receipt_u,
 a.close_out_sized_receipt_c,
 a.close_out_sized_receipt_r,
 a.casepack_receipt_u,
 a.casepack_receipt_c,
 a.casepack_receipt_r,
 a.prepack_receipt_u,
 a.prepack_receipt_c,
 a.prepack_receipt_r,
 a.sized_receipt_u,
 a.sized_receipt_c,
 a.sized_receipt_r,
 a.unknown_receipt_u,
 a.unknown_receipt_c,
 a.unknown_receipt_r,
 a.transfer_out_u,
 a.transfer_in_u,
 a.racking_last_chance_out_u,
 a.flx_in_u,
 a.stock_balance_out_u,
 a.stock_balance_in_u,
 a.reserve_stock_in_u,
 a.pah_in_u,
 a.cust_return_out_u,
 a.cust_return_in_u,
 a.cust_transfer_out_u,
 a.cust_transfer_in_u,
  CASE
  WHEN a.channel_num = 120
  THEN 808
  WHEN a.channel_num = 250
  THEN 828
  ELSE CAST(TRUNC(a.store_num) AS INTEGER)
  END AS store_number,
 COALESCE(f2.class_frame, f.class_frame, 'AllClasses') AS class_frame,
 COALESCE(f.supplier_frame, 'AllSuppliers') AS supplier_frame
FROM combine AS a
 LEFT JOIN frames AS f ON a.channel_num = f.channel_num AND a.dept_idnt = f.dept_idnt AND LOWER(TRIM(FORMAT('%11d', a.dept_idnt
            )) || '~~' || TRIM(FORMAT('%11d', a.class_idnt)) || '~~' || a.size_1_frame) = LOWER(f.class_frame) AND LOWER(a
     .size_1_frame) = LOWER(f.frame) AND LOWER(a.supplier_idnt) = LOWER(f.supplier_frame)
 LEFT JOIN frames2 AS f2 ON a.channel_num = f2.channel_num AND a.dept_idnt = f2.dept_idnt AND LOWER(TRIM(FORMAT('%11d',
           a.dept_idnt)) || '~~' || TRIM(FORMAT('%11d', a.class_idnt)) || '~~' || a.size_1_frame) = LOWER(f2.class_frame
     ) AND LOWER(a.size_1_frame) = LOWER(f2.frame);


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_actuals;


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_actuals
SELECT DISTINCT channel_num,
  store_number AS store_num,
  ly_ty,
   CASE
   WHEN LOWER(supplier_frame) = LOWER('AllSuppliers')
   THEN 'N'
   ELSE npg_ind
   END AS npg_ind,
  dept_idnt,
  class_frame,
  supplier_frame,
  size_1_frame,
  CAST(size_1_rank AS FLOAT64) AS size_1_rank,
  size_1_id,
  MAX(rp_ind) AS rp_ind,
  SUM(net_sales_u) AS net_sales_u,
  SUM(ds_net_sales_u) AS ds_net_sales_u,
  SUM(reg_net_sales_u) AS reg_net_sales_u,
  SUM(pro_net_sales_u) AS pro_net_sales_u,
  SUM(clr_net_sales_u) AS clr_net_sales_u,
  SUM(net_sales_r) AS net_sales_r,
  SUM(ds_net_sales_r) AS ds_net_sales_r,
  SUM(reg_net_sales_r) AS reg_net_sales_r,
  SUM(pro_net_sales_r) AS pro_net_sales_r,
  SUM(clr_net_sales_r) AS clr_net_sales_r,
  SUM(net_sales_c) AS net_sales_c,
  SUM(ds_net_sales_c) AS ds_net_sales_c,
  SUM(reg_net_sales_c) AS reg_net_sales_c,
  SUM(pro_net_sales_c) AS pro_net_sales_c,
  SUM(clr_net_sales_c) AS clr_net_sales_c,
  SUM(returns_u) AS returns_u,
  SUM(ds_returns_u) AS ds_returns_u,
  SUM(reg_returns_u) AS reg_returns_u,
  SUM(pro_returns_u) AS pro_returns_u,
  SUM(clr_returns_u) AS clr_returns_u,
  SUM(returns_r) AS returns_r,
  SUM(ds_returns_r) AS ds_returns_r,
  SUM(reg_returns_r) AS reg_returns_r,
  SUM(pro_returns_r) AS pro_returns_r,
  SUM(clr_returns_r) AS clr_returns_r,
  SUM(returns_c) AS returns_c,
  SUM(ds_returns_c) AS ds_returns_c,
  SUM(reg_returns_c) AS reg_returns_c,
  SUM(pro_returns_c) AS pro_returns_c,
  SUM(clr_returns_c) AS clr_returns_c,
  SUM(gross_sales_u) AS gross_sales_u,
  SUM(ds_gross_sales_u) AS ds_gross_sales_u,
  SUM(reg_gross_sales_u) AS reg_gross_sales_u,
  SUM(pro_gross_sales_u) AS pro_gross_sales_u,
  SUM(clr_gross_sales_u) AS clr_gross_sales_u,
  SUM(gross_sales_r) AS gross_sales_r,
  SUM(ds_gross_sales_r) AS ds_gross_sales_r,
  SUM(reg_gross_sales_r) AS reg_gross_sales_r,
  SUM(pro_gross_sales_r) AS pro_gross_sales_r,
  SUM(clr_gross_sales_r) AS clr_gross_sales_r,
  SUM(gross_sales_c) AS gross_sales_c,
  SUM(ds_gross_sales_c) AS ds_gross_sales_c,
  SUM(reg_gross_sales_c) AS reg_gross_sales_c,
  SUM(pro_gross_sales_c) AS pro_gross_sales_c,
  SUM(clr_gross_sales_c) AS clr_gross_sales_c,
  SUM(product_margin) AS product_margin,
  SUM(ds_product_margin) AS ds_product_margin,
  SUM(reg_product_margin) AS reg_product_margin,
  SUM(pro_product_margin) AS pro_product_margin,
  SUM(clr_product_margin) AS clr_product_margin,
  SUM(eoh_u) AS eoh_u,
  SUM(reg_eoh_u) AS reg_eoh_u,
  SUM(cl_eoh_u) AS cl_eoh_u,
  SUM(eoh_r) AS eoh_r,
  SUM(reg_eoh_r) AS reg_eoh_r,
  SUM(cl_eoh_r) AS cl_eoh_r,
  SUM(eoh_c) AS eoh_c,
  SUM(reg_eoh_c) AS reg_eoh_c,
  SUM(cl_eoh_c) AS cl_eoh_c,
  SUM(COALESCE(receipts_u, 0)) AS receipts_u,
  SUM(COALESCE(receipts_c, 0)) AS receipts_c,
  SUM(COALESCE(receipts_r, 0)) AS receipts_r,
  SUM(COALESCE(ds_receipts_u, 0)) AS ds_receipts_u,
  SUM(COALESCE(ds_receipts_c, 0)) AS ds_receipts_c,
  SUM(COALESCE(ds_receipts_r, 0)) AS ds_receipts_r,
  SUM(COALESCE(close_out_receipt_u, 0)) AS close_out_receipt_u,
  SUM(COALESCE(close_out_receipt_c, 0)) AS close_out_receipt_c,
  SUM(COALESCE(close_out_receipt_r, 0)) AS close_out_receipt_r,
  SUM(COALESCE(close_out_sized_receipt_u, 0)) AS close_out_sized_receipt_u,
  SUM(COALESCE(close_out_sized_receipt_c, 0)) AS close_out_sized_receipt_c,
  SUM(COALESCE(close_out_sized_receipt_r, 0)) AS close_out_sized_receipt_r,
  SUM(COALESCE(casepack_receipt_u, 0)) AS casepack_receipt_u,
  SUM(COALESCE(casepack_receipt_c, 0)) AS casepack_receipt_c,
  SUM(COALESCE(casepack_receipt_r, 0)) AS casepack_receipt_r,
  SUM(COALESCE(prepack_receipt_u, 0)) AS prepack_receipt_u,
  SUM(COALESCE(prepack_receipt_c, 0)) AS prepack_receipt_c,
  SUM(COALESCE(prepack_receipt_r, 0)) AS prepack_receipt_r,
  SUM(COALESCE(sized_receipt_u, 0)) AS sized_receipt_u,
  SUM(COALESCE(sized_receipt_c, 0)) AS sized_receipt_c,
  SUM(COALESCE(sized_receipt_r, 0)) AS sized_receipt_r,
  SUM(COALESCE(unknown_receipt_u, 0)) AS unknown_receipt_u,
  SUM(COALESCE(unknown_receipt_c, 0)) AS unknown_receipt_c,
  SUM(COALESCE(unknown_receipt_r, 0)) AS unknown_receipt_r,
  SUM(COALESCE(transfer_out_u, 0)) AS transfer_out_u,
  SUM(COALESCE(transfer_in_u, 0)) AS transfer_in_u,
  SUM(COALESCE(racking_last_chance_out_u, 0)) AS racking_last_chance_out_u,
  SUM(COALESCE(flx_in_u, 0)) AS flx_in_u,
  SUM(COALESCE(stock_balance_out_u, 0)) AS stock_balance_out_u,
  SUM(COALESCE(stock_balance_in_u, 0)) AS stock_balance_in_u,
  SUM(COALESCE(reserve_stock_in_u, 0)) AS reserve_stock_in_u,
  SUM(COALESCE(pah_in_u, 0)) AS pah_in_u,
  SUM(COALESCE(cust_return_out_u, 0)) AS cust_return_out_u,
  SUM(COALESCE(cust_return_in_u, 0)) AS cust_return_in_u,
  SUM(COALESCE(cust_transfer_out_u, 0)) AS cust_transfer_out_u,
  SUM(COALESCE(cust_transfer_in_u, 0)) AS cust_transfer_in_u,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS update_timestamp,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() AS update_timestamp_tz
 FROM combine_2 AS a
 GROUP BY channel_num,
  store_num,
  ly_ty,
  npg_ind,
  dept_idnt,
  class_frame,
  supplier_frame,
  size_1_frame,
  size_1_rank,
  size_1_id
 EXCEPT DISTINCT
 SELECT channel_num,
  store_num,
  ly_ty,
  npg_ind,
  dept_idnt,
  class_frame,
  supplier_frame,
  size_1_frame,
  size_1_rank,
  size_1_id,
  rp_ind,
  net_sales_u,
  ds_net_sales_u,
  reg_net_sales_u,
  pro_net_sales_u,
  clr_net_sales_u,
  net_sales_r,
  ds_net_sales_r,
  reg_net_sales_r,
  pro_net_sales_r,
  clr_net_sales_r,
  net_sales_c,
  ds_net_sales_c,
  reg_net_sales_c,
  pro_net_sales_c,
  clr_net_sales_c,
  returns_u,
  ds_returns_u,
  reg_returns_u,
  pro_returns_u,
  clr_returns_u,
  returns_r,
  ds_returns_r,
  reg_returns_r,
  pro_returns_r,
  clr_returns_r,
  returns_c,
  ds_returns_c,
  reg_returns_c,
  pro_returns_c,
  clr_returns_c,
  gross_sales_u,
  ds_gross_sales_u,
  reg_gross_sales_u,
  pro_gross_sales_u,
  clr_gross_sales_u,
  gross_sales_r,
  ds_gross_sales_r,
  reg_gross_sales_r,
  pro_gross_sales_r,
  clr_gross_sales_r,
  gross_sales_c,
  ds_gross_sales_c,
  reg_gross_sales_c,
  pro_gross_sales_c,
  clr_gross_sales_c,
  product_margin,
  ds_product_margin,
  reg_product_margin,
  pro_product_margin,
  clr_product_margin,
  eoh_u,
  reg_eoh_u,
  cl_eoh_u,
  eoh_r,
  reg_eoh_r,
  cl_eoh_r,
  eoh_c,
  reg_eoh_c,
  cl_eoh_c,
  receipts_u,
  receipts_c,
  receipts_r,
  ds_receipts_u,
  ds_receipts_c,
  ds_receipts_r,
  close_out_receipt_u,
  close_out_receipt_c,
  close_out_receipt_r,
  close_out_sized_receipt_u,
  close_out_sized_receipt_c,
  close_out_sized_receipt_r,
  casepack_receipt_u,
  casepack_receipt_c,
  casepack_receipt_r,
  prepack_receipt_u,
  prepack_receipt_c,
  prepack_receipt_r,
  sized_receipt_u,
  sized_receipt_c,
  sized_receipt_r,
  unknown_receipt_u,
  unknown_receipt_c,
  unknown_receipt_r,
  transfer_out_u,
  transfer_in_u,
  racking_last_chance_out_u,
  flx_in_u,
  stock_balance_out_u,
  stock_balance_in_u,
  reserve_stock_in_u,
  pah_in_u,
  cust_return_out_u,
  cust_return_in_u,
  cust_transfer_out_u,
  cust_transfer_in_u,
  update_timestamp,
  update_timestamp_tz
 FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.size_actuals;

