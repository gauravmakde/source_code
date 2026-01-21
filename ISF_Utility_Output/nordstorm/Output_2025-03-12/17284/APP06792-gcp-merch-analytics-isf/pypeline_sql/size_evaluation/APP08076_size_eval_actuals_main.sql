/*
Name: Size Curves Evaluation Actuals
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
    - size_actuals

Dependancies: 
    - size_eval_cal_week_vw
    - supp_size_hierarchy
    - size_curves_model_rec
*/

-- -- DROP TABLE sales_sku_base;
CREATE MULTISET VOLATILE TABLE sales_sku_base AS (
SELECT 
     channel_num AS channel_num
    ,store_num
    ,week_num AS week_idnt
    ,month_idnt
    ,CASE WHEN week_num = ly_week_num_realigned THEN 'LY' ELSE 'TY' END AS ly_ty
    ,half_idnt
    ,dropship_ind
    ,rp_ind
    ,npg_ind
    ,rms_sku_num 
    --net sales
      --unit
    ,s.net_sales_tot_units AS net_sales_u
    ,s.net_sales_tot_regular_units AS reg_net_sales_u
    ,s.net_sales_tot_promo_units AS pro_net_sales_u
    ,s.net_sales_tot_clearance_units AS clr_net_sales_u
      --retail
    ,s.net_sales_tot_retl AS net_sales_r
    ,s.net_sales_tot_regular_retl AS reg_net_sales_r
    ,s.net_sales_tot_promo_retl AS pro_net_sales_r
    ,s.net_sales_tot_clearance_retl AS clr_net_sales_r
      --cost
    ,s.net_sales_tot_cost AS net_sales_c
    ,s.net_sales_tot_regular_cost AS reg_net_sales_c
    ,s.net_sales_tot_promo_cost AS pro_net_sales_c
    ,s.net_sales_tot_clearance_cost AS clr_net_sales_c
    --returns
      --unit
    ,s.returns_tot_units AS returns_u
    ,s.returns_tot_regular_units AS reg_returns_u
    ,s.returns_tot_promo_units AS pro_returns_u
    ,s.returns_tot_clearance_units AS clr_returns_u
      --retail
    ,s.returns_tot_retl AS returns_r
    ,s.returns_tot_regular_retl AS reg_returns_r
    ,s.returns_tot_promo_retl AS pro_returns_r
    ,s.returns_tot_clearance_retl AS clr_returns_r
      --cost
    ,s.returns_tot_cost AS returns_c
    ,s.returns_tot_regular_cost AS reg_returns_c
    ,s.returns_tot_promo_cost AS pro_returns_c
    ,s.returns_tot_clearance_cost AS clr_returns_c
    --gross sales
      --unit
    ,s.gross_sales_tot_units AS gross_sales_u
    ,s.gross_sales_tot_regular_units AS reg_gross_sales_u
    ,s.gross_sales_tot_promo_units AS pro_gross_sales_u
    ,s.gross_sales_tot_clearance_units AS clr_gross_sales_u
      --retail
    ,s.gross_sales_tot_retl AS gross_sales_r
    ,s.gross_sales_tot_regular_retl AS reg_gross_sales_r
    ,s.gross_sales_tot_promo_retl AS pro_gross_sales_r
    ,s.gross_sales_tot_clearance_retl AS clr_gross_sales_r
      --cost
    ,s.gross_sales_tot_cost AS gross_sales_c
    ,s.gross_sales_tot_regular_cost AS reg_gross_sales_c
    ,s.gross_sales_tot_promo_cost AS pro_gross_sales_c
    ,s.gross_sales_tot_clearance_cost AS clr_gross_sales_c
FROM prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw s
JOIN {environment_schema}.size_eval_cal_week_vw w
  ON (w.week_idnt = s.week_num
  OR w.ly_week_num_realigned = s.week_num)
 AND w.last_completed_3_months = 1 
WHERE channel_num IN (110, 210, 250, 120)
  AND division_num IN (351, 345, 310)
  AND (week_num IN (SELECT week_idnt FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1 GROUP BY 1) 
   OR week_num IN (SELECT ly_week_num_realigned FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1 GROUP BY 1))
  AND (net_sales_tot_units > 0 
   OR returns_tot_units > 0 
   OR gross_sales_tot_units > 0)
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,week_idnt
    ,ly_ty
    ,month_idnt
    ,half_idnt
    ,dropship_ind
    ,rp_ind
    ,npg_ind
    ,rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN(channel_num, store_num, week_idnt, month_idnt, half_idnt, ly_ty, dropship_ind, rp_ind, npg_ind)
    ,COLUMN(channel_num, store_num, week_idnt, dropship_ind, rp_ind, npg_ind, rms_sku_num)
    ,COLUMN(rms_sku_num)
    ON sales_sku_base;


-- -- DROP TABLE sales_sku;

CREATE MULTISET VOLATILE TABLE sales_sku AS (
SELECT 
     channel_num
    ,store_num
    ,month_idnt
    ,half_idnt
    ,week_idnt
    ,ly_ty
    ,dropship_ind
    ,rp_ind
    ,s.npg_ind
    ,rms_sku_num
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    
    ,s.net_sales_u AS net_sales_u
    ,s.reg_net_sales_u AS reg_net_sales_u
    ,s.pro_net_sales_u AS pro_net_sales_u
    ,s.clr_net_sales_u AS clr_net_sales_u
      --retail
    ,s.net_sales_r AS net_sales_r
    ,s.reg_net_sales_r AS reg_net_sales_r
    ,s.pro_net_sales_r AS pro_net_sales_r
    ,s.clr_net_sales_r AS clr_net_sales_r
      --cost
    ,s.net_sales_c AS net_sales_c
    ,s.reg_net_sales_c AS reg_net_sales_c
    ,s.pro_net_sales_c AS pro_net_sales_c
    ,s.clr_net_sales_c AS clr_net_sales_c
    --returns
      --unit
    ,s.returns_u AS returns_u
    ,s.reg_returns_u AS reg_returns_u
    ,s.pro_returns_u AS pro_returns_u
    ,s.clr_returns_u AS clr_returns_u
      --retail
    ,s.returns_r AS returns_r
    ,s.reg_returns_r AS reg_returns_r
    ,s.pro_returns_r AS pro_returns_r
    ,s.clr_returns_r AS clr_returns_r
      --cost
    ,s.returns_c AS returns_c
    ,s.reg_returns_c AS reg_returns_c
    ,s.pro_returns_c AS pro_returns_c
    ,s.clr_returns_c AS clr_returns_c
    --gross sales
      --unit
    ,s.gross_sales_u AS gross_sales_u
    ,s.reg_gross_sales_u AS reg_gross_sales_u
    ,s.pro_gross_sales_u AS pro_gross_sales_u
    ,s.clr_gross_sales_u AS clr_gross_sales_u
      --retail
    ,s.gross_sales_r AS gross_sales_r
    ,s.reg_gross_sales_r AS reg_gross_sales_r
    ,s.pro_gross_sales_r AS pro_gross_sales_r
    ,s.clr_gross_sales_r AS clr_gross_sales_r
      --cost
    ,s.gross_sales_c AS gross_sales_c
    ,s.reg_gross_sales_c AS reg_gross_sales_c
    ,s.pro_gross_sales_c AS pro_gross_sales_c
    ,s.clr_gross_sales_c AS clr_gross_sales_c
FROM sales_sku_base s
JOIN {environment_schema}.supp_size_hierarchy h
  ON s.rms_sku_num = h.sku_idnt 
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,month_idnt
    ,half_idnt
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS
;
    
COLLECT STATISTICS 
     COLUMN(channel_num, store_num, month_idnt, half_idnt, ly_ty, npg_ind, dept_idnt, class_idnt, supplier_idnt, size_1_frame, size_1_rank, size_1_id) 
    ,COLUMN(class_idnt)
    ,COLUMN(dept_idnt)
    ,COLUMN(month_idnt)
    ,COLUMN(half_idnt)
    ,COLUMN(store_num)
    ,COLUMN(channel_num) 
    ,COLUMN(rp_ind)
    ,COLUMN(dropship_ind)
    ,COLUMN(size_1_rank)
    ON sales_sku;

-- -- DROP TABLE sales_size;

CREATE MULTISET VOLATILE TABLE sales_size AS (
SELECT 
     channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    ,MAX(rp_ind) AS rp_ind
    ,SUM(s.net_sales_u) AS net_sales_u
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.net_sales_u ELSE 0 END) AS ds_net_sales_u
    ,SUM(s.reg_net_sales_u) AS reg_net_sales_u
    ,SUM(s.pro_net_sales_u) AS pro_net_sales_u
    ,SUM(s.clr_net_sales_u) AS clr_net_sales_u
      --retail
    ,SUM(s.net_sales_r) AS net_sales_r
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.net_sales_r ELSE 0 END) AS ds_net_sales_r
    ,SUM(s.reg_net_sales_r) AS reg_net_sales_r
    ,SUM(s.pro_net_sales_r) AS pro_net_sales_r
    ,SUM(s.clr_net_sales_r) AS clr_net_sales_r
      --cost
    ,SUM(s.net_sales_c) AS net_sales_c
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.net_sales_c ELSE 0 END) AS ds_net_sales_c
    ,SUM(s.reg_net_sales_c) AS reg_net_sales_c
    ,SUM(s.pro_net_sales_c) AS pro_net_sales_c
    ,SUM(s.clr_net_sales_c) AS clr_net_sales_c
    --returns
      --unit
    ,SUM(s.returns_u) AS returns_u
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.returns_u ELSE 0 END) AS ds_returns_u
    ,SUM(s.reg_returns_u) AS reg_returns_u
    ,SUM(s.pro_returns_u) AS pro_returns_u
    ,SUM(s.clr_returns_u) AS clr_returns_u
      --retail
    ,SUM(s.returns_r) AS returns_r
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.returns_r ELSE 0 END) AS ds_returns_r
    ,SUM(s.reg_returns_r) AS reg_returns_r
    ,SUM(s.pro_returns_r) AS pro_returns_r
    ,SUM(s.clr_returns_r) AS clr_returns_r
      --cost
    ,SUM(s.returns_c) AS returns_c
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.returns_c ELSE 0 END) AS ds_returns_c
    ,SUM(s.reg_returns_c) AS reg_returns_c
    ,SUM(s.pro_returns_c) AS pro_returns_c
    ,SUM(s.clr_returns_c) AS clr_returns_c
    --gross sales
      --unit
    ,SUM(s.gross_sales_u) AS gross_sales_u
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.gross_sales_u ELSE 0 END) AS ds_gross_sales_u
    ,SUM(s.reg_gross_sales_u) AS reg_gross_sales_u
    ,SUM(s.pro_gross_sales_u) AS pro_gross_sales_u
    ,SUM(s.clr_gross_sales_u) AS clr_gross_sales_u
      --retail
    ,SUM(s.gross_sales_r) AS gross_sales_r
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.gross_sales_r ELSE 0 END) AS ds_gross_sales_r
    ,SUM(s.reg_gross_sales_r) AS reg_gross_sales_r
    ,SUM(s.pro_gross_sales_r) AS pro_gross_sales_r
    ,SUM(s.clr_gross_sales_r) AS clr_gross_sales_r
      --cost
    ,SUM(s.gross_sales_c) AS gross_sales_c
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.gross_sales_c ELSE 0 END) AS ds_gross_sales_c
    ,SUM(s.reg_gross_sales_c) AS reg_gross_sales_c
    ,SUM(s.pro_gross_sales_c) AS pro_gross_sales_c
    ,SUM(s.clr_gross_sales_c) AS clr_gross_sales_c
    --product margin
    ,SUM(s.net_sales_r) - SUM(s.net_sales_c) AS product_margin
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN s.net_sales_r ELSE 0 END) - SUM(CASE WHEN dropship_ind = 'Y' THEN s.net_sales_c ELSE 0 END) AS ds_product_margin
    ,SUM(reg_net_sales_r) - SUM(reg_net_sales_c) AS reg_product_margin
    ,SUM(pro_net_sales_r) - SUM(pro_net_sales_c )AS pro_product_margin    
    ,SUM(clr_net_sales_r) - SUM(clr_net_sales_c) AS clr_product_margin   
    -- Inventory
    ,CAST(0 AS INTEGER) AS eoh_u
    ,CAST(0 AS INTEGER) AS reg_eoh_u
    ,CAST(0 AS INTEGER) AS cl_eoh_u
    ,CAST(0 AS DECIMAL(38,4)) AS eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS cl_eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS eoh_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_eoh_c
    ,CAST(0 AS DECIMAL(38,4)) AS cl_eoh_c
FROM sales_sku s
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN(store_num) 
    ,COLUMN(channel_num ,store_num, ly_ty, npg_ind ,dept_idnt ,class_idnt, supplier_idnt ,size_1_frame, size_1_rank, size_1_id) 
    ON sales_size;

-- -- DROP TABLE inv_sku_base;
CREATE MULTISET VOLATILE TABLE inv_sku_base AS (
SELECT 
     channel_num AS channel_num
    ,store_num
    ,week_num AS week_idnt
    ,month_idnt
    ,half_idnt
    ,CASE WHEN week_num = ly_week_num_realigned THEN 'LY' ELSE 'TY' END AS ly_ty
    ,rp_ind
    ,npg_ind
    ,rms_sku_num 
    ,eoh_total_units
    ,eoh_regular_units
    ,eoh_clearance_units
    ,eoh_total_retail
    ,eoh_regular_retail
    ,eoh_clearance_retail
    ,eoh_total_cost
    ,eoh_regular_cost
    ,eoh_clearance_cost
FROM prd_nap_usr_vws.merch_inventory_sku_store_week_fact_vw i
JOIN {environment_schema}.size_eval_cal_week_vw w
  ON (w.week_idnt = i.week_num
  OR w.ly_week_num_realigned = i.week_num)
 AND w.last_completed_3_months = 1 
WHERE channel_num IN (110, 210, 250, 120)
  AND division_num IN (351, 345, 310)
  AND (week_num IN (SELECT MAX(week_idnt) FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1) 
   OR week_num IN (SELECT MAX(ly_week_num_realigned) FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1))
  AND eoh_total_units > 0
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,week_idnt
    ,half_idnt
    ,month_idnt
    ,ly_ty
    ,npg_ind
    ,rms_sku_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     COLUMN(rms_sku_num)
    ,COLUMN(store_num)
    ,COLUMN(channel_num, store_num, week_idnt, month_idnt, half_idnt, ly_ty, npg_ind)
     ON inv_sku_base;

-- -- DROP TABLE inv_sku;

CREATE MULTISET VOLATILE TABLE inv_sku AS (
SELECT 
     channel_num
    ,CAST(store_num AS INTEGER) AS store_num
    ,week_idnt
    ,month_idnt
    ,half_idnt
    ,ly_ty
    ,rp_ind
    ,s.npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    ,eoh_total_units
    ,eoh_regular_units
    ,eoh_clearance_units
    ,eoh_total_retail
    ,eoh_regular_retail
    ,eoh_clearance_retail
    ,eoh_total_cost
    ,eoh_regular_cost
    ,eoh_clearance_cost
FROM inv_sku_base s
JOIN {environment_schema}.supp_size_hierarchy h
  ON s.rms_sku_num = h.sku_idnt 
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,month_idnt
    ,half_idnt
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS
; 

COLLECT STATISTICS 
     COLUMN(channel_num ,store_num ,month_idnt, half_idnt, ly_ty, npg_ind ,dept_idnt ,class_idnt, supplier_idnt ,size_1_frame, size_1_rank,size_1_id )
    ,COLUMN(class_idnt)
    ,COLUMN(dept_idnt)
    ,COLUMN(month_idnt)
    ,COLUMN(half_idnt)
    ,COLUMN(store_num)
    ,COLUMN(channel_num) 
    ,COLUMN(rp_ind)
    ,COLUMN(size_1_rank)
    ON inv_sku;


-- DROP TABLE inv_size;

CREATE MULTISET VOLATILE TABLE inv_size AS (
SELECT 
     channel_num
    ,store_num
    ,ly_ty  
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    ,MAX(rp_ind) AS rp_ind
    ,CAST(0 AS INTEGER) AS net_sales_u
    ,CAST(0 AS INTEGER) AS ds_net_sales_u
    ,CAST(0 AS INTEGER) AS reg_net_sales_u
    ,CAST(0 AS INTEGER) AS pro_net_sales_u
    ,CAST(0 AS INTEGER) AS clr_net_sales_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_net_sales_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_net_sales_c
    --returns
      --unit
    ,CAST(0 AS INTEGER) AS returns_u
    ,CAST(0 AS INTEGER) AS ds_returns_u
    ,CAST(0 AS INTEGER) AS reg_returns_u
    ,CAST(0 AS INTEGER) AS pro_returns_u
    ,CAST(0 AS INTEGER) AS clr_returns_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_returns_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_returns_c
    --gross sales
      --unit
    ,CAST(0 AS INTEGER) AS gross_sales_u
    ,CAST(0 AS INTEGER) AS ds_gross_sales_u
    ,CAST(0 AS INTEGER) AS reg_gross_sales_u
    ,CAST(0 AS INTEGER) AS pro_gross_sales_u
    ,CAST(0 AS INTEGER) AS clr_gross_sales_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_gross_sales_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_gross_sales_c
    --product margin
    ,CAST(0 AS DECIMAL(38,4)) AS product_margin
    ,CAST(0 AS DECIMAL(38,4)) AS ds_product_margin
    ,CAST(0 AS DECIMAL(38,4)) AS reg_product_margin
    ,CAST(0 AS DECIMAL(38,4))AS pro_product_margin    
    ,CAST(0 AS DECIMAL(38,4)) AS clr_product_margin  
    -- inventory 
    ,SUM(eoh_total_units) AS eoh_u
    ,SUM(eoh_regular_units) AS reg_eoh_u
    ,SUM(eoh_clearance_units) AS cl_eoh_u
    ,SUM(eoh_total_retail) AS eoh_r
    ,SUM(eoh_regular_retail) AS reg_eoh_r
    ,SUM(eoh_clearance_retail) AS cl_eoh_r
    ,SUM(eoh_total_cost) AS eoh_c
    ,SUM(eoh_regular_cost) AS reg_eoh_c
    ,SUM(eoh_clearance_cost) AS cl_eoh_c
FROM inv_sku
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN(store_num)
    ,COLUMN(channel_num, store_num, ly_ty, npg_ind ,dept_idnt ,class_idnt, supplier_idnt,size_1_frame, size_1_rank, size_1_id) 
    ON inv_size; 
    

-- -- DROP TABLE actuals_size;
CREATE MULTISET VOLATILE TABLE actuals_size AS (
SELECT 
     *
FROM sales_size s

UNION ALL 

SELECT 
     * 
FROM inv_size i
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN(channel_num ,store_num, ly_ty, npg_ind ,dept_idnt ,class_idnt,supplier_idnt ,size_1_frame, size_1_rank, size_1_id)
    ,COLUMN(class_idnt)
    ,COLUMN(dept_idnt)
    ,COLUMN(ly_ty)
    ,COLUMN(store_num) 
    ,COLUMN(channel_num) 
    ON actuals_size;

-- -- DROP TABLE inv_sales;
CREATE MULTISET VOLATILE TABLE inv_sales AS (
SELECT
     channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    ,MAX(rp_ind) AS rp_ind
    -- Net Sales
      -- units
    ,SUM(COALESCE(net_sales_u, 0)) AS net_sales_u
    ,SUM(COALESCE(ds_net_sales_u, 0)) AS ds_net_sales_u
    ,SUM(COALESCE(reg_net_sales_u, 0)) AS reg_net_sales_u
    ,SUM(COALESCE(pro_net_sales_u, 0)) AS pro_net_sales_u
    ,SUM(COALESCE(clr_net_sales_u, 0)) AS clr_net_sales_u
      --retail
    ,SUM(COALESCE(net_sales_r, 0)) AS net_sales_r
    ,SUM(COALESCE(ds_net_sales_r, 0)) AS ds_net_sales_r
    ,SUM(COALESCE(reg_net_sales_r, 0)) AS reg_net_sales_r
    ,SUM(COALESCE(pro_net_sales_r, 0)) AS pro_net_sales_r
    ,SUM(COALESCE(clr_net_sales_r, 0)) AS clr_net_sales_r
      --cost
    ,SUM(COALESCE(net_sales_c, 0)) AS net_sales_c
    ,SUM(COALESCE(ds_net_sales_c, 0)) AS ds_net_sales_c
    ,SUM(COALESCE(reg_net_sales_c, 0)) AS reg_net_sales_c
    ,SUM(COALESCE(pro_net_sales_c, 0)) AS pro_net_sales_c
    ,SUM(COALESCE(clr_net_sales_c, 0)) AS clr_net_sales_c
    --returns
      --unit
    ,SUM(COALESCE(returns_u, 0)) AS returns_u
    ,SUM(COALESCE(ds_returns_u, 0)) AS ds_returns_u
    ,SUM(COALESCE(reg_returns_u, 0)) AS reg_returns_u
    ,SUM(COALESCE(pro_returns_u, 0)) AS pro_returns_u
    ,SUM(COALESCE(clr_returns_u, 0)) AS clr_returns_u
      --retail
    ,SUM(COALESCE(returns_r, 0)) AS returns_r
    ,SUM(COALESCE(ds_returns_r, 0)) AS ds_returns_r
    ,SUM(COALESCE(reg_returns_r, 0)) AS reg_returns_r
    ,SUM(COALESCE(pro_returns_r, 0)) AS pro_returns_r
    ,SUM(COALESCE(clr_returns_r, 0)) AS clr_returns_r
      --cost
    ,SUM(COALESCE(returns_c, 0)) AS returns_c
    ,SUM(COALESCE(ds_returns_c, 0)) AS ds_returns_c
    ,SUM(COALESCE(reg_returns_c, 0)) AS reg_returns_c
    ,SUM(COALESCE(pro_returns_c, 0)) AS pro_returns_c
    ,SUM(COALESCE(clr_returns_c, 0)) AS clr_returns_c
    --gross sales
      --unit
    ,SUM(COALESCE(gross_sales_u, 0)) AS gross_sales_u
    ,SUM(COALESCE(ds_gross_sales_u, 0)) AS ds_gross_sales_u
    ,SUM(COALESCE(reg_gross_sales_u, 0)) AS reg_gross_sales_u
    ,SUM(COALESCE(pro_gross_sales_u, 0)) AS pro_gross_sales_u
    ,SUM(COALESCE(clr_gross_sales_u, 0)) AS clr_gross_sales_u
      --retail
    ,SUM(COALESCE(gross_sales_r, 0)) AS gross_sales_r
    ,SUM(COALESCE(ds_gross_sales_r, 0)) AS ds_gross_sales_r
    ,SUM(COALESCE(reg_gross_sales_r, 0)) AS reg_gross_sales_r
    ,SUM(COALESCE(pro_gross_sales_r, 0)) AS pro_gross_sales_r
    ,SUM(COALESCE(clr_gross_sales_r, 0)) AS clr_gross_sales_r
      --cost
    ,SUM(COALESCE(gross_sales_c, 0)) AS gross_sales_c
    ,SUM(COALESCE(ds_gross_sales_c, 0)) AS ds_gross_sales_c
    ,SUM(COALESCE(reg_gross_sales_c, 0)) AS reg_gross_sales_c
    ,SUM(COALESCE(pro_gross_sales_c, 0)) AS pro_gross_sales_c
    ,SUM(COALESCE(clr_gross_sales_c, 0)) AS clr_gross_sales_c
    --product margin
    ,SUM(COALESCE(product_margin, 0)) AS product_margin
    ,SUM(COALESCE(ds_product_margin, 0)) AS ds_product_margin
    ,SUM(COALESCE(reg_product_margin, 0)) AS reg_product_margin
    ,SUM(COALESCE(pro_product_margin, 0))AS pro_product_margin    
    ,SUM(COALESCE(clr_product_margin, 0)) AS clr_product_margin
    -- inventory
    ,SUM(COALESCE(eoh_u, 0)) AS eoh_u
    ,SUM(COALESCE(reg_eoh_u, 0)) AS reg_eoh_u
    ,SUM(COALESCE(cl_eoh_u, 0)) AS cl_eoh_u
    ,SUM(COALESCE(eoh_r, 0)) AS eoh_r
    ,SUM(COALESCE(reg_eoh_r, 0)) AS reg_eoh_r
    ,SUM(COALESCE(cl_eoh_r, 0)) AS cl_eoh_r
    ,SUM(COALESCE(eoh_c, 0)) AS eoh_c
    ,SUM(COALESCE(reg_eoh_c, 0)) AS reg_eoh_c
    ,SUM(COALESCE(cl_eoh_c, 0)) AS cl_eoh_c
    -- receipts
    ,CAST(0 AS INTEGER) AS receipts_u
    ,CAST(0 AS DECIMAL(38,4)) AS receipts_c
    ,CAST(0 AS DECIMAL(38,4)) AS receipts_r
    -- receipts
    ,CAST(0 AS INTEGER) AS ds_receipts_u
    ,CAST(0 AS DECIMAL(38,4)) AS ds_receipts_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_receipts_r
    -- close outs
    ,CAST(0 AS INTEGER) AS close_out_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS close_out_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS close_out_receipt_r
    -- close outs
    ,CAST(0 AS INTEGER) AS close_out_sized_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS close_out_sized_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS close_out_sized_receipt_r
    -- casepacks
    ,CAST(0 AS INTEGER) AS casepack_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS casepack_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS casepack_receipt_r
    -- prepacks
    ,CAST(0 AS INTEGER) AS prepack_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS prepack_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS prepack_receipt_r
    -- Item
    ,CAST(0 AS INTEGER) AS sized_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS sized_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS sized_receipt_r
    -- unknown
    ,CAST(0 AS INTEGER) AS unknown_receipt_u
    ,CAST(0 AS DECIMAL(38,4))AS unknown_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS unknown_receipt_r
    -- transfers 
    ,CAST(0 AS INTEGER)AS transfer_out_u
    ,CAST(0 AS INTEGER) AS transfer_in_u
    ,CAST(0 AS INTEGER) AS racking_last_chance_out_u
    ,CAST(0 AS INTEGER) AS flx_in_u
    ,CAST(0 AS INTEGER) AS stock_balance_out_u
    ,CAST(0 AS INTEGER)AS stock_balance_in_u
    ,CAST(0 AS INTEGER) AS reserve_stock_in_u
    ,CAST(0 AS INTEGER) AS pah_in_u
    ,CAST(0 AS INTEGER) AS cust_return_out_u
    ,CAST(0 AS INTEGER) AS cust_return_in_u
    ,CAST(0 AS INTEGER) AS cust_transfer_out_u
    ,CAST(0 AS INTEGER) AS cust_transfer_in_u
FROM actuals_size a
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     COLUMN(channel_num, store_num, ly_ty, npg_ind, dept_idnt, class_idnt, supplier_idnt, size_1_frame, size_1_rank, size_1_id)
    ON inv_sales;


-- receipts;

-- -- -- DROP TABLE po_base;
CREATE MULTISET VOLATILE TABLE po_base AS (
WITH allocation AS (
    SELECT DISTINCT 
         po_fct.operation_id AS purchase_order_number
        ,purchase_order_type
        ,item_type
    FROM prd_nap_usr_vws.allocation_item_fact_vw po_fct
    WHERE po_end_ship_date >= '2023-01-01'
      AND operation_id IS NOT NULL
      AND operation_type = 'PURCHASE_ORDER'
    QUALIFY dense_rank() OVER (PARTITION BY operation_id ORDER BY source_system) = 1
) 
SELECT 
     po_fct.purchase_order_num
	,po_fct.status
	,po_fct.order_type
	,CASE WHEN po_hdr.order_type IN ('AUTOMATIC_REORDER', 'BUYER_REORDER') THEN 'Y' ELSE 'N' END AS rp_ind
	,COALESCE(po_fct.purchaseorder_type, po_hdr.po_type, a.purchase_order_type) AS purchase_order_type
	,po_hdr.internal_po_ind
	,po_hdr.npg_ind
	,po_fct.dropship_ind
	,merchandise_source
	,item_type
FROM prd_nap_usr_vws.purchase_order_fact po_fct
LEFT JOIN prd_nap_usr_vws.purchase_order_header_fact po_hdr
  ON po_fct.purchase_order_num = po_hdr.purchase_order_number
LEFT JOIN allocation a
  ON po_fct.purchase_order_num = a.purchase_order_number
JOIN (
        SELECT DISTINCT 
             dept_idnt 
        FROM {environment_schema}.supp_size_hierarchy
    ) d
  ON po_fct.department_id = d.dept_idnt
WHERE po_fct.open_to_buy_endofweek_date >= '2023-01-01'
) WITH DATA
PRIMARY INDEX(purchase_order_num)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN(purchase_order_type ,item_type) 
    ,COLUMN(purchase_order_type)
    ,COLUMN(purchase_order_num) 
     ON po_base;


-- -- -- DROP TABLE receipts;
CREATE MULTISET VOLATILE TABLE receipts AS (
SELECT 
     CASE WHEN w.ly_week_num_realigned = cal.week_idnt THEN 'LY' ELSE 'TY' END AS ly_ty
    ,rcpt.store_num AS store_num
    ,st.channel_num
    ,rcpt.dropship_ind
    ,rcpt.rp_ind AS rp_ind
    ,h.npg_ind
    ,h.dept_idnt
    ,h.class_idnt
    ,h.supplier_idnt
    ,h.size_1_frame
    ,h.size_1_rank
    ,h.size_1_id
    ,CASE WHEN pot.po_type_code IS NULL AND rcpt.dropship_ind = 'Y' THEN 'DR'
          WHEN pot.po_type_code IS NULL AND rcpt.rp_ind = 'Y' THEN 'RP'
          WHEN po.purchase_order_type IS NOT NULL THEN po.purchase_order_type 
          ELSE 'NONE' END AS po_type_code
    ,CASE WHEN pot.po_type_code IS NULL AND rcpt.dropship_ind = 'Y' THEN 'Dropship'
          WHEN pot.po_type_code IS NULL AND rcpt.rp_ind = 'Y' THEN 'Replenishment'
          WHEN pot.po_type_desc IS NOT NULL THEN pot.po_type_desc
          ELSE 'NONE' END AS po_type_desc
    ,CASE WHEN po_type_code IN ('HF', 'HL', 'PM', 'RS', 'RY', 'RZ', 'DN', 'OL', 'CN', 'CO', 'NQ', 'TS', 'DB', 'DS', 'BK', 'IN', 'PU', 'XR') THEN 'N'
          WHEN pot.po_type_code IS NULL AND rcpt.dropship_ind = 'Y' THEN 'N'
          WHEN pot.po_type_code IS NULL AND rcpt.rp_ind = 'Y' THEN 'N'
          WHEN item_type IN ('CASEPACK', 'PREPACK') THEN 'N' 
          ELSE 'Y' END AS jda
    ,CASE WHEN merchandise_source = 'CLOSE OUT' OR purchase_order_type = 'GE' THEN 'Y' ELSE 'N' END AS close_out
    ,CASE WHEN item_type IS NULL AND rcpt.dropship_ind = 'Y' THEN 'DS'
          WHEN item_type IS NULL AND rcpt.rp_ind = 'Y' THEN 'RP'
          WHEN item_type IS NOT NULL THEN item_type
          ELSE 'UNKNOWN' END AS item_type
	,SUM(receipts_units + receipts_crossdock_units) AS receipts_u
    ,SUM(receipts_cost + receipts_crossdock_cost) AS receipts_c
    ,SUM(receipts_retail + receipts_crossdock_retail) AS receipts_r
FROM prd_nap_usr_vws.merch_poreceipt_sku_store_fact_vw rcpt
JOIN {environment_schema}.size_eval_cal cal
  ON rcpt.tran_date = cal.day_date
 AND cal.hist_ind = 1
JOIN prd_nap_usr_vws.price_store_dim_vw st
  ON rcpt.store_num = st.store_num
JOIN po_base po
  ON rcpt.poreceipt_order_number = po.purchase_order_num
JOIN {environment_schema}.size_eval_cal_week_vw w
  ON (w.week_idnt = cal.week_idnt
  OR w.ly_week_num_realigned = cal.week_idnt)
 AND w.last_completed_3_months = 1 
LEFT JOIN (
            SELECT DISTINCT 
                 po_type_code
                ,po_type_desc 
            FROM {environment_schema}.po_types
    ) pot
  ON po.purchase_order_type = pot.po_type_code
JOIN {environment_schema}.supp_size_hierarchy h
  ON h.sku_idnt = rcpt.sku_num
WHERE (cal.week_idnt IN (SELECT week_idnt FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1 GROUP BY 1) 
   OR cal.week_idnt IN (SELECT ly_week_num_realigned FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1 GROUP BY 1))
  AND tran_code = '20'
  AND channel_num IN (110, 210, 250, 120)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,ly_ty
    ,dropship_ind
    ,rp_ind
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN(ly_ty, store_num, channel_num, dropship_ind, rp_ind, npg_ind, dept_idnt, class_idnt, supplier_idnt, size_1_frame, size_1_rank, size_1_id, po_type_code, po_type_desc, jda, close_out, item_type) 
    ,COLUMN(ly_ty, store_num, channel_num, npg_ind, dept_idnt, class_idnt, supplier_idnt, size_1_frame, size_1_rank, size_1_id) 
     ON receipts;

-- -- -- DROP TABLE trans_base;
CREATE MULTISET VOLATILE TABLE trans_base AS (
SELECT
     a.purchase_order_num
    ,rcpt.sku_num AS sku_num
    ,rcpt.store_num AS store_num
    ,st.channel_num
    ,cal.week_idnt
    ,rcpt.dropship_ind
    ,rcpt.rp_ind AS rp_ind
    ,SUM(receipts_units + receipts_crossdock_units) AS receipts_u
    ,SUM(receipts_cost + receipts_crossdock_cost) AS receipts_c
    ,SUM(receipts_retail + receipts_crossdock_retail) AS receipts_r
FROM prd_nap_usr_vws.MERCH_PORECEIPT_SKU_STORE_FACT_VW rcpt
JOIN PRD_NAP_USR_VWS.PURCHASE_ORDER_ITEM_DISTRIBUTELOCATION_FACT a
  ON cast(a.external_distribution_id AS VARCHAR(100)) = rcpt.poreceipt_order_number
 AND a.rms_sku_num = rcpt.sku_num
 AND a.distribute_location_id = rcpt.store_num
JOIN {environment_schema}.size_eval_cal cal
  ON rcpt.tran_date = cal.day_date
 AND cal.hist_ind = 1
JOIN {environment_schema}.supp_size_hierarchy h
  ON h.sku_idnt = rcpt.sku_num
JOIN prd_nap_usr_vws.price_store_dim_vw st
  ON rcpt.store_num = st.store_num
WHERE rcpt.tran_code = '30'
  AND channel_num IN (110, 210, 250, 120)
GROUP BY 1,2,3,4,5,6,7
) WITH DATA
PRIMARY INDEX(
     purchase_order_num
    ,channel_num
    ,store_num
    ,week_idnt
    ,sku_num
    ,dropship_ind
    ,rp_ind
    )
ON COMMIT PRESERVE ROWS
;


COLLECT STATISTICS 
     COLUMN(store_num, channel_num, dropship_ind, rp_ind)
    ,COLUMN(purchase_order_num)
    ,COLUMN(purchase_order_num, sku_num, store_num, channel_num, week_idnt, dropship_ind, rp_ind) 
    ,COLUMN(channel_num)
    ,COLUMN(week_idnt)
    ON trans_base;


-- -- -- DROP TABLE trans;
CREATE MULTISET VOLATILE TABLE trans AS (
SELECT 
     CASE WHEN w.ly_week_num_realigned = t.week_idnt THEN 'LY' ELSE 'TY' END AS ly_ty
    ,t.store_num AS store_num
    ,t.channel_num
    ,t.dropship_ind
    ,t.rp_ind 
    ,h.npg_ind
    ,h.dept_idnt
    ,h.class_idnt
    ,h.supplier_idnt
    ,h.size_1_frame
    ,h.size_1_rank
    ,h.size_1_id
    ,CASE WHEN pot.po_type_code IS NULL AND t.dropship_ind = 'Y' THEN 'DR'
          WHEN pot.po_type_code IS NULL AND t.rp_ind = 'Y' THEN 'RP'
          WHEN po.purchase_order_type IS NOT NULL THEN po.purchase_order_type 
          ELSE 'NONE' END AS po_type_code
    ,CASE WHEN pot.po_type_code IS NULL AND t.dropship_ind = 'Y' THEN 'Dropship'
          WHEN pot.po_type_code IS NULL AND t.rp_ind = 'Y' THEN 'Replenishment'
          WHEN pot.po_type_desc IS NOT NULL THEN pot.po_type_desc
          ELSE 'NONE' END AS po_type_desc
    ,CASE WHEN po_type_code IN ('HF', 'HL', 'PM', 'RS', 'RY', 'RZ', 'DN', 'OL', 'CN', 'CO', 'NQ', 'TS', 'DB', 'DS', 'BK', 'IN', 'PU', 'XR') THEN 'N'
          WHEN pot.po_type_code IS NULL AND t.dropship_ind = 'Y' THEN 'N'
          WHEN pot.po_type_code IS NULL AND t.rp_ind = 'Y' THEN 'N'
          WHEN item_type IN ('CASEPACK', 'PREPACK') THEN 'N' 
          ELSE 'Y' END AS jda
    ,CASE WHEN merchandise_source = 'CLOSE OUT' OR purchase_order_type = 'GE' THEN 'Y' ELSE 'N' END AS close_out
    ,CASE WHEN item_type IS NULL AND t.dropship_ind = 'Y' THEN 'DS'
          WHEN item_type IS NULL AND t.rp_ind = 'Y' THEN 'RP'
          WHEN item_type IS NOT NULL THEN item_type
          ELSE 'UNKNOWN' END AS item_type
	,SUM(receipts_u) AS receipts_u
    ,SUM(receipts_c) AS receipts_c
    ,SUM(receipts_r) AS receipts_r
FROM trans_base t
JOIN po_base po
  ON t.purchase_order_num = po.purchase_order_num
JOIN {environment_schema}.size_eval_cal_week_vw w
  ON (w.week_idnt = t.week_idnt
  OR w.ly_week_num_realigned = t.week_idnt)
 AND w.last_completed_3_months = 1 
LEFT JOIN (
            SELECT DISTINCT 
                 po_type_code
                ,po_type_desc 
            FROM {environment_schema}.po_types
    ) pot
  ON po.purchase_order_type = pot.po_type_code
JOIN {environment_schema}.supp_size_hierarchy h
  ON h.sku_idnt = t.sku_num
WHERE (t.week_idnt IN (SELECT week_idnt FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1 GROUP BY 1) 
   OR t.week_idnt IN (SELECT ly_week_num_realigned FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1 GROUP BY 1))
  AND channel_num IN (110, 210, 250, 120)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,ly_ty
    ,dropship_ind
    ,rp_ind
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
    COLUMN(ly_ty, store_num, channel_num, npg_ind, dept_idnt, class_idnt, supplier_idnt, size_1_frame, size_1_rank, size_1_id, 
           po_type_code, po_type_desc, jda, close_out, item_type)
    ,COLUMN(rp_ind)
    ,COLUMN(dropship_ind) 
    ON trans;


-- DROP TABLE receipts_trans;
CREATE MULTISET VOLATILE TABLE receipts_trans AS (
SELECT
     channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    ,MAX(rp_ind) AS rp_ind
    -- net sales
        -- units
    ,CAST(0 AS INTEGER) AS net_sales_u
    ,CAST(0 AS INTEGER) AS ds_net_sales_u
    ,CAST(0 AS INTEGER) AS reg_net_sales_u
    ,CAST(0 AS INTEGER) AS pro_net_sales_u
    ,CAST(0 AS INTEGER) AS clr_net_sales_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_net_sales_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_net_sales_c
    --returns
      --unit
    ,CAST(0 AS INTEGER) AS returns_u
    ,CAST(0 AS INTEGER) AS ds_returns_u
    ,CAST(0 AS INTEGER) AS reg_returns_u
    ,CAST(0 AS INTEGER) AS pro_returns_u
    ,CAST(0 AS INTEGER) AS clr_returns_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_returns_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_returns_c
    --gross sales
      --unit
    ,CAST(0 AS INTEGER) AS gross_sales_u
    ,CAST(0 AS INTEGER) AS ds_gross_sales_u
    ,CAST(0 AS INTEGER) AS reg_gross_sales_u
    ,CAST(0 AS INTEGER) AS pro_gross_sales_u
    ,CAST(0 AS INTEGER) AS clr_gross_sales_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_gross_sales_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_gross_sales_c
    --product margin
    ,CAST(0 AS DECIMAL(38,4)) AS product_margin
    ,CAST(0 AS DECIMAL(38,4)) AS ds_product_margin
    ,CAST(0 AS DECIMAL(38,4)) AS reg_product_margin
    ,CAST(0 AS DECIMAL(38,4))AS pro_product_margin    
    ,CAST(0 AS DECIMAL(38,4)) AS clr_product_margin  
    -- Inventory
    ,CAST(0 AS INTEGER) AS eoh_u
    ,CAST(0 AS INTEGER) AS reg_eoh_u
    ,CAST(0 AS INTEGER) AS cl_eoh_u
    ,CAST(0 AS DECIMAL(38,4)) AS eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS cl_eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS eoh_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_eoh_c
    ,CAST(0 AS DECIMAL(38,4)) AS cl_eoh_c
    -- receipts
    ,SUM(receipts_u) AS receipts_u
    ,SUM(receipts_c) AS receipts_c
    ,SUM(receipts_r) AS receipts_r
    -- DS reciepts 
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN receipts_u ELSE 0 END) AS ds_receipts_u
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN receipts_c ELSE 0 END) AS ds_receipts_c
    ,SUM(CASE WHEN dropship_ind = 'Y' THEN receipts_r ELSE 0 END) AS ds_receipts_r
    -- close outs
    ,SUM(CASE WHEN close_out = 'Y' THEN receipts_u ELSE 0 END) AS close_out_receipt_u
    ,SUM(CASE WHEN close_out = 'Y' THEN receipts_c ELSE 0 END) AS close_out_receipt_c
    ,SUM(CASE WHEN close_out = 'Y' THEN receipts_r ELSE 0 END) AS close_out_receipt_r
    -- close outs
    ,SUM(CASE WHEN close_out = 'Y' AND item_type = 'ITEM' THEN receipts_u ELSE 0 END) AS close_out_sized_receipt_u
    ,SUM(CASE WHEN close_out = 'Y' AND item_type = 'ITEM'  THEN receipts_c ELSE 0 END) AS close_out_sized_receipt_c
    ,SUM(CASE WHEN close_out = 'Y' AND item_type = 'ITEM'  THEN receipts_r ELSE 0 END) AS close_out_sized_receipt_r
    -- casepacks
    ,SUM(CASE WHEN item_type = 'CASEPACK' THEN receipts_u ELSE 0 END) AS casepack_receipt_u
    ,SUM(CASE WHEN item_type = 'CASEPACK'  THEN receipts_c ELSE 0 END) AS casepack_receipt_c
    ,SUM(CASE WHEN item_type = 'CASEPACK'  THEN receipts_r ELSE 0 END) AS casepack_receipt_r
    -- prepacks
    ,SUM(CASE WHEN item_type = 'PREPACK' THEN receipts_u ELSE 0 END) AS prepack_receipt_u
    ,SUM(CASE WHEN item_type = 'PREPACK'  THEN receipts_c ELSE 0 END) AS prepack_receipt_c
    ,SUM(CASE WHEN item_type = 'PREPACK'  THEN receipts_r ELSE 0 END) AS prepack_receipt_r
    -- Item
    ,SUM(CASE WHEN item_type = 'ITEM' THEN receipts_u ELSE 0 END) AS sized_receipt_u
    ,SUM(CASE WHEN item_type = 'ITEM'  THEN receipts_c ELSE 0 END) AS sized_receipt_c
    ,SUM(CASE WHEN item_type = 'ITEM'  THEN receipts_r ELSE 0 END) AS sized_receipt_r
    -- unknown
    ,SUM(CASE WHEN item_type = 'UNKNOWN' THEN receipts_u ELSE 0 END) AS unknown_receipt_u
    ,SUM(CASE WHEN item_type = 'UNKNOWN'  THEN receipts_c ELSE 0 END) AS unknown_receipt_c
    ,SUM(CASE WHEN item_type = 'UNKNOWN'  THEN receipts_r ELSE 0 END) AS unknown_receipt_r
    -- transfers 
    ,CAST(0 AS INTEGER)AS transfer_out_u
    ,CAST(0 AS INTEGER) AS transfer_in_u
    ,CAST(0 AS INTEGER) AS racking_last_chance_out_u
    ,CAST(0 AS INTEGER) AS flx_in_u
    ,CAST(0 AS INTEGER) AS stock_balance_out_u
    ,CAST(0 AS INTEGER)AS stock_balance_in_u
    ,CAST(0 AS INTEGER) AS reserve_stock_in_u
    ,CAST(0 AS INTEGER) AS pah_in_u
    ,CAST(0 AS INTEGER) AS cust_return_out_u
    ,CAST(0 AS INTEGER) AS cust_return_in_u
    ,CAST(0 AS INTEGER) AS cust_transfer_out_u
    ,CAST(0 AS INTEGER) AS cust_transfer_in_u
FROM (
        SELECT * FROM receipts

        UNION ALL

        SELECT * FROM trans
    ) r
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    )
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
    COLUMN(ly_ty, store_num, channel_num, npg_ind, dept_idnt, class_idnt, supplier_idnt, size_1_frame, size_1_rank, size_1_id) 
    ON receipts_trans;


CREATE MULTISET VOLATILE TABLE transfer_type AS (
    SELECT 
         tr.transfer_context_value
        ,fct.transfer_context_type
    FROM prd_nap_usr_vws.rms14_transfer_created_fact AS tr
    JOIN prd_nap_usr_vws.rms_cost_transfers_fact fct
      ON tr.operation_num = fct.transfer_num
     AND tr.rms_sku_num = fct.rms_sku_num
    WHERE fct.transfer_context_type IN ('RACKING', 'LAST CHANCE', 'STOCK_BALANCE', 'RESERVE_STOCK_TRANSFER', 'RACK_PACK_AND_HOLD', 'CUSTOMER_RETURN', 'CUSTOMER_TRANSFER')
    GROUP BY 1,2
) WITH DATA
PRIMARY INDEX(transfer_context_value)
ON COMMIT PRESERVE ROWS
;

COLLECT STATS
     COLUMN(transfer_context_value)
     ON transfer_type;


-- -- DROP TABLE transfer_base;
CREATE MULTISET VOLATILE TABLE transfer_base AS (
SELECT 
     tr.operation_num
    ,tr.rms_sku_num
    ,sh.receipt_date
    ,w.week_idnt
    ,w.month_idnt
    ,w.half_idnt
    ,CASE WHEN w.ly_week_num_realigned = cal.week_idnt THEN 'LY' ELSE 'TY' END AS ly_ty
    ,tr.transfer_context_value
    ,tt.transfer_context_type
    ,tr.from_location_id 
    ,tr.to_location_id
    ,transfer_qty AS transfer_qty
FROM prd_nap_usr_vws.rms14_transfer_created_fact AS tr
JOIN prd_nap_usr_vws.rms14_transfer_shipment_receipt_fact AS sh
  ON sh.operation_num = tr.operation_num 
 AND sh.rms_sku_num = tr.rms_sku_num 
JOIN {environment_schema}.size_eval_cal cal
  ON sh.receipt_date = cal.day_date
 AND cal.hist_ind = 1 
JOIN (
        SELECT * 
        FROM {environment_schema}.size_eval_cal_week_vw
        WHERE last_completed_3_months = 1 
    ) w
  ON w.week_idnt = cal.week_idnt
  OR w.ly_week_num_realigned = cal.week_idnt
LEFT JOIN transfer_type tt
  ON tr.transfer_context_value = tt.transfer_context_value
WHERE tr.transfer_context_value NOT IN ('GT', 'GTR', 'HL', 'ICR', 'NXTR', 'NULL')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
) WITH DATA
PRIMARY INDEX(rms_sku_num
    ,receipt_date
    ,week_idnt
    ,month_idnt
    ,half_idnt
    ,ly_ty
    ,transfer_context_value
    ,transfer_context_type
    ,to_location_id
    ,from_location_id)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN(to_location_id) 
    ,COLUMN(from_location_id) 
    ,COLUMN(month_idnt, half_idnt, ly_ty, transfer_context_value, transfer_context_type)
    ,COLUMN(rms_sku_num, from_location_id) 
    ,COLUMN(rms_sku_num, to_location_id) 
    ,COLUMN(rms_sku_num, month_idnt, half_idnt, ly_ty, transfer_context_value, transfer_context_type, from_location_id)
    ,COLUMN(rms_sku_num, month_idnt, half_idnt, ly_ty, transfer_context_value, transfer_context_type, to_location_id)
     ON transfer_base;

-- -- DROP TABLE transfers;
CREATE MULTISET VOLATILE TABLE transfers AS (
SELECT 
     operation_num
    ,a.rms_sku_num
    ,a.receipt_date
    ,a.week_idnt
    ,a.month_idnt
    ,a.half_idnt
    ,a.ly_ty
    ,a.transfer_context_value
    ,a.transfer_context_type
    ,a.from_location_id AS store_num
    ,st.channel_num
    ,h.dept_idnt
    ,h.class_idnt
    ,h.supplier_idnt
    ,h.npg_ind
    ,h.size_1_frame
    ,h.size_1_rank
    ,h.size_1_id
    ,a.transfer_qty AS transfer_out_qty
    ,CAST(0 AS INTEGER) AS transfer_in_qty
FROM transfer_base a
JOIN prd_nap_usr_vws.price_store_dim_vw st
  ON a.from_location_id = st.store_num
JOIN {environment_schema}.supp_size_hierarchy h
  ON h.sku_idnt = a.rms_sku_num
WHERE st.channel_num IN (110, 210, 250, 120)

UNION ALL 

SELECT 
     operation_num
    ,a.rms_sku_num
    ,a.receipt_date
    ,a.week_idnt
    ,a.month_idnt
    ,a.half_idnt
    ,a.ly_ty
    ,a.transfer_context_value
    ,a.transfer_context_type
    ,a.to_location_id AS store_num
    ,st.channel_num
    ,h.dept_idnt
    ,h.class_idnt
    ,h.supplier_idnt
    ,h.npg_ind
    ,h.size_1_frame
    ,h.size_1_rank
    ,h.size_1_id
    ,CAST(0 AS INTEGER) AS transfer_out_qty
    ,transfer_qty AS transfer_in_qty   
FROM transfer_base a
JOIN prd_nap_usr_vws.price_store_dim_vw st
  ON a.to_location_id = st.store_num
JOIN {environment_schema}.supp_size_hierarchy h
  ON h.sku_idnt = a.rms_sku_num
WHERE st.channel_num IN (110, 210, 250, 120)
) WITH DATA
PRIMARY INDEX(
     ly_ty
    ,transfer_context_value
    ,transfer_context_type
    ,store_num
    ,channel_num
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,npg_ind
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
) 
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN (ly_ty, store_num, channel_num, dept_idnt, class_idnt, supplier_idnt, npg_ind, size_1_frame, size_1_id) 
    ,COLUMN (class_idnt)
    ,COLUMN (dept_idnt) 
    ,COLUMN (channel_num)
    ,COLUMN (half_idnt)
    ,COLUMN (month_idnt) 
    ON transfers;


-- DROP TABLE transfers_in_out;
CREATE MULTISET VOLATILE TABLE transfers_in_out AS (
SELECT
     channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    ,MAX('N') AS rp_ind
    -- net sales
        -- units
    ,CAST(0 AS INTEGER) AS net_sales_u
    ,CAST(0 AS INTEGER) AS ds_net_sales_u
    ,CAST(0 AS INTEGER) AS reg_net_sales_u
    ,CAST(0 AS INTEGER) AS pro_net_sales_u
    ,CAST(0 AS INTEGER) AS clr_net_sales_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_net_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_net_sales_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_net_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_net_sales_c
    --returns
      --unit
    ,CAST(0 AS INTEGER) AS returns_u
    ,CAST(0 AS INTEGER) AS ds_returns_u
    ,CAST(0 AS INTEGER) AS reg_returns_u
    ,CAST(0 AS INTEGER) AS pro_returns_u
    ,CAST(0 AS INTEGER) AS clr_returns_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_returns_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_returns_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_returns_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_returns_c
    --gross sales
      --unit
    ,CAST(0 AS INTEGER) AS gross_sales_u
    ,CAST(0 AS INTEGER) AS ds_gross_sales_u
    ,CAST(0 AS INTEGER) AS reg_gross_sales_u
    ,CAST(0 AS INTEGER) AS pro_gross_sales_u
    ,CAST(0 AS INTEGER) AS clr_gross_sales_u
      --retail
    ,CAST(0 AS DECIMAL(38,4)) AS gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS ds_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS pro_gross_sales_r
    ,CAST(0 AS DECIMAL(38,4)) AS clr_gross_sales_r
      --cost
    ,CAST(0 AS DECIMAL(38,4)) AS gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS pro_gross_sales_c
    ,CAST(0 AS DECIMAL(38,4)) AS clr_gross_sales_c
    --product margin
    ,CAST(0 AS DECIMAL(38,4)) AS product_margin
    ,CAST(0 AS DECIMAL(38,4)) AS ds_product_margin
    ,CAST(0 AS DECIMAL(38,4)) AS reg_product_margin
    ,CAST(0 AS DECIMAL(38,4))AS pro_product_margin    
    ,CAST(0 AS DECIMAL(38,4)) AS clr_product_margin  
    -- Inventory
    ,CAST(0 AS INTEGER) AS eoh_u
    ,CAST(0 AS INTEGER) AS reg_eoh_u
    ,CAST(0 AS INTEGER) AS cl_eoh_u
    ,CAST(0 AS DECIMAL(38,4)) AS eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS reg_eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS cl_eoh_r
    ,CAST(0 AS DECIMAL(38,4)) AS eoh_c
    ,CAST(0 AS DECIMAL(38,4)) AS reg_eoh_c
    ,CAST(0 AS DECIMAL(38,4)) AS cl_eoh_c
        -- receipts
    ,CAST(0 AS INTEGER) AS receipts_u
    ,CAST(0 AS DECIMAL(38,4)) AS receipts_c
    ,CAST(0 AS DECIMAL(38,4)) AS receipts_r
     -- DS receipts
    ,CAST(0 AS INTEGER) AS ds_receipts_u
    ,CAST(0 AS DECIMAL(38,4)) AS ds_receipts_c
    ,CAST(0 AS DECIMAL(38,4)) AS ds_receipts_r
    -- close outs
    ,CAST(0 AS INTEGER) AS close_out_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS close_out_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS close_out_receipt_r
    -- close outs
    ,CAST(0 AS INTEGER) AS close_out_sized_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS close_out_sized_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS close_out_sized_receipt_r
    -- casepacks
    ,CAST(0 AS INTEGER) AS casepack_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS casepack_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS casepack_receipt_r
    -- prepacks
    ,CAST(0 AS INTEGER) AS prepack_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS prepack_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS prepack_receipt_r
    -- Item
    ,CAST(0 AS INTEGER) AS sized_receipt_u
    ,CAST(0 AS DECIMAL(38,4)) AS sized_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS sized_receipt_r
    -- unknown
    ,CAST(0 AS INTEGER) AS unknown_receipt_u
    ,CAST(0 AS DECIMAL(38,4))AS unknown_receipt_c
    ,CAST(0 AS DECIMAL(38,4)) AS unknown_receipt_r
    -- transfers 
    ,SUM(transfer_out_qty) AS transfer_out_u
    ,SUM(transfer_in_qty) AS transfer_in_u
    ,SUM(CASE WHEN transfer_context_type IN ('RACKING', 'LAST CHANCE') THEN transfer_out_qty END) AS racking_last_chance_out_u
    ,SUM(CASE WHEN transfer_context_type = 'RACKING' THEN transfer_in_qty END) AS flx_in_u
    ,SUM(CASE WHEN transfer_context_type = 'STOCK_BALANCE' THEN transfer_out_qty END) AS stock_balance_out_u
    ,SUM(CASE WHEN transfer_context_type = 'STOCK_BALANCE' THEN transfer_in_qty END) AS stock_balance_in_u
    ,SUM(CASE WHEN transfer_context_type = 'RESERVE_STOCK_TRANSFER' THEN transfer_in_qty END) AS reserve_stock_in_u
    ,SUM(CASE WHEN transfer_context_type = 'RACK_PACK_AND_HOLD' THEN transfer_in_qty END) AS pah_in_u
    ,SUM(CASE WHEN transfer_context_type = 'CUSTOMER_RETURN' THEN transfer_out_qty END) AS cust_return_out_u
    ,SUM(CASE WHEN transfer_context_type = 'CUSTOMER_RETURN' THEN transfer_in_qty END) AS cust_return_in_u
    ,SUM(CASE WHEN transfer_context_type = 'CUSTOMER_TRANSFER' THEN transfer_out_qty END) AS cust_transfer_out_u
    ,SUM(CASE WHEN transfer_context_type = 'CUSTOMER_TRANSFER' THEN transfer_in_qty END) AS cust_transfer_in_u
FROM transfers
GROUP BY 1,2,3,4,5,6,7,8,9,10
) WITH DATA 
PRIMARY INDEX(
     ly_ty
    ,store_num
    ,channel_num
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,npg_ind
    ,size_1_rank
    ,size_1_frame
    ,size_1_id
    )
ON COMMIT PRESERVE ROWS 
;

COLLECT STATISTICS 
     COLUMN (ly_ty, store_num, channel_num, dept_idnt, class_idnt, supplier_idnt, npg_ind, size_1_rank, size_1_frame, size_1_id)
    ,COLUMN (class_idnt)
    ,COLUMN (rp_ind)
    ,COLUMN (dept_idnt) 
    ,COLUMN (channel_num)
    ON transfers_in_out;


-- DROP TABLE frames;
CREATE MULTISET VOLATILE TABLE frames AS (
    SELECT  
         half_idnt
        ,channel_num
        ,dept_idnt
        ,class_frame
        ,frame
        ,supplier_frame
    FROM {environment_schema}.size_curves_model_rec
    WHERE class_frame <> 'NA'
      AND store_num IS NOT NULL
      AND half_idnt = (SELECT MIN(DISTINCT half_idnt) FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1)
    GROUP BY 1,2,3,4,5,6
) WITH DATA
PRIMARY INDEX(
         half_idnt
        ,channel_num
        ,dept_idnt
        ,class_frame
        ,frame
        ,supplier_frame
    )
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN (half_idnt
        ,channel_num
        ,dept_idnt
        ,class_frame
        ,frame
        ,supplier_frame) 
     ON frames;

-- DROP TABLE frames2;
CREATE MULTISET VOLATILE TABLE frames2 AS (
    SELECT  
         half_idnt
        ,channel_num
        ,dept_idnt
        ,frame
        ,class_frame
    FROM {environment_schema}.size_curves_model_rec
    WHERE half_idnt = (SELECT MIN(DISTINCT half_idnt) FROM {environment_schema}.size_eval_cal_week_vw WHERE last_completed_3_months = 1)
      AND store_num IS NOT NULL
    GROUP BY 1,2,3,4,5
) WITH DATA
PRIMARY INDEX(
         half_idnt
        ,channel_num
        ,dept_idnt
        ,frame 
    )
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS 
     COLUMN (half_idnt, channel_num, dept_idnt, frame) 
     ON frames2;

COLLECT STATISTICS 
     COLUMN (channel_num ,store_num, ly_ty, npg_ind, dept_idnt, class_idnt, supplier_idnt, size_1_frame, size_1_rank, size_1_id)
    ,COLUMN (channel_num, dept_idnt, class_idnt, supplier_idnt, size_1_frame)
     ON inv_sales;
     
-- DROP TABLE combine; 
CREATE MULTISET VOLATILE TABLE combine AS (
SELECT 
    a.*
--    ,TRIM(a.dept_idnt) || '~~' || TRIM(a.class_idnt) || '~~' || a.size_1_frame AS class_frame
FROM (
        SELECT * 
        FROM inv_sales

        UNION ALL 

        SELECT * FROM receipts_trans

        UNION ALL 

        SELECT * FROM transfers_in_out
    ) a

 ) WITH DATA
PRIMARY INDEX(channel_num
    ,store_num
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,supplier_idnt
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    )
ON COMMIT PRESERVE ROWS
;


COLLECT STATS
     COLUMN(channel_num, store_num,  ly_ty, npg_ind, dept_idnt, class_idnt, supplier_idnt, size_1_frame, size_1_rank, size_1_id)
    ,COLUMN(channel_num, supplier_idnt)
    ,COLUMN(size_1_rank)
    ON combine;
    
-- SELECT TOP 10 * FROM frames2;
-- -- DROP TABLE combine_2;
CREATE MULTISET VOLATILE TABLE combine_2 AS (
    SELECT 
        a.*
       ,CASE WHEN a.channel_num = 120 THEN 808
             WHEN a.channel_num = 250 THEN 828
             ELSE store_num
             END AS store_number
       ,COALESCE(f2.class_frame, f.class_frame, 'AllClasses') AS class_frame
       ,COALESCE(f.supplier_frame, 'AllSuppliers') AS supplier_frame
    FROM combine a
    LEFT JOIN frames f
      ON a.channel_num = f.channel_num 
     AND a.dept_idnt = f.dept_idnt
     AND TRIM(a.dept_idnt) || '~~' || TRIM(a.class_idnt) || '~~' || a.size_1_frame = f.class_frame
     AND a.size_1_frame = f.frame
     AND a.supplier_idnt = f.supplier_frame
    LEFT JOIN frames2 f2
      ON a.channel_num = f2.channel_num 
     AND a.dept_idnt = f2.dept_idnt
     AND TRIM(a.dept_idnt) || '~~' || TRIM(a.class_idnt) || '~~' || a.size_1_frame = f2.class_frame
     AND a.size_1_frame = f2.frame
 ) WITH DATA
PRIMARY INDEX(channel_num
    ,store_number
    ,ly_ty
    ,npg_ind
    ,dept_idnt
    ,class_idnt
    ,class_frame
    ,supplier_idnt
    ,supplier_frame
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    )
ON COMMIT PRESERVE ROWS
;     

COLLECT STATS
     COLUMN(channel_num, store_number, ly_ty, npg_ind, dept_idnt, class_idnt, class_frame, supplier_idnt, supplier_frame, size_1_frame, size_1_rank, size_1_id)
    ,COLUMN(channel_num, class_frame, supplier_idnt)
    ,COLUMN(size_1_rank)
    ,COLUMN (CLASS_IDNT)
    ,COLUMN (DEPT_IDNT) 
    ,COLUMN (CHANNEL_NUM)
    ON combine_2;
   

DELETE FROM {environment_schema}.size_actuals ALL;
INSERT INTO {environment_schema}.size_actuals
SELECT
     a.channel_num
    ,a.store_number AS store_num
    ,ly_ty
    ,CASE WHEN supplier_frame = 'AllSuppliers' THEN 'N' ELSE npg_ind END AS npg_ind
    ,dept_idnt
    ,a.class_frame 
    ,supplier_frame
    ,size_1_frame
    ,size_1_rank
    ,size_1_id
    ,MAX(rp_ind) AS rp_ind
    -- Net Sales
      -- units
    ,SUM(COALESCE(net_sales_u, 0)) AS net_sales_u
    ,SUM(COALESCE(ds_net_sales_u, 0)) AS ds_net_sales_u
    ,SUM(COALESCE(reg_net_sales_u, 0)) AS reg_net_sales_u
    ,SUM(COALESCE(pro_net_sales_u, 0)) AS pro_net_sales_u
    ,SUM(COALESCE(clr_net_sales_u, 0)) AS clr_net_sales_u
      --retail
    ,SUM(COALESCE(net_sales_r, 0)) AS net_sales_r
    ,SUM(COALESCE(ds_net_sales_r, 0)) AS ds_net_sales_r
    ,SUM(COALESCE(reg_net_sales_r, 0)) AS reg_net_sales_r
    ,SUM(COALESCE(pro_net_sales_r, 0)) AS pro_net_sales_r
    ,SUM(COALESCE(clr_net_sales_r, 0)) AS clr_net_sales_r
      --cost
    ,SUM(COALESCE(net_sales_c, 0)) AS net_sales_c
    ,SUM(COALESCE(ds_net_sales_c, 0)) AS ds_net_sales_c
    ,SUM(COALESCE(reg_net_sales_c, 0)) AS reg_net_sales_c
    ,SUM(COALESCE(pro_net_sales_c, 0)) AS pro_net_sales_c
    ,SUM(COALESCE(clr_net_sales_c, 0)) AS clr_net_sales_c
    --returns
      --unit
    ,SUM(COALESCE(returns_u, 0)) AS returns_u
    ,SUM(COALESCE(ds_returns_u, 0)) AS ds_returns_u
    ,SUM(COALESCE(reg_returns_u, 0)) AS reg_returns_u
    ,SUM(COALESCE(pro_returns_u, 0)) AS pro_returns_u
    ,SUM(COALESCE(clr_returns_u, 0)) AS clr_returns_u
      --retail
    ,SUM(COALESCE(returns_r, 0)) AS returns_r
    ,SUM(COALESCE(ds_returns_r, 0)) AS ds_returns_r
    ,SUM(COALESCE(reg_returns_r, 0)) AS reg_returns_r
    ,SUM(COALESCE(pro_returns_r, 0)) AS pro_returns_r
    ,SUM(COALESCE(clr_returns_r, 0)) AS clr_returns_r
      --cost
    ,SUM(COALESCE(returns_c, 0)) AS returns_c
    ,SUM(COALESCE(ds_returns_c, 0)) AS ds_returns_c
    ,SUM(COALESCE(reg_returns_c, 0)) AS reg_returns_c
    ,SUM(COALESCE(pro_returns_c, 0)) AS pro_returns_c
    ,SUM(COALESCE(clr_returns_c, 0)) AS clr_returns_c
    --gross sales
      --unit
    ,SUM(COALESCE(gross_sales_u, 0)) AS gross_sales_u
    ,SUM(COALESCE(ds_gross_sales_u, 0)) AS ds_gross_sales_u
    ,SUM(COALESCE(reg_gross_sales_u, 0)) AS reg_gross_sales_u
    ,SUM(COALESCE(pro_gross_sales_u, 0)) AS pro_gross_sales_u
    ,SUM(COALESCE(clr_gross_sales_u, 0)) AS clr_gross_sales_u
      --retail
    ,SUM(COALESCE(gross_sales_r, 0)) AS gross_sales_r
    ,SUM(COALESCE(ds_gross_sales_r, 0)) AS ds_gross_sales_r
    ,SUM(COALESCE(reg_gross_sales_r, 0)) AS reg_gross_sales_r
    ,SUM(COALESCE(pro_gross_sales_r, 0)) AS pro_gross_sales_r
    ,SUM(COALESCE(clr_gross_sales_r, 0)) AS clr_gross_sales_r
      --cost
    ,SUM(COALESCE(gross_sales_c, 0)) AS gross_sales_c
    ,SUM(COALESCE(ds_gross_sales_c, 0)) AS ds_gross_sales_c
    ,SUM(COALESCE(reg_gross_sales_c, 0)) AS reg_gross_sales_c
    ,SUM(COALESCE(pro_gross_sales_c, 0)) AS pro_gross_sales_c
    ,SUM(COALESCE(clr_gross_sales_c, 0)) AS clr_gross_sales_c
    --product margin
    ,SUM(COALESCE(product_margin, 0)) AS product_margin
    ,SUM(COALESCE(ds_product_margin, 0)) AS ds_product_margin
    ,SUM(COALESCE(reg_product_margin, 0)) AS reg_product_margin
    ,SUM(COALESCE(pro_product_margin, 0))AS pro_product_margin    
    ,SUM(COALESCE(clr_product_margin, 0)) AS clr_product_margin
    -- inventory
    ,SUM(COALESCE(eoh_u, 0)) AS eoh_u
    ,SUM(COALESCE(reg_eoh_u, 0)) AS reg_eoh_u
    ,SUM(COALESCE(cl_eoh_u, 0)) AS cl_eoh_u
    ,SUM(COALESCE(eoh_r, 0)) AS eoh_r
    ,SUM(COALESCE(reg_eoh_r, 0)) AS reg_eoh_r
    ,SUM(COALESCE(cl_eoh_r, 0)) AS cl_eoh_r
    ,SUM(COALESCE(eoh_c, 0)) AS eoh_c
    ,SUM(COALESCE(reg_eoh_c, 0)) AS reg_eoh_c
    ,SUM(COALESCE(cl_eoh_c, 0)) AS cl_eoh_c
    -- receipts
    ,SUM(COALESCE(receipts_u, 0)) AS receipts_u
    ,SUM(COALESCE(receipts_c, 0)) AS receipts_c
    ,SUM(COALESCE(receipts_r, 0)) AS receipts_r
    -- DS reciepts 
    ,SUM(COALESCE(ds_receipts_u, 0)) AS ds_receipts_u
    ,SUM(COALESCE(ds_receipts_c, 0)) AS ds_receipts_c
    ,SUM(COALESCE(ds_receipts_r, 0)) AS ds_receipts_r
    -- close outs
    ,SUM(COALESCE(close_out_receipt_u, 0)) AS close_out_receipt_u
    ,SUM(COALESCE(close_out_receipt_c, 0)) AS close_out_receipt_c
    ,SUM(COALESCE(close_out_receipt_r, 0)) AS close_out_receipt_r
    -- sized close outs
    ,SUM(COALESCE(close_out_sized_receipt_u, 0)) AS close_out_sized_receipt_u
    ,SUM(COALESCE(close_out_sized_receipt_c, 0)) AS close_out_sized_receipt_c
    ,SUM(COALESCE(close_out_sized_receipt_r, 0)) AS close_out_sized_receipt_r
    -- casepacks
    ,SUM(COALESCE(casepack_receipt_u, 0)) AS casepack_receipt_u
    ,SUM(COALESCE(casepack_receipt_c, 0))AS casepack_receipt_c
    ,SUM(COALESCE(casepack_receipt_r, 0)) AS casepack_receipt_r
    -- prepacks
    ,SUM(COALESCE(prepack_receipt_u, 0)) AS prepack_receipt_u
    ,SUM(COALESCE(prepack_receipt_c, 0)) AS prepack_receipt_c
    ,SUM(COALESCE(prepack_receipt_r, 0)) AS prepack_receipt_r
    -- Item
    ,SUM(COALESCE(sized_receipt_u, 0)) AS sized_receipt_u
    ,SUM(COALESCE(sized_receipt_c, 0)) AS sized_receipt_c
    ,SUM(COALESCE(sized_receipt_r, 0)) AS sized_receipt_r
    -- unknown
    ,SUM(COALESCE(unknown_receipt_u, 0)) AS unknown_receipt_u
    ,SUM(COALESCE(unknown_receipt_c, 0)) AS unknown_receipt_c
    ,SUM(COALESCE(unknown_receipt_r, 0)) AS unknown_receipt_r
    -- transfers
    ,SUM(COALESCE(transfer_out_u , 0)) AS transfer_out_u
    ,SUM(COALESCE(transfer_in_u, 0)) AS transfer_in_u
    ,SUM(COALESCE(racking_last_chance_out_u, 0)) AS racking_last_chance_out_u
    ,SUM(COALESCE(flx_in_u, 0)) AS flx_in_u
    ,SUM(COALESCE(stock_balance_out_u, 0)) AS stock_balance_out_u
    ,SUM(COALESCE(stock_balance_in_u, 0)) AS stock_balance_in_u
    ,SUM(COALESCE(reserve_stock_in_u, 0)) AS reserve_stock_in_u
    ,SUM(COALESCE(pah_in_u, 0)) AS pah_in_u
    ,SUM(COALESCE(cust_return_out_u, 0)) AS cust_return_out_u
    ,SUM(COALESCE(cust_return_in_u, 0)) AS cust_return_in_u
    ,SUM(COALESCE(cust_transfer_out_u, 0)) AS cust_transfer_out_u
    ,SUM(COALESCE(cust_transfer_in_u , 0)) AS cust_transfer_in_u
    ,CURRENT_TIMESTAMP AS update_timestamp
FROM combine_2 a
GROUP BY 1,2,3,4,5,6,7,8,9,10
;


COLLECT STATISTICS 
     COLUMN (store_num, class_frame, supplier_frame, size_1_id)
    ,COLUMN (ly_ty)
    ,COLUMN (channel_num, store_num,dept_idnt, supplier_frame, class_frame, size_1_id)
    ,COLUMN (channel_num, store_num, class_frame, size_1_id) 
    ,COLUMN (store_num, class_frame, supplier_frame) 
    ,COLUMN (store_num, class_frame, supplier_frame, size_1_id) 
    ,COLUMN (channel_num, store_num, dept_idnt, class_frame, supplier_frame,size_1_frame, size_1_id)
    ,COLUMN (store_num, ly_ty, class_frame, supplier_frame) 
    ,COLUMN (store_num, ly_ty, class_frame, supplier_frame, size_1_id) 
    ,COLUMN (store_num) 
    ,COLUMN (store_num, dept_idnt, size_1_frame, size_1_id) 
    ,COLUMN (store_num, dept_idnt, class_frame)
    ,COLUMN (store_num, dept_idnt, class_frame, supplier_frame)
    ,COLUMN (store_num, dept_idnt, class_frame, supplier_frame, size_1_id)
    ,COLUMN (dept_idnt)
    ,COLUMN (store_num, dept_idnt, class_frame,supplier_frame, size_1_frame)
    ON {environment_schema}.size_actuals;