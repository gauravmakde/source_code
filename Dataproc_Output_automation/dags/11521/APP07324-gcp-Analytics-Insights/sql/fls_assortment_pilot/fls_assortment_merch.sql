
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=fls_assortment_11521_ACE_ENG;
---    Task_Name=fls_assortment_merch;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS week_dim AS WITH week_dim_base AS (SELECT week_num,
   week_end_date,
   week_label,
   week_index,
   first_week_of_month_flag,
   last_week_of_month_flag,
   month_num,
   month_abrv,
   month_label,
   month_index,
   quarter_num,
   quarter_label,
   half_num,
   half_label,
   year_num,
   year_label,
   true_week_num
  FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim
  GROUP BY week_num,
   week_end_date,
   week_label,
   week_index,
   first_week_of_month_flag,
   last_week_of_month_flag,
   month_num,
   month_abrv,
   month_label,
   month_index,
   quarter_num,
   quarter_label,
   half_num,
   half_label,
   year_num,
   year_label,
   true_week_num) (SELECT DISTINCT week_num,
   week_end_date,
   week_label,
   week_index,
   first_week_of_month_flag,
   last_week_of_month_flag,
   month_num,
   month_abrv,
   month_label,
   month_index,
   quarter_num,
   quarter_label,
   half_num,
   half_label,
   year_num,
   year_label,
   true_week_num,
   COUNT(week_num) OVER (PARTITION BY month_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS week_count
   
  FROM week_dim_base AS w
  WHERE week_num IN (SELECT week_num
     FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats column (week_num) on week_dim;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sales
--cluster BY cc_num
AS
SELECT p.cc_num,
 s.store_num,
 w.week_num,
 m.rp_ind,
 1 AS sales_flag,
 0 AS inv_flag,
 0 AS rcpt_flag,
 0 AS twist_flag,
 0 AS sff_flag,
 0 AS trans_flag,
 SUM(m.net_sales_tot_retl) AS sales_retail,
 SUM(m.net_sales_tot_cost) AS sales_cost,
 SUM(m.net_sales_tot_units) AS sales_units,
 SUM(m.net_sales_tot_regular_retl) AS sales_reg_retail,
 SUM(m.net_sales_tot_regular_cost) AS sales_reg_cost,
 SUM(m.net_sales_tot_regular_units) AS sales_reg_units,
 SUM(m.net_sales_tot_promo_retl) AS sales_pro_retail,
 SUM(m.net_sales_tot_promo_cost) AS sales_pro_cost,
 SUM(m.net_sales_tot_promo_units) AS sales_pro_units,
 SUM(m.net_sales_tot_clearance_retl) AS sales_clr_retail,
 SUM(m.net_sales_tot_clearance_cost) AS sales_clr_cost,
 SUM(m.net_sales_tot_clearance_units) AS sales_clr_units,
 SUM(CASE
   WHEN LOWER(m.fulfill_type_code) = LOWER('VendorDropShip')
   THEN m.net_sales_tot_retl
   ELSE NULL
   END) AS sales_ds_retail,
 SUM(CASE
   WHEN LOWER(m.fulfill_type_code) = LOWER('VendorDropShip')
   THEN m.net_sales_tot_cost
   ELSE NULL
   END) AS sales_ds_cost,
 SUM(CASE
   WHEN LOWER(m.fulfill_type_code) = LOWER('VendorDropShip')
   THEN m.net_sales_tot_units
   ELSE NULL
   END) AS sales_ds_units,
 SUM(m.gross_sales_tot_retl) AS sales_gross_retail,
 SUM(m.gross_sales_tot_cost) AS sales_gross_cost,
 SUM(m.gross_sales_tot_units) AS sales_gross_units,
 SUM(m.returns_tot_retl) AS return_retail,
 SUM(m.returns_tot_cost) AS return_cost,
 SUM(m.returns_tot_units) AS return_units,
 SUM(CASE
   WHEN LOWER(m.fulfill_type_code) = LOWER('VendorDropShip')
   THEN m.returns_tot_retl
   ELSE NULL
   END) AS return_ds_retail,
 SUM(CASE
   WHEN LOWER(m.fulfill_type_code) = LOWER('VendorDropShip')
   THEN m.returns_tot_cost
   ELSE NULL
   END) AS return_ds_cost,
 SUM(CASE
   WHEN LOWER(m.fulfill_type_code) = LOWER('VendorDropShip')
   THEN m.returns_tot_units
   ELSE NULL
   END) AS return_ds_units,
 0 AS boh_retail,
 0 AS boh_cost,
 0 AS boh_units,
 0 AS eoh_retail,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS rcpt_po_retail,
 0 AS rcpt_po_cost,
 0 AS rcpt_po_units,
 0 AS rcpt_ds_retail,
 0 AS rcpt_ds_cost,
 0 AS rcpt_ds_units,
 0 AS tsfr_rs_in_retail,
 0 AS tsfr_rs_in_cost,
 0 AS tsfr_rs_in_units,
 0 AS tsfr_rack_in_retail,
 0 AS tsfr_rack_in_cost,
 0 AS tsfr_rack_in_units,
 0 AS tsfr_pah_in_retail,
 0 AS tsfr_pah_in_cost,
 0 AS tsfr_pah_in_units,
 0 AS twist_instock_traffic,
 0 AS twist_total_traffic,
 0 AS sff_units,
 0 AS bopus_units,
 0 AS dsr_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS m
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(m.rms_sku_num) = LOWER(p.rms_sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON m.store_num = s.store_num
 INNER JOIN week_dim AS w ON m.week_num = w.true_week_num
GROUP BY p.cc_num,
 s.store_num,
 w.week_num,
 m.rp_ind
HAVING sales_gross_units > 0 OR return_units > 0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num) ,column ( cc_num , store_num , week_num , rp_ind) on sales;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS inv
--cluster BY cc_num
AS
SELECT t6.cc_num,
 t6.store_num,
 t6.week_num0 AS week_num,
 t6.rp_ind,
 0 AS sales_flag,
 1 AS inv_flag,
 0 AS rcpt_flag,
 0 AS twist_flag,
 0 AS sff_flag,
 0 AS trans_flag,
 0 AS sales_retail,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS sales_reg_retail,
 0 AS sales_reg_cost,
 0 AS sales_reg_units,
 0 AS sales_pro_retail,
 0 AS sales_pro_cost,
 0 AS sales_pro_units,
 0 AS sales_clr_retail,
 0 AS sales_clr_cost,
 0 AS sales_clr_units,
 0 AS sales_ds_retail,
 0 AS sales_ds_cost,
 0 AS sales_ds_units,
 0 AS sales_gross_retail,
 0 AS sales_gross_cost,
 0 AS sales_gross_units,
 0 AS return_retail,
 0 AS return_cost,
 0 AS return_units,
 0 AS return_ds_retail,
 0 AS return_ds_cost,
 0 AS return_ds_units,
 t6.boh_retail,
 t6.boh_cost,
 t6.boh_units,
 t6.eoh_retail,
 t6.eoh_cost,
 t6.eoh_units,
 0 AS rcpt_po_retail,
 0 AS rcpt_po_cost,
 0 AS rcpt_po_units,
 0 AS rcpt_ds_retail,
 0 AS rcpt_ds_cost,
 0 AS rcpt_ds_units,
 0 AS tsfr_rs_in_retail,
 0 AS tsfr_rs_in_cost,
 0 AS tsfr_rs_in_units,
 0 AS tsfr_rack_in_retail,
 0 AS tsfr_rack_in_cost,
 0 AS tsfr_rack_in_units,
 0 AS tsfr_pah_in_retail,
 0 AS tsfr_pah_in_cost,
 0 AS tsfr_pah_in_units,
 0 AS twist_instock_traffic,
 0 AS twist_total_traffic,
 0 AS sff_units,
 0 AS bopus_units,
 0 AS dsr_units
FROM (SELECT i.store_num,
   i.rp_ind,
   p.cc_num,
   w.week_num AS week_num0,
   SUM(i.inventory_boh_total_retail_amt_ty) AS boh_retail,
   SUM(i.inventory_boh_total_cost_amt_ty) AS boh_cost,
   SUM(i.inventory_boh_total_units_ty) AS boh_units,
   SUM(i.inventory_eoh_total_retail_amt_ty) AS eoh_retail,
   SUM(i.inventory_eoh_total_cost_amt_ty) AS eoh_cost,
   SUM(i.inventory_eoh_total_units_ty) AS eoh_units
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_agg_fact AS i
   INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(i.rms_sku_num) = LOWER(p.rms_sku_num)
   INNER JOIN week_dim AS w ON i.week_num = w.true_week_num
  WHERE i.store_num IN (SELECT DISTINCT store_num
     FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s)
   AND i.week_num IN (SELECT DISTINCT true_week_num
     FROM week_dim)
  GROUP BY i.store_num,
   i.rp_ind,
   p.cc_num,
   w.week_num
  HAVING boh_units > 0 OR eoh_units > 0) AS t6;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num) ,column ( cc_num , store_num , week_num , rp_ind) on inv;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rcpt
--cluster BY cc_num
AS
SELECT t0.cc_num,
 t0.store_num0 AS store_num,
 t0.week_num0 AS week_num,
 t0.rp_ind,
 0 AS sales_flag,
 0 AS inv_flag,
 1 AS rcpt_flag,
 0 AS twist_flag,
 0 AS sff_flag,
 0 AS trans_flag,
 0 AS sales_retail,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS sales_reg_retail,
 0 AS sales_reg_cost,
 0 AS sales_reg_units,
 0 AS sales_pro_retail,
 0 AS sales_pro_cost,
 0 AS sales_pro_units,
 0 AS sales_clr_retail,
 0 AS sales_clr_cost,
 0 AS sales_clr_units,
 0 AS sales_ds_retail,
 0 AS sales_ds_cost,
 0 AS sales_ds_units,
 0 AS sales_gross_retail,
 0 AS sales_gross_cost,
 0 AS sales_gross_units,
 0 AS return_retail,
 0 AS return_cost,
 0 AS return_units,
 0 AS return_ds_retail,
 0 AS return_ds_cost,
 0 AS return_ds_units,
 0 AS boh_retail,
 0 AS boh_cost,
 0 AS boh_units,
 0 AS eoh_retail,
 0 AS eoh_cost,
 0 AS eoh_units,
 t0.rcpt_po_retail,
 t0.rcpt_po_cost,
 t0.rcpt_po_units,
 t0.rcpt_ds_retail,
 t0.rcpt_ds_cost,
 t0.rcpt_ds_units,
 0 AS tsfr_rs_in_retail,
 0 AS tsfr_rs_in_cost,
 0 AS tsfr_rs_in_units,
 0 AS tsfr_rack_in_retail,
 0 AS tsfr_rack_in_cost,
 0 AS tsfr_rack_in_units,
 0 AS tsfr_pah_in_retail,
 0 AS tsfr_pah_in_cost,
 0 AS tsfr_pah_in_units,
 0 AS twist_instock_traffic,
 0 AS twist_total_traffic,
 0 AS sff_units,
 0 AS bopus_units,
 0 AS dsr_units
FROM (SELECT r.rp_ind,
   s.store_num AS store_num0,
   p.cc_num,
   w.week_num AS week_num0,
   SUM(r.receipts_po_retail_amt_ty) AS rcpt_po_retail,
   SUM(r.receipts_po_cost_amt_ty) AS rcpt_po_cost,
   SUM(r.receipts_po_units_ty) AS rcpt_po_units,
   SUM(r.receipts_dropship_retail_amt_ty) AS rcpt_ds_retail,
   SUM(r.receipts_dropship_cost_amt_ty) AS rcpt_ds_cost,
   SUM(r.receipts_dropship_units_ty) AS rcpt_ds_units
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_agg_fact AS r
   INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON r.store_num = s.store_num
   INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(r.rms_sku_num) = LOWER(p.rms_sku_num)
   INNER JOIN week_dim AS w ON r.week_num = w.true_week_num
  GROUP BY r.rp_ind,
   s.store_num,
   p.cc_num,
   w.week_num
  HAVING rcpt_po_units > 0 OR rcpt_ds_units > 0) AS t0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num) ,column ( cc_num , store_num , week_num , rp_ind) on rcpt;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS tsfr
--cluster BY cc_num
AS
SELECT t1.cc_num,
 t1.store_num0 AS store_num,
 t1.week_num0 AS week_num,
 t1.rp_ind,
 0 AS sales_flag,
 0 AS inv_flag,
 1 AS rcpt_flag,
 0 AS twist_flag,
 0 AS sff_flag,
 0 AS trans_flag,
 0 AS sales_retail,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS sales_reg_retail,
 0 AS sales_reg_cost,
 0 AS sales_reg_units,
 0 AS sales_pro_retail,
 0 AS sales_pro_cost,
 0 AS sales_pro_units,
 0 AS sales_clr_retail,
 0 AS sales_clr_cost,
 0 AS sales_clr_units,
 0 AS sales_ds_retail,
 0 AS sales_ds_cost,
 0 AS sales_ds_units,
 0 AS sales_gross_retail,
 0 AS sales_gross_cost,
 0 AS sales_gross_units,
 0 AS return_retail,
 0 AS return_cost,
 0 AS return_units,
 0 AS return_ds_retail,
 0 AS return_ds_cost,
 0 AS return_ds_units,
 0 AS boh_retail,
 0 AS boh_cost,
 0 AS boh_units,
 0 AS eoh_retail,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS rcpt_po_retail,
 0 AS rcpt_po_cost,
 0 AS rcpt_po_units,
 0 AS rcpt_ds_retail,
 0 AS rcpt_ds_cost,
 0 AS rcpt_ds_units,
 t1.tsfr_rs_in_retail,
 t1.tsfr_rs_in_cost,
 t1.tsfr_rs_in_units,
 t1.tsfr_rack_in_retail,
 t1.tsfr_rack_in_cost,
 t1.tsfr_rack_in_units,
 t1.tsfr_pah_in_retail,
 t1.tsfr_pah_in_cost,
 t1.tsfr_pah_in_units,
 0 AS twist_instock_traffic,
 0 AS twist_total_traffic,
 0 AS sff_units,
 0 AS bopus_units,
 0 AS dsr_units
FROM (SELECT t.rp_ind,
   s.store_num AS store_num0,
   p.cc_num,
   w.week_num AS week_num0,
   SUM(t.transfer_in_reserve_stock_retail_amt_ty) AS tsfr_rs_in_retail,
   SUM(t.transfer_in_reserve_stock_cost_amt_ty) AS tsfr_rs_in_cost,
   SUM(t.transfer_in_reserve_stock_units_ty) AS tsfr_rs_in_units,
   SUM(t.transfer_in_racking_retail_amt_ty) AS tsfr_rack_in_retail,
   SUM(t.transfer_in_racking_cost_amt_ty) AS tsfr_rack_in_cost,
   SUM(t.transfer_in_racking_units_ty) AS tsfr_rack_in_units,
   SUM(t.transfer_in_pack_and_hold_retail_amt_ty) AS tsfr_pah_in_retail,
   SUM(t.transfer_in_pack_and_hold_cost_amt_ty) AS tsfr_pah_in_cost,
   SUM(t.transfer_in_pack_and_hold_units_ty) AS tsfr_pah_in_units
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_store_week_agg_fact AS t
   INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON t.store_num = s.store_num
   INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(t.rms_sku_num) = LOWER(p.rms_sku_num)
   INNER JOIN week_dim AS w ON t.week_num = w.true_week_num
  GROUP BY t.rp_ind,
   s.store_num,
   p.cc_num,
   w.week_num
  HAVING tsfr_rs_in_units > 0 OR tsfr_rack_in_units > 0 OR tsfr_pah_in_units > 0) AS t1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num) , column ( cc_num , store_num , week_num , rp_ind) on tsfr;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cc_store_month_fact1
--cluster BY cc_num
AS
SELECT cc_num,
 store_num,
 week_num,
  CASE
  WHEN LOWER(rp_ind) = LOWER('Y')
  THEN 1
  ELSE 0
  END AS rp_flag,
 MAX(sales_flag) AS sales_flag,
 MAX(inv_flag) AS inv_flag,
 MAX(rcpt_flag) AS rcpt_flag,
 MAX(twist_flag) AS twist_flag,
 MAX(sff_flag) AS sff_flag,
 MAX(trans_flag) AS trans_flag,
 SUM(sales_retail) AS sales_retail,
 SUM(sales_cost) AS sales_cost,
 SUM(sales_units) AS sales_units,
 SUM(sales_reg_retail) AS sales_reg_retail,
 SUM(sales_reg_cost) AS sales_reg_cost,
 SUM(sales_reg_units) AS sales_reg_units,
 SUM(sales_pro_retail) AS sales_pro_retail,
 SUM(sales_pro_cost) AS sales_pro_cost,
 SUM(sales_pro_units) AS sales_pro_units,
 SUM(sales_clr_retail) AS sales_clr_retail,
 SUM(sales_clr_cost) AS sales_clr_cost,
 SUM(sales_clr_units) AS sales_clr_units,
 SUM(sales_ds_retail) AS sales_ds_retail,
 SUM(sales_ds_cost) AS sales_ds_cost,
 SUM(sales_ds_units) AS sales_ds_units,
 SUM(sales_gross_retail) AS sales_gross_retail,
 SUM(sales_gross_cost) AS sales_gross_cost,
 SUM(sales_gross_units) AS sales_gross_units,
 SUM(return_retail) AS return_retail,
 SUM(return_cost) AS return_cost,
 SUM(return_units) AS return_units,
 SUM(return_ds_retail) AS return_ds_retail,
 SUM(return_ds_cost) AS return_ds_cost,
 SUM(return_ds_units) AS return_ds_units,
 SUM(boh_retail) AS boh_retail,
 SUM(boh_cost) AS boh_cost,
 SUM(boh_units) AS boh_units,
 SUM(eoh_retail) AS eoh_retail,
 SUM(eoh_cost) AS eoh_cost,
 SUM(eoh_units) AS eoh_units,
 SUM(rcpt_po_retail) AS rcpt_po_retail,
 SUM(rcpt_po_cost) AS rcpt_po_cost,
 SUM(rcpt_po_units) AS rcpt_po_units,
 SUM(rcpt_ds_retail) AS rcpt_ds_retail,
 SUM(rcpt_ds_cost) AS rcpt_ds_cost,
 SUM(rcpt_ds_units) AS rcpt_ds_units,
 SUM(tsfr_rs_in_retail) AS tsfr_rs_in_retail,
 SUM(tsfr_rs_in_cost) AS tsfr_rs_in_cost,
 SUM(tsfr_rs_in_units) AS tsfr_rs_in_units,
 SUM(tsfr_rack_in_retail) AS tsfr_rack_in_retail,
 SUM(tsfr_rack_in_cost) AS tsfr_rack_in_cost,
 SUM(tsfr_rack_in_units) AS tsfr_rack_in_units,
 SUM(tsfr_pah_in_retail) AS tsfr_pah_in_retail,
 SUM(tsfr_pah_in_cost) AS tsfr_pah_in_cost,
 SUM(tsfr_pah_in_units) AS tsfr_pah_in_units,
 SUM(twist_instock_traffic) AS twist_instock_traffic,
 SUM(twist_total_traffic) AS twist_total_traffic,
 SUM(sff_units) AS sff_units,
 SUM(bopus_units) AS bopus_units,
 SUM(dsr_units) AS dsr_units
FROM (SELECT *
   FROM sales
   UNION ALL
   SELECT *
   FROM inv
   UNION ALL
   SELECT *
   FROM rcpt
   UNION ALL
   SELECT *
   FROM tsfr) AS u
GROUP BY cc_num,
 store_num,
 week_num,
 rp_flag;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS sales;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS inv;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS rcpt;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS tsfr;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS twist_base
--cluster BY cc_num, store_num, week_num, rp_flag
AS
SELECT p.cc_num,
 s.store_num,
 d.week_num,
 t.rp_idnt AS rp_flag,
 t.mc_instock_ind,
 t.allocated_traffic
FROM `{{params.gcp_project_id}}`.t2dl_das_twist.twist_daily AS t
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(t.rms_sku_num) = LOWER(p.rms_sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim AS d ON t.day_date = d.day_date
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON CAST(t.store_num AS FLOAT64) = s.store_num
WHERE t.day_date BETWEEN (SELECT MIN(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim) AND (SELECT MAX(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim)
 AND t.allocated_traffic > 0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num, store_num, week_num, rp_flag) , column (cc_num) , column (store_num) , column (week_num) , column(rp_flag) on twist_base;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS twist
--cluster BY cc_num
AS
SELECT cc_num,
 store_num,
 week_num,
 rp_flag,
 0 AS sales_flag,
 0 AS inv_flag,
 0 AS rcpt_flag,
 1 AS twist_flag,
 0 AS sff_flag,
 0 AS trans_flag,
 0 AS sales_retail,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS sales_reg_retail,
 0 AS sales_reg_cost,
 0 AS sales_reg_units,
 0 AS sales_pro_retail,
 0 AS sales_pro_cost,
 0 AS sales_pro_units,
 0 AS sales_clr_retail,
 0 AS sales_clr_cost,
 0 AS sales_clr_units,
 0 AS sales_ds_retail,
 0 AS sales_ds_cost,
 0 AS sales_ds_units,
 0 AS sales_gross_retail,
 0 AS sales_gross_cost,
 0 AS sales_gross_units,
 0 AS return_retail,
 0 AS return_cost,
 0 AS return_units,
 0 AS return_ds_retail,
 0 AS return_ds_cost,
 0 AS return_ds_units,
 0 AS boh_retail,
 0 AS boh_cost,
 0 AS boh_units,
 0 AS eoh_retail,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS rcpt_po_retail,
 0 AS rcpt_po_cost,
 0 AS rcpt_po_units,
 0 AS rcpt_ds_retail,
 0 AS rcpt_ds_cost,
 0 AS rcpt_ds_units,
 0 AS tsfr_rs_in_retail,
 0 AS tsfr_rs_in_cost,
 0 AS tsfr_rs_in_units,
 0 AS tsfr_rack_in_retail,
 0 AS tsfr_rack_in_cost,
 0 AS tsfr_rack_in_units,
 0 AS tsfr_pah_in_retail,
 0 AS tsfr_pah_in_cost,
 0 AS tsfr_pah_in_units,
 SUM(mc_instock_ind * allocated_traffic) AS twist_instock_traffic,
 SUM(allocated_traffic) AS twist_total_traffic,
 0 AS sff_units,
 0 AS bopus_units,
 0 AS dsr_units
FROM twist_base AS t
GROUP BY cc_num,
 store_num,
 week_num,
 rp_flag
HAVING twist_total_traffic > 0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num) ,column ( cc_num , store_num , week_num , rp_flag) on twist;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS twist_base;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rp
--cluster BY rms_sku_num, location_num
AS
SELECT t0.rms_sku_num,
 t0.location_num,
 t0.on_sale_date,
 t0.off_sale_date,
 LEAD(t0.on_sale_date, 1) OVER (PARTITION BY t0.rms_sku_num, t0.location_num ORDER BY t0.on_sale_date, t0.off_sale_date
  ) AS next_on_sale_date,
  CASE
  WHEN DATE_DIFF(t0.off_sale_date, (LEAD(t0.on_sale_date, 1) OVER (PARTITION BY t0.rms_sku_num, t0.location_num ORDER BY
        t0.on_sale_date, t0.off_sale_date)), DAY) >= 0
  THEN DATE_SUB(LEAD(t0.on_sale_date, 1) OVER (PARTITION BY t0.rms_sku_num, t0.location_num ORDER BY t0.on_sale_date, t0
      .off_sale_date), INTERVAL 1 DAY)
  ELSE t0.off_sale_date
  END AS off_sale_date_adj
FROM (SELECT rp.rms_sku_num,
   rp.location_num,
   rp.on_sale_date,
   rp.off_sale_date,
   rp.change_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rp_sku_loc_dim_hist AS rp
   INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(rp.rms_sku_num) = LOWER(p.rms_sku_num)
   INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON rp.location_num = s.store_num
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY rp.rms_sku_num, rp.location_num, rp.on_sale_date ORDER BY rp.change_date DESC
        )) = 1) AS t0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sff
--cluster BY cc_num
AS
SELECT p.cc_num,
 s.store_num,
 d.week_num,
  CASE
  WHEN rp.rms_sku_num IS NOT NULL
  THEN 1
  ELSE 0
  END AS rp_flag,
 0 AS sales_flag,
 0 AS inv_flag,
 0 AS rcpt_flag,
 0 AS twist_flag,
 1 AS sff_flag,
 0 AS trans_flag,
 0 AS sales_retail,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS sales_reg_retail,
 0 AS sales_reg_cost,
 0 AS sales_reg_units,
 0 AS sales_pro_retail,
 0 AS sales_pro_cost,
 0 AS sales_pro_units,
 0 AS sales_clr_retail,
 0 AS sales_clr_cost,
 0 AS sales_clr_units,
 0 AS sales_ds_retail,
 0 AS sales_ds_cost,
 0 AS sales_ds_units,
 0 AS sales_gross_retail,
 0 AS sales_gross_cost,
 0 AS sales_gross_units,
 0 AS return_retail,
 0 AS return_cost,
 0 AS return_units,
 0 AS return_ds_retail,
 0 AS return_ds_cost,
 0 AS return_ds_units,
 0 AS boh_retail,
 0 AS boh_cost,
 0 AS boh_units,
 0 AS eoh_retail,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS rcpt_po_retail,
 0 AS rcpt_po_cost,
 0 AS rcpt_po_units,
 0 AS rcpt_ds_retail,
 0 AS rcpt_ds_cost,
 0 AS rcpt_ds_units,
 0 AS tsfr_rs_in_retail,
 0 AS tsfr_rs_in_cost,
 0 AS tsfr_rs_in_units,
 0 AS tsfr_rack_in_retail,
 0 AS tsfr_rack_in_cost,
 0 AS tsfr_rack_in_units,
 0 AS tsfr_pah_in_retail,
 0 AS tsfr_pah_in_cost,
 0 AS tsfr_pah_in_units,
 0 AS twist_instock_traffic,
 0 AS twist_total_traffic,
 SUM(o.order_line_quantity) AS sff_units,
 0 AS bopus_units,
 0 AS dsr_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS o
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(o.rms_sku_num) = LOWER(p.rms_sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON CASE
   WHEN o.shipped_node_num = 209
   THEN 210
   ELSE o.shipped_node_num
   END = s.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim AS d ON o.order_date_pacific = d.day_date
 LEFT JOIN rp ON LOWER(o.rms_sku_num) = LOWER(rp.rms_sku_num) AND s.store_num = rp.location_num AND o.order_date_pacific
   BETWEEN rp.on_sale_date AND rp.off_sale_date_adj
WHERE LOWER(o.delivery_method_code) = LOWER('SHIP')
 AND o.canceled_tmstp_pacific IS NULL
 AND LOWER(o.shipped_node_type_code) IN (LOWER('SS'), LOWER('FL'))
 AND LOWER(o.gift_with_purchase_ind) = LOWER('N')
 AND LOWER(o.beauty_sample_ind) = LOWER('N')
 AND o.shipped_node_num IS NOT NULL
GROUP BY p.cc_num,
 s.store_num,
 d.week_num,
 rp_flag;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num) ,column ( cc_num , store_num , week_num , rp_flag) on sff;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS trans_base
--cluster BY sku_num
AS
SELECT COALESCE(t.sku_num, t.hl_sku_num) AS sku_num,
 t.business_day_date,
  CASE
  WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_order_type
      ) = LOWER('CustInitWebOrder') AND LOWER(t.line_item_fulfillment_type) = LOWER('StorePickup')
  THEN 'bopus'
  WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
     ) = LOWER('StoreShipSend')
  THEN 'sff'
  WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
     ) = LOWER('VendorDropShip')
  THEN 'dsr'
  ELSE NULL
  END AS tran_type,
  CASE
  WHEN LOWER(CASE
     WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_order_type
         ) = LOWER('CustInitWebOrder') AND LOWER(t.line_item_fulfillment_type) = LOWER('StorePickup')
     THEN 'bopus'
     WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
        ) = LOWER('StoreShipSend')
     THEN 'sff'
     WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
        ) = LOWER('VendorDropShip')
     THEN 'dsr'
     ELSE NULL
     END) = LOWER('bopus')
  THEN h.fulfilling_store_num
  WHEN LOWER(CASE
     WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_order_type
         ) = LOWER('CustInitWebOrder') AND LOWER(t.line_item_fulfillment_type) = LOWER('StorePickup')
     THEN 'bopus'
     WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
        ) = LOWER('StoreShipSend')
     THEN 'sff'
     WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
        ) = LOWER('VendorDropShip')
     THEN 'dsr'
     ELSE NULL
     END) = LOWER('sff')
  THEN COALESCE(h.fulfilling_store_num, t.intent_store_num)
  WHEN LOWER(CASE
     WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_order_type
         ) = LOWER('CustInitWebOrder') AND LOWER(t.line_item_fulfillment_type) = LOWER('StorePickup')
     THEN 'bopus'
     WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
        ) = LOWER('StoreShipSend')
     THEN 'sff'
     WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
        ) = LOWER('VendorDropShip')
     THEN 'dsr'
     ELSE NULL
     END) = LOWER('dsr')
  THEN h.ringing_store_num
  ELSE NULL
  END AS store_num,
 t.line_item_quantity
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact AS t
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_hdr_fact AS h ON t.global_tran_id = h.global_tran_id AND t.business_day_date = h
   .business_day_date
WHERE LOWER(t.line_item_merch_nonmerch_ind) = LOWER('MERCH')
 AND LOWER(t.line_item_fulfillment_type) IN (LOWER('StorePickup'), LOWER('StoreShipSend'), LOWER('VendorDropShip'))
 AND t.business_day_date BETWEEN (SELECT MIN(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim) AND (SELECT MAX(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim)
 AND CASE
   WHEN LOWER(CASE
      WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_order_type
          ) = LOWER('CustInitWebOrder') AND LOWER(t.line_item_fulfillment_type) = LOWER('StorePickup')
      THEN 'bopus'
      WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
         ) = LOWER('StoreShipSend')
      THEN 'sff'
      WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
         ) = LOWER('VendorDropShip')
      THEN 'dsr'
      ELSE NULL
      END) = LOWER('bopus')
   THEN h.fulfilling_store_num
   WHEN LOWER(CASE
      WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_order_type
          ) = LOWER('CustInitWebOrder') AND LOWER(t.line_item_fulfillment_type) = LOWER('StorePickup')
      THEN 'bopus'
      WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
         ) = LOWER('StoreShipSend')
      THEN 'sff'
      WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
         ) = LOWER('VendorDropShip')
      THEN 'dsr'
      ELSE NULL
      END) = LOWER('sff')
   THEN COALESCE(h.fulfilling_store_num, t.intent_store_num)
   WHEN LOWER(CASE
      WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_order_type
          ) = LOWER('CustInitWebOrder') AND LOWER(t.line_item_fulfillment_type) = LOWER('StorePickup')
      THEN 'bopus'
      WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
         ) = LOWER('StoreShipSend')
      THEN 'sff'
      WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH')) AND LOWER(t.line_item_fulfillment_type
         ) = LOWER('VendorDropShip')
      THEN 'dsr'
      ELSE NULL
      END) = LOWER('dsr')
   THEN h.ringing_store_num
   ELSE NULL
   END IN (SELECT store_num
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim)
 AND h.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (sku_num) , column(store_num) , column(business_day_date) , column(sku_num, store_num, business_day_date) on trans_base;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS trans
--cluster BY cc_num
AS
SELECT p.cc_num,
 s.store_num,
 d.week_num,
  CASE
  WHEN rp.rms_sku_num IS NOT NULL
  THEN 1
  ELSE 0
  END AS rp_flag,
 0 AS sales_flag,
 0 AS inv_flag,
 0 AS rcpt_flag,
 0 AS twist_flag,
 0 AS sff_flag,
 1 AS trans_flag,
 0 AS sales_retail,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS sales_reg_retail,
 0 AS sales_reg_cost,
 0 AS sales_reg_units,
 0 AS sales_pro_retail,
 0 AS sales_pro_cost,
 0 AS sales_pro_units,
 0 AS sales_clr_retail,
 0 AS sales_clr_cost,
 0 AS sales_clr_units,
 0 AS sales_ds_retail,
 0 AS sales_ds_cost,
 0 AS sales_ds_units,
 0 AS sales_gross_retail,
 0 AS sales_gross_cost,
 0 AS sales_gross_units,
 0 AS return_retail,
 0 AS return_cost,
 0 AS return_units,
 0 AS return_ds_retail,
 0 AS return_ds_cost,
 0 AS return_ds_units,
 0 AS boh_retail,
 0 AS boh_cost,
 0 AS boh_units,
 0 AS eoh_retail,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS rcpt_po_retail,
 0 AS rcpt_po_cost,
 0 AS rcpt_po_units,
 0 AS rcpt_ds_retail,
 0 AS rcpt_ds_cost,
 0 AS rcpt_ds_units,
 0 AS tsfr_rs_in_retail,
 0 AS tsfr_rs_in_cost,
 0 AS tsfr_rs_in_units,
 0 AS tsfr_rack_in_retail,
 0 AS tsfr_rack_in_cost,
 0 AS tsfr_rack_in_units,
 0 AS tsfr_pah_in_retail,
 0 AS tsfr_pah_in_cost,
 0 AS tsfr_pah_in_units,
 0 AS twist_instock_traffic,
 0 AS twist_total_traffic,
 0 AS sff_units,
 SUM(CASE
   WHEN LOWER(t.tran_type) = LOWER('bopus')
   THEN t.line_item_quantity
   ELSE NULL
   END) AS bopus_units,
 SUM(CASE
   WHEN LOWER(t.tran_type) = LOWER('dsr')
   THEN t.line_item_quantity
   ELSE NULL
   END) AS dsr_units
FROM trans_base AS t
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(t.sku_num) = LOWER(p.rms_sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON t.store_num = s.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim AS d ON t.business_day_date = d.day_date
 LEFT JOIN rp ON LOWER(t.sku_num) = LOWER(rp.rms_sku_num) AND t.store_num = rp.location_num AND t.business_day_date
   BETWEEN rp.on_sale_date AND rp.off_sale_date_adj
GROUP BY p.cc_num,
 s.store_num,
 d.week_num,
 rp_flag
HAVING bopus_units > 0 OR dsr_units > 0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num) ,column ( cc_num , store_num , week_num , rp_flag) on trans;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS cc_store_month_fact
--cluster BY cc_num
AS
SELECT cc_num,
 store_num,
 week_num,
 rp_flag,
 MAX(sales_flag) AS sales_flag,
 MAX(inv_flag) AS inv_flag,
 MAX(rcpt_flag) AS rcpt_flag,
 MAX(twist_flag) AS twist_flag,
 MAX(sff_flag) AS sff_flag,
 MAX(trans_flag) AS trans_flag,
 SUM(sales_retail) AS sales_retail,
 SUM(sales_cost) AS sales_cost,
 SUM(sales_units) AS sales_units,
 SUM(sales_reg_retail) AS sales_reg_retail,
 SUM(sales_reg_cost) AS sales_reg_cost,
 SUM(sales_reg_units) AS sales_reg_units,
 SUM(sales_pro_retail) AS sales_pro_retail,
 SUM(sales_pro_cost) AS sales_pro_cost,
 SUM(sales_pro_units) AS sales_pro_units,
 SUM(sales_clr_retail) AS sales_clr_retail,
 SUM(sales_clr_cost) AS sales_clr_cost,
 SUM(sales_clr_units) AS sales_clr_units,
 SUM(sales_ds_retail) AS sales_ds_retail,
 SUM(sales_ds_cost) AS sales_ds_cost,
 SUM(sales_ds_units) AS sales_ds_units,
 SUM(sales_gross_retail) AS sales_gross_retail,
 SUM(sales_gross_cost) AS sales_gross_cost,
 SUM(sales_gross_units) AS sales_gross_units,
 SUM(return_retail) AS return_retail,
 SUM(return_cost) AS return_cost,
 SUM(return_units) AS return_units,
 SUM(return_ds_retail) AS return_ds_retail,
 SUM(return_ds_cost) AS return_ds_cost,
 SUM(return_ds_units) AS return_ds_units,
 SUM(boh_retail) AS boh_retail,
 SUM(boh_cost) AS boh_cost,
 SUM(boh_units) AS boh_units,
 SUM(eoh_retail) AS eoh_retail,
 SUM(eoh_cost) AS eoh_cost,
 SUM(eoh_units) AS eoh_units,
 SUM(rcpt_po_retail) AS rcpt_po_retail,
 SUM(rcpt_po_cost) AS rcpt_po_cost,
 SUM(rcpt_po_units) AS rcpt_po_units,
 SUM(rcpt_ds_retail) AS rcpt_ds_retail,
 SUM(rcpt_ds_cost) AS rcpt_ds_cost,
 SUM(rcpt_ds_units) AS rcpt_ds_units,
 SUM(tsfr_rs_in_retail) AS tsfr_rs_in_retail,
 SUM(tsfr_rs_in_cost) AS tsfr_rs_in_cost,
 SUM(tsfr_rs_in_units) AS tsfr_rs_in_units,
 SUM(tsfr_rack_in_retail) AS tsfr_rack_in_retail,
 SUM(tsfr_rack_in_cost) AS tsfr_rack_in_cost,
 SUM(tsfr_rack_in_units) AS tsfr_rack_in_units,
 SUM(tsfr_pah_in_retail) AS tsfr_pah_in_retail,
 SUM(tsfr_pah_in_cost) AS tsfr_pah_in_cost,
 SUM(tsfr_pah_in_units) AS tsfr_pah_in_units,
 SUM(twist_instock_traffic) AS twist_instock_traffic,
 SUM(twist_total_traffic) AS twist_total_traffic,
 SUM(sff_units) AS sff_units,
 SUM(bopus_units) AS bopus_units,
 SUM(dsr_units) AS dsr_units
FROM (SELECT *
   FROM cc_store_month_fact1
   UNION ALL
   SELECT *
   FROM twist
   UNION ALL
   SELECT *
   FROM sff
   UNION ALL
   SELECT *
   FROM trans) AS u
GROUP BY cc_num,
 store_num,
 week_num,
 rp_flag;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(cc_num) , column (week_num) , column (store_num) on cc_store_month_fact;
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS twist;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS anniv
--cluster BY cc_num, week_num
AS
SELECT p.cc_num,
 d.week_num,
 MAX(a.anniv_item_ind) AS anniv_flag
FROM `{{params.gcp_project_id}}`.t2dl_das_scaled_events.anniversary_sku_chnl_date AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(a.sku_idnt) = LOWER(p.rms_sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim AS d ON a.day_dt = d.day_date
WHERE LOWER(a.channel_country) = LOWER('US')
 AND LOWER(a.selling_channel) = LOWER('STORE')
GROUP BY p.cc_num,
 d.week_num;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num, week_num) on anniv;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.fls_assortment_pilot_merch_base;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.fls_assortment_pilot_merch_base
(SELECT f.cc_num,
  f.store_num,
  s.channel_num,
  f.week_num,
  w.month_num,
  COALESCE(f.rp_flag, 0) AS rp_flag,
  f.sales_flag,
  f.inv_flag,
  f.rcpt_flag,
  f.twist_flag,
  f.sff_flag,
  f.trans_flag,
  f.sales_retail,
  f.sales_cost,
  cast(trunc(f.sales_units) AS INTEGER) AS sales_units,
  f.sales_reg_retail,
  f.sales_reg_cost,
  cast(trunc(f.sales_reg_units) AS INTEGER) AS sales_reg_units,
  f.sales_pro_retail,
  f.sales_pro_cost,
  cast(trunc(f.sales_pro_units) AS INTEGER) AS sales_pro_units,
  f.sales_clr_retail,
  f.sales_clr_cost,
  cast(trunc(f.sales_clr_units) AS INTEGER) AS sales_clr_units,
  f.sales_ds_retail,
  f.sales_ds_cost,
  cast(trunc(f.sales_ds_units) AS INTEGER) AS sales_ds_units,
  f.sales_gross_retail,
  f.sales_gross_cost,
  cast(trunc(f.sales_gross_units) AS INTEGER) AS sales_gross_units,
  f.return_retail,
  f.return_cost,
  cast(trunc(f.return_units) AS INTEGER) AS return_units,
  f.return_ds_retail,
  f.return_ds_cost,
  cast(trunc(f.return_ds_units) AS INTEGER) AS return_ds_units,
  f.boh_retail,
  f.boh_cost,
  f.boh_units,
  f.eoh_retail,
  f.eoh_cost,
  f.eoh_units,
  f.rcpt_po_retail,
  f.rcpt_po_cost,
  f.rcpt_po_units,
  f.rcpt_ds_retail,
  f.rcpt_ds_cost,
  f.rcpt_ds_units,
  f.tsfr_rs_in_retail,
  f.tsfr_rs_in_cost,
  f.tsfr_rs_in_units,
  f.tsfr_rack_in_retail,
  f.tsfr_rack_in_cost,
  f.tsfr_rack_in_units,
  f.tsfr_pah_in_retail,
  f.tsfr_pah_in_cost,
  f.tsfr_pah_in_units,
  f.twist_instock_traffic,
  f.twist_total_traffic,
  f.sff_units,
  f.bopus_units,
  f.dsr_units,
  CAST(ROUND(COALESCE((SUM(f.sales_gross_retail) OVER (PARTITION BY s.channel_num, f.cc_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(f.sales_gross_units) OVER (PARTITION BY s.channel_num, f.cc_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), (SUM(f
        .eoh_retail) OVER (PARTITION BY s.channel_num, f.cc_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(f.eoh_units) OVER (PARTITION BY s.channel_num, f.cc_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), (SUM(f.boh_retail) OVER (PARTITION BY s.channel_num
          , f.cc_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(f.boh_units) OVER (PARTITION BY s.channel_num, f.cc_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), (SUM(f.rcpt_po_retail + f.rcpt_ds_retail + f.tsfr_rs_in_retail + f.tsfr_rack_in_retail
           + f.tsfr_pah_in_retail) OVER (PARTITION BY s.channel_num, f.cc_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(f.rcpt_po_units + f.rcpt_ds_units + f.tsfr_rs_in_units + f.tsfr_rack_in_units + f.tsfr_pah_in_units) OVER (PARTITION BY s.channel_num, f.cc_num
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), (SUM(f.return_retail) OVER (PARTITION BY s.channel_num, f.cc_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / NULLIF(SUM(f.return_units) OVER (PARTITION BY s.channel_num, f.cc_num RANGE BETWEEN
        UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)), 2) AS NUMERIC) AS aur_dim,
  SUM(f.sales_units + f.eoh_units) OVER (PARTITION BY f.cc_num, f.store_num, f.week_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS qual_ats_units,
  COALESCE(a.anniv_flag, 0) AS anniv_flag,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS data_update_tmstp
 FROM cc_store_month_fact AS f
  LEFT JOIN anniv AS a ON LOWER(f.cc_num) = LOWER(a.cc_num) AND f.week_num = a.week_num
  INNER JOIN week_dim AS w ON f.week_num = w.week_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON f.store_num = s.store_num);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (cc_num, store_num, week_num) , column (cc_num) , column (store_num) , column (week_num) on t2dl_das_ccs_categories.fls_assortment_pilot_merch_base;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_cc_dim;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_cc_dim
(SELECT cc_num,
  MAX(style_desc) AS style_desc,
  MAX(style_group_num) AS style_group_num,
  MAX(web_style_num) AS web_style_num,
  MAX(color_num) AS color_num,
  MAX(color_desc) AS color_desc,
  MAX(div_num) AS div_num,
  MAX(div_label) AS div_label,
  MAX(sdiv_num) AS sdiv_num,
  MAX(sdiv_label) AS sdiv_label,
  MAX(dept_num) AS dept_num,
  MAX(dept_label) AS dept_label,
  MAX(class_num) AS class_num,
  MAX(class_label) AS class_label,
  MAX(sbclass_num) AS sbclass_num,
  MAX(sbclass_label) AS sbclass_label,
  MAX(supplier_num) AS supplier_num,
  MAX(supplier_name) AS supplier_name,
  MAX(brand_name) AS brand_name,
  MAX(anchor_brand_ind) AS anchor_brand_ind,
  MAX(npg_ind) AS npg_ind,
  MAX(quantrix_category) AS quantrix_category,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim
 WHERE cc_num IN (SELECT DISTINCT cc_num
    FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.fls_assortment_pilot_merch_base)
 GROUP BY cc_num);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
