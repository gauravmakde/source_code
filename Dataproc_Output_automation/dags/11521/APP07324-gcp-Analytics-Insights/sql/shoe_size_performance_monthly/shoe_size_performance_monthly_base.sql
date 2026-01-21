BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=shoe_size_performance_monthly_base_11521_ACE_ENG;
---     Task_Name=shoe_size_performance_monthly_base;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS variable_start_end_week
-- cluster by start_week_num, end_week_num
AS
SELECT MAX(CASE
   WHEN day_date < CAST({{params.start_date}} AS DATE)
   THEN week_idnt
   ELSE NULL
   END) AS start_week_num,
 MAX(CASE
   WHEN day_date < CAST({{params.end_date}} AS DATE)
   THEN week_idnt
   ELSE NULL
   END) AS end_week_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lkup
-- cluster by week_idnt_true
AS
SELECT DISTINCT week_idnt AS week_idnt_true,
 week_end_day_date,
 month_end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE week_idnt BETWEEN (SELECT start_week_num
   FROM variable_start_end_week) AND (SELECT end_week_num
   FROM variable_start_end_week);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS store_lkup
-- cluster by store_num
AS
SELECT DISTINCT store_num,
 TRIM(FORMAT('%11d', store_num) || ', ' || store_name) AS store_label,
 channel_num,
   TRIM(FORMAT('%11d', channel_num)) || ', ' || channel_desc AS channel_label,
 store_address_county,
 TRIM(FORMAT('%11d', region_num) || ', ' || region_desc) AS region_label,
 channel_country AS country,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM')
  THEN 'N'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK')
  THEN 'NR'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(store_country_code) = LOWER('US')
 AND channel_num IN (110, 111, 120, 121, 130, 140, 210, 211, 220, 221, 250, 260, 261, 310, 311)
 AND LOWER(store_abbrev_name) <> LOWER('CLOSED')
 AND channel_num IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sku_lkup
-- cluster by rms_sku_num
AS
SELECT DISTINCT rms_sku_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS ps
WHERE LOWER(channel_country) = LOWER('US')
 AND div_num IN (310, 345);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS PRIMARY INDEX (rms_sku_num)  ON sku_lkup;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sales
-- cluster by rms_sku_num, store_num, week_num
AS
SELECT s.rms_sku_num,
 s.store_num,
 s.week_num,
 s.rp_ind,
 s.dropship_ind AS ds_ind,
 RPAD('R', 3, ' ') AS price_type,
 SUM(s.net_sales_tot_regular_retl) AS sales_dollars,
 SUM(s.net_sales_tot_regular_cost) AS sales_cost,
 SUM(s.net_sales_tot_regular_units) AS sales_units,
 SUM(s.returns_tot_regular_retl) AS return_dollars,
 SUM(s.returns_tot_regular_cost) AS return_cost,
 SUM(s.returns_tot_regular_units) AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 SUM(CASE
   WHEN LOWER(s.wac_avlbl_ind) = LOWER('Y')
   THEN s.net_sales_tot_regular_retl
   ELSE NULL
   END) AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s
 INNER JOIN date_lkup AS dt ON s.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON s.store_num = store.store_num
 INNER JOIN sku_lkup AS sku ON LOWER(s.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE s.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
GROUP BY s.rms_sku_num,
 s.store_num,
 s.week_num,
 s.rp_ind,
 ds_ind,
 price_type
UNION ALL
SELECT s0.rms_sku_num,
 s0.store_num,
 s0.week_num,
 s0.rp_ind,
 s0.dropship_ind AS ds_ind,
 RPAD('C', 2, ' ') AS price_type,
 SUM(s0.net_sales_tot_clearance_retl) AS sales_dollars,
 SUM(s0.net_sales_tot_clearance_cost) AS sales_cost,
 SUM(s0.net_sales_tot_clearance_units) AS sales_units,
 SUM(s0.returns_tot_clearance_retl) AS return_dollars,
 SUM(s0.returns_tot_clearance_cost) AS return_cost,
 SUM(s0.returns_tot_clearance_units) AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 SUM(CASE
   WHEN LOWER(s0.wac_avlbl_ind) = LOWER('Y')
   THEN s0.net_sales_tot_clearance_retl
   ELSE NULL
   END) AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s0
 INNER JOIN date_lkup AS dt0 ON s0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup AS store0 ON s0.store_num = store0.store_num
 INNER JOIN sku_lkup AS sku0 ON LOWER(s0.rms_sku_num) = LOWER(sku0.rms_sku_num)
WHERE s0.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
GROUP BY s0.rms_sku_num,
 s0.store_num,
 s0.week_num,
 s0.rp_ind,
 ds_ind,
 price_type
UNION ALL
SELECT s1.rms_sku_num,
 s1.store_num,
 s1.week_num,
 s1.rp_ind,
 s1.dropship_ind AS ds_ind,
 RPAD('P', 3, ' ') AS price_type,
 SUM(s1.net_sales_tot_promo_retl) AS sales_dollars,
 SUM(s1.net_sales_tot_promo_cost) AS sales_cost,
 SUM(s1.net_sales_tot_promo_units) AS sales_units,
 SUM(s1.returns_tot_promo_retl) AS return_dollars,
 SUM(s1.returns_tot_promo_cost) AS return_cost,
 SUM(s1.returns_tot_promo_units) AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 SUM(CASE
   WHEN LOWER(s1.wac_avlbl_ind) = LOWER('Y')
   THEN s1.net_sales_tot_promo_retl
   ELSE NULL
   END) AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw AS s1
 INNER JOIN date_lkup AS dt1 ON s1.week_num = dt1.week_idnt_true
 INNER JOIN store_lkup AS store1 ON s1.store_num = store1.store_num
 INNER JOIN sku_lkup AS sku1 ON LOWER(s1.rms_sku_num) = LOWER(sku1.rms_sku_num)
WHERE s1.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
GROUP BY s1.rms_sku_num,
 s1.store_num,
 s1.week_num,
 s1.rp_ind,
 ds_ind,
 price_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(rms_sku_num, store_num, week_num) ,column(rms_sku_num) ,column(store_num) ,column(week_num) on sales;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS demand
-- cluster by rms_sku_num, store_num, week_num
AS
SELECT d.rms_sku_num,
 d.store_num,
 d.week_num,
 d.rp_ind,
 RPAD('Y', 1, ' ') AS ds_ind,
 RPAD('R', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 COALESCE(d.jwn_demand_dropship_fulfilled_regular_retail_amt_ty, 0) AS demand_dollars,
 COALESCE(d.jwn_demand_dropship_fulfilled_regular_units_ty, 0) AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d
 INNER JOIN date_lkup AS dt ON d.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON d.store_num = store.store_num
 INNER JOIN sku_lkup AS sku ON LOWER(d.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE d.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
 AND COALESCE(d.jwn_demand_dropship_fulfilled_regular_units_ty, 0) <> 0
UNION ALL
SELECT d0.rms_sku_num,
 d0.store_num,
 d0.week_num,
 d0.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 RPAD('R', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
  COALESCE(d0.jwn_demand_regular_retail_amt_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_regular_retail_amt_ty, 0
   ) AS demand_dollars,
  COALESCE(d0.jwn_demand_regular_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_regular_units_ty, 0) AS
 demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d0
 INNER JOIN date_lkup AS dt0 ON d0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup AS store0 ON d0.store_num = store0.store_num
 INNER JOIN sku_lkup AS sku0 ON LOWER(d0.rms_sku_num) = LOWER(sku0.rms_sku_num)
WHERE d0.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
 AND COALESCE(d0.jwn_demand_regular_units_ty, 0) - COALESCE(d0.jwn_demand_dropship_fulfilled_regular_units_ty, 0) <> 0
UNION ALL
SELECT d1.rms_sku_num,
 d1.store_num,
 d1.week_num,
 d1.rp_ind,
 RPAD('Y', 1, ' ') AS ds_ind,
 RPAD('C', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 COALESCE(d1.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty, 0) AS demand_dollars,
 COALESCE(d1.jwn_demand_dropship_fulfilled_clearance_units_ty, 0) AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d1
 INNER JOIN date_lkup AS dt1 ON d1.week_num = dt1.week_idnt_true
 INNER JOIN store_lkup AS store1 ON d1.store_num = store1.store_num
 INNER JOIN sku_lkup AS sku1 ON LOWER(d1.rms_sku_num) = LOWER(sku1.rms_sku_num)
WHERE d1.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
 AND COALESCE(d1.jwn_demand_dropship_fulfilled_clearance_units_ty, 0) <> 0
UNION ALL
SELECT d2.rms_sku_num,
 d2.store_num,
 d2.week_num,
 d2.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 RPAD('C', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
  COALESCE(d2.jwn_demand_clearance_retail_amt_ty, 0) - COALESCE(d2.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty
   , 0) AS demand_dollars,
  COALESCE(d2.jwn_demand_clearance_units_ty, 0) - COALESCE(d2.jwn_demand_dropship_fulfilled_clearance_units_ty, 0) AS
 demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d2
 INNER JOIN date_lkup AS dt2 ON d2.week_num = dt2.week_idnt_true
 INNER JOIN store_lkup AS store2 ON d2.store_num = store2.store_num
 INNER JOIN sku_lkup AS sku2 ON LOWER(d2.rms_sku_num) = LOWER(sku2.rms_sku_num)
WHERE d2.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
 AND COALESCE(d2.jwn_demand_clearance_units_ty, 0) - COALESCE(d2.jwn_demand_dropship_fulfilled_clearance_units_ty, 0) <>
  0
UNION ALL
SELECT d3.rms_sku_num,
 d3.store_num,
 d3.week_num,
 d3.rp_ind,
 RPAD('Y', 1, ' ') AS ds_ind,
 RPAD('P', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 COALESCE(d3.jwn_demand_dropship_fulfilled_promo_retail_amt_ty, 0) AS demand_dollars,
 COALESCE(d3.jwn_demand_dropship_fulfilled_promo_units_ty, 0) AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dropship_dollars,
 0 AS receipt_dropship_units,
 0 AS receipt_dropship_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d3
 INNER JOIN date_lkup AS dt3 ON d3.week_num = dt3.week_idnt_true
 INNER JOIN store_lkup AS store3 ON d3.store_num = store3.store_num
 INNER JOIN sku_lkup AS sku3 ON LOWER(d3.rms_sku_num) = LOWER(sku3.rms_sku_num)
WHERE d3.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
 AND COALESCE(d3.jwn_demand_dropship_fulfilled_promo_units_ty, 0) <> 0
UNION ALL
SELECT d4.rms_sku_num,
 d4.store_num,
 d4.week_num,
 d4.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 RPAD('P', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
  COALESCE(d4.jwn_demand_promo_retail_amt_ty, 0) - COALESCE(d4.jwn_demand_dropship_fulfilled_promo_retail_amt_ty, 0) AS
 demand_dollars,
  COALESCE(d4.jwn_demand_promo_units_ty, 0) - COALESCE(d4.jwn_demand_dropship_fulfilled_promo_units_ty, 0) AS
 demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_jwn_demand_sku_store_week_agg_fact AS d4
 INNER JOIN date_lkup AS dt4 ON d4.week_num = dt4.week_idnt_true
 INNER JOIN store_lkup AS store4 ON d4.store_num = store4.store_num
 INNER JOIN sku_lkup AS sku4 ON LOWER(d4.rms_sku_num) = LOWER(sku4.rms_sku_num)
WHERE d4.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
 AND COALESCE(d4.jwn_demand_promo_units_ty, 0) - COALESCE(d4.jwn_demand_dropship_fulfilled_promo_units_ty, 0) <> 0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(rms_sku_num, store_num, week_num) ,column(rms_sku_num) ,column(store_num) ,column(week_num) on  demand;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS inventory
-- cluster by rms_sku_num, store_num, week_num
AS
SELECT i.rms_sku_num,
 s.store_num,
 i.week_num,
 i.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 RPAD('R', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_units,
 0 AS demand_dollars,
 0 AS demand_units,
 SUM(i.eoh_regular_retail) AS eoh_dollars,
 SUM(i.eoh_regular_cost) AS eoh_cost,
 SUM(i.eoh_regular_units) AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_cost,
 0 AS receipt_units,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_cost,
 0 AS transfer_pah_units,
 0 AS oo_dollars,
 0 AS oo_cost,
 0 AS oo_units,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_cost,
 0 AS oo_4weeks_units,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_cost,
 0 AS oo_12weeks_units,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS i
 INNER JOIN date_lkup AS dt ON i.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS s ON CAST(i.store_num AS FLOAT64) = s.store_num
 INNER JOIN sku_lkup AS sku ON LOWER(i.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE i.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
GROUP BY i.rms_sku_num,
 s.store_num,
 i.week_num,
 i.rp_ind,
 ds_ind,
 price_type
HAVING eoh_units <> 0
UNION ALL
SELECT i0.rms_sku_num,
 s0.store_num,
 i0.week_num,
 i0.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 RPAD('C', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_units,
 0 AS demand_dollars,
 0 AS demand_units,
 SUM(i0.eoh_clearance_retail) AS eoh_dollars,
 SUM(i0.eoh_clearance_cost) AS eoh_cost,
 SUM(i0.eoh_clearance_units) AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_cost,
 0 AS receipt_units,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_cost,
 0 AS transfer_pah_units,
 0 AS oo_dollars,
 0 AS oo_cost,
 0 AS oo_units,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_cost,
 0 AS oo_4weeks_units,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_cost,
 0 AS oo_12weeks_units,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS i0
 INNER JOIN date_lkup AS dt0 ON i0.week_num = dt0.week_idnt_true
 INNER JOIN store_lkup AS s0 ON CAST(i0.store_num AS FLOAT64) = s0.store_num
 INNER JOIN sku_lkup AS sku0 ON LOWER(i0.rms_sku_num) = LOWER(sku0.rms_sku_num)
WHERE i0.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
GROUP BY i0.rms_sku_num,
 s0.store_num,
 i0.week_num,
 i0.rp_ind,
 ds_ind,
 price_type
HAVING eoh_units <> 0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS PRIMARY INDEX(rms_sku_num, store_num, week_num) ,COLUMN(rms_sku_num) ,COLUMN(store_num) ,COLUMN(week_num) ON inventory;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS receipts_stg
-- cluster by rms_sku_num
AS
SELECT b.rms_sku_num,
 b.store_num,
 b.week_num,
 b.dropship_ind,
 b.rp_ind,
 b.receipts_regular_units,
 b.receipts_crossdock_regular_units,
 b.receipts_regular_retail,
 b.receipts_crossdock_regular_retail,
 b.receipts_regular_cost,
 b.receipts_crossdock_regular_cost,
 b.receipts_clearance_units,
 b.receipts_crossdock_clearance_units,
 b.receipts_clearance_retail,
 b.receipts_crossdock_clearance_retail,
 b.receipts_clearance_cost,
 b.receipts_crossdock_clearance_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw AS b
 INNER JOIN store_lkup AS store ON b.store_num = store.store_num
WHERE b.week_num IN (SELECT DISTINCT week_idnt_true
   FROM date_lkup)
 AND (b.receipts_regular_units > 0 OR b.receipts_clearance_units > 0 OR b.receipts_crossdock_regular_units > 0 OR b.receipts_crossdock_clearance_units
     > 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,STORE_NUM ,WEEK_NUM,DROPSHIP_IND ,RP_IND) ON receipts_stg;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS transfer_stg
-- cluster by rms_sku_num
AS
SELECT b.rms_sku_num,
 b.store_num,
 b.week_num,
 b.rp_ind,
 b.reservestock_transfer_in_regular_units,
 b.reservestock_transfer_in_clearance_units,
 b.reservestock_transfer_in_regular_retail,
 b.reservestock_transfer_in_clearance_retail,
 b.reservestock_transfer_in_regular_cost,
 b.reservestock_transfer_in_clearance_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS b
 INNER JOIN store_lkup AS store ON b.store_num = store.store_num
WHERE b.week_num IN (SELECT DISTINCT week_idnt_true
   FROM date_lkup)
 AND (b.reservestock_transfer_in_regular_units > 0 OR b.reservestock_transfer_in_clearance_units > 0);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,STORE_NUM ,WEEK_NUM, RP_IND) ON transfer_stg;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS receipt
-- cluster by rms_sku_num, store_num, week_num
AS
SELECT base.rms_sku_num,
 base.store_num,
 base.week_num,
 base.rp_ind,
 base.dropship_ind AS ds_ind,
 RPAD('R', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 SUM(base.receipts_regular_retail + base.receipts_crossdock_regular_retail) AS receipt_dollars,
 SUM(base.receipts_regular_units + base.receipts_crossdock_regular_units) AS receipt_units,
 SUM(base.receipts_regular_cost + base.receipts_crossdock_regular_cost) AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM receipts_stg AS base
 INNER JOIN sku_lkup AS sku ON LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
GROUP BY base.rms_sku_num,
 base.store_num,
 base.week_num,
 base.rp_ind,
 ds_ind,
 price_type
UNION ALL
SELECT base0.rms_sku_num,
 base0.store_num,
 base0.week_num,
 base0.rp_ind,
 base0.dropship_ind AS ds_ind,
 RPAD('C', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 SUM(base0.receipts_clearance_retail + base0.receipts_crossdock_clearance_retail) AS receipt_dollars,
 SUM(base0.receipts_clearance_units + base0.receipts_crossdock_clearance_units) AS receipt_units,
 SUM(base0.receipts_clearance_cost + base0.receipts_crossdock_clearance_cost) AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM receipts_stg AS base0
 INNER JOIN sku_lkup AS sku0 ON LOWER(base0.rms_sku_num) = LOWER(sku0.rms_sku_num)
GROUP BY base0.rms_sku_num,
 base0.store_num,
 base0.week_num,
 base0.rp_ind,
 ds_ind,
 price_type
UNION ALL
SELECT base1.rms_sku_num,
 base1.store_num,
 base1.week_num,
 base1.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 RPAD('C', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 SUM(base1.reservestock_transfer_in_clearance_retail) AS receipt_dollars,
 SUM(base1.reservestock_transfer_in_clearance_units) AS receipt_units,
 SUM(base1.reservestock_transfer_in_clearance_cost) AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM transfer_stg AS base1
 INNER JOIN sku_lkup AS sku1 ON LOWER(base1.rms_sku_num) = LOWER(sku1.rms_sku_num)
GROUP BY base1.rms_sku_num,
 base1.store_num,
 base1.week_num,
 base1.rp_ind,
 ds_ind,
 price_type
UNION ALL
SELECT base2.rms_sku_num,
 base2.store_num,
 base2.week_num,
 base2.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 RPAD('R', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 SUM(base2.reservestock_transfer_in_regular_retail) AS receipt_dollars,
 SUM(base2.reservestock_transfer_in_regular_units) AS receipt_units,
 SUM(base2.reservestock_transfer_in_regular_cost) AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM transfer_stg AS base2
 INNER JOIN sku_lkup AS sku2 ON LOWER(base2.rms_sku_num) = LOWER(sku2.rms_sku_num)
GROUP BY base2.rms_sku_num,
 base2.store_num,
 base2.week_num,
 base2.rp_ind,
 ds_ind,
 price_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(rms_sku_num, store_num, week_num) ,column(rms_sku_num) ,column(store_num) ,column(week_num) on    receipt;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS transfer_ph
-- cluster by rms_sku_num, store_num, week_num
AS
SELECT s.rms_sku_num,
 s.store_num,
 s.week_num,
 s.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 RPAD('N/A', 3, ' ') AS price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 SUM(s.packandhold_transfer_in_retail) AS transfer_pah_dollars,
 SUM(s.packandhold_transfer_in_units) AS transfer_pah_units,
 SUM(s.packandhold_transfer_in_cost) AS transfer_pah_cost,
 0 AS oo_dollars,
 0 AS oo_units,
 0 AS oo_cost,
 0 AS oo_4weeks_dollars,
 0 AS oo_4weeks_units,
 0 AS oo_4weeks_cost,
 0 AS oo_12weeks_dollars,
 0 AS oo_12weeks_units,
 0 AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw AS s
 INNER JOIN date_lkup AS dt ON s.week_num = dt.week_idnt_true
 INNER JOIN store_lkup AS store ON s.store_num = store.store_num
 INNER JOIN sku_lkup AS sku ON LOWER(s.rms_sku_num) = LOWER(sku.rms_sku_num)
WHERE s.week_num BETWEEN (SELECT MIN(week_idnt_true)
   FROM date_lkup) AND (SELECT MAX(week_idnt_true)
   FROM date_lkup)
GROUP BY s.rms_sku_num,
 s.store_num,
 s.week_num,
 s.rp_ind,
 ds_ind,
 price_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(rms_sku_num, store_num, week_num) ,column(rms_sku_num) ,column(store_num) ,column(week_num) on transfer_PH;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS oo_stage
-- cluster by rms_sku_num
AS
SELECT base.otb_eow_date,
 base.store_num,
 base.rms_sku_num,
 base.anticipated_price_type AS price_type,
  CASE
  WHEN LOWER(base.order_type) = LOWER('AUTOMATIC_REORDER')
  THEN 'Y'
  ELSE 'N'
  END AS rp_ind,
 base.quantity_open AS on_order_units,
 ROUND(CAST(base.quantity_open * base.anticipated_retail_amt AS NUMERIC), 2) AS on_order_retail_dollars,
 ROUND(CAST(base.quantity_open * base.unit_cost_amt AS NUMERIC), 2) AS on_order_cost_dollars,
  CASE
  WHEN base.otb_eow_date <= (SELECT DATE_ADD(MAX(week_end_day_date), INTERVAL 28 DAY)
    FROM date_lkup)
  THEN base.quantity_open
  ELSE 0
  END AS on_order_4wk_units,
  CASE
  WHEN base.otb_eow_date <= (SELECT DATE_ADD(MAX(week_end_day_date), INTERVAL 28 DAY)
    FROM date_lkup)
  THEN ROUND(CAST(base.quantity_open * base.anticipated_retail_amt AS NUMERIC), 2)
  ELSE 0
  END AS on_order_4wk_retail_dollars,
  CASE
  WHEN base.otb_eow_date <= (SELECT DATE_ADD(MAX(week_end_day_date), INTERVAL 28 DAY)
    FROM date_lkup)
  THEN ROUND(CAST(base.quantity_open * base.unit_cost_amt AS NUMERIC), 2)
  ELSE 0
  END AS on_order_4wk_cost_dollars,
  CASE
  WHEN base.otb_eow_date <= (SELECT DATE_ADD(MAX(week_end_day_date), INTERVAL 84 DAY)
    FROM date_lkup)
  THEN base.quantity_open
  ELSE 0
  END AS on_order_12wk_units,
  CASE
  WHEN base.otb_eow_date <= (SELECT DATE_ADD(MAX(week_end_day_date), INTERVAL 84 DAY)
    FROM date_lkup)
  THEN ROUND(CAST(base.quantity_open * base.anticipated_retail_amt AS NUMERIC), 2)
  ELSE 0
  END AS on_order_12wk_retail_dollars,
  CASE
  WHEN base.otb_eow_date <= (SELECT DATE_ADD(MAX(week_end_day_date), INTERVAL 84 DAY)
    FROM date_lkup)
  THEN ROUND(CAST(base.quantity_open * base.unit_cost_amt AS NUMERIC), 2)
  ELSE 0
  END AS on_order_12wk_cost_dollars
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS base
 INNER JOIN store_lkup AS store ON base.store_num = store.store_num
WHERE base.quantity_open > 0
 AND (LOWER(base.status) = LOWER('CLOSED') AND base.end_ship_date >= DATE_SUB((SELECT MAX(week_end_day_date)
       FROM date_lkup), INTERVAL 45 DAY) OR LOWER(base.status) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
 AND base.first_approval_date IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,STORE_NUM , price_type,RP_IND) ON  oo_stage;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS onorder
-- cluster by rms_sku_num, store_num, week_num
AS
SELECT base.rms_sku_num,
 base.store_num,
  (SELECT MAX(week_idnt_true)
  FROM date_lkup) AS week_num,
 base.rp_ind,
 RPAD('N', 1, ' ') AS ds_ind,
 base.price_type,
 0 AS sales_dollars,
 0 AS sales_cost,
 0 AS sales_units,
 0 AS return_dollars,
 0 AS return_cost,
 0 AS return_unit,
 0 AS demand_dollars,
 0 AS demand_units,
 0 AS eoh_dollars,
 0 AS eoh_cost,
 0 AS eoh_units,
 0 AS receipt_dollars,
 0 AS receipt_units,
 0 AS receipt_cost,
 0 AS transfer_pah_dollars,
 0 AS transfer_pah_units,
 0 AS transfer_pah_cost,
 SUM(base.on_order_retail_dollars) AS oo_dollars,
 SUM(base.on_order_units) AS oo_units,
 SUM(base.on_order_cost_dollars) AS oo_cost,
 CAST(SUM(base.on_order_4wk_retail_dollars) AS BIGNUMERIC) AS oo_4weeks_dollars,
 CAST(SUM(base.on_order_4wk_units) AS NUMERIC) AS oo_4weeks_units,
 CAST(SUM(base.on_order_4wk_cost_dollars) AS BIGNUMERIC) AS oo_4weeks_cost,
 CAST(SUM(base.on_order_12wk_retail_dollars) AS BIGNUMERIC) AS oo_12weeks_dollars,
 CAST(SUM(base.on_order_12wk_units) AS NUMERIC) AS oo_12weeks_units,
 CAST(SUM(base.on_order_12wk_cost_dollars) AS BIGNUMERIC) AS oo_12weeks_cost,
 0 AS net_sales_tot_retl_with_cost
FROM oo_stage AS base
 INNER JOIN sku_lkup AS sku ON LOWER(base.rms_sku_num) = LOWER(sku.rms_sku_num)
GROUP BY base.rms_sku_num,
 base.store_num,
 week_num,
 base.rp_ind,
 ds_ind,
 base.price_type;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index(rms_sku_num, store_num, week_num) ,column(rms_sku_num) ,column(store_num) ,column(week_num)   on onorder;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS full_data_stg_base AS
SELECT *
FROM sales
UNION ALL
SELECT *
FROM demand
UNION ALL
SELECT *
FROM inventory
UNION ALL
SELECT *
FROM receipt
UNION ALL
SELECT *
FROM transfer_ph
UNION ALL
SELECT *
FROM onorder;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.shoe_size_performance_monthly_base
WHERE week_num BETWEEN (SELECT start_week_num
        FROM variable_start_end_week) AND (SELECT end_week_num
        FROM variable_start_end_week);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.shoe_size_performance_monthly_base
(SELECT rms_sku_num,
  store_num,
  week_num,
  rp_ind,
  ds_ind,
  price_type,
  ROUND(CAST(sales_dollars AS NUMERIC), 2) AS sales_dollars,
  ROUND(CAST(sales_cost AS NUMERIC), 2) AS sales_cost,
  sales_units,
  ROUND(CAST(return_dollars AS NUMERIC), 2) AS return_dollars,
  ROUND(CAST(return_cost AS NUMERIC), 2) AS return_cost,
  return_unit,
  ROUND(CAST(demand_dollars AS NUMERIC), 2) AS demand_dollars,
  demand_units,
  CAST(eoh_dollars AS NUMERIC) AS eoh_dollars,
  CAST(eoh_cost AS NUMERIC) AS eoh_cost,
  eoh_units,
  ROUND(CAST(receipt_dollars AS NUMERIC), 2) AS receipt_dollars,
  receipt_units,
  ROUND(CAST(receipt_cost AS NUMERIC), 2) AS receipt_cost,
  ROUND(CAST(transfer_pah_dollars AS NUMERIC), 2) AS transfer_pah_dollars,
  transfer_pah_units,
  ROUND(CAST(transfer_pah_cost AS NUMERIC), 2) AS transfer_pah_cost,
  oo_dollars,
  oo_units,
  oo_cost,
  CAST(oo_4weeks_dollars AS NUMERIC) AS oo_4weeks_dollars,
  oo_4weeks_units,
  CAST(oo_4weeks_cost AS NUMERIC) AS oo_4weeks_cost,
  CAST(oo_12weeks_dollars AS NUMERIC) AS oo_12weeks_dollars,
  oo_12weeks_units,
  CAST(oo_12weeks_cost AS NUMERIC) AS oo_12weeks_cost,
  CAST(net_sales_tot_retl_with_cost AS NUMERIC) AS net_sales_tot_retl_with_cost,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM full_data_stg_base AS a);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS  COLUMN (PARTITION), COLUMN (week_num) on t2dl_das_ccs_categories.shoe_size_performance_monthly_base;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
