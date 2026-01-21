
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=fls_assortment_pilot_11521_ACE_ENG;
---    Task_Name=fls_assortment_trans;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS rp
--CLUSTER BY rms_sku_num, location_num
AS
SELECT rms_sku_num,
 location_num,
 on_sale_date,
 off_sale_date,
 LEAD(on_sale_date, 1) OVER (PARTITION BY rms_sku_num, location_num ORDER BY on_sale_date, off_sale_date) AS
 next_on_sale_date,
  CASE
  WHEN DATE_DIFF(off_sale_date, (LEAD(on_sale_date, 1) OVER (PARTITION BY rms_sku_num, location_num ORDER BY
        on_sale_date, off_sale_date)), DAY) >= 0
  THEN DATE_SUB(LEAD(on_sale_date, 1) OVER (PARTITION BY rms_sku_num, location_num ORDER BY on_sale_date, off_sale_date
    ), INTERVAL 1 DAY)
  ELSE off_sale_date
  END AS off_sale_date_adj
FROM (SELECT rms_sku_num,
   location_num,
   on_sale_date,
   off_sale_date,
   change_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rp_sku_loc_dim_hist AS rp
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, location_num, on_sale_date ORDER BY change_date DESC)) = 1) AS
 t0;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (rms_sku_num, location_num) on rp;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS trans
--CLUSTER BY trip_id, acp_id, sku_num, store_num
AS
SELECT h.acp_id,
 t.store_num,
 h.ringing_store_num,
 h.fulfilling_store_num,
 t.intent_store_num,
 COALESCE(h.fulfilling_store_num, t.intent_store_num) AS wac_store_num,
  CASE
  WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
  THEN COALESCE(h.order_date, t.tran_date)
  WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH'))
  THEN t.tran_date
  ELSE NULL
  END AS sale_date,
 RPAD(CONCAT(RPAD(h.acp_id, 1, ' '), ':', RPAD(CAST(t.store_num AS STRING), 1, ' '), ':', RPAD(CAST(CASE
      WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
      THEN COALESCE(h.order_date, t.tran_date)
      WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH'))
      THEN t.tran_date
      ELSE NULL
      END AS STRING), 1, ' ')), 1, ' ') AS trip_id,
 t.global_tran_id,
 t.business_day_date,
 h.order_num,
  CASE
  WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
  THEN 1
  ELSE 0
  END AS positive_sale_flag,
  CASE
  WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH'))
  THEN 1
  ELSE 0
  END AS negative_sale_flag,
 COALESCE(t.sku_num, t.hl_sku_num) AS sku_num,
  CASE
  WHEN t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
  THEN t.line_item_quantity
  WHEN t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH'))
  THEN - t.line_item_quantity
  ELSE NULL
  END AS units,
 t.line_net_usd_amt AS sale_amt,
 t.tran_type_code,
 t.tran_line_id,
 t.line_item_seq_num,
 t.line_item_activity_type_desc,
 t.line_item_fulfillment_type,
 t.line_item_order_type,
 t.employee_discount_flag,
  CASE
  WHEN t.line_item_promo_usd_amt < 0
  THEN 1
  ELSE 0
  END AS promo_flag
FROM (SELECT CASE
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickup'
        ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
    THEN 808
    WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickup'
        ) AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
    THEN 867
    ELSE intent_store_num
    END AS store_num,
   global_tran_id,
   business_day_date,
   tran_type_code,
   line_item_seq_num,
   line_item_activity_type_code,
   line_item_activity_type_desc,
   unique_source_id,
   ringing_store_num,
   register_num,
   tran_num,
   tran_version_num,
   intent_store_num,
   claims_destination_store_num,
   sku_num,
   upc_num,
   tran_date,
   tran_time,
   line_item_merch_nonmerch_ind,
   merch_dept_num,
   nonmerch_fee_code,
   line_item_promo_id,
   line_net_usd_amt,
   line_net_amt,
   line_item_net_amt_currency_code,
   line_item_quantity,
   line_item_fulfillment_type,
   line_item_order_type,
   employee_discount_flag,
   employee_discount_num,
   employee_discount_usd_amt,
   employee_discount_amt,
   employee_discount_currency_code,
   line_item_promo_usd_amt,
   line_item_promo_amt,
   line_item_promo_amt_currency_code,
   merch_unique_item_id,
   merch_price_adjust_reason,
   line_item_capture_system,
   original_business_date,
   original_ringing_store_num,
   original_register_num,
   original_tran_num,
   original_line_item_usd_amt,
   original_line_item_amt,
   original_line_item_amt_currency_code,
   line_item_regular_price,
   line_item_regular_price_currency_code,
   line_item_split_tax_seq_num,
   line_item_split_tax_pct,
   line_item_split_tax_type,
   line_item_split_tax_amt,
   dw_batch_date,
   dw_sys_load_tmstp,
   dw_sys_updt_tmstp,
   error_flag,
   tran_latest_version_ind,
   hl_sku_num,
   pos_class_num,
   item_source,
   original_transaction_identifier,
   commission_slsprsn_num,
   line_item_tax_amt,
   line_item_tax_currency_code,
   line_item_tax_usd_amt,
   line_item_tax_exempt_flag,
   line_item_tax_pct,
   price_adj_code,
   tran_line_id,
   original_tran_date,
   rms_sku_num,
   original_ship_to_zip_code,
   original_tran_time_stamp,
   original_tran_line_item_seq_num,
   financial_retail_tran_record_id,
   financial_retail_tran_line_id,
   partner_relationship_num,
   partner_relationship_type_code,
   original_financial_retail_tran_record_id,
   ownership_model
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact) AS t
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_hdr_fact AS h ON t.global_tran_id = h.global_tran_id AND t.business_day_date = h
   .business_day_date
WHERE LOWER(t.line_item_merch_nonmerch_ind) = LOWER('MERCH')
 AND (t.line_net_usd_amt > 0 AND LOWER(t.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH')) AND COALESCE(h.order_date, t
      .tran_date) BETWEEN (SELECT MIN(day_date)
      FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim) AND (SELECT MAX(day_date)
      FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim) OR t.line_net_usd_amt < 0 AND LOWER(t.tran_type_code) IN (LOWER('RETN'
        ), LOWER('EXCH')) AND t.tran_date BETWEEN (SELECT MIN(day_date)
      FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim) AND (SELECT MAX(day_date)
      FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim))
 AND t.store_num IN (SELECT store_num
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim)
 AND h.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (trip_id, acp_id, sku_num, store_num) , column (acp_id) , column (sale_date) , column (trip_id, acp_id, sale_date) , column (store_num) , column (sku_num) , column (business_day_date) , column (acp_id, global_tran_id) , column (order_num, sku_num, tran_line_id) , column (wac_store_num) on trans;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS loyalty_base
--CLUSTER BY acp_id
AS
SELECT a.acp_id,
 a.mktg_profile_type,
 a.acp_loyalty_id AS loyalty_id,
 INITCAP(l.rewards_level) AS nordy_level,
 l.start_day_date,
  CASE
  WHEN l.end_day_date = (LEAD(l.start_day_date, 1) OVER (PARTITION BY a.acp_id, a.acp_loyalty_id ORDER BY l.start_day_date
       ))
  THEN DATE_SUB(l.end_day_date, INTERVAL 1 DAY)
  ELSE l.end_day_date
  END AS end_day_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS l ON LOWER(a.acp_loyalty_id) = LOWER(l.loyalty_id)
WHERE l.start_day_date <= (SELECT MAX(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim)
 AND l.end_day_date >= (SELECT MIN(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (acp_id) , column (start_day_date, end_day_date) on loyalty_base;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS trans_trips
--CLUSTER BY acp_id, sale_date
AS
SELECT trip_id,
 acp_id,
 sale_date
FROM trans
GROUP BY trip_id,
 acp_id,
 sale_date;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS loyalty
--CLUSTER BY trip_id
AS
SELECT t.trip_id,
 l.nordy_level,
 ROW_NUMBER() OVER (PARTITION BY t.trip_id ORDER BY CASE
     WHEN LOWER(l.nordy_level) = LOWER('Member')
     THEN 1
     WHEN LOWER(l.nordy_level) = LOWER('Influencer')
     THEN 2
     WHEN LOWER(l.nordy_level) = LOWER('Ambassador')
     THEN 3
     WHEN LOWER(l.nordy_level) = LOWER('Icon')
     THEN 4
     ELSE NULL
     END DESC) AS rn
FROM trans_trips AS t
 INNER JOIN loyalty_base AS l ON LOWER(t.acp_id) = LOWER(l.acp_id) AND t.sale_date BETWEEN l.start_day_date AND l.end_day_date
   
QUALIFY (ROW_NUMBER() OVER (PARTITION BY t.trip_id ORDER BY CASE
       WHEN LOWER(l.nordy_level) = LOWER('Member')
       THEN 1
       WHEN LOWER(l.nordy_level) = LOWER('Influencer')
       THEN 2
       WHEN LOWER(l.nordy_level) = LOWER('Ambassador')
       THEN 3
       WHEN LOWER(l.nordy_level) = LOWER('Icon')
       THEN 4
       ELSE NULL
       END DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_ntn
--CLUSTER BY trip_id
AS
SELECT t.trip_id,
 MAX(CASE
   WHEN n.acp_id IS NOT NULL
   THEN 1
   ELSE 0
   END) AS ntn_flag
FROM trans AS t
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_fact AS n ON LOWER(t.acp_id) = LOWER(n.acp_id) AND t.global_tran_id = n.ntn_global_tran_id
   
GROUP BY t.trip_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS shipped_orders
--CLUSTER BY order_num, rms_sku_num, order_line_num
AS
SELECT o.order_num,
 o.rms_sku_num,
 o.order_line_num,
 o.destination_country_code AS country_code,
  CASE
  WHEN LOWER(o.destination_country_code) = LOWER('US')
  THEN d2.dma_desc
  WHEN LOWER(o.destination_country_code) = LOWER('CA')
  THEN CASE
   WHEN d3.ca_dma_code = 405
   THEN 'Riviere-du-Loup, QC'
   WHEN d3.ca_dma_code = 412
   THEN 'Sept-Iles, QC'
   WHEN d3.ca_dma_code = 421
   THEN 'Quebec, QC'
   WHEN d3.ca_dma_code = 442
   THEN 'Trois-Rivieres, QC'
   WHEN d3.ca_dma_code = 462
   THEN 'Montreal, QC'
   ELSE d3.ca_dma_desc
   END
  ELSE NULL
  END AS dma,
  CASE
  WHEN LOWER(o.destination_country_code) = LOWER('US')
  THEN z.state_name
  WHEN LOWER(o.destination_country_code) = LOWER('CA')
  THEN d3.prov_desc
  ELSE NULL
  END AS state_province,
  CASE
  WHEN LOWER(o.destination_country_code) = LOWER('US')
  THEN z.state_code
  WHEN LOWER(o.destination_country_code) = LOWER('CA')
  THEN d3.prov_code
  ELSE NULL
  END AS state_province_code
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS o
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_us_zip_dma AS d1 ON LOWER(o.destination_country_code) = LOWER('US') AND LOWER(o.destination_zip_code
    ) = LOWER(d1.us_zip_code)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_dma AS d2 ON d1.us_dma_code = d2.dma_code
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.org_ca_zip_dma AS d3 ON LOWER(o.destination_country_code) = LOWER('CA') AND LOWER(SUBSTR(o.destination_zip_code
     , 0, 3)) = LOWER(d3.postal_code)
 LEFT JOIN (SELECT DISTINCT country_code,
   zip_code,
    CASE
    WHEN LOWER(state_code) = LOWER('DC')
    THEN 'District of Columbia'
    ELSE INITCAP(state_name)
    END AS state_name,
   state_code
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.zip_codes_dim) AS z ON LOWER(o.destination_country_code) = LOWER(z.country_code) AND LOWER(o.destination_zip_code
    ) = LOWER(z.zip_code)
WHERE o.order_num IN (SELECT order_num
   FROM trans
   WHERE order_num IS NOT NULL);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS wac
--CLUSTER BY sku_num, location_num
AS
SELECT sku_num,
 location_num,
 weighted_average_cost,
 weighted_average_cost_currency_code,
 eff_begin_dt,
  CASE
  WHEN eff_end_dt = (LEAD(eff_begin_dt, 1) OVER (PARTITION BY sku_num, location_num ORDER BY eff_begin_dt, eff_end_dt))
  THEN DATE_SUB(eff_end_dt, INTERVAL 1 DAY)
  ELSE eff_end_dt
  END AS eff_end_dt_mod
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.weighted_average_cost_date_dim AS c
WHERE eff_begin_dt <= (SELECT MAX(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim)
QUALIFY CASE
   WHEN eff_end_dt = (LEAD(eff_begin_dt, 1) OVER (PARTITION BY sku_num, location_num ORDER BY eff_begin_dt, eff_end_dt)
     )
   THEN DATE_SUB(eff_end_dt, INTERVAL 1 DAY)
   ELSE eff_end_dt
   END >= (SELECT MIN(day_date)
   FROM `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_day_dim);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS trans_base
--CLUSTER BY trip_id, sku_num, sale_date, store_num
AS
SELECT t.acp_id,
 t.store_num,
 t.ringing_store_num,
 t.fulfilling_store_num,
 t.intent_store_num,
 t.wac_store_num,
 t.sale_date,
 t.trip_id,
 t.global_tran_id,
 t.business_day_date,
 t.order_num,
 t.positive_sale_flag,
 t.negative_sale_flag,
 t.sku_num,
 t.units,
 t.sale_amt,
 t.tran_type_code,
 t.tran_line_id,
 t.line_item_seq_num,
 t.line_item_activity_type_desc,
 t.line_item_fulfillment_type,
 t.line_item_order_type,
 t.employee_discount_flag,
 t.promo_flag,
 p.cc_num,
  t.sale_amt / NULLIF(t.units, 0) AS unit_price,
  CASE
  WHEN o.order_num IS NOT NULL
  THEN o.dma
  ELSE s.store_dma_desc
  END AS dma,
 COALESCE(n.ntn_flag, 0) AS ntn_flag,
 COALESCE(l.nordy_level, 'Nonmember') AS nordy_level,
 seg.predicted_ct_segment AS customer_segment,
 COALESCE(w.weighted_average_cost, w2.weighted_average_cost) AS weighted_average_cost,
  CASE
  WHEN rp.rms_sku_num IS NOT NULL
  THEN 1
  ELSE 0
  END AS rp_flag
FROM trans AS t
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_sku_dim AS p ON LOWER(t.sku_num) = LOWER(p.rms_sku_num)
 INNER JOIN `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.pilot_store_dim AS s ON t.store_num = s.store_num
 LEFT JOIN customer_ntn AS n ON LOWER(t.trip_id) = LOWER(n.trip_id)
 LEFT JOIN loyalty AS l ON LOWER(t.trip_id) = LOWER(l.trip_id)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment AS seg ON
  LOWER(t.acp_id) = LOWER(seg.acp_id)
 LEFT JOIN shipped_orders AS o ON LOWER(t.order_num) = LOWER(o.order_num) AND LOWER(t.sku_num) = LOWER(o.rms_sku_num)
  AND t.tran_line_id = o.order_line_num
 LEFT JOIN wac AS w ON LOWER(t.sku_num) = LOWER(w.sku_num) AND t.wac_store_num = CAST(w.location_num AS FLOAT64) AND t.business_day_date
   BETWEEN w.eff_begin_dt AND w.eff_end_dt_mod
 LEFT JOIN wac AS w2 ON LOWER(t.sku_num) = LOWER(w2.sku_num) AND t.wac_store_num = CAST(w2.location_num AS FLOAT64) AND
   w2.eff_end_dt_mod = DATE '9999-12-31'
 LEFT JOIN rp ON LOWER(t.sku_num) = LOWER(rp.rms_sku_num) AND t.intent_store_num = rp.location_num AND t.sale_date
   BETWEEN rp.on_sale_date AND rp.off_sale_date_adj;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (trip_id, acp_id, sku_num, store_num) , column (acp_id) , column (sale_date) , column (trip_id, acp_id, sale_date) , column (store_num) , column (sku_num) , column (business_day_date) , column (acp_id, global_tran_id) , column (order_num, sku_num, tran_line_id) , column (wac_store_num) on trans;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.fls_assortment_pilot_trans_base;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.fls_assortment_pilot_trans_base
(SELECT 
  acp_id,
  store_num,
  ringing_store_num,
  fulfilling_store_num,
  intent_store_num,
  wac_store_num,
  sale_date,
  trip_id,
  global_tran_id,
  business_day_date,
  order_num,
  positive_sale_flag,
  negative_sale_flag,
  sku_num,
  CAST(units AS NUMERIC) AS units,
  sale_amt,
  tran_type_code,
  tran_line_id,
  line_item_seq_num,
  line_item_activity_type_desc,
  line_item_fulfillment_type,
  line_item_order_type,
  employee_discount_flag,
  promo_flag,
  cc_num,
  unit_price,
  dma,
  ntn_flag,
  nordy_level,
  customer_segment,
  weighted_average_cost,
  rp_flag,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM trans_base AS t);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect stats primary index (trip_id, sku_num, sale_date, store_num) , column (sku_num) , column (store_num) , column (sale_date) , column (sku_num, sale_date) , column (acp_id) , column (trip_id) , column (sku_num, store_num, sale_date) on `{{params.gcp_project_id}}`.{{params.shoe_categories_t2_schema}}.fls_assortment_pilot_trans_base;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
