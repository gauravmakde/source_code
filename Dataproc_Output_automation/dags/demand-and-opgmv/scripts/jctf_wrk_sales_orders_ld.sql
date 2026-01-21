-- BT;
/*
Clarity transaction fact starts with transaction work load for all record sources.
For Record Source='S' a transaction id is generated from the transaction fields.
Rules for transaction id creation is as follows:
If order number is present, order number is loaded as the transaction id.
If order number is not present and if the transaction has original transaction details populated ,
the original transaction keys are used.
If order number is not present and if the transaction has no original transaction details the transaction key
is loaded as the transaction id.
*/

--log table clean up
DELETE FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact
WHERE LOWER(sql_desc) = LOWER('clarity_transaction_work_ld') AND LOWER(load_desc) = LOWER('Incremental') AND
  curr_batch_date = (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT'));


--Step 1: Truncate and load WRK table for ERTM SALES and order records to be loaded for the active batch.
DELETE FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk
WHERE LOWER(record_source) IN (LOWER('S'), LOWER('O'));


--Log load step
INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  1 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_FCT' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);



--Step 2:Insert data to the transaction work table for record source='S'
INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk (transaction_id, transaction_line_seq_num,
 record_source, global_tran_id, demand_date, demand_tmstp_pacific,demand_tmstp_pacific_tz, business_day_date, tran_type_code,
 line_item_activity_type_code, channel, channel_country, channel_brand, selling_channel, business_unit_num,
 ringing_store_num, intent_store_num, ship_zip_code, bill_zip_code, order_num, rms_sku_num, acp_id, ent_cust_id,
 loyalty_status, register_num, tran_num, order_platform_type, platform_code, first_routed_location_type,
 first_routed_method, first_routed_location, fulfillment_status, fulfillment_tmstp_pacific, fulfilled_from_location_type
 , fulfilled_from_method, fulfilled_from_location, line_item_fulfillment_type, line_item_order_type, delivery_method,
 delivery_method_subtype, curbside_ind, jwn_first_routed_demand_ind, jwn_fulfilled_demand_ind, gift_with_purchase_ind,
 smart_sample_ind, zero_value_unit_ind, line_item_quantity, demand_units, line_net_usd_amt, line_net_amt,
 line_item_currency_code, jwn_gross_demand_ind, jwn_reported_demand_ind, employee_discount_ind,
 employee_discount_usd_amt, employee_discount_amt, compare_price_amt, price_type, price_adj_code,
 same_day_price_adjust_ind, price_adjustment_ind, loyalty_deferral_usd_amt, loyalty_deferral_amt,
 loyalty_breakage_usd_amt, loyalty_breakage_amt, gift_card_issued_usd_amt, gift_card_issued_amt,
 gift_card_reload_adj_usd_amt, gift_card_reload_adj_amt, gc_expected_breakage_usd_amt, gc_expected_breakage_amt,
 gift_card_redeemed_usd_amt, gift_card_redeemed_amt, wac_eligible_ind, wac_available_ind, weighted_average_cost,
 operational_gmv_cost_usd_amt, operational_gmv_cost_amt, cost_of_sales_usd_amt, cost_of_sales_amt, clarity_metric,
 clarity_metric_line_item, variable_cost_usd_amt, variable_cost_amt, inventory_business_model, upc_num, rms_style_num,
 style_desc, style_group_num, style_group_desc, sbclass_num, sbclass_name, class_num, class_name, dept_num, dept_name,
 subdivision_num, subdivision_name, division_num, division_name, merch_dept_num, nonmerch_fee_code, fee_code_desc,
 vpn_num, vendor_brand_name, supplier_num, supplier_name, payto_vendor_num, payto_vendor_name, npg_ind,
 same_day_store_return_ind, product_return_ind, return_probability, jwn_srr_retail_usd_amt, jwn_srr_retail_amt,
 jwn_srr_units, jwn_srr_cost_usd_amt, jwn_srr_cost_amt, non_merch_ind, operational_gmv_usd_amt, operational_gmv_amt,
 jwn_operational_gmv_ind, jwn_reported_gmv_ind, jwn_merch_net_sales_ind, net_sales_usd_amt, net_sales_amt,
 jwn_reported_net_sales_ind, jwn_reported_cost_of_sales_ind, jwn_reported_gross_margin_ind, jwn_variable_expenses_ind,
 original_line_item_usd_amt, original_line_item_amt, original_line_item_amt_currency_code, return_date, sale_date,
 remote_selling_type, ertm_tran_date, tran_line_id, service_type, rp_on_sale_date, rp_off_sale_date, dw_batch_date,
 dw_sys_load_tmstp)
(SELECT CASE
   WHEN LOWER(rx.transaction_id) <> LOWER('')
   THEN rx.transaction_id
   WHEN LOWER(fct.order_num) <> LOWER('')
   THEN SUBSTR(fct.order_num, 1, 64)
   WHEN LOWER(fct.original_transaction_identifier) <> LOWER('')
   THEN TRIM(FORMAT('%2d', fct.original_ringing_store_num)) || '_' || COALESCE(TRIM(FORMAT('%2.0f', fct.original_register_num
           )), 'sdm') || '_' || COALESCE(TRIM(fct.original_tran_num), 'sdm') || '_' || FORMAT_DATE('%F', fct.original_business_date
     )
   ELSE TRIM(FORMAT('%2d', fct.ringing_store_num)) || '_' || COALESCE(TRIM(FORMAT('%2.0f', fct.register_num)), 'sdm') ||
       '_' || COALESCE(TRIM(FORMAT('%2.0f', fct.tran_num)), 'sdm') || '_' || FORMAT_DATE('%F', fct.business_day_date)
   END AS transaction_id,
  fct.line_item_seq_num,
  fct.record_source,
  fct.global_tran_id,
  fct.demand_date,
  fct.demand_tmstp_pacific_utc,
  fct.demand_tmstp_pacific_tz,
  fct.business_day_date,
  fct.tran_type_code,
  fct.line_item_activity_type_code,
  fct.channel,
  fct.channel_country,
  fct.channel_brand,
  fct.selling_channel,
  fct.business_unit_num,
  fct.ringing_store_num,
  fct.intent_store_num,
  fct.ship_zip_code,
  fct.bill_zip_code,
  COALESCE(fct.order_num, rx.order_num) AS order_num,
  fct.rms_sku_num,
  fct.acp_id,
  fct.ent_cust_id,
  fct.loyalty_status,
  fct.register_num,
  fct.tran_num,
  fct.order_platform_type,
  fct.platform_code,
  fct.first_routed_location_type,
  fct.first_routed_method,
  fct.first_routed_location,
  fct.fulfillment_status,
  fct.fulfillment_tmstp_pacific,
  fct.fulfilled_from_location_type,
  fct.fulfilled_from_method,
  fct.fulfilled_from_location,
  fct.line_item_fulfillment_type,
  fct.line_item_order_type,
  fct.delivery_method,
  fct.delivery_method_subtype,
  fct.curbside_ind,
  fct.jwn_first_routed_demand_ind,
  fct.jwn_fulfilled_demand_ind,
  fct.gift_with_purchase_ind,
  fct.smart_sample_ind,
  fct.zero_value_unit_ind,
  fct.line_item_quantity,
  fct.demand_units,
  fct.line_net_usd_amt,
  fct.line_net_amt,
  fct.line_item_currency_code,
  fct.jwn_gross_demand_ind,
  fct.jwn_reported_demand_ind,
  fct.employee_discount_ind,
  fct.employee_discount_usd_amt,
  fct.employee_discount_amt,
  fct.compare_price_amt,
  fct.price_type,
  fct.price_adj_code,
  fct.same_day_price_adjust_ind,
  fct.price_adjustment_ind,
  fct.loyalty_deferral_usd_amt,
  fct.loyalty_deferral_amt,
  fct.loyalty_breakage_usd_amt,
  fct.loyalty_breakage_amt,
  fct.gift_card_issued_usd_amt,
  fct.gift_card_issued_amt,
  fct.gift_card_reload_adj_usd_amt,
  fct.gift_card_reload_adj_amt,
  fct.gc_expected_breakage_usd_amt,
  fct.gc_expected_breakage_amt,
  fct.gift_card_redeemed_usd_amt,
  fct.gift_card_redeemed_amt,
  fct.wac_eligible_ind,
  fct.wac_available_ind,
  fct.weighted_average_cost,
  fct.cost_of_sales_usd_amt AS operational_gmv_cost_usd_amt,
  fct.cost_of_sales_amt AS operational_gmv_cost_amt,
  fct.cost_of_sales_usd_amt,
  fct.cost_of_sales_amt,
  fct.clarity_metric,
  fct.clarity_metric_line_item,
  fct.variable_cost_usd_amt,
  fct.variable_cost_amt,
  fct.inventory_business_model,
  fct.upc_num,
  fct.rms_style_num,
  fct.style_desc,
  fct.style_group_num,
  fct.style_group_desc,
  fct.sbclass_num,
  fct.sbclass_name,
  fct.class_num,
  fct.class_name,
  fct.dept_num,
  fct.dept_name,
  fct.subdivision_num,
  fct.subdivision_name,
  fct.division_num,
  fct.division_name,
  fct.merch_dept_num,
  fct.nonmerch_fee_code,
  fct.fee_code_desc,
  fct.vpn_num,
  fct.vendor_brand_name,
  fct.supplier_num,
  fct.supplier_name,
  fct.payto_vendor_num,
  fct.payto_vendor_name,
  fct.npg_ind,
  fct.same_day_store_return_ind,
  fct.product_return_ind,
  fct.return_probability,
  fct.jwn_srr_retail_usd_amt,
  fct.jwn_srr_retail_amt,
  fct.jwn_srr_units,
  fct.jwn_srr_cost_usd_amt,
  fct.jwn_srr_cost_amt,
  fct.non_merch_ind,
  fct.operational_gmv_usd_amt,
  fct.operational_gmv_amt,
  fct.jwn_operational_gmv_ind,
  fct.jwn_reported_gmv_ind,
  fct.jwn_merch_net_sales_ind,
  fct.net_sales_usd_amt,
  fct.net_sales_amt,
  fct.jwn_reported_net_sales_ind,
  fct.jwn_reported_cost_of_sales_ind,
  fct.jwn_reported_gross_margin_ind,
  fct.jwn_variable_expenses_ind,
  fct.original_line_item_usd_amt,
  fct.original_line_item_amt,
  fct.original_line_item_amt_currency_code,
  fct.return_date,
  fct.sale_date,
  fct.remote_selling_type,
  fct.ertm_tran_date,
  fct.tran_line_id,
  fct.service_type,
  fct.rp_on_sale_date,
  fct.rp_off_sale_date,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS fct
  INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS k ON fct.global_tran_id = k.global_tran_id AND
      fct.business_day_date = k.business_day_date AND fct.line_item_seq_num = k.line_item_seq_num AND LOWER(k.status_code
     ) = LOWER('+')
  LEFT JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_return_sale_xref AS rx ON fct.global_tran_id = rx.return_global_tran_id
      AND fct.line_item_seq_num = rx.return_line_item_seq_num AND fct.business_day_date = rx.return_business_day_date);


--Log load step
INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  2 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_FCT' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);



/* Step 3: Next step is the load for Order data from POR ORDER table*/

INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk (transaction_id, transaction_line_seq_num,
 record_source, demand_date, demand_tmstp_pacific, demand_tmstp_pacific_tz ,business_day_date, channel, channel_country, channel_brand,
 selling_channel, business_unit_num, ringing_store_num, intent_store_num, ship_zip_code, bill_zip_code, order_num,
 rms_sku_num, acp_id, ent_cust_id, loyalty_status, order_platform_type, platform_subtype, platform_code,
 first_routed_tmstp_pacific, first_routed_location_type, first_routed_method, first_routed_location, fulfillment_status
 , fulfillment_tmstp_pacific, fulfilled_from_location_type, fulfilled_from_method, fulfilled_from_location,
 line_item_fulfillment_type, line_item_order_type, pickup_store_num, delivery_method, delivery_method_subtype,
 curbside_ind, cancel_reason_code, cancel_reason_subtype, canceled_date_pacific, canceled_tmstp_pacific,
 same_day_fraud_cancel_ind, pre_route_fraud_cancel_ind, cancel_timing, customer_cancel_ind, same_day_cust_cancel_ind,
 pre_route_cust_cancel_ind, jwn_pre_route_cancel_ind, post_route_cancel_ind, first_route_pending_ind, backorder_hold_ind
 , curr_backorder_hold_ind, unrouted_demand_ind, jwn_first_routed_demand_ind, jwn_fulfilled_demand_ind,
 gift_with_purchase_ind, smart_sample_ind, zero_value_unit_ind, line_item_quantity, demand_units, line_net_usd_amt,
 line_net_amt, line_item_currency_code, jwn_gross_demand_ind, jwn_reported_demand_ind, employee_discount_ind,
 employee_discount_usd_amt, employee_discount_amt, compare_price_amt, inventory_business_model, upc_num, rms_style_num,
 style_desc, style_group_num, style_group_desc, sbclass_num, sbclass_name, class_num, class_name, dept_num, dept_name,
 subdivision_num, subdivision_name, division_num, division_name, vpn_num, vendor_brand_name, supplier_num, supplier_name
 , payto_vendor_num, payto_vendor_name, npg_ind, product_return_ind, return_probability, jwn_operational_gmv_ind,
 return_date, sale_date, remote_selling_type, service_type, rp_on_sale_date, rp_off_sale_date, dw_batch_date,
 dw_sys_load_tmstp)


(SELECT SUBSTR(order_num, 1, 64) AS transaction_id,
  order_line_num AS transaction_line_seq_num,
  record_source,
  demand_date_pacific AS demand_date,
  demand_tmstp_pacific_utc,
  demand_tmstp_pacific_tz,
  business_day_date,
  channel,
  channel_country_code,
  channel_brand_code,
  selling_channel_code,
  business_unit_num,
  ringing_store_num,
  intent_store_num,
  ship_zip_code,
  bill_zip_code,
  order_num,
  rms_sku_num,
  acp_id,
  ent_cust_id,
  SUBSTR('UNKNOWN_VALUE', 1, 20) AS loyalty_status,
  order_platform_type,
  platform_subtype,
  platform_code,
  first_routed_tmstp_pacific,
  first_routed_location_type_code,
  first_routed_method_code,
  first_routed_location,
  fulfillment_status,
  fulfillment_tmstp_pacific,
  fulfilled_from_location_type_code,
  fulfilled_from_method_code,
  fulfilled_from_location,
  line_item_fulfillment_type_code,
  line_item_order_type_code,
  pickup_store_num,
  delivery_method_code,
  delivery_method_subtype_code,
  curbside_pickup_ind,
  cancel_reason_code,
  cancel_reason_subtype_code,
  order_line_canceled_date_pacific,
  order_line_canceled_tmstp_pacific,
  same_day_fraud_cancel_ind,
  pre_route_fraud_cancel_ind,
  cancel_timing,
  customer_cancel_ind,
  same_day_cust_cancel_ind,
  pre_route_cust_cancel_ind,
  jwn_pre_route_cancel_ind,
  post_route_cancel_ind,
  first_route_pending_ind,
  backorder_hold_ind,
  curr_backorder_hold_ind,
  unrouted_demand_ind,
  jwn_first_routed_demand_ind,
  jwn_fulfilled_demand_ind,
  gift_with_purchase_ind,
  smart_sample_ind,
  zero_value_unit_ind,
  line_item_quantity,
  demand_units,
  line_net_amt_usd,
  line_net_amt,
  line_item_currency_code,
  jwn_gross_demand_ind,
  jwn_reported_demand_ind,
  employee_discount_ind,
  employee_discount_amt_usd,
  employee_discount_amt,
  compare_price_amt,
  inventory_business_model,
  upc_num,
  rms_style_num,
  style_desc,
  style_group_num,
  style_group_desc,
  subclass_num,
  subclass_name,
  class_num,
  class_name,
  dept_num,
  dept_name,
  subdivision_num,
  subdivision_name,
  division_num,
  division_name,
  vpn_num,
  vendor_brand_name,
  supplier_num,
  supplier_name,
  payto_vendor_num,
  payto_vendor_name,
  npg_ind,
  product_return_ind,
  return_probability_pct,
  jwn_operational_gmv_ind,
  return_date,
  sale_date,
  remote_selling_type,
  service_type,
  rp_on_sale_date,
  rp_off_sale_date,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_HIS')) AS dw_batch_datet,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_por_wrk);


--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  3 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_FCT' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);

--Step 4-Update statements to prep the transaction work table


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk AS tgt SET
 demand_date = SRC.demand_date,
 demand_tmstp_pacific = SRC.demand_tmstp_pacific_utc,
 demand_tmstp_pacific_tz = SRC.demand_tmstp_pacific_tz,
 channel = SRC.channel,
 channel_country = SRC.channel_country,
 selling_channel = SRC.new_selling_channel,
 ship_zip_code = SRC.ship_zip_code,
 bill_zip_code = SRC.bill_zip_code,
 ent_cust_id = SRC.ent_cust_id,
 order_platform_type = SRC.order_platform_type,
 platform_subtype = SRC.new_platform_subtype,
 platform_code = SRC.new_platform_code,
 first_routed_tmstp_pacific = SRC.first_routed_tmstp_pacific,
 first_routed_location_type = SRC.first_routed_location_type,
 first_routed_method = SRC.first_routed_method,
 first_routed_location = SRC.first_routed_location,
 fulfillment_tmstp_pacific = SRC.new_fulfillment_tmstp_pacific,
 fulfilled_from_location_type = SRC.new_fulfilled_from_location_type,
 fulfilled_from_method = SRC.new_fulfilled_from_method,
 fulfilled_from_location = SRC.new_fulfilled_from_location,
 delivery_method = SRC.new_delivery_method,
 delivery_method_subtype = SRC.new_delivery_method_subtype,
 pickup_store_num = SRC.new_pickup_store_num,
 curbside_ind = SRC.new_curbside_ind,
 remote_selling_type = SRC.new_remote_selling_type,
 return_probability = CASE
  WHEN LOWER(tgt.line_item_activity_type_code) = LOWER('S')
  THEN SRC.return_probability
  ELSE tgt.return_probability
  END,
 jwn_srr_retail_usd_amt = CASE
  WHEN LOWER(tgt.line_item_activity_type_code) = LOWER('S')
  THEN IFNULL(- 1 * SRC.return_probability * tgt.operational_gmv_usd_amt, 0)
  ELSE tgt.jwn_srr_retail_usd_amt
  END,
 jwn_srr_retail_amt = CASE
  WHEN LOWER(tgt.line_item_activity_type_code) = LOWER('S')
  THEN IFNULL(- 1 * SRC.return_probability * tgt.operational_gmv_amt, 0)
  ELSE tgt.jwn_srr_retail_amt
  END,
 jwn_srr_units = CASE
  WHEN LOWER(tgt.line_item_activity_type_code) = LOWER('S')
  THEN IFNULL(- 1 * tgt.line_item_quantity * SRC.return_probability, 0)
  ELSE tgt.jwn_srr_units
  END,
 jwn_srr_cost_usd_amt = CASE
  WHEN LOWER(tgt.line_item_activity_type_code) = LOWER('S')
  THEN IFNULL(- 1 * SRC.return_probability * tgt.operational_gmv_cost_usd_amt, 0)
  ELSE tgt.jwn_srr_cost_usd_amt
  END,
 jwn_srr_cost_amt = CASE
  WHEN LOWER(tgt.line_item_activity_type_code) = LOWER('S')
  THEN IFNULL(- 1 * SRC.return_probability * tgt.operational_gmv_cost_amt, 0)
  ELSE tgt.jwn_srr_cost_amt
  END,
 fulfillment_exclude_ind = SRC.fulfillment_exclude_ind FROM (SELECT s.transaction_id,
   s.transaction_line_seq_num,
   s.record_source,
   s.business_day_date,
   s.order_num,
   s.rms_sku_num,
   s.global_tran_id,
   s.tran_type_code,
   s.line_item_activity_type_code,
   o.demand_date,
   o.demand_tmstp_pacific_utc,
   o.demand_tmstp_pacific_tz,
   o.channel,
   o.channel_country,
    CASE
    WHEN LOWER(o.channel) = LOWER('DIGITAL')
    THEN 'ONLINE'
    ELSE 'STORE'
    END AS new_selling_channel,
   o.ship_zip_code,
   o.bill_zip_code,
   o.ent_cust_id,
   o.order_platform_type,
    CASE
    WHEN LOWER(COALESCE(s.platform_subtype, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
    THEN o.platform_subtype
    ELSE s.platform_subtype
    END AS new_platform_subtype,
    CASE
    WHEN LOWER(COALESCE(s.platform_code, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
    THEN o.platform_code
    ELSE s.platform_code
    END AS new_platform_code,
   o.first_routed_tmstp_pacific,
   o.first_routed_location_type,
   o.first_routed_method,
   o.first_routed_location,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.fulfillment_tmstp_pacific
    WHEN o.fulfillment_tmstp_pacific IS NULL AND LOWER(COALESCE(s.fulfilled_from_location_type, 'UNKNOWN_VALUE')) NOT IN
       (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
    THEN s.fulfillment_tmstp_pacific
    ELSE o.fulfillment_tmstp_pacific
    END AS new_fulfillment_tmstp_pacific,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.fulfilled_from_location_type
    WHEN LOWER(COALESCE(s.fulfilled_from_location_type, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'
       ))
    THEN o.fulfilled_from_location_type
    ELSE s.fulfilled_from_location_type
    END AS new_fulfilled_from_location_type,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.fulfilled_from_method
    WHEN LOWER(COALESCE(s.fulfilled_from_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
     AND LOWER(o.fulfilled_from_method) NOT LIKE LOWER('%UNKNOWN%')
    THEN o.fulfilled_from_method
    ELSE s.fulfilled_from_method
    END AS new_fulfilled_from_method,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.fulfilled_from_location
    WHEN LOWER(COALESCE(s.fulfilled_from_location_type, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'
        )) AND o.fulfilled_from_location IS NOT NULL
    THEN o.fulfilled_from_location
    WHEN LOWER(s.fulfilled_from_location_type) = LOWER('Vendor (Drop Ship)') AND LOWER(o.fulfilled_from_location_type) =
      LOWER('Vendor (Drop Ship)')
    THEN o.fulfilled_from_location
    ELSE s.fulfilled_from_location
    END AS new_fulfilled_from_location,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.delivery_method
    WHEN LOWER(COALESCE(s.delivery_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
    THEN CASE
     WHEN LOWER(o.delivery_method) = LOWER('BOPUS') AND s.business_unit_num IN (5000, 6000, 6500, 5800)
     THEN SUBSTR('Ship to Store', 1, 20)
     ELSE o.delivery_method
     END
    WHEN LOWER(s.delivery_method) = LOWER('Ship to Customer/Other') AND s.business_unit_num IN (5000, 6000, 6500, 5800)
      AND LOWER(o.delivery_method) = LOWER('BOPUS') AND o.pickup_store_num > 0
    THEN 'Ship to Store'
    ELSE s.delivery_method
    END AS new_delivery_method,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.delivery_method_subtype
    WHEN LOWER(COALESCE(s.delivery_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
    THEN CASE
     WHEN LOWER(CASE
         WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
         THEN s.delivery_method
         WHEN LOWER(COALESCE(s.delivery_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
         THEN CASE
          WHEN LOWER(o.delivery_method) = LOWER('BOPUS') AND s.business_unit_num IN (5000, 6000, 6500, 5800)
          THEN SUBSTR('Ship to Store', 1, 20)
          ELSE o.delivery_method
          END
         WHEN LOWER(s.delivery_method) = LOWER('Ship to Customer/Other') AND s.business_unit_num IN (5000, 6000, 6500,
              5800) AND LOWER(o.delivery_method) = LOWER('BOPUS') AND o.pickup_store_num > 0
         THEN 'Ship to Store'
         ELSE s.delivery_method
         END) <> LOWER('Ship to Customer/Other') OR LOWER(o.delivery_method_subtype) NOT LIKE LOWER('%PICKUP%')
     THEN o.delivery_method_subtype
     ELSE 'UNKNOWN_VALUE'
     END
    WHEN LOWER(s.delivery_method_subtype) = LOWER('UNKNOWN_VALUE')
    THEN CASE
     WHEN LOWER(CASE
         WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
         THEN s.delivery_method
         WHEN LOWER(COALESCE(s.delivery_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
         THEN CASE
          WHEN LOWER(o.delivery_method) = LOWER('BOPUS') AND s.business_unit_num IN (5000, 6000, 6500, 5800)
          THEN SUBSTR('Ship to Store', 1, 20)
          ELSE o.delivery_method
          END
         WHEN LOWER(s.delivery_method) = LOWER('Ship to Customer/Other') AND s.business_unit_num IN (5000, 6000, 6500,
              5800) AND LOWER(o.delivery_method) = LOWER('BOPUS') AND o.pickup_store_num > 0
         THEN 'Ship to Store'
         ELSE s.delivery_method
         END) IN (LOWER('BOPUS'), LOWER('Ship to Store')) AND LOWER(o.delivery_method) = LOWER('BOPUS')
     THEN o.delivery_method_subtype
     WHEN LOWER(s.delivery_method) = LOWER(o.delivery_method) AND (LOWER(CASE
           WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
           THEN s.delivery_method
           WHEN LOWER(COALESCE(s.delivery_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE')
             )
           THEN CASE
            WHEN LOWER(o.delivery_method) = LOWER('BOPUS') AND s.business_unit_num IN (5000, 6000, 6500, 5800)
            THEN SUBSTR('Ship to Store', 1, 20)
            ELSE o.delivery_method
            END
           WHEN LOWER(s.delivery_method) = LOWER('Ship to Customer/Other') AND s.business_unit_num IN (5000, 6000, 6500
                , 5800) AND LOWER(o.delivery_method) = LOWER('BOPUS') AND o.pickup_store_num > 0
           THEN 'Ship to Store'
           ELSE s.delivery_method
           END) <> LOWER('Ship to Customer/Other') OR LOWER(o.delivery_method_subtype) NOT LIKE LOWER('%PICKUP%'))
     THEN o.delivery_method_subtype
     ELSE 'UNKNOWN_VALUE'
     END
    ELSE s.delivery_method_subtype
    END AS new_delivery_method_subtype,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.pickup_store_num
    WHEN s.intent_store_num = o.pickup_store_num AND LOWER(s.line_item_fulfillment_type) = LOWER('StorePickup')
    THEN o.pickup_store_num
    WHEN LOWER(CASE
         WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
         THEN s.delivery_method
         WHEN LOWER(COALESCE(s.delivery_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
         THEN CASE
          WHEN LOWER(o.delivery_method) = LOWER('BOPUS') AND s.business_unit_num IN (5000, 6000, 6500, 5800)
          THEN SUBSTR('Ship to Store', 1, 20)
          ELSE o.delivery_method
          END
         WHEN LOWER(s.delivery_method) = LOWER('Ship to Customer/Other') AND s.business_unit_num IN (5000, 6000, 6500,
              5800) AND LOWER(o.delivery_method) = LOWER('BOPUS') AND o.pickup_store_num > 0
         THEN 'Ship to Store'
         ELSE s.delivery_method
         END) IN (LOWER('BOPUS'), LOWER('Ship to Store')) AND s.pickup_store_num IS NULL AND o.pickup_store_num > 0
    THEN o.pickup_store_num
    ELSE s.pickup_store_num
    END AS new_pickup_store_num,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.curbside_ind
    WHEN LOWER(CASE
       WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
       THEN s.delivery_method
       WHEN LOWER(COALESCE(s.delivery_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
       THEN CASE
        WHEN LOWER(o.delivery_method) = LOWER('BOPUS') AND s.business_unit_num IN (5000, 6000, 6500, 5800)
        THEN SUBSTR('Ship to Store', 1, 20)
        ELSE o.delivery_method
        END
       WHEN LOWER(s.delivery_method) = LOWER('Ship to Customer/Other') AND s.business_unit_num IN (5000, 6000, 6500,
            5800) AND LOWER(o.delivery_method) = LOWER('BOPUS') AND o.pickup_store_num > 0
       THEN 'Ship to Store'
       ELSE s.delivery_method
       END) IN (LOWER('BOPUS'), LOWER('Ship to Store'))
    THEN o.curbside_ind
    WHEN LOWER(CASE
        WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
        THEN s.delivery_method
        WHEN LOWER(COALESCE(s.delivery_method, 'UNKNOWN_VALUE')) IN (LOWER('UNKNOWN_VALUE'), LOWER('NOT_APPLICABLE'))
        THEN CASE
         WHEN LOWER(o.delivery_method) = LOWER('BOPUS') AND s.business_unit_num IN (5000, 6000, 6500, 5800)
         THEN SUBSTR('Ship to Store', 1, 20)
         ELSE o.delivery_method
         END
        WHEN LOWER(s.delivery_method) = LOWER('Ship to Customer/Other') AND s.business_unit_num IN (5000, 6000, 6500,
             5800) AND LOWER(o.delivery_method) = LOWER('BOPUS') AND o.pickup_store_num > 0
        THEN 'Ship to Store'
        ELSE s.delivery_method
        END) IN (LOWER('BOPUS'), LOWER('Ship to Store')) AND LOWER(s.curbside_ind) = LOWER('NOT_APPLICABLE')
    THEN 'N'
    ELSE s.curbside_ind
    END AS new_curbside_ind,
   o.return_probability,
    CASE
    WHEN LOWER(s.line_item_activity_type_code) = LOWER('N')
    THEN s.remote_selling_type
    WHEN LOWER(s.remote_selling_type) NOT IN (LOWER('Self-Service'), LOWER('NOT_APPLICABLE')) AND LOWER(o.remote_selling_type
       ) <> LOWER('Self-Service')
    THEN o.remote_selling_type
    ELSE s.remote_selling_type
    END AS new_remote_selling_type,
    CASE
    WHEN LOWER(s.tran_type_code) = LOWER('SALE') AND LOWER(s.line_item_activity_type_code) = LOWER('S')
    THEN 'Y'
    ELSE 'N'
    END AS fulfillment_exclude_ind
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS s
   INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS o ON LOWER(o.transaction_id) = LOWER(s.transaction_id
         ) AND LOWER(o.rms_sku_num) = LOWER(s.rms_sku_num) AND (ABS(o.line_item_quantity) = ABS(s.line_item_quantity) OR
          LOWER(s.tran_type_code) = LOWER('RETN') AND LOWER(s.line_item_activity_type_code) = LOWER('S')) AND (LOWER(s.line_item_fulfillment_type
           ) = LOWER(o.line_item_fulfillment_type) OR LOWER(s.line_item_fulfillment_type) = LOWER('UNKNOWN_VALUE') OR s
          .intent_store_num = o.pickup_store_num AND LOWER(s.line_item_fulfillment_type) = LOWER('StorePickup') OR s.tran_line_id
         = o.transaction_line_seq_num) AND (s.fulfilled_from_location IS NULL OR s.fulfilled_from_location = o.fulfilled_from_location
           OR LOWER(o.fulfilled_from_location_type) = LOWER('Vendor (Drop Ship)') OR LOWER(s.tran_type_code) <> LOWER('SALE'
         ) OR LOWER(s.line_item_activity_type_code) <> LOWER('S'))
  WHERE LOWER(s.order_num) <> LOWER('')
   AND LOWER(s.record_source) = LOWER('S')
   AND LOWER(s.jwn_operational_gmv_ind) = LOWER('Y')
   AND LOWER(s.rms_sku_num) <> LOWER('')
   AND LOWER(o.fulfillment_status) = LOWER('FulFilled')
   AND LOWER(o.record_source) = LOWER('O')
   AND LOWER(o.rms_sku_num) <> LOWER('')
   AND LOWER(o.jwn_operational_gmv_ind) = LOWER('Y')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY s.global_tran_id, s.transaction_line_seq_num, s.rms_sku_num, CASE
        WHEN LOWER(s.line_item_activity_type_code) = LOWER('S') AND LOWER(s.tran_type_code) = LOWER('SALE')
        THEN 1
        WHEN LOWER(s.line_item_activity_type_code) = LOWER('S') AND LOWER(s.tran_type_code) = LOWER('RETN')
        THEN 2
        WHEN LOWER(s.line_item_activity_type_code) = LOWER('R')
        THEN 3
        ELSE 4
        END ORDER BY CASE
        WHEN LOWER(s.upc_num) = LOWER(o.upc_num)
        THEN 1
        ELSE 2
        END, CASE
        WHEN LOWER(s.line_item_fulfillment_type) = LOWER(o.line_item_fulfillment_type)
        THEN 1
        ELSE 2
        END, CASE
        WHEN s.fulfilled_from_location = o.fulfilled_from_location
        THEN 1
        ELSE 2
        END, CASE
        WHEN s.tran_line_id = o.transaction_line_seq_num AND s.tran_line_id > 2
        THEN 1
        ELSE 2
        END, CASE
        WHEN s.tran_line_id = o.transaction_line_seq_num
        THEN 1
        ELSE 2
        END, s.business_day_date, s.global_tran_id, s.transaction_line_seq_num)) = 1) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
       AND tgt.business_day_date = SRC.business_day_date AND tgt.global_tran_id = SRC.global_tran_id AND LOWER(tgt.record_source
    ) = LOWER('S') AND LOWER(tgt.order_num) = LOWER(SRC.order_num);


--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  4 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_HIS' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_HIS')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);


--Step 5: --Propagate the price_adjustment_ind flag to other rows referring to same purchase line item

UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk AS tgt SET
 price_adjustment_ind = SRC.price_adjustment_ind FROM (SELECT DISTINCT transaction_id,
   demand_date,
   demand_tmstp_pacific_utc,
   rms_sku_num,
   price_adjustment_ind
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk
  WHERE LOWER(price_adjustment_ind) = LOWER('Y')
   AND LOWER(record_source) IN (LOWER('S'), LOWER('O'))) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.demand_date = SRC.demand_date AND tgt.demand_tmstp_pacific
      = SRC.demand_tmstp_pacific_utc AND LOWER(tgt.rms_sku_num) = LOWER(SRC.rms_sku_num) AND LOWER(COALESCE(tgt.line_item_activity_type_code
     , 'S')) = LOWER('S') AND LOWER(tgt.record_source) IN (LOWER('S'), LOWER('O'));


--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  5 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_HIS' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_HIS')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);


--Step 6: --Join to PRODUCT_PRICE_TIMELINE_DIM in order to determine price type used for the purchase/order

UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk AS tgt SET
 price_type = SRC.price_type FROM (SELECT wrk.transaction_id,
   wrk.transaction_line_seq_num,
   wrk.record_source,
   wrk.global_tran_id,
   wrk.rms_sku_num,
   wrk.business_day_date,
   wrk.compare_price_amt,
   wrk.intent_store_num,
   wrk.demand_tmstp_pacific,
    CASE
    WHEN LOWER(wrk.jwn_operational_gmv_ind) = LOWER('N')
    THEN SUBSTR('NOT_APPLICABLE', 1, 20)
    WHEN LOWER(price.ownership_retail_price_type_code) = LOWER('CLEARANCE')
    THEN 'Clearance'
    WHEN price.ownership_retail_price_amt = wrk.compare_price_amt AND LOWER(price.ownership_retail_price_type_code) =
      LOWER('CLEARANCE')
    THEN 'Clearance'
    WHEN LOWER(wrk.price_type) = LOWER('Promotion')
    THEN wrk.price_type
    WHEN LOWER(TRIM(COALESCE(wrk.rms_sku_num, ''))) = LOWER('')
    THEN 'UNKNOWN_VALUE'
    WHEN price.selling_retail_price_amt = wrk.compare_price_amt AND LOWER(price.selling_retail_price_type_code) = LOWER('REGULAR'
       )
    THEN 'Regular Price'
    WHEN LOWER(price.selling_retail_price_type_code) = LOWER('PROMOTION') AND LOWER(price.ownership_retail_price_type_code
       ) <> LOWER('CLEARANCE')
    THEN 'Promotion'
    WHEN LOWER(promo.rms_sku_num) <> LOWER('')
    THEN 'Promotion'
    ELSE 'Regular Price'
    END AS price_type
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS wrk
   LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.product_price_timeline_dim AS price ON LOWER(price.rms_sku_num) = LOWER(wrk.rms_sku_num)
        AND LOWER(price.channel_country) = LOWER(wrk.channel_country) AND LOWER(price.channel_brand) = LOWER(wrk.channel_brand
         ) AND LOWER(price.selling_channel) = LOWER(wrk.selling_channel) AND wrk.demand_tmstp_pacific >= price.eff_begin_tmstp
       AND wrk.demand_tmstp_pacific < price.eff_end_tmstp
   LEFT JOIN (SELECT DISTINCT wrk0.transaction_id,
     wrk0.transaction_line_seq_num,
     wrk0.rms_sku_num,
     wrk0.record_source,
     wrk0.business_day_date
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS wrk0
     INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.product_promotion_timeline_dim AS pr0 ON LOWER(wrk0.rms_sku_num) = LOWER(pr0.rms_sku_num
               ) AND LOWER(wrk0.channel_brand) = LOWER(pr0.channel_brand) AND LOWER(wrk0.channel_country) = LOWER(pr0.channel_country
              ) AND LOWER(wrk0.selling_channel) = LOWER(pr0.selling_channel) AND wrk0.demand_tmstp_pacific >= pr0.eff_begin_tmstp
            AND wrk0.demand_tmstp_pacific < pr0.eff_end_tmstp AND wrk0.compare_price_amt > 0 AND wrk0.compare_price_amt
        = pr0.simple_promo_fixed_price_amt AND LOWER(wrk0.line_item_currency_code) = LOWER(pr0.simple_promo_fixed_price_currency_code
        )
    WHERE LOWER(wrk0.rms_sku_num) <> LOWER('')
     AND LOWER(wrk0.jwn_operational_gmv_ind) = LOWER('Y')
     AND LOWER(wrk0.record_source) IN (LOWER('S'), LOWER('O'))) AS promo ON LOWER(promo.transaction_id) = LOWER(wrk.transaction_id
         ) AND wrk.transaction_line_seq_num = promo.transaction_line_seq_num AND LOWER(promo.record_source) = LOWER(wrk
        .record_source) AND wrk.business_day_date = promo.business_day_date AND LOWER(promo.rms_sku_num) = LOWER(wrk.rms_sku_num
      )
  WHERE LOWER(wrk.rms_sku_num) <> LOWER('')
   AND LOWER(wrk.jwn_operational_gmv_ind) = LOWER('Y')
   AND LOWER(wrk.record_source) IN (LOWER('S'), LOWER('O'))) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
       AND LOWER(tgt.record_source) = LOWER(SRC.record_source) AND COALESCE(tgt.global_tran_id, - 1) = COALESCE(SRC.global_tran_id
     , - 1) AND LOWER(tgt.rms_sku_num) = LOWER(SRC.rms_sku_num) AND tgt.business_day_date = SRC.business_day_date;


--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  6 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_HIS' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_HIS')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);


--Step 7: -- Assign Flash Event ind (which depends on demand store and demand timestamp)


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk AS tgt SET
 flash_event_ind = SRC.flash_event_ind FROM (SELECT DISTINCT wrk.transaction_id,
   wrk.transaction_line_seq_num,
   wrk.record_source,
   wrk.global_tran_id,
   wrk.demand_tmstp_pacific_utc,
   RPAD('Y', 1, ' ') AS flash_event_ind
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS wrk
   INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.product_selling_event_flash_vw AS evtsku ON LOWER(evtsku.sku_num) = LOWER(wrk.rms_sku_num
           ) AND LOWER(evtsku.channel_brand) = LOWER(wrk.channel_brand) AND LOWER(evtsku.channel_country) = LOWER(wrk.channel_country
          ) AND LOWER(evtsku.selling_channel) = LOWER(wrk.selling_channel) AND wrk.demand_tmstp_pacific_utc BETWEEN evtsku.item_start_tmstp_utc
       AND evtsku.item_end_tmstp_utc AND LOWER(evtsku.sku_type) = LOWER('RMS') AND LOWER(evtsku.tag_name) = LOWER('FLASH')
  WHERE LOWER(wrk.rms_sku_num) <> LOWER('')
   AND LOWER(wrk.record_source) IN (LOWER('S'), LOWER('O'))) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
      AND LOWER(tgt.record_source) = LOWER(SRC.record_source) AND COALESCE(tgt.global_tran_id, - 1) = COALESCE(SRC.global_tran_id
    , - 1) AND tgt.demand_tmstp_pacific = SRC.demand_tmstp_pacific_utc;


--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  7 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_HIS' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_HIS')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);


--Step 8 : Sync S and O records at a transaction line item level

UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk AS tgt SET
 intent_store_num = SRC.ertm_intent_store,
 business_unit_num = SRC.ertm_business_unit_num,
 channel_brand = SRC.ertm_channel_brand,
 acp_id = SRC.ertm_acp_id,
 fulfillment_tmstp_pacific = SRC.new_fulfillment_tmstp_pacific,
 fulfilled_from_location_type = SRC.new_fulfilled_from_location_type,
 fulfilled_from_method = SRC.new_fulfilled_from_method,
 fulfilled_from_location = SRC.new_fulfilled_from_location,
 delivery_method = SRC.new_delivery_method,
 delivery_method_subtype = SRC.new_delivery_method_subtype,
 price_type = SRC.new_price_type,
 inventory_business_model = SRC.new_inventory_business_model FROM (SELECT o.transaction_id,
   o.transaction_line_seq_num,
   o.rms_sku_num,
   o.business_day_date,
   o.order_num,
   s.global_tran_id,
   s.transaction_line_seq_num AS ertm_seq_num,
   s.ringing_store_num AS ertm_ringing_store_num,
   s.intent_store_num AS ertm_intent_store,
   s.business_unit_num AS ertm_business_unit_num,
   s.channel_brand AS ertm_channel_brand,
   s.acp_id AS ertm_acp_id,
   s.business_unit_num AS ertm_business_unit,
   s.fulfillment_tmstp_pacific AS new_fulfillment_tmstp_pacific,
   s.fulfilled_from_location_type AS new_fulfilled_from_location_type,
   s.fulfilled_from_method AS new_fulfilled_from_method,
   s.fulfilled_from_location AS new_fulfilled_from_location,
   s.delivery_method AS new_delivery_method,
   s.delivery_method_subtype AS new_delivery_method_subtype,
   s.price_type AS new_price_type,
   s.inventory_business_model AS new_inventory_business_model
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS s
   INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS o ON LOWER(o.transaction_id) = LOWER(s.transaction_id
         ) AND LOWER(o.rms_sku_num) = LOWER(s.rms_sku_num) AND ABS(o.line_item_quantity) = ABS(s.line_item_quantity) AND
      (LOWER(s.line_item_fulfillment_type) = LOWER(o.line_item_fulfillment_type) OR LOWER(s.line_item_fulfillment_type)
          = LOWER('UNKNOWN_VALUE') OR s.intent_store_num = o.pickup_store_num AND LOWER(s.line_item_fulfillment_type) =
          LOWER('StorePickup') OR s.tran_line_id = o.transaction_line_seq_num) AND (s.fulfilled_from_location IS NULL OR
        s.fulfilled_from_location = o.fulfilled_from_location OR LOWER(o.fulfilled_from_location_type) = LOWER('Vendor (Drop Ship)'
        ))
  WHERE LOWER(s.order_num) <> LOWER('')
   AND LOWER(s.record_source) = LOWER('S')
   AND LOWER(s.jwn_operational_gmv_ind) = LOWER('Y')
   AND LOWER(s.rms_sku_num) <> LOWER('')
   AND LOWER(s.line_item_activity_type_code) = LOWER('S')
   AND LOWER(s.tran_type_code) = LOWER('SALE')
   AND LOWER(o.fulfillment_status) = LOWER('FulFilled')
   AND LOWER(o.record_source) = LOWER('O')
   AND LOWER(o.rms_sku_num) <> LOWER('')
   AND LOWER(o.jwn_operational_gmv_ind) = LOWER('Y')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY o.order_num, o.transaction_line_seq_num, o.rms_sku_num, CASE
        WHEN LOWER(s.line_item_activity_type_code) = LOWER('S') AND LOWER(s.tran_type_code) = LOWER('SALE')
        THEN 1
        WHEN LOWER(s.line_item_activity_type_code) = LOWER('S') AND LOWER(s.tran_type_code) = LOWER('RETN')
        THEN 2
        WHEN LOWER(s.line_item_activity_type_code) = LOWER('R')
        THEN 3
        ELSE 4
        END ORDER BY CASE
        WHEN LOWER(s.upc_num) = LOWER(o.upc_num)
        THEN 1
        ELSE 2
        END, CASE
        WHEN LOWER(s.line_item_fulfillment_type) = LOWER(o.line_item_fulfillment_type)
        THEN 1
        ELSE 2
        END, CASE
        WHEN s.fulfilled_from_location = o.fulfilled_from_location
        THEN 1
        ELSE 2
        END, CASE
        WHEN s.tran_line_id = o.transaction_line_seq_num AND s.tran_line_id > 2
        THEN 1
        ELSE 2
        END, CASE
        WHEN s.tran_line_id = o.transaction_line_seq_num
        THEN 1
        ELSE 2
        END, s.business_day_date, s.global_tran_id, s.transaction_line_seq_num)) = 1) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
       AND tgt.business_day_date = SRC.business_day_date AND LOWER(tgt.record_source) = LOWER('O') AND LOWER(tgt.order_num
    ) = LOWER(SRC.order_num) AND LOWER(tgt.rms_sku_num) = LOWER(SRC.rms_sku_num);



--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  8 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_HIS' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_HIS')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);


--Step 9 Determine what the customer loyalty status was at the time of demand

UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk AS tgt SET
 loyalty_status = SRC.loyalty_status FROM (SELECT wrk.transaction_id,
   wrk.transaction_line_seq_num,
   wrk.record_source,
   wrk.global_tran_id,
   lvl.rewards_level AS loyalty_status
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS wrk
   INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.analytical_customer AS acust ON LOWER(acust.acp_id) = LOWER(wrk.acp_id) AND LOWER(acust.acp_loyalty_id
      ) <> LOWER('')
   INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.loyalty_level_lifecycle_fact AS lvl ON LOWER(lvl.loyalty_id) = LOWER(acust.acp_loyalty_id
        ) AND CAST(wrk.demand_tmstp_pacific AS DATE) >= lvl.start_day_date AND CAST(wrk.demand_tmstp_pacific AS DATE) <
      lvl.end_day_date AND lvl.end_day_date >= DATE '2019-01-01'
  WHERE LOWER(wrk.acp_id) <> LOWER('')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY wrk.transaction_id, wrk.transaction_line_seq_num, wrk.record_source, wrk.global_tran_id
       ORDER BY CASE
        WHEN LOWER(lvl.rewards_level) IN (LOWER('MEMBER'))
        THEN 1
        WHEN LOWER(lvl.rewards_level) IN (LOWER('INSIDER'), LOWER('L1'))
        THEN 2
        WHEN LOWER(lvl.rewards_level) IN (LOWER('INFLUENCER'), LOWER('L2'))
        THEN 3
        WHEN LOWER(lvl.rewards_level) IN (LOWER('AMBASSADOR'), LOWER('L3'))
        THEN 4
        WHEN LOWER(lvl.rewards_level) IN (LOWER('ICON'), LOWER('L4'))
        THEN 5
        ELSE 0
        END, lvl.start_day_date, lvl.end_day_date, lvl.dw_sys_updt_tmstp DESC, lvl.loyalty_id)) = 1) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
     AND LOWER(tgt.record_source) = LOWER(SRC.record_source) AND COALESCE(tgt.global_tran_id, - 1) = COALESCE(SRC.global_tran_id
   , - 1);


--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  9 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_HIS' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_HIS')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);


--Step 10: --Synchronize Dimension fields on Return/PriceAdj line items with corresponding Sale line item

UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_wrk AS tgt SET
 demand_date = SRC.demand_date,
 demand_tmstp_pacific = SRC.demand_tmstp_pacific_utc,
 demand_tmstp_pacific_tz =SRC.demand_tmstp_pacific_tz,
 channel = SRC.channel,
 channel_country = SRC.channel_country,
 selling_channel = SRC.selling_channel,
 ship_zip_code = SRC.ship_zip_code,
 bill_zip_code = SRC.bill_zip_code,
 order_platform_type = SRC.order_platform_type,
 platform_subtype = SRC.platform_subtype,
 first_routed_tmstp_pacific = SRC.first_routed_tmstp_pacific,
 first_routed_location_type = SRC.first_routed_location_type,
 first_routed_method = SRC.first_routed_method,
 first_routed_location = SRC.first_routed_location,
 fulfillment_tmstp_pacific = SRC.fulfillment_tmstp_pacific,
 fulfilled_from_location_type = SRC.fulfilled_from_location_type,
 fulfilled_from_method = SRC.fulfilled_from_method,
 fulfilled_from_location = SRC.fulfilled_from_location,
 pickup_store_num = SRC.pickup_store_num,
 delivery_method = SRC.delivery_method,
 delivery_method_subtype = SRC.delivery_method_subtype,
 curbside_ind = SRC.curbside_ind,
 employee_discount_ind = SRC.employee_discount_ind,
 price_type = SRC.new_price_type,
 price_adjustment_ind = SRC.price_adjustment_ind,
 inventory_business_model = SRC.inventory_business_model,
 remote_selling_type = SRC.remote_selling_type,
 flash_event_ind = SRC.flash_event_ind,
 jwn_srr_retail_usd_amt = SRC.jwn_srr_retail_usd_amt,
 jwn_srr_retail_amt = SRC.jwn_srr_retail_amt,
 jwn_srr_units = SRC.jwn_srr_units,
 jwn_srr_cost_usd_amt = SRC.jwn_srr_cost_usd_amt,
 jwn_srr_cost_amt = SRC.jwn_srr_cost_amt FROM (SELECT rtn.transaction_id,
   rtn.transaction_line_seq_num,
   rtn.record_source,
   rtn.global_tran_id,
   rtn.business_day_date,
   sls.demand_date,
   sls.demand_tmstp_pacific_utc,
   sls.demand_tmstp_pacific_tz,
   sls.channel,
   sls.channel_country,
   sls.selling_channel,
   sls.ship_zip_code,
   sls.bill_zip_code,
   sls.order_platform_type,
   sls.platform_subtype,
   sls.first_routed_tmstp_pacific,
   sls.first_routed_location_type,
   sls.first_routed_method,
   sls.first_routed_location,
   sls.fulfillment_tmstp_pacific,
   sls.fulfilled_from_location_type,
   sls.fulfilled_from_method,
   sls.fulfilled_from_location,
   sls.pickup_store_num,
   sls.delivery_method,
   sls.delivery_method_subtype,
   sls.curbside_ind,
   sls.employee_discount_ind,
    CASE
    WHEN LOWER(rtn.tran_type_code) = LOWER('RETN') AND LOWER(rtn.line_item_activity_type_code) = LOWER('S')
    THEN rtn.price_type
    ELSE sls.price_type
    END AS new_price_type,
   sls.price_adjustment_ind,
   sls.inventory_business_model,
   sls.remote_selling_type,
   sls.flash_event_ind,
    CASE
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y') AND rcal.month_idnt = scal.month_idnt
    THEN sls.jwn_srr_retail_usd_amt - rtn.line_net_usd_amt
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * rtn.line_net_usd_amt
    ELSE 0
    END AS jwn_srr_retail_usd_amt,
    CASE
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y') AND rcal.month_idnt = scal.month_idnt
    THEN sls.jwn_srr_retail_amt - rtn.line_net_amt
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * rtn.line_net_amt
    ELSE 0
    END AS jwn_srr_retail_amt,
    CASE
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y') AND rcal.month_idnt = scal.month_idnt
    THEN - 1 * sls.jwn_srr_units - rtn.line_item_quantity
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y')
    THEN CAST(rtn.line_item_quantity AS NUMERIC)
    ELSE 0
    END AS jwn_srr_units,
    CASE
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y') AND rcal.month_idnt = scal.month_idnt
    THEN sls.jwn_srr_cost_usd_amt - rtn.operational_gmv_cost_usd_amt
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * rtn.operational_gmv_cost_usd_amt
    ELSE 0
    END AS jwn_srr_cost_usd_amt,
    CASE
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y') AND rcal.month_idnt = scal.month_idnt
    THEN sls.jwn_srr_cost_amt - rtn.operational_gmv_cost_amt
    WHEN LOWER(rtn.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * rtn.operational_gmv_cost_amt
    ELSE 0
    END AS jwn_srr_cost_amt
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS rtn
   INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_return_sale_xref AS xref ON LOWER(rtn.transaction_id) = LOWER(xref
        .transaction_id) AND rtn.global_tran_id = xref.return_global_tran_id AND rtn.transaction_line_seq_num = xref.return_line_item_seq_num
       AND LOWER(rtn.record_source) = LOWER('S')
   INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS sls ON LOWER(sls.transaction_id) = LOWER(xref
        .transaction_id) AND xref.sale_global_tran_id = sls.global_tran_id AND xref.sale_line_item_seq_num = sls.transaction_line_seq_num
       AND LOWER(sls.record_source) = LOWER('S')
   INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.day_cal_454_dim AS rcal ON xref.return_business_day_date = rcal.day_date
   INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.day_cal_454_dim AS scal ON xref.sale_business_day_date = scal.day_date) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
      AND tgt.business_day_date = SRC.business_day_date AND LOWER(tgt.record_source) = LOWER('S') AND tgt.global_tran_id
   = SRC.global_tran_id;


--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_work_ld' AS sql_desc,
  'Incremental' AS load_desc,
  10 AS step_num,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS step_log_timstp,
  'JWN_CLARITY_TRAN_HIS' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_HIS')) AS curr_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp);

--ET;
-- BT;
--COLLECT STATS on the work table post load
-- COLLECT STATS ON {{params.DBJWNENV}}_NAP_JWN_METRICS_STG.JWN_CLARITY_TRANSACTION_WRK;
--ET;
