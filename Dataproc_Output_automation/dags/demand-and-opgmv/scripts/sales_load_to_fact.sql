DELETE
FROM
  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_retailtran_loyalty_giftcard_por_fact AS tgt
WHERE
  LOWER(record_source) = LOWER('S')
  AND EXISTS (
  SELECT
    1
  FROM
    {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS kys
  WHERE
    tgt.global_tran_id = global_tran_id
    AND tgt.business_day_date = business_day_date
    AND tgt.line_item_seq_num = line_item_seq_num
    AND LOWER(status_code) = LOWER('-'));










MERGE INTO
  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_retailtran_loyalty_giftcard_por_fact AS tgt
USING
  (
  SELECT
    *
  FROM
    {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_retailtran_loyalty_giftcard_por_wrk
  WHERE
    LOWER(record_source) IN (LOWER('S'))) AS SRC
ON
  tgt.global_tran_id = SRC.global_tran_id
  AND tgt.line_item_seq_num = SRC.line_item_seq_num
  AND LOWER(tgt.record_source) = LOWER(SRC.record_source)
  AND tgt.business_day_date = SRC.business_day_date

  WHEN MATCHED THEN UPDATE 
  SET 
  demand_date = SRC.demand_date, 
  demand_tmstp_pacific = SRC.demand_tmstp_pacific, 
  demand_tmstp_pacific_tz = SRC.demand_tmstp_pacific_tz,
  tran_type_code = SRC.tran_type_code, 
  line_item_activity_type_code = SRC.line_item_activity_type_code, 
  channel = SRC.channel, 
  channel_country = SRC.channel_country, 
  channel_brand = SRC.channel_brand, 
  selling_channel = SRC.selling_channel, 
  business_unit_num = SRC.business_unit_num, 
  ringing_store_num = SRC.ringing_store_num, 
  intent_store_num = SRC.intent_store_num, 
  ship_zip_code = SRC.ship_zip_code, 
  bill_zip_code = SRC.bill_zip_code, 
  order_num = SRC.order_num, 
  rms_sku_num = SRC.rms_sku_num, 
  acp_id = SRC.acp_id, 
  ent_cust_id = SRC.ent_cust_id, 
  loyalty_status = SRC.loyalty_status, 
  register_num = SRC.register_num, 
  tran_num = SRC.tran_num, 
  order_platform_type = SRC.order_platform_type, 
  platform_code = SRC.platform_code, 
  first_routed_location_type = SRC.first_routed_location_type, 
  first_routed_method = SRC.first_routed_method, 
  first_routed_location = SRC.first_routed_location, 
  fulfillment_status = SRC.fulfillment_status, 
  fulfillment_tmstp_pacific = SRC.fulfillment_tmstp_pacific, 
  fulfilled_from_location_type = SRC.fulfilled_from_location_type, 
  fulfilled_from_method = SRC.fulfilled_from_method, 
  fulfilled_from_location = SRC.fulfilled_from_location, 
  line_item_fulfillment_type = SRC.line_item_fulfillment_type, 
  line_item_order_type = SRC.line_item_order_type, 
  delivery_method = SRC.delivery_method, 
  delivery_method_subtype = SRC.delivery_method_subtype, 
  curbside_ind = SRC.curbside_ind, 
  jwn_first_routed_demand_ind = SRC.jwn_first_routed_demand_ind, 
  jwn_fulfilled_demand_ind = SRC.jwn_fulfilled_demand_ind, 
  gift_with_purchase_ind = SRC.gift_with_purchase_ind, 
  smart_sample_ind = SRC.smart_sample_ind, 
  zero_value_unit_ind = SRC.zero_value_unit_ind, 
  line_item_quantity = SRC.line_item_quantity, 
  demand_units = SRC.demand_units, 
  line_net_usd_amt = SRC.line_net_usd_amt, 
  line_net_amt = SRC.line_net_amt, 
  line_item_currency_code = SRC.line_item_currency_code, 
  jwn_gross_demand_ind = SRC.jwn_gross_demand_ind, 
  jwn_reported_demand_ind = SRC.jwn_reported_demand_ind, 
  employee_discount_ind = SRC.employee_discount_ind, 
  employee_discount_usd_amt = SRC.employee_discount_usd_amt, 
  employee_discount_amt = SRC.employee_discount_amt, 
  compare_price_amt = SRC.compare_price_amt, 
  price_type = SRC.price_type, 
  price_adj_code = SRC.price_adj_code, 
  same_day_price_adjust_ind = SRC.same_day_price_adjust_ind, 
  price_adjustment_ind = SRC.price_adjustment_ind, 
  wac_eligible_ind = SRC.wac_eligible_ind, 
  wac_available_ind = SRC.wac_available_ind, 
  weighted_average_cost = SRC.weighted_average_cost, 
  cost_of_sales_usd_amt = SRC.cost_of_sales_usd_amt, 
  cost_of_sales_amt = SRC.cost_of_sales_amt, 
  clarity_metric = SRC.clarity_metric, 
  clarity_metric_line_item = SRC.clarity_metric_line_item, 
  variable_cost_usd_amt = SRC.variable_cost_usd_amt, 
  variable_cost_amt = SRC.variable_cost_amt, 
  inventory_business_model = SRC.inventory_business_model, 
  upc_num = SRC.upc_num, 
  rms_style_num = SRC.rms_style_num, 
  style_desc = SRC.style_desc, 
  style_group_num = SRC.style_group_num, 
  style_group_desc = SRC.style_group_desc, 
  sbclass_num = SRC.sbclass_num, 
  sbclass_name = SRC.sbclass_name, 
  class_num = SRC.class_num, 
  class_name = SRC.class_name, 
  dept_num = SRC.dept_num, 
  dept_name = SRC.dept_name, 
  subdivision_num = SRC.subdivision_num, 
  subdivision_name = SRC.subdivision_name, 
  division_num = SRC.division_num, 
  division_name = SRC.division_name, 
  merch_dept_num = SRC.merch_dept_num, 
  nonmerch_fee_code = SRC.nonmerch_fee_code, 
  fee_code_desc = SRC.fee_code_desc, 
  vpn_num = SRC.vpn_num, 
  vendor_brand_name = SRC.vendor_brand_name, 
  supplier_num = SRC.supplier_num, 
  supplier_name = SRC.supplier_name, 
  payto_vendor_num = SRC.payto_vendor_num, 
  payto_vendor_name = SRC.payto_vendor_name, 
  npg_ind = SRC.npg_ind, 
  same_day_store_return_ind = SRC.same_day_store_return_ind, 
  product_return_ind = SRC.product_return_ind, 
  return_probability = SRC.return_probability, 
  jwn_srr_retail_usd_amt = SRC.jwn_srr_retail_usd_amt, 
  jwn_srr_retail_amt = SRC.jwn_srr_retail_amt, 
  jwn_srr_units = SRC.jwn_srr_units, 
  jwn_srr_cost_usd_amt = SRC.jwn_srr_cost_usd_amt, 
  jwn_srr_cost_amt = SRC.jwn_srr_cost_amt, 
  non_merch_ind = SRC.non_merch_ind, 
  operational_gmv_usd_amt = SRC.operational_gmv_usd_amt, 
  operational_gmv_amt = SRC.operational_gmv_amt, 
  jwn_operational_gmv_ind = SRC.jwn_operational_gmv_ind, 
  jwn_reported_gmv_ind = SRC.jwn_reported_gmv_ind, 
  jwn_merch_net_sales_ind = SRC.jwn_merch_net_sales_ind, 
  net_sales_usd_amt = SRC.net_sales_usd_amt, 
  net_sales_amt = SRC.net_sales_amt, 
  jwn_reported_net_sales_ind = SRC.jwn_reported_net_sales_ind, 
  jwn_reported_cost_of_sales_ind = SRC.jwn_reported_cost_of_sales_ind, 
  jwn_reported_gross_margin_ind = SRC.jwn_reported_gross_margin_ind, 
  jwn_variable_expenses_ind = SRC.jwn_variable_expenses_ind, 
  original_business_date = SRC.original_business_date, 
  original_ringing_store_num = SRC.original_ringing_store_num, 
  original_register_num = SRC.original_register_num, 
  original_tran_num = SRC.original_tran_num, 
  original_global_tran_id = SRC.global_tran_id, 
  original_transaction_identifier = SRC.original_transaction_identifier, 
  original_line_item_usd_amt = SRC.original_line_item_usd_amt, 
  original_line_item_amt = SRC.original_line_item_amt, 
  original_line_item_amt_currency_code = SRC.original_line_item_amt_currency_code, 
  return_date = SRC.return_date, 
  sale_date = SRC.sale_date, 
  remote_selling_type = SRC.remote_selling_type, 
  ertm_tran_date = SRC.ertm_tran_date, 
  tran_line_id = SRC.tran_line_id, 
  service_type = SRC.service_type, 
  rp_on_sale_date = SRC.rp_on_sale_date, 
  rp_off_sale_date = SRC.rp_off_sale_date, 
  dw_batch_date = SRC.dw_batch_date, 
  dw_sys_load_tmstp = SRC.dw_sys_load_tmstp
  WHEN NOT MATCHED
  THEN
INSERT
( line_item_seq_num
, record_source
, global_tran_id
, demand_date
, demand_tmstp_pacific
, demand_tmstp_pacific_tz
, business_day_date
, tran_type_code
, line_item_activity_type_code
, channel
, channel_country
, channel_brand
, selling_channel
, business_unit_num
, ringing_store_num
, intent_store_num
, ship_zip_code
, bill_zip_code
, order_num
, rms_sku_num
, acp_id
, ent_cust_id
, loyalty_status
, register_num
, tran_num
, order_platform_type
, platform_code
, first_routed_location_type
, first_routed_method
, first_routed_location
, fulfillment_status
, fulfillment_tmstp_pacific
, fulfilled_from_location_type
, fulfilled_from_method
, fulfilled_from_location
, line_item_fulfillment_type
, line_item_order_type
, delivery_method
, delivery_method_subtype
, curbside_ind
, jwn_first_routed_demand_ind
, jwn_fulfilled_demand_ind
, gift_with_purchase_ind
, smart_sample_ind
, zero_value_unit_ind
, line_item_quantity
, demand_units
, line_net_usd_amt
, line_net_amt
, line_item_currency_code
, jwn_gross_demand_ind
, jwn_reported_demand_ind
, employee_discount_ind
, employee_discount_usd_amt
, employee_discount_amt
, compare_price_amt
, price_type
, price_adj_code
, same_day_price_adjust_ind
, price_adjustment_ind
, loyalty_deferral_usd_amt
, loyalty_breakage_usd_amt
, loyalty_deferral_amt
, loyalty_breakage_amt
, gift_card_issued_usd_amt
, gift_card_issued_amt
, gift_card_reload_adj_usd_amt
, gift_card_reload_adj_amt
, gc_expected_breakage_usd_amt
, gc_expected_breakage_amt
, gift_card_redeemed_usd_amt
, gift_card_redeemed_amt
, wac_eligible_ind
, wac_available_ind
, weighted_average_cost
, cost_of_sales_usd_amt
, cost_of_sales_amt
, clarity_metric
, clarity_metric_line_item
, variable_cost_usd_amt
, variable_cost_amt
, inventory_business_model
, upc_num
, rms_style_num
, style_desc
, style_group_num
, style_group_desc
, sbclass_num
, sbclass_name
, class_num
, class_name
, dept_num
, dept_name
, subdivision_num
, subdivision_name
, division_num
, division_name
, merch_dept_num
, nonmerch_fee_code
, fee_code_desc
, vpn_num
, vendor_brand_name
, supplier_num
, supplier_name
, payto_vendor_num
, payto_vendor_name
, npg_ind
, same_day_store_return_ind
, product_return_ind
, return_probability
, jwn_srr_retail_usd_amt
, jwn_srr_retail_amt
, jwn_srr_units
, jwn_srr_cost_usd_amt
, jwn_srr_cost_amt
, non_merch_ind
, operational_gmv_usd_amt
, operational_gmv_amt
, jwn_operational_gmv_ind
, jwn_reported_gmv_ind
, jwn_merch_net_sales_ind
, net_sales_usd_amt
, net_sales_amt
, jwn_reported_net_sales_ind
, jwn_reported_cost_of_sales_ind
, jwn_reported_gross_margin_ind
, jwn_variable_expenses_ind
, original_business_date
, original_ringing_store_num
, original_register_num
, original_tran_num
, original_global_tran_id
, original_transaction_identifier
, original_line_item_usd_amt
, original_line_item_amt
, original_line_item_amt_currency_code
, return_date
, sale_date
, remote_selling_type
, ertm_tran_date
, tran_line_id
, service_type
, rp_on_sale_date
, rp_off_sale_date
, dw_batch_date
, dw_sys_load_tmstp)

VALUES
  (SRC.line_item_seq_num, 
  SRC.record_source, 
  SRC.global_tran_id, 
  SRC.demand_date, 
  SRC.demand_tmstp_pacific, 
  SRC.demand_tmstp_pacific_tz,
  SRC.business_day_date, 
  SRC.tran_type_code, 
  SRC.line_item_activity_type_code, 
  SRC.channel, 
  SRC.channel_country, 
  SRC.channel_brand, 
  SRC.selling_channel, 
  SRC.business_unit_num, 
  SRC.ringing_store_num, 
  SRC.intent_store_num, 
  SRC.ship_zip_code, 
  SRC.bill_zip_code, 
  SRC.order_num, 
  SRC.rms_sku_num, 
  SRC.acp_id, 
  SRC.ent_cust_id, 
  SRC.loyalty_status, 
  SRC.register_num, 
  SRC.tran_num, 
  SRC.order_platform_type, 
  SRC.platform_code, 
  SRC.first_routed_location_type, 
  SRC.first_routed_method, 
  SRC.first_routed_location, 
  SRC.fulfillment_status, 
  SRC.fulfillment_tmstp_pacific, 
  SRC.fulfilled_from_location_type, 
  SRC.fulfilled_from_method, 
  SRC.fulfilled_from_location, 
  SRC.line_item_fulfillment_type, 
  SRC.line_item_order_type, 
  SRC.delivery_method, 
  SRC.delivery_method_subtype, 
  SRC.curbside_ind, 
  SRC.jwn_first_routed_demand_ind, 
  SRC.jwn_fulfilled_demand_ind, 
  SRC.gift_with_purchase_ind, 
  SRC.smart_sample_ind, 
  SRC.zero_value_unit_ind, 
  SRC.line_item_quantity, 
  SRC.demand_units, 
  SRC.line_net_usd_amt, 
  SRC.line_net_amt , 
  SRC.line_item_currency_code,
  SRC.jwn_gross_demand_ind,
  SRC.jwn_reported_demand_ind,
  SRC.employee_discount_ind,
  SRC.employee_discount_usd_amt,
  SRC.employee_discount_amt, 
  SRC.compare_price_amt, 
  SRC.price_type, SRC.price_adj_code, 
  SRC.same_day_price_adjust_ind, 
  SRC.price_adjustment_ind, 
  SRC.loyalty_deferral_usd_amt, 
  SRC.loyalty_breakage_usd_amt, 
  SRC.loyalty_deferral_amt, 
  SRC.loyalty_breakage_amt, 
  SRC.gift_card_issued_usd_amt, 
  SRC.gift_card_issued_amt, 
  SRC.gift_card_reload_adj_usd_amt, 
  SRC.gift_card_reload_adj_amt, 
  SRC.gc_expected_breakage_usd_amt, 
  SRC.gc_expected_breakage_amt, 
  SRC.gift_card_redeemed_usd_amt, 
  SRC.gift_card_redeemed_amt , 
  SRC.wac_eligible_ind, 
  SRC.wac_available_ind , 
  SRC.weighted_average_cost, 
  SRC.cost_of_sales_usd_amt, 
  SRC.cost_of_sales_amt, 
  SRC.clarity_metric, 
  SRC.clarity_metric_line_item , 
  SRC.variable_cost_usd_amt, 
  SRC.variable_cost_amt, 
  SRC.inventory_business_model, 
  SRC.upc_num, 
  SRC.rms_style_num, 
  SRC.style_desc, 
  SRC.style_group_num, 
  SRC.style_group_desc, 
  SRC.sbclass_num, 
  SRC.sbclass_name, 
  SRC.class_num, 
  SRC.class_name, 
  SRC.dept_num, 
  SRC.dept_name, 
  SRC.subdivision_num, 
  SRC.subdivision_name, 
  SRC.division_num, 
  SRC.division_name, 
  SRC.merch_dept_num, SRC.nonmerch_fee_code, 
  SRC.fee_code_desc, 
  SRC.vpn_num, 
  SRC.vendor_brand_name, 
  SRC.supplier_num, 
  SRC.supplier_name, 
  SRC.payto_vendor_num, 
  SRC.payto_vendor_name, 
  SRC.npg_ind, 
  SRC.same_day_store_return_ind, 
  SRC.product_return_ind , 
  SRC.return_probability, 
  SRC.jwn_srr_retail_usd_amt, 
  SRC.jwn_srr_retail_amt, 
  SRC.jwn_srr_units, 
  SRC.jwn_srr_cost_usd_amt, 
  SRC.jwn_srr_cost_amt, 
  SRC.non_merch_ind ,
  SRC.operational_gmv_usd_amt, 
  SRC.operational_gmv_amt,
  SRC.jwn_operational_gmv_ind,
  SRC.jwn_reported_gmv_ind, 
  SRC.jwn_merch_net_sales_ind ,
  SRC.net_sales_usd_amt, 
  SRC.net_sales_amt,
  SRC.jwn_reported_net_sales_ind,
  SRC.jwn_reported_cost_of_sales_ind,
  SRC.jwn_reported_gross_margin_ind, 
  SRC.jwn_variable_expenses_ind, 
  SRC.original_business_date,
  SRC.original_ringing_store_num, 
  SRC.original_register_num, 
  SRC.original_tran_num, 
  SRC.original_global_tran_id, 
  SRC.original_transaction_identifier ,
  SRC.original_line_item_usd_amt, 
  SRC.original_line_item_amt, 
  SRC.original_line_item_amt_currency_code, 
  SRC.return_date, 
  SRC.sale_date, 
  SRC.remote_selling_type, 
  SRC.ertm_tran_date, 
  SRC.tran_line_id, 
  SRC.service_type, 
  SRC.rp_on_sale_date, 
  SRC.rp_off_sale_date, 
  SRC.dw_batch_date, 
  SRC.dw_sys_load_tmstp);
  --ET;