/*Delete process log entry for the same batch date and sql desc*/




DELETE FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact
WHERE LOWER(sql_desc) = LOWER('clarity_transaction_fact_ld') AND LOWER(load_desc) = LOWER('Incremental') AND
  curr_batch_date = (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT'));


--Purge out any ERTM records that are no longer keyed to a visible transaction.


DELETE FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt
WHERE LOWER(record_source) = LOWER('S') AND EXISTS (SELECT 1
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS kys
  WHERE tgt.global_tran_id = global_tran_id
   AND tgt.business_day_date = business_day_date
   AND tgt.transaction_line_seq_num = line_item_seq_num
   AND LOWER(status_code) = LOWER('-'));


/*The Fact merge happens from the transaction work table. Post merge for S and O records,
adjustments happen for previously loaded records in clarity transaction fact*/


--, ringing_store_num = SRC.ringing_store_num


--, ringing_store_num = SRC.ringing_store_num


MERGE INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt
USING (SELECT *
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk
 WHERE LOWER(record_source) IN (LOWER('S'), LOWER('O'))) AS SRC
ON LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
      AND LOWER(tgt.record_source) = LOWER(SRC.record_source) AND COALESCE(tgt.global_tran_id, - 1) = COALESCE(SRC.global_tran_id
    , - 1) AND tgt.business_day_date = SRC.business_day_date
WHEN MATCHED THEN UPDATE SET
 demand_date = SRC.demand_date,
 demand_tmstp_pacific = SRC.demand_tmstp_pacific_utc,
 demand_tmstp_pacific_tz = SRC.demand_tmstp_pacific_tz,
 cost_tran_date = SRC.cost_tran_date,
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
 platform_subtype = SRC.platform_subtype,
 platform_code = SRC.platform_code,
 first_routed_tmstp_pacific = SRC.first_routed_tmstp_pacific,
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
 fulfillment_exclude_ind = SRC.fulfillment_exclude_ind,
 pickup_store_num = SRC.pickup_store_num,
 delivery_method = SRC.delivery_method,
 delivery_method_subtype = SRC.delivery_method_subtype,
 curbside_ind = SRC.curbside_ind,
 cancel_reason_code = SRC.cancel_reason_code,
 cancel_reason_subtype = SRC.cancel_reason_subtype,
 canceled_date_pacific = SRC.canceled_date_pacific,
 canceled_tmstp_pacific = SRC.canceled_tmstp_pacific,
 same_day_fraud_cancel_ind = SRC.same_day_fraud_cancel_ind,
 pre_route_fraud_cancel_ind = SRC.pre_route_fraud_cancel_ind,
 cancel_timing = SRC.cancel_timing,
 customer_cancel_ind = SRC.customer_cancel_ind,
 same_day_cust_cancel_ind = SRC.same_day_cust_cancel_ind,
 pre_route_cust_cancel_ind = SRC.pre_route_cust_cancel_ind,
 jwn_pre_route_cancel_ind = SRC.jwn_pre_route_cancel_ind,
 post_route_cancel_ind = SRC.post_route_cancel_ind,
 first_route_pending_ind = SRC.first_route_pending_ind,
 backorder_hold_ind = SRC.backorder_hold_ind,
 curr_backorder_hold_ind = SRC.curr_backorder_hold_ind,
 unrouted_demand_ind = SRC.unrouted_demand_ind,
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
 loyalty_deferral_usd_amt = SRC.loyalty_deferral_usd_amt,
 loyalty_breakage_usd_amt = SRC.loyalty_breakage_usd_amt,
 loyalty_deferral_amt = SRC.loyalty_deferral_amt,
 loyalty_breakage_amt = SRC.loyalty_breakage_amt,
 gift_card_issued_usd_amt = SRC.gift_card_issued_usd_amt,
 gift_card_issued_amt = SRC.gift_card_issued_amt,
 gift_card_reload_adj_usd_amt = SRC.gift_card_reload_adj_usd_amt,
 gift_card_reload_adj_amt = SRC.gift_card_reload_adj_amt,
 gc_expected_breakage_usd_amt = SRC.gc_expected_breakage_usd_amt,
 gc_expected_breakage_amt = SRC.gc_expected_breakage_amt,
 gift_card_redeemed_usd_amt = SRC.gift_card_redeemed_usd_amt,
 gift_card_redeemed_amt = SRC.gift_card_redeemed_amt,
 wac_eligible_ind = SRC.wac_eligible_ind,
 wac_available_ind = SRC.wac_available_ind,
 weighted_average_cost = SRC.weighted_average_cost,
 operational_gmv_cost_usd_amt = SRC.operational_gmv_cost_usd_amt,
 operational_gmv_cost_amt = SRC.operational_gmv_cost_amt,
 cost_of_sales_usd_amt = SRC.cost_of_sales_usd_amt,
 cost_of_sales_amt = SRC.cost_of_sales_amt,
 vfmd_amt = SRC.vfmd_amt,
 claims_amt = SRC.claims_amt,
 mark_out_of_stock_amt = SRC.mark_out_of_stock_amt,
 shrink_inventory_adjustments_fc_amt = SRC.shrink_inventory_adjustments_fc_amt,
 shrink_fc_pi_accrual_amt = SRC.shrink_fc_pi_accrual_amt,
 debit_credit_memos_amt = SRC.debit_credit_memos_amt,
 wac_adjustments_amt = SRC.wac_adjustments_amt,
 rtv_cost_adjustments_amt = SRC.rtv_cost_adjustments_amt,
 receiver_cost_adjustments_amt = SRC.receiver_cost_adjustments_amt,
 invoice_term_discounts_amt = SRC.invoice_term_discounts_amt,
 clarity_metric = SRC.clarity_metric,
 clarity_metric_line_item = SRC.clarity_metric_line_item,
 variable_cost_usd_amt = SRC.variable_cost_usd_amt,
 variable_cost_amt = SRC.variable_cost_amt,
 accounting_adj_usd_amt = SRC.accounting_adj_usd_amt,
 accounting_adj_amt = SRC.accounting_adj_amt,
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
 original_line_item_usd_amt = SRC.original_line_item_usd_amt,
 original_line_item_amt = SRC.original_line_item_amt,
 original_line_item_amt_currency_code = SRC.original_line_item_amt_currency_code,
 return_date = SRC.return_date,
 sale_date = SRC.sale_date,
 remote_selling_type = SRC.remote_selling_type,
 ertm_tran_date = SRC.ertm_tran_date,
 tran_line_id = SRC.tran_line_id,
 service_type = SRC.service_type,
 flash_event_ind = SRC.flash_event_ind,
 rp_on_sale_date = SRC.rp_on_sale_date,
 rp_off_sale_date = SRC.rp_off_sale_date,
 dw_sys_updt_tmstp = current_datetime('PST8PDT')
WHEN NOT MATCHED THEN INSERT (transaction_id, transaction_line_seq_num, record_source, global_tran_id, demand_date,
 demand_tmstp_pacific,demand_tmstp_pacific_tz, business_day_date, cost_tran_date, tran_type_code, line_item_activity_type_code, channel,
 channel_country, channel_brand, selling_channel, business_unit_num, ringing_store_num, intent_store_num, ship_zip_code
 , bill_zip_code, order_num, rms_sku_num, acp_id, ent_cust_id, loyalty_status, register_num, tran_num,
 order_platform_type, platform_subtype, platform_code, first_routed_tmstp_pacific, first_routed_location_type,
 first_routed_method, first_routed_location, fulfillment_status, fulfillment_tmstp_pacific, fulfilled_from_location_type
 , fulfilled_from_method, fulfilled_from_location, line_item_fulfillment_type, line_item_order_type,
 fulfillment_exclude_ind, pickup_store_num, delivery_method, delivery_method_subtype, curbside_ind, cancel_reason_code,
 cancel_reason_subtype, canceled_date_pacific, canceled_tmstp_pacific, same_day_fraud_cancel_ind,
 pre_route_fraud_cancel_ind, cancel_timing, customer_cancel_ind, same_day_cust_cancel_ind, pre_route_cust_cancel_ind,
 jwn_pre_route_cancel_ind, post_route_cancel_ind, first_route_pending_ind, backorder_hold_ind, curr_backorder_hold_ind,
 unrouted_demand_ind, jwn_first_routed_demand_ind, jwn_fulfilled_demand_ind, gift_with_purchase_ind, smart_sample_ind,
 zero_value_unit_ind, line_item_quantity, demand_units, line_net_usd_amt, line_net_amt, line_item_currency_code,
 jwn_gross_demand_ind, jwn_reported_demand_ind, employee_discount_ind, employee_discount_usd_amt, employee_discount_amt
 , compare_price_amt, price_type, price_adj_code, same_day_price_adjust_ind, price_adjustment_ind,
 loyalty_deferral_usd_amt, loyalty_deferral_amt, loyalty_breakage_usd_amt, loyalty_breakage_amt,
 gift_card_issued_usd_amt, gift_card_issued_amt, gift_card_reload_adj_usd_amt, gift_card_reload_adj_amt,
 gc_expected_breakage_usd_amt, gc_expected_breakage_amt, gift_card_redeemed_usd_amt, gift_card_redeemed_amt,
 wac_eligible_ind, wac_available_ind, weighted_average_cost, operational_gmv_cost_usd_amt, operational_gmv_cost_amt,
 cost_of_sales_usd_amt, cost_of_sales_amt, vfmd_amt, claims_amt, mark_out_of_stock_amt,
 shrink_inventory_adjustments_fc_amt, shrink_fc_pi_accrual_amt, debit_credit_memos_amt, wac_adjustments_amt,
 rtv_cost_adjustments_amt, receiver_cost_adjustments_amt, invoice_term_discounts_amt, clarity_metric,
 clarity_metric_line_item, variable_cost_usd_amt, variable_cost_amt, accounting_adj_usd_amt, accounting_adj_amt,
 inventory_business_model, upc_num, rms_style_num, style_desc, style_group_num, style_group_desc, sbclass_num,
 sbclass_name, class_num, class_name, dept_num, dept_name, subdivision_num, subdivision_name, division_num,
 division_name, merch_dept_num, nonmerch_fee_code, fee_code_desc, vpn_num, vendor_brand_name, supplier_num,
 supplier_name, payto_vendor_num, payto_vendor_name, npg_ind, same_day_store_return_ind, product_return_ind,
 return_probability, jwn_srr_retail_usd_amt, jwn_srr_retail_amt, jwn_srr_units, jwn_srr_cost_usd_amt, jwn_srr_cost_amt,
 non_merch_ind, operational_gmv_usd_amt, operational_gmv_amt, jwn_operational_gmv_ind, jwn_reported_gmv_ind,
 jwn_merch_net_sales_ind, net_sales_usd_amt, net_sales_amt, jwn_reported_net_sales_ind, jwn_reported_cost_of_sales_ind,
 jwn_reported_gross_margin_ind, jwn_variable_expenses_ind, original_line_item_usd_amt, original_line_item_amt,
 original_line_item_amt_currency_code, return_date, sale_date, remote_selling_type, ertm_tran_date, tran_line_id,
 service_type, flash_event_ind, rp_on_sale_date, rp_off_sale_date, dw_batch_date, dw_sys_load_tmstp) VALUES(SRC.transaction_id
 , SRC.transaction_line_seq_num, SRC.record_source, SRC.global_tran_id, SRC.demand_date, SRC.demand_tmstp_pacific_utc,SRC.demand_tmstp_pacific_tz, SRC.business_day_date
 , SRC.cost_tran_date, SRC.tran_type_code, SRC.line_item_activity_type_code, SRC.channel, SRC.channel_country, SRC.channel_brand
 , SRC.selling_channel, SRC.business_unit_num, SRC.ringing_store_num, SRC.intent_store_num, SRC.ship_zip_code, SRC.bill_zip_code
 , SRC.order_num, SRC.rms_sku_num, SRC.acp_id, SRC.ent_cust_id, SRC.loyalty_status, SRC.register_num, SRC.tran_num, SRC
 .order_platform_type, SRC.platform_subtype, SRC.platform_code, SRC.first_routed_tmstp_pacific, SRC.first_routed_location_type
 , SRC.first_routed_method, SRC.first_routed_location, SRC.fulfillment_status, SRC.fulfillment_tmstp_pacific, SRC.fulfilled_from_location_type
 , SRC.fulfilled_from_method, SRC.fulfilled_from_location, SRC.line_item_fulfillment_type, SRC.line_item_order_type, SRC
 .fulfillment_exclude_ind, SRC.pickup_store_num, SRC.delivery_method, SRC.delivery_method_subtype, SRC.curbside_ind, SRC
 .cancel_reason_code, SRC.cancel_reason_subtype, SRC.canceled_date_pacific, SRC.canceled_tmstp_pacific, SRC.same_day_fraud_cancel_ind
 , SRC.pre_route_fraud_cancel_ind, SRC.cancel_timing, SRC.customer_cancel_ind, SRC.same_day_cust_cancel_ind, SRC.pre_route_cust_cancel_ind
 , SRC.jwn_pre_route_cancel_ind, SRC.post_route_cancel_ind, SRC.first_route_pending_ind, SRC.backorder_hold_ind, SRC.curr_backorder_hold_ind
 , SRC.unrouted_demand_ind, SRC.jwn_first_routed_demand_ind, SRC.jwn_fulfilled_demand_ind, SRC.gift_with_purchase_ind,
 SRC.smart_sample_ind, SRC.zero_value_unit_ind, SRC.line_item_quantity, SRC.demand_units, SRC.line_net_usd_amt, SRC.line_net_amt
 , SRC.line_item_currency_code, SRC.jwn_gross_demand_ind, SRC.jwn_reported_demand_ind, SRC.employee_discount_ind, SRC.employee_discount_usd_amt
 , SRC.employee_discount_amt, SRC.compare_price_amt, SRC.price_type, SRC.price_adj_code, SRC.same_day_price_adjust_ind,
 SRC.price_adjustment_ind, SRC.loyalty_deferral_usd_amt, SRC.loyalty_breakage_usd_amt, SRC.loyalty_deferral_amt, SRC.loyalty_breakage_amt
 , SRC.gift_card_issued_usd_amt, SRC.gift_card_issued_amt, SRC.gift_card_reload_adj_usd_amt, SRC.gift_card_reload_adj_amt
 , SRC.gc_expected_breakage_usd_amt, SRC.gc_expected_breakage_amt, SRC.gift_card_redeemed_usd_amt, SRC.gift_card_redeemed_amt
 , SRC.wac_eligible_ind, SRC.wac_available_ind, SRC.weighted_average_cost, SRC.operational_gmv_cost_usd_amt, SRC.operational_gmv_cost_amt
 , SRC.cost_of_sales_usd_amt, SRC.cost_of_sales_amt, SRC.vfmd_amt, SRC.claims_amt, SRC.mark_out_of_stock_amt, SRC.shrink_inventory_adjustments_fc_amt
 , SRC.shrink_fc_pi_accrual_amt, SRC.debit_credit_memos_amt, SRC.wac_adjustments_amt, SRC.rtv_cost_adjustments_amt, SRC
 .receiver_cost_adjustments_amt, SRC.invoice_term_discounts_amt, SRC.clarity_metric, SRC.clarity_metric_line_item, SRC.variable_cost_usd_amt
 , SRC.variable_cost_amt, SRC.accounting_adj_usd_amt, SRC.accounting_adj_amt, SRC.inventory_business_model, SRC.upc_num
 , SRC.rms_style_num, SRC.style_desc, SRC.style_group_num, SRC.style_group_desc, SRC.sbclass_num, SRC.sbclass_name, SRC
 .class_num, SRC.class_name, SRC.dept_num, SRC.dept_name, SRC.subdivision_num, SRC.subdivision_name, SRC.division_num,
 SRC.division_name, SRC.merch_dept_num, SRC.nonmerch_fee_code, SRC.fee_code_desc, SRC.vpn_num, SRC.vendor_brand_name,
 SRC.supplier_num, SRC.supplier_name, SRC.payto_vendor_num, SRC.payto_vendor_name, SRC.npg_ind, SRC.same_day_store_return_ind
 , SRC.product_return_ind, SRC.return_probability, SRC.jwn_srr_retail_usd_amt, SRC.jwn_srr_retail_amt, SRC.jwn_srr_units
 , SRC.jwn_srr_cost_usd_amt, SRC.jwn_srr_cost_amt, SRC.non_merch_ind, SRC.operational_gmv_usd_amt, SRC.operational_gmv_amt
 , SRC.jwn_operational_gmv_ind, SRC.jwn_reported_gmv_ind, SRC.jwn_merch_net_sales_ind, SRC.net_sales_usd_amt, SRC.net_sales_amt
 , SRC.jwn_reported_net_sales_ind, SRC.jwn_reported_cost_of_sales_ind, SRC.jwn_reported_gross_margin_ind, SRC.jwn_variable_expenses_ind
 , SRC.original_line_item_usd_amt, SRC.original_line_item_amt, SRC.original_line_item_amt_currency_code, SRC.return_date
 , SRC.sale_date, SRC.remote_selling_type, SRC.ertm_tran_date, SRC.tran_line_id, SRC.service_type, SRC.flash_event_ind,
 SRC.rp_on_sale_date, SRC.rp_off_sale_date, CURRENT_DATE, current_datetime('PST8PDT')
 );


--Log load step


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_fact_ld' AS sql_desc,
  'Incremental' AS load_desc,
  1 AS step_num,
  current_datetime('PST8PDT') AS step_log_timstp,
  'JWN_CLARITY_TRAN_FCT' AS interface_code,
 (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS curr_batch_date,
  current_datetime('PST8PDT') AS dw_sys_load_tmstp);

--Step 2: check for Amperity updates that need to be applied.

TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_new_acp_wrk;

INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_transaction_new_acp_wrk (transaction_id, global_tran_id,
 business_day_date, acp_id)
(SELECT DISTINCT jctf.transaction_id,
  rthf.global_tran_id,
  rthf.business_day_date,
  rthf.acp_id
 FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_hdr_fact AS rthf
  INNER JOIN (SELECT DISTINCT transaction_id,
    global_tran_id,
    business_day_date,
    acp_id
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact
   WHERE LOWER(record_source) = LOWER('S')) AS jctf ON rthf.global_tran_id = jctf.global_tran_id AND rthf.business_day_date
      = jctf.business_day_date AND LOWER(COALESCE(jctf.acp_id, '')) <> LOWER(COALESCE(rthf.acp_id, ''))
 WHERE rthf.business_day_date BETWEEN DATE '2021-01-01' AND (DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)));

INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_fact_ld' AS sql_desc,
  'Incremental' AS load_desc,
  2 AS step_num,
  current_datetime('PST8PDT') AS step_log_timstp,
  'JWN_CLARITY_TRAN_FCT' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS curr_batch_date,
  current_datetime('PST8PDT') AS dw_sys_load_tmstp);


--Step 3  --For the 'S' records that had an acp id change above,
--Determine what the customer loyalty status was at the time of demand & refresh it and acp_id key


--COALESCE is TD performance trick to avoid skewing on NULL acp_id keys


--Filter condition in PRD_NAP_USR_VWS.LOYALTY_LEVEL_LIFECYCLE_FACT_VW


--Filter down to highest loyalty rewards level that matches the join condition


--Need to touch the audit columns such as dw_sys_updt_tmstp...


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt 
SET
 loyalty_status = SRC.loyalty_status,
 acp_id = SRC.acp_id,
 dw_batch_date = (SELECT dw_batch_dt FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')),
 dw_sys_updt_tmstp = current_datetime('PST8PDT') 
FROM (SELECT fct.transaction_id,
   fct.transaction_line_seq_num,
   fct.record_source,
   wrk.global_tran_id,
   wrk.acp_id,
   lvl.rewards_level AS loyalty_status
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_new_acp_wrk AS wrk
  INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact AS fct 
  ON LOWER(fct.transaction_id) = LOWER(wrk.transaction_id) 
  AND wrk.global_tran_id = fct.global_tran_id 
  AND wrk.business_day_date = fct.business_day_date
  AND LOWER(fct.record_source) = LOWER('S')
  LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.analytical_customer AS acust 
  ON LOWER(acust.acp_id) = LOWER(COALESCE(wrk.acp_id, '@' || TRIM(FORMAT('%20d', wrk.global_tran_id)))) 
  AND LOWER(acust.acp_loyalty_id) <> LOWER('')
  LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.loyalty_level_lifecycle_fact AS lvl 
  ON LOWER(lvl.loyalty_id) = LOWER(COALESCE(acust.acp_loyalty_id, '@' || TRIM(FORMAT('%20d', wrk.global_tran_id)))) 
  AND CAST(fct.demand_tmstp_pacific AS DATE) >= lvl.start_day_date
  AND CAST(fct.demand_tmstp_pacific AS DATE) < lvl.end_day_date 
  AND lvl.end_day_date >= DATE '2019-01-01'
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY fct.transaction_id, fct.transaction_line_seq_num, fct.record_source, fct.global_tran_id
  ORDER BY CASE WHEN LOWER(lvl.rewards_level) IN (LOWER('MEMBER')) THEN 1
                WHEN LOWER(lvl.rewards_level) IN (LOWER('INSIDER'), LOWER('L1')) THEN 2
                WHEN LOWER(lvl.rewards_level) IN (LOWER('INFLUENCER'), LOWER('L2')) THEN 3
                WHEN LOWER(lvl.rewards_level) IN (LOWER('AMBASSADOR'), LOWER('L3')) THEN 4
                WHEN LOWER(lvl.rewards_level) IN (LOWER('ICON'), LOWER('L4')) THEN 5
                ELSE 0
           END, 
  lvl.start_day_date, 
  lvl.end_day_date, 
  lvl.dw_sys_updt_tmstp DESC, 
  lvl.loyalty_id)) = 1) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) 
AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
AND LOWER(tgt.record_source) = LOWER(SRC.record_source) 
AND COALESCE(tgt.global_tran_id,- 1) = COALESCE(SRC.global_tran_id, - 1);


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_fact_ld' AS sql_desc,
  'Incremental' AS load_desc,
  3 AS step_num,
  current_datetime('PST8PDT') AS step_log_timstp,
  'JWN_CLARITY_TRAN_FCT' AS interface_code,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS curr_batch_date,
  current_datetime('PST8PDT') AS dw_sys_load_tmstp);


--Step 4  --For the 'O' records that had an acp id change above,


--Determine what the customer loyalty status was at the time of demand & refresh it and acp_id key


--Sometimes a stale UPC-to-SKU lookup was used


--Following QUALIFY clause determines the best match by checking several conditions that would give


--us confidence that we are performing the correct match.  There are lots of times when the tran_line_id


--on the ERTM record is just 1 or 2 and implies a false match so we have this fuzzy match logic here.


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt 
SET loyalty_status = SRC.loyalty_status,
 acp_id = SRC.ertm_acp_id,
 dw_batch_date = (SELECT dw_batch_dt FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')),
 dw_sys_updt_tmstp = current_datetime('PST8PDT') 
 FROM (SELECT o.transaction_id,  o.business_day_date,  o.demand_tmstp_pacific_utc,o.demand_tmstp_pacific_tz,  o.rms_sku_num,  o.upc_num,  o.order_num,  o.transaction_line_seq_num,  s.global_tran_id,  s.transaction_line_seq_num AS ertm_seq_num,  s.rms_sku_num AS ertm_sku,  s.upc_num AS ertm_upc,  s.business_day_date AS ertm_business_day_date,  s.acp_id AS ertm_acp_id,  s.loyalty_status,  s.line_item_fulfillment_type AS ertm_fufillment_type,  s.fulfilled_from_location AS ertm_fulfilled_from_location,  s.tran_line_id AS ertm_tran_line_id
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_new_acp_wrk AS wrk
       INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact AS s 
       ON LOWER(s.transaction_id) = LOWER(wrk.transaction_id) 
       AND wrk.global_tran_id = s.global_tran_id 
       AND wrk.business_day_date = s.business_day_date 
       AND LOWER(s.record_source) = LOWER('S') 
       AND LOWER(s.order_num) <> LOWER('')
       INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact AS o 
       ON LOWER(s.order_num) = LOWER(o.order_num)
       AND LOWER(s.tran_type_code) = LOWER('SALE') 
       AND LOWER(s.line_item_activity_type_code) = LOWER('S') 
       AND LOWER(s.order_num) <> LOWER('') 
       AND s.business_day_date >= DATE '2021-01-01' 
       AND s.demand_date = o.demand_date 
       AND (LOWER(s.rms_sku_num) = LOWER(o.rms_sku_num) OR LOWER(s.upc_num) = LOWER(o.upc_num)) 
       AND s.line_item_quantity = o.line_item_quantity
       WHERE LOWER(o.record_source) = LOWER('O')
       QUALIFY (ROW_NUMBER() OVER (PARTITION BY o.transaction_id, o.rms_sku_num, o.transaction_line_seq_num 
       ORDER BY CASE WHEN LOWER(s.line_item_fulfillment_type) = LOWER(o.line_item_fulfillment_type) THEN 1 ELSE 2 END, 
        CASE WHEN o.fulfilled_from_location = s.fulfilled_from_location THEN 1 ELSE 2 END, 
        CASE WHEN s.tran_line_id = o.transaction_line_seq_num AND s.tran_line_id > 2 THEN 1 ELSE 2 END, 
        CASE WHEN s.tran_line_id = o.transaction_line_seq_num THEN 1 ELSE 2 END, s.business_day_date, s.global_tran_id, s.transaction_line_seq_num)) = 1) AS SRC
WHERE LOWER(tgt.record_source) = LOWER('O') 
AND LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) 
AND tgt.business_day_date = SRC.business_day_date 
AND tgt.demand_tmstp_pacific = SRC.demand_tmstp_pacific_utc 
AND tgt.demand_tmstp_pacific_tz = SRC.demand_tmstp_pacific_tz
AND LOWER(tgt.rms_sku_num) = LOWER(SRC.rms_sku_num) 
AND LOWER(tgt.order_num) = LOWER(SRC.order_num) 
AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num;


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_fact_ld' AS sql_desc,
  'Incremental' AS load_desc,
  4 AS step_num,
  current_datetime('PST8PDT') AS step_log_timstp,
  'JWN_CLARITY_TRAN_FCT' AS interface_code,
  (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS curr_batch_date,
  current_datetime('PST8PDT') AS dw_sys_load_tmstp);


--Update WAC values if late-arriving data to WEIGHTED_AVERAGE_COST table and something changes.


--Logic below is tailored from similar join & expressions in the *_sales_delta script.



UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt SET
 wac_available_ind = SRC.wac_available_ind2,
 weighted_average_cost = SRC.weighted_average_cost2,
 operational_gmv_cost_usd_amt = SRC.operational_gmv_cost_usd_amt2,
 operational_gmv_cost_amt = SRC.operational_gmv_cost_amt2,
 cost_of_sales_usd_amt = SRC.cost_of_sales_usd_amt,
 cost_of_sales_amt = SRC.cost_of_sales_amt,
 jwn_srr_cost_usd_amt = IFNULL(- 1 * tgt.return_probability * SRC.operational_gmv_cost_usd_amt2, 0),
 jwn_srr_cost_amt = IFNULL(- 1 * tgt.return_probability * SRC.operational_gmv_cost_amt2, 0),
 dw_sys_updt_tmstp = current_datetime('PST8PDT') FROM (SELECT rtdf.transaction_id,
   rtdf.record_source,
   rtdf.transaction_line_seq_num,
   rtdf.global_tran_id,
   rtdf.business_day_date,
    CASE
    WHEN LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND LOWER(wac.sku_num) <> LOWER('')
    THEN RPAD('Y', 1, ' ')
    ELSE 'N'
    END AS wac_available_ind2,
    CASE
    WHEN rtdf.line_net_amt <> 0
    THEN COALESCE(CASE
       WHEN CASE
         WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
          rtdf.original_line_item_amt_currency_code IS NOT NULL
         THEN - 1 * rtdf.original_line_item_amt
         ELSE rtdf.line_net_amt
         END > 0
       THEN CASE
        WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
          LOWER('Y')
        THEN COALESCE(wac.weighted_average_cost, 0)
        ELSE 0
        END
       ELSE CASE
        WHEN CASE
           WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
            rtdf.original_line_item_amt_currency_code IS NOT NULL
           THEN - 1 * rtdf.original_line_item_amt
           ELSE rtdf.line_net_amt
           END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
        THEN CASE
         WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind)
           = LOWER('Y')
         THEN COALESCE(wac.weighted_average_cost, 0)
         ELSE 0
         END
        ELSE NULL
        END
       END, 0) - COALESCE(CASE
       WHEN CASE
         WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
          rtdf.original_line_item_amt_currency_code IS NOT NULL
         THEN - 1 * rtdf.original_line_item_amt
         ELSE rtdf.line_net_amt
         END < 0
       THEN CASE
        WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
          LOWER('Y')
        THEN COALESCE(wac.weighted_average_cost, 0)
        ELSE 0
        END
       WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
          WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
           rtdf.original_line_item_amt_currency_code IS NOT NULL
          THEN - 1 * rtdf.original_line_item_amt
          ELSE rtdf.line_net_amt
          END = 0
       THEN CASE
        WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
          LOWER('Y')
        THEN COALESCE(wac.weighted_average_cost, 0)
        ELSE 0
        END
       ELSE NULL
       END, 0)
    ELSE 0
    END AS weighted_average_cost2,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * IFNULL(CASE
       WHEN rtdf.line_net_amt <> 0
       THEN COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END > 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE CASE
           WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
           THEN CASE
            WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
               ) = LOWER('Y')
            THEN COALESCE(wac.weighted_average_cost, 0)
            ELSE 0
            END
           ELSE NULL
           END
          END, 0) - COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END < 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
             WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
              AND rtdf.original_line_item_amt_currency_code IS NOT NULL
             THEN - 1 * rtdf.original_line_item_amt
             ELSE rtdf.line_net_amt
             END = 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE NULL
          END, 0)
       ELSE 0
       END, 0)
    ELSE 0
    END AS operational_gmv_cost_usd_amt2,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * IFNULL(CASE
       WHEN rtdf.line_net_amt <> 0
       THEN COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END > 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE CASE
           WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
           THEN CASE
            WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
               ) = LOWER('Y')
            THEN COALESCE(wac.weighted_average_cost, 0)
            ELSE 0
            END
           ELSE NULL
           END
          END, 0) - COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END < 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
             WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
              AND rtdf.original_line_item_amt_currency_code IS NOT NULL
             THEN - 1 * rtdf.original_line_item_amt
             ELSE rtdf.line_net_amt
             END = 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE NULL
          END, 0)
       ELSE 0
       END, 0)
    ELSE 0
    END AS operational_gmv_cost_amt2,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * IFNULL(CASE
       WHEN rtdf.line_net_amt <> 0
       THEN COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END > 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE CASE
           WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
           THEN CASE
            WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
               ) = LOWER('Y')
            THEN COALESCE(wac.weighted_average_cost, 0)
            ELSE 0
            END
           ELSE NULL
           END
          END, 0) - COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END < 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
             WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
              AND rtdf.original_line_item_amt_currency_code IS NOT NULL
             THEN - 1 * rtdf.original_line_item_amt
             ELSE rtdf.line_net_amt
             END = 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE NULL
          END, 0)
       ELSE 0
       END, 0)
    ELSE 0
    END AS cost_of_sales_usd_amt,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN - 1 * IFNULL(CASE
       WHEN rtdf.line_net_amt <> 0
       THEN COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END > 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE CASE
           WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
           THEN CASE
            WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
               ) = LOWER('Y')
            THEN COALESCE(wac.weighted_average_cost, 0)
            ELSE 0
            END
           ELSE NULL
           END
          END, 0) - COALESCE(CASE
          WHEN CASE
            WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
             AND rtdf.original_line_item_amt_currency_code IS NOT NULL
            THEN - 1 * rtdf.original_line_item_amt
            ELSE rtdf.line_net_amt
            END < 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
             WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
              AND rtdf.original_line_item_amt_currency_code IS NOT NULL
             THEN - 1 * rtdf.original_line_item_amt
             ELSE rtdf.line_net_amt
             END = 0
          THEN CASE
           WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
              ) = LOWER('Y')
           THEN COALESCE(wac.weighted_average_cost, 0)
           ELSE 0
           END
          ELSE NULL
          END, 0)
       ELSE 0
       END, 0)
    ELSE 0
    END AS cost_of_sales_amt
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact AS rtdf
   INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.weighted_average_cost_date_dim AS wac ON LOWER(wac.sku_num) = LOWER(rtdf.rms_sku_num) AND
          LOWER(wac.location_num) = LOWER(TRIM(FORMAT('%11d', rtdf.intent_store_num))) AND rtdf.business_day_date >= wac
         .eff_begin_dt AND rtdf.business_day_date < wac.eff_end_dt AND LOWER(wac.weighted_average_cost_currency_code) =
       LOWER(rtdf.line_item_currency_code) AND LOWER(wac.sku_num_type) = LOWER('RMS') AND CAST(wac.last_updated_tmstp AS DATE)
     >= DATE_SUB(CURRENT_DATE, INTERVAL 3 DAY)
  WHERE LOWER(rtdf.record_source) = LOWER('S')
   AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
   AND rtdf.business_day_date >= DATE '2021-01-01'
   AND (LOWER(CASE
          WHEN LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND LOWER(wac.sku_num) <> LOWER('')
          THEN RPAD('Y', 1, ' ')
          ELSE 'N'
          END) <> LOWER(rtdf.wac_available_ind) OR CASE
         WHEN rtdf.line_net_amt <> 0
         THEN COALESCE(CASE
            WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END > 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, 0)
             ELSE 0
             END
            ELSE CASE
             WHEN CASE
                WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                    ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                THEN - 1 * rtdf.original_line_item_amt
                ELSE rtdf.line_net_amt
                END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
             THEN CASE
              WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                 ) = LOWER('Y')
              THEN COALESCE(wac.weighted_average_cost, 0)
              ELSE 0
              END
             ELSE NULL
             END
            END, 0) - COALESCE(CASE
            WHEN CASE
              WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
               AND rtdf.original_line_item_amt_currency_code IS NOT NULL
              THEN - 1 * rtdf.original_line_item_amt
              ELSE rtdf.line_net_amt
              END < 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, 0)
             ELSE 0
             END
            WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
               WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                   ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
               THEN - 1 * rtdf.original_line_item_amt
               ELSE rtdf.line_net_amt
               END = 0
            THEN CASE
             WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                ) = LOWER('Y')
             THEN COALESCE(wac.weighted_average_cost, 0)
             ELSE 0
             END
            ELSE NULL
            END, 0)
         ELSE 0
         END <> rtdf.weighted_average_cost OR CASE
        WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
        THEN - 1 * IFNULL(CASE
           WHEN rtdf.line_net_amt <> 0
           THEN COALESCE(CASE
              WHEN CASE
                WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                    ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                THEN - 1 * rtdf.original_line_item_amt
                ELSE rtdf.line_net_amt
                END > 0
              THEN CASE
               WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                  ) = LOWER('Y')
               THEN COALESCE(wac.weighted_average_cost, 0)
               ELSE 0
               END
              ELSE CASE
               WHEN CASE
                  WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                      ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                  THEN - 1 * rtdf.original_line_item_amt
                  ELSE rtdf.line_net_amt
                  END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
               THEN CASE
                WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                   ) = LOWER('Y')
                THEN COALESCE(wac.weighted_average_cost, 0)
                ELSE 0
                END
               ELSE NULL
               END
              END, 0) - COALESCE(CASE
              WHEN CASE
                WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                    ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                THEN - 1 * rtdf.original_line_item_amt
                ELSE rtdf.line_net_amt
                END < 0
              THEN CASE
               WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                  ) = LOWER('Y')
               THEN COALESCE(wac.weighted_average_cost, 0)
               ELSE 0
               END
              WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
                 WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                     ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                 THEN - 1 * rtdf.original_line_item_amt
                 ELSE rtdf.line_net_amt
                 END = 0
              THEN CASE
               WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                  ) = LOWER('Y')
               THEN COALESCE(wac.weighted_average_cost, 0)
               ELSE 0
               END
              ELSE NULL
              END, 0)
           ELSE 0
           END, 0)
        ELSE 0
        END <> rtdf.operational_gmv_cost_usd_amt OR CASE
       WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
       THEN - 1 * IFNULL(CASE
          WHEN rtdf.line_net_amt <> 0
          THEN COALESCE(CASE
             WHEN CASE
               WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                   ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
               THEN - 1 * rtdf.original_line_item_amt
               ELSE rtdf.line_net_amt
               END > 0
             THEN CASE
              WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                 ) = LOWER('Y')
              THEN COALESCE(wac.weighted_average_cost, 0)
              ELSE 0
              END
             ELSE CASE
              WHEN CASE
                 WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                     ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                 THEN - 1 * rtdf.original_line_item_amt
                 ELSE rtdf.line_net_amt
                 END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
              THEN CASE
               WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                  ) = LOWER('Y')
               THEN COALESCE(wac.weighted_average_cost, 0)
               ELSE 0
               END
              ELSE NULL
              END
             END, 0) - COALESCE(CASE
             WHEN CASE
               WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                   ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
               THEN - 1 * rtdf.original_line_item_amt
               ELSE rtdf.line_net_amt
               END < 0
             THEN CASE
              WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                 ) = LOWER('Y')
              THEN COALESCE(wac.weighted_average_cost, 0)
              ELSE 0
              END
             WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
                WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y'
                    ) AND rtdf.original_line_item_amt_currency_code IS NOT NULL
                THEN - 1 * rtdf.original_line_item_amt
                ELSE rtdf.line_net_amt
                END = 0
             THEN CASE
              WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind
                 ) = LOWER('Y')
              THEN COALESCE(wac.weighted_average_cost, 0)
              ELSE 0
              END
             ELSE NULL
             END, 0)
          ELSE 0
          END, 0)
       ELSE 0
       END <> rtdf.operational_gmv_cost_amt)) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) 
AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
AND LOWER(tgt.record_source) = LOWER(SRC.record_source) 
AND tgt.global_tran_id = SRC.global_tran_id 
AND tgt.business_day_date = SRC.business_day_date;


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.load_process_log_fact (sql_desc, load_desc, step_num, step_log_timstp,
 interface_code, curr_batch_date, dw_sys_load_tmstp)
(SELECT 'clarity_transaction_fact_ld' AS sql_desc,
  'Incremental' AS load_desc,
  5 AS step_num,
  current_datetime('PST8PDT') AS step_log_timstp,
  'JWN_CLARITY_TRAN_FCT' AS interface_code,
  (SELECT dw_batch_dt 
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup 
    WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS curr_batch_date,
  current_datetime('PST8PDT') AS dw_sys_load_tmstp);


--Update WAC values for late-arriving fallback scenarios at channel level from WEIGHTED_AVERAGE_COST_CHANNEL_DIM.


--Logic below is tailored from similar join & expressions in the *_sales_delta script.




  UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt 
  SET
 wac_available_ind = SRC.wac_available_ind2,
 weighted_average_cost = CAST(SRC.weighted_average_cost2 AS NUMERIC),
 operational_gmv_cost_usd_amt = CAST(SRC.operational_gmv_cost_usd_amt2 AS NUMERIC),
 operational_gmv_cost_amt = CAST(SRC.operational_gmv_cost_amt2 AS NUMERIC),
 cost_of_sales_usd_amt = CAST(SRC.cost_of_sales_usd_amt AS NUMERIC),
 cost_of_sales_amt = CAST(SRC.cost_of_sales_amt AS NUMERIC),
 jwn_srr_cost_usd_amt = IFNULL(- 1 * tgt.return_probability * SRC.operational_gmv_cost_usd_amt2, 0),
 jwn_srr_cost_amt = IFNULL(- 1 * tgt.return_probability * SRC.operational_gmv_cost_amt2, 0),
 dw_sys_updt_tmstp = current_datetime('PST8PDT') 
 FROM (SELECT rtdf.transaction_id,
   rtdf.record_source,
   rtdf.transaction_line_seq_num,
   rtdf.global_tran_id,
   rtdf.business_day_date,
   (CASE
  WHEN rtdf.line_net_amt <> 0
  THEN COALESCE(CASE
     WHEN CASE
       WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
        rtdf.original_line_item_amt_currency_code IS NOT NULL
       THEN - 1 * rtdf.original_line_item_amt
       ELSE rtdf.line_net_amt
       END > 0
     THEN CASE
      WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
        LOWER('Y')
      THEN COALESCE(wacc.weighted_average_cost, 0)
      ELSE 0
      END
     ELSE CASE
      WHEN CASE
         WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
          rtdf.original_line_item_amt_currency_code IS NOT NULL
         THEN - 1 * rtdf.original_line_item_amt
         ELSE rtdf.line_net_amt
         END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
      THEN CASE
       WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
         LOWER('Y')
       THEN COALESCE(wacc.weighted_average_cost, 0)
       ELSE 0
       END
      ELSE NULL
      END
     END, 0) - COALESCE(CASE
     WHEN CASE
       WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
        rtdf.original_line_item_amt_currency_code IS NOT NULL
       THEN - 1 * rtdf.original_line_item_amt
       ELSE rtdf.line_net_amt
       END < 0
     THEN CASE
      WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
        LOWER('Y')
      THEN COALESCE(wacc.weighted_average_cost, 0)
      ELSE 0
      END
     WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
        WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
         rtdf.original_line_item_amt_currency_code IS NOT NULL
        THEN - 1 * rtdf.original_line_item_amt
        ELSE rtdf.line_net_amt
        END = 0
     THEN CASE
      WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
        LOWER('Y')
      THEN COALESCE(wacc.weighted_average_cost, 0)
      ELSE 0
      END
     ELSE NULL
     END, 0)
  ELSE 0
  END) AS weighted_average_cost2,
    CASE
    WHEN LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND LOWER(wacc.sku_num) <> LOWER('')
    THEN RPAD('Y', 1, ' ')
    ELSE 'N'
    END AS wac_available_ind2,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN CAST(- 1 * IFNULL(1, 0) AS INTEGER)
    ELSE 0
    END AS operational_gmv_cost_usd_amt2,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN CAST(- 1 * IFNULL(1, 0) AS INTEGER)
    ELSE 0
    END AS operational_gmv_cost_amt2,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN CAST(- 1 * IFNULL(1, 0) AS INTEGER)
    ELSE 0
    END AS cost_of_sales_usd_amt,
    CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN CAST(- 1 * IFNULL(1, 0) AS INTEGER)
    ELSE 0
    END AS cost_of_sales_amt
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact AS rtdf
   INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim AS s01 ON rtdf.intent_store_num = s01.store_num
   INNER JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.weighted_average_cost_channel_dim AS wacc ON LOWER(wacc.sku_num) = LOWER(rtdf.rms_sku_num
          ) AND s01.channel_num = wacc.channel_num AND LOWER(wacc.sku_num_type) = LOWER('RMS') AND rtdf.business_day_date
        >= wacc.eff_begin_dt AND rtdf.business_day_date < wacc.eff_end_dt AND CAST(wacc.dw_sys_load_tmstp AS DATE) >=
     DATE_SUB(CURRENT_DATE, INTERVAL 3 DAY)
  WHERE LOWER(rtdf.record_source) = LOWER('S')
   AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y')
   AND LOWER(rtdf.wac_available_ind) = LOWER('N')
   AND rtdf.business_day_date >= DATE '2021-01-01'
   AND (LOWER(CASE
          WHEN LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND LOWER(wacc.sku_num) <> LOWER('')
          THEN RPAD('Y', 1, ' ')
          ELSE 'N'
          END) <> LOWER(rtdf.wac_available_ind) OR 1 <> rtdf.weighted_average_cost OR CASE
        WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
        THEN CAST(- 1 * IFNULL(1, 0) AS INTEGER)
        ELSE 0
        END <> rtdf.operational_gmv_cost_usd_amt OR CASE
       WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
       THEN CAST(- 1 * IFNULL(1, 0) AS INTEGER)
       ELSE 0
       END <> rtdf.operational_gmv_cost_amt)
	   AND (  (CASE
    WHEN LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND LOWER(wacc.sku_num) <> LOWER('')
    THEN RPAD('Y', 1, ' ')
    ELSE 'N'
    END) <> rtdf.wac_available_ind
		  OR (CASE
  WHEN rtdf.line_net_amt <> 0
  THEN COALESCE(CASE
     WHEN CASE
       WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
        rtdf.original_line_item_amt_currency_code IS NOT NULL
       THEN - 1 * rtdf.original_line_item_amt
       ELSE rtdf.line_net_amt
       END > 0
     THEN CASE
      WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
        LOWER('Y')
      THEN COALESCE(wacc.weighted_average_cost, 0)
      ELSE 0
      END
     ELSE CASE
      WHEN CASE
         WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
          rtdf.original_line_item_amt_currency_code IS NOT NULL
         THEN - 1 * rtdf.original_line_item_amt
         ELSE rtdf.line_net_amt
         END = 0 AND LOWER(rtdf.tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
      THEN CASE
       WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
         LOWER('Y')
       THEN COALESCE(wacc.weighted_average_cost, 0)
       ELSE 0
       END
      ELSE NULL
      END
     END, 0) - COALESCE(CASE
     WHEN CASE
       WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
        rtdf.original_line_item_amt_currency_code IS NOT NULL
       THEN - 1 * rtdf.original_line_item_amt
       ELSE rtdf.line_net_amt
       END < 0
     THEN CASE
      WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
        LOWER('Y')
      THEN COALESCE(wacc.weighted_average_cost, 0)
      ELSE 0
      END
     WHEN LOWER(rtdf.tran_type_code) IN (LOWER('RETN')) AND CASE
        WHEN - 1 * rtdf.original_line_item_amt <> rtdf.line_net_amt AND LOWER(rtdf.wac_eligible_ind) = LOWER('Y') AND
         rtdf.original_line_item_amt_currency_code IS NOT NULL
        THEN - 1 * rtdf.original_line_item_amt
        ELSE rtdf.line_net_amt
        END = 0
     THEN CASE
      WHEN LOWER(COALESCE(rtdf.price_adj_code, '')) NOT IN (LOWER('A'), LOWER('B')) AND LOWER(rtdf.wac_eligible_ind) =
        LOWER('Y')
      THEN COALESCE(wacc.weighted_average_cost, 0)
      ELSE 0
      END
     ELSE NULL
     END, 0)
  ELSE 0
  END) <> rtdf.weighted_average_cost
		  OR CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN CAST(- 1 * IFNULL(1, 0) AS INTEGER)
    ELSE 0
    END  <> rtdf.operational_gmv_cost_usd_amt
		  OR CASE
    WHEN LOWER(rtdf.jwn_operational_gmv_ind) = LOWER('Y')
    THEN CAST(- 1 * IFNULL(1, 0) AS INTEGER)
    ELSE 0
    END <> rtdf.operational_gmv_cost_amt
		  )
		  ) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) 
AND tgt.transaction_line_seq_num = SRC.transaction_line_seq_num
AND LOWER(tgt.record_source) = LOWER(SRC.record_source) 
AND tgt.global_tran_id = SRC.global_tran_id 
AND tgt.business_day_date = SRC.business_day_date;


--Update the loyalty defferal amounts that changed on a given day into transaction fact


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt 
SET
 loyalty_deferral_usd_amt = SRC.loyalty_deferral_usd_amt,
 loyalty_deferral_amt = SRC.loyalty_deferral_amt,
 loyalty_breakage_usd_amt = SRC.loyalty_breakage_usd_amt,
 loyalty_breakage_amt = SRC.loyalty_breakage_amt,
 dw_batch_date = (SELECT max(dw_batch_dt) FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')),
 dw_sys_updt_tmstp = current_datetime('PST8PDT') 
 FROM (SELECT *
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw
       WHERE dw_batch_date = (SELECT max(dw_batch_dt)
                              FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
                              WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES'))) AS SRC
WHERE tgt.transaction_line_seq_num = SRC.line_item_seq_num 
AND LOWER(tgt.record_source) = LOWER(SRC.record_source) 
AND tgt.global_tran_id = SRC.global_tran_id 
AND tgt.business_day_date = SRC.business_day_date 
AND (tgt.loyalty_deferral_usd_amt <> SRC.loyalty_deferral_usd_amt 
OR tgt.loyalty_deferral_amt <> SRC.loyalty_deferral_amt 
OR tgt.loyalty_breakage_usd_amt <> SRC.loyalty_breakage_usd_amt
OR tgt.loyalty_breakage_amt <> SRC.loyalty_breakage_amt);


--Update the giftcard Breakage amounts that changed on a given day into transaction fact


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt 
SET
 gift_card_issued_usd_amt = SRC.gift_card_issued_usd_amt,
 gift_card_issued_amt = SRC.gift_card_issued_amt,
 gift_card_reload_adj_usd_amt = SRC.gift_card_reload_adj_usd_amt,
 gift_card_reload_adj_amt = SRC.gift_card_reload_adj_amt,
 gc_expected_breakage_usd_amt = SRC.gc_expected_breakage_usd_amt,
 gc_expected_breakage_amt = SRC.gc_expected_breakage_amt,
 gift_card_redeemed_usd_amt = SRC.gift_card_redeemed_usd_amt,
 gift_card_redeemed_amt = SRC.gift_card_redeemed_amt,
 dw_batch_date = (SELECT max(dw_batch_dt) FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')),
 dw_sys_updt_tmstp = current_datetime('PST8PDT') 
FROM (SELECT *
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw
      WHERE dw_batch_date = (SELECT max(dw_batch_dt) 
                             FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup 
                             WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES'))) AS SRC
WHERE tgt.transaction_line_seq_num = SRC.line_item_seq_num 
AND LOWER(tgt.record_source) = LOWER(SRC.record_source) 
AND tgt.global_tran_id = SRC.global_tran_id 
AND tgt.business_day_date = SRC.business_day_date 
AND (tgt.gift_card_issued_usd_amt <> SRC.gift_card_issued_usd_amt 
OR tgt.gift_card_issued_amt <> SRC.gift_card_issued_amt 
OR tgt.gift_card_reload_adj_usd_amt <> SRC.gift_card_reload_adj_usd_amt 
OR tgt.gift_card_reload_adj_amt <> SRC.gift_card_reload_adj_amt 
OR tgt.gc_expected_breakage_usd_amt <> SRC.gc_expected_breakage_usd_amt 
OR tgt.gc_expected_breakage_amt <> SRC.gc_expected_breakage_amt 
OR tgt.gift_card_redeemed_usd_amt <> SRC.gift_card_redeemed_usd_amt 
OR tgt.gift_card_redeemed_amt <> SRC.gift_card_redeemed_amt);


--Propagate the price_adjustment_ind flag to other rows referring to same purchase line item


UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_transaction_fact AS tgt 
SET price_adjustment_ind = SRC.price_adjustment_ind 
FROM (SELECT DISTINCT transaction_id, demand_date, demand_tmstp_pacific_utc,demand_tmstp_pacific_tz, rms_sku_num, price_adjustment_ind
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk
      WHERE LOWER(price_adjustment_ind) = LOWER('Y')
      AND LOWER(record_source) IN (LOWER('S'), LOWER('O'))) AS SRC
WHERE LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) 
AND tgt.demand_date = SRC.demand_date 
AND tgt.demand_tmstp_pacific= SRC.demand_tmstp_pacific_utc
AND tgt.demand_tmstp_pacific_tz= SRC.demand_tmstp_pacific_tz
AND LOWER(tgt.rms_sku_num) = LOWER(SRC.rms_sku_num) 
AND LOWER(COALESCE(tgt.line_item_activity_type_code, 'S')) = LOWER('S') 
AND LOWER(tgt.price_adjustment_ind) <> LOWER('Y') 
AND LOWER(tgt.record_source) IN (LOWER('S'), LOWER('O'));