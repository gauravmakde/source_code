TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_rp_anticipated_spend_onorder_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_rp_anticipated_spend_onorder_wrk (purchase_order_number, rms_sku_num, epm_sku_num,
 store_num, week_num, ship_location_id, rms_casepack_num, epm_casepack_num, order_from_vendor_id, anticipated_price_type
 , order_category_code, po_type, order_type, purchase_type, status, start_ship_date, end_ship_date, otb_eow_date,
 first_approval_date, latest_approval_date, first_approval_event_tmstp_pacific,first_approval_event_tmstp_pacific_tz, latest_approval_event_tmstp_pacific,latest_approval_event_tmstp_pacific_tz,
 anticipated_retail_amt, total_expenses_per_unit_currency, quantity_ordered, quantity_canceled, quantity_received,
 unit_cost_amt, total_expenses_per_unit_amt, total_duty_per_unit_amt, unit_estimated_landing_cost, channel_num,
 on_order_total_unit, on_order_total_elc_amt, on_order_total_retail_amt, rp_ant_spend_plan_evnt_tmstp,
 rp_ant_spend_plan_evnt_tmstp_tz,
 rp_ant_spend_plan_evnt_tmstp_pacific,rp_ant_spend_plan_evnt_tmstp_pacific_tz, rp_ant_spend_plan_date_pacific, dw_batch_date, dw_sys_load_tmstp)
(SELECT on_order.purchase_order_number,
  on_order.rms_sku_num,
  on_order.epm_sku_num,
  on_order.store_num,
  on_order.week_num,
  on_order.ship_location_id,
  on_order.rms_casepack_num,
  on_order.epm_casepack_num,
  on_order.order_from_vendor_id,
  on_order.anticipated_price_type,
  on_order.order_category_code,
  on_order.po_type,
  on_order.order_type,
  on_order.purchase_type,
  on_order.status,
  on_order.start_ship_date,
  on_order.end_ship_date,
  on_order.otb_eow_date,
  on_order.first_approval_date,
  on_order.latest_approval_date, 
  cast(on_order.first_approval_event_tmstp_pacific as timestamp),
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(first_approval_event_tmstp_pacific AS STRING)) AS first_approval_event_tmstp_pacific_tz,
  cast(on_order.latest_approval_event_tmstp_pacific as timestamp),
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(latest_approval_event_tmstp_pacific AS STRING)) AS latest_approval_event_tmstp_pacific_tz,
  on_order.anticipated_retail_amt,
  on_order.total_expenses_per_unit_currency,
  on_order.quantity_ordered,
  on_order.quantity_canceled,
  on_order.quantity_received,
  on_order.unit_cost_amt,
  on_order.total_expenses_per_unit_amt,
  on_order.total_duty_per_unit_amt,
  on_order.unit_estimated_landing_cost,
  rp_ant.channel_num,
  on_order.quantity_open AS on_order_total_unit,
  on_order.total_estimated_landing_cost AS on_order_total_elc_amt,
  on_order.total_anticipated_retail_amt AS on_order_total_retail_amt,
  cast(rp_ant.max_event_tmstp as timestamp) AS rp_ant_spend_plan_evnt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(rp_ant.max_event_tmstp AS STRING)) AS rp_ant_spend_plan_evnt_tmstp_tz,

  cast(rp_ant.max_event_tmstp AS timestamp) rp_ant_spend_plan_evnt_tmstp_pacific,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(rp_ant.max_event_tmstp AS STRING)) AS rp_ant_spend_plan_evnt_tmstp_pacific_tz,
  CAST(rp_ant.max_event_tmstp AS DATE) AS rp_ant_spend_plan_date_pacific,
  CURRENT_DATE('PST8PDT'),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_on_order_fact_vw AS on_order
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_rp_booked_measures_wrk_vw AS rp_ant ON LOWER(on_order.rms_sku_num) = LOWER(rp_ant.rms_sku_num
     ) AND on_order.week_num = rp_ant.week_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS store ON on_order.store_num = store.store_num AND rp_ant.channel_num = store
    .channel_num
 WHERE on_order.order_type IN (SELECT config_value
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
    WHERE LOWER(config_key) = LOWER('MFP_ORDER_TYPE'))
  AND (on_order.first_approval_date < DATE_SUB(CAST(rp_ant.max_event_tmstp AS DATE), INTERVAL 1 DAY) OR on_order.first_approval_date
       < DATE_SUB(CAST(rp_ant.max_event_tmstp AS DATE), INTERVAL 1 DAY) AND on_order.latest_approval_date > DATE_SUB(CAST(rp_ant.max_event_tmstp AS DATE)
       ,INTERVAL 1 DAY)));


--COLLECT STATS ON prd_NAP_STG.MERCH_RP_ANTICIPATED_SPEND_ONORDER_WRK