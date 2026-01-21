-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=mfp_financial_country_banner_plan_weekly_snapshot_load;
-- Task_Name=mfp_financial_country_banner_plan_t1_load_last_fiscal_week_snapshot;'
-- FOR SESSION VOLATILE;

--ET;



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_financial_country_banner_plan_snapshot_fact AS tgt
WHERE snapshot_week_id = (SELECT week_idnt AS snapshot_week_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
  WHERE day_date < CURRENT_DATE('PST8PDT')
   AND day_num_of_fiscal_week = 7
  QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_financial_country_banner_plan_snapshot_fact (source_publish_time,source_publish_time_tz, approved_time,approved_time_tz, week_id,
 department_number, channel_country, pricing_channel, last_updated_time_in_millis, last_updated_time,last_updated_time_tz,
 financial_plan_version, alternate_inventory_model, snapshot_week_id, active_beginning_inventory_dollar_currency_code,
 active_beginning_inventory_dollar_amount, active_beginning_inventory_units,
 active_beginning_period_inventory_target_units, inactive_beginning_period_inventory_dollar_currency_code,
 inactive_beginning_period_inventory_dollar_amount, inactive_beginning_period_inventory_units,
 discount_terms_cost_currency_code, discount_terms_cost_amount, active_ending_inventory_dollar_currency_code,
 active_ending_inventory_dollar_amount, active_ending_inventory_units, inactive_ending_inventory_dollar_currency_code,
 inactive_ending_inventory_dollar_amount, inactive_ending_inventory_units, active_inventory_in_dollar_currency_code,
 active_inventory_in_dollar_amount, active_inventory_in_units, active_inventory_out_dollar_currency_code,
 active_inventory_out_dollar_amount, active_inventory_out_units, inactive_inventory_in_dollar_currency_code,
 inactive_inventory_in_dollar_amount, inactive_inventory_in_units, inactive_inventory_out_dollar_currency_code,
 inactive_inventory_out_dollar_amount, inactive_inventory_out_units, other_inventory_in_dollar_currency_code,
 other_inventory_in_dollar_amount, other_inventory_in_units, other_inventory_out_dollar_currency_code,
 other_inventory_out_dollar_amount, other_inventory_out_units, racking_in_dollar_currency_code, racking_in_dollar_amount
 , racking_in_units, racking_out_dollar_currency_code, racking_out_dollar_amount, racking_out_units,
 merch_margin_retail_currency_code, merch_margin_retail_amount, active_mos_dollar_currency_code,
 active_mos_dollar_amount, active_mos_units, inactive_mos_dollar_currency_code, inactive_mos_dollar_amount,
 inactive_mos_units, active_receipts_dollar_currency_code, active_receipts_dollar_amount, active_receipts_units,
 inactive_receipts_dollar_currency_code, inactive_receipts_dollar_amount, inactive_receipts_units,
 reserve_receipts_dollar_currency_code, reserve_receipts_dollar_amount, reserve_receipts_units,
 reclass_in_dollar_currency_code, reclass_in_dollar_amount, reclass_in_units, reclass_out_dollar_currency_code,
 reclass_out_dollar_amount, reclass_out_units, active_rtv_dollar_currency_code, active_rtv_dollar_amount,
 active_rtv_units, inactive_rtv_dollar_currency_code, inactive_rtv_dollar_amount, inactive_rtv_units,
 vendor_funds_cost_currency_code, vendor_funds_cost_amount, inventory_movement_drop_ship_in_dollar_currency_code,
 inventory_movement_drop_ship_in_dollar_amount, inventory_movement_drop_ship_in_units,
 inventory_movement_drop_ship_out_dollar_currency_code, inventory_movement_drop_ship_out_dollar_amount,
 inventory_movement_drop_ship_out_units, other_inventory_adjustments_dollar_currency_code,
 other_inventory_adjustments_dollar_amount, other_inventory_adjustments_units, active_opentobuy_cost_currency_code,
 active_opentobuy_cost_amount, inactive_opentobuy_cost_currency_code, inactive_opentobuy_cost_amount,
 active_commitments_cost_currency_code, active_commitments_cost_amount, inactive_commitments_cost_currency_code,
 inactive_commitments_cost_amount, dw_sys_load_tmstp,dw_sys_load_tmstp_tz)
(SELECT fct.source_publish_time_utc,fct.source_publish_time_tz,
  fct.approved_time_utc,fct.approved_time_tz,
  fct.week_id,
  fct.department_number,
  fct.channel_country,
  fct.pricing_channel,
  fct.last_updated_time_in_millis,
  fct.last_updated_time_utc,fct.last_updated_time_tz,
  fct.financial_plan_version,
  fct.alternate_inventory_model,
  dcd.snapshot_week_id,
  fct.active_beginning_inventory_dollar_currency_code,
  fct.active_beginning_inventory_dollar_amount,
  fct.active_beginning_inventory_units,
  fct.active_beginning_period_inventory_target_units,
  fct.inactive_beginning_period_inventory_dollar_currency_code,
  fct.inactive_beginning_period_inventory_dollar_amount,
  fct.inactive_beginning_period_inventory_units,
  fct.discount_terms_cost_currency_code,
  fct.discount_terms_cost_amount,
  fct.active_ending_inventory_dollar_currency_code,
  fct.active_ending_inventory_dollar_amount,
  fct.active_ending_inventory_units,
  fct.inactive_ending_inventory_dollar_currency_code,
  fct.inactive_ending_inventory_dollar_amount,
  fct.inactive_ending_inventory_units,
  fct.active_inventory_in_dollar_currency_code,
  fct.active_inventory_in_dollar_amount,
  fct.active_inventory_in_units,
  fct.active_inventory_out_dollar_currency_code,
  fct.active_inventory_out_dollar_amount,
  fct.active_inventory_out_units,
  fct.inactive_inventory_in_dollar_currency_code,
  fct.inactive_inventory_in_dollar_amount,
  fct.inactive_inventory_in_units,
  fct.inactive_inventory_out_dollar_currency_code,
  fct.inactive_inventory_out_dollar_amount,
  fct.inactive_inventory_out_units,
  fct.other_inventory_in_dollar_currency_code,
  fct.other_inventory_in_dollar_amount,
  fct.other_inventory_in_units,
  fct.other_inventory_out_dollar_currency_code,
  fct.other_inventory_out_dollar_amount,
  fct.other_inventory_out_units,
  fct.racking_in_dollar_currency_code,
  fct.racking_in_dollar_amount,
  fct.racking_in_units,
  fct.racking_out_dollar_currency_code,
  fct.racking_out_dollar_amount,
  fct.racking_out_units,
  fct.merch_margin_retail_currency_code,
  fct.merch_margin_retail_amount,
  fct.active_mos_dollar_currency_code,
  fct.active_mos_dollar_amount,
  fct.active_mos_units,
  fct.inactive_mos_dollar_currency_code,
  fct.inactive_mos_dollar_amount,
  fct.inactive_mos_units,
  fct.active_receipts_dollar_currency_code,
  fct.active_receipts_dollar_amount,
  fct.active_receipts_units,
  fct.inactive_receipts_dollar_currency_code,
  fct.inactive_receipts_dollar_amount,
  fct.inactive_receipts_units,
  fct.reserve_receipts_dollar_currency_code,
  fct.reserve_receipts_dollar_amount,
  fct.reserve_receipts_units,
  fct.reclass_in_dollar_currency_code,
  fct.reclass_in_dollar_amount,
  fct.reclass_in_units,
  fct.reclass_out_dollar_currency_code,
  fct.reclass_out_dollar_amount,
  fct.reclass_out_units,
  fct.active_rtv_dollar_currency_code,
  fct.active_rtv_dollar_amount,
  fct.active_rtv_units,
  fct.inactive_rtv_dollar_currency_code,
  fct.inactive_rtv_dollar_amount,
  fct.inactive_rtv_units,
  fct.vendor_funds_cost_currency_code,
  fct.vendor_funds_cost_amount,
  fct.inventory_movement_drop_ship_in_dollar_currency_code,
  fct.inventory_movement_drop_ship_in_dollar_amount,
  fct.inventory_movement_drop_ship_in_units,
  fct.inventory_movement_drop_ship_out_dollar_currency_code,
  fct.inventory_movement_drop_ship_out_dollar_amount,
  fct.inventory_movement_drop_ship_out_units,
  fct.other_inventory_adjustments_dollar_currency_code,
  fct.other_inventory_adjustments_dollar_amount,
  fct.other_inventory_adjustments_units,
  fct.active_opentobuy_cost_currency_code,
  fct.active_opentobuy_cost_amount,
  fct.inactive_opentobuy_cost_currency_code,
  fct.inactive_opentobuy_cost_amount,
  fct.active_commitments_cost_currency_code,
  fct.active_commitments_cost_amount,
  fct.inactive_commitments_cost_currency_code,
  fct.inactive_commitments_cost_amount,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT'))  AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_financial_country_banner_plan_fct AS fct
  INNER JOIN (SELECT week_idnt AS snapshot_week_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
   WHERE day_date < CURRENT_DATE('PST8PDT')
    AND day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1) AS dcd ON TRUE
 WHERE fct.week_id >= (SELECT DISTINCT month_start_week_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
    WHERE day_date < CURRENT_DATE('PST8PDT')
     AND day_num_of_fiscal_week = 7
    QUALIFY (DENSE_RANK() OVER (ORDER BY month_start_week_idnt DESC)) = 3));


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_financial_country_banner_plan_snapshot_fact AS sf
WHERE snapshot_week_id <= (SELECT retention_week_end_idnt
  FROM (SELECT month_start_day_date,
     month_end_day_date,
     month_end_week_idnt,
     MIN(month_end_week_idnt) OVER (ORDER BY month_idnt ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS
     retention_week_end_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_month_cal_454_vw) AS a
  WHERE CURRENT_DATE BETWEEN month_start_day_date AND month_end_day_date);