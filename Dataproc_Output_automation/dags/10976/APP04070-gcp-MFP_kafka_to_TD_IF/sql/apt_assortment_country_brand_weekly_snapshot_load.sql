DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_category_country_plan_snapshot_fact AS tgt
WHERE snapshot_week_id = (SELECT week_idnt AS snapshot_week_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
  WHERE day_date < CURRENT_DATE('PST8PDT')
   AND day_num_of_fiscal_week = 7
  QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_category_country_plan_snapshot_fact (snapshot_week_id, event_time,event_time_tz,
 plan_published_date, selling_country, selling_brand, category, price_band, dept_num, last_updated_time_in_millis,
 last_updated_time, last_updated_time_tz, month_num, month_label, alternate_inventory_model, average_inventory_units,
 average_inventory_retail_currcycd, average_inventory_retail_amt, average_inventory_cost_currcycd,
 average_inventory_cost_amt, beginning_of_period_inventory_units, beginning_of_period_inventory_retail_currcycd,
 beginning_of_period_inventory_retail_amt, beginning_of_period_inventory_cost_currcycd,
 beginning_of_period_inventory_cost_amt, return_to_vendor_units, return_to_vendor_retail_currcycd,
 return_to_vendor_retail_amt, return_to_vendor_cost_currcycd, return_to_vendor_cost_amt, rack_transfer_units,
 rack_transfer_retail_currcycd, rack_transfer_retail_amt, rack_transfer_cost_currcycd, rack_transfer_cost_amt,
 active_inventory_in_units, active_inventory_in_retail_currcycd, active_inventory_in_retail_amt,
 active_inventory_in_cost_currcycd, active_inventory_in_cost_amt, active_inventory_out_units,
 active_inventory_out_retail_currcycd, active_inventory_out_retail_amt, active_inventory_out_cost_currcycd,
 active_inventory_out_cost_amt, receipts_units, receipts_retail_currcycd, receipts_retail_amt, receipts_cost_currcycd,
 receipts_cost_amt, receipt_less_reserve_units, receipt_less_reserve_retail_currcycd, receipt_less_reserve_retail_amt,
 receipt_less_reserve_cost_currcycd, receipt_less_reserve_cost_amt, pah_transfer_in_units,
 pah_transfer_in_retail_currcycd, pah_transfer_in_retail_amt, pah_transfer_in_cost_currcycd, pah_transfer_in_cost_amt,
 shrink_units, shrink_retail_currcycd, shrink_retail_amt, shrink_cost_currcycd, shrink_cost_amt, dw_sys_load_tmstp)
(SELECT dcd.snapshot_week_id,
  fct.event_time_UTC,
  fct.event_time_tz,
  fct.plan_published_date,
  fct.selling_country,
  fct.selling_brand,
  fct.category,
  fct.price_band,
  fct.dept_num,
  fct.last_updated_time_in_millis,
  fct.last_updated_time_utc,
  fct.last_updated_time_tz,
  fct.month_num,
  fct.month_label,
  fct.alternate_inventory_model,
  fct.average_inventory_units,
  fct.average_inventory_retail_currcycd,
  fct.average_inventory_retail_amt,
  fct.average_inventory_cost_currcycd,
  fct.average_inventory_cost_amt,
  fct.beginning_of_period_inventory_units,
  fct.beginning_of_period_inventory_retail_currcycd,
  fct.beginning_of_period_inventory_retail_amt,
  fct.beginning_of_period_inventory_cost_currcycd,
  fct.beginning_of_period_inventory_cost_amt,
  fct.return_to_vendor_units,
  fct.return_to_vendor_retail_currcycd,
  fct.return_to_vendor_retail_amt,
  fct.return_to_vendor_cost_currcycd,
  fct.return_to_vendor_cost_amt,
  fct.rack_transfer_units,
  fct.rack_transfer_retail_currcycd,
  fct.rack_transfer_retail_amt,
  fct.rack_transfer_cost_currcycd,
  fct.rack_transfer_cost_amt,
  fct.active_inventory_in_units,
  fct.active_inventory_in_retail_currcycd,
  fct.active_inventory_in_retail_amt,
  fct.active_inventory_in_cost_currcycd,
  fct.active_inventory_in_cost_amt,
  fct.active_inventory_out_units,
  fct.active_inventory_out_retail_currcycd,
  fct.active_inventory_out_retail_amt,
  fct.active_inventory_out_cost_currcycd,
  fct.active_inventory_out_cost_amt,
  fct.receipts_units,
  fct.receipts_retail_currcycd,
  fct.receipts_retail_amt,
  fct.receipts_cost_currcycd,
  fct.receipts_cost_amt,
  fct.receipt_less_reserve_units,
  fct.receipt_less_reserve_retail_currcycd,
  fct.receipt_less_reserve_retail_amt,
  fct.receipt_less_reserve_cost_currcycd,
  fct.receipt_less_reserve_cost_amt,
  fct.pah_transfer_in_units,
  fct.pah_transfer_in_retail_currcycd,
  fct.pah_transfer_in_retail_amt,
  fct.pah_transfer_in_cost_currcycd,
  fct.pah_transfer_in_cost_amt,
  fct.shrink_units,
  fct.shrink_retail_currcycd,
  fct.shrink_retail_amt,
  fct.shrink_cost_currcycd,
  fct.shrink_cost_amt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_assortment_category_country_plan_fact AS fct
  INNER JOIN (SELECT week_idnt AS snapshot_week_id,
    month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
   WHERE day_date < CURRENT_DATE('PST8PDT')
    AND day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1) AS dcd ON TRUE
 WHERE fct.month_num >= dcd.month_idnt);


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_category_country_plan_snapshot_fact AS sf
WHERE snapshot_week_id <= (SELECT retention_week_end_idnt
  FROM (SELECT month_start_day_date,
     month_end_day_date,
     month_end_week_idnt,
     MIN(month_end_week_idnt) OVER (ORDER BY month_idnt ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS
     retention_week_end_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_month_cal_454_vw) AS a
  WHERE CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date);