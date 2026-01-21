-- Setting query band, currently not applicable in BigQuery
-- Simulation of metadata tracking for app context

-- DELETE Query


DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk dmnd
WHERE dmnd.day_num IN (
  SELECT dcal.day_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim cal
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup etl
    ON cal.day_date = etl.dw_batch_dt
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim dcal
    ON dcal.week_idnt = cal.week_idnt
  WHERE LOWER(etl.interface_code) = LOWER('MRIC_DAY_AGG_DLY')
)
AND LOWER(dmnd.src_ind) = LOWER('DMND');

-- INSERT Query for current year data
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk (
  rms_sku_num,
  store_num,
  day_num,
  rp_ind,
  jwn_demand_total_units_ty,
  jwn_demand_total_retail_amt_ty,
  jwn_demand_regular_units_ty,
  jwn_demand_regular_retail_amt_ty,
  jwn_demand_promo_units_ty,
  jwn_demand_promo_retail_amt_ty,
  jwn_demand_clearance_units_ty,
  jwn_demand_clearance_retail_amt_ty,
  jwn_demand_persistent_units_ty,
  jwn_demand_persistent_retail_amt_ty,
  jwn_demand_persistent_regular_units_ty,
  jwn_demand_persistent_regular_retail_amt_ty,
  jwn_demand_persistent_promo_units_ty,
  jwn_demand_persistent_promo_retail_amt_ty,
  jwn_demand_persistent_clearance_units_ty,
  jwn_demand_persistent_clearance_retail_amt_ty,
  jwn_demand_flash_units_ty,
  jwn_demand_flash_retail_amt_ty,
  jwn_demand_flash_regular_units_ty,
  jwn_demand_flash_regular_retail_amt_ty,
  jwn_demand_flash_promo_units_ty,
  jwn_demand_flash_promo_retail_amt_ty,
  jwn_demand_flash_clearance_units_ty,
  jwn_demand_flash_clearance_retail_amt_ty,
  src_ind,
  dw_sys_load_tmstp,
  dw_sys_load_dt
)
SELECT 
  dmnd.rms_sku_num,
  dmnd.store_num,
  dmnd.day_num,
  dmnd.rp_ind,
  dmnd.jwn_demand_total_units,
  dmnd.jwn_demand_total_retail_amt,
  dmnd.jwn_demand_regular_units,
  dmnd.jwn_demand_regular_retail_amt,
  dmnd.jwn_demand_promo_units,
  dmnd.jwn_demand_promo_retail_amt,
  dmnd.jwn_demand_clearance_units,
  dmnd.jwn_demand_clearance_retail_amt,
  dmnd.jwn_demand_persistent_units,
  dmnd.jwn_demand_persistent_retail_amt,
  dmnd.jwn_demand_persistent_regular_units,
  dmnd.jwn_demand_persistent_regular_retail_amt,
  dmnd.jwn_demand_persistent_promo_units,
  dmnd.jwn_demand_persistent_promo_retail_amt,
  dmnd.jwn_demand_persistent_clearance_units,
  dmnd.jwn_demand_persistent_clearance_retail_amt,
  dmnd.jwn_demand_flash_units,
  dmnd.jwn_demand_flash_retail_amt,
  dmnd.jwn_demand_flash_regular_units,
  dmnd.jwn_demand_flash_regular_retail_amt,
  dmnd.jwn_demand_flash_promo_units,
  dmnd.jwn_demand_flash_promo_retail_amt,
  dmnd.jwn_demand_flash_clearance_units,
  dmnd.jwn_demand_flash_clearance_retail_amt,
  'DMND',
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CURRENT_DATE('PST8PDT') AS dw_sys_load_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_jwn_demand_sku_store_day_fact_vw dmnd
WHERE dmnd.day_num IN (
  SELECT dcal.day_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim cal
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup etl
    ON cal.day_date = etl.dw_batch_dt
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim dcal
    ON dcal.week_idnt = cal.week_idnt
  WHERE LOWER(etl.interface_code) = LOWER('MRIC_DAY_AGG_DLY')
);

-- INSERT Query for last year data
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk (
  rms_sku_num,
  store_num,
  day_num,
  rp_ind,
  jwn_demand_total_units_ly,
  jwn_demand_total_retail_amt_ly,
  jwn_demand_regular_units_ly,
  jwn_demand_regular_retail_amt_ly,
  jwn_demand_promo_units_ly,
  jwn_demand_promo_retail_amt_ly,
  jwn_demand_clearance_units_ly,
  jwn_demand_clearance_retail_amt_ly,
  jwn_demand_persistent_units_ly,
  jwn_demand_persistent_retail_amt_ly,
  jwn_demand_persistent_regular_units_ly,
  jwn_demand_persistent_regular_retail_amt_ly,
  jwn_demand_persistent_promo_units_ly,
  jwn_demand_persistent_promo_retail_amt_ly,
  jwn_demand_persistent_clearance_units_ly,
  jwn_demand_persistent_clearance_retail_amt_ly,
  jwn_demand_flash_units_ly,
  jwn_demand_flash_retail_amt_ly,
  jwn_demand_flash_regular_units_ly,
  jwn_demand_flash_regular_retail_amt_ly,
  jwn_demand_flash_promo_units_ly,
  jwn_demand_flash_promo_retail_amt_ly,
  jwn_demand_flash_clearance_units_ly,
  jwn_demand_flash_clearance_retail_amt_ly,
  src_ind,
  dw_sys_load_tmstp,
  dw_sys_load_dt
)
SELECT 
  dmnd.rms_sku_num,
  dmnd.store_num,
  cal.day_idnt,
  dmnd.rp_ind,
  dmnd.jwn_demand_total_units,
  dmnd.jwn_demand_total_retail_amt,
  dmnd.jwn_demand_regular_units,
  dmnd.jwn_demand_regular_retail_amt,
  dmnd.jwn_demand_promo_units,
  dmnd.jwn_demand_promo_retail_amt,
  dmnd.jwn_demand_clearance_units,
  dmnd.jwn_demand_clearance_retail_amt,
  dmnd.jwn_demand_persistent_units,
  dmnd.jwn_demand_persistent_retail_amt,
  dmnd.jwn_demand_persistent_regular_units,
  dmnd.jwn_demand_persistent_regular_retail_amt,
  dmnd.jwn_demand_persistent_promo_units,
  dmnd.jwn_demand_persistent_promo_retail_amt,
  dmnd.jwn_demand_persistent_clearance_units,
  dmnd.jwn_demand_persistent_clearance_retail_amt,
  dmnd.jwn_demand_flash_units,
  dmnd.jwn_demand_flash_retail_amt,
  dmnd.jwn_demand_flash_regular_units,
  dmnd.jwn_demand_flash_regular_retail_amt,
  dmnd.jwn_demand_flash_promo_units,
  dmnd.jwn_demand_flash_promo_retail_amt,
  dmnd.jwn_demand_flash_clearance_units,
  dmnd.jwn_demand_flash_clearance_retail_amt,
  'DMND',
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CURRENT_DATE('PST8PDT') AS dw_sys_load_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_jwn_demand_sku_store_day_fact_vw dmnd
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim cal
  ON cal.day_idnt_last_year_realigned = dmnd.day_num
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim dcal
  ON dcal.week_idnt = cal.week_idnt
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup etl
  ON dcal.day_date = etl.dw_batch_dt
WHERE LOWER(etl.interface_code) = LOWER('MRIC_DAY_AGG_DLY');
