-- DELETE operation


DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk AS sls
WHERE sls.day_num IN (
  SELECT dcal.day_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
    ON cal.day_date = etl.dw_batch_dt
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcal
    ON dcal.week_idnt = cal.week_idnt
  WHERE LOWER(etl.interface_code) = LOWER('MRIc_DAY_AGG_DLY')
)
AND LOWER(sls.src_ind) = LOWER('SLS');

-- INSERT operation (current year)
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk
(
  rms_sku_num, store_num, day_num, rp_ind, 
  jwn_operational_gmv_total_units_ty, jwn_operational_gmv_total_retail_amt_ty, jwn_operational_gmv_total_cost_amt_ty,
  jwn_operational_gmv_regular_units_ty, jwn_operational_gmv_regular_retail_amt_ty, jwn_operational_gmv_regular_cost_amt_ty,
  jwn_operational_gmv_promo_units_ty, jwn_operational_gmv_promo_retail_amt_ty, jwn_operational_gmv_promo_cost_amt_ty,
  jwn_operational_gmv_clearance_units_ty, jwn_operational_gmv_clearance_retail_amt_ty, jwn_operational_gmv_clearance_cost_amt_ty,
  jwn_returns_total_units_ty, jwn_returns_total_retail_amt_ty, jwn_returns_total_cost_amt_ty,
  jwn_returns_regular_units_ty, jwn_returns_regular_retail_amt_ty, jwn_returns_regular_cost_amt_ty,
  jwn_returns_promo_units_ty, jwn_returns_promo_retail_amt_ty, jwn_returns_promo_cost_amt_ty,
  jwn_returns_clearance_units_ty, jwn_returns_clearance_retail_amt_ty, jwn_returns_clearance_cost_amt_ty,
  src_ind, dw_sys_load_tmstp, dw_sys_load_dt
)
SELECT
  sls.rms_sku_num, sls.store_num, sls.day_num, sls.rp_ind,
  sls.jwn_operational_gmv_total_units, sls.jwn_operational_gmv_total_retail_amt, sls.jwn_operational_gmv_total_cost_amt,
  sls.jwn_operational_gmv_regular_units, sls.jwn_operational_gmv_regular_retail_amt, sls.jwn_operational_gmv_regular_cost_amt,
  sls.jwn_operational_gmv_promo_units, sls.jwn_operational_gmv_promo_retail_amt, sls.jwn_operational_gmv_promo_cost_amt,
  sls.jwn_operational_gmv_clearance_units, sls.jwn_operational_gmv_clearance_retail_amt, sls.jwn_operational_gmv_clearance_cost_amt,
  sls.jwn_returns_total_units, sls.jwn_returns_total_retail_amt, sls.jwn_returns_total_cost_amt,
  sls.jwn_returns_regular_units, sls.jwn_returns_regular_retail_amt, sls.jwn_returns_regular_cost_amt,
  sls.jwn_returns_promo_units, sls.jwn_returns_promo_retail_amt, sls.jwn_returns_promo_cost_amt,
  sls.jwn_returns_clearance_units, sls.jwn_returns_clearance_retail_amt, sls.jwn_returns_clearance_cost_amt,
  'SLS', 
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CURRENT_DATE('PST8PDT')
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_jwn_sale_return_sku_store_day_fact_vw AS sls
WHERE sls.day_num IN (
  SELECT dcal.day_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
    ON cal.day_date = etl.dw_batch_dt
  JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcal
    ON dcal.week_idnt = cal.week_idnt
  WHERE LOWER(etl.interface_code) = LOWER('MRIc_DAY_AGG_DLY')
);

-- INSERT operation (last year realigned)
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk
(
  rms_sku_num, store_num, day_num, rp_ind,
  jwn_operational_gmv_total_units_ly, jwn_operational_gmv_total_retail_amt_ly, jwn_operational_gmv_total_cost_amt_ly,
  jwn_operational_gmv_regular_units_ly, jwn_operational_gmv_regular_retail_amt_ly, jwn_operational_gmv_regular_cost_amt_ly,
  jwn_operational_gmv_promo_units_ly, jwn_operational_gmv_promo_retail_amt_ly, jwn_operational_gmv_promo_cost_amt_ly,
  jwn_operational_gmv_clearance_units_ly, jwn_operational_gmv_clearance_retail_amt_ly, jwn_operational_gmv_clearance_cost_amt_ly,
  jwn_returns_total_units_ly, jwn_returns_total_retail_amt_ly, jwn_returns_total_cost_amt_ly,
  jwn_returns_regular_units_ly, jwn_returns_regular_retail_amt_ly, jwn_returns_regular_cost_amt_ly,
  jwn_returns_promo_units_ly, jwn_returns_promo_retail_amt_ly, jwn_returns_promo_cost_amt_ly,
  jwn_returns_clearance_units_ly, jwn_returns_clearance_retail_amt_ly, jwn_returns_clearance_cost_amt_ly,
  src_ind, dw_sys_load_tmstp, dw_sys_load_dt
)
SELECT
  sls.rms_sku_num, sls.store_num, cal.day_idnt, sls.rp_ind,
  sls.jwn_operational_gmv_total_units, sls.jwn_operational_gmv_total_retail_amt, sls.jwn_operational_gmv_total_cost_amt,
  sls.jwn_operational_gmv_regular_units, sls.jwn_operational_gmv_regular_retail_amt, sls.jwn_operational_gmv_regular_cost_amt,
  sls.jwn_operational_gmv_promo_units, sls.jwn_operational_gmv_promo_retail_amt, sls.jwn_operational_gmv_promo_cost_amt,
  sls.jwn_operational_gmv_clearance_units, sls.jwn_operational_gmv_clearance_retail_amt, sls.jwn_operational_gmv_clearance_cost_amt,
  sls.jwn_returns_total_units, sls.jwn_returns_total_retail_amt, sls.jwn_returns_total_cost_amt,
  sls.jwn_returns_regular_units, sls.jwn_returns_regular_retail_amt, sls.jwn_returns_regular_cost_amt,
  sls.jwn_returns_promo_units, sls.jwn_returns_promo_retail_amt, sls.jwn_returns_promo_cost_amt,
  sls.jwn_returns_clearance_units, sls.jwn_returns_clearance_retail_amt, sls.jwn_returns_clearance_cost_amt,
  'SLS', 
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CURRENT_DATE('PST8PDT')
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_jwn_sale_return_sku_store_day_fact_vw AS sls
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal
  ON cal.day_idnt_last_year_realigned = sls.day_num
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS dcal
  ON dcal.week_idnt = cal.week_idnt
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
  ON dcal.day_date = etl.dw_batch_dt
WHERE LOWER(etl.interface_code) = LOWER('MRIc_DAY_AGG_DLY');
