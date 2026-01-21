
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_vendor_funds_sku_loc_week_agg_fact AS vfmd_agg_vw
WHERE vfmd_agg_vw.WEEK_NUM BETWEEN
  (SELECT START_REBUILD_WEEK_NUM 
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
   WHERE LOWER(tran_vw1.INTERFACE_CODE) = LOWER('MERCH_NAP_VFMD_DLY'))
  AND 
  (SELECT END_REBUILD_WEEK_NUM 
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw2
   WHERE LOWER(tran_vw2.INTERFACE_CODE) = LOWER('MERCH_NAP_VFMD_DLY'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_vendor_funds_sku_loc_week_agg_fact
SELECT 
  week_num,
  rms_sku_num,
  store_num,
  ROUND(CAST(SUM(vendor_funds_cost) AS NUMERIC), 4) AS vendor_funds_cost,
  vendor_funds_cost_currency_code,
  CAST(SUM(vendor_funds_regular_cost) AS NUMERIC) AS vendor_funds_regular_cost,
  CAST(SUM(vendor_funds_promotion_cost) AS NUMERIC) AS vendor_funds_promotion_cost,
  CAST(SUM(vendor_funds_clearance_cost) AS NUMERIC) AS vendor_funds_clearance_cost,
  SUM(vendor_funds_units) AS vendor_funds_units,
  SUM(vendor_funds_regular_units) AS vendor_funds_regular_units,
  SUM(vendor_funds_promotion_units) AS vendor_funds_promotion_units,
  SUM(vendor_funds_clearance_units) AS vendor_funds_clearance_units,
  ROUND(CAST(SUM(vendor_funds_retail) AS NUMERIC), 4) AS vendor_funds_retail,
  CAST(SUM(vendor_funds_regular_retail) AS NUMERIC) AS vendor_funds_regular_retail,
  CAST(SUM(vendor_funds_promotion_retail) AS NUMERIC) AS vendor_funds_promotion_retail,
  CAST(SUM(vendor_funds_clearance_retail) AS NUMERIC) AS vendor_funds_clearance_retail,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CURRENT_DATE('PST8PDT') AS dw_batch_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_vendor_funds_columnar_vw AS vfmd_col_vw
WHERE vfmd_col_vw.WEEK_NUM BETWEEN
  (SELECT START_REBUILD_WEEK_NUM 
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1 
   WHERE LOWER(tran_vw1.INTERFACE_CODE) = LOWER('MERCH_NAP_VFMD_DLY'))
  AND 
  (SELECT END_REBUILD_WEEK_NUM 
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw2 
   WHERE LOWER(tran_vw2.INTERFACE_CODE) = LOWER('MERCH_NAP_VFMD_DLY'))
GROUP BY 
  week_num,
  rms_sku_num,
  store_num,
  vendor_funds_cost_currency_code;
