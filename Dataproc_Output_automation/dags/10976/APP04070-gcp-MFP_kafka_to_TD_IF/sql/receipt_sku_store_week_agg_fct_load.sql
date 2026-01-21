BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=receipt_sku_store_week_agg_fct_load;
---Task_Name=receipt_sku_store_week_agg_fct_load_job_000;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_poreceipt_sku_store_week_fact AS receipt_week_fct
WHERE week_num BETWEEN (SELECT start_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t1
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_PORCPT_DLY')) AND (SELECT end_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t2
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_PORCPT_DLY'));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_poreceipt_sku_store_week_fact
(SELECT week_num,
  rms_sku_num,
  store_num,
  dropship_ind,
  rp_ind,
  CAST(SUM(receipts_active_regular_retail) AS NUMERIC) AS receipts_active_regular_retail,
  SUM(receipts_active_regular_units) AS receipts_active_regular_units,
  CAST(SUM(receipts_active_regular_cost) AS NUMERIC) AS receipts_active_regular_cost,
  CAST(SUM(receipts_inactive_regular_retail) AS NUMERIC) AS receipts_inactive_regular_retail,
  SUM(receipts_inactive_regular_units) AS receipts_inactive_regular_units,
  CAST(SUM(receipts_inactive_regular_cost) AS NUMERIC) AS receipts_inactive_regular_cost,
  CAST(SUM(receipts_active_promo_retail) AS NUMERIC) AS receipts_active_promo_retail,
  SUM(receipts_active_promo_units) AS receipts_active_promo_units,
  CAST(SUM(receipts_active_promo_cost) AS NUMERIC) AS receipts_active_promo_cost,
  CAST(SUM(receipts_inactive_promo_retail) AS NUMERIC) AS receipts_inactive_promo_retail,
  SUM(receipts_inactive_promo_units) AS receipts_inactive_promo_units,
  CAST(SUM(receipts_inactive_promo_cost) AS NUMERIC) AS receipts_inactive_promo_cost,
  CAST(SUM(receipts_active_clearance_retail) AS NUMERIC) AS receipts_active_clearance_retail,
  SUM(receipts_active_clearance_units) AS receipts_active_clearance_units,
  CAST(SUM(receipts_active_clearance_cost) AS NUMERIC) AS receipts_active_clearance_cost,
  CAST(SUM(receipts_inactive_clearance_retail) AS NUMERIC) AS receipts_inactive_clearance_retail,
  SUM(receipts_inactive_clearance_units) AS receipts_inactive_clearance_units,
  CAST(SUM(receipts_inactive_clearance_cost) AS NUMERIC) AS receipts_inactive_clearance_cost,
  CAST(SUM(receipts_crossdock_active_regular_retail) AS NUMERIC) AS receipts_crossdock_active_regular_retail,
  SUM(receipts_crossdock_active_regular_units) AS receipts_crossdock_active_regular_units,
  CAST(SUM(receipts_crossdock_active_regular_cost) AS NUMERIC) AS receipts_crossdock_active_regular_cost,
  CAST(SUM(receipts_crossdock_inactive_regular_retail) AS NUMERIC) AS receipts_crossdock_inactive_regular_retail,
  SUM(receipts_crossdock_inactive_regular_units) AS receipts_crossdock_inactive_regular_units,
  CAST(SUM(receipts_crossdock_inactive_regular_cost) AS NUMERIC) AS receipts_crossdock_inactive_regular_cost,
  CAST(SUM(receipts_crossdock_active_promo_retail) AS NUMERIC) AS receipts_crossdock_active_promo_retail,
  SUM(receipts_crossdock_active_promo_units) AS receipts_crossdock_active_promo_units,
  CAST(SUM(receipts_crossdock_active_promo_cost) AS NUMERIC) AS receipts_crossdock_active_promo_cost,
  CAST(SUM(receipts_crossdock_inactive_promo_retail) AS NUMERIC) AS receipts_crossdock_inactive_promo_retail,
  SUM(receipts_crossdock_inactive_promo_units) AS receipts_crossdock_inactive_promo_units,
  CAST(SUM(receipts_crossdock_inactive_promo_cost) AS NUMERIC) AS receipts_crossdock_inactive_promo_cost,
  CAST(SUM(receipts_crossdock_active_clearance_retail) AS NUMERIC) AS receipts_crossdock_active_clearance_retail,
  SUM(receipts_crossdock_active_clearance_units) AS receipts_crossdock_active_clearance_units,
  CAST(SUM(receipts_crossdock_active_clearance_cost) AS NUMERIC) AS receipts_crossdock_active_clearance_cost,
  CAST(SUM(receipts_crossdock_inactive_clearance_retail) AS NUMERIC) AS receipts_crossdock_inactive_clearance_retail,
  SUM(receipts_crossdock_inactive_clearance_units) AS receipts_crossdock_inactive_clearance_units,
  CAST(SUM(receipts_crossdock_inactive_clearance_cost) AS NUMERIC) AS receipts_crossdock_inactive_clearance_cost,
  receipts_cost_currency_code,
  receipts_retail_currency_code,
  receipts_crossdock_cost_currency_code,
  receipts_crossdock_retail_currency_code,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_poreceipt_sku_store_day_columnar_vw AS receipt_col_vw
 WHERE week_num BETWEEN (SELECT start_rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t1
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_PORCPT_DLY')) AND (SELECT end_rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t2
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_PORCPT_DLY'))
 GROUP BY rms_sku_num,
  store_num,
  dropship_ind,
  week_num,
  rp_ind,
  receipts_cost_currency_code,
  receipts_retail_currency_code,
  receipts_crossdock_cost_currency_code,
  receipts_crossdock_retail_currency_code);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

END;
