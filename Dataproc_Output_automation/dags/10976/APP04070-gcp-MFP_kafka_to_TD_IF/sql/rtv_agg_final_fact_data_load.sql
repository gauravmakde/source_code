BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=rtv_week_agg_fact_load;
---Task_Name=rtv_agg_fact_load_job_2;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_return_to_vendor_sku_loc_week_agg_fact AS rtv_agg_vw
WHERE week_num BETWEEN (SELECT start_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RTV_DLY')) 
      AND (SELECT end_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw2
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RTV_DLY'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_return_to_vendor_sku_loc_week_agg_fact (rms_sku_num, store_num, week_num, rp_ind,
 rtv_cost_currency_code, rtv_retail_currency_code, rtv_active_cost, rtv_active_units, rtv_active_retail,
 rtv_inactive_cost, rtv_inactive_units, rtv_inactive_retail, rtv_total_cost, rtv_total_units, rtv_total_retail,
 dw_sys_load_tmstp, dw_batch_date)
(SELECT rms_sku_num,
  store_num,
  week_num,
  rp_ind,
  rtv_cost_currency_code,
  rtv_retail_currency_code,
  CAST(SUM(rtv_active_cost) AS NUMERIC) AS rtv_active_cost,
  SUM(rtv_active_units) AS rtv_active_units,
  CAST(SUM(rtv_active_retail) AS NUMERIC) AS rtv_active_retail,
  CAST(SUM(rtv_inactive_cost) AS NUMERIC) AS rtv_inactive_cost,
  SUM(rtv_inactive_units) AS rtv_inactive_units,
  CAST(SUM(rtv_inactive_retail) AS NUMERIC) AS rtv_inactive_retail,
  ROUND(CAST(SUM(rtv_total_cost) AS NUMERIC), 4) AS rtv_total_cost,
  SUM(rtv_total_units) AS rtv_total_units,
  ROUND(CAST(SUM(rtv_total_retail) AS NUMERIC), 4) AS rtv_total_retail,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CURRENT_DATE('PST8PDT') AS dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_return_to_vendor_columnar_vw AS rtv_col_vw
 WHERE week_num BETWEEN (SELECT start_rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RTV_DLY')) AND (SELECT end_rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw2
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RTV_DLY'))
 GROUP BY rms_sku_num,
  store_num,
  week_num,
  rtv_cost_currency_code,
  rtv_retail_currency_code,
  rp_ind);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
