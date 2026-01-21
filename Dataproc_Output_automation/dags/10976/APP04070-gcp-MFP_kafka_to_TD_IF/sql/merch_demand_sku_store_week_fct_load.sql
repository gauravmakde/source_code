BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=demand_apt_week_fact_load_10976_tech_nap_merch;
---Task_Name=demand_apt_data_load_job_0;'*/
-- COMMIT TRANSACTION;

-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.demand_sku_store_week_agg_fact AS demand_agg_vw
WHERE week_num BETWEEN (SELECT start_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t1
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_DEMAND_DLY')) AND (SELECT end_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t2
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_DEMAND_DLY'));


-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.demand_sku_store_week_agg_fact
(SELECT rms_sku_num,
  store_num,
  week_num,
  fulfill_type_code,
  SUM(demand_tot_amt) AS demand_tot_amt,
  CAST(SUM(demand_regular_amt) AS NUMERIC) AS demand_regular_amt,
  CAST(SUM(demand_promo_amt) AS NUMERIC) AS demand_promo_amt,
  CAST(SUM(demand_clearence_amt) AS NUMERIC) AS demand_clearence_amt,
  CAST(SUM(demand_unknown_amt) AS NUMERIC) AS demand_unknown_amt,
  SUM(demand_tot_qty) AS demand_tot_qty,
  SUM(demand_regular_qty) AS demand_regular_qty,
  SUM(demand_promo_qty) AS demand_promo_qty,
  SUM(demand_clearence_qty) AS demand_clearence_qty,
  SUM(demand_unknown_qty) AS demand_unknown_qty,
  CAST(SUM(demand_tot_flash_amt) AS NUMERIC) AS demand_tot_flash_amt,
  CAST(SUM(demand_flash_regular_amt) AS NUMERIC) AS demand_flash_regular_amt,
  CAST(SUM(demand_flash_promo_amt) AS NUMERIC) AS demand_flash_promo_amt,
  CAST(SUM(demand_flash_clearence_amt) AS NUMERIC) AS demand_flash_clearence_amt,
  CAST(SUM(demand_flash_unknown_amt) AS NUMERIC) AS demand_flash_unknown_amt,
  SUM(demand_tot_flash_qty) AS demand_tot_flash_qty,
  SUM(demand_flash_regular_qty) AS demand_flash_regular_qty,
  SUM(demand_flash_promo_qty) AS demand_flash_promo_qty,
  SUM(demand_flash_clearence_qty) AS demand_flash_clearence_qty,
  SUM(demand_flash_unknown_qty) AS demand_flash_unknown_qty,
  CAST(SUM(demand_tot_persistent_amt) AS NUMERIC) AS demand_tot_persistent_amt,
  CAST(SUM(demand_persistent_regular_amt) AS NUMERIC) AS demand_persistent_regular_amt,
  CAST(SUM(demand_persistent_promo_amt) AS NUMERIC) AS demand_persistent_promo_amt,
  CAST(SUM(demand_persistent_clearence_amt) AS NUMERIC) AS demand_persistent_clearence_amt,
  CAST(SUM(demand_persistent_unknown_amt) AS NUMERIC) AS demand_persistent_unknown_amt,
  SUM(demand_tot_persistent_qty) AS demand_tot_persistent_qty,
  SUM(demand_persistent_regular_qty) AS demand_persistent_regular_qty,
  SUM(demand_persistent_promo_qty) AS demand_persistent_promo_qty,
  SUM(demand_persistent_clearence_qty) AS demand_persistent_clearence_qty,
  SUM(demand_persistent_unknown_qty) AS demand_persistent_unknown_qty,
  MAX(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)) AS dw_sys_load_tmstp,
  MAX(CURRENT_DATE('PST8PDT')) AS dw_sys_load_dt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_demand_sku_store_day_columnar_vw
 WHERE demand_date BETWEEN (SELECT start_rebuild_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t1
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_DEMAND_DLY')) AND (SELECT end_rebuild_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS t1
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_DEMAND_DLY'))
 GROUP BY rms_sku_num,
  store_num,
  week_num,
  fulfill_type_code);



EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- -- COMMIT TRANSACTION;

-- RAISE USING MESSAGE = @@error.message;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/

-- RAISE USING MESSAGE = @@error.message;
END;
