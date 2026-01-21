BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=disc_terms_agg_fact_final_data_load_10976_tech_nap_merch;
---Task_Name=disc_terms_agg_fact_data_load_job_00;'*/
-- COMMIT TRANSACTION;

-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- BEGIN
-- SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_disc_terms_sku_loc_week_agg_fact AS disc_agg_vw
WHERE week_num BETWEEN (SELECT start_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_DISC_DLY')) AND (SELECT end_rebuild_week_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw2
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_DISC_DLY'));


-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;
-- END;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_disc_terms_sku_loc_week_agg_fact
(SELECT rms_sku_num,
  store_num,
  week_num,
  disc_terms_cost_currency_code,
  disc_terms_retail_currency_code,
  ROUND(CAST(SUM(disc_terms_cost) AS NUMERIC), 4) AS disc_terms_cost,
  CAST(SUM(disc_terms_regular_cost) AS NUMERIC) AS disc_terms_regular_cost,
  CAST(SUM(disc_terms_promotion_cost) AS NUMERIC) AS disc_terms_promotion_cost,
  CAST(SUM(disc_terms_clearance_cost) AS NUMERIC) AS disc_terms_clearance_cost,
  SUM(disc_terms_units) AS disc_terms_units,
  SUM(disc_terms_regular_units) AS disc_terms_regular_units,
  SUM(disc_terms_promotion_units) AS disc_terms_promotion_units,
  SUM(disc_terms_clearance_units) AS disc_terms_clearance_units,
  ROUND(CAST(SUM(disc_terms_retail) AS NUMERIC), 4) AS disc_terms_retail,
  CAST(SUM(disc_terms_regular_retail) AS NUMERIC) AS disc_terms_regular_retail,
  CAST(SUM(disc_terms_promotion_retail) AS NUMERIC) AS disc_terms_promotion_retail,
  CAST(SUM(disc_terms_clearance_retail) AS NUMERIC) AS disc_terms_clearance_retail,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CURRENT_DATE('PST8PDT') AS dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_disc_terms_columnar_vw AS disc_col_vw
 WHERE week_num BETWEEN (SELECT start_rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw1
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_DISC_DLY')) AND (SELECT end_rebuild_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS tran_vw2
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_DISC_DLY'))
 GROUP BY rms_sku_num,
  store_num,
  week_num,
  disc_terms_cost_currency_code,
  disc_terms_retail_currency_code);


-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
