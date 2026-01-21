BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=demand_fact_load_10976_tech_nap_merch;
---Task_Name=DMND_LD_t4_ord_dlta_to_prev;'*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- BEGIN
-- SET _ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_detail_prev_fct AS prev
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_daily_delta_fct AS fct
    WHERE LOWER(prev.order_num) = LOWER(order_num) AND LOWER(prev.order_line_id) = LOWER(order_line_id) AND prev.order_date_pacific BETWEEN (SELECT MIN(order_date_pacific)
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_daily_delta_fct) AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 14 DAY)));


-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;
-- END;
-- BEGIN
-- SET _ERROR_CODE  =  0;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_detail_prev_fct (order_num, order_line_id, order_line_num, order_tmstp_pacific,
 order_date_pacific, order_type_code, rms_sku_num, order_line_amount, order_line_quantity, promise_type_code,
 destination_node_num, backorder_ind, source_platform_code, source_channel_country_code, source_channel_code,
 source_store_num, canceled_date_pacific, cancel_reason_code, shipped_date_pacific, returned_without_pickup_date_pacific
 , shipped_to_customer_date_pacific, picked_up_by_customer_date_pacific, picked_up_by_customer_node_num,
 first_released_date_pacific, first_released_node_type_code, last_released_node_type_code, fulfilled_node_type_code,
 pre_release_cancel_ind, fraud_cancel_ind, dw_batch_id, dw_batch_date, dw_sys_load_tmstp, dw_sys_updt_tmstp,
 order_line_promotion_id_list, partner_relationship_id, partner_relationship_type_code)
(SELECT order_num,
  order_line_id,
  order_line_num,
  order_tmstp_pacific,
  order_date_pacific,
  order_type_code,
  rms_sku_num,
  order_line_amount,
  order_line_quantity,
  promise_type_code,
  destination_node_num,
  backorder_ind,
  source_platform_code,
  source_channel_country_code,
  source_channel_code,
  source_store_num,
  canceled_date_pacific,
  cancel_reason_code,
  shipped_date_pacific,
  returned_without_pickup_date_pacific,
  shipped_to_customer_date_pacific,
  picked_up_by_customer_date_pacific,
  picked_up_by_customer_node_num,
  first_released_date_pacific,
  first_released_node_type_code,
  last_released_node_type_code,
  fulfilled_node_type_code,
  pre_release_cancel_ind,
  fraud_cancel_ind,
  dw_batch_id,
  dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  order_line_promotion_id_list,
  partner_relationship_id,
  partner_relationship_type_code
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_daily_delta_fct);


-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;
-- END;
-- BEGIN
-- SET _ERROR_CODE  =  0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup SET
    config_value = (SELECT SUBSTR(CAST(MAX(dw_batch_id) AS STRING), 1, 32) AS dw_batch_id
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_daily_delta_fct)
WHERE LOWER(interface_code) = LOWER('NAP_DEMAND') AND LOWER(config_key) = LOWER('LAST_BATCH_ID');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
-- END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
