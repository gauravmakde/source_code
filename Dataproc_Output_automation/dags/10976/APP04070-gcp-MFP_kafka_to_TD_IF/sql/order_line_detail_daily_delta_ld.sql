
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=demand_fact_load_10976_tech_nap_merch;
Task_Name=DMND_LD_t1_ord_fct_to_ord_dlta;'*/
BEGIN TRANSACTION;

BEGIN
SET ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_daily_delta_fct;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_daily_delta_fct (order_num, order_line_id, order_line_num, order_tmstp_pacific,
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
   CASE
   WHEN LOWER(COALESCE(backorder_ind, '')) = LOWER('')
   THEN 'N'
   ELSE backorder_ind
   END AS backorder_ind,
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
  COALESCE(first_released_date_pacific, last_released_date_pacific) AS
  a_last_released_date_pacific_a_first_released_date_pacific,
  COALESCE(first_released_node_type_code, last_released_node_type_code) AS
  a_last_released_node_type_code_a_first_released_node_type_code,
  last_released_node_type_code,
  fulfilled_node_type_code,
   CASE
   WHEN LOWER(COALESCE(pre_release_cancel_ind, '')) = LOWER('')
   THEN 'N'
   ELSE pre_release_cancel_ind
   END AS pre_release_cancel_ind,
   CASE
   WHEN LOWER(COALESCE(fraud_cancel_ind, '')) = LOWER('')
   THEN 'N'
   ELSE fraud_cancel_ind
   END AS fraud_cancel_ind,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp,
  dw_sys_updt_tmstp,
  order_line_promotion_id_list,
  partner_relationship_id,
  partner_relationship_type_code
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_detail_fact AS a
 WHERE COALESCE(dw_batch_id, 0) BETWEEN (SELECT CAST(TRUNC(CAST(CASE
       WHEN config_value = ''
       THEN '0'
       ELSE config_value
       END AS FLOAT64)) AS INTEGER) AS config_value
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
    WHERE LOWER(interface_code) = LOWER('NAP_DEMAND')
     AND LOWER(config_key) = LOWER('LAST_BATCH_ID')) AND (SELECT MAX(batch_id)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_info
    WHERE LOWER(interface_code) = LOWER('ORDER_LINE_DETAIL_FACT')
     AND LOWER(status_code) = LOWER('FINISHED'))
  AND order_date_pacific <> DATE '4444-04-04'
  AND order_line_amount IS NOT NULL);

  
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
END;
