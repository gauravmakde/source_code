BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_order_cancel_17393_customer_das_customer;
---Task_Name=order_cancel_fact_load;'*/
BEGIN
SET _ERROR_CODE  =  0;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_line_cancel_fact AS trg
USING (SELECT   order_num,
  order_line_id,
  order_line_num,
  order_tmstp_utc,
order_tmstp_tz,
  order_tmstp_pacific,
  order_date_pacific,
  NULL AS acp_id,
  NULL AS acp_id_updt_date,
  ent_cust_id,
  shopper_id,
  order_line_amount,
  order_line_amount_usd,
  order_line_quantity,
  CASE
    WHEN COALESCE(order_line_employee_discount_amount_usd, 0) = 0 THEN 'N'
    ELSE 'Y'
END
  AS employee_discount_flag,
  requested_max_promise_tmstp_utc,
  requested_max_promise_tmstp_tz,
  requested_max_promise_tmstp_pacific,
  requested_max_promise_date_pacific,
  promise_type_code,
  upc_code,
  destination_node_num,
  destination_city,
  destination_state,
  destination_zip_code,
  destination_country_code,
  delivery_method_code,
  requested_level_of_service_code,
  backorder_display_ind,
  source_channel_country_code,
  source_channel_code,
  source_platform_code,
  source_store_num,
  canceled_tmstp_utc,
canceled_tmstp_tz,
  canceled_tmstp_pacific,
  canceled_date_pacific,
  cancel_reason_code,
(
  SELECT
    curr_batch_date
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE
    LOWER(RTRIM(subject_area_nm)) = LOWER(RTRIM('ORDER_CANCEL'))) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_detail_fact
WHERE
  dw_batch_date >= (
  SELECT
    DATE_SUB(curr_batch_date, INTERVAL 1 DAY)
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE
    LOWER(RTRIM(subject_area_nm)) = LOWER(RTRIM('ORDER_CANCEL')))
  AND cancel_reason_code IS NOT NULL) AS src

ON LOWER(RTRIM(src.order_num)) = LOWER(RTRIM(trg.order_num)) AND LOWER(RTRIM(src.order_line_id)) = LOWER(RTRIM(trg.order_line_id)) AND src.order_date_pacific = trg.order_date_pacific

WHEN MATCHED THEN UPDATE SET
    order_line_num = src.order_line_num,
    order_tmstp = src.order_tmstp_utc,
    order_tmstp_tz = src.order_tmstp_tz,
    order_tmstp_pacific = src.order_tmstp_pacific,
    ent_cust_id = src.ent_cust_id,
    shopper_id = src.shopper_id,
    order_line_amount = src.order_line_amount,
    order_line_amount_usd = src.order_line_amount_usd,
    order_line_quantity = src.order_line_quantity,
    employee_discount_flag = src.employee_discount_flag,
    requested_max_promise_tmstp = src.requested_max_promise_tmstp_utc,
    requested_max_promise_tmstp_tz = src.requested_max_promise_tmstp_tz,
    requested_max_promise_tmstp_pacific = src.requested_max_promise_tmstp_pacific,
    requested_max_promise_date_pacific = src.requested_max_promise_date_pacific,
    promise_type_code = src.promise_type_code,
    upc_code = src.upc_code,
    destination_node_num = src.destination_node_num,
    destination_city = src.destination_city,
    destination_state = src.destination_state,
    destination_zip_code = src.destination_zip_code,
    destination_country_code = src.destination_country_code,
    delivery_method_code = src.delivery_method_code,
    requested_level_of_service_code = src.requested_level_of_service_code,
    backorder_display_ind = src.backorder_display_ind,
    source_channel_country_code = src.source_channel_country_code,
    source_channel_code = src.source_channel_code,
    source_platform_code = src.source_platform_code,
    source_store_num = src.source_store_num,
    canceled_tmstp = src.canceled_tmstp_utc,
    canceled_tmstp_tz = src.canceled_tmstp_tz,
    canceled_tmstp_pacific = src.canceled_tmstp_pacific,
    canceled_date_pacific = src.canceled_date_pacific,
    cancel_reason_code = src.cancel_reason_code,
    dw_batch_date = src.dw_batch_date,
    dw_sys_updt_tmstp = src.dw_sys_updt_tmstp
WHEN NOT MATCHED THEN INSERT VALUES(  src.order_num,
  src.order_line_id,
  src.order_line_num,
  src.order_tmstp_utc,
  src.order_tmstp_tz,
  src.order_tmstp_pacific,
  src.order_date_pacific,
  CAST(src.acp_id AS STRING),
  CAST(CAST(src.acp_id_updt_date AS STRING) AS DATE),
  src.ent_cust_id,
  src.shopper_id,
  src.order_line_amount,
  src.order_line_amount_usd,
  src.order_line_quantity,
  src.employee_discount_flag,
  src.requested_max_promise_tmstp_utc,
  src.requested_max_promise_tmstp_tz,
  src.requested_max_promise_tmstp_pacific,
  src.requested_max_promise_date_pacific,
  src.promise_type_code,
  src.upc_code,
  src.destination_node_num,
  src.destination_city,
  src.destination_state,
  src.destination_zip_code,
  src.destination_country_code,
  src.delivery_method_code,
  src.requested_level_of_service_code,
  src.backorder_display_ind,
  src.source_channel_country_code,
  src.source_channel_code,
  src.source_platform_code,
  src.source_store_num,
  src.canceled_tmstp_utc,
  src.canceled_tmstp_tz,
  src.canceled_tmstp_pacific,
  src.canceled_date_pacific,
  src.cancel_reason_code,
  src.dw_batch_date,
  src.dw_sys_load_tmstp,
  src.dw_sys_updt_tmstp);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
