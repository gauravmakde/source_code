
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=demand_fact_load_10976_tech_nap_merch;
---Task_Name=DMND_LD_t2_ord_dlta_to_dmnd_dlta;'*/
BEGIN TRANSACTION;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_sku_store_daily_fct;


BEGIN
SET ERROR_CODE  =  0;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.order_sku_store_daily_fct (order_date_pacific, activity_date_pacific, rms_sku_num, store_num,
 source_platform_code, order_source_code, fulfill_type_code, ordered_amt, ordered_qty, returned_amt, returned_qty,
 shipped_amt, shipped_qty, same_day_canceled_amt, same_day_canceled_qty, fraud_canceled_amt, fraud_canceled_qty,
 canceled_total_amt, canceled_total_qty, dw_batch_date, dw_sys_load_tmstp, order_line_promotion_id_list)
(SELECT order_date_pacific,
  activity_date_pacific,
  rms_sku_num,
  CAST(TRUNC(CAST(store_num AS FLOAT64)) AS INTEGER) AS store_num,
  source_platform_code,
  order_source_code,
  fulfill_type_code,
  CAST(SUM(ordered_amt) AS NUMERIC) AS ordered_amt,
  SUM(ordered_qty) AS ordered_qty,
  CAST(SUM(returned_amt) AS NUMERIC) AS returned_amt,
  SUM(returned_qty) AS returned_qty,
  CAST(SUM(shipped_amt) AS NUMERIC) AS shipped_amt,
  SUM(shipped_qty) AS shipped_qty,
  CAST(SUM(same_day_canceled_amt) AS NUMERIC) AS same_day_canceled_amt,
  SUM(same_day_canceled_qty) AS same_day_canceled_qty,
  CAST(SUM(fraud_canceled_amt) AS NUMERIC) AS fraud_canceled_amt,
  SUM(fraud_canceled_qty) AS fraud_canceled_qty,
  CAST(SUM(canceled_total_amt) AS NUMERIC) AS canceled_total_amt,
  SUM(canceled_total_qty) AS canceled_total_qty,
  MAX(dw_batch_date) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  order_line_promotion_id_list
 FROM (SELECT order_date_pacific,
     order_date_pacific AS activity_date_pacific,
     source_platform_code,
      CASE
      WHEN LOWER(COALESCE(first_released_node_type_code, '')) = LOWER('')
      THEN '-1'
      ELSE first_released_node_type_code
      END AS fulfill_type_code,
     rms_sku_num,
      CASE
      WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN '5405'
      WHEN LOWER(promise_type_code) LIKE LOWER('%PICKUP%')
      THEN SUBSTR(CAST(COALESCE(destination_node_num, picked_up_by_customer_node_num, - 1) AS STRING), 1, 32)
      WHEN LOWER(source_platform_code) = LOWER('POS')
      THEN SUBSTR(CAST(COALESCE(source_store_num, - 1) AS STRING), 1, 32)
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN '828'
      WHEN LOWER(source_channel_country_code) = LOWER('CA')
      THEN '867'
      ELSE '808'
      END AS store_num,
      CASE
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN 'NRHL'
      WHEN LOWER(source_channel_country_code) = LOWER('CA')
      THEN 'NCA'
      ELSE 'NCOM'
      END AS order_source_code,
     COALESCE(order_line_amount * COALESCE(order_line_quantity, 0), 0) AS ordered_amt,
     COALESCE(order_line_quantity, 0) AS ordered_qty,
     order_line_promotion_id_list,
     0 AS returned_amt,
     0 AS returned_qty,
     0 AS shipped_amt,
     0 AS shipped_qty,
     0 AS same_day_canceled_amt,
     0 AS same_day_canceled_qty,
     0 AS fraud_canceled_amt,
     0 AS fraud_canceled_qty,
     0 AS canceled_total_amt,
     0 AS canceled_total_qty,
     dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_daily_delta_fct
    WHERE first_released_date_pacific IS NOT NULL
     OR (LOWER(fraud_cancel_ind) = LOWER('Y') OR LOWER(pre_release_cancel_ind) = LOWER('Y')) AND
       first_released_date_pacific IS NULL AND canceled_date_pacific IS NOT NULL

UNION ALL

    SELECT order_date_pacific,
     returned_without_pickup_date_pacific AS activity_date_pacific,
     source_platform_code,
      CASE
      WHEN LOWER(COALESCE(last_released_node_type_code, '')) = LOWER('')
      THEN '-1'
      ELSE last_released_node_type_code
      END AS fulfill_type_code,
     rms_sku_num,
      CASE
      WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN '5405'
      WHEN LOWER(promise_type_code) LIKE LOWER('%PICKUP%')
      THEN SUBSTR(CAST(COALESCE(destination_node_num, picked_up_by_customer_node_num, - 1) AS STRING), 1, 32)
      WHEN LOWER(source_platform_code) = LOWER('POS')
      THEN SUBSTR(CAST(COALESCE(source_store_num, - 1) AS STRING), 1, 32)
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN '828'
      WHEN LOWER(source_channel_country_code) = LOWER('CA')
      THEN '867'
      ELSE '808'
      END AS store_num,
      CASE
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN 'NRHL'
      WHEN LOWER(source_channel_country_code) = LOWER('CA')
      THEN 'NCA'
      ELSE 'NCOM'
      END AS order_source_code,
     0 AS ordered_amt,
     0 AS ordered_qty,
     order_line_promotion_id_list,
     COALESCE(order_line_amount * order_line_quantity, 0) AS returned_amt,
     COALESCE(order_line_quantity, 0) AS returned_qty,
     0 AS shipped_amt,
     0 AS shipped_qty,
     0 AS same_day_canceled_amt,
     0 AS same_day_canceled_qty,
     0 AS fraud_canceled_amt,
     0 AS fraud_canceled_qty,
     0 AS canceled_total_amt,
     0 AS canceled_total_qty,
     dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_daily_delta_fct
    WHERE returned_without_pickup_date_pacific IS NOT NULL

UNION ALL
    
	SELECT order_date_pacific,
      CASE
      WHEN LOWER(promise_type_code) LIKE LOWER('%PICKUP%')
      THEN COALESCE(shipped_to_customer_date_pacific, picked_up_by_customer_date_pacific)
      ELSE shipped_date_pacific
      END AS activity_date_pacific,
     source_platform_code,
      CASE
      WHEN LOWER(COALESCE(last_released_node_type_code, '')) = LOWER('')
      THEN '-1'
      ELSE last_released_node_type_code
      END AS fulfill_type_code,
     rms_sku_num,
      CASE
      WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN '5405'
      WHEN LOWER(promise_type_code) LIKE LOWER('%PICKUP%')
      THEN SUBSTR(CAST(COALESCE(destination_node_num, picked_up_by_customer_node_num, - 1) AS STRING), 1, 32)
      WHEN LOWER(source_platform_code) = LOWER('POS')
      THEN SUBSTR(CAST(COALESCE(source_store_num, - 1) AS STRING), 1, 32)
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN '828'
      WHEN LOWER(source_channel_country_code) = LOWER('CA')
      THEN '867'
      ELSE '808'
      END AS store_num,
      CASE
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN 'NRHL'
      WHEN LOWER(source_channel_country_code) = LOWER('CA')
      THEN 'NCA'
      ELSE 'NCOM'
      END AS order_source_code,
     0 AS ordered_amt,
     0 AS ordered_qty,
     order_line_promotion_id_list,
     0 AS returned_amt,
     0 AS returned_qty,
     COALESCE(order_line_amount * order_line_quantity, 0) AS shipped_amt,
     COALESCE(order_line_quantity, 0) AS shipped_qty,
     0 AS same_day_canceled_amt,
     0 AS same_day_canceled_qty,
     0 AS fraud_canceled_amt,
     0 AS fraud_canceled_qty,
     0 AS canceled_total_amt,
     0 AS canceled_total_qty,
     dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_daily_delta_fct
    WHERE LOWER(promise_type_code) LIKE LOWER('%PICKUP%') AND (shipped_to_customer_date_pacific IS NOT NULL OR
        picked_up_by_customer_date_pacific IS NOT NULL)
     OR LOWER(promise_type_code) NOT LIKE LOWER('%PICKUP%') AND shipped_date_pacific IS NOT NULL
    
UNION ALL

    SELECT order_date_pacific,
     canceled_date_pacific AS activity_date_pacific,
     source_platform_code,
      CASE
      WHEN LOWER(COALESCE(last_released_node_type_code, '')) = LOWER('')
      THEN '-1'
      ELSE last_released_node_type_code
      END AS fulfill_type_code,
     rms_sku_num,
      CASE
      WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
      THEN '5405'
      WHEN LOWER(promise_type_code) LIKE LOWER('%PICKUP%')
      THEN SUBSTR(CAST(COALESCE(destination_node_num, picked_up_by_customer_node_num, - 1) AS STRING), 1, 32)
      WHEN LOWER(source_platform_code) = LOWER('POS')
      THEN SUBSTR(CAST(COALESCE(source_store_num, - 1) AS STRING), 1, 32)
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN '828'
      WHEN LOWER(source_channel_country_code) = LOWER('CA')
      THEN '867'
      ELSE '808'
      END AS store_num,
      CASE
      WHEN LOWER(source_channel_code) = LOWER('RACK')
      THEN 'NRHL'
      WHEN LOWER(source_channel_country_code) = LOWER('CA')
      THEN 'NCA'
      ELSE 'NCOM'
      END AS order_source_code,
     0 AS ordered_amt,
     0 AS ordered_qty,
     order_line_promotion_id_list,
     0 AS returned_amt,
     0 AS returned_qty,
     0 AS shipped_amt,
     0 AS shipped_qty,
      CASE
      WHEN order_date_pacific = canceled_date_pacific AND LOWER(fraud_cancel_ind) <> LOWER('Y')
      THEN COALESCE(order_line_amount * COALESCE(order_line_quantity, 0), 0)
      ELSE 0
      END AS same_day_canceled_amt,
      CASE
      WHEN order_date_pacific = canceled_date_pacific AND LOWER(fraud_cancel_ind) <> LOWER('Y')
      THEN COALESCE(order_line_quantity, 0)
      ELSE 0
      END AS same_day_canceled_qty,
      CASE
      WHEN LOWER(fraud_cancel_ind) = LOWER('Y')
      THEN COALESCE(order_line_amount * COALESCE(order_line_quantity, 0), 0)
      ELSE 0
      END AS fraud_canceled_amt,
      CASE
      WHEN LOWER(fraud_cancel_ind) = LOWER('Y')
      THEN COALESCE(order_line_quantity, 0)
      ELSE 0
      END AS fraud_canceled_qty,
     COALESCE(order_line_amount * COALESCE(order_line_quantity, 0), 0) AS canceled_total_amt,
     COALESCE(order_line_quantity, 0) AS canceled_total_qty,
     dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_line_daily_delta_fct
    WHERE canceled_date_pacific IS NOT NULL) AS a
 GROUP BY order_date_pacific,
  activity_date_pacific,
  source_platform_code,
  fulfill_type_code,
  rms_sku_num,
  store_num,
  order_source_code,
  order_line_promotion_id_list);

  

/*SET QUERY_BAND = NONE FOR SESSION;*/
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
END;