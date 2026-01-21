
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=demand_fact_load_10976_tech_nap_merch;
---Task_Name=DMND_LD_t3_dmnd_dlta_to_fct;'*/
BEGIN TRANSACTION;

BEGIN
SET ERROR_CODE  =  0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.demand_sku_store_fct AS target 
SET
    ordered_amt = target.ordered_amt + source.ordered_amt,
    ordered_qty = target.ordered_qty + source.ordered_qty,
    demand_amt = CAST(target.demand_amt + source.demand_amt AS NUMERIC),
    demand_qty = target.demand_qty + source.demand_qty,
    returned_amt = target.returned_amt + source.returned_amt,
    returned_qty = target.returned_qty + source.returned_qty,
    shipped_amt = target.shipped_amt + source.shipped_amt,
    shipped_qty = target.shipped_qty + source.shipped_qty,
    same_day_canceled_amt = target.same_day_canceled_amt + source.same_day_canceled_amt,
    same_day_canceled_qty = target.same_day_canceled_qty + source.same_day_canceled_qty,
    fraud_canceled_amt = target.fraud_canceled_amt + source.fraud_canceled_amt,
    fraud_canceled_qty = target.fraud_canceled_qty + source.fraud_canceled_qty,
    canceled_total_amt = target.canceled_total_amt + source.canceled_total_amt,
    canceled_total_qty = target.canceled_total_qty + source.canceled_total_qty,
    dw_batch_date = source.dw_batch_date,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) FROM (SELECT demand_date_pacific, rms_sku_num, store_num, order_source_code, fulfill_type_code, ordered_amt, ordered_qty, demand_amt, demand_qty, order_line_promotion_id_list, returned_amt, returned_qty, shipped_amt, shipped_qty, same_day_canceled_amt, same_day_canceled_qty, fraud_canceled_amt, fraud_canceled_qty, canceled_total_amt, canceled_total_qty, dw_batch_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_sku_store_daily_fct) AS source
WHERE source.demand_date_pacific = target.demand_date_pacific AND LOWER(source.rms_sku_num) = LOWER(target.rms_sku_num) AND source.store_num = target.store_num AND LOWER(source.order_source_code) = LOWER(target.order_source_code) AND LOWER(source.fulfill_type_code) = LOWER(target.fulfill_type_code) AND LOWER(COALESCE(source.order_line_promotion_id_list, '')) = LOWER(COALESCE(target.order_line_promotion_id_list, ''));



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.demand_sku_store_fct (demand_date_pacific, rms_sku_num, store_num, order_source_code,
 fulfill_type_code, ordered_amt, ordered_qty, demand_amt, demand_qty, returned_amt, returned_qty, shipped_amt,
 shipped_qty, same_day_canceled_amt, same_day_canceled_qty, fraud_canceled_amt, fraud_canceled_qty, canceled_total_amt,
 canceled_total_qty, dw_batch_date, dw_sys_load_tmstp, dw_sys_updt_tmstp, order_line_promotion_id_list)
(SELECT a.demand_date_pacific,
  a.rms_sku_num,
  a.store_num,
  a.order_source_code,
  a.fulfill_type_code,
  a.ordered_amt,
  a.ordered_qty,
  CAST(a.demand_amt AS NUMERIC) AS demand_amt,
  a.demand_qty,
  a.returned_amt,
  a.returned_qty,
  a.shipped_amt,
  a.shipped_qty,
  a.same_day_canceled_amt,
  a.same_day_canceled_qty,
  a.fraud_canceled_amt,
  a.fraud_canceled_qty,
  a.canceled_total_amt,
  a.canceled_total_qty,
  a.dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  a.order_line_promotion_id_list
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.order_sku_store_daily_fct AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.demand_sku_store_fct AS cofct ON a.demand_date_pacific = cofct.demand_date_pacific AND LOWER(a.rms_sku_num
         ) = LOWER(cofct.rms_sku_num) AND a.store_num = cofct.store_num AND LOWER(a.order_source_code) = LOWER(cofct.order_source_code
       ) AND LOWER(a.fulfill_type_code) = LOWER(cofct.fulfill_type_code) AND LOWER(COALESCE(a.order_line_promotion_id_list
      , '')) = LOWER(COALESCE(cofct.order_line_promotion_id_list, ''))
 WHERE cofct.demand_date_pacific IS NULL);

 
 
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
END;