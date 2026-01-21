UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
    dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL 1 DAY),
    extract_start_dt = DATE_ADD(etl_batch_dt_lkup.extract_start_dt, INTERVAL 1 DAY),
    extract_end_dt = DATE_ADD(etl_batch_dt_lkup.extract_end_dt, INTERVAL 1 DAY)
WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_TRNSF') AND dw_batch_dt < CURRENT_DATE('PST8PDT');

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_poreceipt_sku_store_fact AS mpssf
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_transfer_ledger_fact AS mtlf
    WHERE LOWER(mpssf.sku_num) = LOWER(sku_num) AND mpssf.store_num = location_num AND LOWER(mpssf.adjustment_type) = LOWER('-1') AND LOWER(mpssf.poreceipt_order_number) = LOWER(transfer_ledger_id) AND mpssf.event_id = event_id AND mpssf.tran_date = transaction_date AND CAST(dw_sys_load_tmstp AS DATE) > DATE_SUB(CAST((SELECT dw_batch_dt
                        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
                        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_TRNSF')) AS DATE), INTERVAL (SELECT interface_freq
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
                    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_TRNSF')) DAY) AND CAST(dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
                    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_TRNSF')) AS DATE) AND CAST(mpssf.dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
                    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_TRNSF')) AS DATE));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_poreceipt_sku_store_fact (poreceipt_order_number, dropship_ind, rp_ind, event_time,event_time_tz,
 shipment_num, adjustment_type, store_num, sku_num, tran_code, tran_date, receipts_units, receipts_cost_currency_code,
 receipts_cost, receipts_retail_currency_code, receipts_retail, receipts_crossdock_units,
 receipts_crossdock_cost_currency_code, receipts_crossdock_cost, receipts_crossdock_retail_currency_code,
 receipts_crossdock_retail, dw_sys_load_tmstp, dw_batch_date, event_id)
(SELECT SUBSTR(mtlf.transfer_ledger_id, 1, 100) AS poreceipt_order_number,
  'N' AS dropship_ind,
   CASE
   WHEN LOWER(podf.order_type) = LOWER('AUTOMATIC_REORDER') OR LOWER(podf.order_type) = LOWER('BUYER_REORDER')
   THEN 'Y'
   ELSE 'N'
   END AS rp_ind,
  mtlf.event_time,
  mtlf.event_time_tz,
  SUBSTR(CAST(mtlf.shipment_num AS STRING), 1, 100) AS shipment_num,
  '-1' AS adjustment_type,
  mtlf.location_num AS store_num,
  mtlf.sku_num,
  CAST(trunc(cast(CASE
    WHEN mtlf.transaction_code = ''
    THEN '0'
    ELSE mtlf.transaction_code
    END as float64)) AS INTEGER) AS tran_code,
  mtlf.transaction_date AS tran_date,
  0 AS receipts_units,
  mtlf.total_cost_currency_code AS receipts_cost_currency_code,
  0 AS receipts_cost,
  mtlf.total_retail_currency_code AS receipts_retail_currency_code,
  0 AS receipts_retail,
  mtlf.quantity AS receipts_crossdock_units,
  mtlf.total_cost_currency_code AS receipts_crossdock_cost_currency_code,
  ROUND(CAST(mtlf.total_cost_amount AS NUMERIC), 4) AS receipts_crossdock_cost,
  mtlf.total_retail_currency_code AS receipts_crossdock_retail_currency_code,
  ROUND(CAST(mtlf.total_retail_amount AS NUMERIC), 4) AS receipts_crossdock_retail,
  CAST(mtlf.dw_sys_load_tmstp AS DATETIME) AS dw_sys_load_tmstp,
  CURRENT_DATE("PST8PDT") AS dw_batch_date,
  mtlf.event_id
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_transfer_ledger_fact AS mtlf
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_distributelocation_fact AS podf ON LOWER(TRIM(mtlf.transfer_ledger_id)) =
     LOWER(TRIM(SUBSTR(podf.external_distribution_id, 1, 60))) AND LOWER(mtlf.sku_num) = LOWER(podf.rms_sku_num) AND
    mtlf.location_num = CAST(podf.distribute_location_id AS FLOAT64)
 WHERE LOWER(mtlf.event_type) = LOWER('ALLOCATION_SHIP_TO_LOCATION_LEDGER_POSTED')
  AND LOWER(mtlf.transfer_operation_type) = LOWER('ALLOCATION')
  AND CAST(mtlf.dw_sys_load_tmstp AS DATE) > DATE_SUB(CAST((SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_TRNSF')) AS DATE), INTERVAL (SELECT interface_freq
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_TRNSF')) DAY)
  AND CAST(mtlf.dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_TRNSF')) AS DATE));
