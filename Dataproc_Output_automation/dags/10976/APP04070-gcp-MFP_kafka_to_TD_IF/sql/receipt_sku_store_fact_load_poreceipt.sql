UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup SET
 dw_batch_dt = DATE_ADD(etl_batch_dt_lkup.dw_batch_dt, INTERVAL 1 DAY),
 extract_start_dt = DATE_ADD(etl_batch_dt_lkup.extract_start_dt, INTERVAL 1 DAY),
 extract_end_dt = DATE_ADD(etl_batch_dt_lkup.extract_end_dt, INTERVAL 1 DAY)
WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_PORL') AND dw_batch_dt < CURRENT_DATE('PST8PDT');


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_poreceipt_sku_store_fact AS mpssf
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_poreceipt_ledger_fact AS mplf
 WHERE LOWER(mpssf.sku_num) = LOWER(sku_num)
  AND mpssf.store_num = store_num
  AND LOWER(mpssf.adjustment_type) <> LOWER('-1')
  AND LOWER(mpssf.poreceipt_order_number) = LOWER(poreceipt_order_number)
  AND mpssf.event_id = event_id
  AND mpssf.tran_date = tran_date
  AND CAST(dw_sys_load_tmstp AS DATE) > DATE_SUB(CAST((SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_PORL')) AS DATE), INTERVAL (SELECT interface_freq
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_PORL')) DAY)
  AND CAST(dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_PORL')) AS DATE)
  AND CAST(mpssf.dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_PORL')) AS DATE));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_poreceipt_sku_store_fact (poreceipt_order_number, dropship_ind, rp_ind, event_time,event_time_tz,
 shipment_num, adjustment_type, store_num, sku_num, tran_code, tran_date, receipts_units, receipts_cost_currency_code,
 receipts_cost, receipts_retail_currency_code, receipts_retail, receipts_crossdock_units,
 receipts_crossdock_cost_currency_code, receipts_crossdock_cost, receipts_crossdock_retail_currency_code,
 receipts_crossdock_retail, dw_sys_load_tmstp, dw_batch_date, event_id)
(SELECT mplf.poreceipt_order_number,
   CASE
   WHEN mplf.tran_code = 20 AND mplf.store_num IN (808, 828) AND mplf.poreceipt_order_number IS NOT NULL
   THEN 'Y'
   ELSE 'N'
   END AS dropship_ind,
   CASE
   WHEN LOWER(pohef.order_type) = LOWER('AUTOMATIC_REORDER') OR LOWER(pohef.order_type) = LOWER('BUYER_REORDER')
   THEN 'Y'
   ELSE 'N'
   END AS rp_ind,
  mplf.event_time_UTC ,
   mplf.event_time_tz,
  mplf.shipment_num,
  mplf.adjustment_type,
  mplf.store_num,
  mplf.sku_num,
  mplf.tran_code,
  mplf.tran_date,
  mplf.quantity AS receipts_units,
  mplf.total_cost_curr_code AS receipts_cost_currency_code,
  ROUND(CAST(mplf.total_cost_amount AS NUMERIC), 4) AS receipts_cost,
  mplf.total_retail_curr_code AS receipts_retail_currency_code,
  ROUND(CAST(mplf.total_retail_amount AS NUMERIC), 4) AS receipts_retail,
  0 AS receipts_crossdock_units,
  mplf.total_cost_curr_code AS receipts_crossdock_cost_currency_code,
  0 AS receipts_crossdock_cost,
  mplf.total_retail_curr_code AS receipts_crossdock_retail_currency_code,
  0 AS receipts_crossdock_retail,
  mplf.dw_sys_load_tmstp,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  mplf.event_id
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_poreceipt_ledger_fact AS mplf
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.purchase_order_header_fact AS pohef ON LOWER(pohef.purchase_order_number) = LOWER(mplf.poreceipt_order_number
    )
 WHERE CAST(mplf.dw_sys_load_tmstp AS DATE) > DATE_SUB(CAST((SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_PORL')) AS DATE), INTERVAL (SELECT interface_freq
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_PORL')) DAY)
  AND CAST(mplf.dw_sys_load_tmstp AS DATE) <= CAST((SELECT dw_batch_dt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('MERCH_NAP_RCPT_PORL')) AS DATE));