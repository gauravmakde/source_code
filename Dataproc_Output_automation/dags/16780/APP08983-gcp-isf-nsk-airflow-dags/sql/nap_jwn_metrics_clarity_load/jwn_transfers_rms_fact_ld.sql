CREATE TEMPORARY TABLE IF NOT EXISTS delta_transfers_skus
AS
SELECT rms_sku_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_shipment_receipt_logical_fact
WHERE dw_batch_date = DATE_ADD((SELECT curr_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')), INTERVAL 1 DAY)
GROUP BY rms_sku_num;


CREATE TEMPORARY TABLE IF NOT EXISTS delta_transfers_full
AS
SELECT fct.rms_sku_num,
 fct.operation_type,
  CASE
  WHEN COALESCE(fct.from_location_id, 0) < 10000
  THEN COALESCE(fct.from_location_id, 0)
  WHEN COALESCE(fct.from_location_id, 0) > 10000 AND COALESCE(fct.from_location_id, 0) < 100000
  THEN COALESCE(fct.from_location_id - 10000, 0)
  ELSE COALESCE(fct.from_location_id - 100000, 0)
  END AS from_location_num,
  CASE
  WHEN COALESCE(fct.to_location_id, 0) < 10000
  THEN COALESCE(fct.to_location_id, 0)
  WHEN COALESCE(fct.to_location_id, 0) > 10000 AND COALESCE(fct.to_location_id, 0) < 100000
  THEN COALESCE(fct.to_location_id - 10000, 0)
  ELSE COALESCE(fct.to_location_id - 100000, 0)
  END AS to_location_num,
 fct.receipt_date AS transfer_in_date,
 fct.ship_date AS transfer_out_date,
 cntxt.transfer_type,
  CASE
  WHEN LOWER(cntxt.transfer_context_value) = LOWER('NULL')
  THEN NULL
  ELSE cntxt.transfer_context_value
  END AS transfer_context_type,
 SUM(fct.receipt_qty) AS transfer_in_qty,
 SUM(fct.ship_qty) AS transfer_out_qty
FROM (SELECT fct.operation_num,
   fct.operation_type,
   fct.shipment_id,
   fct.carton_id,
   fct.rms_sku_num,
   fct.transfer_source,
   fct.ship_date,
   fct.ship_tmstp,
   fct.bill_of_lading,
   fct.expected_arrival_date,
   fct.expected_arrival_tmstp,
   fct.asn_number,
   fct.from_location_id,
   fct.from_location_type,
   fct.from_logical_location_id,
   fct.from_logical_location_type,
   fct.to_location_id,
   fct.to_location_type,
   fct.to_logical_location_id,
   fct.to_logical_location_type,
   fct.upc_num,
   fct.ship_qty,
   fct.ship_cancelled_qty,
   fct.receipt_date,
   fct.receipt_tmstp,
   fct.receipt_qty,
   fct.receipt_cancelled_qty,
   fct.transfer_source_match,
   fct.asns_match,
   fct.from_locations_match,
   fct.from_logical_locations_match,
   fct.to_locations_match,
   fct.to_logical_locations_match,
   fct.upcs_match,
   fct.dw_batch_id,
   fct.dw_batch_date,
   fct.dw_sys_load_tmstp,
   fct.dw_sys_updt_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_shipment_receipt_logical_fact AS fct
   INNER JOIN delta_transfers_skus AS d ON LOWER(fct.rms_sku_num) = LOWER(d.rms_sku_num)) AS fct
 LEFT JOIN (SELECT fct2.operation_num,
   fct2.operation_type,
   fct2.rms_sku_num,
   fct2.transfer_source,
   fct2.status,
   fct2.create_date,
   fct2.create_tmstp,
   fct2.request_userid,
   fct2.transfer_type,
   fct2.transfer_context_value,
   fct2.transaction_id,
   fct2.routing_code,
   fct2.freight_code,
   fct2.delivery_date,
   fct2.department_number,
   fct2.event_date,
   fct2.event_timestamp,
   fct2.from_location_id,
   fct2.from_location_type,
   fct2.from_logical_location_id,
   fct2.from_logical_location_type,
   fct2.to_location_id,
   fct2.to_location_type,
   fct2.to_logical_location_id,
   fct2.to_logical_location_type,
   fct2.upc_num,
   fct2.transfer_qty,
   fct2.cancelled_qty,
   fct2.dw_batch_id,
   fct2.dw_batch_date,
   fct2.dw_sys_load_tmstp,
   fct2.dw_sys_updt_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_created_logical_fact AS fct2
   INNER JOIN delta_transfers_skus AS d0 ON LOWER(fct2.rms_sku_num) = LOWER(d0.rms_sku_num)
  WHERE LOWER(fct2.transfer_context_value) NOT IN (LOWER('RC'), LOWER('RD'), LOWER('CD'), LOWER('RV'), LOWER('RTV'))) AS
 cntxt ON LOWER(fct.operation_num) = LOWER(cntxt.operation_num) AND LOWER(fct.rms_sku_num) = LOWER(cntxt.rms_sku_num)
WHERE fct.from_location_id IS NOT NULL
GROUP BY fct.rms_sku_num,
 fct.operation_type,
 from_location_num,
 to_location_num,
 transfer_in_date,
 transfer_out_date,
 cntxt.transfer_type,
 transfer_context_type;


CREATE TEMPORARY TABLE IF NOT EXISTS delta_transfers_4delete_wrk
AS
SELECT f.rms_sku_num,
 f.operation_type,
 f.from_location_num,
 f.to_location_num,
 f.transfer_in_date,
 f.transfer_out_date,
 f.transfer_type,
 f.transfer_context_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS f
 INNER JOIN delta_transfers_skus AS d ON LOWER(f.rms_sku_num) = LOWER(d.rms_sku_num)
 LEFT JOIN delta_transfers_full AS wrk ON LOWER(f.rms_sku_num) = LOWER(wrk.rms_sku_num) AND LOWER(f.operation_type) =
         LOWER(wrk.operation_type) AND f.from_location_num = wrk.from_location_num AND f.to_location_num = wrk.to_location_num
        AND COALESCE(f.transfer_in_date, DATE '1970-01-01') = COALESCE(wrk.transfer_in_date, DATE '1970-01-01') AND
     COALESCE(f.transfer_out_date, DATE '1970-01-01') = COALESCE(wrk.transfer_out_date, DATE '1970-01-01') AND LOWER(COALESCE(f
      .transfer_type, 'NULL')) = LOWER(COALESCE(wrk.transfer_type, 'NULL')) AND LOWER(COALESCE(f.transfer_context_type,
     'NULL')) = LOWER(COALESCE(wrk.transfer_context_type, 'NULL'))
WHERE wrk.rms_sku_num IS NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS jwn_transfers_rms_fact_before
AS
SELECT a.to_location_num,
 a.from_location_num,
 a.rms_sku_num,
 a.transfer_out_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS a
 INNER JOIN delta_transfers_skus AS wrk ON LOWER(a.rms_sku_num) = LOWER(wrk.rms_sku_num)
WHERE a.transfer_out_date >= DATE '2022-10-30'
GROUP BY a.to_location_num,
 a.from_location_num,
 a.rms_sku_num,
 a.transfer_out_date;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_transfers_rms_fact AS fct
WHERE EXISTS (SELECT 1
 FROM delta_transfers_4delete_wrk AS wrk
 WHERE LOWER(fct.rms_sku_num) = LOWER(rms_sku_num)
  AND LOWER(fct.operation_type) = LOWER(operation_type)
  AND fct.from_location_num = from_location_num
  AND fct.to_location_num = to_location_num
  AND COALESCE(fct.transfer_in_date, DATE '1970-01-01') = COALESCE(transfer_in_date, DATE '1970-01-01')
  AND COALESCE(fct.transfer_out_date, DATE '1970-01-01') = COALESCE(transfer_out_date, DATE '1970-01-01')
  AND LOWER(COALESCE(fct.transfer_type, 'NULL')) = LOWER(COALESCE(transfer_type, 'NULL'))
  AND LOWER(COALESCE(fct.transfer_context_type, 'NULL')) = LOWER(COALESCE(transfer_context_type, 'NULL')));


DELETE FROM delta_transfers_full wrk
WHERE EXISTS (SELECT 1
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS fct
 WHERE LOWER(wrk.rms_sku_num) = LOWER(rms_sku_num)
  AND LOWER(wrk.operation_type) = LOWER(operation_type)
  AND wrk.from_location_num = from_location_num
  AND wrk.to_location_num = to_location_num
  AND COALESCE(wrk.transfer_in_date, DATE '1970-01-01') = COALESCE(transfer_in_date, DATE '1970-01-01')
  AND COALESCE(wrk.transfer_out_date, DATE '1970-01-01') = COALESCE(transfer_out_date, DATE '1970-01-01')
  AND COALESCE(wrk.transfer_in_qty, 0) = COALESCE(transfer_in_qty, 0)
  AND COALESCE(wrk.transfer_out_qty, 0) = COALESCE(transfer_out_qty, 0)
  AND LOWER(COALESCE(wrk.transfer_type, 'NULL')) = LOWER(COALESCE(transfer_type, 'NULL'))
  AND LOWER(COALESCE(wrk.transfer_context_type, 'NULL')) = LOWER(COALESCE(transfer_context_type, 'NULL')));


DROP TABLE IF EXISTS delta_transfers_skus;


DROP TABLE IF EXISTS delta_transfers_4delete_wrk;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_transfers_rms_fact AS tgt
USING (SELECT rms_sku_num,
  operation_type,
  from_location_num,
  to_location_num,
  transfer_in_date,
  transfer_out_date,
  transfer_type,
  transfer_context_type,
  SUM(transfer_in_qty) AS transfer_in_qty,
  SUM(transfer_out_qty) AS transfer_out_qty,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM delta_transfers_full
 GROUP BY rms_sku_num,
  operation_type,
  from_location_num,
  to_location_num,
  transfer_in_date,
  transfer_out_date,
  transfer_type,
  transfer_context_type) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(SRC.operation_type) = LOWER(tgt.operation_type) AND SRC.from_location_num
        = tgt.from_location_num AND SRC.to_location_num = tgt.to_location_num AND COALESCE(SRC.transfer_in_date,
      DATE '1970-01-01') = COALESCE(tgt.transfer_in_date, DATE '1970-01-01') AND COALESCE(SRC.transfer_out_date,
     DATE '1970-01-01') = COALESCE(tgt.transfer_out_date, DATE '1970-01-01') AND LOWER(COALESCE(SRC.transfer_type,
     'NULL')) = LOWER(COALESCE(tgt.transfer_type, 'NULL')) AND LOWER(COALESCE(SRC.transfer_context_type, 'NULL')) =
  LOWER(COALESCE(tgt.transfer_context_type, 'NULL'))
WHEN MATCHED THEN UPDATE SET
 transfer_in_qty = SRC.transfer_in_qty,
 transfer_out_qty = SRC.transfer_out_qty,
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp = cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) as timestamp),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN 
INSERT VALUES(SRC.rms_sku_num, SRC.operation_type, SRC.from_location_num, SRC.to_location_num, SRC
 .transfer_in_date, SRC.transfer_out_date, SRC.transfer_type, SRC.transfer_context_type, SRC.transfer_in_qty, SRC.transfer_out_qty
 , SRC.dw_batch_id, SRC.dw_batch_date, cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) as timestamp),
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
  , CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) as TIMESTAMP)
  ,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 );


DROP TABLE IF EXISTS delta_transfers_full;


CREATE TEMPORARY TABLE IF NOT EXISTS delta_transfers_wrk
AS
SELECT rms_sku_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact
WHERE dw_batch_date = (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
GROUP BY rms_sku_num;


CREATE TEMPORARY TABLE IF NOT EXISTS jwn_transfers_rms_fact_after
AS
SELECT a.to_location_num,
 a.from_location_num,
 a.rms_sku_num,
 a.transfer_out_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS a
 INNER JOIN delta_transfers_wrk AS wrk ON LOWER(a.rms_sku_num) = LOWER(wrk.rms_sku_num)
WHERE a.transfer_out_date >= DATE '2022-10-30'
GROUP BY a.to_location_num,
 a.from_location_num,
 a.rms_sku_num,
 a.transfer_out_date;


CREATE TEMPORARY TABLE IF NOT EXISTS jwn_transfers_rms_fact_wrk
AS
SELECT COALESCE(transfer_in.rms_sku_num, transfer_out.rms_sku_num) AS rms_sku_num,
 COALESCE(transfer_in.location_num, transfer_out.location_num) AS location_num,
 COALESCE(transfer_in.location_type, transfer_out.location_type) AS location_type,
 COALESCE(transfer_in.channel_num, transfer_out.channel_num) AS channel_num,
 COALESCE(transfer_in.channel_desc, transfer_out.channel_desc) AS channel_desc,
 COALESCE(transfer_in.store_country_code, transfer_out.store_country_code) AS store_country_code,
 COALESCE(transfer_in.reporting_date, transfer_out.reporting_date) AS reporting_date,
 COALESCE(transfer_in.po_transfer_in_qty, 0) AS po_transfer_in_qty,
 COALESCE(transfer_in.reserve_stock_transfer_in_qty, 0) AS reserve_stock_transfer_in_qty,
 COALESCE(transfer_in.pack_and_hold_transfer_in_qty, 0) AS pack_and_hold_transfer_in_qty,
 COALESCE(transfer_in.racking_transfer_in_qty, 0) AS racking_transfer_in_qty,
 COALESCE(transfer_in.ghost_transfer_in_qty, 0) AS ghost_transfer_in_qty,
 COALESCE(transfer_in.other_transfer_in_qty, 0) AS other_transfer_in_qty,
 COALESCE(transfer_in.po_transfer_in_retail_amt, 0) AS po_transfer_in_retail_amt,
 COALESCE(transfer_in.reserve_stock_transfer_in_retail_amt, 0) AS reserve_stock_transfer_in_retail_amt,
 COALESCE(transfer_in.pack_and_hold_transfer_in_retail_amt, 0) AS pack_and_hold_transfer_in_retail_amt,
 COALESCE(transfer_in.racking_transfer_in_retail_amt, 0) AS racking_transfer_in_retail_amt,
 COALESCE(transfer_in.ghost_transfer_in_retail_amt, 0) AS ghost_transfer_in_retail_amt,
 COALESCE(transfer_in.other_transfer_in_retail_amt, 0) AS other_transfer_in_retail_amt,
 COALESCE(transfer_out.po_transfer_out_qty, 0) AS po_transfer_out_qty,
 COALESCE(transfer_out.reserve_stock_transfer_out_qty, 0) AS reserve_stock_transfer_out_qty,
 COALESCE(transfer_out.pack_and_hold_transfer_out_qty, 0) AS pack_and_hold_transfer_out_qty,
 COALESCE(transfer_out.racking_transfer_out_qty, 0) AS racking_transfer_out_qty,
 COALESCE(transfer_out.ghost_transfer_out_qty, 0) AS ghost_transfer_out_qty,
 COALESCE(transfer_out.other_transfer_out_qty, 0) AS other_transfer_out_qty,
 COALESCE(transfer_in.dw_batch_date, transfer_out.dw_batch_date) AS dw_batch_date
FROM (SELECT a.rms_sku_num,
   a.to_location_num AS location_num,
   a.in_location_type_code AS location_type,
   a.in_location_channel_num AS channel_num,
   a.in_location_channel_desc AS channel_desc,
   a.in_location_country_code AS store_country_code,
   a.reporting_date,
   a.po_transfer_in_qty,
   a.reserve_stock_transfer_in_qty,
   a.pack_and_hold_transfer_in_qty,
   a.racking_transfer_in_qty,
   a.ghost_transfer_in_qty,
   a.other_transfer_in_qty,
   a.po_transfer_in_retail_amt,
   a.reserve_stock_transfer_in_retail_amt,
   a.pack_and_hold_transfer_in_retail_amt,
   a.racking_transfer_in_retail_amt,
   a.ghost_transfer_in_retail_amt,
   a.other_transfer_in_retail_amt,
   a.dw_batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_virtual_rms_transfers_in_fact_vw AS a
   INNER JOIN delta_transfers_wrk AS wrk ON LOWER(a.rms_sku_num) = LOWER(wrk.rms_sku_num)
  WHERE a.reporting_date >= DATE '2022-10-30') AS transfer_in
 FULL JOIN (SELECT a0.rms_sku_num,
   a0.from_location_num AS location_num,
   a0.out_location_type_code AS location_type,
   a0.out_location_channel_num AS channel_num,
   a0.out_location_channel_desc AS channel_desc,
   a0.out_location_country_code AS store_country_code,
   a0.reporting_date,
   a0.po_transfer_out_qty,
   a0.reserve_stock_transfer_out_qty,
   a0.pack_and_hold_transfer_out_qty,
   a0.racking_transfer_out_qty,
   a0.ghost_transfer_out_qty,
   a0.other_transfer_out_qty,
   a0.dw_batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_out_fact_vw AS a0
   INNER JOIN delta_transfers_wrk AS wrk0 ON LOWER(a0.rms_sku_num) = LOWER(wrk0.rms_sku_num)
  WHERE a0.reporting_date >= DATE '2022-10-30') AS transfer_out ON LOWER(transfer_in.rms_sku_num) = LOWER(transfer_out.rms_sku_num
         ) AND COALESCE(transfer_in.location_num, 0) = COALESCE(transfer_out.location_num, 0) AND LOWER(COALESCE(transfer_in
         .location_type, '0')) = LOWER(COALESCE(transfer_out.location_type, '0')) AND COALESCE(transfer_in.channel_num,
       0) = COALESCE(transfer_out.channel_num, 0) AND LOWER(COALESCE(transfer_in.channel_desc, '0')) = LOWER(COALESCE(transfer_out
       .channel_desc, '0')) AND LOWER(COALESCE(transfer_in.store_country_code, '0')) = LOWER(COALESCE(transfer_out.store_country_code
      , '0')) AND transfer_in.reporting_date = transfer_out.reporting_date;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact AS tgt
USING (SELECT a.rms_sku_num,
   a.transfer_out_date AS reporting_date,
   a.to_location_num AS location_num
  FROM jwn_transfers_rms_fact_before AS a
   INNER JOIN delta_transfers_wrk AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num)
   LEFT JOIN jwn_transfers_rms_fact_after AS c ON LOWER(a.rms_sku_num) = LOWER(c.rms_sku_num) AND a.to_location_num = c
       .to_location_num AND a.from_location_num = c.from_location_num AND a.transfer_out_date = c.transfer_out_date
  WHERE c.rms_sku_num IS NULL
  UNION DISTINCT
  SELECT a0.rms_sku_num,
   a0.transfer_out_date AS reporting_date,
   a0.from_location_num AS location_num
  FROM jwn_transfers_rms_fact_before AS a0
   INNER JOIN delta_transfers_wrk AS b0 ON LOWER(a0.rms_sku_num) = LOWER(b0.rms_sku_num)
   LEFT JOIN jwn_transfers_rms_fact_after AS c0 ON LOWER(a0.rms_sku_num) = LOWER(c0.rms_sku_num) AND a0.to_location_num
       = c0.to_location_num AND a0.from_location_num = c0.from_location_num AND a0.transfer_out_date = c0.transfer_out_date
     
  WHERE c0.rms_sku_num IS NULL) AS SRC
ON SRC.location_num = tgt.location_num AND LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.reporting_date = tgt
  .reporting_date
WHEN MATCHED THEN UPDATE SET
 po_transfer_in_qty = 0,
 reserve_stock_transfer_in_qty = 0,
 pack_and_hold_transfer_in_qty = 0,
 racking_transfer_in_qty = 0,
 ghost_transfer_in_qty = 0,
 other_transfer_in_qty = 0,
 po_transfer_out_qty = 0,
 reserve_stock_transfer_out_qty = 0,
 pack_and_hold_transfer_out_qty = 0,
 racking_transfer_out_qty = 0,
 ghost_transfer_out_qty = 0,
 other_transfer_out_qty = 0,
 po_transfer_in_retail_amt = 0.0,
 reserve_stock_transfer_in_retail_amt = 0.0,
 pack_and_hold_transfer_in_retail_amt = 0.0,
 racking_transfer_in_retail_amt = 0.0,
 ghost_transfer_in_retail_amt = 0.0,
 other_transfer_in_retail_amt = 0.0;


DELETE FROM jwn_transfers_rms_fact_wrk wrk
WHERE EXISTS (SELECT 1
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact AS fct
 WHERE LOWER(rms_sku_num) = LOWER(wrk.rms_sku_num)
  AND location_num = wrk.location_num
  AND LOWER(COALESCE(location_type, 'NULL')) = LOWER(COALESCE(wrk.location_type, 'NULL'))
  AND LOWER(COALESCE(FORMAT('%11d', channel_num), 'NULL')) = LOWER(COALESCE(FORMAT('%11d', wrk.channel_num), 'NULL'))
  AND LOWER(COALESCE(channel_desc, 'NULL')) = LOWER(COALESCE(wrk.channel_desc, 'NULL'))
  AND LOWER(COALESCE(store_country_code, 'NULL')) = LOWER(COALESCE(wrk.store_country_code, 'NULL'))
  AND reporting_date = wrk.reporting_date
  AND po_transfer_in_qty = wrk.po_transfer_in_qty
  AND reserve_stock_transfer_in_qty = wrk.reserve_stock_transfer_in_qty
  AND pack_and_hold_transfer_in_qty = wrk.pack_and_hold_transfer_in_qty
  AND racking_transfer_in_qty = wrk.racking_transfer_in_qty
  AND ghost_transfer_in_qty = wrk.ghost_transfer_in_qty
  AND other_transfer_in_qty = wrk.other_transfer_in_qty
  AND po_transfer_out_qty = wrk.po_transfer_out_qty
  AND reserve_stock_transfer_out_qty = wrk.reserve_stock_transfer_out_qty
  AND pack_and_hold_transfer_out_qty = wrk.pack_and_hold_transfer_out_qty
  AND racking_transfer_out_qty = wrk.racking_transfer_out_qty
  AND ghost_transfer_out_qty = wrk.ghost_transfer_out_qty
  AND other_transfer_out_qty = wrk.other_transfer_out_qty
  AND po_transfer_in_retail_amt = wrk.po_transfer_in_retail_amt
  AND reserve_stock_transfer_in_retail_amt = wrk.reserve_stock_transfer_in_retail_amt
  AND pack_and_hold_transfer_in_retail_amt = wrk.pack_and_hold_transfer_in_retail_amt
  AND racking_transfer_in_retail_amt = wrk.racking_transfer_in_retail_amt
  AND ghost_transfer_in_retail_amt = wrk.ghost_transfer_in_retail_amt
  AND other_transfer_in_retail_amt = wrk.other_transfer_in_retail_amt);




MERGE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact TGT
USING (
    SELECT
           a.rms_sku_num AS rms_sku_num,
           a.location_num AS location_num,
           a.location_type AS location_type,
           a.channel_num AS channel_num,
           a.channel_desc AS channel_desc,
           a.store_country_code AS store_country_code,
           a.reporting_date,
           po_transfer_in_qty,
           reserve_stock_transfer_in_qty,
           pack_and_hold_transfer_in_qty,
           racking_transfer_in_qty,
           ghost_transfer_in_qty,
           other_transfer_in_qty,
           po_transfer_in_retail_amt,
           reserve_stock_transfer_in_retail_amt,
           pack_and_hold_transfer_in_retail_amt,
           racking_transfer_in_retail_amt,
           ghost_transfer_in_retail_amt,
           other_transfer_in_retail_amt,
           po_transfer_out_qty,
           reserve_stock_transfer_out_qty,
           pack_and_hold_transfer_out_qty,
           racking_transfer_out_qty,
           ghost_transfer_out_qty,
           other_transfer_out_qty,
           (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE SUBJECT_AREA_NM ='NAP_ASCP_CLARITY_LOAD') AS dw_batch_id,
           (SELECT CURR_BATCH_DATE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE SUBJECT_AREA_NM ='NAP_ASCP_CLARITY_LOAD') AS dw_batch_date,
           COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0) AS unit_price_amt,
           COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code) AS price_type_code,
           'TRANSFERS' AS inventory_source
           ,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz
           ,`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_updt_tmstp_tz
    FROM jwn_transfers_rms_fact_wrk a
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw psd
        ON a.location_num = psd.store_num
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim pro
        ON cast(psd.price_store_num as string) = pro.store_num
        AND a.rms_sku_num = pro.rms_sku_num
        AND a.reporting_date >= CAST(pro.eff_begin_tmstp AS DATE)
        AND a.reporting_date < CAST(pro.eff_end_tmstp AS DATE)
) AS src
ON SRC.location_num = TGT.location_num
AND SRC.rms_sku_num = TGT.rms_sku_num
AND SRC.reporting_date = TGT.reporting_date
WHEN MATCHED THEN 
    UPDATE SET
         unit_price_amt = SRC.unit_price_amt,
         price_type_code = SRC.price_type_code,
         po_transfer_in_qty = SRC.po_transfer_in_qty,
         reserve_stock_transfer_in_qty = SRC.reserve_stock_transfer_in_qty,
         pack_and_hold_transfer_in_qty = SRC.pack_and_hold_transfer_in_qty,
         racking_transfer_in_qty = SRC.racking_transfer_in_qty,
         ghost_transfer_in_qty = SRC.ghost_transfer_in_qty,
         other_transfer_in_qty = SRC.other_transfer_in_qty,
         po_transfer_in_retail_amt = SRC.po_transfer_in_retail_amt,
         reserve_stock_transfer_in_retail_amt = SRC.reserve_stock_transfer_in_retail_amt,
         pack_and_hold_transfer_in_retail_amt = SRC.pack_and_hold_transfer_in_retail_amt,
         racking_transfer_in_retail_amt = SRC.racking_transfer_in_retail_amt,
         ghost_transfer_in_retail_amt = SRC.ghost_transfer_in_retail_amt,
         other_transfer_in_retail_amt = SRC.other_transfer_in_retail_amt,
         po_transfer_out_qty = SRC.po_transfer_out_qty,
         reserve_stock_transfer_out_qty = SRC.reserve_stock_transfer_out_qty,
         pack_and_hold_transfer_out_qty = SRC.pack_and_hold_transfer_out_qty,
         racking_transfer_out_qty = SRC.racking_transfer_out_qty,
         ghost_transfer_out_qty = SRC.ghost_transfer_out_qty,
         other_transfer_out_qty = SRC.other_transfer_out_qty,
         dw_batch_id = SRC.dw_batch_id,
         dw_batch_date = SRC.dw_batch_date,
         dw_sys_updt_tmstp = cast(cast(timestamp(current_datetime('PST8PDT')) as datetime) as timestamp),
		 dw_sys_updt_tmstp_tz = SRC.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN 
    INSERT (
         location_num, location_type, channel_num, channel_desc, store_country_code, rms_sku_num, reporting_date,
         unit_price_amt, price_type_code, po_transfer_in_qty, reserve_stock_transfer_in_qty, pack_and_hold_transfer_in_qty,
         racking_transfer_in_qty, ghost_transfer_in_qty, other_transfer_in_qty, po_transfer_in_retail_amt,
         reserve_stock_transfer_in_retail_amt, pack_and_hold_transfer_in_retail_amt, racking_transfer_in_retail_amt,
         ghost_transfer_in_retail_amt, other_transfer_in_retail_amt, po_transfer_out_qty, reserve_stock_transfer_out_qty,
         pack_and_hold_transfer_out_qty, racking_transfer_out_qty, ghost_transfer_out_qty, other_transfer_out_qty,
         inventory_source, dw_batch_id, dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz
    ) VALUES (
         SRC.location_num, SRC.location_type, SRC.channel_num, SRC.channel_desc, SRC.store_country_code, SRC.rms_sku_num, SRC.reporting_date,
         SRC.unit_price_amt, SRC.price_type_code, SRC.po_transfer_in_qty, SRC.reserve_stock_transfer_in_qty, SRC.pack_and_hold_transfer_in_qty,
         SRC.racking_transfer_in_qty, SRC.ghost_transfer_in_qty, SRC.other_transfer_in_qty, SRC.po_transfer_in_retail_amt,
         SRC.reserve_stock_transfer_in_retail_amt, SRC.pack_and_hold_transfer_in_retail_amt, SRC.racking_transfer_in_retail_amt,
         SRC.ghost_transfer_in_retail_amt, SRC.other_transfer_in_retail_amt, SRC.po_transfer_out_qty, SRC.reserve_stock_transfer_out_qty,
         SRC.pack_and_hold_transfer_out_qty, SRC.racking_transfer_out_qty, SRC.ghost_transfer_out_qty, SRC.other_transfer_out_qty,
         SRC.inventory_source, SRC.dw_batch_id, SRC.dw_batch_date, timestamp(current_datetime('PST8PDT')),dw_sys_load_tmstp_tz, timestamp(current_datetime('PST8PDT')),dw_sys_updt_tmstp_tz
    );


