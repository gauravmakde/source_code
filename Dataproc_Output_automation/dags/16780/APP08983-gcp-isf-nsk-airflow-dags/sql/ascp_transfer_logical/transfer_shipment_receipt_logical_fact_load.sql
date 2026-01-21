BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS isf_dag_control_log_wrk AS
SELECT 
isf_dag_nm,
step_nm,
 batch_id,
 tbl_nm,
 metric_nm,
 metric_value,
  metric_tmstp_utc,
 metric_tmstp_tz,
 metric_date
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS transfer_shipment_receipt_logical_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_TRANSFER_SHIPMENT_RECEIPT_LOGICAL_LDG_JOB')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;



BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO isf_dag_control_log_wrk
(
isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp_utc,
metric_tmstp_tz,
metric_date
)
WITH dummy AS (SELECT 1 AS num)
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 'NULL' AS step_nm,
  (SELECT batch_id
  FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_FCT.TRANSFER_SHIPMENT_RECEIPT_LOGICAL_FACT', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 CAST(CURRENT_DATETIME('GMT') AS STRING) AS metric_value,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS metric_tmstp,
 'GMT' as metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 'NULL' AS step_nm,
  (SELECT batch_id
  FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.TRANSFER_SHIPMENT_RECEIPT_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('ascp_transfer_logical_16780_TECH_SC_NAP_insights')
    AND LOWER(tbl_nm) = LOWER('TRANSFER_SHIPMENT_RECEIPT_LOGICAL_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS metric_tmstp,
'GMT' as metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 'NULL' AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.TRANSFER_SHIPMENT_RECEIPT_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM transfer_shipment_receipt_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 'NULL' AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.TRANSFER_SHIPMENT_RECEIPT_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM transfer_shipment_receipt_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
 'NULL' AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_NAP_STG.TRANSFER_SHIPMENT_RECEIPT_LOGICAL_LDG', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobendtime
    FROM transfer_shipment_receipt_logical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS TIMESTAMP) AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('TRANSFER_SHIPMENT_RECEIPT_LOGICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_transfer_logical',  'load_06_teradata',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_TRANSFER_LOGICAL');


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.transfer_shipment_receipt_logical_gtt
(
  operation_num,
  operation_type,
  shipment_id,
  carton_id,
  rms_sku_num,
  shipment_transfer_source,
  receipt_transfer_source,
  ship_date,
  ship_tmstp,
  ship_tmstp_tz,
  bill_of_lading,
  expected_arrival_date,
  expected_arrival_tmstp,
  expected_arrival_tmstp_tz,
  shipment_asn_number,
  shipment_from_location_id,
  shipment_from_location_type,
  shipment_from_logical_location_id,
  shipment_from_logical_location_type,
  shipment_to_location_id,
  shipment_to_location_type,
  shipment_to_logical_location_id,
  shipment_to_logical_location_type,
  shipment_upc_num,
  receipt_asn_number,
  receipt_from_location_id,
  receipt_from_location_type,
  receipt_from_logical_location_id,
  receipt_from_logical_location_type,
  receipt_to_location_id,
  receipt_to_location_type,
  receipt_to_logical_location_id,
  receipt_to_logical_location_type,
  receipt_upc_num,
  ship_qty,
  ship_cancelled_qty,
  receipt_date,
  receipt_tmstp,
  receipt_tmstp_tz,
  receipt_qty,
  receipt_cancelled_qty
)
SELECT
 SUBSTR(TRIM(operationno), 1, 16),
 RPAD(TRIM(operationtype), 1, ' '),
 SUBSTR(TRIM(shipment_id), 1, 24),
 SUBSTR(TRIM(carton_cartonid), 1, 32),
 SUBSTR(TRIM(items_product_id), 1, 16),
 RPAD(TRIM(shipment_transfer_source), 5, ' '),
 RPAD(TRIM(receipt_transfer_source), 5, ' '),
 CAST(SUBSTR(shipments_shiptime, 1, 10) AS DATE),
 cast(REGEXP_REPLACE(REPLACE(REPLACE(shipments_shiptime, 'Z', '+00:00'), 'T', ' '), '(?i)b|T',
   ' ') as timestamp),
   '+00:00',
 SUBSTR(TRIM(shipments_billoflading), 1, 32),
 CAST(SUBSTR(shipments_expectedarrivaltime, 1, 10) AS DATE),
 cast(REGEXP_REPLACE(REPLACE(REPLACE(shipments_expectedarrivaltime, 'Z', '+00:00'), 'T', ' '),
   '(?i)b|T', ' ')as timestamp),
   '+00:00',
  CASE
  WHEN LOWER(shipments_asnnumber) = LOWER('""')
  THEN NULL
  ELSE SUBSTR(TRIM(shipments_asnnumber), 1, 32)
  END,
  CASE
  WHEN shipments_fromlocation_id IS NULL
  THEN NULL
  ELSE CAST(CAST(trunc(cast(TRIM(shipments_fromlocation_id) as float64)) AS BIGINT) AS INTEGER)
  END,
 RPAD(TRIM(shipments_fromlocation_type), 1, ' '),
  CASE
  WHEN shipments_fromlogicallocation_id IS NULL
  THEN NULL
  ELSE CAST(CAST(trunc(cast(TRIM(shipments_fromlogicallocation_id)as float64)) AS BIGINT) AS INTEGER)
  END,
 RPAD(TRIM(shipments_fromlogicallocation_type), 1, ' '),
  CASE
  WHEN shipments_tolocation_id IS NULL
  THEN NULL
  ELSE CAST(CAST(trunc(cast(TRIM(shipments_tolocation_id)as float64)) AS BIGINT) AS INTEGER)
  END,
 RPAD(TRIM(shipments_tolocation_type), 1, ' '),
  CASE
  WHEN shipments_tologicallocation_id IS NULL
  THEN NULL
  ELSE CAST(CAST(trunc(cast(TRIM(shipments_tologicallocation_id) as float64)) AS BIGINT) AS INTEGER)
  END,
 RPAD(TRIM(shipments_tologicallocation_type), 1, ' '),
 shipments_items_product_upc,
  CASE
  WHEN LOWER(receipts_asnnumber) = LOWER('""')
  THEN NULL
  ELSE SUBSTR(TRIM(receipts_asnnumber), 1, 32)
  END,
  CASE
  WHEN receipts_fromlocation_id IS NULL
  THEN NULL
  ELSE CAST(CAST(trunc(cast(TRIM(receipts_fromlocation_id) as float64)) AS BIGINT) AS INTEGER)
  END,
 RPAD(TRIM(receipts_fromlocation_type), 1, ' '),
  CASE
  WHEN receipts_fromlogicallocation_id IS NULL
  THEN NULL
  ELSE CAST(CAST(trunc(cast(TRIM(receipts_fromlogicallocation_id) as float64))AS BIGINT)  AS INTEGER)
  END,
 RPAD(TRIM(receipts_fromlogicallocation_type), 1, ' '),
  CASE
  WHEN receipts_tolocation_id IS NULL
  THEN NULL
  ELSE CAST(CAST(trunc(cast(TRIM(receipts_tolocation_id) as float64)) AS BIGINT) AS INTEGER)
  END,
 RPAD(TRIM(receipts_tolocation_type), 1, ' '),
  CASE
  WHEN receipts_tologicallocation_id IS NULL
  THEN NULL
  ELSE CAST(CAST(trunc(cast(TRIM(receipts_tologicallocation_id) as float64)) AS BIGINT) AS INTEGER)
  END,
 RPAD(TRIM(receipts_tologicallocation_type), 1, ' '),
 receipts_items_product_upc,
 cast(trunc(cast(shipments_items_quantity as float64)) as int64),
 cast(trunc(cast(shipments_items_cancelledquantity as float64))as int64),
 CAST(SUBSTR(receipts_receipttime, 1, 10) AS DATE) AS receipt_date,
 cast(REGEXP_REPLACE(REPLACE(REPLACE(receipts_receipttime, 'Z', '+00:00'), 'T', ' '), '(?i)b|T'
   ,' ') as timestamp) AS receipt_tmstp,
   '+00:00' as receipt_tmstp_tz,
 cast(trunc(cast(receipts_items_quantity as float64)) as int64),
 cast(trunc(cast(receipts_items_cancelledquantity as float64)) as int64)
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_shipment_receipt_logical_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY operationno, operationtype, shipment_id, carton_cartonid, items_product_id
   ORDER BY COALESCE(receipts_receipttime, shipments_shiptime) DESC)) = 1;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.transfer_shipment_receipt_logical_fact fact
USING (
  SELECT
    operation_num,
    operation_type,
    shipment_id,
    carton_id,
    rms_sku_num,
    COALESCE(shipment_transfer_source, receipt_transfer_source) as transfer_source,
    ship_date,
    ship_tmstp ,
    ship_tmstp_tz,
    bill_of_lading,
    expected_arrival_date,
    expected_arrival_tmstp,
   expected_arrival_tmstp_tz,
    COALESCE(shipment_asn_number, receipt_asn_number) as asn_number,
    COALESCE(shipment_from_location_id, receipt_from_location_id) as from_location_id,
    COALESCE(shipment_from_location_type, receipt_from_location_type) as from_location_type,
    COALESCE(shipment_from_logical_location_id, receipt_from_logical_location_id) as from_logical_location_id,
    COALESCE(shipment_from_logical_location_type, receipt_from_logical_location_type) as from_logical_location_type,
    COALESCE(shipment_to_location_id, receipt_to_location_id) as to_location_id,
    COALESCE(shipment_to_location_type, receipt_to_location_type) as to_location_type,
    COALESCE(shipment_to_logical_location_id, receipt_to_logical_location_id) as to_logical_location_id,
    COALESCE(shipment_to_logical_location_type, receipt_to_logical_location_type) as to_logical_location_type,
    COALESCE(shipment_upc_num, receipt_upc_num) as upc_num,
    ship_qty,
    ship_cancelled_qty,
    receipt_date,
    receipt_tmstp,
    receipt_tmstp_tz,
    receipt_qty,
    receipt_cancelled_qty,
    CASE WHEN COALESCE(shipment_transfer_source, receipt_transfer_source) <> COALESCE(receipt_transfer_source, shipment_transfer_source) then 'N' else 'Y' end as transfer_source_match,
    CASE WHEN COALESCE(shipment_asn_number, receipt_asn_number) <> COALESCE(receipt_asn_number, shipment_asn_number) then 'N' else 'Y' end as asns_match,
    CASE WHEN (COALESCE(shipment_from_location_id, receipt_from_location_id) <> COALESCE(receipt_from_location_id, shipment_from_location_id)
      OR COALESCE(shipment_from_location_type, receipt_from_location_type) <> COALESCE(receipt_from_location_type, shipment_from_location_type)) then 'N' else 'Y' end as from_locations_match,
    CASE WHEN (COALESCE(shipment_from_logical_location_id, receipt_from_logical_location_id) <> COALESCE(receipt_from_logical_location_id, shipment_from_logical_location_id)
      OR COALESCE(shipment_from_logical_location_type, receipt_from_logical_location_type) <> COALESCE(receipt_from_logical_location_type, shipment_from_logical_location_type)) then 'N' else 'Y' end as from_logical_locations_match,
    CASE WHEN (COALESCE(shipment_to_location_id, receipt_to_location_id) <> COALESCE(receipt_to_location_id, shipment_to_location_id)
      OR COALESCE(shipment_to_location_type, receipt_to_location_type) <> COALESCE(receipt_to_location_type, shipment_to_location_type)) then 'N' else 'Y' end as to_locations_match,
    CASE WHEN (COALESCE(shipment_to_logical_location_id, receipt_to_logical_location_id) <> COALESCE(receipt_to_logical_location_id, shipment_to_logical_location_id)
      OR COALESCE(shipment_to_logical_location_type, receipt_to_logical_location_type) <> COALESCE(receipt_to_logical_location_type, shipment_to_logical_location_type)) then 'N' else 'Y' end as to_logical_locations_match,
    CASE WHEN COALESCE(shipment_upc_num, receipt_upc_num) <> COALESCE(receipt_upc_num, shipment_upc_num) then 'N' else 'Y' end as upcs_match,
   (SELECT BATCH_ID FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM = '{{params.subject_area}}') as dw_batch_id,
    (SELECT CURR_BATCH_DATE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE SUBJECT_AREA_NM = '{{params.subject_area}}') as dw_batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.transfer_shipment_receipt_logical_gtt
) ldg
ON fact.operation_num = ldg.operation_num
  AND fact.operation_type = ldg.operation_type
  AND fact.shipment_id = ldg.shipment_id
  AND fact.carton_id = ldg.carton_id
  AND fact.rms_sku_num = ldg.rms_sku_num
WHEN MATCHED THEN
  UPDATE SET
    transfer_source = ldg.transfer_source,
    ship_date = ldg.ship_date,
    ship_tmstp = ldg.ship_tmstp,
    ship_tmstp_tz = ldg.ship_tmstp_tz,
    bill_of_lading = ldg.bill_of_lading,
    expected_arrival_date = ldg.expected_arrival_date,
    expected_arrival_tmstp = ldg.expected_arrival_tmstp,
    expected_arrival_tmstp_tz= ldg.expected_arrival_tmstp_tz,
    asn_number = ldg.asn_number,
    from_location_id = ldg.from_location_id,
    from_location_type = ldg.from_location_type,
    from_logical_location_id = ldg.from_logical_location_id,
    from_logical_location_type = ldg.from_logical_location_type,
    to_location_id = ldg.to_location_id,
    to_location_type = ldg.to_location_type,
    to_logical_location_id = ldg.to_logical_location_id,
    to_logical_location_type = ldg.to_logical_location_type,
    upc_num = ldg.upc_num,
    ship_qty = ldg.ship_qty,
    ship_cancelled_qty = ldg.ship_cancelled_qty,
    receipt_date = ldg.receipt_date,
    receipt_tmstp = ldg.receipt_tmstp,
    receipt_tmstp_tz = ldg.receipt_tmstp_tz,
    receipt_qty = ldg.receipt_qty,
    receipt_cancelled_qty = ldg.receipt_cancelled_qty,
    transfer_source_match = ldg.transfer_source_match,
    asns_match = ldg.asns_match,
    from_locations_match = ldg.from_locations_match,
    from_logical_locations_match = ldg.from_logical_locations_match,
    to_locations_match = ldg.to_locations_match,
    to_logical_locations_match = ldg.to_logical_locations_match,
    upcs_match = ldg.upcs_match,
    dw_batch_id = ldg.dw_batch_id,
    dw_batch_date = ldg.dw_batch_date,
    dw_sys_updt_tmstp = cast(current_datetime('PST8PDT')as timestamp),
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
  WHEN NOT MATCHED THEN
    INSERT (
      operation_num,
      operation_type,
      shipment_id,
      carton_id,
      rms_sku_num,
      transfer_source,
      ship_date,
      ship_tmstp,
      ship_tmstp_tz,
      bill_of_lading,
      expected_arrival_date,
      expected_arrival_tmstp,
      expected_arrival_tmstp_tz,
      asn_number,
      from_location_id,
      from_location_type,
      from_logical_location_id,
      from_logical_location_type,
      to_location_id,
      to_location_type,
      to_logical_location_id,
      to_logical_location_type,
      upc_num,
      ship_qty,
      ship_cancelled_qty,
      receipt_date,
      receipt_tmstp,
      receipt_tmstp_tz,
      receipt_qty,
      receipt_cancelled_qty,
      transfer_source_match,
      asns_match,
      from_locations_match,
      from_logical_locations_match,
      to_locations_match,
      to_logical_locations_match,
      upcs_match,
      dw_batch_id,
      dw_batch_date,
      dw_sys_load_tmstp,
      dw_sys_load_tmstp_tz,
      dw_sys_updt_tmstp,
      dw_sys_updt_tmstp_tz
    )
    VALUES (
      ldg.operation_num,
      ldg.operation_type,
      ldg.shipment_id,
      ldg.carton_id,
      ldg.rms_sku_num,
      ldg.transfer_source,
      ldg.ship_date,
      ldg.ship_tmstp,
      ldg.ship_tmstp_tz,
      ldg.bill_of_lading,
      ldg.expected_arrival_date,
      ldg.expected_arrival_tmstp,
      ldg.expected_arrival_tmstp_tz,
      ldg.asn_number,
      ldg.from_location_id,
      ldg.from_location_type,
      ldg.from_logical_location_id,
      ldg.from_logical_location_type,
      ldg.to_location_id,
      ldg.to_location_type,
      ldg.to_logical_location_id,
      ldg.to_logical_location_type,
      ldg.upc_num,
      ldg.ship_qty,
      ldg.ship_cancelled_qty,
      ldg.receipt_date,
      ldg.receipt_tmstp,
      ldg.receipt_tmstp_tz,
      ldg.receipt_qty,
      ldg.receipt_cancelled_qty,
      ldg.transfer_source_match,
      ldg.asns_match,
      ldg.from_locations_match,
      ldg.from_logical_locations_match,
      ldg.to_locations_match,
      ldg.to_logical_locations_match,
      ldg.upcs_match,
      ldg.dw_batch_id,
      ldg.dw_batch_date,
      cast(current_datetime('PST8PDT')as timestamp),
      `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
      cast(current_datetime('PST8PDT')as timestamp),
      `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
    );

--COLLECT STATS ON  `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.TRANSFER_SHIPMENT_RECEIPT_LOGICAL_FACT INDEX (operation_num, operation_type, shipment_id, carton_id, rms_sku_num);

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1('TRANSFER_SHIPMENT_RECEIPTS_LOGICAL_LDG_TO_BASE',  'NAP_ASCP_TRANSFER_LOGICAL',  '{{params.dbenv}}_NAP_BASE_VWS',  'TRANSFER_SHIPMENT_RECEIPT_LOGICAL_LDG',  '{{params.dbenv}}_NAP_FCT',  'TRANSFER_SHIPMENT_RECEIPT_LOGICAL_FACT',  'Count_Distinct',  0,  'T-S',  'concat(operationno, operationtype, shipment_id, carton_cartonid, items_product_id)',  'concat(operation_num, operation_type, shipment_id, carton_id, rms_sku_num)',  NULL,  NULL,  'N');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('TRANSFER_SHIPMENT_RECEIPT_LOGICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  'ascp_transfer_logical',  'load_06_teradata',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'NAP_ASCP_TRANSFER_LOGICAL');

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `isf_dag_control_log_wrk`
(SELECT 'ascp_transfer_logical_16780_TECH_SC_NAP_insights' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  SUBSTR('{{params.dbenv}}_NAP_FCT.TRANSFER_SHIPMENT_RECEIPT_LOGICAL_FACT', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS STRING), 1, 60) AS metric_value,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP) AS metric_tmstp,
  'GMT' as metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
 SET
    metric_value = wrk.metric_value,
    metric_tmstp = wrk.metric_tmstp_utc,
    metric_tmstp_tz = wrk.metric_tmstp_tz 
    FROM isf_dag_control_log_wrk as wrk
WHERE LOWER(isf_dag_control_log.isf_dag_nm) = LOWER(wrk.isf_dag_nm) AND LOWER(isf_dag_control_log.tbl_nm) = LOWER(wrk.tbl_nm) AND LOWER(isf_dag_control_log.metric_nm) = LOWER(wrk.metric_nm) AND isf_dag_control_log.batch_id = wrk.batch_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
END;
