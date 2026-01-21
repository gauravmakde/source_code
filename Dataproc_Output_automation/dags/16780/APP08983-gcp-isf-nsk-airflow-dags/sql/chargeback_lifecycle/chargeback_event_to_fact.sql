INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.ascp_error_ldg (target_table, unique_key, reason, dw_batch_id, dw_sys_load_tmstp,dw_sys_load_tmstp_tz,
 dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz)
(SELECT 'CHARGEBACK_EVENT_FACT',
     'ChargebackId: ' || chargeback_id || ', EventName: ' || event_name,
  'Revision ChargebackID or EventName are NULL',
  CAST(dw_batch_id AS BIGINT) AS dw_batch_id,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_updt_tmstp_tz
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.chargeback_event_ldg
 WHERE chargeback_id IS NULL
 OR event_name IS NULL);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.ascp_error_ldg (target_table, unique_key, reason, dw_batch_id, dw_sys_load_tmstp,dw_sys_load_tmstp_tz,
 dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz)
(SELECT 'CHARGEBACK_EVENT_FACT',
     'ChargebackId: ' || chargeback_id || ', EventName: ' || event_name,
     'Unable to cast amount: ' || FORMAT('%20d', amount_units) || ' and ' || amount_nanos,
  CAST(dw_batch_id AS BIGINT) AS dw_batch_id,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.chargeback_event_ldg
 WHERE CAST(amount_units AS BIGINT) > 99999999999999
  OR LENGTH(RTRIM(TRIM(amount_nanos), '0')) > 4);


/***********************************************************************************
-- Merge into the target fact table
************************************************************************************/


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.chargeback_event_fact AS fact
USING 
(SELECT 
chargeback_id,
  last_updated_time,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(last_updated_time as string)) as last_updated_time_tz,
  event_name,
  event_date_pacific,
  event_time,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(event_time as string)) as event_time_tz,
  vendor_id,
  rms_purchase_order_id,
  invoice_number,
  inbound_shipment_number,
  vendor_bill_of_lading,
  amount_currency_code AS currency_code,
  ROUND(CAST(CAST(amount_units AS BIGINT) + CAST(trunc(cast(CASE
        WHEN amount_nanos = ''
        THEN '0'
        ELSE amount_nanos
        END as float64)) AS INTEGER) * 0.000000001 AS NUMERIC), 4) AS amount,
  units,
  violation_type,
  exemption_reason,
  TRIM(dw_batch_id) AS dw_batch_id
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.chargeback_event_ldg
 WHERE (amount_units IS NULL OR CAST(amount_units AS BIGINT) <= 99999999999999)
  AND (amount_nanos IS NULL OR LENGTH(RTRIM(TRIM(amount_nanos), '0')) <= 4)
  AND event_name IS NOT NULL
  AND chargeback_id IS NOT NULL) AS ldg
ON LOWER(ldg.event_name) = LOWER(fact.event_name) AND LOWER(ldg.chargeback_id) = LOWER(fact.chargeback_id) 
AND cast(ldg.event_time as timestamp) = fact.event_time
WHEN MATCHED THEN UPDATE SET
 vendor_id = ldg.vendor_id,
 last_updated_time = cast(ldg.last_updated_time as timestamp),
 last_updated_time_tz = ldg.last_updated_time_tz,
 rms_purchase_order_id = ldg.rms_purchase_order_id,
 invoice_number = ldg.invoice_number,
 inbound_shipment_number = ldg.inbound_shipment_number,
 vendor_bill_of_lading = ldg.vendor_bill_of_lading,
 currency_code = ldg.currency_code,
 amount = ldg.amount,
 units = ldg.units,
 violation_type = ldg.violation_type,
 exemption_reason = ldg.exemption_reason,
 dw_batch_id = ldg.dw_batch_id,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS timestamp),
 dw_sys_updt_tmstp_tz = 'PST8PDT'
WHEN NOT MATCHED THEN INSERT 
(
chargeback_id
,last_updated_time
,last_updated_time_tz
,event_name
,event_date_pacific
,event_time
,event_time_tz
,vendor_id
,rms_purchase_order_id
,invoice_number
,inbound_shipment_number
,vendor_bill_of_lading
,currency_code
,amount
,units
,violation_type
,exemption_reason
,dw_batch_id
,dw_sys_load_tmstp
,dw_sys_load_tmstp_tz
,dw_sys_updt_tmstp
,dw_sys_updt_tmstp_tz
)
VALUES
(
  ldg.chargeback_id,
  cast(ldg.last_updated_time as timestamp),
  ldg.last_updated_time_tz,
  ldg.event_name,
  ldg.event_date_pacific,
  cast(ldg.event_time as timestamp),
  ldg.event_time_tz,
  ldg.vendor_id,
  ldg.rms_purchase_order_id,
  ldg.invoice_number,
  ldg.inbound_shipment_number,
  ldg.vendor_bill_of_lading,
  ldg.currency_code,
  ldg.amount,
  ldg.units,
  ldg.violation_type,
  ldg.exemption_reason,
  ldg.dw_batch_id,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS timestamp),
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS timestamp),
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 );


/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/


--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.CHARGEBACK_EVENT_FACT INDEX (chargeback_id, event_name)
