TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.chargeback_event_ldg;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.chargeback_event_ldg
WITH chargeback AS (
  SELECT
    *
  FROM
    `scoi.chargeback_lifecycle`
  WHERE
    batch_id = 'SQL.PARAM.AIRFLOW_RUN_ID'
), exempted_exploded AS (
  SELECT
    chargeback.chargebackid,
    chargeback.lastupdatedtime,
    'VendorChargebackExempted' AS event_name,
    chargeback.vendorchargebackexempteddetails AS exempted,
    chargeback.batch_id
  FROM
    chargeback
  WHERE
    chargeback.vendorchargebackexempteddetails IS NOT NULL
), issued_exploded AS (
  SELECT
    chargeback.chargebackid,
    chargeback.lastupdatedtime,
    'VendorChargebackIssued' AS event_name,
    chargeback.vendorchargebackissueddetails AS issued,
    chargeback.batch_id
  FROM
    chargeback
  WHERE
    chargeback.vendorchargebackissueddetails IS NOT NULL
), reversed_exploded AS (
  SELECT
    chargeback.chargebackid,
    chargeback.lastupdatedtime,
    'VendorChargebackReversed' AS event_name,
    details AS reversed,
    chargeback.batch_id
  FROM
    chargeback,
    UNNEST(chargeback.vendorchargebackreverseddetails) AS details
  WHERE
    details IS NOT NULL
), chargeback_events_temp AS (
  SELECT
    exempted_exploded.chargebackid AS chargeback_id,
    TIMESTAMP(exempted_exploded.lastupdatedtime) AS last_updated_time,
    exempted_exploded.event_name AS event_name,
    CAST(TIMESTAMP(exempted.eventtime) AS DATE) AS event_date_pacific,
    TIMESTAMP(exempted.eventtime) AS event_time,
    exempted.vendorid AS vendor_id,
    exempted.rmspurchaseorderid AS rms_purchase_order_id,
    exempted.invoicenumber AS invoice_number,
    exempted.inboundshipmentnumber AS inbound_shipment_number,
    exempted.vendorbilloflading AS vendor_bill_of_lading,
    exempted.amount.currencycode AS amount_currency_code,
    exempted.amount.units AS amount_units,
    CAST(exempted.amount.nanos AS STRING) AS amount_nanos, -- FIXED
    exempted.units AS units,
    exempted.violationtype AS violation_type,
    exempted.exemptionreason AS exemption_reason,
    exempted_exploded.batch_id AS dw_batch_id
  FROM
    exempted_exploded
  UNION ALL
  SELECT
    issued_exploded.chargebackid AS chargeback_id,
    TIMESTAMP(issued_exploded.lastupdatedtime) AS last_updated_time,
    issued_exploded.event_name AS event_name,
    CAST(TIMESTAMP(issued.eventtime) AS DATE) AS event_date_pacific,
    TIMESTAMP(issued.eventtime) AS event_time,
    issued.vendorid AS vendor_id,
    issued.rmspurchaseorderid AS rms_purchase_order_id,
    issued.invoicenumber AS invoice_number,
    issued.inboundshipmentnumber AS inbound_shipment_number,
    issued.vendorbilloflading AS vendor_bill_of_lading,
    issued.amount.currencycode AS amount_currency_code,
    issued.amount.units AS amount_units,
    CAST(issued.amount.nanos AS STRING) AS amount_nanos, -- FIXED
    issued.units AS units,
    issued.violationtype AS violation_type,
    NULL AS exemption_reason,
    issued_exploded.batch_id AS dw_batch_id
  FROM
    issued_exploded
  UNION ALL
  SELECT
    reversed_exploded.chargebackid AS chargeback_id,
    TIMESTAMP(reversed_exploded.lastupdatedtime) AS last_updated_time,
    reversed_exploded.event_name AS event_name,
    CAST(TIMESTAMP(reversed_exploded.reversed.eventtime) AS DATE) AS event_date_pacific,
    TIMESTAMP(reversed_exploded.reversed.eventtime) AS event_time,
    NULL AS vendor_id,
    NULL AS rms_purchase_order_id,
    NULL AS invoice_number,
    NULL AS inbound_shipment_number,
    NULL AS vendor_bill_of_lading,
    reversed_exploded.reversed.amount.currencycode AS amount_currency_code,
    reversed_exploded.reversed.amount.units AS amount_units,
    CAST(reversed_exploded.reversed.amount.nanos AS STRING) AS amount_nanos, -- FIXED
    NULL AS units,
    NULL AS violation_type,
    NULL AS exemption_reason,
    reversed_exploded.batch_id AS dw_batch_id
  FROM
    reversed_exploded
)
SELECT
  chargeback_id,
  last_updated_time,
  FORMAT('%+03d:00', CAST(EXTRACT(HOUR FROM CURRENT_DATETIME('America/Los_Angeles') - CURRENT_DATETIME('UTC')) AS INT64)) AS last_updated_time_tz,
  event_name,
  event_date_pacific,
  event_time,
  FORMAT('%+03d:00', CAST(EXTRACT(HOUR FROM CURRENT_DATETIME('America/Los_Angeles') - CURRENT_DATETIME('UTC')) AS INT64)) AS event_time_tz,
  vendor_id,
  rms_purchase_order_id,
  invoice_number,
  inbound_shipment_number,
  vendor_bill_of_lading,
  amount_currency_code,
  amount_units,
  amount_nanos,
  units,
  violation_type,
  exemption_reason,
  dw_batch_id
FROM
  chargeback_events_temp;
