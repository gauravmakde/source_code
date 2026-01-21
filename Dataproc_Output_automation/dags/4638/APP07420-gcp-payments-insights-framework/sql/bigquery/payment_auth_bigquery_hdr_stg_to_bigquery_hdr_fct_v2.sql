CREATE TEMPORARY TABLE IF NOT EXISTS payment_authorization_hdr_v2_ldg_temp
CLUSTER BY event_id, purchase_identifier_id
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_authorization_hdr_v2_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY event_id, purchase_identifier_id ORDER BY event_time DESC)) = 1;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_hdr_v2_fact
WHERE (event_id, purchase_identifier_id) IN (SELECT (event_id, purchase_identifier_id)
  FROM payment_authorization_hdr_v2_ldg_temp);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_hdr_v2_fact (event_id, event_time, activity_date, obj_created_tmstp,
 obj_last_updt_tmstp, channel_country, channel_brand, selling_channel, purchase_identifier_type, purchase_identifier_id
 , pos_store, pos_register, pos_transaction, pos_business_date, store_transaction_transaction_id,
 store_transaction_session_id, store_transaction_purchase_id, currency_code, total_amt, authorization_type,
 merchant_identifier, failure_reason, failure_source, failure_reason_code, failure_reason_message, dw_batch_date,
 dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT event_id,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(event_time AS DATETIME)) AS DATETIME) AS event_time,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(event_time AS DATETIME)) AS DATETIME) AS DATE) AS activity_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(obj_created_tmstp AS DATETIME)) AS DATETIME) AS obj_created_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(obj_last_updt_tmstp AS DATETIME)) AS DATETIME) AS obj_last_updt_tmstp,
  channel_country,
  channel_brand,
  selling_channel,
  purchase_identifier_type,
  purchase_identifier_id,
  pos_store,
  pos_register,
  pos_transaction,
  CAST(pos_business_date AS DATE) AS pos_business_date,
  store_transaction_transaction_id,
  store_transaction_session_id,
  store_transaction_purchase_id,
  currency_code,
  total_amt,
  authorization_type,
  merchant_identifier,
  failure_reason,
  failure_source,
  failure_reason_code,
  failure_reason_message,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM payment_authorization_hdr_v2_ldg_temp);