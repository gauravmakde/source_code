-- mandatory Query Band part
-- SET QUERY_BAND = 'App_ID=app07420; DAG_ID=payment_auth_result_teradata;Task_Name=teradata_stg_to_teradata_fct_job;' -- noqa
-- FOR SESSION VOLATILE;



-- Create temporary tables for selecting distinct records by 'settlementID'.



CREATE TEMPORARY TABLE IF NOT EXISTS temp_payment_authorization_result_stg
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_authorization_result_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY authorization_result_id ORDER BY transaction_timestamp DESC)) = 1;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_result_fact AS psr
USING temp_payment_authorization_result_stg AS src
ON LOWER(src.authorization_result_id) = LOWER(psr.authorization_result_id) 
AND CAST(CONCAT(src.transaction_timestamp, '+00:00') AS TIMESTAMP)
  = psr.transaction_timestamp
WHEN MATCHED THEN UPDATE SET
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
 channel_country = src.channel_country,
 channel_brand = src.channel_brand,
 selling_channel = src.selling_channel,
 acquirer = src.acquirer,
 authorization_amount_currency_code = src.authorization_amount_currency_code,
 authorization_amount = src.authorization_amount,
 store_number = src.store_number,
 processing_date = src.processing_date,
 tender_type = src.tender_type,
 transaction_time_zone_offset_minutes = src.transaction_time_zone_offset_minutes
WHEN NOT MATCHED THEN INSERT (dw_sys_load_tmstp, dw_sys_updt_tmstp, channel_country, channel_brand, selling_channel,
 acquirer, authorization_amount_currency_code, authorization_amount, authorization_result_id, store_number, tender_type
 , processing_date, transaction_timestamp,transaction_timestamp_tz, transaction_time_zone_offset_minutes) 
 
 VALUES(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 , CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), src.channel_country, src.channel_brand, src.selling_channel
 , src.acquirer, src.authorization_amount_currency_code, src.authorization_amount, src.authorization_result_id, src.store_number
 , src.tender_type, src.processing_date,
  CAST(CONCAT(src.transaction_timestamp, '+00:00') AS TIMESTAMP), 
  ('+00:00') , src.transaction_time_zone_offset_minutes);


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_bank_card_authorization_result_fact AS pbc
USING temp_payment_authorization_result_stg AS src
ON LOWER(src.authorization_result_id) = LOWER(pbc.authorization_result_id)
WHEN MATCHED THEN UPDATE SET
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
 acquirer_merchant_identifier = src.acquirer,
 authorization_type = src.authorization_type,
 card_type = src.card_type,
 card_subtype = src.card_subtype,
 entry_mode = src.entry_mode,
 merchant_category_code = src.merchant_category_code,
 token_value = src.token_value,
 token_authority = src.token_authority,
 token_data_classification = src.token_data_classification,
 token_type = src.token_type,
 bank_card_authorization_source = src.bank_card_authorization_source,
 expiration_date_month = src.expiration_date_month,
 expiration_date_year = CAST(CAST(CAST(src.expiration_date_year AS STRING) AS FLOAT64) AS SMALLINT),
 authorization_code = src.authorization_code,
 credit_response_code = src.credit_response_code,
 credit_response_category = src.credit_response_category,
 address_verification_response_code = src.address_verification_response_code,
 authorization_service_indicator = src.authorization_service_indicator,
 cvv_two_presence_indicator = src.cvv_two_presence_indicator,
 cvv_two_response_indicator = src.cvv_two_response_indicator,
 mail_phone_indicator = src.mail_phone_indicator,
 vendor_authorization_system = src.vendor_authorization_system,
 mastercard_banknet_reference_number = src.mastercard_banknet_reference_number,
 mastercard_banknet_settlement_date = src.mastercard_banknet_settlement_date,
 visa_transaction_id = src.visa_transaction_id,
 visa_validation_code = src.visa_validation_code,
 visa_authorization_response_code = src.visa_authorization_response_code,
 debit_response_code = src.debit_response_code,
 debit_response_category = src.debit_response_category
WHEN NOT MATCHED THEN INSERT VALUES(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 , src.authorization_result_id, src.acquirer_merchant_identifier, src.authorization_type, src.card_type, src.card_subtype
 , src.entry_mode, src.merchant_category_code, src.token_value, src.token_authority, src.token_data_classification, src
 .token_type, src.bank_card_authorization_source, src.expiration_date_month, CAST(CAST(CAST(src.expiration_date_year AS STRING) AS FLOAT64)
  AS SMALLINT), src.authorization_code, src.credit_response_code, src.credit_response_category, src.address_verification_response_code
 , src.authorization_service_indicator, src.cvv_two_presence_indicator, src.cvv_two_response_indicator, src.mail_phone_indicator
 , src.vendor_authorization_system, src.mastercard_banknet_reference_number, src.mastercard_banknet_settlement_date, src
 .visa_transaction_id, src.visa_validation_code, src.visa_authorization_response_code, src.debit_response_code, src.debit_response_category
 );


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_result_fact SET
 payment_id_authorization = TO_HEX(SHA256(CONCAT(COALESCE(TRIM(C.authorization_code), ''), COALESCE(TRIM(C.token_value),
    ''), COALESCE(TRIM(SUBSTR(FORMAT('%.2f', ROUND(CAST(C.authorization_amount AS NUMERIC), 2)), 1, 50)), '')))) FROM (SELECT
   a.authorization_result_id,
   b.authorization_code,
   b.token_value,
   a.authorization_amount
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_authorization_result_fact AS a
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_bank_card_authorization_result_fact AS b ON LOWER(a.authorization_result_id) =
    LOWER(b.authorization_result_id)
  WHERE a.authorization_result_id IN (SELECT DISTINCT authorization_result_id
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_authorization_result_ldg)) AS C
WHERE LOWER(payment_authorization_result_fact.authorization_result_id) = LOWER(C.authorization_result_id);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_result_fact SET
 payment_id_settlement_result = TO_HEX(SHA256(CONCAT(COALESCE(TRIM(t2.tender_type), ''), COALESCE(TRIM(t2.store_number),
    ''), COALESCE(TRIM(t2.token_value), ''), COALESCE(TRIM(t2.acquirer_merchant_identifier), ''),COALESCE(TRIM(FORMAT_TIMESTAMP('%F %H:%M:%S', t2.transaction_timestamp)), ''), COALESCE(TRIM(t2.card_type
     ), '')))) FROM (SELECT a.authorization_result_id,
   a.tender_type,
   a.store_number,
   b.token_value,
   b.acquirer_merchant_identifier,
   a.transaction_timestamp,
   b.card_type
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_authorization_result_fact AS a
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_bank_card_authorization_result_fact AS b ON LOWER(a.authorization_result_id) =
    LOWER(b.authorization_result_id)
  WHERE a.authorization_result_id IN (SELECT DISTINCT authorization_result_id
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_authorization_result_ldg)) AS t2
WHERE LOWER(payment_authorization_result_fact.authorization_result_id) = LOWER(t2.authorization_result_id);

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_authorization_result_ldg;