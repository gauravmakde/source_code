
CREATE TEMPORARY TABLE IF NOT EXISTS temp_payment_authorization_result_details_stg
---CLUSTER BY authorization_result_id
AS
SELECT DISTINCT par.dw_sys_load_tmstp,
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(par.dw_sys_load_tmstp AS STRING)) as dw_sys_load_tmstp_tz,
 par.dw_sys_updt_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(par.dw_sys_updt_tmstp AS STRING)) as dw_sys_updt_tmstp_tz,
 par.channel_country,
 par.channel_brand,
 par.selling_channel,
 par.acquirer,
 par.authorization_amount_currency_code,
 par.authorization_amount,
 par.authorization_result_id,
 par.store_number,
 par.tender_type,
 par.processing_date,
 par.transaction_timestamp,
par.transaction_timestamp_tz,
 par.transaction_time_zone_offset_minutes,
 pbc.acquirer_merchant_identifier,
 pbc.authorization_type,
 pbc.card_type,
 pbc.card_subtype,
 pbc.entry_mode,
 pbc.merchant_category_code,
 pbc.token_value,
 pbc.token_authority,
 pbc.token_data_classification,
 pbc.token_type,
 pbc.bank_card_authorization_source,
 pbc.expiration_date_month,
 pbc.expiration_date_year,
 pbc.authorization_code,
 pbc.credit_response_code,
 pbc.credit_response_category,
 pbc.address_verification_response_code,
 pbc.authorization_service_indicator,
 pbc.cvv_two_presence_indicator,
 pbc.cvv_two_response_indicator,
 pbc.mail_phone_indicator,
 pbc.vendor_authorization_system,
 pbc.mastercard_banknet_reference_number,
 pbc.mastercard_banknet_settlement_date,
 pbc.visa_transaction_id,
 pbc.visa_validation_code,
 pbc.visa_authorization_response_code,
 pbc.debit_response_code,
 pbc.debit_response_category,
 'N' AS isafterpay,
 SUBSTR('N/A', 1, 128) AS bin_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_authorization_result_fact AS par
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_bank_card_authorization_result_fact AS pbc ON LOWER(par.authorization_result_id) =
  LOWER(pbc.authorization_result_id)
WHERE CAST(par.dw_sys_updt_tmstp AS DATE) > (SELECT COALESCE(DATE_SUB(MAX(CAST(dw_sys_updt_tmstp AS DATE)), INTERVAL 2
      DAY), DATE '2023-11-01') AS dw_sys_updt_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_authorization_result_details_fact);
   
   

CREATE TEMPORARY TABLE IF NOT EXISTS  payment_result_bin_ldg_temp AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_result_bin_fact;


UPDATE temp_payment_authorization_result_details_stg
SET ISAFTERPAY = 'Y', 
    BIN_TYPE = 'In store'
WHERE AUTHORIZATION_RESULT_ID IN (
  SELECT DISTINCT A.AUTHORIZATION_RESULT_ID
  FROM temp_payment_authorization_result_details_stg AS A
  INNER JOIN payment_result_bin_ldg_temp AS B
    ON A.AUTHORIZATION_AMOUNT = B.AMOUNT
    AND A.AUTHORIZATION_CODE = B.AUTHORIZATION_CODE
    AND A.STORE_NUMBER = B.STORE_NUMBER
    AND A.TENDER_TYPE = B.TENDER_TYPE
    AND A.CARD_TYPE = B.CARD_TYPE
    AND DATE(TIMESTAMP(A.TRANSACTION_TIMESTAMP, 'America/Los_Angeles')) = B.TRANSACTION_DATE
  WHERE B.BIN = '427140'
);


UPDATE temp_payment_authorization_result_details_stg
SET ISAFTERPAY = 'Y', 
    BIN_TYPE = 'Online virtual card'
WHERE AUTHORIZATION_RESULT_ID IN (
  SELECT DISTINCT A.AUTHORIZATION_RESULT_ID
  FROM temp_payment_authorization_result_details_stg AS A
  INNER JOIN payment_result_bin_ldg_temp AS B
    ON A.AUTHORIZATION_AMOUNT = B.AMOUNT
    AND A.AUTHORIZATION_CODE = B.AUTHORIZATION_CODE
    AND A.STORE_NUMBER = B.STORE_NUMBER
    AND A.TENDER_TYPE = B.TENDER_TYPE
    AND A.CARD_TYPE = B.CARD_TYPE
    AND DATE(TIMESTAMP(A.TRANSACTION_TIMESTAMP, 'America/Los_Angeles')) = B.TRANSACTION_DATE
  WHERE B.BIN = '411361'
);


UPDATE temp_payment_authorization_result_details_stg
SET ISAFTERPAY = 'Y', 
    BIN_TYPE = 'Long term installments'
WHERE AUTHORIZATION_RESULT_ID IN (
  SELECT DISTINCT A.AUTHORIZATION_RESULT_ID
  FROM temp_payment_authorization_result_details_stg AS A
  INNER JOIN payment_result_bin_ldg_temp AS B
    ON A.AUTHORIZATION_AMOUNT = B.AMOUNT
    AND A.AUTHORIZATION_CODE = B.AUTHORIZATION_CODE
    AND A.STORE_NUMBER = B.STORE_NUMBER
    AND A.TENDER_TYPE = B.TENDER_TYPE
    AND A.CARD_TYPE = B.CARD_TYPE
    AND DATE(TIMESTAMP(A.TRANSACTION_TIMESTAMP, 'America/Los_Angeles')) = B.TRANSACTION_DATE
  WHERE B.BIN = '486696'
);




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_result_details_fact
WHERE authorization_result_id IN (SELECT authorization_result_id
        FROM temp_payment_authorization_result_details_stg);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_result_details_fact (dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz, channel_country
 , channel_brand, selling_channel, acquirer, authorization_amount_currency_code, authorization_amount,
 authorization_result_id, store_number, tender_type, processing_date, transaction_timestamp ,
 transaction_timestamp_tz,
 transaction_time_zone_offset_minutes, acquirer_merchant_identifier, authorization_type, card_type, card_subtype,
 entry_mode, merchant_category_code, token_value, token_authority, token_data_classification, token_type,
 bank_card_authorization_source, expiration_date_month, expiration_date_year, authorization_code, credit_response_code,
 credit_response_category, address_verification_response_code, authorization_service_indicator,
 cvv_two_presence_indicator, cvv_two_response_indicator, mail_phone_indicator, vendor_authorization_system,
 mastercard_banknet_reference_number, mastercard_banknet_settlement_date, visa_transaction_id, visa_validation_code,
 visa_authorization_response_code, debit_response_code, debit_response_category, isafterpay, bin_type)
(SELECT CAST(dw_sys_load_tmstp AS TIMESTAMP) AS dw_sys_load_tmstp,
dw_sys_load_tmstp_tz,
  CAST(dw_sys_updt_tmstp AS TIMESTAMP) AS dw_sys_updt_tmstp,
  dw_sys_updt_tmstp_tz,
  channel_country,
  channel_brand,
  selling_channel,
  acquirer,
  authorization_amount_currency_code,
  authorization_amount,
  authorization_result_id,
  store_number,
  tender_type,
  processing_date,
  cast(transaction_timestamp as timestamp) as transaction_timestamp,
  transaction_timestamp_tz,
  transaction_time_zone_offset_minutes,
  acquirer_merchant_identifier,
  authorization_type,
  card_type,
  card_subtype,
  entry_mode,
  merchant_category_code,
  token_value,
  token_authority,
  token_data_classification,
  token_type,
  bank_card_authorization_source,
  expiration_date_month,
  expiration_date_year,
  authorization_code,
  credit_response_code,
  credit_response_category,
  address_verification_response_code,
  authorization_service_indicator,
  cvv_two_presence_indicator,
  cvv_two_response_indicator,
  mail_phone_indicator,
  vendor_authorization_system,
  mastercard_banknet_reference_number,
  mastercard_banknet_settlement_date,
  visa_transaction_id,
  visa_validation_code,
  visa_authorization_response_code,
  debit_response_code,
  debit_response_category,
  isafterpay,
  bin_type
 FROM temp_payment_authorization_result_details_stg);

