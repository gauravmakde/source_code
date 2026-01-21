-- END TRANSACTION;

CREATE TEMPORARY TABLE IF NOT EXISTS payment_result_bin_ldg_temp
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_result_bin_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY transaction_date DESC)) = 1;


-- END TRANSACTION;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_result_bin_fact AS prb
USING payment_result_bin_ldg_temp AS src
ON LOWER(src.id) = LOWER(prb.id)
WHEN MATCHED THEN UPDATE SET
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
 acquirer = src.acquirer,
 amount_currency_code = src.amount_currency_code,
 amount = src.amount,
 bin = src.bin,
 event_source = src.event_source,
 tender_type = src.tender_type,
 approval_status = src.approval_status,
 channel_country = src.channel_country,
 channel_brand = src.channel_brand,
 selling_channel = src.selling_channel,
 transaction_date = src.transaction_date,
 store_number = src.store_number,
 entry_mode = src.entry_mode,
 bank_card_credit_response_code = src.bank_card_credit_response_code,
 bank_card_debit_response_code = src.bank_card_debit_response_code,
 vendor_activity_date = src.vendor_activity_date,
 vendor_chargeback_id = src.id,
 chargeback_category = src.chargeback_category,
 chargeback_response_code = src.chargeback_response_code,
 chargeback_state = src.chargeback_state,
 interchange_amount_currency_code = src.interchange_amount_currency_code,
 interchange_amount = src.interchange_amount,
 interchange_code = src.interchange_code,
 interchange_adjustment_amount_currency_code = src.interchange_adjustment_amount_currency_code,
 interchange_adjustment_amount = src.interchange_adjustment_amount,
 interchange_adjustment_reason = src.interchange_adjustment_reason,
 card_type = src.card_type,
 card_subtype = src.card_subtype,
 bank_card_authorization_type = src.bank_card_authorization_type,
 settlement_transaction_type = src.settlement_transaction_type,
 merchant_category_code = src.merchant_category_code,
 authorization_code = src.authorization_code,
 visa_authorization_response_code = src.visa_authorization_response_code,
 authorization_source = src.authorization_source,
 visa_validation_code = src.visa_validation_code,
 credit_response_category = src.credit_response_category,
 debit_response_category = src.debit_response_category,
 vendor_authorization_system = src.vendor_authorization_system,
 authorization_service_indicator = src.authorization_service_indicator,
 mail_phone_indicator = src.mail_phone_indicator,
 address_verification_response_code = src.address_verification_response_code,
 cvv2_response_indicator = src.cvv2_response_indicator,
 cvv2_presence_indicator = src.cvv2_presence_indicator,
 card_product_type = src.card_product_type
WHEN NOT MATCHED THEN INSERT VALUES(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 , src.id, src.acquirer, src.amount_currency_code, src.amount, src.bin, src.event_source, src.tender_type, src.approval_status
 , src.channel_country, src.channel_brand, src.selling_channel, src.transaction_date, src.store_number, src.entry_mode,
 src.bank_card_credit_response_code, src.bank_card_debit_response_code, src.vendor_activity_date, src.vendor_chargeback_id
 , src.chargeback_category, src.chargeback_response_code, src.chargeback_state, src.interchange_amount_currency_code,
 src.interchange_amount, src.interchange_code, src.interchange_adjustment_amount_currency_code, src.interchange_adjustment_amount
 , src.interchange_adjustment_reason, src.card_type, src.card_subtype, src.bank_card_authorization_type, src.settlement_transaction_type
 , src.merchant_category_code, src.authorization_code, src.visa_authorization_response_code, src.authorization_source,
 src.visa_validation_code, src.credit_response_category, src.debit_response_category, src.vendor_authorization_system,
 src.authorization_service_indicator, src.mail_phone_indicator, src.address_verification_response_code, src.cvv2_response_indicator
 , src.cvv2_presence_indicator, src.card_product_type);


-- Remove records from temporary tables


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_result_bin_ldg;


-- END TRANSACTION;
