CREATE TEMPORARY TABLE IF NOT EXISTS temp_payment_settlement_result_details_stg
---CLUSTER BY settlement_result_received_id
AS
SELECT DISTINCT par.dw_sys_load_tmstp,
`{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(par.dw_sys_load_tmstp AS STRING)) as dw_sys_load_tmstp_tz,
 par.dw_sys_updt_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(par.dw_sys_updt_tmstp AS STRING)) as dw_sys_updt_tmstp_tz,
 par.channel_country,
 par.channel_brand,
 par.selling_channel,
 par.acquirer,
 par.settlement_amount_currency_code,
 par.settlement_amount,
 par.settlement_result_received_id,
 par.store_number,
 par.tender_type,
 par.processing_date,
 par.transaction_timestamp,
par.transaction_timestamp_tz,
 par.transaction_time_zone_offset_minutes,
 par.transaction_type,
 pbc.acquirer_merchant_identifier,
 pbc.bank_card_authorization_source,
 pbc.authorization_code,
 pbc.vendor_settlement_code,
 pbc.expiration_date_month,
 pbc.expiration_date_year,
 pbc.interchange_code,
 pbc.interchange_amount_currency_code,
 pbc.interchange_amount,
 pbc.merchant_category_code,
 pbc.entry_mode,
 pbc.token_value,
 pbc.token_authority,
 pbc.token_data_classification,
 pbc.token_type,
 pbc.transaction_match_identifier,
 pbc.card_type,
 pbc.card_subtype,
 pbc.token_requestor_id,
 pbc.card_product_type,
 pbc.interchange_adjustment_amount_currency_code,
 pbc.interchange_adjustment_amount,
 pbc.interchange_adjustment_reason,
 pbc.emv_transaction_indicator,
 pbc.network_reference_number,
 pbc.visa_transaction_id,
 pbc.mastercard_bank_net_reference_number,
 pbc.terminal_id,
 'N' AS isafterpay,
 SUBSTR('N/A', 1, 128) AS bin_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_settlement_result_fact AS par
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_bank_card_settlement_result_fact AS pbc ON LOWER(par.settlement_result_received_id
   ) = LOWER(pbc.settlement_result_received_id)
WHERE CAST(par.dw_sys_updt_tmstp AS DATE) > (SELECT COALESCE(DATE_SUB(MAX(CAST(dw_sys_updt_tmstp AS DATE)), INTERVAL 2
      DAY), DATE '2023-11-01') AS dw_sys_updt_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_settlement_result_details_fact);





CREATE TEMPORARY TABLE IF NOT EXISTS payment_result_bin_ldg_temp
---CLUSTER BY id
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.payment_result_bin_fact;





UPDATE temp_payment_settlement_result_details_stg SET
 isafterpay = 'Y',
 bin_type = SUBSTR('In store', 1, 3) FROM (SELECT DISTINCT a.settlement_result_received_id
  FROM temp_payment_settlement_result_details_stg AS a
   INNER JOIN payment_result_bin_ldg_temp AS b ON a.settlement_amount = b.amount AND LOWER(a.authorization_code) = LOWER(b
          .authorization_code) AND LOWER(a.store_number) = LOWER(b.store_number) AND LOWER(a.tender_type) = LOWER(b.tender_type
        ) AND LOWER(a.card_type) = LOWER(b.card_type) AND CAST(a.transaction_timestamp AS DATE) = b.transaction_date
  WHERE LOWER(b.bin) = LOWER('427140')) AS C
WHERE LOWER(temp_payment_settlement_result_details_stg.settlement_result_received_id) = LOWER(C.settlement_result_received_id
  );





UPDATE temp_payment_settlement_result_details_stg SET
 isafterpay = 'Y',
 bin_type = SUBSTR('Online virtual card', 1, 3) FROM (SELECT DISTINCT a.settlement_result_received_id
  FROM temp_payment_settlement_result_details_stg AS a
   INNER JOIN payment_result_bin_ldg_temp AS b ON a.settlement_amount = b.amount AND LOWER(a.authorization_code) = LOWER(b
          .authorization_code) AND LOWER(a.store_number) = LOWER(b.store_number) AND LOWER(a.tender_type) = LOWER(b.tender_type
        ) AND LOWER(a.card_type) = LOWER(b.card_type) AND CAST(a.transaction_timestamp AS DATE) = b.transaction_date
  WHERE LOWER(b.bin) = LOWER('411361')) AS C
WHERE LOWER(temp_payment_settlement_result_details_stg.settlement_result_received_id) = LOWER(C.settlement_result_received_id
  );





UPDATE temp_payment_settlement_result_details_stg SET
 isafterpay = 'Y',
 bin_type = SUBSTR('Long term installments', 1, 3) FROM (SELECT DISTINCT a.settlement_result_received_id
  FROM temp_payment_settlement_result_details_stg AS a
   INNER JOIN payment_result_bin_ldg_temp AS b ON a.settlement_amount = b.amount AND LOWER(a.authorization_code) = LOWER(b
          .authorization_code) AND LOWER(a.store_number) = LOWER(b.store_number) AND LOWER(a.tender_type) = LOWER(b.tender_type
        ) AND LOWER(a.card_type) = LOWER(b.card_type) AND CAST(a.transaction_timestamp AS DATE) = b.transaction_date
  WHERE LOWER(b.bin) = LOWER('486696')) AS C
WHERE LOWER(temp_payment_settlement_result_details_stg.settlement_result_received_id) = LOWER(C.settlement_result_received_id
  );


--Delete if exist


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_settlement_result_details_fact
WHERE settlement_result_received_id IN (SELECT settlement_result_received_id
  FROM temp_payment_settlement_result_details_stg);


-- Insert into FACT table


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_settlement_result_details_fact (dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz,
 settlement_result_received_id, channel_country, channel_brand, selling_channel, acquirer,
 settlement_amount_currency_code, settlement_amount, store_number, processing_date, transaction_timestamp,transaction_timestamp_tz,
 transaction_time_zone_offset_minutes, transaction_type, tender_type, acquirer_merchant_identifier,
 bank_card_authorization_source, authorization_code, vendor_settlement_code, expiration_date_month, expiration_date_year
 , interchange_code, interchange_amount_currency_code, interchange_amount, merchant_category_code, entry_mode,
 token_value, token_authority, token_data_classification, token_type, transaction_match_identifier, card_type,
 card_subtype, token_requestor_id, card_product_type, interchange_adjustment_amount_currency_code,
 interchange_adjustment_amount, interchange_adjustment_reason, emv_transaction_indicator, network_reference_number,
 visa_transaction_id, mastercard_bank_net_reference_number, terminal_id, isafterpay, bin_type)
(SELECT CAST(dw_sys_load_tmstp AS TIMESTAMP) AS dw_sys_load_tmstp,
dw_sys_load_tmstp_tz,
  CAST(dw_sys_updt_tmstp AS TIMESTAMP) AS dw_sys_updt_tmstp,
  dw_sys_updt_tmstp_tz,
  settlement_result_received_id,
  channel_country,
  channel_brand,
  selling_channel,
  acquirer,
  settlement_amount_currency_code,
  settlement_amount,
  store_number,
  processing_date,
  cast(transaction_timestamp as timestamp),
  transaction_timestamp_tz,
  transaction_time_zone_offset_minutes,
  transaction_type,
  tender_type,
  acquirer_merchant_identifier,
  bank_card_authorization_source,
  authorization_code,
  vendor_settlement_code,
  expiration_date_month,
  expiration_date_year,
  interchange_code,
  interchange_amount_currency_code,
  interchange_amount,
  merchant_category_code,
  entry_mode,
  token_value,
  token_authority,
  token_data_classification,
  token_type,
  transaction_match_identifier,
  card_type,
  card_subtype,
  token_requestor_id,
  card_product_type,
  interchange_adjustment_amount_currency_code,
  interchange_adjustment_amount,
  interchange_adjustment_reason,
  emv_transaction_indicator,
  network_reference_number,
  visa_transaction_id,
  mastercard_bank_net_reference_number,
  terminal_id,
  isafterpay,
  bin_type
 FROM temp_payment_settlement_result_details_stg);
 
 