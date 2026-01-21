-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

CREATE TEMPORARY VIEW temp_object_model AS
SELECT *
FROM kafka_customer_payment_authorization_result_analytical_avro;

CREATE TEMPORARY VIEW temp_kafka_customer_payment_authorization_result_analytical_avro AS
SELECT
    lastupdatedtime AS last_updated_time,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    acquirer AS acquirer,
    authorizationamount.currencycode AS authorization_amount_currency_code,
    cast(
        cast(authorizationamount.units AS INTEGER) + (authorizationamount.nanos / 1000000000) AS DECIMAL (38, 9)
    ) AS authorization_amount,
    authorizationresultid AS authorization_result_id,
    storenumber AS store_number,
    processingdate AS processing_date,
    tendertype AS tender_type,
    transactiontimestamp.timestamp AS transaction_timestamp,
    transactiontimestamp.timezoneoffsetminutes AS transaction_time_zone_offset_minutes,
    bankcardauthorizationresult.acquirermerchantidentifier AS acquirer_merchant_identifier,
    bankcardauthorizationresult.authorizationtype AS authorization_type,
    bankcardauthorizationresult.cardtypeinfo.cardtype AS card_type,
    bankcardauthorizationresult.cardtypeinfo.cardsubtype AS card_subtype,
    bankcardauthorizationresult.entrymode AS entry_mode,
    bankcardauthorizationresult.merchantcategorycode AS merchant_category_code,
    bankcardauthorizationresult.bankcardtoken.token.value AS token_value,
    bankcardauthorizationresult.bankcardtoken.token.authority AS token_authority,
    bankcardauthorizationresult.bankcardtoken.token.dataclassification AS token_data_classification,
    bankcardauthorizationresult.bankcardtoken.tokentype AS token_type,
    bankcardauthorizationresult.authorizationsource AS bank_card_authorization_source,
    bankcardauthorizationresult.expirationdate.month AS expiration_date_month,
    bankcardauthorizationresult.expirationdate.year AS expiration_date_year,
    bankcardauthorizationresult.authorizationcode AS authorization_code,
    bankcardauthorizationresult.creditresponsecode AS credit_response_code,
    bankcardauthorizationresult.creditresponsecategory AS credit_response_category,
    bankcardauthorizationresult.addressverificationresponsecode AS address_verification_response_code,
    bankcardauthorizationresult.authorizationserviceindicator AS authorization_service_indicator,
    bankcardauthorizationresult.cvv2presenceindicator AS cvv_two_presence_indicator,
    bankcardauthorizationresult.cvv2responseindicator AS cvv_two_response_indicator,
    bankcardauthorizationresult.mailphoneindicator AS mail_phone_indicator,
    bankcardauthorizationresult.vendorauthorizationsystem AS vendor_authorization_system,
    bankcardauthorizationresult.mastercardtransactionnetworkdetails.mastercardbanknetreferencenumber
    AS mastercard_banknet_reference_number,
    bankcardauthorizationresult.mastercardtransactionnetworkdetails.mastercardbanknetsettlementdate
    AS mastercard_banknet_settlement_date,
    bankcardauthorizationresult.visatransactionnetworkdetails.visatransactionid AS visa_transaction_id,
    bankcardauthorizationresult.visatransactionnetworkdetails.visavalidationcode AS visa_validation_code,
    bankcardauthorizationresult.visatransactionnetworkdetails.visaauthorizationresponsecode
    AS visa_authorization_response_code,
    bankcardauthorizationresult.debitresponsecode AS debit_response_code,
    bankcardauthorizationresult.debitresponsecategory AS debit_response_category,
    element_at(headers, 'Id') AS hdr_id,
    element_at(headers, 'AppId') AS hdr_appid,
    element_at(headers, 'EventTime') AS hdr_eventtime,
    element_at(headers, 'SystemTime') AS hdr_systemtime
FROM temp_object_model;

--Sink
INSERT INTO TABLE payment_authorization_result_stg
SELECT
    hdr_id,
    hdr_appid,
    hdr_eventtime,
    hdr_systemtime,
    last_updated_time,
    channel_country,
    channel_brand,
    selling_channel,
    acquirer,
    authorization_amount_currency_code,
    authorization_amount,
    authorization_result_id,
    store_number,
    processing_date,
    tender_type,
    transaction_timestamp,
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
    debit_response_category
FROM temp_kafka_customer_payment_authorization_result_analytical_avro;
