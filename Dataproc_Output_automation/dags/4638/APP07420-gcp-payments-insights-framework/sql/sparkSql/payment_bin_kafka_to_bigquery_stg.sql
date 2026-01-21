-- Read Kafka into temp table

CREATE TEMPORARY VIEW temp_object_model AS
SELECT *
FROM kafka_payment_result_bin_analytical_avro;

-- Transform in temp view
CREATE TEMPORARY VIEW temp_kafka_payment_result_bin_analytical_avro AS
SELECT
    id AS id,
    acquirer AS acquirer,
    amount.currencycode AS amount_currency_code,
    cast(
        cast(amount.units AS INTEGER) + (amount.nanos / 1000000000) AS DECIMAL (38, 9)
    ) AS amount,
    bin AS bin,
    eventsource AS event_source,
    tendertype AS tender_type,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    transactiondate AS transaction_date,
    storenumber AS store_number,
    entrymode AS entry_mode,
    cardtype.cardtype AS card_type,
    cardtype.cardsubtype AS card_subtype,
    authorizationdetails.bankcardcreditresponsecode AS bank_card_credit_response_code,
    authorizationdetails.bankcarddebitresponsecode AS bank_card_debit_response_code,
    authorizationdetails.bankcardauthorizationtype AS bank_card_authorization_type,
    authorizationdetails.creditresponsecategory AS credit_response_category,
    authorizationdetails.debitresponsecategory AS debit_response_category,
    authorizationdetails.vendorauthorizationsystem AS vendor_authorization_system,
    authorizationdetails.authorizationserviceindicator AS authorization_service_indicator,
    authorizationdetails.mailphoneindicator AS mail_phone_indicator,
    authorizationdetails.addressverificationresponsecode AS address_verification_response_code,
    authorizationdetails.cvv2responseindicator AS cvv2_response_indicator,
    authorizationdetails.cvv2presenceindicator AS cvv2_presence_indicator,
    chargebackdetails.vendoractivitydate AS vendor_activity_date,
    chargebackdetails.vendorchargebackid AS vendor_chargeback_id,
    chargebackdetails.chargebackcategory AS chargeback_category,
    chargebackdetails.chargebackreasoncode AS chargeback_response_code,
    chargebackdetails.chargebackstate AS chargeback_state,
    settlementdetails.interchangeamount.currencycode AS interchange_amount_currency_code,
    cast(
        cast(
            settlementdetails.interchangeamount.units AS INTEGER
        ) + (settlementdetails.interchangeamount.nanos / 1000000000) AS DECIMAL (38, 9)
    ) AS interchange_amount,
    settlementdetails.interchangecode AS interchange_code,
    settlementdetails.interchangeadjustmentamount.currencycode AS interchange_adjustment_amount_currency_code,
    cast(
        cast(
            settlementdetails.interchangeadjustmentamount.units AS INTEGER
        ) + (settlementdetails.interchangeadjustmentamount.nanos / 1000000000) AS DECIMAL (38, 9)
    ) AS interchange_adjustment_amount,
    settlementdetails.interchangeadjustmentreason AS interchange_adjustment_reason,
    settlementdetails.settlementtransactiontype AS settlement_transaction_type,
    settlementdetails.cardproducttype AS card_product_type,
    CASE
        WHEN (authorizationdetails IS NOT NULL AND authorizationdetails.merchantcategorycode IS NOT NULL)
            THEN authorizationdetails.merchantcategorycode
        WHEN (settlementdetails IS NOT NULL AND settlementdetails.merchantcategorycode IS NOT NULL)
            THEN settlementdetails.merchantcategorycode
        ELSE ''
    END AS merchant_category_code,
    CASE
        WHEN (authorizationdetails IS NOT NULL AND authorizationdetails.authorizationcode IS NOT NULL)
            THEN authorizationdetails.authorizationcode
        WHEN (settlementdetails IS NOT NULL AND settlementdetails.authorizationcode IS NOT NULL)
            THEN settlementdetails.authorizationcode
        ELSE ''
    END AS authorization_code,
    CASE
        WHEN (authorizationdetails IS NOT NULL AND authorizationdetails.visaauthorizationresponsecode IS NOT NULL)
            THEN authorizationdetails.visaauthorizationresponsecode
        WHEN (settlementdetails IS NOT NULL AND settlementdetails.visaauthorizationresponsecode IS NOT NULL)
            THEN settlementdetails.visaauthorizationresponsecode
        ELSE ''
    END AS visa_authorization_response_code,
    CASE
        WHEN (authorizationdetails IS NOT NULL AND authorizationdetails.authorizationsource IS NOT NULL)
            THEN authorizationdetails.authorizationsource
        WHEN (settlementdetails IS NOT NULL AND settlementdetails.authorizationsource IS NOT NULL)
            THEN settlementdetails.authorizationsource
        ELSE ''
    END AS authorization_source,
    CASE
        WHEN (authorizationdetails IS NOT NULL AND authorizationdetails.visavalidationcode IS NOT NULL)
            THEN authorizationdetails.visavalidationcode
        WHEN (settlementdetails IS NOT NULL AND settlementdetails.visavalidationcode IS NOT NULL)
            THEN settlementdetails.visavalidationcode
        ELSE ''
    END AS visa_validation_code,
    element_at(
        headers, 'Id'
    ) AS hdr_id,
    element_at(
        headers, 'AppId'
    ) AS hdr_appid,
    element_at(
        headers, 'EventTime'
    ) AS hdr_eventtime,
    element_at(
        headers, 'SystemTime'
    ) AS hdr_systemtime,
    CASE
        WHEN (upper(isapproved) = 'TRUE') THEN 'APPROVED'
        WHEN (upper(isapproved) = 'FALSE') THEN 'DECLINED'
        WHEN (upper(isapproved) = 'NOT_SET') THEN 'NOT_SET'
    END AS approval_status
FROM temp_object_model;

-- Sink to Teradata
INSERT INTO TABLE payment_result_bin_ldg
SELECT
    hdr_id,
    hdr_appid,
    hdr_eventtime,
    hdr_systemtime,
    id,
    acquirer,
    amount_currency_code,
    amount,
    bin,
    event_source,
    tender_type,
    approval_status,
    channel_country,
    channel_brand,
    selling_channel,
    transaction_date,
    store_number,
    entry_mode,
    bank_card_credit_response_code,
    bank_card_debit_response_code,
    vendor_activity_date,
    vendor_chargeback_id,
    chargeback_category,
    chargeback_response_code,
    chargeback_state,
    interchange_amount_currency_code,
    interchange_amount,
    interchange_code,
    interchange_adjustment_amount_currency_code,
    interchange_adjustment_amount,
    interchange_adjustment_reason,
    card_type,
    card_subtype,
    bank_card_authorization_type,
    settlement_transaction_type,
    merchant_category_code,
    authorization_code,
    visa_authorization_response_code,
    authorization_source,
    visa_validation_code,
    credit_response_category,
    debit_response_category,
    vendor_authorization_system,
    authorization_service_indicator,
    mail_phone_indicator,
    address_verification_response_code,
    cvv2_response_indicator,
    cvv2_presence_indicator,
    card_product_type
FROM temp_kafka_payment_result_bin_analytical_avro;
