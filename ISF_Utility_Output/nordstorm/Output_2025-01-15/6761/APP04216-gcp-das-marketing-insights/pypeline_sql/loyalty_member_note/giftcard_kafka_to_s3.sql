-- Prepare final table from kafka source
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    accountNumber.value AS accountNumber,
    accountNumber.dataClassification AS accountNumber_dataClassification,
    accountNumber.authority AS accountNumber_authority,
    cardType,
    promotionCode,
    classId,
    alternateMerchantId,
    country,
    currencyCode,
    saleLocation,
    expirationDate,
    expirationType,
    deliveryMethod,
    customerEmail.value AS customerEmail,
    activationDate,
    activityReason,
    accountStatus,
    accountStatusDate,
    balance.currencyCode AS balance_currencyCode,
    balance.units AS balance_units,
    balance.nanos AS balance_nanos,
    balanceChangeDate,
    giftCardTransactionId,
    decode(headers.SystemTime,'UTF-8') AS SystemTime
FROM kafka_source;

-- Write result table
INSERT OVERWRITE giftcard_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE giftcard_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
