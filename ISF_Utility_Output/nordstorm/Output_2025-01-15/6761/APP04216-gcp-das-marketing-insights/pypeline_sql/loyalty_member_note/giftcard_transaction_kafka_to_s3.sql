-- Prepare final table from kafka source
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    transactionId AS giftcard_transaction_id,
    transactionTime,
    transactionCode,
    merchantId,
    alternateMerchantId,
    accountNumber.value AS accountNumber_value,
    accountNumber.dataClassification AS accountNumber_dataClassification,
    accountNumber.authority AS accountNumber_authority,
    transactionAmount.currencyCode AS transactionAmount_currencyCode,
    transactionAmount.units AS transactionAmount_units,
    transactionAmount.nanos AS transactionAmount_nanos,
    lockedAmount.currencyCode AS lockedAmount_currencyCode,
    lockedAmount.units AS lockedAmount_units,
    lockedAmount.nanos AS lockedAmount_nanos,
    expirationDate,
    exchangeRate,
    pointOfSalePurchasedId.store,
    pointOfSalePurchasedId.register,
    pointOfSalePurchasedId.transaction,
    pointOfSalePurchasedId.businessDate,
    parentTransactionId,
    requestId,
    processorTransactionId,
    sourceCode,
    decode(headers.SystemTime,'UTF-8') AS SystemTime,
    channel.channelCountry AS channel_channel_country,
    channel.channelBrand AS channel_channel_brand,
    channel.sellingChannel AS channel_selling_channel
FROM kafka_source;

-- Write result table
INSERT OVERWRITE giftcard_transaction_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE giftcard_transaction_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
