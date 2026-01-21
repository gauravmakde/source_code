-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=payment_refund_teradata;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
CREATE TEMPORARY VIEW temp_kafka_payment_refund_succeeded_v2 AS
SELECT * FROM kafka_payment_refund_succeeded_v2;

-- building the refund hdr table
--reading the refund success hdr fields
CREATE TEMPORARY VIEW refund_success_hdr_temp AS
SELECT
    ELEMENT_AT(headers, 'Id') AS event_id,
    ELEMENT_AT(headers, 'Type') AS event_type,
    ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-test') AS nord_test,
    ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-load') AS nord_load,
    CAST(eventTime AS TIMESTAMP) AS event_time,
    source.channelCountry AS channel_country,
    source.channel AS channel,
    source.platform AS platform,
    source.feature AS feature,
    source.serviceName AS service_name,
    source.store AS store,
    source.register AS register,
    purchaseId.type AS purchase_identifier_type,
    purchaseId.id AS purchase_identifier_id,
    CASE
        WHEN purchaseId.storeRegisterTransactionDate IS NOT NULL THEN purchaseId.storeRegisterTransactionDate.store
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseId.storeTransactionIdentifierDetail.purchaseId.store
    END AS pos_store,
    CASE
        WHEN purchaseId.storeRegisterTransactionDate IS NOT NULL THEN purchaseId.storeRegisterTransactionDate.register
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseId.storeTransactionIdentifierDetail.purchaseId.register
    END AS pos_register,
    CASE
        WHEN purchaseId.storeRegisterTransactionDate IS NOT NULL THEN purchaseId.storeRegisterTransactionDate.transaction
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseId.storeTransactionIdentifierDetail.purchaseId.transaction
    END AS pos_transaction,
    CASE
        WHEN purchaseId.storeRegisterTransactionDate IS NOT NULL THEN CAST(purchaseId.storeRegisterTransactionDate.businessDate AS DATE)
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL THEN CAST(purchaseId.storeTransactionIdentifierDetail.purchaseId.businessDate AS DATE)
    END AS pos_business_date,
    CASE
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseId.storeTransactionIdentifierDetail.transactionId
    END AS store_transaction_transaction_id,
    CASE
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseId.storeTransactionIdentifierDetail.sessionId
    END AS store_transaction_session_id,
    transactionId.id AS transaction_id,
    transactionId.type AS transaction_id_type,
    serviceTicketId AS service_ticket_id,
    reason AS reason,
    total.currencyCode AS currency_code,
    CAST(
        CAST(
            total.units AS INTEGER
        ) + (total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    merchantIdentifier AS merchant_identifier,
    '' AS failure_reason
FROM temp_kafka_payment_refund_succeeded_v2;

--inserting into the teradata hdr stg table
INSERT INTO TABLE payment_refund_success_hdr_stg
SELECT DISTINCT
    event_id,
    event_type,
    event_time,
    channel_country,
    channel,
    platform,
    feature,
    service_name,
    store,
    register,
    purchase_identifier_type,
    purchase_identifier_id,
    pos_store,
    pos_register,
    pos_transaction,
    pos_business_date,
    store_transaction_transaction_id,
    store_transaction_session_id,
    transaction_id,
    transaction_id_type,
    service_ticket_id,
    reason,
    currency_code,
    total_amt,
    merchant_identifier,
    failure_reason
FROM refund_success_hdr_temp
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
------------------------------------------------------------------

--building the refund tender table
--reading the refund success tender - bankCardRefundResultV2s
CREATE TEMPORARY VIEW refund_success_bankCardRefundV2 AS
WITH refund_success_bankCardRefundV2_exploded AS (
    SELECT
        ELEMENT_AT(headers, 'Id') AS event_id,
        ELEMENT_AT(headers, 'Type') AS event_type,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-test') AS nord_test,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-load') AS nord_load,
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'bankCardRefundResultV2s' AS tender_record_name,
        transactionId.id AS transaction_id,
        purchaseId.id AS purchase_identifier_id,
        EXPLODE(bankCardRefundResultV2s) AS bankCardRefundResultV2s
    FROM temp_kafka_payment_refund_succeeded_v2
)

SELECT
    event_id,
    event_type,
    nord_test,
    nord_load,
    event_time,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    bankCardRefundResultV2s.bankCardResult.cardTypeInfo.cardType AS card_type,
    bankCardRefundResultV2s.bankCardResult.cardTypeInfo.cardSubType AS card_sub_type,
    bankCardRefundResultV2s.bankCardResult.token.token.value AS tender_item_account_no,
    'bank_card_token_value' AS tender_item_account_value_type,
    bankCardRefundResultV2s.bankCardResult.token.token.authority AS token_authority,
    bankCardRefundResultV2s.bankCardResult.token.token.dataClassification AS token_data_classification,
    bankCardRefundResultV2s.bankCardResult.token.tokenType AS token_type,
    bankCardRefundResultV2s.bankCardResult.total.currencyCode AS currency_code,
    CAST(
        CAST(
            bankCardRefundResultV2s.bankCardResult.total.units AS INTEGER
        ) + (bankCardRefundResultV2s.bankCardResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    CASE
        WHEN bankCardRefundResultV2s.bankCardResult.transactionTime IS NOT NULL THEN CAST(bankCardRefundResultV2s.bankCardResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    bankCardRefundResultV2s.bankCardResult.transactionResult.resultType AS transaction_type,
    bankCardRefundResultV2s.bankCardResult.transactionResult.status AS transaction_status,
    bankCardRefundResultV2s.bankCardResult.transactionResult.failureReason AS transaction_failure_desc,
    bankCardRefundResultV2s.bankCardResult.tokenRequestorId AS token_requestor_id,
    bankCardRefundResultV2s.vendorSettlementCode AS vendor_settlement_code,
    'CREDIT_CARD' AS tender_type,
    NULL AS gift_card_note_type,
    NULL AS paypal_tndr_rslt_order_id,
    NULL AS paypal_tndr_rslt_payer_id,
    NULL AS paypal_stlmnt_rslt_id,
    NULL AS paypal_stlmnt_rslt_parent_ref_id,
    NULL AS paypal_stlmnt_rslt_fee_currency_code,
    NULL AS paypal_stlmnt_rslt_fee_amount,
    NULL AS paypal_stlmnt_rslt_net_amt_currency_code,
    NULL AS paypal_stlmnt_rslt_net_amt_amt,
    NULL AS paypal_billing_agreement_tndr_rslt_id_value,
    NULL AS paypal_tndr_rslt_payer_email_value,
    NULL AS paypal_tndr_rslt_payer_account_country,
    NULL AS note_refund_reason
FROM refund_success_bankCardRefundV2_exploded;


--reading the refund success tender - afterPayVirtualCardRefundResults
CREATE TEMPORARY VIEW refund_success_afterPayVirtualCard AS
WITH refund_success_afterPayVirtualCard_exploded AS (
    SELECT
        ELEMENT_AT(headers, 'Id') AS event_id,
        ELEMENT_AT(headers, 'Type') AS event_type,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-test') AS nord_test,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-load') AS nord_load,
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'afterPayVirtualCardRefundResults' AS tender_record_name,
        transactionId.id AS transaction_id,
        purchaseId.id AS purchase_identifier_id,
        EXPLODE(afterPayVirtualCardRefundResults) AS afterPayVirtualCardRefundResults
    FROM temp_kafka_payment_refund_succeeded_v2
)

SELECT
    event_id,
    event_type,
    nord_test,
    nord_load,
    event_time,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    afterPayVirtualCardRefundResults.bankCardResult.cardTypeInfo.cardType AS card_type,
    afterPayVirtualCardRefundResults.bankCardResult.cardTypeInfo.cardSubType AS card_sub_type,
    afterPayVirtualCardRefundResults.bankCardResult.token.token.value AS tender_item_account_no,
    'bank_card_token_value' AS tender_item_account_value_type,
    afterPayVirtualCardRefundResults.bankCardResult.token.token.authority AS token_authority,
    afterPayVirtualCardRefundResults.bankCardResult.token.token.dataClassification AS token_data_classification,
    afterPayVirtualCardRefundResults.bankCardResult.token.tokenType AS token_type,
    afterPayVirtualCardRefundResults.bankCardResult.total.currencyCode AS currency_code,
    CAST(
        CAST(
            afterPayVirtualCardRefundResults.bankCardResult.total.units AS INTEGER
        ) + (afterPayVirtualCardRefundResults.bankCardResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    CASE
        WHEN afterPayVirtualCardRefundResults.bankCardResult.transactionTime IS NOT NULL THEN CAST(afterPayVirtualCardRefundResults.bankCardResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    afterPayVirtualCardRefundResults.bankCardResult.transactionResult.resultType AS transaction_type,
    afterPayVirtualCardRefundResults.bankCardResult.transactionResult.status AS transaction_status,
    afterPayVirtualCardRefundResults.bankCardResult.transactionResult.failureReason AS transaction_failure_desc,
    afterPayVirtualCardRefundResults.bankCardResult.tokenRequestorId AS token_requestor_id,
    afterPayVirtualCardRefundResults.vendorSettlementCode AS vendor_settlement_code,
    'AFTERPAY' as tender_type,
    NULL AS gift_card_note_type,
    NULL AS paypal_tndr_rslt_order_id,
    NULL AS paypal_tndr_rslt_payer_id,
    NULL AS paypal_stlmnt_rslt_id,
    NULL AS paypal_stlmnt_rslt_parent_ref_id,
    NULL AS paypal_stlmnt_rslt_fee_currency_code,
    NULL AS paypal_stlmnt_rslt_fee_amount,
    NULL AS paypal_stlmnt_rslt_net_amt_currency_code,
    NULL AS paypal_stlmnt_rslt_net_amt_amt,
    NULL AS paypal_billing_agreement_tndr_rslt_id_value,
    NULL AS paypal_tndr_rslt_payer_email_value,
    NULL AS paypal_tndr_rslt_payer_account_country,
    NULL AS note_refund_reason
FROM refund_success_afterPayVirtualCard_exploded;

--reading the refund success tender - giftCardNoteIssuedResults
CREATE TEMPORARY VIEW refund_success_giftCardNoteIssued AS
WITH refund_success_giftCardNoteIssued_exploded AS (
    SELECT
        ELEMENT_AT(headers, 'Id') AS event_id,
        ELEMENT_AT(headers, 'Type') AS event_type,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-test') AS nord_test,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-load') AS nord_load,
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'giftCardNoteIssuedResults' AS tender_record_name,
        transactionId.id AS transaction_id,
        purchaseId.id AS purchase_identifier_id,
        EXPLODE(giftCardNoteIssuedResults) AS giftCardNoteIssuedResults
    FROM temp_kafka_payment_refund_succeeded_v2
)

SELECT
    event_id,
    event_type,
    nord_test,
    nord_load,
    event_time,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    NULL AS card_type,
    NULL AS card_sub_type,
    giftCardNoteIssuedResults.eGiftCardId AS tender_item_account_no,
    'eGiftCardId' AS tender_item_account_value_type,
    NULL AS token_authority,
    NULL AS token_data_classification,
    NULL AS token_type,
    giftCardNoteIssuedResults.amount.currencyCode AS currency_code,
    CAST(
        CAST(
            giftCardNoteIssuedResults.amount.units AS INTEGER
        ) + (giftCardNoteIssuedResults.amount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    CAST(giftCardNoteIssuedResults.transactionTime AS TIMESTAMP) AS transaction_time,
    NULL AS transaction_type,
    giftCardNoteIssuedResults.transactionStatus AS transaction_status,
    NULL AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS vendor_settlement_code,
    CASE
        WHEN giftCardNoteIssuedResults.giftCardNoteType = 'NORDSTROM_NOTE' THEN 'NORDSTROM_NOTE'
        ELSE 'GIFT_CARD'
    END AS tender_type,
    giftCardNoteIssuedResults.giftCardNoteType AS gift_card_note_type,
    NULL AS paypal_tndr_rslt_order_id,
    NULL AS paypal_tndr_rslt_payer_id,
    NULL AS paypal_stlmnt_rslt_id,
    NULL AS paypal_stlmnt_rslt_parent_ref_id,
    NULL AS paypal_stlmnt_rslt_fee_currency_code,
    NULL AS paypal_stlmnt_rslt_fee_amount,
    NULL AS paypal_stlmnt_rslt_net_amt_currency_code,
    NULL AS paypal_stlmnt_rslt_net_amt_amt,
    NULL AS paypal_billing_agreement_tndr_rslt_id_value,
    NULL AS paypal_tndr_rslt_payer_email_value,
    NULL AS paypal_tndr_rslt_payer_account_country,
    NULL AS note_refund_reason
FROM refund_success_giftCardNoteIssued_exploded;

--reading the refund success tender - payPalRefundResults
CREATE TEMPORARY VIEW refund_success_payPalRefund AS
WITH refund_success_payPalRefund_exploded AS (
    SELECT
        ELEMENT_AT(headers, 'Id') AS event_id,
        ELEMENT_AT(headers, 'Type') AS event_type,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-test') AS nord_test,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-load') AS nord_load,
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'payPalRefundResults' AS tender_record_name,
        transactionId.id AS transaction_id,
        purchaseId.id AS purchase_identifier_id,
        EXPLODE(payPalRefundResults) AS payPalRefundResults
    FROM temp_kafka_payment_refund_succeeded_v2
)

SELECT
    event_id,
    event_type,
    nord_test,
    nord_load,
    event_time,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    NULL AS card_type,
    NULL AS card_sub_type,
    NULL AS tender_item_account_no,
    'payPalRefundResults_tenderResult' AS tender_item_account_value_type,
    NULL AS token_authority,
    NULL AS token_data_classification,
    NULL AS token_type,
    payPalRefundResults.tenderResult.total.currencyCode AS currency_code,
    CAST(
        CAST(
            payPalRefundResults.tenderResult.total.units AS INTEGER
        ) + (payPalRefundResults.tenderResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    CASE
        WHEN payPalRefundResults.tenderResult.transactionTime IS NOT NULL THEN CAST(payPalRefundResults.tenderResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    payPalRefundResults.tenderResult.transactionType AS transaction_type,
    payPalRefundResults.tenderResult.transactionStatus.status AS transaction_status,
    payPalRefundResults.tenderResult.transactionStatus.failureDescription AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS vendor_settlement_code,
    'PAYPAL' AS tender_type,
    NULL AS gift_card_note_type,
    payPalRefundResults.tenderResult.payPalOrderId AS paypal_tndr_rslt_order_id,
    payPalRefundResults.tenderResult.payerId AS paypal_tndr_rslt_payer_id,
    payPalRefundResults.id AS paypal_stlmnt_rslt_id,
    payPalRefundResults.parentReferenceId AS paypal_stlmnt_rslt_parent_ref_id,
    payPalRefundResults.fee.currencyCode AS paypal_stlmnt_rslt_fee_currency_code,
    CAST(
        CAST(
            payPalRefundResults.fee.units AS INTEGER
        ) + (payPalRefundResults.fee.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS paypal_stlmnt_rslt_fee_amount,
    payPalRefundResults.netAmount.currencyCode AS paypal_stlmnt_rslt_net_amt_currency_code,
    CAST(
        CAST(
            payPalRefundResults.netAmount.units AS INTEGER
        ) + (payPalRefundResults.netAmount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS paypal_stlmnt_rslt_net_amt_amt,
    NULL AS paypal_billing_agreement_tndr_rslt_id_value,
    NULL AS paypal_tndr_rslt_payer_email_value,
    NULL AS paypal_tndr_rslt_payer_account_country,
    NULL AS note_refund_reason
FROM refund_success_payPalRefund_exploded;

--reading the refund success tender - payPalBillingAgreementRefundResults
CREATE TEMPORARY VIEW refund_success_payPalBillingAgreement AS
WITH refund_success_payPalBillingAgreement_exploded AS (
    SELECT
        ELEMENT_AT(headers, 'Id') AS event_id,
        ELEMENT_AT(headers, 'Type') AS event_type,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-test') AS nord_test,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-load') AS nord_load,
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'payPalBillingAgreementRefundResults' AS tender_record_name,
        transactionId.id AS transaction_id,
        purchaseId.id AS purchase_identifier_id,
        EXPLODE(payPalBillingAgreementRefundResults) AS payPalBillingAgreementRefundResults
    FROM temp_kafka_payment_refund_succeeded_v2
)

SELECT
    event_id,
    event_type,
    nord_test,
    nord_load,
    event_time,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    NULL AS card_type,
    NULL AS card_sub_type,
    NULL AS tender_item_account_no,
    'payPalBillingAgreementTenderResult' AS tender_item_account_value_type,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.billingAgreementId.authority AS token_authority,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.billingAgreementId.dataClassification AS token_data_classification,
    NULL AS token_type,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.total.currencyCode AS currency_code,
    CAST(
        CAST(
            payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.total.units AS INTEGER
        ) + (payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    CASE
        WHEN payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.transactionTime IS NOT NULL THEN CAST(payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.transactionType AS transaction_type,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.transactionStatus.status AS transaction_status,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.transactionStatus.failureDescription AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS vendor_settlement_code,
    'PAYPAL_BILLING_AGREEMENT' AS tender_type,
    NULL AS gift_card_note_type,
    NULL AS paypal_tndr_rslt_order_id,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.payerId AS paypal_tndr_rslt_payer_id,
    payPalBillingAgreementRefundResults.id AS paypal_stlmnt_rslt_id,
    payPalBillingAgreementRefundResults.parentReferenceId AS paypal_stlmnt_rslt_parent_ref_id,
    payPalBillingAgreementRefundResults.fee.currencyCode AS paypal_stlmnt_rslt_fee_currency_code,
    CAST(
        CAST(
            payPalBillingAgreementRefundResults.fee.units AS INTEGER
        ) + (payPalBillingAgreementRefundResults.fee.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS paypal_stlmnt_rslt_fee_amount,
    payPalBillingAgreementRefundResults.netAmount.currencyCode AS paypal_stlmnt_rslt_net_amt_currency_code,
    CAST(
        CAST(
            payPalBillingAgreementRefundResults.netAmount.units AS INTEGER
        ) + (payPalBillingAgreementRefundResults.netAmount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS paypal_stlmnt_rslt_net_amt_amt,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.billingAgreementId.value AS paypal_billing_agreement_tndr_rslt_id_value,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.payerEmail.value AS paypal_tndr_rslt_payer_email_value,
    payPalBillingAgreementRefundResults.payPalBillingAgreementTenderResult.payerAccountCountry AS paypal_tndr_rslt_payer_account_country,
    NULL AS note_refund_reason
FROM refund_success_payPalBillingAgreement_exploded;

--reading the refund success tender - nordstromNoteRefundedResults
CREATE TEMPORARY VIEW refund_success_nordstromNoteRefunded AS
WITH refund_success_nordstromNoteRefunded_exploded AS (
    SELECT
        ELEMENT_AT(headers, 'Id') AS event_id,
        ELEMENT_AT(headers, 'Type') AS event_type,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-test') AS nord_test,
        ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-load') AS nord_load,
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'nordstromNoteRefundedResults' AS tender_record_name,
        transactionId.id AS transaction_id,
        purchaseId.id AS purchase_identifier_id,
        EXPLODE(nordstromNoteRefundedResults) AS nordstromNoteRefundedResults
    FROM temp_kafka_payment_refund_succeeded_v2
)

SELECT
    event_id,
    event_type,
    nord_test,
    nord_load,
    event_time,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    NULL AS card_type,
    NULL AS card_sub_type,
    nordstromNoteRefundedResults.noteNumber.value AS tender_item_account_no,
    'noteNumber' AS tender_item_account_value_type,
    nordstromNoteRefundedResults.noteNumber.authority AS token_authority,
    nordstromNoteRefundedResults.noteNumber.dataClassification AS token_data_classification,
    NULL AS token_type,
    nordstromNoteRefundedResults.amount.currencyCode AS currency_code,
    CAST(
        CAST(
            nordstromNoteRefundedResults.amount.units AS INTEGER
        ) + (nordstromNoteRefundedResults.amount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    NULL AS transaction_time,
    nordstromNoteRefundedResults.tenderTransactionResult.resultType AS transaction_type,
    nordstromNoteRefundedResults.tenderTransactionResult.status AS transaction_status,
    nordstromNoteRefundedResults.tenderTransactionResult.failureReason AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS vendor_settlement_code,
    'NORDSTROM_NOTE' AS tender_type,
    NULL AS gift_card_note_type,
    NULL AS paypal_tndr_rslt_order_id,
    NULL AS paypal_tndr_rslt_payer_id,
    NULL AS paypal_stlmnt_rslt_id,
    NULL AS paypal_stlmnt_rslt_parent_ref_id,
    NULL AS paypal_stlmnt_rslt_fee_currency_code,
    NULL AS paypal_stlmnt_rslt_fee_amount,
    NULL AS paypal_stlmnt_rslt_net_amt_currency_code,
    NULL AS paypal_stlmnt_rslt_net_amt_amt,
    NULL AS paypal_billing_agreement_tndr_rslt_id_value,
    NULL AS paypal_tndr_rslt_payer_email_value,
    NULL AS paypal_tndr_rslt_payer_account_country,
    nordstromNoteRefundedResults.reason AS note_refund_reason
FROM refund_success_nordstromNoteRefunded_exploded;

--unioning all the success tender types
CREATE TEMPORARY VIEW refund_success_tender_union
AS
SELECT DISTINCT * FROM refund_success_bankCardRefundV2
UNION
SELECT DISTINCT * FROM refund_success_afterPayVirtualCard
UNION
SELECT DISTINCT * FROM refund_success_giftCardNoteIssued
UNION
SELECT DISTINCT * FROM refund_success_payPalRefund
UNION
SELECT DISTINCT * FROM refund_success_payPalBillingAgreement
UNION
SELECT DISTINCT * FROM refund_success_nordstromNoteRefunded;

--inserting into the teradata tender stg table
INSERT INTO TABLE payment_refund_success_tender_stg
SELECT DISTINCT
    event_id,
    event_type,
    event_time,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    card_type,
    card_sub_type,
    tender_item_account_no,
    tender_item_account_value_type,
    token_authority,
    token_data_classification,
    token_type,
    currency_code,
    total_amt,
    transaction_time,
    transaction_type,
    transaction_status,
    transaction_failure_desc,
    token_requestor_id,
    vendor_settlement_code,
    tender_type,
    gift_card_note_type,
    paypal_tndr_rslt_order_id,
    paypal_tndr_rslt_payer_id,
    paypal_stlmnt_rslt_id,
    paypal_stlmnt_rslt_parent_ref_id,
    paypal_stlmnt_rslt_fee_currency_code,
    paypal_stlmnt_rslt_fee_amount,
    paypal_stlmnt_rslt_net_amt_currency_code,
    paypal_stlmnt_rslt_net_amt_amt,
    paypal_billing_agreement_tndr_rslt_id_value,
    paypal_tndr_rslt_payer_email_value,
    paypal_tndr_rslt_payer_account_country,
    note_refund_reason
FROM refund_success_tender_union
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
--------------------------------------------------------------------
