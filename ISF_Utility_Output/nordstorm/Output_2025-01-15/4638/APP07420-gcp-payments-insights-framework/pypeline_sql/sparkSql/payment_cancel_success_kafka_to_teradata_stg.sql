-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=payment_cancel_teradata;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
CREATE TEMPORARY VIEW temp_kafka_payment_cancel_success AS
SELECT * FROM kafka_payment_cancel_success;

-- building the cancel hdr table
--reading the cancel success hdr fields
CREATE TEMPORARY VIEW cancel_success_hdr_temp AS
SELECT
    element_at(headers, 'Id') AS event_id,
    orderNumber AS order_number,
    cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
    element_at(headers, 'Type') AS event_type,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load,
    reason AS reason,
    total.currencyCode AS currency_code,
    cast(
        cast(
            total.units AS INTEGER
        ) + (total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    merchantIdentifier AS merchant_identifier,
    serviceTicketId AS service_ticket_id,
    '' AS failure_reason
FROM temp_kafka_payment_cancel_success;

--inserting into the teradata hdr stg table
INSERT INTO TABLE payment_cancel_success_hdr_stg
SELECT DISTINCT
    event_id,
    order_number,
    event_time,
    event_type,
    reason,
    currency_code,
    total_amt,
    merchant_identifier,
    service_ticket_id,
    failure_reason
FROM cancel_success_hdr_temp
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );

-- building the cancel dtl table
--reading the cancel success dtl fields exploding orderLineId
CREATE TEMPORARY VIEW cancel_success_dtl_temp1 AS
WITH cancel_success_orderLineId_exploded AS (
    SELECT
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load,
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        orderNumber AS order_number,
        explode(orderLineId) AS order_line_id
    FROM temp_kafka_payment_cancel_success
)

SELECT
    event_id,
    event_type,
    nord_test,
    nord_load,
    event_time,
    order_number,
    order_line_id,
    NULL AS service_line_id
FROM cancel_success_orderLineId_exploded;

--reading the cancel success dtl fields exploding serviceLineId
CREATE TEMPORARY VIEW cancel_success_dtl_temp2 AS
WITH cancel_success_serviceLineId_exploded AS (
    SELECT
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load,
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        orderNumber AS order_number,
        explode(serviceLineId) AS service_line_id
    FROM temp_kafka_payment_cancel_success
)

SELECT
    event_id,
    event_type,
    nord_test,
    nord_load,
    event_time,
    order_number,
    NULL AS order_line_id,
    service_line_id
FROM cancel_success_serviceLineId_exploded;

-- union of above two temp views
CREATE TEMPORARY VIEW cancel_success_dtl_temp_union
AS
SELECT DISTINCT * FROM cancel_success_dtl_temp1
UNION
SELECT DISTINCT * FROM cancel_success_dtl_temp2;


--inserting into the teradata dtl stg table
INSERT INTO TABLE payment_cancel_success_dtl_stg
SELECT DISTINCT
    event_id,
    event_time,
    event_type,
    order_number,
    order_line_id,
    service_line_id
FROM cancel_success_dtl_temp_union
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );


--building the cancel tender table
--reading the cancel success tender - giftCardRefundResults
CREATE TEMPORARY VIEW cancel_success_giftCardRefund AS
WITH cancel_success_giftCardRefund_exploded AS (
    SELECT
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        'giftCardRefundResults' AS tender_record_name,
        orderNumber AS order_number,
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load,
        explode(giftCardRefundResults) AS giftCardRefundResults
    FROM temp_kafka_payment_cancel_success
)

SELECT
    event_id,
    order_number,
    event_time,
    event_type,
    nord_test,
    nord_load,
    'GIFT_CARD' AS tender_type,
    tender_record_name,
    NULL AS card_type,
    NULL AS card_sub_type,
    CASE
        WHEN giftCardRefundResults.accountNumber IS NOT NULL THEN giftCardRefundResults.accountNumber.value
    END AS tender_item_account_no,
    'gift_card_account_number_value' AS tender_item_account_value_type,
    CASE
        WHEN giftCardRefundResults.accountNumber IS NOT NULL THEN giftCardRefundResults.accountNumber.authority
    END AS token_authority,
    CASE
        WHEN giftCardRefundResults.accountNumber IS NOT NULL THEN giftCardRefundResults.accountNumber.dataClassification
    END AS token_data_classification,
    NULL AS token_type,
    giftCardRefundResults.amount.currencyCode AS currency_code,
    cast(
        cast(
            giftCardRefundResults.amount.units AS INTEGER
        ) + (giftCardRefundResults.amount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    cast(giftCardRefundResults.transactionTime AS TIMESTAMP) AS transaction_time,
    NULL AS transaction_type,
    giftCardRefundResults.transactionStatus AS transaction_status,
    NULL AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS gift_card_note_type,
    giftCardRefundResults.giftCardIssuanceId AS gift_card_issuance_id,
    giftCardRefundResults.giftCardRefundMethod AS gift_card_refund_method,
    NULL AS gift_card_balance_lock_id,
    NULL AS paypal_order_id,
    NULL AS paypal_payer_id,
    NULL AS paypal_billing_agreement_id_value,
    NULL AS paypal_payer_email_value,
    NULL AS paypal_payer_account_country,
    NULL AS note_refund_reason
FROM cancel_success_giftCardRefund_exploded;

--reading the cancel success tender - giftCardNoteUnlockResults
CREATE TEMPORARY VIEW cancel_success_giftCardNoteUnlock AS
WITH cancel_success_giftCardNoteUnlock_exploded AS (
    SELECT
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        'giftCardNoteUnlockResults' AS tender_record_name,
        orderNumber AS order_number,
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load,
        explode(giftCardNoteUnlockResults) AS giftCardNoteUnlockResults
    FROM temp_kafka_payment_cancel_success
)

SELECT
    event_id,
    order_number,
    event_time,
    event_type,
    nord_test,
    nord_load,
    CASE
        WHEN giftCardNoteUnlockResults.giftCardNoteType = 'NORDSTROM_NOTE' THEN 'NORDSTROM_NOTE'
        ELSE 'GIFT_CARD'
    END AS tender_type,
    tender_record_name,
    NULL AS card_type,
    NULL AS card_sub_type,
    giftCardNoteUnlockResults.accountNumber.value AS tender_item_account_no,
    'gift_card_account_number_value' AS tender_item_account_value_type,
    giftCardNoteUnlockResults.accountNumber.authority AS token_authority,
    giftCardNoteUnlockResults.accountNumber.dataClassification AS token_data_classification,
    NULL AS token_type,
    giftCardNoteUnlockResults.amount.currencyCode AS currency_code,
    cast(
        cast(
            giftCardNoteUnlockResults.amount.units AS INTEGER
        ) + (giftCardNoteUnlockResults.amount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    cast(giftCardNoteUnlockResults.transactionTime AS TIMESTAMP) AS transaction_time,
    NULL AS transaction_type,
    giftCardNoteUnlockResults.transactionStatus AS transaction_status,
    NULL AS transaction_failure_desc,
    NULL AS token_requestor_id,
    giftCardNoteUnlockResults.giftCardNoteType AS gift_card_note_type,
    NULL AS gift_card_issuance_id,
    NULL AS gift_card_refund_method,
    giftCardNoteUnlockResults.balanceLockId AS gift_card_balance_lock_id,
    NULL AS paypal_order_id,
    NULL AS paypal_payer_id,
    NULL AS paypal_billing_agreement_id_value,
    NULL AS paypal_payer_email_value,
    NULL AS paypal_payer_account_country,
    NULL AS note_refund_reason
FROM cancel_success_giftCardNoteUnlock_exploded;

--reading the cancel success tender - nordstromNoteRefundedResults
CREATE TEMPORARY VIEW cancel_success_nordstromNoteRefunded AS
WITH cancel_success_nordstromNoteRefunded_exploded AS (
    SELECT
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        'nordstromNoteRefundedResults' AS tender_record_name,
        orderNumber AS order_number,
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load,
        explode(nordstromNoteRefundedResults) AS nordstromNoteRefundedResults
    FROM temp_kafka_payment_cancel_success
)

SELECT
    event_id,
    order_number,
    event_time,
    event_type,
    nord_test,
    nord_load,
    'NORDSTROM_NOTE' AS tender_type,
    tender_record_name,
    NULL AS card_type,
    NULL AS card_sub_type,
    nordstromNoteRefundedResults.noteNumber.value AS tender_item_account_no,
    'note_number_value' AS tender_item_account_value_type,
    nordstromNoteRefundedResults.noteNumber.authority AS token_authority,
    nordstromNoteRefundedResults.noteNumber.dataClassification AS token_data_classification,
    NULL AS token_type,
    nordstromNoteRefundedResults.amount.currencyCode AS currency_code,
    cast(
        cast(
            nordstromNoteRefundedResults.amount.units AS INTEGER
        ) + (nordstromNoteRefundedResults.amount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    NULL AS transaction_time,
    nordstromNoteRefundedResults.tenderTransactionResult.resultType AS transaction_type,
    nordstromNoteRefundedResults.tenderTransactionResult.status AS transaction_status,
    nordstromNoteRefundedResults.tenderTransactionResult.failureReason AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS gift_card_note_type,
    NULL AS gift_card_issuance_id,
    NULL AS gift_card_refund_method,
    NULL AS gift_card_balance_lock_id,
    NULL AS paypal_order_id,
    NULL AS paypal_payer_id,
    NULL AS paypal_billing_agreement_id_value,
    NULL AS paypal_payer_email_value,
    NULL AS paypal_payer_account_country,
    nordstromNoteRefundedResults.reason AS note_refund_reason
FROM cancel_success_nordstromNoteRefunded_exploded;


--reading the cancel success tender - bankCardOrderCanceledResultV2
CREATE TEMPORARY VIEW cancel_success_bankCardOrderCanceledResultV2 AS
WITH cancel_success_bankCardOrderCanceledV2 AS (
    SELECT
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        'bankCardOrderCanceledResultV2' AS tender_record_name,
        orderNumber AS order_number,
        bankCardOrderCanceledResultV2 AS bankCardOrderCanceledResultV2,
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load
    FROM temp_kafka_payment_cancel_success
    WHERE bankCardOrderCanceledResultV2 IS NOT NULL
)

SELECT
    event_id,
    order_number,
    event_time,
    event_type,
    nord_test,
    nord_load,
    CASE
        WHEN bankCardOrderCanceledResultV2.cardTypeInfo.cardType = 'NG' THEN 'GIFT_CARD'
        WHEN bankCardOrderCanceledResultV2.cardTypeInfo.cardType = 'UNKNOWN' THEN 'UNKNOWN'
        ELSE 'CREDIT_CARD'
    END AS tender_type,
    tender_record_name,
    bankCardOrderCanceledResultV2.cardTypeInfo.cardType AS card_type,
    bankCardOrderCanceledResultV2.cardTypeInfo.cardSubType AS card_sub_type,
    bankCardOrderCanceledResultV2.token.token.value AS tender_item_account_no,
    'bank_card_token_value' AS tender_item_account_value_type,
    bankCardOrderCanceledResultV2.token.token.authority AS token_authority,
    bankCardOrderCanceledResultV2.token.token.dataClassification AS token_data_classification,
    bankCardOrderCanceledResultV2.token.tokenType AS token_type,
    bankCardOrderCanceledResultV2.total.currencyCode AS currency_code,
    cast(
        cast(
            bankCardOrderCanceledResultV2.total.units AS INTEGER
        ) + (bankCardOrderCanceledResultV2.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    CASE
        WHEN bankCardOrderCanceledResultV2.transactionTime IS NOT NULL THEN cast(bankCardOrderCanceledResultV2.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    bankCardOrderCanceledResultV2.transactionResult.resultType AS transaction_type,
    bankCardOrderCanceledResultV2.transactionResult.status AS transaction_status,
    bankCardOrderCanceledResultV2.transactionResult.failureReason AS transaction_failure_desc,
    bankCardOrderCanceledResultV2.tokenRequestorId AS token_requestor_id,
    NULL AS gift_card_note_type,
    NULL AS gift_card_issuance_id,
    NULL AS gift_card_refund_method,
    NULL AS gift_card_balance_lock_id,
    NULL AS paypal_order_id,
    NULL AS paypal_payer_id,
    NULL AS paypal_billing_agreement_id_value,
    NULL AS paypal_payer_email_value,
    NULL AS paypal_payer_account_country,
    NULL AS note_refund_reason
FROM cancel_success_bankCardOrderCanceledV2;

--reading the cancel success tender - afterPayCanceledResult
CREATE TEMPORARY VIEW cancel_success_afterPayCanceledResult AS
WITH cancel_success_afterPayCanceled AS (
    SELECT
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        'afterPayCanceledResult' AS tender_record_name,
        orderNumber AS order_number,
        afterPayCanceledResult AS afterPayCanceledResult,
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load
    FROM temp_kafka_payment_cancel_success
    WHERE afterPayCanceledResult IS NOT NULL
)

SELECT
    event_id,
    order_number,
    event_time,
    event_type,
    nord_test,
    nord_load,
    'AFTERPAY' as tender_type,
    tender_record_name,
    NULL AS card_type,
    NULL AS card_sub_type,
    NULL AS tender_item_account_no,
    NULL AS tender_item_account_value_type,
    NULL AS token_authority,
    NULL AS token_data_classification,
    NULL AS token_type,
    afterPayCanceledResult.total.currencyCode AS currency_code,
    cast(
        cast(
            afterPayCanceledResult.total.units AS INTEGER
        ) + (afterPayCanceledResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    cast(afterPayCanceledResult.transactionTime AS TIMESTAMP) AS transaction_time,
    afterPayCanceledResult.transactionType AS transaction_type,
    afterPayCanceledResult.transactionStatus AS transaction_status,
    NULL AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS gift_card_note_type,
    NULL AS gift_card_issuance_id,
    NULL AS gift_card_refund_method,
    NULL AS gift_card_balance_lock_id,
    NULL AS paypal_order_id,
    NULL AS paypal_payer_id,
    NULL AS paypal_billing_agreement_id_value,
    NULL AS paypal_payer_email_value,
    NULL AS paypal_payer_account_country,
    NULL AS note_refund_reason
FROM cancel_success_afterPayCanceled;

--reading the cancel success tender - payPalOrderCanceledResult
CREATE TEMPORARY VIEW cancel_success_payPalOrderCanceledResult AS
WITH cancel_success_payPalOrderCanceled AS (
    SELECT
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        'payPalOrderCanceledResult' AS tender_record_name,
        orderNumber AS order_number,
        payPalOrderCanceledResult AS payPalOrderCanceledResult,
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load
    FROM temp_kafka_payment_cancel_success
    WHERE payPalOrderCanceledResult IS NOT NULL
)

SELECT
    event_id,
    order_number,
    event_time,
    event_type,
    nord_test,
    nord_load,
    'PAYPAL' AS tender_type,
    tender_record_name,
    NULL AS card_type,
    NULL AS card_sub_type,
    NULL AS tender_item_account_no,
    NULL AS tender_item_account_value_type,
    NULL AS token_authority,
    NULL AS token_data_classification,
    NULL AS token_type,
    payPalOrderCanceledResult.total.currencyCode AS currency_code,
    cast(
        cast(
            payPalOrderCanceledResult.total.units AS INTEGER
        ) + (payPalOrderCanceledResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    CASE
        WHEN payPalOrderCanceledResult.transactionTime IS NOT NULL THEN cast(payPalOrderCanceledResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    payPalOrderCanceledResult.transactionType AS transaction_type,
    payPalOrderCanceledResult.transactionStatus.status AS transaction_status,
    payPalOrderCanceledResult.transactionStatus.failureDescription AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS gift_card_note_type,
    NULL AS gift_card_issuance_id,
    NULL AS gift_card_refund_method,
    NULL AS gift_card_balance_lock_id,
    payPalOrderCanceledResult.payPalOrderId AS paypal_order_id,
    payPalOrderCanceledResult.payerId AS paypal_payer_id,
    NULL AS paypal_billing_agreement_id_value,
    NULL AS paypal_payer_email_value,
    NULL AS paypal_payer_account_country,
    NULL AS note_refund_reason
FROM cancel_success_payPalOrderCanceled;

--reading the cancel success tender - payPalBillingAgreementOrderCanceledResult
CREATE TEMPORARY VIEW cancel_success_payPalBillingAgreementOrderCanceledResult AS
WITH cancel_success_payPalBillingAgreementOrderCanceled AS (
    SELECT
        cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
        'payPalBillingAgreementOrderCanceledResult' AS tender_record_name,
        orderNumber AS order_number,
        payPalBillingAgreementOrderCanceledResult AS payPalBillingAgreementOrderCanceledResult,
        element_at(headers, 'Id') AS event_id,
        element_at(headers, 'Type') AS event_type,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
        element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load
    FROM temp_kafka_payment_cancel_success
    WHERE payPalBillingAgreementOrderCanceledResult IS NOT NULL
)

SELECT
    event_id,
    order_number,
    event_time,
    event_type,
    nord_test,
    nord_load,
    'PAYPAL_BILLING_AGREEMENT' AS tender_type,
    tender_record_name,
    NULL AS card_type,
    NULL AS card_sub_type,
    NULL AS tender_item_account_no,
    NULL AS tender_item_account_value_type,
    payPalBillingAgreementOrderCanceledResult.billingAgreementId.authority AS token_authority,
    payPalBillingAgreementOrderCanceledResult.billingAgreementId.dataClassification AS token_data_classification,
    NULL AS token_type,
    payPalBillingAgreementOrderCanceledResult.total.currencyCode AS currency_code,
    cast(
        cast(
            payPalBillingAgreementOrderCanceledResult.total.units AS INTEGER
        ) + (payPalBillingAgreementOrderCanceledResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    CASE
        WHEN payPalBillingAgreementOrderCanceledResult.transactionTime IS NOT NULL THEN cast(payPalBillingAgreementOrderCanceledResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    payPalBillingAgreementOrderCanceledResult.transactionType AS transaction_type,
    payPalBillingAgreementOrderCanceledResult.transactionStatus.status AS transaction_status,
    payPalBillingAgreementOrderCanceledResult.transactionStatus.failureDescription AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS gift_card_note_type,
    NULL AS gift_card_issuance_id,
    NULL AS gift_card_refund_method,
    NULL AS gift_card_balance_lock_id,
    NULL AS paypal_order_id,
    payPalBillingAgreementOrderCanceledResult.payerId AS paypal_payer_id,
    payPalBillingAgreementOrderCanceledResult.billingAgreementId.value AS paypal_billing_agreement_id_value,
    payPalBillingAgreementOrderCanceledResult.payerEmail.value AS paypal_payer_email_value,
    payPalBillingAgreementOrderCanceledResult.payerAccountCountry AS paypal_payer_account_country,
    NULL AS note_refund_reason
FROM cancel_success_payPalBillingAgreementOrderCanceled;

--unioning all success tender types
CREATE TEMPORARY VIEW cancel_success_tender_union
AS
SELECT DISTINCT * FROM cancel_success_giftCardRefund
UNION
SELECT DISTINCT * FROM cancel_success_giftCardNoteUnlock
UNION
SELECT DISTINCT * FROM cancel_success_nordstromNoteRefunded
UNION
SELECT DISTINCT * FROM cancel_success_bankCardOrderCanceledResultV2
UNION
SELECT DISTINCT * FROM cancel_success_afterPayCanceledResult
UNION
SELECT DISTINCT * FROM cancel_success_payPalOrderCanceledResult
UNION
SELECT DISTINCT * FROM cancel_success_payPalBillingAgreementOrderCanceledResult;

--inserting into the teradata tender stg table
INSERT INTO TABLE payment_cancel_success_tender_stg
SELECT DISTINCT
    event_id,
    order_number,
    event_time,
    event_type,
    tender_type,
    tender_record_name,
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
    gift_card_note_type,
    gift_card_issuance_id,
    gift_card_refund_method,
    gift_card_balance_lock_id,
    paypal_order_id,
    paypal_payer_id,
    paypal_billing_agreement_id_value,
    paypal_payer_email_value,
    paypal_payer_account_country,
    note_refund_reason
FROM cancel_success_tender_union
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
