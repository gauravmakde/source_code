-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=payment_settlement_intent_teradata_v2;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
CREATE TEMPORARY VIEW temp_kafka_settlement_intent_v2 AS
SELECT * FROM kafka_payment_settlement_intent_analytical_avro;


-- building the refund hdr table
-- reading the refund success hdr fields
CREATE TEMPORARY VIEW settlement_intent_hdr_temp AS
SELECT
    CAST(ELEMENT_AT(headers, 'Id') AS STRING) AS event_id,
    CAST(eventTime AS TIMESTAMP) AS event_time,
    source.channelCountry AS source_channel_country,
    source.channel AS source_channel,
    source.platform AS source_platform,
    source.feature AS source_feature,
    source.serviceName AS source_service_name,
    source.store AS source_store,
    source.register AS source_register,
    purchaseIdentifier.type AS purchase_identifier_type,
    purchaseIdentifier.id AS purchase_identifier_id,
    CASE
        WHEN purchaseIdentifier.storeRegisterTransactionDate IS NOT NULL THEN purchaseIdentifier.storeRegisterTransactionDate.store
        WHEN purchaseIdentifier.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseIdentifier.storeTransactionIdentifierDetail.purchaseId.store
    END AS pos_store,
    CASE
        WHEN purchaseIdentifier.storeRegisterTransactionDate IS NOT NULL THEN purchaseIdentifier.storeRegisterTransactionDate.register
        WHEN purchaseIdentifier.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseIdentifier.storeTransactionIdentifierDetail.purchaseId.register
    END AS pos_register,
    CASE
        WHEN purchaseIdentifier.storeRegisterTransactionDate IS NOT NULL THEN purchaseIdentifier.storeRegisterTransactionDate.transaction
        WHEN purchaseIdentifier.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseIdentifier.storeTransactionIdentifierDetail.purchaseId.transaction
    END AS pos_transaction,
    CASE
        WHEN purchaseIdentifier.storeRegisterTransactionDate IS NOT NULL THEN CAST(purchaseIdentifier.storeRegisterTransactionDate.businessDate AS DATE)
        WHEN purchaseIdentifier.storeTransactionIdentifierDetail IS NOT NULL THEN CAST(purchaseIdentifier.storeTransactionIdentifierDetail.purchaseId.businessDate AS DATE)
    END AS pos_business_date,
    CASE
        WHEN purchaseIdentifier.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseIdentifier.storeTransactionIdentifierDetail.transactionId
    END AS store_transaction_transaction_id,
    CASE
        WHEN purchaseIdentifier.storeTransactionIdentifierDetail IS NOT NULL THEN purchaseIdentifier.storeTransactionIdentifierDetail.sessionId
    END AS store_transaction_session_id,
    transactionIdentifier.id AS transaction_id,
    transactionIdentifier.type AS transaction_id_type,
    serviceTicketId AS service_ticket_id,
    totalRequestedAmount.currencyCode AS currency_code,
    CAST(
        CAST(
            totalRequestedAmount.units AS INTEGER
        ) + (totalRequestedAmount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    merchantIdentifier AS merchant_identifier,
    failureReason AS failure_reason
FROM temp_kafka_settlement_intent_v2;

--inserting into the teradata hdr stg table
INSERT INTO TABLE payment_settlement_intent_hdr_stg
SELECT DISTINCT
    event_id,
    event_time,
    source_channel_country,
    source_channel,
    source_platform,
    source_feature,
    source_service_name,
    source_store,
    source_register,
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
    currency_code,
    total_amt,
    merchant_identifier,
    failure_reason
FROM settlement_intent_hdr_temp;
------------------------------------------------------------------

--reading the refund success tender - bankCardSettlementResultV2s
CREATE TEMPORARY VIEW settlement_success_bankCardResultV2 AS
WITH settlement_success_bankCardResultV2_exploded AS (
    SELECT
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'bankCardSettlementResultsV2' AS tender_record_name,
        transactionIdentifier.id AS transaction_id,
        purchaseIdentifier.id AS purchase_identifier_id,
        EXPLODE(bankCardSettlementResults) AS bankCardSettlementResultV2s
    FROM temp_kafka_settlement_intent_v2
)

SELECT
    event_time,
    'CREDIT_CARD' AS tender_type,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    bankCardSettlementResultV2s.bankCardResult.cardTypeInfo.cardType AS card_type,
    bankCardSettlementResultV2s.bankCardResult.cardTypeInfo.cardSubType AS card_sub_type,
    bankCardSettlementResultV2s.bankCardResult.token.token.value AS tender_item_account_no,
    'bank_card_token_value' AS tender_item_account_value_type,
    bankCardSettlementResultV2s.bankCardResult.token.token.authority AS token_authority,
    bankCardSettlementResultV2s.bankCardResult.token.token.dataClassification AS token_data_classification,
    bankCardSettlementResultV2s.bankCardResult.token.tokenType AS token_type,
    bankCardSettlementResultV2s.bankCardResult.total.currencyCode AS currency_code,
    CAST(
        CAST(
            bankCardSettlementResultV2s.bankCardResult.total.units AS INTEGER
        ) + (bankCardSettlementResultV2s.bankCardResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    'bankcard total amount' AS total_amt_type,
    CASE
        WHEN bankCardSettlementResultV2s.bankCardResult.transactionTime IS NOT NULL THEN CAST(bankCardSettlementResultV2s.bankCardResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    CASE
        WHEN bankCardSettlementResultV2s.bankCardResult.transactionTime IS NOT NULL THEN CAST(bankCardSettlementResultV2s.bankCardResult.transactionTime AS DATE)
    END AS transaction_date,
    bankCardSettlementResultV2s.bankCardResult.transactionResult.resultType AS transaction_type,
    bankCardSettlementResultV2s.bankCardResult.transactionResult.status AS transaction_status,
    bankCardSettlementResultV2s.bankCardResult.transactionResult.failureReason AS transaction_failure_desc,
    bankCardSettlementResultV2s.bankCardResult.tokenRequestorId AS token_requestor_id,
    bankCardSettlementResultV2s.vendorSettlementCode AS vendor_settlement_code,
    NULL AS gift_card_note_type,
    NULL AS gift_card_req_amt_currency_code,
    NULL AS gift_card_req_amt,
    NULL AS balance_lock_id,
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
    NULL AS paypal_tndr_rslt_payer_account_country
FROM settlement_success_bankCardResultV2_exploded;

--reading the refund success tender - afterPayVirtualCardSettlementResults
CREATE TEMPORARY VIEW settlement_success_afterPayVirtualCard AS
WITH settlement_success_afterPayVirtualCard_exploded AS (
    SELECT
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'afterPayVirtualCardSettlementResults' AS tender_record_name,
        transactionIdentifier.id AS transaction_id,
        purchaseIdentifier.id AS purchase_identifier_id,
        EXPLODE(afterPayVirtualCardSettlementResults) AS afterPayVirtualCardSettlementResults
    FROM temp_kafka_settlement_intent_v2
)

SELECT
    event_time,
    'AFTERPAY' as tender_type,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    afterPayVirtualCardSettlementResults.bankCardResult.cardTypeInfo.cardType AS card_type,
    afterPayVirtualCardSettlementResults.bankCardResult.cardTypeInfo.cardSubType AS card_sub_type,
    afterPayVirtualCardSettlementResults.bankCardResult.token.token.value AS tender_item_account_no,
    'bank_card_token_value' AS tender_item_account_value_type,
    afterPayVirtualCardSettlementResults.bankCardResult.token.token.authority AS token_authority,
    afterPayVirtualCardSettlementResults.bankCardResult.token.token.dataClassification AS token_data_classification,
    afterPayVirtualCardSettlementResults.bankCardResult.token.tokenType AS token_type,
    afterPayVirtualCardSettlementResults.bankCardResult.total.currencyCode AS currency_code,
    CAST(
        CAST(
            afterPayVirtualCardSettlementResults.bankCardResult.total.units AS INTEGER
        ) + (afterPayVirtualCardSettlementResults.bankCardResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    'afterpay total amount' AS total_amt_type,
    CASE
        WHEN afterPayVirtualCardSettlementResults.bankCardResult.transactionTime IS NOT NULL THEN CAST(afterPayVirtualCardSettlementResults.bankCardResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    CASE
        WHEN afterPayVirtualCardSettlementResults.bankCardResult.transactionTime IS NOT NULL THEN CAST(afterPayVirtualCardSettlementResults.bankCardResult.transactionTime AS DATE)
    END AS transaction_date,
    afterPayVirtualCardSettlementResults.bankCardResult.transactionResult.resultType AS transaction_type,
    afterPayVirtualCardSettlementResults.bankCardResult.transactionResult.status AS transaction_status,
    afterPayVirtualCardSettlementResults.bankCardResult.transactionResult.failureReason AS transaction_failure_desc,
    afterPayVirtualCardSettlementResults.bankCardResult.tokenRequestorId AS token_requestor_id,
    afterPayVirtualCardSettlementResults.vendorSettlementCode AS vendor_settlement_code,
    NULL AS gift_card_note_type,
    NULL AS gift_card_req_amt_currency_code,
    NULL AS gift_card_req_amt,
    NULL AS balance_lock_id,
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
    NULL AS paypal_tndr_rslt_payer_account_country
FROM settlement_success_afterPayVirtualCard_exploded;

--reading the refund success tender - giftCardNoteTenderResults
CREATE TEMPORARY VIEW settlement_success_giftCardNoteTender AS
WITH settlement_success_giftCardNoteTenderResults_exploded AS (
    SELECT
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'giftCardNoteTenderResults' AS tender_record_name,
        transactionIdentifier.id AS transaction_id,
        purchaseIdentifier.id AS purchase_identifier_id,
        EXPLODE(giftCardNoteTenderResults) AS giftCardNoteTenderResults
    FROM temp_kafka_settlement_intent_v2
)

SELECT
    event_time,
    CASE
        WHEN giftCardNoteTenderResults.giftCardNoteType = 'NORDSTROM_NOTE' THEN 'NORDSTROM_NOTE'
        ELSE 'GIFT_CARD'
    END AS tender_type,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    NULL AS card_type,
    NULL AS card_sub_type,
    giftCardNoteTenderResults.accountNumber.value AS tender_item_account_no,
    'tokenizedValue' AS tender_item_account_value_type,
    NULL AS token_authority,
    NULL AS token_data_classification,
    NULL AS token_type,
    giftCardNoteTenderResults.appliedAmount.currencyCode AS currency_code,
    CAST(
        CAST(
            giftCardNoteTenderResults.appliedAmount.units AS INTEGER
        ) + (giftCardNoteTenderResults.appliedAmount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    'giftcard applied amount' AS total_amt_type,
    CAST(giftCardNoteTenderResults.transactionTime AS TIMESTAMP) AS transaction_time,
    CAST(giftCardNoteTenderResults.transactionTime AS DATE) AS transaction_date,
    NULL AS transaction_type,
    giftCardNoteTenderResults.transactionStatus AS transaction_status,
    NULL AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS vendor_settlement_code,
    giftCardNoteTenderResults.giftCardNoteType AS gift_card_note_type,
    giftCardNoteTenderResults.requestedAmount.currencyCode AS gift_card_req_amt_currency_code,
    CAST(
        CAST(
            giftCardNoteTenderResults.requestedAmount.units AS INTEGER
        ) + (giftCardNoteTenderResults.requestedAmount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS gift_card_req_amt,
    giftCardNoteTenderResults.balanceLockId AS balance_lock_id,
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
    NULL AS paypal_tndr_rslt_payer_account_country
FROM settlement_success_giftCardNoteTenderResults_exploded;

--reading the settlement success tender - payPalSettlementResult
CREATE TEMPORARY VIEW settlement_success_payPalSettlement AS
WITH settlement_success_payPalSettlement_exploded AS (
    SELECT
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'payPalSettlementResult' AS tender_record_name,
        transactionIdentifier.id AS transaction_id,
        purchaseIdentifier.id AS purchase_identifier_id,
        EXPLODE(payPalSettlementResults) AS payPalSettlementResult
    FROM temp_kafka_settlement_intent_v2
)

SELECT
    event_time,
    'PAYPAL' AS tender_type,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    NULL AS card_type,
    NULL AS card_sub_type,
    NULL AS tender_item_account_no,
    'payPalSettlementResult_tenderResult' AS tender_item_account_value_type,
    NULL AS token_authority,
    NULL AS token_data_classification,
    NULL AS token_type,
    payPalSettlementResult.tenderResult.total.currencyCode AS currency_code,
    CAST(
        CAST(
            payPalSettlementResult.tenderResult.total.units AS INTEGER
        ) + (payPalSettlementResult.tenderResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    'paypal tender result amount' AS total_amt_type,
    CASE
        WHEN payPalSettlementResult.tenderResult.transactionTime IS NOT NULL THEN CAST(payPalSettlementResult.tenderResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    CASE
        WHEN payPalSettlementResult.tenderResult.transactionTime IS NOT NULL THEN CAST(payPalSettlementResult.tenderResult.transactionTime AS DATE)
    END AS transaction_date,
    payPalSettlementResult.tenderResult.transactionType AS transaction_type,
    payPalSettlementResult.tenderResult.transactionStatus.status AS transaction_status,
    payPalSettlementResult.tenderResult.transactionStatus.failureDescription AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS vendor_settlement_code,
    NULL AS gift_card_note_type,
    NULL AS gift_card_req_amt_currency_code,
    NULL AS gift_card_req_amt,
    NULL AS balance_lock_id,
    payPalSettlementResult.tenderResult.payPalOrderId AS paypal_tndr_rslt_order_id,
    payPalSettlementResult.tenderResult.payerId AS paypal_tndr_rslt_payer_id,
    payPalSettlementResult.id AS paypal_stlmnt_rslt_id,
    payPalSettlementResult.parentReferenceId AS paypal_stlmnt_rslt_parent_ref_id,
    payPalSettlementResult.fee.currencyCode AS paypal_stlmnt_rslt_fee_currency_code,
    CAST(
        CAST(
            payPalSettlementResult.fee.units AS INTEGER
        ) + (payPalSettlementResult.fee.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS paypal_stlmnt_rslt_fee_amount,
    payPalSettlementResult.netAmount.currencyCode AS paypal_stlmnt_rslt_net_amt_currency_code,
    CAST(
        CAST(
            payPalSettlementResult.netAmount.units AS INTEGER
        ) + (payPalSettlementResult.netAmount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS paypal_stlmnt_rslt_net_amt_amt,
    NULL AS paypal_billing_agreement_tndr_rslt_id_value,
    NULL AS paypal_tndr_rslt_payer_email_value,
    NULL AS paypal_tndr_rslt_payer_account_country
FROM settlement_success_payPalSettlement_exploded;

--reading the refund success tender - payPalBillingAgreementSettlementResult
CREATE TEMPORARY VIEW settlement_success_payPalBillingAgreement AS
WITH settlement_success_payPalBillingAgreement_exploded AS (
    SELECT
        CAST(eventTime AS TIMESTAMP) AS event_time,
        'payPalBillingAgreementSettlementResult' AS tender_record_name,
        transactionIdentifier.id AS transaction_id,
        purchaseIdentifier.id AS purchase_identifier_id,
        EXPLODE(payPalBillingAgreementSettlementResults) AS payPalBillingAgreementSettlementResult
    FROM temp_kafka_settlement_intent_v2
)

SELECT
    event_time,
    'PAYPAL_BILLING_AGREEMENT' AS tender_type,
    tender_record_name,
    transaction_id,
    purchase_identifier_id,
    NULL AS card_type,
    NULL AS card_sub_type,
    NULL AS tender_item_account_no,
    'payPalBillingAgreementSettlementResult' AS tender_item_account_value_type,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.billingAgreementId.authority AS token_authority,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.billingAgreementId.dataClassification AS token_data_classification,
    NULL AS token_type,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.total.currencyCode AS currency_code,
    CAST(
        CAST(
            payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.total.units AS INTEGER
        ) + (payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.total.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS total_amt,
    'paypal tender result amount' AS total_amt_type,
    CASE
        WHEN payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.transactionTime IS NOT NULL THEN CAST(payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.transactionTime AS TIMESTAMP)
    END AS transaction_time,
    CASE
        WHEN payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.transactionTime IS NOT NULL THEN CAST(payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.transactionTime AS DATE)
    END AS transaction_date,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.transactionType AS transaction_type,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.transactionStatus.status AS transaction_status,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.transactionStatus.failureDescription AS transaction_failure_desc,
    NULL AS token_requestor_id,
    NULL AS vendor_settlement_code,
    NULL AS gift_card_note_type,
    NULL AS gift_card_req_amt_currency_code,
    NULL AS gift_card_req_amt,
    NULL AS balance_lock_id,
    NULL AS paypal_tndr_rslt_order_id,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.payerId AS paypal_tndr_rslt_payer_id,
    payPalBillingAgreementSettlementResult.id AS paypal_stlmnt_rslt_id,
    payPalBillingAgreementSettlementResult.parentReferenceId AS paypal_stlmnt_rslt_parent_ref_id,
    payPalBillingAgreementSettlementResult.fee.currencyCode AS paypal_stlmnt_rslt_fee_currency_code,
    CAST(
        CAST(
            payPalBillingAgreementSettlementResult.fee.units AS INTEGER
        ) + (payPalBillingAgreementSettlementResult.fee.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS paypal_stlmnt_rslt_fee_amount,
    payPalBillingAgreementSettlementResult.netAmount.currencyCode AS paypal_stlmnt_rslt_net_amt_currency_code,
    CAST(
        CAST(
            payPalBillingAgreementSettlementResult.netAmount.units AS INTEGER
        ) + (payPalBillingAgreementSettlementResult.netAmount.nanos / 1000000000) AS DECIMAL (14, 4)
    ) AS paypal_stlmnt_rslt_net_amt_amt,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.billingAgreementId.value AS paypal_billing_agreement_tndr_rslt_id_value,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.payerEmail.value AS paypal_tndr_rslt_payer_email_value,
    payPalBillingAgreementSettlementResult.payPalBillingAgreementTenderResult.payerAccountCountry AS paypal_tndr_rslt_payer_account_country
FROM settlement_success_payPalBillingAgreement_exploded;


--unioning all the success tender types
CREATE TEMPORARY VIEW settlement_success_tender_union
AS
SELECT DISTINCT * FROM settlement_success_bankCardResultV2
UNION
SELECT DISTINCT * FROM settlement_success_afterPayVirtualCard
UNION
SELECT DISTINCT * FROM settlement_success_giftCardNoteTender
UNION
SELECT DISTINCT * FROM settlement_success_payPalSettlement
UNION
SELECT DISTINCT * FROM settlement_success_payPalBillingAgreement;

--inserting into the teradata tender stg table
INSERT INTO TABLE payment_settlement_intent_stg
SELECT DISTINCT
    event_time,
    tender_type,
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
    total_amt_type,
    transaction_time,
    transaction_date,
    transaction_type,
    transaction_status,
    transaction_failure_desc,
    token_requestor_id,
    vendor_settlement_code,
    gift_card_note_type,
    gift_card_req_amt_currency_code,
    gift_card_req_amt,
    balance_lock_id,
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
    paypal_tndr_rslt_payer_account_country
FROM settlement_success_tender_union;
--------------------------------------------------------------------
