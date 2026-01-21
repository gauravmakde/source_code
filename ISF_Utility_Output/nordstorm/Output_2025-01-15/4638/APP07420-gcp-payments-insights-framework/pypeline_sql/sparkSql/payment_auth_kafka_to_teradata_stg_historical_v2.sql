------------------------------------------------------------------------------------------------------------------------
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=payment_auth_teradata_v2_historical;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
CREATE TEMPORARY VIEW CUSTOMER_PAYMENT_AUTHORIZATION_ANALYTICAL AS
SELECT value.*
FROM payments_object_model.payment_authorization_parquet;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO TABLE PAYMENT_AUTHORIZATION_HDR_V2_LDG
SELECT DISTINCT
    id AS event_id,
    date_format(cast(eventTime AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss.SSS') AS event_time,
    date_format(cast(objectMetadata.createdTime AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss.SSS') AS obj_created_tmstp,
    date_format(cast(objectMetadata.lastUpdatedTime AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss.SSS') AS obj_last_updt_tmstp,

    channel.channelCountry AS channel_country,
    channel.channelBrand AS channel_brand,
    channel.sellingChannel AS selling_channel,
    purchaseId.type AS purchase_identifier_type,
    purchaseId.id AS purchase_identifier_id,
    CASE
        WHEN purchaseId.storeRegisterTransactionDate IS NOT NULL
            THEN substring(purchaseId.storeRegisterTransactionDate.store, 0, 10)
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL
            THEN substring(purchaseId.storeTransactionIdentifierDetail.purchaseId.store, 0, 10)
    END AS pos_store,
    CASE
        WHEN purchaseId.storeRegisterTransactionDate IS NOT NULL
            THEN substring(purchaseId.storeRegisterTransactionDate.register, 0, 10)
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL
            THEN substring(purchaseId.storeTransactionIdentifierDetail.purchaseId.register, 0, 10)
    END AS pos_register,
    CASE
        WHEN purchaseId.storeRegisterTransactionDate IS NOT NULL
            THEN substring(purchaseId.storeRegisterTransactionDate.transaction, 0, 10)
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL
            THEN substring(purchaseId.storeTransactionIdentifierDetail.purchaseId.transaction, 0, 10)
    END AS pos_transaction,
    CASE
        WHEN purchaseId.storeRegisterTransactionDate IS NOT NULL
            THEN cast(purchaseId.storeRegisterTransactionDate.businessDate AS DATE)
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL
            THEN cast(purchaseId.storeTransactionIdentifierDetail.purchaseId.businessDate AS DATE)
    END AS pos_business_date,
    CASE
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL
            THEN purchaseId.storeTransactionIdentifierDetail.transactionId
    END AS store_transaction_transaction_id,
    CASE
        WHEN purchaseId.storeTransactionIdentifierDetail IS NOT NULL
            THEN purchaseId.storeTransactionIdentifierDetail.sessionId
    END AS store_transaction_session_id,
    CASE
        WHEN purchaseId IS NOT NULL
            THEN purchaseId.id
    END AS store_transaction_purchase_id,
    amount.currencyCode AS currency_code,
    cast(cast(amount.units AS INTEGER) + (amount.nanos / 1000000000) AS DECIMAL (14, 4)) AS total_amt,
    authorizationType AS authorization_type,
    merchantIdentifier AS merchant_identifier,
    failureReason AS failure_reason,
    failureSource AS failure_source,
    failureReasonCode AS failure_reason_code,
    failureReasonMessage AS failure_reason_message
FROM CUSTOMER_PAYMENT_AUTHORIZATION_ANALYTICAL;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO TABLE PAYMENT_AUTHORIZATION_TENDER_V2_LDG
SELECT DISTINCT
    id AS event_id,
    date_format(cast(eventTime AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss.SSS') AS event_time,
    'PayPalAuthorizationDetails' AS tender_record_name,
    'PAYPAL' AS tender_type,
    purchaseId.id AS purchase_identifier_id,
    '' AS card_type,
    '' AS card_sub_type,
    '' AS tender_item_account_no,
    '' AS tender_item_account_value_type,
    '' AS token_authority,
    '' AS token_data_classification,
    '' AS token_type,
    '' AS last_four,
    '' AS expiration_date,
    '' AS approval_status,
    '' AS authorization_code,
    '' AS emv_auth_info,
    '' AS payment_key,
    '' AS emv_tags_info,
    '' AS cvv_result_code,
    '' AS avs_result_code,
    '' AS token_requestor_id,
    '' AS gift_card_note_type,
    '' AS gift_card_access_code_value,
    '' AS tender_capture_method,
    '' AS gift_card_balance_lock_id,
    payPalAuthorizationDetails.payPalOrderId AS paypal_order_id,
    payPalAuthorizationDetails.payerId AS paypal_payer_id,
    payPalAuthorizationDetails.authorizationId AS paypal_authorization_id,
    '' AS paypal_billing_agreement_id_value,
    '' AS paypal_payer_email_value,
    '' AS paypal_payer_account_country
FROM CUSTOMER_PAYMENT_AUTHORIZATION_ANALYTICAL
WHERE payPalAuthorizationDetails IS NOT NULL;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO TABLE PAYMENT_AUTHORIZATION_TENDER_V2_LDG
SELECT DISTINCT
    id AS event_id,
    date_format(cast(eventTime AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss.SSS') AS event_time,
    'PayPalBillingAgreementAuthorizationDetails' AS tender_record_name,
    'PAYPAL_BILLING_AGREEMENT' AS tender_type,
    purchaseId.id AS purchase_identifier_id,
    '' AS card_type,
    '' AS card_sub_type,
    '' AS tender_item_account_no,
    '' AS tender_item_account_value_type,
    '' AS token_authority,
    '' AS token_data_classification,
    '' AS token_type,
    '' AS last_four,
    '' AS expiration_date,
    '' AS approval_status,
    '' AS authorization_code,
    '' AS emv_auth_info,
    '' AS payment_key,
    '' AS emv_tags_info,
    '' AS cvv_result_code,
    '' AS avs_result_code,
    '' AS token_requestor_id,
    '' AS gift_card_note_type,
    '' AS gift_card_access_code_value,
    '' AS tender_capture_method,
    '' AS gift_card_balance_lock_id,
    '' AS paypal_order_id,
    payPalBillingAgreementAuthorizationDetails.payerId AS paypal_payer_id,
    payPalBillingAgreementAuthorizationDetails.authorizationId AS paypal_authorization_id,
    payPalBillingAgreementAuthorizationDetails.billingAgreementId.value AS paypal_billing_agreement_id_value,
    payPalBillingAgreementAuthorizationDetails.payerEmail.value AS paypal_payer_email_value,
    payPalBillingAgreementAuthorizationDetails.payerAccountCountry AS paypal_payer_account_country
FROM CUSTOMER_PAYMENT_AUTHORIZATION_ANALYTICAL
WHERE payPalBillingAgreementAuthorizationDetails IS NOT NULL;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO TABLE PAYMENT_AUTHORIZATION_TENDER_V2_LDG
SELECT DISTINCT
    id AS event_id,
    date_format(cast(eventTime AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss.SSS') AS event_time,
    'BankCardAuthorizationDetails' AS tender_record_name,
    'CREDIT_CARD' AS tender_type,
    purchaseId.id AS purchase_identifier_id,
    bankCardAuthorizationDetails.tenderDetails.cardTypeinfo.cardType AS card_type,
    bankCardAuthorizationDetails.tenderDetails.cardTypeinfo.cardSubType AS card_sub_type,
    CASE
        WHEN bankCardAuthorizationDetails.tenderDetails.token IS NOT NULL
            THEN bankCardAuthorizationDetails.tenderDetails.token.token.value
    END AS tender_item_account_no,
    'bank_card_token_value' AS tender_item_account_value_type,
    CASE
        WHEN bankCardAuthorizationDetails.tenderDetails.token IS NOT NULL
            THEN bankCardAuthorizationDetails.tenderDetails.token.token.authority
    END AS token_authority,
    CASE
        WHEN bankCardAuthorizationDetails.tenderDetails.token IS NOT NULL
            THEN bankCardAuthorizationDetails.tenderDetails.token.token.dataClassification
    END AS token_data_classification,
    CASE
        WHEN bankCardAuthorizationDetails.tenderDetails.token IS NOT NULL
            THEN bankCardAuthorizationDetails.tenderDetails.token.tokenType
    END AS token_type,
    CASE
        WHEN bankCardAuthorizationDetails.tenderDetails.lastFour IS NOT NULL
            THEN bankCardAuthorizationDetails.tenderDetails.lastFour.value
    END AS last_four,
    CASE
        WHEN bankCardAuthorizationDetails.tenderDetails.expirationDate IS NOT NULL
            THEN concat(from_unixtime(unix_timestamp(bankCardAuthorizationDetails.tenderDetails.expirationDate.month, 'MMMM'), 'MM'), '/', (bankCardAuthorizationDetails.tenderDetails.expirationDate.year))
    END AS expiration_date,
    CASE
        WHEN bankCardAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN bankCardAuthorizationDetails.paymentAuthorizationInfo.approvalStatus
    END AS approval_status,
    CASE
        WHEN bankCardAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN bankCardAuthorizationDetails.paymentAuthorizationInfo.authorizationCode
    END AS authorization_code,
    CASE
        WHEN bankCardAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN bankCardAuthorizationDetails.paymentAuthorizationInfo.emvAuthorizationInfo
    END AS emv_auth_info,
    CASE
        WHEN bankCardAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN bankCardAuthorizationDetails.paymentAuthorizationInfo.paymentKey
    END AS payment_key,
    CASE
        WHEN bankCardAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN bankCardAuthorizationDetails.paymentAuthorizationInfo.emvTagsInfo
    END AS emv_tags_info,
    CASE
        WHEN bankCardAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN bankCardAuthorizationDetails.paymentAuthorizationInfo.cvvResultCode
    END AS cvv_result_code,
    CASE
        WHEN bankCardAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN bankCardAuthorizationDetails.paymentAuthorizationInfo.avsResultCode
    END AS avs_result_code,
    bankCardAuthorizationDetails.tokenRequestorId AS token_requestor_id,
    '' AS gift_card_note_type,
    '' AS gift_card_access_code_value,
    '' AS tender_capture_method,
    '' AS gift_card_balance_lock_id,
    '' AS paypal_order_id,
    '' AS paypal_payer_id,
    '' AS paypal_authorization_id,
    '' AS paypal_billing_agreement_id_value,
    '' AS paypal_payer_email_value,
    '' AS paypal_payer_account_country
FROM CUSTOMER_PAYMENT_AUTHORIZATION_ANALYTICAL
WHERE bankCardAuthorizationDetails IS NOT NULL;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO TABLE PAYMENT_AUTHORIZATION_TENDER_V2_LDG
SELECT DISTINCT
    id AS event_id,
    date_format(cast(eventTime AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss.SSS') AS event_time,
    'AfterpayAuthorizationDetails' AS tender_record_name,
    'AFTERPAY' AS tender_type,
    purchaseId.id AS purchase_identifier_id,
    afterpayAuthorizationDetails.tenderDetails.cardTypeinfo.cardType AS card_type,
    afterpayAuthorizationDetails.tenderDetails.cardTypeinfo.cardSubType AS card_sub_type,
    CASE
        WHEN afterpayAuthorizationDetails.tenderDetails.token IS NOT NULL
            THEN afterpayAuthorizationDetails.tenderDetails.token.token.value
    END AS tender_item_account_no,
    'bank_card_token_value' AS tender_item_account_value_type,
    CASE
        WHEN afterpayAuthorizationDetails.tenderDetails.token IS NOT NULL
            THEN afterpayAuthorizationDetails.tenderDetails.token.token.authority
    END AS token_authority,
    CASE
        WHEN afterpayAuthorizationDetails.tenderDetails.token IS NOT NULL
            THEN afterpayAuthorizationDetails.tenderDetails.token.token.dataClassification
    END AS token_data_classification,
    CASE
        WHEN afterpayAuthorizationDetails.tenderDetails.token IS NOT NULL
            THEN afterpayAuthorizationDetails.tenderDetails.token.tokenType
    END AS token_type,
    CASE
        WHEN afterpayAuthorizationDetails.tenderDetails.lastFour IS NOT NULL
            THEN afterpayAuthorizationDetails.tenderDetails.lastFour.value
    END AS last_four,
    CASE
        WHEN afterpayAuthorizationDetails.tenderDetails.expirationDate IS NOT NULL
            THEN concat(from_unixtime(unix_timestamp(afterpayAuthorizationDetails.tenderDetails.expirationDate.month, 'MMMM'), 'MM'), '/', (afterpayAuthorizationDetails.tenderDetails.expirationDate.year))
    END AS expiration_date,
    CASE
        WHEN afterpayAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN afterpayAuthorizationDetails.paymentAuthorizationInfo.approvalStatus
    END AS approval_status,
    CASE
        WHEN afterpayAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN afterpayAuthorizationDetails.paymentAuthorizationInfo.authorizationCode
    END AS authorization_code,
    CASE
        WHEN afterpayAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN afterpayAuthorizationDetails.paymentAuthorizationInfo.emvAuthorizationInfo
    END AS emv_auth_info,
    CASE
        WHEN afterpayAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN afterpayAuthorizationDetails.paymentAuthorizationInfo.paymentKey
    END AS payment_key,
    CASE
        WHEN afterpayAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN afterpayAuthorizationDetails.paymentAuthorizationInfo.emvTagsInfo
    END AS emv_tags_info,
    CASE
        WHEN afterpayAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN afterpayAuthorizationDetails.paymentAuthorizationInfo.cvvResultCode
    END AS cvv_result_code,
    CASE
        WHEN afterpayAuthorizationDetails.paymentAuthorizationInfo IS NOT NULL
            THEN afterpayAuthorizationDetails.paymentAuthorizationInfo.avsResultCode
    END AS avs_result_code,
    '' AS token_requestor_id,
    '' AS gift_card_note_type,
    '' AS gift_card_access_code_value,
    '' AS tender_capture_method,
    '' AS gift_card_balance_lock_id,
    '' AS paypal_order_id,
    '' AS paypal_payer_id,
    '' AS paypal_authorization_id,
    '' AS paypal_billing_agreement_id_value,
    '' AS paypal_payer_email_value,
    '' AS paypal_payer_account_country
FROM CUSTOMER_PAYMENT_AUTHORIZATION_ANALYTICAL
WHERE afterpayAuthorizationDetails IS NOT NULL;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO TABLE PAYMENT_AUTHORIZATION_TENDER_V2_LDG
SELECT DISTINCT
    id AS event_id,
    date_format(cast(eventTime AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss.SSS') AS event_time,
    'GiftCardNoteAuthorizationDetails' AS tender_record_name,
    CASE
        WHEN giftCardNoteAuthorizationDetails.giftCardNote.giftCardNoteType = 'NORDSTROM_NOTE'
            THEN 'NORDSTROM_NOTE'
        ELSE 'GIFT_CARD'
    END AS tender_type,
    purchaseId.id AS purchase_identifier_id,
    '' AS card_type,
    '' AS card_sub_type,
    giftCardNoteAuthorizationDetails.giftCardNote.accountNumber.value AS tender_item_account_no,
    'gift_card_account_number_value' AS tender_item_account_value_type,
    giftCardNoteAuthorizationDetails.giftCardNote.accountNumber.authority AS token_authority,
    giftCardNoteAuthorizationDetails.giftCardNote.accountNumber.dataClassification AS token_data_classification,
    '' AS token_type,
    giftCardNoteAuthorizationDetails.giftCardNote.lastFour.value AS last_four,
    '' AS expiration_date,
    '' AS approval_status,
    '' AS authorization_code,
    '' AS emv_auth_info,
    '' AS payment_key,
    '' AS emv_tags_info,
    '' AS cvv_result_code,
    '' AS avs_result_code,
    '' AS token_requestor_id,
    giftCardNoteAuthorizationDetails.giftCardNote.giftCardNoteType AS gift_card_note_type,
    CASE
        WHEN giftCardNoteAuthorizationDetails.giftCardNote.accessCode IS NOT NULL
            THEN giftCardNoteAuthorizationDetails.giftCardNote.accessCode.value
    END AS gift_card_access_code_value,
    giftCardNoteAuthorizationDetails.giftCardNote.tenderCaptureMethod AS tender_capture_method,
    giftCardNoteAuthorizationDetails.balanceLockId AS gift_card_balance_lock_id,
    '' AS paypal_order_id,
    '' AS paypal_payer_id,
    '' AS paypal_authorization_id,
    '' AS paypal_billing_agreement_id_value,
    '' AS paypal_payer_email_value,
    '' AS paypal_payer_account_country
FROM CUSTOMER_PAYMENT_AUTHORIZATION_ANALYTICAL
WHERE giftCardNoteAuthorizationDetails IS NOT NULL;
------------------------------------------------------------------------------------------------------------------------
