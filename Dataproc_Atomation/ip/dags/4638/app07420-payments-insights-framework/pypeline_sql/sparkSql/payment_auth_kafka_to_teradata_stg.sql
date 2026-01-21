-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

CREATE TEMPORARY VIEW temp_object_model AS
SELECT *
FROM kafka_customer_payment_authorization_analytical_avro;

CREATE TEMPORARY VIEW temp_kafka_customer_payment_authorization_analytical_avro AS
SELECT
    id AS id,
    objectmetadata.lastupdatedtime AS last_updated_time,
    objectmetadata.createdtime AS created_timestamp,
    eventtime AS event_time,
    cast(eventtime AS DATE) AS event_date,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    purchaseid.type AS purchase_identifier_type,
    purchaseid.id AS purchase_identifier_id,
    amount.currencycode AS amount_currency_code,
    cast(
        cast(amount.units AS INTEGER) + (amount.nanos / 1000000000) AS DECIMAL (38, 9)
    ) AS total_amount,
    merchantIdentifier AS merchant_identifier,
    authorizationtype AS authorization_type,
    tendertype AS tender_type,
    failurereason AS failure_reason,
    CASE
        WHEN (paypalbillingagreementauthorizationdetails IS NOT NULL AND paypalbillingagreementauthorizationdetails.payeraccountcountry IS NOT NULL)
            THEN paypalbillingagreementauthorizationdetails.payeraccountcountry
        ELSE ''
    END AS payer_account_country,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.token.token.value
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.token.token.value
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL)
            THEN giftcardnoteauthorizationdetails.giftcardnote.accountnumber.value
        ELSE ''
    END AS tender_item_account_no,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN 'bank_card_token_value'
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN 'bank_card_token_value'
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL)
            THEN 'gift_card_account_number_value'
        ELSE ''
    END AS tender_item_account_value_type,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.token.token.authority
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.token.token.authority
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL)
            THEN giftcardnoteauthorizationdetails.giftcardnote.accountnumber.authority
        ELSE ''
    END AS token_authority,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.token.token.dataclassification
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.token.token.dataclassification
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL)
            THEN giftcardnoteauthorizationdetails.giftcardnote.accountnumber.dataclassification
        ELSE ''
    END AS token_data_classification,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.token.tokentype
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.tenderdetails.token IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.token.tokentype
        ELSE ''
    END AS token_type,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL)
            THEN bankcardauthorizationdetails.tokenrequestorid
        ELSE ''
    END AS token_requestor_id,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.cardtypeinfo.cardtype
        WHEN (afterpayauthorizationdetails IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.cardtypeinfo.cardtype
        ELSE ''
    END AS card_type,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.cardtypeinfo.cardsubtype
        WHEN (afterpayauthorizationdetails IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.cardtypeinfo.cardsubtype
        ELSE ''
    END AS card_subtype,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.tenderdetails.lastfour IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.lastfour.value
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.tenderdetails.lastfour IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.lastfour.value
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL)
            THEN giftcardnoteauthorizationdetails.giftcardnote.lastfour.value
        ELSE ''
    END AS last_four,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.tenderdetails.expirationdate IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.expirationdate.month
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.tenderdetails.expirationdate IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.expirationdate.month
        ELSE ''
    END AS expiration_date_month,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.tenderdetails.expirationdate IS NOT NULL)
            THEN bankcardauthorizationdetails.tenderdetails.expirationdate.year
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.tenderdetails.expirationdate IS NOT NULL)
            THEN afterpayauthorizationdetails.tenderdetails.expirationdate.year
    END AS expiration_date_year,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN bankcardauthorizationdetails.paymentauthorizationinfo.approvalstatus
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN afterpayauthorizationdetails.paymentauthorizationinfo.approvalstatus
        ELSE ''
    END AS approval_status,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN bankcardauthorizationdetails.paymentauthorizationinfo.authorizationcode
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN afterpayauthorizationdetails.paymentauthorizationinfo.authorizationcode
        ELSE ''
    END AS authorization_code,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN bankcardauthorizationdetails.paymentauthorizationinfo.emvauthorizationinfo
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN afterpayauthorizationdetails.paymentauthorizationinfo.emvauthorizationinfo
        ELSE ''
    END AS emv_authorization_info,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN bankcardauthorizationdetails.paymentauthorizationinfo.paymentkey
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN afterpayauthorizationdetails.paymentauthorizationinfo.paymentkey
        ELSE ''
    END AS payment_key,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN bankcardauthorizationdetails.paymentauthorizationinfo.emvtagsinfo
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN afterpayauthorizationdetails.paymentauthorizationinfo.emvtagsinfo
        ELSE ''
    END AS emv_tags_info,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN bankcardauthorizationdetails.paymentauthorizationinfo.cvvresultcode
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN afterpayauthorizationdetails.paymentauthorizationinfo.cvvresultcode
        ELSE ''
    END AS cvv_result_code,
    CASE
        WHEN (bankcardauthorizationdetails IS NOT NULL AND bankcardauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN bankcardauthorizationdetails.paymentauthorizationinfo.avsresultcode
        WHEN (afterpayauthorizationdetails IS NOT NULL AND afterpayauthorizationdetails.paymentauthorizationinfo IS NOT NULL)
            THEN afterpayauthorizationdetails.paymentauthorizationinfo.avsresultcode
        ELSE ''
    END AS avs_result_code,
    CASE
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL)
            THEN giftcardnoteauthorizationdetails.balancelockid
        ELSE ''
    END AS gift_card_balance_lock_id,
    CASE
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL AND giftcardnoteauthorizationdetails.giftcardnote.accesscode IS NOT NULL)
            THEN giftcardnoteauthorizationdetails.giftcardnote.accesscode.value
        ELSE ''
    END AS gift_card_access_code_value,
    CASE
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL)
            THEN giftcardnoteauthorizationdetails.giftcardnote.giftcardnotetype
        ELSE ''
    END AS gift_card_note_type,
    CASE
        WHEN (giftcardnoteauthorizationdetails IS NOT NULL)
            THEN giftcardnoteauthorizationdetails.giftcardnote.tendercapturemethod
        ELSE ''
    END AS gift_card_tender_capture_method
FROM temp_object_model;

--Sink to STG
INSERT INTO TABLE payment_authorization_stg
SELECT
    id,
    last_updated_time,
    created_timestamp,
    event_time,
    event_date,
    channel_country,
    channel_brand,
    selling_channel,
    purchase_identifier_type,
    purchase_identifier_id,
    amount_currency_code,
    total_amount,
    merchant_identifier,
    authorization_type,
    tender_type,
    failure_reason,
    payer_account_country,
    tender_item_account_no,
    tender_item_account_value_type,
    token_authority,
    token_data_classification,
    token_type,
    token_requestor_id,
    card_type,
    card_subtype,
    last_four,
    expiration_date_month,
    expiration_date_year,
    approval_status,
    authorization_code,
    emv_authorization_info,
    payment_key,
    emv_tags_info,
    cvv_result_code,
    avs_result_code,
    gift_card_balance_lock_id,
    gift_card_access_code_value,
    gift_card_note_type,
    gift_card_tender_capture_method
FROM temp_kafka_customer_payment_authorization_analytical_avro;
