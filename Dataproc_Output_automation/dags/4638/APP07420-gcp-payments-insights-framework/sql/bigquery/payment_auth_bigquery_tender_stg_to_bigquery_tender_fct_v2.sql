------------------------------------------------------------------------------------------------------------------------
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=payment_auth_teradata_v2;
Task_Name=teradata_tender_stg_to_teradata_tender_fct_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
END TRANSACTION;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
CREATE MULTISET VOLATILE TABLE PAYMENT_AUTHORIZATION_TENDER_V2_LDG_TEMP AS (
    SELECT DISTINCT *
    FROM PRD_NAP_STG.PAYMENT_AUTHORIZATION_TENDER_V2_LDG
    QUALIFY
        row_number() OVER (
            PARTITION BY
                EVENT_ID, PURCHASE_IDENTIFIER_ID
            ORDER BY EVENT_TIME DESC
        ) = 1
) WITH DATA PRIMARY INDEX (EVENT_ID, PURCHASE_IDENTIFIER_ID) ON COMMIT PRESERVE ROWS;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
END TRANSACTION;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
DELETE FROM PRD_NAP_BASE_VWS.PAYMENT_AUTHORIZATION_TENDER_V2_FACT
WHERE (
    EVENT_ID,
    PURCHASE_IDENTIFIER_ID
    )
IN (
    SELECT
        EVENT_ID,
        PURCHASE_IDENTIFIER_ID
    FROM PAYMENT_AUTHORIZATION_TENDER_V2_LDG_TEMP
);
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO PRD_NAP_BASE_VWS.PAYMENT_AUTHORIZATION_TENDER_V2_FACT (
    DW_BATCH_DATE,
    DW_SYS_LOAD_TMSTP,
    DW_SYS_UPDT_TMSTP,

    TENDER_TYPE,
    TENDER_RECORD_NAME,
    EVENT_ID,
    EVENT_TIME,
    ACTIVITY_DATE,
    PURCHASE_IDENTIFIER_ID,
    CARD_TYPE,
    CARD_SUB_TYPE,
    TENDER_ITEM_ACCOUNT_NO,
    TENDER_ITEM_ACCOUNT_VALUE_TYPE,
    TOKEN_AUTHORITY,
    TOKEN_DATA_CLASSIFICATION,
    TOKEN_TYPE,
    LAST_FOUR,
    EXPIRATION_DATE,
    APPROVAL_STATUS,
    AUTHORIZATION_CODE,
    EMV_AUTH_INFO,
    PAYMENT_KEY,
    EMV_TAGS_INFO,
    CVV_RESULT_CODE,
    AVS_RESULT_CODE,
    TOKEN_REQUESTOR_ID,
    GIFT_CARD_NOTE_TYPE,
    GIFT_CARD_ACCESS_CODE_VALUE,
    TENDER_CAPTURE_METHOD,
    GIFT_CARD_BALANCE_LOCK_ID,
    PAYPAL_ORDER_ID,
    PAYPAL_PAYER_ID,
    PAYPAL_AUTHORIZATION_ID,
    PAYPAL_BILLING_AGREEMENT_ID_VALUE,
    PAYPAL_PAYER_EMAIL_VALUE,
    PAYPAL_PAYER_ACCOUNT_COUNTRY
)
SELECT
    current_date AS DW_BATCH_DATE,
    current_timestamp(0) AS DW_SYS_LOAD_TMSTP,
    current_timestamp(0) AS DW_SYS_UPDT_TMSTP,

    TENDER_TYPE,
    TENDER_RECORD_NAME,
    EVENT_ID,
    cast(EVENT_TIME AS TIMESTAMP) AS EVENT_TIME,
    cast(cast(EVENT_TIME AS TIMESTAMP) AS DATE) AS ACTIVITY_DATE,
    PURCHASE_IDENTIFIER_ID,
    CARD_TYPE,
    CARD_SUB_TYPE,
    TENDER_ITEM_ACCOUNT_NO,
    TENDER_ITEM_ACCOUNT_VALUE_TYPE,
    TOKEN_AUTHORITY,
    TOKEN_DATA_CLASSIFICATION,
    TOKEN_TYPE,
    LAST_FOUR,
    EXPIRATION_DATE,
    APPROVAL_STATUS,
    AUTHORIZATION_CODE,
    EMV_AUTH_INFO,
    PAYMENT_KEY,
    EMV_TAGS_INFO,
    CVV_RESULT_CODE,
    AVS_RESULT_CODE,
    TOKEN_REQUESTOR_ID,
    GIFT_CARD_NOTE_TYPE,
    GIFT_CARD_ACCESS_CODE_VALUE,
    TENDER_CAPTURE_METHOD,
    GIFT_CARD_BALANCE_LOCK_ID,
    PAYPAL_ORDER_ID,
    PAYPAL_PAYER_ID,
    PAYPAL_AUTHORIZATION_ID,
    PAYPAL_BILLING_AGREEMENT_ID_VALUE,
    PAYPAL_PAYER_EMAIL_VALUE,
    PAYPAL_PAYER_ACCOUNT_COUNTRY
FROM PAYMENT_AUTHORIZATION_TENDER_V2_LDG_TEMP;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
DELETE FROM PRD_NAP_STG.PAYMENT_AUTHORIZATION_TENDER_V2_LDG;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
SET QUERY_BAND = NONE FOR SESSION; -- noqa
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
END TRANSACTION;
------------------------------------------------------------------------------------------------------------------------
