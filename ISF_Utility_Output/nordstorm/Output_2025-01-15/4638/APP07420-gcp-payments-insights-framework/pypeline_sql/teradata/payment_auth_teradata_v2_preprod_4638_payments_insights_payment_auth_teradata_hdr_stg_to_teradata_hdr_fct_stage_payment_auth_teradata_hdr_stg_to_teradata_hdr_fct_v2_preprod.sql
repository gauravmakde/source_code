------------------------------------------------------------------------------------------------------------------------
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=payment_auth_teradata_v2;
Task_Name=teradata_hdr_stg_to_teradata_hdr_fct_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
END TRANSACTION;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
CREATE MULTISET VOLATILE TABLE PAYMENT_AUTHORIZATION_HDR_V2_LDG_TEMP AS (
    SELECT DISTINCT *
    FROM PREPROD_NAP_STG.PAYMENT_AUTHORIZATION_HDR_V2_LDG
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
DELETE FROM PREPROD_NAP_BASE_VWS.PAYMENT_AUTHORIZATION_HDR_V2_FACT
WHERE (
    EVENT_ID,
    PURCHASE_IDENTIFIER_ID
    )
IN (
    SELECT
        EVENT_ID,
        PURCHASE_IDENTIFIER_ID
    FROM PAYMENT_AUTHORIZATION_HDR_V2_LDG_TEMP
);
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO PREPROD_NAP_BASE_VWS.PAYMENT_AUTHORIZATION_HDR_V2_FACT (
    DW_BATCH_DATE,
    DW_SYS_LOAD_TMSTP,
    DW_SYS_UPDT_TMSTP,

    EVENT_ID,
    EVENT_TIME,
    ACTIVITY_DATE,
    OBJ_CREATED_TMSTP,
    OBJ_LAST_UPDT_TMSTP,
    CHANNEL_COUNTRY,
    CHANNEL_BRAND,
    SELLING_CHANNEL,
    PURCHASE_IDENTIFIER_TYPE,
    PURCHASE_IDENTIFIER_ID,
    POS_STORE,
    POS_REGISTER,
    POS_TRANSACTION,
    POS_BUSINESS_DATE,
    STORE_TRANSACTION_TRANSACTION_ID,
    STORE_TRANSACTION_SESSION_ID,
    STORE_TRANSACTION_PURCHASE_ID,
    CURRENCY_CODE,
    TOTAL_AMT,
    AUTHORIZATION_TYPE,
    MERCHANT_IDENTIFIER,
    FAILURE_REASON,
    FAILURE_SOURCE,
    FAILURE_REASON_CODE,
    FAILURE_REASON_MESSAGE
)
SELECT
    current_date AS DW_BATCH_DATE,
    current_timestamp(0) AS DW_SYS_LOAD_TMSTP,
    current_timestamp(0) AS DW_SYS_UPDT_TMSTP,

    EVENT_ID,
    cast(EVENT_TIME AS TIMESTAMP) AS EVENT_TIME,
    cast(cast(EVENT_TIME AS TIMESTAMP) AS DATE) AS ACTIVITY_DATE,
    cast(OBJ_CREATED_TMSTP AS TIMESTAMP) AS OBJ_CREATED_TMSTP,
    cast(OBJ_LAST_UPDT_TMSTP AS TIMESTAMP) AS OBJ_LAST_UPDT_TMSTP,
    CHANNEL_COUNTRY,
    CHANNEL_BRAND,
    SELLING_CHANNEL,
    PURCHASE_IDENTIFIER_TYPE,
    PURCHASE_IDENTIFIER_ID,
    POS_STORE,
    POS_REGISTER,
    POS_TRANSACTION,
    cast(POS_BUSINESS_DATE AS DATE) AS POS_BUSINESS_DATE,
    STORE_TRANSACTION_TRANSACTION_ID,
    STORE_TRANSACTION_SESSION_ID,
    STORE_TRANSACTION_PURCHASE_ID,
    CURRENCY_CODE,
    TOTAL_AMT,
    AUTHORIZATION_TYPE,
    MERCHANT_IDENTIFIER,
    FAILURE_REASON,
    FAILURE_SOURCE,
    FAILURE_REASON_CODE,
    FAILURE_REASON_MESSAGE
FROM PAYMENT_AUTHORIZATION_HDR_V2_LDG_TEMP;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
SET QUERY_BAND = NONE FOR SESSION; -- noqa
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
END TRANSACTION;
------------------------------------------------------------------------------------------------------------------------
