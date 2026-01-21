------------------------------------------------------------------------------------------------------------------------
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=fraud_decision_lifecycle_teradata;
Task_Name=teradata_stg_to_teradata_fct_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
END TRANSACTION;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
CREATE MULTISET VOLATILE TABLE FRAUD_DECISION_LIFECYCLE_LDG_TEMP AS (
    SELECT DISTINCT *
    FROM PREPROD_NAP_PYMNT_FRAUD_STG.FRAUD_DECISION_LIFECYCLE_LDG
    QUALIFY
        row_number() OVER (
            PARTITION BY
                EVENT_ID, ORDER_ID
            ORDER BY EVENT_TIME DESC
        ) = 1
) WITH DATA PRIMARY INDEX (EVENT_ID, ORDER_ID) ON COMMIT PRESERVE ROWS;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
END TRANSACTION;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
DELETE FROM PREPROD_NAP_PYMNT_FRAUD_BASE_VWS.FRAUD_DECISION_LIFECYCLE_FACT
WHERE (
    EVENT_ID,
    ORDER_ID
    )
IN (
    SELECT
        EVENT_ID,
        ORDER_ID
    FROM FRAUD_DECISION_LIFECYCLE_LDG_TEMP
);
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
INSERT INTO PREPROD_NAP_PYMNT_FRAUD_BASE_VWS.FRAUD_DECISION_LIFECYCLE_FACT (
    ORDER_ID,
    ORDER_LINE_ID,
    AUTHORITY,
    FRAUD_DECISION,
    DECISION_DETAIL,
    CHANNEL_COUNTRY,
    CHANNEL_BRAND,
    SELLING_CHANNEL,
    STORE_NUMBER,
    REGISTER,
    FEATURE,
    SERVICE_NAME,
    SERVICE_TICKET_ID,
    METADATA_CREATED_TIME,
    METADATA_LAST_UPDATED_TIME,
    METADATA_LAST_TRIGGERING_EVENT_NAME,
    EVENT_ID,
    EVENT_TIME,
    EVENT_DATE,
    DW_BATCH_DATE,
    DW_SYS_LOAD_TMSTP,
    DW_SYS_UPDT_TMSTP
)
SELECT
    ORDER_ID,
    ORDER_LINE_ID,
    AUTHORITY,
    FRAUD_DECISION,
    DECISION_DETAIL,
    CHANNEL_COUNTRY,
    CHANNEL_BRAND,
    SELLING_CHANNEL,
    STORE_NUMBER,
    REGISTER,
    FEATURE,
    SERVICE_NAME,
    SERVICE_TICKET_ID,
    cast(METADATA_CREATED_TIME AS TIMESTAMP) AS METADATA_CREATED_TIME,
    cast(METADATA_LAST_UPDATED_TIME AS TIMESTAMP) AS METADATA_LAST_UPDATED_TIME,
    METADATA_LAST_TRIGGERING_EVENT_NAME,
    EVENT_ID,
    cast(EVENT_TIME AS TIMESTAMP) AS EVENT_TIME,
    cast(cast(EVENT_TIME AS TIMESTAMP) AS DATE) AS EVENT_DATE,
    current_date AS DW_BATCH_DATE,
    current_timestamp AS DW_SYS_LOAD_TMSTP,
    current_timestamp AS DW_SYS_UPDT_TMSTP
FROM FRAUD_DECISION_LIFECYCLE_LDG_TEMP;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
DELETE FROM PREPROD_NAP_PYMNT_FRAUD_STG.FRAUD_DECISION_LIFECYCLE_LDG;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
SET QUERY_BAND = NONE FOR SESSION; -- noqa
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
END TRANSACTION;
------------------------------------------------------------------------------------------------------------------------
