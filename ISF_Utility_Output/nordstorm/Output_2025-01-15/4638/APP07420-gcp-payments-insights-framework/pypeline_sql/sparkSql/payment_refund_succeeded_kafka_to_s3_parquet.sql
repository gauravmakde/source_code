-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=payment_refund_and_cancel_s3_parquet;
Task_Name=kafka_to_s3_parquet_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
CREATE TEMPORARY VIEW temp_kafka_payment_refund_succeeded_v2 AS
SELECT * FROM kafka_payment_refund_succeeded_v2;

CREATE TEMPORARY VIEW refund_success_temp AS
SELECT
    ELEMENT_AT(headers, 'Id') AS eventId,
    ELEMENT_AT(headers, 'Type') AS eventType,
    ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-test') AS nord_test,
    ELEMENT_AT(TRANSFORM_KEYS(headers, (k, v) -> LOWER(k)), 'nord-load') AS nord_load,
    eventTime,
    source,
    purchaseId,
    transactionId,
    serviceTicketId,
    reason,
    total,
    merchantIdentifier,
    bankCardRefundResults,
    bankCardRefundResultV2s,
    afterPayLegacyCardRefundResults,
    afterPayVirtualCardRefundResults,
    giftCardIssuedResults,
    giftCardNoteIssuedResults,
    payPalRefundResults,
    payPalBillingAgreementRefundResults,
    nordstromNoteRefundedResults
FROM temp_kafka_payment_refund_succeeded_v2;

--Writing to s3 parquet bucket
INSERT INTO TABLE payment_refund_succeeded_event_parquet PARTITION (
    year, month, day
)
SELECT
    eventId,
    eventTime,
    source,
    purchaseId,
    transactionId,
    serviceTicketId,
    reason,
    total,
    merchantIdentifier,
    bankCardRefundResults,
    bankCardRefundResultV2s,
    afterPayLegacyCardRefundResults,
    afterPayVirtualCardRefundResults,
    giftCardIssuedResults,
    giftCardNoteIssuedResults,
    payPalRefundResults,
    payPalBillingAgreementRefundResults,
    nordstromNoteRefundedResults,
    DATE_FORMAT(eventTime, 'yyyy') AS year,
    DATE_FORMAT(eventTime, 'MM') AS month,
    DATE_FORMAT(eventTime, 'dd') AS day
FROM refund_success_temp
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
