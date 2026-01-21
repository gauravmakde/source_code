-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=payment_refund_and_cancel_s3_parquet;
Task_Name=kafka_to_s3_parquet_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
CREATE TEMPORARY VIEW temp_kafka_payment_cancel_failed AS
SELECT * FROM kafka_payment_cancel_failed;

CREATE TEMPORARY VIEW cancel_failed_temp AS
SELECT
    element_at(headers, 'Id') AS eventId,
    cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS eventTime,
    element_at(headers, 'Type') AS eventType,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load,
    orderLineId,
    serviceLineId,
    reason,
    total,
    merchantIdentifier,
    bankCardOrderCanceledResult,
    bankCardOrderCanceledResultV2,
    afterPayCanceledResult,
    giftCardIssuedResults,
    giftCardNoteIssuedResults,
    giftCardRefundResults,
    giftCardNoteUnlockResults,
    payPalOrderCanceledResult,
    payPalBillingAgreementOrderCanceledResult,
    nordstromNoteRefundedResults,
    serviceTicketId,
    failureReason
FROM temp_kafka_payment_cancel_failed;

--Writing to s3 parquet bucket
INSERT INTO TABLE payment_cancel_failed_event_parquet PARTITION (
    year, month, day
)
SELECT
    eventId,
    eventTime,
    orderLineId,
    serviceLineId,
    reason,
    total,
    merchantIdentifier,
    bankCardOrderCanceledResult,
    bankCardOrderCanceledResultV2,
    afterPayCanceledResult,
    giftCardIssuedResults,
    giftCardNoteIssuedResults,
    giftCardRefundResults,
    giftCardNoteUnlockResults,
    payPalOrderCanceledResult,
    payPalBillingAgreementOrderCanceledResult,
    nordstromNoteRefundedResults,
    serviceTicketId,
    failureReason,
    date_format(eventTime, 'yyyy') AS year,
    date_format(eventTime, 'MM') AS month,
    date_format(eventTime, 'dd') AS day
FROM cancel_failed_temp
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
