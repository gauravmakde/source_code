-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app06788;
Task_Name=credit_application_approved_kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

-- Read Kafka into temp table
CREATE TEMPORARY VIEW temp_credit_application_approved
AS
SELECT * FROM kafka_credit_application_approved;

CREATE TEMPORARY VIEW credit_application_approved_temp
AS
SELECT
    applicationid AS application_id,
    'Approved' AS application_decision_status,
    customer.id AS customer_id,
    customer.idtype AS customer_id_type,
    clientreferenceid AS client_reference_id,
    producttype AS credit_card_product_type,
    productsubtype AS credit_card_product_subtype,
    cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP) AS event_time,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load
FROM temp_credit_application_approved;

INSERT INTO TABLE credit_application_approved_ldg
SELECT
    event_time,
    application_id,
    application_decision_status,
    customer_id,
    customer_id_type,
    client_reference_id,
    credit_card_product_type,
    credit_card_product_subtype
FROM credit_application_approved_temp
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
