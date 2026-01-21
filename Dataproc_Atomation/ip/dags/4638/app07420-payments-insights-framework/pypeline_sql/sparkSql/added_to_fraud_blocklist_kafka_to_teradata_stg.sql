-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=fraud_blocklist_activity_teradata;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
CREATE TEMPORARY VIEW temp_kafka_added_to_fraud_blocklist AS
SELECT * FROM kafka_added_to_fraud_blocklist;

-- building the fraud_blocklist_activity table
--reading the fraud_blocklist_activity fields
CREATE TEMPORARY VIEW added_to_fraud_blocklist_temp AS
SELECT
    cast(eventTime AS TIMESTAMP) AS event_time,
    'AddedToFraudBlocklist' AS activity_type,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load,
    customer.id as customer_id,
    customer.idType as customer_id_type,
    reason AS reason
FROM temp_kafka_added_to_fraud_blocklist;

--inserting into the teradata stg table
INSERT INTO TABLE added_to_fraud_blocklist_stg
SELECT DISTINCT
    event_time,
    activity_type,
    customer_id,
    customer_id_type,
    reason
FROM added_to_fraud_blocklist_temp
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
