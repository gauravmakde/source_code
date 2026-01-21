-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
DAG_ID=card_to_wallet_threshold_violated_teradata;
Task_Name=kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
CREATE TEMPORARY VIEW temp_kafka_card_to_wallet_threshold_violated AS
SELECT * FROM kafka_card_to_wallet_threshold_violated;

-- building the card_to_wallet_threshold_violated table
--reading the card_to_wallet_threshold_violated fields
CREATE TEMPORARY VIEW card_to_wallet_threshold_violated_temp AS
SELECT
    cast(eventTime AS TIMESTAMP) AS event_time,
    customer.id as customer_id,
    customer.idType as customer_id_type,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    violationreason AS violation_reason,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load
FROM temp_kafka_card_to_wallet_threshold_violated;

--inserting into the teradata stg table
INSERT INTO TABLE card_to_wallet_threshold_violated_stg
SELECT DISTINCT
    event_time,
    customer_id,
    customer_id_type,
    channel_country,
    channel_brand,
    selling_channel,
    violation_reason
FROM card_to_wallet_threshold_violated_temp
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
