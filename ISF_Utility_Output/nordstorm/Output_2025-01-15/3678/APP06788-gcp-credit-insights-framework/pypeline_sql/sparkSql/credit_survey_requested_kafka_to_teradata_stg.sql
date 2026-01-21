-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app06788;
Task_Name=credit_survey_requested_kafka_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

-- Read Kafka into temp table
CREATE TEMPORARY VIEW temp_credit_survey_requested_object
AS
SELECT * FROM kafka_customer_credit_customer_support_contacted_survey_requested_avro;

CREATE TEMPORARY VIEW temp_credit_survey_requested
AS
SELECT
    eventtime AS event_time,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    creditaccountid AS credit_account_id,
    creditcustomersupportcontactedid AS credit_customer_support_contacted_id,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load
FROM temp_credit_survey_requested_object;

INSERT INTO TABLE credit_survey_requested_ldg
SELECT
    event_time,
    channel_country,
    channel_brand,
    selling_channel,
    credit_account_id,
    credit_customer_support_contacted_id
FROM temp_credit_survey_requested
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
