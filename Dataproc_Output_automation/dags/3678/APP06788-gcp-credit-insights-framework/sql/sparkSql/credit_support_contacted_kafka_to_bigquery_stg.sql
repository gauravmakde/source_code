-- Read Kafka into temp table
CREATE TEMPORARY VIEW temp_credit_support_contacted_object
AS
SELECT * FROM kafka_customer_credit_customer_support_contacted_avro;

CREATE TEMPORARY VIEW temp_credit_support_contacted
AS
SELECT
    eventtime AS event_time,
    channel.channelcountry AS channel_country,
    channel.channelbrand AS channel_brand,
    channel.sellingchannel AS selling_channel,
    creditaccountid AS credit_account_id,
    creditcustomersupportcontactedid AS credit_customer_support_contacted_id,
    callinfoprocessorid AS call_info_processor_id,
    callaccountinfoprocessorid AS call_account_info_processor_id,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-test') AS nord_test,
    element_at(transform_keys(headers, (k, v) -> lower(k)), 'nord-load') AS nord_load
FROM temp_credit_support_contacted_object;

INSERT INTO TABLE credit_support_contacted_ldg
SELECT
    cast(event_time as string) AS event_time,
    channel_country,
    channel_brand,
    selling_channel,
    credit_account_id,
    credit_customer_support_contacted_id,
    call_info_processor_id,
    call_account_info_processor_id
FROM temp_credit_support_contacted
WHERE
    -- Header filtering based on environment
    (
        ('{db_env}' = 'PRD' AND nord_test IS NULL AND nord_load IS NULL)
        OR
        ('{db_env}' = 'PREPROD' AND nord_load IS NULL)
        OR
        ('{db_env}' = 'DEV')
    );
