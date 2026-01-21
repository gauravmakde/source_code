-- Prepare final event unsubscribed table
CREATE TEMPORARY VIEW event_unsubscribed_final_extract AS
SELECT
    subscriberKey AS enterpriseId,
    sendId,
    clientId,
    batchId,
    reason,
    eventTime
FROM kafka_source;

-- Write result table
INSERT OVERWRITE event_unsubscribed_data_extract
SELECT * FROM event_unsubscribed_final_extract;

-- Audit table
INSERT OVERWRITE event_unsubscribed_final_count
SELECT COUNT(*) FROM event_unsubscribed_final_extract;
