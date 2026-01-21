-- Prepare final event sent table from kafka source data
CREATE TEMPORARY VIEW event_sent_final_extract AS
SELECT
    subscriberKey AS enterpriseId,
    sendId,
    clientId,
    batchId,
    eventTime
FROM kafka_source;

-- Write result table
INSERT OVERWRITE event_sent_data_extract
SELECT * FROM event_sent_final_extract;

-- Audit table
INSERT OVERWRITE event_sent_final_count
SELECT COUNT(*) FROM event_sent_final_extract;
