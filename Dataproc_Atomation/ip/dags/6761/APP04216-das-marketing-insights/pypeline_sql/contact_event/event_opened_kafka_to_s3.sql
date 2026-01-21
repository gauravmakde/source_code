-- Prepare final event opened table
CREATE TEMPORARY VIEW event_opened_final_extract AS
SELECT
    subscriberKey AS enterpriseId,
    sendId AS sendId,
    clientId AS clientId,
    batchId AS batch_id,
    eventTime AS eventTime
FROM kafka_source;

-- Write result table
INSERT OVERWRITE event_opened_data_extract
SELECT * FROM event_opened_final_extract;

-- Audit table
INSERT OVERWRITE event_opened_final_count
SELECT COUNT(*) FROM event_opened_final_extract;
