-- Prepare final event bounced table
CREATE TEMPORARY VIEW event_bounced_final_extract AS
SELECT
    subscriberKey AS enterpriseId,
    sendId,
    clientId,
    batchId,
    bounceCategory,
    bounceReason,
    eventTime
FROM kafka_source;

-- Write result table
INSERT OVERWRITE event_bounced_data_extract
SELECT * FROM event_bounced_final_extract;

-- Audit table
INSERT OVERWRITE event_bounced_final_count
SELECT COUNT(*) FROM event_bounced_final_extract;
