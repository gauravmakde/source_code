-- Prepare final event clicked table
CREATE TEMPORARY VIEW event_clicked_final_extract AS
SELECT
    subscriberKey AS enterpriseId,
    sendId,
    clientId,
    batchId,
    eventTime,
    sendUrlId,
    urlId,
    url
FROM kafka_source;

-- Write result table
INSERT OVERWRITE event_clicked_data_extract
SELECT * FROM event_clicked_final_extract;

-- Audit table
INSERT OVERWRITE event_clicked_final_count
SELECT COUNT(*) FROM event_clicked_final_extract;
