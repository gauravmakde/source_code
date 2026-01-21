-- Prepare final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    pushJobId,
    requestId,
    deviceId,
    eventTime,
    channelBrand,
    clientId,
    customer.idType AS customerIdType,
    customer.id AS customerId,
    batchId,
    messageId,
    campaignName,
    campaignType,
    campaignSegment,
    appPushId,
    assetId,
    messageTitle,
    fullLinkUrl
from kafka_source;

-- Write result table
INSERT OVERWRITE contact_event_mobile_push_sendlog_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE contact_event_mobile_push_sendlog_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
