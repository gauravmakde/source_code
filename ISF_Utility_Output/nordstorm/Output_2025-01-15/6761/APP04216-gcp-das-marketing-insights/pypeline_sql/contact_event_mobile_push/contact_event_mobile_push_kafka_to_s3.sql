-- Prepare final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    pushJobId,
    requestId,
    deviceBadge,
    channel.channelCountry AS channel_channel_country,
    channel.channelBrand AS channel_channel_brand,
    channel.sellingChannel AS channel_selling_channel,
    fileDate,
    clientId,
    enterpriseClientId,
    appName,
    messageName,
    messageId,
    customer.idType AS customer_id_type,
    customer.id AS customer_id,
    messageContent,
    messagePushActivityTime,
    durationSeconds,
    platform,
    platformVersion,
    messagePushSendTime,
    mobilePushStatusType,
    vendorServiceResponseMessage,
    campaignType,
    decode(headers.SystemTime, 'UTF-8') AS system_time
from kafka_source;

-- Write result table
INSERT OVERWRITE contact_event_mobile_push_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE contact_event_mobile_push_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
