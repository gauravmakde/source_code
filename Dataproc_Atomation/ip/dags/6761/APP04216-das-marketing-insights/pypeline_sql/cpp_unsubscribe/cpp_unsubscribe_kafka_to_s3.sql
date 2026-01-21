-- Prepare final cpp unsubscribe table
CREATE TEMPORARY VIEW cpp_unsubscribe_final_extract AS
SELECT
    enterpriseId,
    emailId,
    sendId,
    clientId,
    channel.channelCountry AS channel_channelCountry,
    channel.channelBrand AS channel_channelBrand,
    channel.sellingChannel AS channel_sellingChannel,
    experience,
    pageInstanceId,
    clientInstanceId,
    customer.idType AS customer_idType,
    customer.id AS customer_id,
    marketingCampaign.utmCampaign AS marketingCampaign_utmCampaign,
    marketingCampaign.utmSource AS marketingCampaign_utmSource,
    marketingCampaign.utmMedium AS marketingCampaign_utmMedium,
    marketingCampaign.utmChannel AS marketingCampaign_utmChannel,
    marketingCampaign.utmTerm AS marketingCampaign_utmTerm,
    marketingCampaign.utmContent AS marketingCampaign_utmContent,
    lastUnsubscribedTime,
    decode(headers.SystemTime,'UTF-8') AS header_SystemTime
FROM kafka_source;

-- Create channel brand preferences temp view with exploded channelBrandPreferences
CREATE TEMPORARY VIEW channel_brand_preferences_explode AS
SELECT
    enterpriseId,
    emailId,
    sendId,
    clientId,
    explode(channelBrandPreferences) AS channel_brand_preference,
    decode(headers.SystemTime,'UTF-8') AS header_SystemTime
FROM kafka_source;

-- Prepare final channel brand preferences table
CREATE TEMPORARY VIEW channel_brand_preferences_final_extract AS
SELECT
    enterpriseId,
    emailId,
    sendId,
    clientId,
    channel_brand_preference.name AS channel_brand_preference_name,
    channel_brand_preference.preferenceValue AS channel_brand_preference_preference_value,
    channel_brand_preference.sourceUpdateDate AS channel_brand_preference_source_update_date,
    header_SystemTime
from channel_brand_preferences_explode;

-- CPP UNSUBSCRIBE

-- Write result table
INSERT OVERWRITE cpp_unsubscribe_data_extract
SELECT * FROM cpp_unsubscribe_final_extract;

-- Audit table
INSERT OVERWRITE cpp_unsubscribe_final_count
SELECT COUNT(*) FROM cpp_unsubscribe_final_extract;

-- CHANNEL BRAND PREFERENCES

-- Write result table
INSERT OVERWRITE channel_brand_preferences_data_extract
SELECT * FROM channel_brand_preferences_final_extract;

-- Audit table
INSERT OVERWRITE channel_brand_preferences_final_count
SELECT COUNT(*) FROM channel_brand_preferences_final_extract;
