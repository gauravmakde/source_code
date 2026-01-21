-- Read kafka source data with coalesce on emailModuleAssetIds and bannerAssetIds
CREATE TEMPORARY VIEW contact_event_sendlog AS
SELECT
    clientId AS client_id,
    sendId AS send_id,
    enterpriseId AS enterprise_id,
    batchId AS batch_id,
    campaignId AS campaign_id,
    regexp_replace(regexp_replace(campaignName, '\u2013', '-'),'\u200B', '') AS campaign_name,
    campaignSegment AS campaign_segment,
    subjectLine AS subject_line,
    coalesce(emailModuleAssetIds, array()) AS email_module_asset_id_list,
    coalesce(bannerAssetIds, array()) AS banner_asset_id_list,
    shellAssetId AS shell_asset_id,
    emailId AS email_id,
    headerAssetId AS header_asset_id,
    customerProfileId.idType AS customer_profile_id_type,
    customerProfileId.id AS customer_profile_id,
    eventTime AS last_updated_time,
    decode(headers.SystemTime, 'UTF-8') AS system_time,
    campaignKind AS campaign_kind
FROM kafka_source;

-- Prepare final contact event sendlog table
CREATE TEMPORARY VIEW contact_event_sendlog_final_extract AS
SELECT
    enterprise_id,
    batch_id,
    send_id,
    client_id,
    campaign_id,
    CASE
        WHEN length(campaign_name) <= 100 THEN campaign_name
        WHEN length(trim(campaign_name)) <= 100 THEN trim(campaign_name)
        ELSE concat(substr(trim(campaign_name), 1, 50), substr(trim(campaign_name), length(trim(campaign_name)) - 49, 50))
    END AS campaign_name,
    campaign_segment,
    subject_line,
    element_at(email_module_asset_id_list, 1) AS email_module_asset_id_1,
    element_at(email_module_asset_id_list, 2) AS email_module_asset_id_2,
    element_at(email_module_asset_id_list, 3) AS email_module_asset_id_3,
    element_at(email_module_asset_id_list, 4) AS email_module_asset_id_4,
    element_at(email_module_asset_id_list, 5) AS email_module_asset_id_5,
    element_at(email_module_asset_id_list, 6) AS email_module_asset_id_6,
    element_at(email_module_asset_id_list, 7) AS email_module_asset_id_7,
    element_at(email_module_asset_id_list, 8) AS email_module_asset_id_8,
    element_at(email_module_asset_id_list, 9) AS email_module_asset_id_9,
    element_at(email_module_asset_id_list, 10) AS email_module_asset_id_10,
    element_at(banner_asset_id_list, 1) AS banner_asset_id_1,
    element_at(banner_asset_id_list, 2) AS banner_asset_id_2,
    element_at(banner_asset_id_list, 3) AS banner_asset_id_3,
    shell_asset_id,
    email_id,
    header_asset_id,
    customer_profile_id_type,
    customer_profile_id,
    last_updated_time,
    system_time,
    campaign_kind
FROM contact_event_sendlog;

-- Write result table
INSERT OVERWRITE contact_event_sendlog_data_extract
SELECT * FROM contact_event_sendlog_final_extract;

-- Audit table
INSERT OVERWRITE contact_event_sendlog_final_count
SELECT COUNT(*) FROM contact_event_sendlog_final_extract;
