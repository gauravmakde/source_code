CREATE TEMPORARY VIEW contact_event_sendlog_avro_src AS SELECT * FROM contact_event_sendlog_avro_source;

CREATE TEMPORARY VIEW contact_event_sendlog_wrk AS
SELECT
    value.clientId as client_id,
    value.sendId as send_id,
    value.enterpriseId as enterprise_id,
    value.batchId as batch_id,
    value.campaignId as campaign_id,
    regexp_replace(value.campaignName, '\u2013', '-') as campaign_name,
    value.campaignSegment as campaign_segment,
    value.subjectLine as subject_line,
    coalesce(value.emailModuleAssetIds, array()) as email_module_asset_id_list,
    coalesce(value.bannerAssetIds, array()) as banner_asset_id_list,
    value.shellAssetId as shell_asset_id,
    value.emailId as email_id,
    value.headerAssetId as header_asset_id,
    value.lastUpdatedTime as last_updated_time,
    '' as system_time,
    value.campaignKind as campaign_kind
FROM contact_event_sendlog_avro_src;

CREATE TEMPORARY VIEW contact_event_sendlog_final AS
SELECT
    enterprise_id,
    batch_id,
    send_id,
    client_id,
    campaign_id,
    campaign_name,
    campaign_segment,
    subject_line,
    element_at(email_module_asset_id_list, 1) as email_module_asset_id_1,
    element_at(email_module_asset_id_list, 2) as email_module_asset_id_2,
    element_at(email_module_asset_id_list, 3) as email_module_asset_id_3,
    element_at(email_module_asset_id_list, 4) as email_module_asset_id_4,
    element_at(email_module_asset_id_list, 5) as email_module_asset_id_5,
    element_at(email_module_asset_id_list, 6) as email_module_asset_id_6,
    element_at(email_module_asset_id_list, 7) as email_module_asset_id_7,
    element_at(email_module_asset_id_list, 8) as email_module_asset_id_8,
    element_at(email_module_asset_id_list, 9) as email_module_asset_id_9,
    element_at(email_module_asset_id_list, 10) as email_module_asset_id_10,
    element_at(banner_asset_id_list, 1) as banner_asset_id_1,
    element_at(banner_asset_id_list, 2) as banner_asset_id_2,
    element_at(banner_asset_id_list, 3) as banner_asset_id_3,
    shell_asset_id,
    case when coalesce(email_id,'')<>'' and cast(email_id as int) is null then -1 else email_id end as email_id,
    header_asset_id,
    last_updated_time,
    system_time,
    campaign_kind
FROM contact_event_sendlog_wrk;

INSERT OVERWRITE hist_data_stg SELECT * FROM contact_event_sendlog_final;

INSERT OVERWRITE hist_audit_stg SELECT COUNT(*) FROM contact_event_sendlog_final;