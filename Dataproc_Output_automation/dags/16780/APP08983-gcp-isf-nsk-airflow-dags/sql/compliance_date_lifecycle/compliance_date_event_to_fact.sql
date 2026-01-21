/***********************************************************************************
-- Add bad records to error table
************************************************************************************/
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.ascp_error_ldg
(
  target_table,
  unique_key,
  reason,
  dw_batch_id,
  dw_sys_load_tmstp,
  dw_sys_load_tmstp_tz,
  dw_sys_updt_tmstp,
  dw_sys_updt_tmstp_tz
)
select
  'COMPLIANCE_DATE_EVENT_FACT',
  'VendorId: ' || coalesce(vendor_id, 'NULL')
      || ', EventName: ' || coalesce(event_name, 'NULL')
      || ', ViolationType: ' || coalesce(violation_type, 'NULL')
      || ', isAppliedToNPG: ' || coalesce(is_applied_to_npg, 'NULL')
      || ', extensionDateId: ' || coalesce(extension_date_id, 'NULL'),
  'Revision VendorId or EventName are NULL',
  CAST(TRUNC(CAST(dw_batch_id AS FLOAT64)) AS INT64) AS dw_batch_id,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) as  dw_sys_load_tmstp, 
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() as  dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) as  dw_sys_updt_tmstp ,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() as  dw_sys_updt_tmstp_tz 
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
where vendor_id is null
or event_name is null
or violation_type is null
or is_applied_to_npg is null
or ( LOWER(event_name) = LOWER('VendorComplianceDateExtended') and (extension_date_id is null) )
;


/***********************************************************************************
-- Delete new keys from the target fact table
************************************************************************************/
DELETE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.compliance_date_event_fact
WHERE LOWER(event_name) = LOWER('VendorComplianceDateAdded')
AND (vendor_id) IN (
      SELECT vendor_id
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateAdded')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null)
AND ( violation_type) IN (
      SELECT violation_type FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateAdded')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null)
AND (is_applied_to_npg) IN (SELECT is_applied_to_npg
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateAdded')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null)
AND event_time IN (SELECT event_time_utc
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateAdded')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null);

DELETE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.compliance_date_event_fact
WHERE LOWER(event_name) = LOWER('VendorComplianceDateExtended')
AND vendor_id IN (
      SELECT vendor_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateExtended')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null
        AND extension_date_id is not null)
AND violation_type IN (
      SELECT violation_type FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateExtended')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null
        AND extension_date_id is not null)
AND is_applied_to_npg IN (
      SELECT is_applied_to_npg FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateExtended')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null)
AND event_time IN (
      SELECT event_time_utc FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateExtended')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null
        AND extension_date_id is not null)
AND extension_date_id IN (SELECT extension_date_id
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
      WHERE LOWER(event_name) = LOWER('VendorComplianceDateExtended')
        AND event_name is not null
        AND vendor_id is not null
        AND violation_type is not null
        AND is_applied_to_npg is not null
        AND extension_date_id is not null);


/***********************************************************************************
-- Merge into the target fact table
************************************************************************************/
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.compliance_date_event_fact (
    vendor_id,
    event_name,
    last_updated_time,
    last_updated_time_tz,
    event_date_pacific,
    event_datetime_pacific,
    event_time,
    event_time_tz,
    violation_type,
    compliance_date_id,
    initiated_date,
    compliance_date,
    is_applied_to_npg,
    extension_date_id,
    extension_date,
    dw_batch_id,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz,
    dw_sys_updt_tmstp,
    dw_sys_updt_tmstp_tz
  )
SELECT
  vendor_id,
  event_name,
  last_updated_time_utc,
  last_updated_time_tz,
  event_date_pacific  ,
  event_datetime_pacific ,
  event_time_utc,
  event_time_tz,
  violation_type,
  compliance_date_id,
  initiated_date ,
  compliance_date,
  is_applied_to_npg,
  extension_date_id,
  extension_date ,
  dw_batch_id,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) ,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`(),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) ,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`()
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.compliance_date_event_ldg
WHERE event_name is not null
AND vendor_id is not null
AND violation_type is not null
AND is_applied_to_npg is not null
AND NOT (   LOWER(event_name) = LOWER('VendorComplianceDateExtended') 
AND extension_date_id is null )
;



/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/
--COLLECT STATS ON `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.compliance_date_event_fact INDEX (vendor_id, event_name);