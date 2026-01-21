--  read from ORC
CREATE TEMPORARY TABLE compliance
  AS
    SELECT
        *
      FROM
        scoi.compliance_date_lifecycle src
      WHERE src.batch_id = 'SQL.PARAM.AIRFLOW_RUN_ID'
;
--  VendorComplianceDateAddedDetails
CREATE TEMPORARY TABLE added_exploded_base
  AS
    SELECT
        compliance.vendorid,
        compliance.lastupdatedtime,
        'VendorComplianceDateAdded' AS eventname,
        details,
        compliance.batch_id
      FROM
        compliance, unnest(compliance.vendorcompliancedateaddeddetails) AS details
;
CREATE TEMPORARY TABLE added_exploded
  AS
    SELECT
        added_exploded_base.vendorid,
        added_exploded_base.lastupdatedtime,
        added_exploded_base.eventname,
        added_exploded_base.details,
        added_exploded_base.batch_id
      FROM
        added_exploded_base
      WHERE added_exploded_base.details IS NOT NULL
;
--  VendorComplianceDateExtendedDetails
CREATE TEMPORARY TABLE extended_exploded_base
  AS
    SELECT
        compliance.vendorid,
        compliance.lastupdatedtime,
        'VendorComplianceDateExtended' AS eventname,
        details,
        compliance.batch_id
      FROM
        compliance,  unnest(compliance.vendorcompliancedateextendeddetails) AS details
;
CREATE TEMPORARY TABLE extended_exploded
  AS
    SELECT
        extended_exploded_base.vendorid,
        extended_exploded_base.lastupdatedtime,
        extended_exploded_base.eventname,
        extended_exploded_base.details,
        extended_exploded_base.batch_id
      FROM
        extended_exploded_base
      WHERE extended_exploded_base.details IS NOT NULL
;
--  Create final table
CREATE TEMPORARY TABLE compliance_events_temp
  AS
    SELECT
        added_exploded.vendorid AS vendor_id,
        added_exploded.eventname AS event_name,
        added_exploded.lastupdatedtime AS last_updated_time,
        substr(CAST(datetime( TIMESTAMP_MILLIS(UNIX_MILLIS(details.eventtime )), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime( TIMESTAMP_MILLIS(UNIX_MILLIS(details.eventtime) ), 'America/Los_Angeles') AS event_datetime_pacific,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', details.eventtime) AS event_time,
        details.violationtype AS violation_type,
        details.compliancedateid AS compliance_date_id,
        details.initiateddate AS initiated_date,
        details.compliancedate AS compliance_date,
        if(details.isappliedtonpg, 'T', 'F') AS is_applied_to_npg,
        NULL AS extension_date_id,
        NULL AS extension_date,
        added_exploded.batch_id AS dw_batch_id
      FROM
        added_exploded
    UNION ALL
    SELECT
        extended_exploded.vendorid AS vendor_id,
        extended_exploded.eventname AS event_name,
        extended_exploded.lastupdatedtime AS last_updated_time,
        substr(CAST(datetime(TIMESTAMP_MILLIS(UNIX_MILLIS(details.eventtime )), 'America/Los_Angeles') as STRING), 0, 10) AS event_date_pacific,
        datetime( TIMESTAMP_MILLIS(UNIX_MILLIS(details.eventtime) ), 'America/Los_Angeles') AS event_datetime_pacific,
        format_datetime('%Y-%m-%d %H:%M:%E3Sxxx', details.eventtime) AS event_time,
        details.violationtype AS violation_type,
        details.compliancedateid AS compliance_date_id,
        details.initiateddate AS initiated_date,
        NULL AS compliance_date,
        if(details.isappliedtonpg, 'T', 'F') AS is_applied_to_npg,
        details.extensiondateid AS extension_date_id,
        details.extensiondate AS extension_date,
        extended_exploded.batch_id AS dw_batch_id
      FROM
        extended_exploded
;
TRUNCATE TABLE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.compliance_date_event_ldg;
INSERT INTO  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.compliance_date_event_ldg 
  SELECT
     compliance_events_temp.vendor_id,
      compliance_events_temp.event_name,
       compliance_events_temp.last_updated_time AS last_updated_time,
      CONCAT('-0', CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(CURRENT_DATETIME("America/Los_Angeles")), HOUR) AS STRING),':00'  ) AS last_updated_time_tz,
            SAFE_CAST(regexp_extract(compliance_events_temp.event_date_pacific, '^([^ ]*)') AS DATE) AS event_date_pacific,
      compliance_events_temp.event_datetime_pacific,
      CAST(compliance_events_temp.event_time AS TIMESTAMP),
      CONCAT('-0', CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(CURRENT_DATETIME("America/Los_Angeles")), HOUR) AS STRING),':00'  ) AS event_time_tz,
      compliance_events_temp.violation_type,
      compliance_events_temp.compliance_date_id,
      compliance_events_temp.initiated_date,
      compliance_events_temp.compliance_date,
      compliance_events_temp.is_applied_to_npg,
      compliance_events_temp.extension_date_id,
      compliance_events_temp.extension_date,
      compliance_events_temp.dw_batch_id,
      CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) ,
      `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`(),
      CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) ,
     `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`()
    FROM
      compliance_events_temp
;