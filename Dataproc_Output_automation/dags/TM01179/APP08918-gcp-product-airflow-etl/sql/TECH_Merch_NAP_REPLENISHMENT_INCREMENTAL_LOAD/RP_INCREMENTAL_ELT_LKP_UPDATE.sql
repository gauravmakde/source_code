
BEGIN TRANSACTION;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup 
SET config_value = (SELECT config_value
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('RP_LOAD_MAX_BATCH_ID')),
    rcd_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHERE LOWER(interface_code) = LOWER('RP_LOAD_MIN_BATCH_ID');
UPDATE
     `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.config_lkup SET
     config_value = SUBSTR(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 1, 14),
     rcd_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHERE LOWER(interface_code) = LOWER('RP_LOAD_MAX_BATCH_ID');

COMMIT TRANSACTION;
