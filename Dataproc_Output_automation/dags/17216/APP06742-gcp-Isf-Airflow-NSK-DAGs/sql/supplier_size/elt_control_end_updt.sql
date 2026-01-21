UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control SET
 batch_end_tmstp = CAST(CURRENT_DATETIME('PST8PDT')AS TIMESTAMP),
 batch_end_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(),
 active_load_ind = 'N'
WHERE LOWER(subject_area_nm) = LOWER('ITEM_SUPPLIER_SIZE_DATA_SERVICE');