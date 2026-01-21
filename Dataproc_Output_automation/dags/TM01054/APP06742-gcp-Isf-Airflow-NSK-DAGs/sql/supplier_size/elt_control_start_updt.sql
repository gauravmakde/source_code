UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control SET
 batch_id = elt_control.batch_id + 1,
 curr_batch_date = CURRENT_DATE('PST8PDT'),
 batch_start_tmstp = CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP),
 batch_start_tmstp_tz = 'PST8PDT',
 active_load_ind = 'Y'
WHERE LOWER(subject_area_nm) = LOWER('ITEM_SUPPLIER_SIZE_DATA_SERVICE');