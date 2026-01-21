UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_UTL.ELT_CONTROL SET
 active_load_ind = 'N',
 batch_end_tmstp = CAST(current_datetime('PST8PDT') AS TIMESTAMP),
 batch_end_tmstp_tz = batch_end_tmstp_tz
WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS') AND LOWER(active_load_ind) = LOWER('Y');



