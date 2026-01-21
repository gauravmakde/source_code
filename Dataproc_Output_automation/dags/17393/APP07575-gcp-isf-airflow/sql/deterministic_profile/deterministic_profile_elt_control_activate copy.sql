

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_START_LOAD('DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}');


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control SET
    curr_batch_date = CURRENT_DATE,
    extract_from_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP),
    extract_to_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP),
    batch_end_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP)
WHERE LOWER(subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}');

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit AS hist
USING (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}')) AS control
ON LOWER(control.subject_area_nm) = LOWER(hist.subject_area_nm) AND control.batch_id = hist.dw_batch_id
WHEN MATCHED THEN UPDATE SET
    dw_batch_date = control.curr_batch_date,
    extract_from = CAST(control.extract_from_tmstp AS TIMESTAMP),
    extract_to = CAST(control.extract_to_tmstp AS TIMESTAMP),
    status_code = CASE WHEN LOWER(control.active_load_ind) = LOWER('Y') THEN 'RUNNING' ELSE 'FINISHED' END,
    is_adhoc_run = 'N',
    dw_sys_start_tmstp = control.batch_start_tmstp,
    dw_sys_end_tmstp = control.batch_end_tmstp,
    src_s3_path = '<SRC_S3_PATH>'
WHEN NOT MATCHED THEN INSERT VALUES(control.subject_area_nm, control.batch_id, control.curr_batch_date, CAST(control.extract_from_tmstp AS TIMESTAMP),control.extract_from_tmstp_tz, CAST(control.extract_to_tmstp AS TIMESTAMP),control.extract_to_tmstp_tz, CASE WHEN LOWER(control.active_load_ind) = LOWER('Y') THEN 'RUNNING' ELSE 'FINISHED' END, 'N', control.batch_start_tmstp, control.batch_end_tmstp, '<SRC_S3_PATH>');

---COMMIT TRANSACTION;
