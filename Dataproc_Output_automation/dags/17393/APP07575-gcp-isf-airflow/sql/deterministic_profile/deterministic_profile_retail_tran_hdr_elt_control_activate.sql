--BEGIN
--DECLARE _ERROR_CODE INT64;
--DECLARE _ERROR_MESSAGE STRING;
--COMMIT TRANSACTION;
--EXCEPTION WHEN ERROR THEN
--ROLLBACK TRANSACTION;
--RAISE USING MESSAGE = @@error.message;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_START_LOAD('UPDATE_RETAIL_TRAN_HDR_DETERMINISTIC_ASSOCIATION{{params.tbl_sfx}}');

--BEGIN
--SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control SET
    curr_batch_date = current_date('PST8PDT'),
    extract_from_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP),
    extract_to_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP),
    batch_end_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP)
WHERE LOWER(subject_area_nm) = LOWER('UPDATE_RETAIL_TRAN_HDR_DETERMINISTIC_ASSOCIATION{{params.tbl_sfx}}');
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--BEGIN
--SET _ERROR_CODE  =  0;
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit{{params.tbl_sfx}} AS hist
USING (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('UPDATE_RETAIL_TRAN_HDR_DETERMINISTIC_ASSOCIATION{{params.tbl_sfx}}')) AS control
ON LOWER(control.subject_area_nm) = LOWER(hist.subject_area_nm) AND control.batch_id = hist.dw_batch_id
WHEN MATCHED THEN UPDATE SET
    dw_batch_date = control.curr_batch_date,
    extract_from = cast(control.extract_from_tmstp_utc as timestamp),
    extract_from_tz = control.extract_from_tmstp_tz,
    extract_to = cast(control.extract_to_tmstp_utc as timestamp),
    extract_to_tz = control.extract_to_tmstp_tz,
    status_code = CASE WHEN LOWER(control.active_load_ind) = LOWER('Y') THEN 'RUNNING' ELSE 'FINISHED' END,
    is_adhoc_run = 'N',
    dw_sys_start_tmstp = control.batch_start_tmstp,
    dw_sys_end_tmstp = control.batch_end_tmstp,
    src_s3_path = NULL
WHEN NOT MATCHED THEN INSERT VALUES(control.subject_area_nm, control.batch_id, current_date('PST8PDT'), control.extract_from_tmstp_utc,control.extract_from_tmstp_tz, control.extract_to_tmstp_utc, control.extract_to_tmstp_tz ,CASE WHEN LOWER(control.active_load_ind) = LOWER('Y') THEN 'RUNNING' ELSE 'FINISHED' END, 'N', control.batch_start_tmstp, control.batch_end_tmstp, CAST(NULL AS STRING));
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--COMMIT TRANSACTION;
--EXCEPTION WHEN ERROR THEN
--ROLLBACK TRANSACTION;
--RAISE USING MESSAGE = @@error.message;
--END;
