--BEGIN
--DECLARE _ERROR_CODE INT64;
--DECLARE _ERROR_MESSAGE STRING;
--COMMIT TRANSACTION;
--EXCEPTION WHEN ERROR THEN
--ROLLBACK TRANSACTION;
--RAISE USING MESSAGE = @@error.message;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_END_LOAD('UPDATE_RETAIL_TRAN_HDR_DETERMINISTIC_ASSOCIATION{tbl_sfx}');

--BEGIN
--SET _ERROR_CODE  =  0;
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit{{params.tbl_sfx}} AS hist
USING (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('UPDATE_RETAIL_TRAN_HDR_DETERMINISTIC_ASSOCIATION{tbl_sfx}')) AS control
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
    src_s3_path = NULL;
--EXCEPTION WHEN ERROR THEN
--SET _ERROR_CODE  =  1;
--SET _ERROR_MESSAGE  =  @@error.message;
--END;
--COMMIT TRANSACTION;
--EXCEPTION WHEN ERROR THEN
--ROLLBACK TRANSACTION;
--RAISE USING MESSAGE = @@error.message;
--END;
