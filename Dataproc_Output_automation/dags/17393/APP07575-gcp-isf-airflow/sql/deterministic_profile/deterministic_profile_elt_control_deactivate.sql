CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_END_LOAD('DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}');

-- Adding/updating a record to BATCH_HIST_AUDIT table from ELT_CONTROL table
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit AS hist
USING (
  SELECT
    *
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE lower(rtrim(subject_area_nm, ' ')) = lower('DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}')
) AS control
ON lower(control.subject_area_nm) = lower(hist.subject_area_nm)
AND control.batch_id = hist.dw_batch_id
WHEN MATCHED THEN 
  UPDATE SET
    dw_batch_date = control.curr_batch_date, 
    extract_from = control.extract_from_tmstp_utc, 
    extract_from_tz = control.extract_from_tmstp_tz, 
    extract_to = control.extract_to_tmstp_utc,
    extract_to_tz = control.extract_to_tmstp_tz, 
    status_code = CASE
      WHEN upper(rtrim(control.active_load_ind, ' ')) = 'Y' THEN 'RUNNING'
      ELSE 'FINISHED'
    END, 
    is_adhoc_run = 'N', 
    dw_sys_start_tmstp = control.batch_start_tmstp, 
    dw_sys_end_tmstp = control.batch_end_tmstp, 
    src_s3_path = '{{params.src_s3_path}}'
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    subject_area_nm, 
    dw_batch_id, 
    dw_batch_date, 
    extract_from, 
    extract_from_tz, 
    extract_to, 
    extract_to_tz, 
    status_code, 
    is_adhoc_run, 
    dw_sys_start_tmstp, 
    dw_sys_end_tmstp, 
    src_s3_path
  )
  VALUES (
    control.subject_area_nm, 
    control.batch_id, 
    control.curr_batch_date, 
    extract_from_tmstp_utc,
    control.extract_from_tmstp_tz,
    control.extract_to_tmstp_utc,
    control.extract_to_tmstp_tz, 
    CASE
      WHEN upper(rtrim(control.active_load_ind, ' ')) = 'Y' THEN 'RUNNING'
      ELSE 'FINISHED'
    END, 
    'N', 
    control.batch_start_tmstp, 
    control.batch_end_tmstp, 
    '{{params.src_s3_path}}'
  );
