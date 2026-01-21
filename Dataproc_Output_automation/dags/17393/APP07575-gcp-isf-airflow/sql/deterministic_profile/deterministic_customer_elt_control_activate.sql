
/* SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_customer_dim_17393_customer_das_customer;
Task_Name=deterministic_customer_elt_control_activate;'
FOR SESSION VOLATILE;*/
--ET;
-----------------------------------------------------------------------------
--------------- SUBJECT_AREA_NM = DETERMINISTIC_CUSTOMER_DIM ----------------
-----------------------------------------------------------------------------
-- Marks ELT as active, creates a new BATCH_ID and sets Batch_Start timestamp at ELT_CONTROL table

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_START_LOAD('DETERMINISTIC_CUSTOMER_DIM{{params.tbl_sfx}}');
 

-- Sets BATCH_DATE to current date, FROM and TO timestamps - to some very deep past value to show they're yet undefined
--BEGIN
--SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control SET
    curr_batch_date = current_date('PST8PDT'),
    extract_from_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP),
    extract_to_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP),
    batch_end_tmstp = CAST('1900-01-01 00:00:00.000000' AS TIMESTAMP)
WHERE LOWER(subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_DIM{{params.tbl_sfx}}');


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit AS hist
USING (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
 WHERE LOWER(subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_DIM{{params.tbl_sfx}}')) AS control
ON LOWER(control.subject_area_nm) = LOWER(hist.subject_area_nm) AND control.batch_id = hist.dw_batch_id
WHEN MATCHED THEN UPDATE SET
 dw_batch_date = control.curr_batch_date,
 extract_from = cast(control.extract_from_tmstp_utc as timestamp),
 extract_from_tz = control.extract_from_tmstp_tz,
 extract_to = cast(control.extract_to_tmstp_utc as timestamp),
 extract_to_tz = control.extract_to_tmstp_tz,
 status_code = CASE
  WHEN LOWER(control.active_load_ind) = LOWER('Y')
  THEN 'RUNNING'
  ELSE 'FINISHED'
  END,
 is_adhoc_run = 'N',
 dw_sys_start_tmstp = control.batch_start_tmstp,
 dw_sys_end_tmstp = control.batch_end_tmstp,
 src_s3_path = '<SRC_S3_PATH>'
WHEN NOT MATCHED THEN INSERT VALUES(control.subject_area_nm, control.batch_id, control.curr_batch_date, control.extract_from_tmstp_utc,
extract_from_tmstp_tz, control.extract_to_tmstp_utc,control.extract_to_tmstp_tz,  CASE
  WHEN LOWER(control.active_load_ind) = LOWER('Y')
  THEN 'RUNNING'
  ELSE 'FINISHED'
  END, 'N', control.batch_start_tmstp, control.batch_end_tmstp, '<SRC_S3_PATH>');

-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;
-- END;
-- Adding/updating a record to BATCH_HIST_AUDIT table from ELT_CONTROL table
/* SET QUERY_BAND = NONE FOR SESSION;*/
--ET;