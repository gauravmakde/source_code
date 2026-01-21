/* SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_customer_dim_17393_customer_das_customer;
Task_Name=deterministic_customer_elt_control_set_elt_from_and_to;'
FOR SESSION VOLATILE;*/

--ET;


-----------------------------------------------------------------------------
--------------- SUBJECT_AREA_NM = DETERMINISTIC_CUSTOMER_DIM ----------------
-----------------------------------------------------------------------------

--Determines ELT extract FROM and TO from STG
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control AS ctrl SET
 extract_from_tmstp = timestamp(stg.min_profile_event_tmstp),
 extract_from_tmstp_tz = (SELECT profile_event_tmstp_tz FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pending_deterministic_customer_dim{{params.tbl_sfx}}
),
 extract_to_tmstp_tz = (SELECT profile_event_tmstp_tz FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pending_deterministic_customer_dim{{params.tbl_sfx}}
),
 extract_to_tmstp = timestamp(stg.max_profile_event_tmstp) FROM (SELECT MIN(profile_event_tmstp) AS min_profile_event_tmstp,
   MAX(profile_event_tmstp) AS max_profile_event_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pending_deterministic_customer_dim{{params.tbl_sfx}}
) AS stg
WHERE LOWER(ctrl.subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_DIM{tbl_sfx}') AND stg.min_profile_event_tmstp
 IS NOT NULL;


----------------------------------------------------------------------------
--  Adding/updating a record to ETL_BATCH_INFO table from ELT_CONTROL table
----------------------------------------------------------------------------
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit{{params.tbl_sfx}}
 AS hist
USING (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
 WHERE LOWER(subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_DIM{tbl_sfx}')) AS control
ON LOWER(control.subject_area_nm) = LOWER(hist.subject_area_nm) AND control.batch_id = hist.dw_batch_id
WHEN MATCHED THEN UPDATE SET
 dw_batch_date = control.curr_batch_date,
 extract_from = control.extract_from_tmstp_utc,
 extract_from_tz = control.extract_from_tmstp_tz,
 extract_to = control.extract_to_tmstp_utc,
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
WHEN NOT MATCHED THEN 
INSERT 
VALUES (
    control.subject_area_nm, 
    control.batch_id, 
    control.curr_batch_date, 
    control.extract_from_tmstp_utc,
    control.extract_from_tmstp_tz
 ,  control.extract_to_tmstp_utc, 
    control.extract_to_tmstp_tz, 
    CASE WHEN LOWER(control.active_load_ind) = LOWER('Y') THEN 'RUNNING' ELSE 'FINISHED' END,
     'N', 
     control.batch_start_tmstp, 
     control.batch_end_tmstp,
      '<SRC_S3_PATH>');


/* SET QUERY_BAND = NONE FOR SESSION;*/

--ET;
