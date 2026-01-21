/* SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_customer_object_ldg_to_dim_17393_customer_das_customer;
Task_Name=customer_object_elt_control_set_elt_from_and_to;'
FOR SESSION VOLATILE;*/



-----------------------------------------------------------------------------
----------------------  SUBJECT_AREA_NM = CUSTOMER_OBJ ----------------------
-----------------------------------------------------------------------------

--Determines ELT extract FROM and TO from STG



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control AS ctrl 
SET
 extract_from_tmstp = stg.min_object_event_tmstp_utc,
 extract_from_tmstp_tz = min_object_event_tmstp_tz,
 extract_to_tmstp = stg.max_object_event_tmstp_utc,
 extract_to_tmstp_tz = max_object_event_tmstp_tz
FROM (SELECT MIN(min_object_event_tmstp_utc) AS min_object_event_tmstp_utc,
   ANY_VALUE(min_object_event_tmstp_TZ HAVING MIN min_object_event_tmstp_utc)  min_object_event_tmstp_tz,
   MAX(max_object_event_tmstp_utc) AS max_object_event_tmstp_utc,
   ANY_VALUE(max_object_event_tmstp_TZ HAVING MIN max_object_event_tmstp_utc)  max_object_event_tmstp_tz
  FROM (SELECT 
     MIN(object_event_tmstp_utc) AS min_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  min_object_event_tmstp_tz,
     MAX(object_event_tmstp_utc) AS max_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  max_object_event_tmstp_tz
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_program_wrk{{params.tbl_sfx}}
     UNION ALL
     SELECT 
     MIN(object_event_tmstp_utc) AS min_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  min_object_event_tmstp_tz,
     MAX(object_event_tmstp_utc) AS max_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  max_object_event_tmstp_tz
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_email_lvl1_wrk{{params.tbl_sfx}}
     UNION ALL
     SELECT 
     MIN(object_event_tmstp_utc) AS min_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  min_object_event_tmstp_tz,
     MAX(object_event_tmstp_utc) AS max_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  max_object_event_tmstp_tz
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_telephone_lvl1_wrk{{params.tbl_sfx}}
     UNION ALL
     SELECT 
     MIN(object_event_tmstp_utc) AS min_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  min_object_event_tmstp_tz,
     MAX(object_event_tmstp_utc) AS max_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  max_object_event_tmstp_tz
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_wrk{{params.tbl_sfx}}
     UNION ALL
     SELECT 
     MIN(object_event_tmstp_utc) AS min_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  min_object_event_tmstp_tz,
     MAX(object_event_tmstp_utc) AS max_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  max_object_event_tmstp_tz
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_merge_alias_wrk{{params.tbl_sfx}}
     UNION ALL
     SELECT 
     MIN(object_event_tmstp_utc) AS min_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  min_object_event_tmstp_tz,
     MAX(object_event_tmstp_utc) AS max_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  max_object_event_tmstp_tz
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_payment_method_wrk{{params.tbl_sfx}}
     UNION ALL
     SELECT 
     MIN(object_event_tmstp_utc) AS min_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  min_object_event_tmstp_tz,
     MAX(object_event_tmstp_utc) AS max_object_event_tmstp_utc,
     ANY_VALUE(object_event_tmstp_TZ HAVING MIN object_event_tmstp_utc)  max_object_event_tmstp_tz
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_master_wrk{{params.tbl_sfx}}
     ) AS t) AS stg
WHERE LOWER(ctrl.subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}') 
AND stg.min_object_event_tmstp_utc IS NOT NULL;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit{{params.tbl_sfx}}
 AS hist
USING (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
 WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')) AS control
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
 src_s3_path = '<src_s3_path>'
WHEN NOT MATCHED THEN INSERT VALUES(control.subject_area_nm, control.batch_id, control.curr_batch_date, control.extract_from_tmstp_utc, control.extract_from_tmstp_tz
 , control.extract_to_tmstp_utc, control.extract_to_tmstp_tz, CASE
  WHEN LOWER(control.active_load_ind) = LOWER('Y')
  THEN 'RUNNING'
  ELSE 'FINISHED'
  END, 'N', control.batch_start_tmstp, control.batch_end_tmstp, '<src_s3_path>');
