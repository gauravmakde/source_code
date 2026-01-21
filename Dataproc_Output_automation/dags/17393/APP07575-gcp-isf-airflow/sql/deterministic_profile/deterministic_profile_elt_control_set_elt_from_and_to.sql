UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control AS ctrl SET extract_from_tmstp = stg.min_header_eventtime,
extract_from_tmstp_tz = stg.min_header_eventtime_tz,
 extract_to_tmstp = stg.max_header_eventtime,
 extract_to_tmstp_tz = stg.max_header_eventtime_tz
  FROM (
  SELECT
      min(t.min_header_eventtime) AS min_header_eventtime,
      ANY_VALUE(t.min_header_eventtime_tz HAVING min (t.min_header_eventtime))  min_header_eventtime_tz,
      max(t.max_header_eventtime) AS max_header_eventtime,
      ANY_VALUE(t.max_header_eventtime_tz HAVING max(t.max_header_eventtime))  max_header_eventtime_tz,
    FROM
      (
        SELECT
            min(deterministic_customer_profile_association_wrk.header_eventtime) AS min_header_eventtime,
             ANY_VALUE(deterministic_customer_profile_association_wrk.header_eventtime_tz HAVING MIN (deterministic_customer_profile_association_wrk.header_eventtime))  min_header_eventtime_tz,
            max(deterministic_customer_profile_association_wrk.header_eventtime) AS max_header_eventtime,
            ANY_VALUE(deterministic_customer_profile_association_wrk.header_eventtime_tz HAVING max (deterministic_customer_profile_association_wrk.header_eventtime))  max_header_eventtime_tz,
          FROM
            `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_profile_association_wrk{{params.tbl_sfx}}
        UNION ALL
        SELECT
              min(deterministic_customer_classification_wrk.header_eventtime) AS min_header_eventtime,
             ANY_VALUE(deterministic_customer_classification_wrk.header_eventtime_tz HAVING MIN (deterministic_customer_classification_wrk.header_eventtime))  min_header_eventtime_tz,
            max(deterministic_customer_classification_wrk.header_eventtime) AS max_header_eventtime,
            ANY_VALUE(deterministic_customer_classification_wrk.header_eventtime_tz HAVING max (deterministic_customer_classification_wrk.header_eventtime))  max_header_eventtime_tz,
          FROM
             `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.deterministic_customer_classification_wrk{{params.tbl_sfx}}
      ) AS t
) AS stg WHERE upper(rtrim(ctrl.subject_area_nm, ' ')) = 'DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}'
 AND stg.min_header_eventtime IS NOT NULL;
-- {tbl_sfx}



MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit{{params.tbl_sfx}} AS hist USING --  {tbl_sfx}
(
  SELECT
      *
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE upper(rtrim(subject_area_nm, ' ')) = 'DETERMINISTIC_CUSTOMER_PROFILE{{params.tbl_sfx}}'
) AS control
ON control.subject_area_nm = hist.subject_area_nm
 AND control.batch_id = hist.dw_batch_id
   WHEN MATCHED THEN UPDATE SET
    dw_batch_date = control.curr_batch_date, 
    extract_from = control.extract_from_tmstp_utc, 
    extract_from_tz = control.extract_from_tmstp_tz, 
    extract_to = control.extract_to_tmstp_utc , 
    extract_to_tz = control.extract_to_tmstp_tz , 
    status_code = CASE
      WHEN upper(rtrim(control.active_load_ind, ' ')) = 'Y' THEN 'RUNNING'
      ELSE 'FINISHED'
    END, 
    is_adhoc_run = 'N', 
    dw_sys_start_tmstp = control.batch_start_tmstp,
    dw_sys_end_tmstp = control.batch_end_tmstp, 
    src_s3_path = '{{params.src_s3_path}}'
   WHEN NOT MATCHED BY TARGET THEN
    INSERT
    VALUES (control.subject_area_nm, control.batch_id, control.curr_batch_date,control.extract_from_tmstp_utc,extract_from_tmstp_tz,control.extract_to_tmstp_utc,extract_to_tmstp_tz, CASE
      WHEN upper(rtrim(control.active_load_ind, ' ')) = 'Y' THEN 'RUNNING'
      ELSE 'FINISHED'
    END, 'N', control.batch_start_tmstp, control.batch_end_tmstp,'{{params.src_s3_path}}')
;
