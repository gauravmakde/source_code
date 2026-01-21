

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_END_LOAD('DETERMINISTIC_CUSTOMER_DIM{{params.tbl_sfx}}');

BEGIN
merge into `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.deterministic_profile_batch_hist_audit as hist
-- MERGE INTO prd_nap_base_vws.deterministic_profile_batch_hist_audit AS hist
USING (SELECT 
subject_area_nm			,
active_load_ind			,
batch_id			    ,
curr_batch_date			,
extract_from_tmstp		,	
extract_from_tmstp_utc	,	
`{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(extract_from_tmstp AS STRING)) AS extract_from_tmstp_tz,			
extract_to_tmstp		,	
extract_to_tmstp_utc	,		
`{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(CAST(extract_to_tmstp AS STRING)) AS extract_to_tmstp_tz,	
batch_start_tmstp		,	
batch_start_tmstp_utc	,		
batch_start_tmstp_tz	,		
batch_end_tmstp			,
batch_end_tmstp_utc		,	
batch_end_tmstp_tz			
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control 
    WHERE LOWER(subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_DIM{tbl_sfx}')) AS control
ON LOWER(control.subject_area_nm) = LOWER(hist.subject_area_nm) AND control.batch_id = hist.dw_batch_id
WHEN MATCHED THEN UPDATE SET
extract_from_tz = cast(control.extract_from_tmstp_tz as string),
extract_to_tz = cast(extract_to_tmstp_tz as string),
    dw_batch_date = control.curr_batch_date,
    extract_from = cast(control.extract_from_tmstp as timestamp), 
    extract_to = cast(control.extract_to_tmstp as timestamp),
    status_code = CASE WHEN LOWER(control.active_load_ind) = LOWER('Y') THEN 'RUNNING' ELSE 'FINISHED' END,
    is_adhoc_run = 'N',
    dw_sys_start_tmstp = control.batch_start_tmstp,
    dw_sys_end_tmstp = control.batch_end_tmstp,
    src_s3_path = '<SRC_S3_PATH>'
WHEN NOT MATCHED THEN INSERT VALUES(control.subject_area_nm, 
control.batch_id, control.curr_batch_date, 
cast(control.extract_from_tmstp as timestamp),
 extract_from_tmstp_tz ,
cast(control.extract_to_tmstp as timestamp),
extract_to_tmstp_tz , 
CASE WHEN LOWER(control.active_load_ind) = LOWER('Y') THEN 'RUNNING' ELSE 'FINISHED' END, 'N', 
control.batch_start_tmstp, control.batch_end_tmstp, '<SRC_S3_PATH>');
-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;
END;


