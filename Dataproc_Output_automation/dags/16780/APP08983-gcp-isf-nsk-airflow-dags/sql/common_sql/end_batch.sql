BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_END_LOAD('{{params.subject_area}}');

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log
 SET
    metric_value =  FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')),
    metric_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME) AS TIMESTAMP),
	metric_tmstp_tz = 'GMT'     
WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}') AND LOWER(metric_nm) = LOWER('ISF_DAG_END_TMSTP') AND batch_id = (SELECT batch_id
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
END;