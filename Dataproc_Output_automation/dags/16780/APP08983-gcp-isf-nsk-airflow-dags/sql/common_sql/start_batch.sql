
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.ELT_CONTROL_START_LOAD('{{params.subject_area}}');

END;