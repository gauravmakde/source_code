CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.BATCH_END_SP ('PREV_DAY_OLDF_BATCH_COMPLETED');


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.BATCH_CLEAN_SP ('PREV_DAY_OLDF_BATCH_COMPLETED');
