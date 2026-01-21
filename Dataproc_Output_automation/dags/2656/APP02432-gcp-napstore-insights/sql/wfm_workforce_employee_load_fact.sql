BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_workforce_employee_load_2656_napstore_insights;
---Task_Name=wfm_workforce_employee_load_1_fct_table;'*/
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS wfm_employee_home_store_temp
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.employee_home_store_stg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY employee_id, store_number ORDER BY last_updated_at DESC, kafka_last_updated_at
      DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.employee_home_store_fact AS tgt
USING wfm_employee_home_store_temp AS src
ON LOWER(src.employee_id) = LOWER(tgt.employee_id) AND LOWER(src.store_number) = LOWER(tgt.store_number) AND src.created_at = tgt.created_at
WHEN MATCHED THEN UPDATE SET
    last_updated_at = src.last_updated_at,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT 
VALUES(
src.employee_id, 
src.store_number, 
src.created_at, 
src.last_updated_at, 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
