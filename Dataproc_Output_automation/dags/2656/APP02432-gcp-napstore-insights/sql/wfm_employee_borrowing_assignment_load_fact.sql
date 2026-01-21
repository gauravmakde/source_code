BEGIN

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;


EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS wfm_employee_borrowing_assignment_temp
--CLUSTER BY id
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.employee_borrowing_assignment_stg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated_at DESC, kafka_last_updated_at DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
COMMIT TRANSACTION;

BEGIN
SET _ERROR_CODE  =  0;
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.employee_borrowing_assignment_fact AS tgt
USING wfm_employee_borrowing_assignment_temp AS src
ON LOWER(src.id) = LOWER(tgt.id)
WHEN MATCHED THEN UPDATE SET
    created_at = src.created_at,
    last_updated_at = src.last_updated_at,
    employee_id = src.id,
    start_date = PARSE_DATE('%F', src.start_date),
    end_date = CASE WHEN src.end_date IS NULL THEN NULL ELSE PARSE_DATE('%F', src.end_date) END,
    store_number = src.store_number,
    is_deleted = cast(trunc(cast(CASE WHEN src.is_deleted = '' THEN '0' ELSE src.is_deleted END as float64)) as integer),
    deleted_at = src.deleted_at,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT 
VALUES(src.id, 
src.created_at, 
src.last_updated_at, 
src.employee_id, 
PARSE_DATE('%F', src.start_date), 
CASE WHEN src.end_date IS NULL THEN NULL ELSE PARSE_DATE('%F', src.end_date) END, src.store_number, 
cast(trunc(cast(CASE WHEN src.is_deleted = '' THEN '0' ELSE src.is_deleted END  as float64)) as integer),
 src.deleted_at, 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
COMMIT TRANSACTION;

/*SET QUERY_BAND = NONE FOR SESSION;*/
COMMIT TRANSACTION;

END;
