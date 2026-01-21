BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wfm_ideal_hours_load_2656_napstore_insights;
---Task_Name=wfm_ideal_hours_load_1_fct_table;'*/
--ET;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS wfm_ideal_labor_hours_temp
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.ideal_labor_hours_stg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated_at DESC, kafka_last_updated_at DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


--ET;
BEGIN
SET _ERROR_CODE  =  0;
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.ideal_labor_hours_fact AS tgt
USING wfm_ideal_labor_hours_temp AS src
ON LOWER(src.id) = LOWER(tgt.id)
WHEN MATCHED THEN UPDATE SET
    created_at = src.created_at,
    last_updated_at = src.last_updated_at,
    store_number = src.store_number,
    workgroup_id = src.id,
    labor_role_id = src.labor_role_id,
    effective_date = PARSE_DATE('%F', src.effective_date),
    local_effective_time = TIME_ADD(TIME '00:00:00' ,INTERVAL CAST(CAST (TRUNC(CAST(src.LOCAL_EFFECTIVE_TIME AS FLOAT64)) AS INTEGER) / 1000  AS INT64) SECOND),
    minutes = src.minutes,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(src.id, src.created_at, src.last_updated_at, src.store_number, src.workgroup_id, src.labor_role_id, PARSE_DATE('%F', src.effective_date),TIME_ADD(TIME '00:00:00' ,INTERVAL CAST(CAST (TRUNC(CAST(src.LOCAL_EFFECTIVE_TIME AS FLOAT64)) AS INTEGER) / 1000  AS INT64) SECOND), src.minutes, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--ET;
/*SET QUERY_BAND = NONE FOR SESSION;*/
--ET;
END;
