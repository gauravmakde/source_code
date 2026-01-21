  CREATE TEMPORARY TABLE IF NOT EXISTS wfm_labor_demand_temp AS
SELECT
  DISTINCT *
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.labor_demand_stg
QUALIFY
  (ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated_at DESC, kafka_last_updated_at DESC)) = 1;


MERGE INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.labor_demand_fact AS tgt
USING
  wfm_labor_demand_temp AS src
ON
  LOWER(src.id) = LOWER(tgt.id)
  WHEN MATCHED THEN UPDATE 
SET created_at = src.created_at,
  last_updated_at = src.last_updated_at, 
  store_number = src.store_number, 
  workgroup_id = src.id, 
  labor_role_id = src.labor_role_id,
  effective_date = PARSE_DATE('%F', src.effective_date), 
  --LOCAL_EFFECTIVE_TIME = TIME '00:00:00' + CAST (SRC.LOCAL_EFFECTIVE_TIME AS INTEGER) / 1000 * INTERVAL '00:00:01' HOUR TO SECOND,
  LOCAL_EFFECTIVE_TIME =TIME_ADD( TIME '00:00:00', INTERVAL CAST(CAST(trunc(cast(SRC.LOCAL_EFFECTIVE_TIME as float64)) AS INTEGER) / 1000 AS INT64) SECOND),
  minutes = src.minutes,
  dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME)
  WHEN NOT MATCHED
  THEN
INSERT
  (id,
    created_at,
    last_updated_at,
    store_number,
    workgroup_id,
    labor_role_id,
    effective_date,
    local_effective_time,
    minutes,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp)
VALUES
  (src.id, src.created_at,
   
  src.last_updated_at, src.store_number,
  src.workgroup_id,
  src.labor_role_id,
  PARSE_DATE('%F', src.effective_date),
 TIME_ADD( TIME '00:00:00', INTERVAL CAST(CAST(trunc(cast(SRC.LOCAL_EFFECTIVE_TIME as float64)) AS INTEGER) / 1000 AS INT64) SECOND),
  -- TIME '00:00:00' + CAST (SRC.LOCAL_EFFECTIVE_TIME AS INTEGER) / 1000 * INTERVAL '00:00:01' HOUR TO SECOND,
  src.minutes,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME));