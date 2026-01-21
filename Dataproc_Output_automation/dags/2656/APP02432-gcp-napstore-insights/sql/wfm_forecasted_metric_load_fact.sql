CREATE TEMPORARY TABLE IF NOT EXISTS wfm_forecasted_metric_temp
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.forecasted_metric_stg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_updated_at DESC, kafka_last_updated_at DESC)) = 1;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.forecasted_metric_fact AS tgt
USING wfm_forecasted_metric_temp AS src
ON LOWER(src.id) = LOWER(tgt.id)
WHEN MATCHED THEN UPDATE SET
    created_at = src.created_at,
    last_updated_at = src.last_updated_at,
    store_number = src.store_number,
    metric_id = src.id,
    metric_frequency = src.metric_frequency,
    forecast_group_id = src.forecast_group_id,
    effective_date = PARSE_DATE('%F', src.effective_date),
	local_effective_time = TIME_ADD( TIME '00:00:00', INTERVAL CAST(CAST(TRUNC(CAST(SRC.LOCAL_EFFECTIVE_TIME AS FLOAT64)) AS INTEGER) / 1000 AS INT64) SECOND),
    final_forecast_value = src.final_forecast_value,
    baseline_forecast_value = src.baseline_forecast_value,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT (id, 
store_number, 
metric_id, 
forecast_group_id, 
effective_date, 
local_effective_time,
final_forecast_value, 
baseline_forecast_value, 
created_at, 
last_updated_at, 
dw_sys_load_tmstp, 
dw_sys_updt_tmstp, 
metric_frequency) 
VALUES(
src.id, 
src.store_number, 
src.metric_id, 
src.forecast_group_id, 
PARSE_DATE('%F', src.effective_date), 
TIME_ADD( TIME '00:00:00', INTERVAL CAST(CAST(TRUNC(CAST(SRC.LOCAL_EFFECTIVE_TIME AS FLOAT64)) AS INTEGER) / 1000 AS INT64) SECOND),
src.final_forecast_value, 
src.baseline_forecast_value, 
src.created_at, 
src.last_updated_at, 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), 
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), 
src.metric_frequency);
