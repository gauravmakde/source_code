-- SET QUERY_BAND = '
-- App_ID=app02432;
-- DAG_ID=hr_outoforder_worker_data_daily_load_2656_napstore_insights;
-- Task_Name=line_position_details_stage_load_7_lineposition_details_dim_tables;'
-- FOR SESSION VOLATILE;

-- ET;

CREATE TEMPORARY TABLE IF NOT EXISTS hr_worker_line_position_details_temp
AS
SELECT DISTINCT worker_number,
 worker_type,
 line_position_details_last_updated,
 line_position_details_last_updated_tz,
 position_details_transaction_id,
  CASE
  WHEN LOWER(beauty_line_assignment) = LOWER('')
  THEN NULL
  ELSE beauty_line_assignment
  END AS beauty_line_assignment,
  CASE
  WHEN LOWER(beauty_line_assignment_id) = LOWER('')
  THEN NULL
  ELSE beauty_line_assignment_id
  END AS beauty_line_assignment_id,
  CASE
  WHEN LOWER(other_line_assignment) = LOWER('')
  THEN NULL
  ELSE other_line_assignment
  END AS other_line_assignment,
  CASE
  WHEN LOWER(line_assignment) = LOWER('')
  THEN NULL
  ELSE line_assignment
  END AS line_assignment,
 line_assignment_change_effective_date,
 line_assignment_change_effective_date_tz
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_stg.hr_worker_v1_ldg
WHERE position_details_transaction_id IS NOT NULL
QUALIFY (ROW_NUMBER() OVER (PARTITION BY worker_number, worker_type, position_details_transaction_id ORDER BY
      line_position_details_last_updated DESC)) = 1;

--ET;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_dim.hr_line_position_details_dim AS tgt
USING hr_worker_line_position_details_temp AS src
ON LOWER(src.position_details_transaction_id) = LOWER(tgt.transaction_id) AND LOWER(src.worker_number) = LOWER(tgt.worker_number
    ) AND LOWER(src.worker_type) = LOWER(tgt.worker_type)
WHEN MATCHED THEN UPDATE SET
 last_updated = src.line_position_details_last_updated,
 last_updated_tz = src.line_position_details_last_updated_tz,
 beauty_line_assignment = src.beauty_line_assignment,
 beauty_line_assignment_id = src.beauty_line_assignment_id,
 other_line_assignment = src.other_line_assignment,
 line_assignment = src.line_assignment,
 line_assignment_change_effective_tmstp = CAST(src.line_assignment_change_effective_date AS DATETIME),
 dw_batch_date = CURRENT_DATE('PST8PDT'),
 dw_sys_load_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT (worker_number, worker_type, last_updated, last_updated_tz, transaction_id,
 beauty_line_assignment, beauty_line_assignment_id, other_line_assignment, line_assignment,
 line_assignment_change_effective_tmstp, dw_batch_date, dw_sys_load_tmstp) VALUES(src.worker_number, src.worker_type,
 src.line_position_details_last_updated, src.line_position_details_last_updated_tz, src.position_details_transaction_id
 , src.beauty_line_assignment, src.beauty_line_assignment_id, src.other_line_assignment, src.line_assignment, CAST(src.line_assignment_change_effective_date AS DATETIME), CURRENT_DATE('PST8PDT'), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 );


-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;

