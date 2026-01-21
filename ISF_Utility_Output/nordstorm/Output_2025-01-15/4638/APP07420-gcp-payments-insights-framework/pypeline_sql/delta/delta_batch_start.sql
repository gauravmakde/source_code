--creating persistant table to use for other event joins
-- noqa: disable=all
CREATE TABLE IF NOT EXISTS {delta_schema_name}.delta_elt_control_tbl
(
    dag_name string,
    subject_area_name string,
    active_load_ind char(1),
    batch_Id int,
    batch_date date,
    batch_start_tmstp timestamp,
    batch_end_tmstp timestamp,
    last_processed_batch_id int,
    upstream_extract_from_batch_id int,
    upstream_extract_to_batch_id int,
    upstream_dag_name string,
    is_latest char(1),
    rcd_load_tmstp timestamp,
    rcd_update_tmstp timestamp
)
USING DELTA
LOCATION 's3://{delta_s3_bucket}/session_event_delta/delta_elt_control_tbl/';
-- noqa: enable=all

--updating the is_latest before starting a new batch
UPDATE {delta_schema_name}.delta_elt_control_tbl
SET
    is_latest = 'N',
    rcd_update_tmstp = current_timestamp()
WHERE
    dag_name = 'session_event_delta'
    AND subject_area_name = 'session_event_metadata'
    AND is_latest = 'Y';

--batch start logic
WITH CTE AS (
    SELECT max(batch_id) AS max_batch_id
    FROM {delta_schema_name}.delta_elt_control_tbl
    WHERE
        dag_name = 'session_event_delta'
        AND subject_area_name = 'session_event_metadata'
),

batch_open AS (
    SELECT CASE WHEN active_load_ind = 'Y' THEN raise_error('Cant Start a new batch as batch is already open') ELSE 'Y' END AS active_load_ind
    FROM {delta_schema_name}.delta_elt_control_tbl
    WHERE batch_id = (SELECT max_batch_id FROM CTE)
),

latest_found AS (
    SELECT CASE WHEN is_latest = 'Y' THEN raise_error('Cant Start a new latest batch as old entry is still latest') ELSE 'Y' END AS is_latest
    FROM {delta_schema_name}.delta_elt_control_tbl
    WHERE batch_id = (SELECT max_batch_id FROM CTE)
)

INSERT INTO TABLE {delta_schema_name}.delta_elt_control_tbl
SELECT
    'session_event_delta' AS dag_name,
    'session_event_metadata' AS subject_area_name,
    (SELECT active_load_ind from batch_open) AS active_load_ind,
    (SELECT max_batch_id + 1 from CTE) AS batch_Id,
    current_date() AS batch_date,
    current_timestamp() AS batch_start_tmstp,
    null AS batch_end_tmstp,
    null AS last_processed_batch_id,
    null AS upstream_extract_from_batch_id,
    null AS upstream_extract_to_batch_id,
    null AS upstream_dag_name,
    (SELECT is_latest from latest_found) AS is_latest,
    current_timestamp() AS rcd_load_tmstp,
    current_timestamp() AS rcd_update_tmstp;
