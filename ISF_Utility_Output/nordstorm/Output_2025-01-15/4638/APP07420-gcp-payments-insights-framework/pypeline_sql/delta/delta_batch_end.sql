--batch end logic
UPDATE {delta_schema_name}.delta_elt_control_tbl
SET
    active_load_ind = 'N',
    batch_end_tmstp = current_timestamp(),
    rcd_update_tmstp = current_timestamp()
WHERE
    dag_name = 'session_event_delta'
    AND subject_area_name = 'session_event_metadata'
    AND is_latest = 'Y'
    AND active_load_ind = 'Y';
