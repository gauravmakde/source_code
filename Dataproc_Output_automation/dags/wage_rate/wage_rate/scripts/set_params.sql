


INSERT INTO  <bq_project_id>.<DBENV>_sca_prf.batch_parameters (interface_id, subject_id, batch_id, parameter_key, parameter_value)
(SELECT interface_id,
  subject_id,
  btch_id,
  '<suffix>' AS parameter_key,
        'year=' || TRIM(FORMAT('%20d', EXTRACT(YEAR FROM curr_load_dt))) || '/month=' || TRIM(FORMAT('%20d', EXTRACT(YEAR FROM curr_load_dt))) || '/day=' || TRIM(FORMAT('%11d', EXTRACT(DAY FROM curr_load_dt))) || '/part-*' AS parameter_value
 FROM  <bq_project_id>.<DBENV>_sca_prf.dl_interface_dt_lkup
 WHERE end_tmstp IS NULL
  AND LOWER(interface_id) = LOWER('<project_id>')
  AND LOWER(subject_id) = LOWER('<subject_id>'));

-- COMMIT TRANSACTION;
