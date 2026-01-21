UPDATE
  <bq_project_id>.<DBENV>_sca_prf.dl_interface_dt_lkup
SET
  end_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHERE
  end_tmstp IS NULL
  AND LOWER(interface_id) = LOWER('<project_id>')
  AND LOWER(subject_id) = LOWER('<subject_id>');
  -- COMMIT TRANSACTION;