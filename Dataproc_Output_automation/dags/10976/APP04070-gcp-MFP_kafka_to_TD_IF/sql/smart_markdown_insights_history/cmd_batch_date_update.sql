

----ET;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.etl_batch_dt_lkup
  SET DW_BATCH_DT      = DW_BATCH_DT      + INTERFACE_FREQ,
      EXTRACT_START_DT = EXTRACT_START_DT + INTERFACE_FREQ,
      EXTRACT_END_DT   = EXTRACT_END_DT   + INTERFACE_FREQ,
      RCD_UPDT_TMSTP   = CURRENT_DATETIME('PST8PDT')
WHERE LOWER(INTERFACE_CODE) = LOWER('CMD_WKLY');






---ET;
