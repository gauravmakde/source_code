

INSERT INTO <DBENV>_SCA_PRF.wage_rate_opr
 (store,
  department,
  average_rate,
  rate_date,
  btch_id,
  rcd_load_tmstp,
 rcd_updt_tmstp)
(SELECT store,
  department,
  ROUND(CAST(average_rate AS NUMERIC), 4) AS average_rate,
  CAST(rate_date AS DATE) AS rate_date,
   (SELECT CAST(FLOOR(btch_id) AS INT64)
   FROM <DBENV>_SCA_PRF.dl_interface_dt_lkup
   WHERE end_tmstp IS NULL
    AND LOWER(interface_id) = LOWER('<project_id>')
    AND LOWER(subject_id) = LOWER('<subject_id>')) AS btch_id,
  CURRENT_DATETIME('PST8PDT') AS rcd_load_tmstp,
  CURRENT_DATETIME('PST8PDT') AS rcd_updt_tmstp
 FROM <DBENV>_SCA_PRF.wage_rate_stg);

-- COMMIT TRANSACTION;


