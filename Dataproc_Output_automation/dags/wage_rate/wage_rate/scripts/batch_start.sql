INSERT INTO <bq_project_id>.<DBENV>_SCA_PRF.dl_interface_dt_lkup (interface_id, subject_id, btch_id, curr_load_dt, start_tmstp)
(SELECT DISTINCT interface_id,
  subject_id,
  btch_id,
  curr_load_dt,
  start_tmstp
 FROM (SELECT interface_id,
    subject_id,
     CASE
     WHEN end_tmstp IS NOT NULL AND DATE_ADD(curr_load_dt, INTERVAL 1 DAY) <= CURRENT_DATE
     THEN COALESCE((SELECT MAX(btch_id)
        FROM <bq_project_id>.<DBENV>_SCA_PRF.dl_interface_dt_lkup
        WHERE LOWER(interface_id) = LOWER('<project_id>')
         AND LOWER(subject_id) = LOWER('<subject_id>')), 0) + 1
     ELSE CAST((SELECT 999999999999999999) AS BIGINT)
     END AS btch_id,
    DATE_ADD(curr_load_dt, INTERVAL 1 DAY) AS curr_load_dt,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS start_tmstp
   FROM (SELECT *
     FROM <bq_project_id>.<DBENV>_SCA_PRF.dl_interface_dt_lkup
     WHERE start_tmstp = (SELECT MAX(start_tmstp)
        FROM <bq_project_id>.<DBENV>_SCA_PRF.dl_interface_dt_lkup
        WHERE LOWER(interface_id) = LOWER('<project_id>') 
         AND LOWER(subject_id) = LOWER('<subject_id>')) 
      AND LOWER(interface_id) = LOWER('<project_id>') 
      AND LOWER(subject_id) = LOWER('<subject_id>')) AS t1  
      ) AS t9
   WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM <bq_project_id>.<DBENV>_SCA_PRF.dl_interface_dt_lkup
     WHERE interface_id = t9.interface_id
      AND subject_id = t9.subject_id
      AND btch_id = t9.btch_id)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY interface_id, subject_id, btch_id)) = 1);