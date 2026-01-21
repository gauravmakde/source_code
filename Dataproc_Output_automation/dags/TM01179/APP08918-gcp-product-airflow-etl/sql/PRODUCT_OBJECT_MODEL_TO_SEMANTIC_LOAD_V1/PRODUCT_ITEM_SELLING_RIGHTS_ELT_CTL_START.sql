
UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control 
SET
 active_load_ind = 'Y',
 batch_id = (SELECT batch_id + 1
  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')),
 curr_batch_date = CASE
  WHEN elt_control.curr_batch_date < CURRENT_DATE('PST8PDT')
  THEN DATE_ADD(elt_control.curr_batch_date, INTERVAL 1 DAY)
  ELSE elt_control.curr_batch_date
  END,
  extract_from_tmstp = TIMESTAMP_SUB(extract_to_tmstp, INTERVAL 1 DAY),
  extract_from_tmstp_tz = extract_to_tmstp_tz,
  extract_to_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
  extract_to_tmstp_tz =  `{{params.dataplex_project_id}}`.jwn_udf.DEFAULT_TZ_PST(),
  batch_start_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  batch_start_tmstp_tz = `{{params.dataplex_project_id}}`.jwn_udf.DEFAULT_TZ_PST()
WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS') AND LOWER(active_load_ind) = LOWER('N') AND
   curr_batch_date <= CURRENT_DATE('PST8PDT') AND (SELECT CASE
     WHEN MAX(cnt) > 1
     THEN CAST(ERROR('Cannot insert duplicate values') AS BIGINT)
     ELSE MAX(cnt)
     END AS cnt
   FROM (SELECT subject_area_nm,
      extract_from_tmstp,
      batch_end_tmstp,
      COUNT(1) AS cnt
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')
      AND LOWER(active_load_ind) = LOWER('N')
      AND curr_batch_date <= CURRENT_DATE('PST8PDT')
     GROUP BY subject_area_nm,
      extract_from_tmstp,
      batch_end_tmstp) AS t1) < 2;