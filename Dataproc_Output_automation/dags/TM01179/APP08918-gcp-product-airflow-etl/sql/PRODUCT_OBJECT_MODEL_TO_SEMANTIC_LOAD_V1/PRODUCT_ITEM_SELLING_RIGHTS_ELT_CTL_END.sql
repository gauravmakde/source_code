UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control SET
 active_load_ind = 'N',
 batch_end_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 batch_end_tmstp_tz = `{{params.dataplex_project_id}}`.jwn_udf.DEFAULT_TZ_PST()
WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS') 
AND LOWER(active_load_ind) = LOWER('Y')
 AND (SELECT
     CASE
     WHEN MAX(cnt) > 1
     THEN CAST(ERROR('Cannot insert duplicate values') AS BIGINT)
     ELSE MAX(cnt)
     END AS cnt
   FROM (SELECT subject_area_nm,
      batch_id,
      curr_batch_date,
      extract_from_tmstp,
      extract_to_tmstp,
      batch_start_tmstp,
      COUNT(1) AS cnt
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')
      AND LOWER(active_load_ind) = LOWER('Y')
     GROUP BY subject_area_nm,
      batch_id,
      curr_batch_date,
      extract_from_tmstp,
      extract_to_tmstp,
      batch_start_tmstp) AS t1) < 2;