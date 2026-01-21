


SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME);


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.retail_tran_deterministic_assoc_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.retail_tran_deterministic_assoc_wrk (global_tran_id, business_day_date, deterministic_profile_id
 , dw_sys_updt_tmstp)
(SELECT DISTINCT CAST(dim.unique_source_id AS BIGINT) AS unique_source_id,
  dim.business_day_date,
  dim.deterministic_profile_id,
  dim.dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_profile_association_dim AS dim
  INNER JOIN (SELECT COALESCE(MAX(extract_to), CAST('1900-01-01 00:00:00' AS DATETIME)) AS extract_to_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_profile_batch_hist_audit AS hist
   WHERE dw_batch_id IN (SELECT MAX(dw_batch_id) AS dw_batch_id
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_profile_batch_hist_audit
      WHERE LOWER(status_code) = LOWER('FINISHED')
       AND LOWER(subject_area_nm) = LOWER('UPDATE_RETAIL_TRAN_HDR_DETERMINISTIC_ASSOCIATION{{params.tbl_sfx}}'))) AS lastrun ON
  TRUE
 WHERE dim.dw_sys_updt_tmstp > CAST(lastrun.extract_to_tmstp AS DATETIME)
  AND dim.business_day_date IS NOT NULL
  AND dim.unique_source_id IS NOT NULL
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY dim.unique_source_id, dim.business_day_date ORDER BY dim.profile_event_tmstp
      DESC)) = 1);



      INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.retail_tran_hdr_fct_err (business_day_date, global_tran_id, error_table, error_code, error_desc
 , dw_batch_date, deterministic_profile_id)
(SELECT DISTINCT wrk.business_day_date,
  wrk.global_tran_id,
  'DETERMINISTIC_CUSTOMER_PROFILE_ASSOCIATION_DIM' AS error_table,
  '4' AS error_code,
  'Multiple DETERMINISTIC_PROFILE_ID associated to the same ERTM transaction' AS error_desc,
  CNTL.curr_batch_date AS dw_batch_date,
  wrk.deterministic_profile_id
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_deterministic_assoc_wrk AS wrk
  INNER JOIN (SELECT global_tran_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_deterministic_assoc_wrk
   WHERE deterministic_profile_id IS NOT NULL
   GROUP BY global_tran_id
   HAVING COUNT(*) > 1) AS wrk_dupes ON wrk.global_tran_id = wrk_dupes.global_tran_id
  INNER JOIN (SELECT *
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('SALES_FACT{{params.tbl_sfx}}')) AS CNTL ON TRUE);




   INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.retail_tran_hdr_fct_err (business_day_date, global_tran_id, error_table, error_code, error_desc
 , dw_batch_date, deterministic_profile_id)
(SELECT DISTINCT wrk.business_day_date,
  wrk.global_tran_id,
  'DETERMINISTIC_CUSTOMER_PROFILE_ASSOCIATION_DIM' AS error_table,
  '4' AS error_code,
  'Transaction from DETERMINISTIC_CUSTOMER_PROFILE_ASSOCIATION_DIM not found in ERTM fact' AS error_desc,
  CNTL.curr_batch_date AS dw_batch_date,
  wrk.deterministic_profile_id
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_deterministic_assoc_wrk AS wrk
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_hdr_fact AS fct ON wrk.global_tran_id = fct.global_tran_id AND wrk.business_day_date
     = fct.business_day_date
  INNER JOIN (SELECT *
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('SALES_FACT{{params.tbl_sfx}}')) AS CNTL ON TRUE
 WHERE fct.global_tran_id IS NULL);


 DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.retail_tran_deterministic_assoc_wrk
WHERE global_tran_id IN (SELECT DISTINCT global_tran_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_hdr_fct_err
  WHERE CAST(error_code AS FLOAT64) = 4
   AND dw_batch_date = (SELECT curr_batch_date
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('SALES_FACT{{params.tbl_sfx}}')));



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.retail_tran_hdr_fact AS tgt SET
 deterministic_profile_id = SRC.deterministic_profile_id,
 dw_batch_date = CNTL.curr_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) FROM (SELECT subject_area_nm
   ,
   curr_batch_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('SALES_FACT{{params.tbl_sfx}}')) AS CNTL, (SELECT global_tran_id,
   business_day_date,
   deterministic_profile_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_deterministic_assoc_wrk) AS SRC
WHERE tgt.global_tran_id = SRC.global_tran_id AND tgt.business_day_date = SRC.business_day_date AND LOWER(CNTL.subject_area_nm
   ) = LOWER('SALES_FACT{{params.tbl_sfx}}');



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.elt_control AS cntl SET
 extract_from_tmstp = src.extract_start_tmstp,
 extract_to_tmstp = CAST(src.extract_end_tmstp AS TIMESTAMP) 
 FROM (SELECT
   'UPDATE_RETAIL_TRAN_HDR_DETERMINISTIC_ASSOCIATION{{params.tbl_sfx}}' AS subject_area_nm,
  lastrun.extract_start_tmstp,
  
   COALESCE(wrk.extract_end_tmstp, lastrun.extract_start_tmstp) AS extract_end_tmstp
  FROM (SELECT cast(MAX(dw_sys_updt_tmstp) as TIMESTAMP) AS extract_end_tmstp

    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.retail_tran_deterministic_assoc_wrk) AS wrk
   INNER JOIN (SELECT COALESCE(MAX(extract_to_utc), CAST('1900-01-01 00:00:00' AS TIMESTAMP)) AS extract_start_tmstp
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_profile_batch_hist_audit AS hist
    WHERE dw_batch_id IN (SELECT MAX(dw_batch_id) AS dw_batch_id
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_profile_batch_hist_audit
       WHERE LOWER(status_code) = LOWER('FINISHED')
        AND LOWER(subject_area_nm) = LOWER('UPDATE_RETAIL_TRAN_HDR_DETERMINISTIC_ASSOCIATION{{params.tbl_sfx}}'))) AS lastrun ON
   TRUE) AS src
WHERE LOWER(cntl.subject_area_nm) = LOWER(src.subject_area_nm);

SELECT timestamp(current_datetime('PST8PDT'));