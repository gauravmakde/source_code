BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfpc_spe_mapping_kafka_to_td;
---Task_Name=job_spe_mapping_third_exec_2;'*/
---FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_dim}}.merch_supplier_groupid_assignment_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_supplier_groupid_assignment_ldg AS stg
    WHERE LOWER(tgt.supplier_group_id) = LOWER(supplier_group_id) 
    AND LOWER(last_updated_time_in_millis) <> LOWER('last_updated_time_in_millis') 
    AND tgt.last_updated_time_in_millis < CAST(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END AS BIGINT));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_supplier_groupid_assignment_ldg AS stg
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.database_name_dim}}.merch_supplier_groupid_assignment_dim AS tgt
    WHERE LOWER(stg.supplier_group_id) = LOWER(supplier_group_id) 
    AND LOWER(stg.last_updated_time_in_millis) <> LOWER('last_updated_time_in_millis') 
    AND CAST(CASE WHEN stg.last_updated_time_in_millis = '' THEN '0' ELSE stg.last_updated_time_in_millis END AS BIGINT) <= last_updated_time_in_millis);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.database_name_dim}}.merch_supplier_groupid_assignment_dim 
(supplier_group_id, selling_brand, department_number,
 supplier_group, is_active, event_time, event_time_tz, last_updated_time_in_millis, last_updated_time, dw_sys_load_tmstp)
(SELECT supplier_group_id,
  selling_brand,
  CAST(department_number AS INTEGER) AS department_number,
  supplier_group,
   CASE
   WHEN LOWER(is_active) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS is_active,
  CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_time) AS TIMESTAMP) AS event_time,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(event_time as string)) AS event_time_tz,
  CAST(CASE
    WHEN last_updated_time_in_millis = ''
    THEN '0'
    ELSE last_updated_time_in_millis
    END AS BIGINT) AS last_updated_time_in_millis,
  CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS BIGINT)) AS DATETIME) AS last_updated_time,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.database_name_staging}}.merch_supplier_groupid_assignment_ldg AS msga
 WHERE LOWER(last_updated_time_in_millis) <> LOWER('last_updated_time_in_millis'));
 
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

END;
