BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_customer_object_ldg_to_dim_17393_customer_das_customer;

---Task_Name=customer_object_ldg_to_wrk;'*/
---FOR SESSION VOLATILE;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_PROGRAM_LDG;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_EMAIL_LDG;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_TELEPHONE_LDG;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_POSTAL_ADDRESS_LDG;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_MERGE_ALIAS_LDG;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_PAYMENT_METHOD_LDG;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_MASTER_LDG;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_CREDIT_ACCOUNT_LDG;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_LINKED_PROGRAM_LDG;

BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_program_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_program_wrk{{params.tbl_sfx}}
(SELECT ldg.unique_source_id,
  ldg.customer_source,
  ldg.program_index,
  ldg.program_index_id,
  ldg.program_index_name,
  ldg.event_type,
  ldg.object_event_tmstp AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.object_event_tmstp AS STRING)) AS object_event_tmstp_tz,
  dim.dw_sys_load_tmstp
 FROM (SELECT uniquesourceid AS unique_source_id,
    SUBSTR(uniquesourceid, 1, STRPOS(LOWER(uniquesourceid), LOWER('::')) - 1) AS customer_source,
    programsindex AS program_index,
     CASE
     WHEN LOWER(COALESCE(programsindex, 'null')) <> LOWER('null')
     THEN SUBSTR(programsindex, STRPOS(LOWER(programsindex), LOWER('::')) + 2)
     ELSE NULL
     END AS program_index_id,
     CASE
     WHEN LOWER(COALESCE(programsindex, 'null')) <> LOWER('null')
     THEN SUBSTR(programsindex, 1, STRPOS(LOWER(programsindex), LOWER('::')) - 1)
     ELSE NULL
     END AS program_index_name,
    event_eventtype AS event_type,
    CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC (CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS
    object_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_program_ldg{{params.tbl_sfx}}) AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_program_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.unique_source_id) = LOWER(dim.unique_source_id)
 WHERE ldg.object_event_tmstp > dim.object_event_tmstp
  OR dim.object_event_tmstp IS NULL
 QUALIFY (RANK() OVER (PARTITION BY ldg.unique_source_id ORDER BY ldg.object_event_tmstp DESC)) = 1);



EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_PROGRAM_WRK;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_email_lvl1_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_email_lvl1_wrk{{params.tbl_sfx}}
(SELECT ldg.uniquesourceid AS unique_source_id,
  ldg.customeridentity_stdemail_value AS std_email_address_token_value,
  ldg.customeridentity_stdemail_authority AS std_email_address_token_authority,
  ldg.customeridentity_stdemail_strategy AS std_email_address_token_strategy,
  ldg.customeridentity_stdalternateemails_value AS alt_email_address_token_value,
  ldg.customeridentity_stdalternateemails_authority AS alt_email_address_token_authority,
  ldg.customeridentity_stdalternateemails_strategy AS alt_email_address_token_strategy,
  ldg.event_eventtype AS event_type,
  CASE WHEN ldg.event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(ldg.event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS ldg_object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CASE WHEN ldg.event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(ldg.event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS STRING)) AS ldg_object_event_tmstp_tz,
  dim.dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_email_ldg{{params.tbl_sfx}} AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_email_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.uniquesourceid) = LOWER(dim.unique_source_id)
 WHERE CASE WHEN ldg.event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(ldg.event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END > CAST(dim.object_event_tmstp AS TIMESTAMP)
  OR dim.object_event_tmstp IS NULL
 QUALIFY (RANK() OVER (PARTITION BY ldg.uniquesourceid ORDER BY CASE WHEN ldg.event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(ldg.event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END DESC)) = 1);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_EMAIL_LVL1_WRK;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_email_lvl2_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_email_lvl2_wrk{{params.tbl_sfx}}
(SELECT COALESCE(st.unique_source_id, alt.unique_source_id) AS unique_source_id,
  COALESCE(st.email_address_token_value, alt.email_address_token_value) AS email_address_token_value,
  COALESCE(st.email_address_token_authority, alt.email_address_token_authority) AS email_address_token_authority,
  COALESCE(st.email_address_token_strategy, alt.email_address_token_strategy) AS email_address_token_strategy,
  COALESCE(alt.email_ind, 'N') AS alt_email_ind,
  COALESCE(st.email_ind, 'N') AS std_email_ind,
  COALESCE(st.object_event_tmstp_utc, alt.object_event_tmstp_utc) AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(COALESCE(st.object_event_tmstp_utc, alt.object_event_tmstp_utc) AS STRING)) AS object_event_tmstp_tz,
  COALESCE(st.dw_sys_load_tmstp, alt.dw_sys_load_tmstp) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT unique_source_id,
    std_email_address_token_value AS email_address_token_value,
    std_email_address_token_authority AS email_address_token_authority,
    std_email_address_token_strategy AS email_address_token_strategy,
    'Y' AS email_ind,
    object_event_tmstp_utc,
    dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_email_lvl1_wrk{{params.tbl_sfx}}
   WHERE LOWER(COALESCE(std_email_address_token_value, 'null')) <> LOWER('null')
    AND LOWER(event_type) <> LOWER('DELETE')) AS st
  FULL JOIN (SELECT DISTINCT unique_source_id,
    alt_email_address_token_value AS email_address_token_value,
    alt_email_address_token_authority AS email_address_token_authority,
    alt_email_address_token_strategy AS email_address_token_strategy,
    'Y' AS email_ind,
    object_event_tmstp_utc,
    dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_email_lvl1_wrk{{params.tbl_sfx}}
   WHERE LOWER(COALESCE(alt_email_address_token_value, 'null')) <> LOWER('null')
    AND LOWER(event_type) <> LOWER('DELETE')) AS alt ON LOWER(st.unique_source_id) = LOWER(alt.unique_source_id) AND
    LOWER(st.email_address_token_value) = LOWER(alt.email_address_token_value));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_EMAIL_LVL2_WRK;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_telephone_lvl1_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_telephone_lvl1_wrk{{params.tbl_sfx}}
(SELECT ldg.uniquesourceid AS unique_source_id,
  ldg.customeridentity_stdmobile_value AS std_telephone_number_token_value,
  ldg.customeridentity_stdmobile_authority AS std_telephone_number_token_authority,
  ldg.customeridentity_stdmobile_strategy AS std_telephone_number_token_strategy,
  ldg.customeridentity_stdalternatetelephones_value AS alt_telephone_number_token_value,
  ldg.customeridentity_stdalternatetelephones_authority AS alt_telephone_number_token_authority,
  ldg.customeridentity_stdalternatetelephones_strategy AS alt_telephone_number_token_strategy,
  ldg.event_eventtype AS event_type,
  CASE WHEN ldg.event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(ldg.event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS ldg_object_event_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CASE WHEN ldg.event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(ldg.event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS STRING)) AS ldg_object_event_tmstp_tz,
  dim.dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_telephone_ldg{{params.tbl_sfx}} AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_telephone_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.uniquesourceid) = LOWER(dim.unique_source_id)
 WHERE CASE WHEN ldg.event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(ldg.event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END > CAST(dim.object_event_tmstp AS TIMESTAMP)
  OR dim.object_event_tmstp IS NULL
 QUALIFY (RANK() OVER (PARTITION BY ldg.uniquesourceid ORDER BY CASE WHEN ldg.event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(ldg.event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(ldg.event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END DESC)) = 1);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_TELEPHONE_LVL1_WRK;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_telephone_lvl2_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_telephone_lvl2_wrk{{params.tbl_sfx}}
(SELECT COALESCE(st.unique_source_id, alt.unique_source_id) AS unique_source_id,
  COALESCE(st.telephone_number_token_value, alt.telephone_number_token_value) AS telephone_number_token_value,
  COALESCE(st.telephone_number_token_authority, alt.telephone_number_token_authority) AS
  telephone_number_token_authority,
  COALESCE(st.telephone_number_token_strategy, alt.telephone_number_token_strategy) AS telephone_number_token_strategy,
  COALESCE(alt.telephone_ind, 'N') AS alt_telephone_ind,
  COALESCE(st.telephone_ind, 'N') AS std_telephone_ind,
  COALESCE(st.object_event_tmstp_utc, alt.object_event_tmstp_utc) AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(COALESCE(st.object_event_tmstp_utc, alt.object_event_tmstp_utc) AS STRING)) AS object_event_tmstp_utc,
  COALESCE(st.dw_sys_load_tmstp, alt.dw_sys_load_tmstp) AS dw_sys_load_tmstp
 FROM (SELECT DISTINCT unique_source_id,
    std_telephone_number_token_value AS telephone_number_token_value,
    std_telephone_number_token_authority AS telephone_number_token_authority,
    std_telephone_number_token_strategy AS telephone_number_token_strategy,
    'Y' AS telephone_ind,
    object_event_tmstp_utc,
    dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_telephone_lvl1_wrk{{params.tbl_sfx}}
   WHERE LOWER(COALESCE(std_telephone_number_token_value, 'null')) <> LOWER('null')
    AND LOWER(event_type) <> LOWER('DELETE')) AS st
  FULL JOIN (SELECT DISTINCT unique_source_id,
    alt_telephone_number_token_value AS telephone_number_token_value,
    alt_telephone_number_token_authority AS telephone_number_token_authority,
    alt_telephone_number_token_strategy AS telephone_number_token_strategy,
    'Y' AS telephone_ind,
    object_event_tmstp_utc,
    dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_telephone_lvl1_wrk{{params.tbl_sfx}}
   WHERE LOWER(COALESCE(alt_telephone_number_token_value, 'null')) <> LOWER('null')
    AND LOWER(event_type) <> LOWER('DELETE')) AS alt ON LOWER(st.unique_source_id) = LOWER(alt.unique_source_id) AND
    LOWER(st.telephone_number_token_value) = LOWER(alt.telephone_number_token_value));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_TELEPHONE_LVL2_WRK;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_obj_postal_address_max{{params.tbl_sfx}}
AS
SELECT uniquesourceid AS unique_source_id,
 MAX(CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END) AS
 m_object_event_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_ldg{{params.tbl_sfx}}
GROUP BY unique_source_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATS COLUMN(unique_source_id) ON CUSTOMER_OBJ_POSTAL_ADDRESS_MAX;

BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_postal_address_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_postal_address_wrk{{params.tbl_sfx}}
(SELECT ldg.unique_source_id,
  ldg.customer_source,
  ldg.address_id,
  ldg.first_name_token_value,
  ldg.first_name_token_authority,
  ldg.first_name_token_strategy,
  ldg.middle_name_token_value,
  ldg.middle_name_token_authority,
  ldg.middle_name_token_strategy,
  ldg.last_name_token_value,
  ldg.last_name_token_authority,
  ldg.last_name_token_strategy,
  ldg.std_line_1_token_value,
  ldg.std_line_1_token_authority,
  ldg.std_line_1_token_strategy,
  ldg.std_line_2_token_value,
  ldg.std_line_2_token_authority,
  ldg.std_line_2_token_strategy,
  ldg.std_city_token_value,
  ldg.std_city_token_authority,
  ldg.std_city_token_strategy,
  ldg.state_token_value,
  ldg.state_token_authority,
  ldg.state_token_strategy,
  ldg.country_token_value,
  ldg.country_token_authority,
  ldg.country_token_strategy,
  ldg.std_postal_code_token_value,
  ldg.std_postal_code_token_authority,
  ldg.std_postal_code_token_strategy,
  ldg.address_type,
  ldg.source_audit_create_tmstp AS source_audit_create_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.source_audit_create_tmstp AS STRING)) AS source_audit_create_tmstp_tz,
  ldg.source_audit_update_tmstp AS source_audit_update_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.source_audit_update_tmstp AS STRING)) AS source_audit_update_tmstp_tz,
  ldg.line_1_token_value,
  ldg.line_1_token_authority,
  ldg.line_1_token_strategy,
  ldg.line_2_token_value,
  ldg.line_2_token_authority,
  ldg.line_2_token_strategy,
  ldg.city_token_value,
  ldg.city_token_authority,
  ldg.city_token_strategy,
  ldg.postal_code_token_value,
  ldg.postal_code_token_authority,
  ldg.postal_code_token_strategy,
  ldg.attributed_by,
  ldg.event_type,
  ldg.object_event_tmstp AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.object_event_tmstp AS STRING)) AS object_event_tmstp_tz,
  CAST(dim.dw_sys_load_tmstp AS DATETIME),
  ldg.is_verified_ind
 FROM (SELECT src.uniquesourceid AS unique_source_id,
    SUBSTR(src.uniquesourceid, 1, STRPOS(LOWER(src.uniquesourceid), LOWER('::')) - 1) AS customer_source,
    src.addresses_addressid AS address_id,
    src.addresses_firstname_value AS first_name_token_value,
    src.addresses_firstname_authority AS first_name_token_authority,
    src.addresses_firstname_strategy AS first_name_token_strategy,
    src.addresses_middlename_value AS middle_name_token_value,
    src.addresses_middlename_authority AS middle_name_token_authority,
    src.addresses_middlename_strategy AS middle_name_token_strategy,
    src.addresses_lastname_value AS last_name_token_value,
    src.addresses_lastname_authority AS last_name_token_authority,
    src.addresses_lastname_strategy AS last_name_token_strategy,
    src.addresses_stdline1_value AS std_line_1_token_value,
    src.addresses_stdline1_authority AS std_line_1_token_authority,
    src.addresses_stdline1_strategy AS std_line_1_token_strategy,
    src.addresses_stdline2_value AS std_line_2_token_value,
    src.addresses_stdline2_authority AS std_line_2_token_authority,
    src.addresses_stdline2_strategy AS std_line_2_token_strategy,
    src.addresses_stdcity_value AS std_city_token_value,
    src.addresses_stdcity_authority AS std_city_token_authority,
    src.addresses_stdcity_strategy AS std_city_token_strategy,
    src.addresses_state_value AS state_token_value,
    src.addresses_state_authority AS state_token_authority,
    src.addresses_state_strategy AS state_token_strategy,
    src.addresses_country_value AS country_token_value,
    src.addresses_country_authority AS country_token_authority,
    src.addresses_country_strategy AS country_token_strategy,
    src.addresses_stdpostalcode_value AS std_postal_code_token_value,
    src.addresses_stdpostalcode_authority AS std_postal_code_token_authority,
    src.addresses_stdpostalcode_strategy AS std_postal_code_token_strategy,
     CASE
     WHEN LOWER(SUBSTR(src.uniquesourceid, 1, STRPOS(LOWER(src.uniquesourceid), LOWER('::')) - 1)) IN (LOWER('PB'),
       LOWER('COM'), LOWER('NFSB'))
     THEN NULL
     ELSE src.address_addresstype
     END AS address_type,
     CASE
     WHEN LENGTH(src.source_audit_create_timestamp) = 28
     THEN CAST(SUBSTR(source_audit_create_timestamp,25,4)||' '||REPLACE(REPLACE(REPLACE(SUBSTR(source_audit_create_timestamp,1,23),'UTC','+00:00'),'PDT','-07:00'),'PST','-08:00') AS TIMESTAMP)
     WHEN LENGTH(src.source_audit_create_timestamp) = 24
     THEN CAST(SUBSTR(REPLACE(source_audit_create_timestamp,'T',' '),1,19)||'+00:00' AS TIMESTAMP)
     ELSE NULL
     END AS source_audit_create_tmstp,
     CASE
     WHEN LENGTH(src.source_audit_update_timestamp) = 28
     THEN CAST(SUBSTR(source_audit_update_timestamp,25,4)||' '||REPLACE(REPLACE(REPLACE(SUBSTR(source_audit_update_timestamp,1,23),'UTC','+00:00'),'PDT','-07:00'),'PST','-08:00') AS TIMESTAMP)
     WHEN LENGTH(src.source_audit_update_timestamp) = 24
     THEN CAST(SUBSTR(REPLACE(source_audit_update_timestamp,'T',' '),1,19)||'+00:00' AS TIMESTAMP)
     ELSE NULL
     END AS source_audit_update_tmstp,
    src.addresses_line1_value AS line_1_token_value,
    src.addresses_line1_authority AS line_1_token_authority,
    src.addresses_line1_strategy AS line_1_token_strategy,
    src.addresses_line2_value AS line_2_token_value,
    src.addresses_line2_authority AS line_2_token_authority,
    src.addresses_line2_strategy AS line_2_token_strategy,
    src.addresses_city_value AS city_token_value,
    src.addresses_city_authority AS city_token_authority,
    src.addresses_city_strategy AS city_token_strategy,
    src.addresses_postalcode_value AS postal_code_token_value,
    src.addresses_postalcode_authority AS postal_code_token_authority,
    src.addresses_postalcode_strategy AS postal_code_token_strategy,
    src.addresses_attributedby AS attributed_by,
    src.event_eventtype AS event_type,
    CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS
    object_event_tmstp,
    src.addresses_is_verified_ind AS is_verified_ind
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_ldg{{params.tbl_sfx}} AS src
    INNER JOIN customer_obj_postal_address_max{{params.tbl_sfx}} AS m ON LOWER(src.uniquesourceid) = LOWER(m.unique_source_id) AND CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END = m
      .m_object_event_tmstp) AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.unique_source_id) = LOWER(dim.unique_source_id)
 WHERE ldg.object_event_tmstp > dim.object_event_tmstp
  OR dim.object_event_tmstp IS NULL);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_POSTAL_ADDRESS_WRK;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_merge_alias_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_merge_alias_wrk{{params.tbl_sfx}}
(SELECT ldg.unique_source_id,
  SUBSTR(ldg.unique_source_id, STRPOS(LOWER(ldg.unique_source_id), LOWER('::')) + 2) AS customer_id,
  ldg.alias_id,
  ldg.event_type,
  ldg.object_event_tmstp AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.object_event_tmstp AS STRING)) AS object_event_tmstp_tz,
  dim.dw_sys_load_tmstp
 FROM (SELECT uniquesourceid AS unique_source_id,
    merge_alias_id AS alias_id,
    event_eventtype AS event_type,
    CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS
    object_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_merge_alias_ldg{{params.tbl_sfx}}) AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_merge_alias_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.unique_source_id) = LOWER(dim.unique_source_id)
 WHERE ldg.object_event_tmstp > dim.object_event_tmstp
  OR dim.object_event_tmstp IS NULL
 QUALIFY (RANK() OVER (PARTITION BY ldg.unique_source_id ORDER BY ldg.object_event_tmstp DESC)) = 1);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_MERGE_ALIAS_WRK;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_obj_payment_method_max{{params.tbl_sfx}}
AS
SELECT uniquesourceid AS unique_source_id,
 MAX(CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END) AS
 m_object_event_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_payment_method_ldg{{params.tbl_sfx}}
GROUP BY unique_source_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS COLUMN(unique_source_id) ON CUSTOMER_OBJ_PAYMENT_METHOD_MAX;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_payment_method_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_payment_method_wrk{{params.tbl_sfx}}
(SELECT ldg.unique_source_id,
  ldg.customer_source,
  ldg.payment_method_id,
  ldg.payment_method_number_token_value,
  ldg.payment_method_number_token_authority,
  ldg.payment_method_number_token_strategy,
  ldg.payment_method_type,
  ldg.card_type,
  ldg.card_sub_type,
  ldg.card_holder_type,
  ldg.product_type,
  ldg.address_id,
  ldg.credit_account_id,
  ldg.credit_account_status,
  ldg.default_payment_method_ind,
  ldg.attributed_by,
  ldg.created_by,
  ldg.source_audit_create_tmstp AS source_audit_create_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.source_audit_create_tmstp AS STRING)) AS source_audit_create_tmstp_tz,
  ldg.source_audit_update_tmstp AS source_audit_update_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.source_audit_update_tmstp AS STRING)) AS source_audit_update_tmstp_tz,
  ldg.event_type,
  ldg.object_event_tmstp AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.object_event_tmstp AS STRING)) AS object_event_tmstp_tz,
  dim.dw_sys_load_tmstp,
  ldg.email_token_value,
  ldg.email_token_authority,
  ldg.email_token_strategy,
  ldg.caid,
  ldg.card_id
 FROM (SELECT src.uniquesourceid AS unique_source_id,
    SUBSTR(src.uniquesourceid, 1, STRPOS(LOWER(src.uniquesourceid), LOWER('::')) - 1) AS customer_source,
    src.paymentmethods_paymentmethodid AS payment_method_id,
    src.paymentmethods_paymentmethodnumber_value AS payment_method_number_token_value,
    src.paymentmethods_paymentmethodnumber_authority AS payment_method_number_token_authority,
    src.paymentmethods_paymentmethodnumber_strategy AS payment_method_number_token_strategy,
    src.paymentmethods_paymentmethodtype AS payment_method_type,
    src.paymentmethods_cardtype AS card_type,
    src.paymentmethods_cardsubtype AS card_sub_type,
    src.paymentmethods_cardholdertype AS card_holder_type,
    src.paymentmethods_producttype AS product_type,
    src.paymentmethods_addressid AS address_id,
    src.paymentmethods_creditaccountid AS credit_account_id,
    src.paymentmethods_creditaccountstatus AS credit_account_status,
    src.paymentmethods_isdefaultpaymentmethod AS default_payment_method_ind,
    src.paymentmethods_attributedby AS attributed_by,
    src.paymentmethods_createdby AS created_by,
     CASE
     WHEN LENGTH(src.source_audit_create_timestamp) = 28
     THEN CAST(SUBSTR(src.source_audit_create_timestamp,25,4)||' '||REPLACE(REPLACE(REPLACE(SUBSTR(src.source_audit_create_timestamp,1,23),'UTC','+00:00'),'PDT','-07:00'),'PST','-08:00') AS TIMESTAMP)
     WHEN LENGTH(src.source_audit_create_timestamp) = 24
     THEN CAST(SUBSTR(REPLACE(src.source_audit_create_timestamp, 'T',' '),1,19)||'+00:00' AS TIMESTAMP)
     ELSE NULL
     END AS source_audit_create_tmstp,
     CASE
     WHEN LENGTH(src.source_audit_update_timestamp) = 28
     THEN CAST(SUBSTR(source_audit_update_timestamp,25,4)||' '||REPLACE(REPLACE(REPLACE(SUBSTR(src.source_audit_update_timestamp,1,23),'UTC','+00:00'),'PDT','-07:00'),'PST','-08:00') AS TIMESTAMP)
     WHEN LENGTH(src.source_audit_update_timestamp) = 24
     THEN CAST(SUBSTR(REPLACE(src.source_audit_update_timestamp, 'T',' '),1,19)||'+00:00' AS TIMESTAMP)
     ELSE NULL
     END AS source_audit_update_tmstp,
    src.event_eventtype AS event_type,
    CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS
    object_event_tmstp,
    src.paymentmethods_email_token_value AS email_token_value,
    src.paymentmethods_email_token_authority AS email_token_authority,
    src.paymentmethods_email_token_strategy AS email_token_strategy,
    src.paymentmethods_caid AS caid,
    src.paymentmethods_cardid AS card_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_payment_method_ldg{{params.tbl_sfx}} AS src
    INNER JOIN customer_obj_payment_method_max{{params.tbl_sfx}} AS m ON LOWER(src.uniquesourceid) = LOWER(m.unique_source_id) AND CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END = m.m_object_event_tmstp) AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_payment_method_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.unique_source_id) = LOWER(dim.unique_source_id)
 WHERE ldg.object_event_tmstp > dim.object_event_tmstp
  OR dim.object_event_tmstp IS NULL);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_PAYMENT_METHOD_WRK;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_master_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_master_wrk{{params.tbl_sfx}}
(SELECT ldg.unique_source_id,
  ldg.customer_source,
  ldg.first_name_token_value,
  ldg.first_name_token_authority,
  ldg.first_name_token_strategy,
  ldg.last_name_token_value,
  ldg.last_name_token_authority,
  ldg.last_name_token_strategy,
  ldg.middle_name_token_value,
  ldg.middle_name_token_authority,
  ldg.middle_name_token_strategy,
  ldg.alternative_name_token_value,
  ldg.alternative_name_token_authority,
  ldg.alternative_name_token_strategy,
  ldg.event_type,
  ldg.event_source_system,
  ldg.object_event_tmstp AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.object_event_tmstp AS STRING)) AS object_event_tmstp_tz,
  dim.dw_sys_load_tmstp
 FROM (SELECT uniquesourceid AS unique_source_id,
    SUBSTR(uniquesourceid, 1, STRPOS(LOWER(uniquesourceid), LOWER('::')) - 1) AS customer_source,
    customeridentity_firstname_value AS first_name_token_value,
    customeridentity_firstname_authority AS first_name_token_authority,
    customeridentity_firstname_strategy AS first_name_token_strategy,
    customeridentity_lastname_value AS last_name_token_value,
    customeridentity_lastname_authority AS last_name_token_authority,
    customeridentity_lastname_strategy AS last_name_token_strategy,
    customeridentity_middlename_value AS middle_name_token_value,
    customeridentity_middlename_authority AS middle_name_token_authority,
    customeridentity_middlename_strategy AS middle_name_token_strategy,
    customeridentity_alternativename_value AS alternative_name_token_value,
    customeridentity_alternativename_authority AS alternative_name_token_authority,
    customeridentity_alternativename_strategy AS alternative_name_token_strategy,
    event_eventtype AS event_type,
    event_sourcesystem AS event_source_system,
    CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS
    object_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_master_ldg{{params.tbl_sfx}}) AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_master_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.unique_source_id) = LOWER(dim.unique_source_id)
 WHERE ldg.object_event_tmstp > dim.object_event_tmstp
  OR dim.object_event_tmstp IS NULL
 QUALIFY (RANK() OVER (PARTITION BY ldg.unique_source_id ORDER BY ldg.object_event_tmstp DESC)) = 1);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_MASTER_WRK;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_obj_credit_account_max{{params.tbl_sfx}}
AS
SELECT uniquesourceid AS unique_source_id,
 MAX(CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END) AS
 m_object_event_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_credit_account_ldg{{params.tbl_sfx}}
GROUP BY unique_source_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATS COLUMN(unique_source_id) ON CUSTOMER_OBJ_CREDIT_ACCOUNT_MAX;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_credit_account_wrk{{params.tbl_sfx}};
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_credit_account_wrk{{params.tbl_sfx}}
(SELECT ldg.unique_source_id,
  ldg.customer_source,
  ldg.caid,
  ldg.open_date AS open_date,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.open_date AS STRING)) AS open_date_tz,
  ldg.closed_date AS closed_date,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.closed_date AS STRING)) AS closed_date_tz,
  ldg.status,
  ldg.event_type,
  ldg.object_event_tmstp AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.object_event_tmstp AS STRING)) AS object_event_tmstp_tz,
  dim.dw_sys_load_tmstp
 FROM (SELECT src.uniquesourceid AS unique_source_id,
    SUBSTR(src.uniquesourceid, 1, STRPOS(LOWER(src.uniquesourceid), LOWER('::')) - 1) AS customer_source,
    src.creditaccounts_caid AS caid,
     CASE
     WHEN LOWER(src.creditaccounts_opendate) LIKE LOWER('0000%')
     THEN NULL
     WHEN src.creditaccounts_opendate IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(src.creditaccounts_opendate AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(src.creditaccounts_opendate AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(src.creditaccounts_opendate AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(src.creditaccounts_opendate AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(src.creditaccounts_opendate AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(src.creditaccounts_opendate AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(src.creditaccounts_opendate, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS open_date,
     CASE
     WHEN LOWER(src.creditaccounts_closeddate) LIKE LOWER('0000%')
     THEN NULL
     WHEN src.creditaccounts_closeddate IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(src.creditaccounts_closeddate AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(src.creditaccounts_closeddate AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(src.creditaccounts_closeddate AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(src.creditaccounts_closeddate AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(src.creditaccounts_closeddate AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(src.creditaccounts_closeddate AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(src.creditaccounts_closeddate, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS closed_date,
    src.creditaccounts_status AS status,
    src.event_eventtype AS event_type,
    CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS
    object_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_credit_account_ldg{{params.tbl_sfx}} AS src
    INNER JOIN customer_obj_credit_account_max{{params.tbl_sfx}} AS m ON LOWER(src.uniquesourceid) = LOWER(m.unique_source_id) AND CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END = m
      .m_object_event_tmstp) AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_credit_account_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.unique_source_id) = LOWER(dim.unique_source_id)
 WHERE ldg.object_event_tmstp > dim.object_event_tmstp
  OR dim.object_event_tmstp IS NULL);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_CREDIT_ACCOUNT_WRK;
BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_obj_linked_program_max{{params.tbl_sfx}}
AS
SELECT uniquesourceid AS unique_source_id,
 MAX(CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000) AS INT64))AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END) AS
 m_object_event_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_linked_program_ldg{{params.tbl_sfx}}
GROUP BY unique_source_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--COLLECT STATS COLUMN(unique_source_id) ON CUSTOMER_OBJ_LINKED_PROGRAM_MAX;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_linked_program_wrk{{params.tbl_sfx}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.customer_obj_linked_program_wrk{{params.tbl_sfx}}
(SELECT ldg.unique_source_id,
  ldg.customer_source,
  ldg.link_id,
  ldg.link_type,
  ldg.link_type_id,
  ldg.link_data_mobile_token_value,
  ldg.link_data_mobile_token_authority,
  ldg.link_data_mobile_token_strategy,
  ldg.event_type,
  ldg.object_event_tmstp AS object_event_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(ldg.object_event_tmstp AS STRING)) AS object_event_tmstp_tz,
  dim.dw_sys_load_tmstp
 FROM (SELECT src.uniquesourceid AS unique_source_id,
    SUBSTR(src.uniquesourceid, 1, STRPOS(LOWER(src.uniquesourceid), LOWER('::')) - 1) AS customer_source,
    src.linkedprograms_link_id AS link_id,
    src.linkedprograms_link_type AS link_type,
    src.linkedprograms_link_type_id AS link_type_id,
    src.linkedprograms_link_data_mobile_token_value AS link_data_mobile_token_value,
    src.linkedprograms_link_data_mobile_token_authority AS link_data_mobile_token_authority,
    src.linkedprograms_link_data_mobile_token_strategy AS link_data_mobile_token_strategy,
    src.event_eventtype AS event_type,
    CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END AS
    object_event_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_linked_program_ldg{{params.tbl_sfx}} AS src
    INNER JOIN customer_obj_linked_program_max{{params.tbl_sfx}} AS m ON LOWER(src.uniquesourceid) = LOWER(m.unique_source_id) AND CASE WHEN event_eventtimestamp IS NOT NULL
    THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(RPAD(CAST(DATETIME_ADD(CAST(CAST(TIMESTAMP_SECONDS(CAST(TRUNC(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64) / 1000)  AS INT64)) AS STRING) AS TIMESTAMP), INTERVAL MOD(CAST((EXTRACT(YEAR FROM CAST(event_eventtimestamp AS TIMESTAMP)) - 1900) * 10000 + (EXTRACT(month FROM CAST(event_eventtimestamp AS TIMESTAMP)) * 100) + EXTRACT(day FROM CAST(event_eventtimestamp AS TIMESTAMP)) AS INT64), 1000) * CAST(0.001 AS INT64) SECOND) AS STRING), 26, ' ') || '+00:00' AS DATETIME)) AS TIMESTAMP)
    ELSE CAST(REPLACE(REPLACE(event_eventtimestamp, 'T',' '),'Z','+00:00') AS TIMESTAMP) END = m
      .m_object_event_tmstp) AS ldg
  LEFT JOIN (SELECT unique_source_id,
    MAX(object_event_tmstp_utc) AS object_event_tmstp,
    MIN(dw_sys_load_tmstp) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_linked_program_dim{{params.tbl_sfx}}
   GROUP BY unique_source_id) AS dim ON LOWER(ldg.unique_source_id) = LOWER(dim.unique_source_id)
 WHERE ldg.object_event_tmstp > dim.object_event_tmstp
  OR dim.object_event_tmstp IS NULL);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATS ON PRD_NAP_STG.CUSTOMER_OBJ_LINKED_PROGRAM_WRK;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;

