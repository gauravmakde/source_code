BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING; 
/* Loads ANY resets that have been updated AS well ANY NEW resets
FROM the LDG TO the FACT TABLE based
ON reset_id */ 
-- SET QUERY_BAND = 'App_ID=app08001;DAG_ID=clearance_reset_10976_tech_nap_merch;Task_Name=job_3_ldg_to_fact;' -- FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE = 0;
MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.clearance_markdown_reset_fact AS tgt
USING(
  SELECT
    ldg.reset_id,
    NULLIF(ldg.rms_sku_num, '') AS rms_sku_num,
    NULLIF(ldg.rms_style_num, '') AS rms_style_num,
    NULLIF(ldg.color_num, '') AS color_num,
    ldg.channel_country,
    ldg.channel_brand,
    ldg.selling_channel,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(ldg.effective_tmstp || '+00:00' AS DATETIME)) AS TIMESTAMP) AS effective_tmstp,
    '+00:00' AS effective_tmstp_tz,
    ldg.user_id,
    ldg.user_type,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(ldg.last_updated_tmstp || '+00:00' AS DATETIME)) AS TIMESTAMP) AS last_updated_tmstp,
    '+00:00' AS last_updated_tmstp_tz
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.clearance_markdown_reset_ldg AS ldg
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_fct.clearance_markdown_reset_fact AS fct
  ON LOWER(ldg.reset_id) = LOWER(fct.reset_id)
  WHERE CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(ldg.last_updated_tmstp || '+00:00' AS DATETIME)) AS DATETIME) > CAST(fct.last_updated_tmstp AS DATETIME) OR fct.reset_id IS NULL
  QUALIFY(ROW_NUMBER() OVER (PARTITION BY ldg.reset_id ORDER BY ldg.last_updated_tmstp DESC)) = 1) AS src
ON LOWER(src.reset_id) = LOWER(tgt.reset_id)
  WHEN MATCHED THEN UPDATE SET rms_sku_num = src.rms_sku_num, rms_style_num = src.rms_style_num, color_num = src.color_num, channel_country = src.channel_country, channel_brand = src.channel_brand, selling_channel = src.selling_channel, effective_tmstp = src.effective_tmstp, effective_tmstp_tz = src.effective_tmstp_tz, user_id = src.user_id, user_type = src.user_type, last_updated_tmstp = src.last_updated_tmstp, last_updated_tmstp_tz = src.last_updated_tmstp_tz, dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
  WHEN NOT MATCHED THEN INSERT VALUES(
src.reset_id, 
src.rms_sku_num, 
src.rms_style_num, 
src.color_num, 
src.channel_country, 
src.channel_brand, 
src.selling_channel, 
src.effective_tmstp,
src.effective_tmstp_tz, 
src.user_id, 
src.user_type, 
src.last_updated_tmstp, 
src.last_updated_tmstp_tz,
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));
   EXCEPTION WHEN ERROR THEN 
   SET _ERROR_CODE = 1; 
   SET _ERROR_MESSAGE = @@error.message;
END
  ; /*SET QUERY_BAND = NONE FOR SESSION;*/
END
  ;