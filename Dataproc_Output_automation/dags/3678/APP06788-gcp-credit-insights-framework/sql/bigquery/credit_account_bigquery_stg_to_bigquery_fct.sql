BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
---FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS credit_account_ldg_temp
AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_stg.credit_account_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY credit_account_id ORDER BY opened_time DESC)) = 1;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS credit_account_bank_card_ldg_temp
AS
SELECT DISTINCT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_stg.credit_account_bank_card_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY added_date DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;

SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_fct.credit_account_fact
WHERE credit_account_id IN (SELECT credit_account_id
        FROM credit_account_ldg_temp);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_fct.credit_account_fact 
(dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz, opened_time,opened_time_tz, closed_date,
 opened_channel_country, opened_channel_brand, selling_channel, credit_account_id, credit_application_id, status,
 metadata_created_time,metadata_created_time_tz, metadata_last_updated_time, metadata_last_updated_time_tz,metadata_last_triggering_event_name, application_received_time,application_received_time_tz,
 source_code)
(SELECT CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() AS dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS
  dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`() AS dw_sys_updt_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(CONCAT(opened_time, '+00:00') AS DATETIME)) AS DATETIME) AS
   TIMESTAMP) AS opened_time,
   `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(CAST(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(CONCAT(opened_time, '+00:00') AS DATETIME)) AS DATETIME) AS
   TIMESTAMP)AS STRING) )  AS opened_time_tz,
  closed_date,
  opened_channel_country,
  opened_channel_brand,
  selling_channel,
  credit_account_id,
  credit_application_id,
  status,
  CAST(metadata_created_time AS TIMESTAMP) AS metadata_created_time,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(metadata_created_time) AS metadata_created_time_tz,
  CAST(metadata_last_updated_time AS TIMESTAMP) AS metadata_last_updated_time,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(metadata_last_updated_time) AS metadata_last_updated_time_tz,
  metadata_last_triggering_event_name,
  CAST(application_received_time AS TIMESTAMP) AS application_received_time,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(application_received_time) AS application_received_time_tz,
  source_code
 FROM credit_account_ldg_temp);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_fct.credit_account_bank_card_fact
WHERE card_id IN (SELECT card_id
        FROM credit_account_bank_card_ldg_temp);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_fct.credit_account_bank_card_fact (dw_sys_load_tmstp, dw_sys_updt_tmstp, credit_account_id,
 card_id, added_date, closed_date, card_member_role, card_expiration_month, card_expiration_year, card_payment_id,
 card_status, name_on_card_value, name_on_card_authority, name_on_card_strategy, name_on_card_data_classification,
 replaced_card_id, activated_date, tender_service_id, tender_service_product_type, city, state, coarse_postal_code,
 card_added_reason, card_closed_reason)
(SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
  credit_account_id,
  card_id,
  added_date,
  closed_date,
  card_member_role,
  card_expiration_month,
  card_expiration_year,
  card_payment_id,
  card_status,
  name_on_card_value,
  name_on_card_authority,
  name_on_card_strategy,
  name_on_card_data_classification,
  replaced_card_id,
  activated_date,
  tender_service_id,
  tender_service_product_type,
  city,
  state,
  coarse_postal_code,
  card_added_reason,
  card_closed_reason
 FROM credit_account_bank_card_ldg_temp);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_stg.credit_account_ldg;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_stg.credit_account_bank_card_ldg;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
