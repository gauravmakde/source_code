
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

-- mandatory Query Band part

-- SET QUERY_BAND = 'App_ID=app06788; DAG_ID=credit_survey_requested_teradata; Task_Name=teradata_stg_to_teradata_fct_job;' -- noqa
-- FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS credit_survey_requested_ldg_temp
AS
SELECT *
FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_credit_stg.credit_survey_requested_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY credit_customer_support_contacted_id ORDER BY event_time DESC)) = 1;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- Delete duplicates
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_credit_fct.credit_survey_requested_fact
WHERE credit_customer_support_contacted_id IN (SELECT credit_customer_support_contacted_id
        FROM credit_survey_requested_ldg_temp);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_credit_fct.credit_survey_requested_fact (event_time, event_time_tz, event_date, channel_country, channel_brand,
 selling_channel, credit_account_id, credit_customer_support_contacted_id, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CONCAT(event_time, '+00:00')) AS DATETIME) AS event_time,
`{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CONCAT(event_time, '+00:00')) AS DATETIME)) as event_time_tz,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(event_time AS DATETIME)) AS DATETIME) AS event_date,
  channel_country,
  channel_brand,
  selling_channel,
  credit_account_id,
  credit_customer_support_contacted_id,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS 
  dw_sys_load_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)) as dw_sys_load_tmstp_tz,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)) as dw_sys_updt_tmstp_tz
 FROM credit_survey_requested_ldg_temp);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- delete records from stg table
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_nap_credit_stg.credit_survey_requested_ldg;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- noqa
END;
