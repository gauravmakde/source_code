
CREATE TEMPORARY TABLE IF NOT EXISTS credit_support_contacted_ldg_temp
AS
SELECT *
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_stg.credit_support_contacted_ldg
QUALIFY (ROW_NUMBER() OVER (PARTITION BY credit_customer_support_contacted_id ORDER BY event_time DESC)) = 1;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_fct.credit_support_contacted_fact
WHERE credit_customer_support_contacted_id IN (SELECT credit_customer_support_contacted_id
        FROM credit_support_contacted_ldg_temp);

		
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_fct.credit_support_contacted_fact (event_time,event_time_tz, event_date, channel_country, channel_brand,
 selling_channel, credit_account_id, credit_customer_support_contacted_id, call_info_processor_id,
 call_account_info_processor_id, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp, dw_sys_updt_tmstp_tz)
(SELECT CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(CONCAT(event_time, '+00:00') AS DATETIME)) AS DATETIME) AS TIMESTAMP) AS event_time,
 '+00:00' as event_time_tz,
CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(event_time AS DATETIME)) AS DATETIME) AS DATE) AS event_date,
  channel_country,
  channel_brand,
  selling_channel,
  credit_account_id, 
  credit_customer_support_contacted_id,
  call_info_processor_id,
  call_account_info_processor_id,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_updt_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as  dw_sys_updt_tmstp_tz
 FROM credit_support_contacted_ldg_temp);

 
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_credit_stg.credit_support_contacted_ldg;