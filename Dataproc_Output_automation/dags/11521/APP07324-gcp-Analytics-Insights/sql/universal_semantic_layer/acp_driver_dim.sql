
CREATE TEMPORARY TABLE IF NOT EXISTS acp_driver
AS
SELECT DISTINCT acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS hdr
WHERE business_day_date >= DATE '2021-01-31';

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.acp_driver_dim;

INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.acp_driver_dim
(SELECT acp_id,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM acp_driver);
