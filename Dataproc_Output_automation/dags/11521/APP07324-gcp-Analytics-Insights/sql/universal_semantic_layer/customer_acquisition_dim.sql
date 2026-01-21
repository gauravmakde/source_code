BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP08818;
DAG_ID=customer_acquisition_dim_11521_ACE_ENG;
---     Task_Name=customer_acquisition_dim;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.customer_acquisition_dim;
EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.customer_acquisition_dim
(SELECT cus.acp_id,
  cus.acquired_date AS customer_acquired_date,
  EXTRACT(YEAR FROM cus.acquired_date) AS customer_acquired_year,
  EXTRACT(MONTH FROM cus.acquired_date) AS customer_acquired_month,
  cus.acquired_box AS customer_acquired_box,
  CAST(TRUNC(DATE_DIFF(CURRENT_DATE('PST8PDT'), cus.acquired_date, DAY) / 365) AS INT64) AS customer_life_years,
  CAST(TRUNC(DATE_DIFF(CURRENT_DATE('PST8PDT'), cus.acquired_date, DAY) / 30) AS INT64) AS customer_life_months,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.t2dl_das_cal.customer_attributes_journey AS cus
  INNER JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.acp_driver_dim AS ac ON LOWER(ac.acp_id) = LOWER(cus.acp_id));


EXCEPTION WHEN ERROR THEN
SET ERROR_CODE  =  1;
SET ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id) ,COLUMN (customer_acquired_date) on t2dl_das_usl.customer_acquisition_dim;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
