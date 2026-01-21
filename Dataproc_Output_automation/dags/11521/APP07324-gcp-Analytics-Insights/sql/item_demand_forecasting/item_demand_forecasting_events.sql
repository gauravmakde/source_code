
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = 'App_ID=APP08073;
DAG_ID=item_demand_forecasting_dataprep_11521_ACE_ENG;
---     Task_Name=item_demand_forecasting_events;'*/
BEGIN
SET _ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_events
WHERE week_start_date >= {{params.start_date}} AND week_start_date <= {{params.end_date}};

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.ip_forecast_t2_schema}}.idf_events
(
channel_brand,
selling_channel,
week_num,
week_start_date,
regular_event_name,
loyalty_event,
last_updated_utc,
dw_sys_load_tmstp
)
WITH wk AS (SELECT DISTINCT week_idnt AS week_num,
 CAST(day_date AS DATE) AS day_date,
 CAST(week_start_day_date AS DATE) AS week_start_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
WHERE CAST(week_start_day_date AS DATE) BETWEEN {{params.start_date}} AND {{params.end_date}})
,event AS (((SELECT 'NORDSTROM_RACK' AS channel_brand,
   'ONLINE' AS selling_channel,
   wk.week_num,
   wk.week_start_date,
   SUBSTR(MAX(CASE
      WHEN LOWER(events.rcom_event_name) = LOWER('null')
      THEN NULL
      ELSE events.rcom_event_name
      END), 1, 100) AS regular_event_name,
   SUBSTR(MAX(CASE
      WHEN LOWER(events.rcom_loyalty_event) = LOWER('0')
      THEN NULL
      ELSE events.rcom_loyalty_event
      END), 1, 100) AS loyalty_event
  FROM `{{params.gcp_project_id}}`.t2dl_das_store_sales_forecasting.loyalty_events_sales_shipping AS events
   INNER JOIN wk ON events.event_date = wk.day_date
  GROUP BY channel_brand,
   selling_channel,
   wk.week_num,
   wk.week_start_date
  UNION DISTINCT
  SELECT 'NORDSTROM_RACK' AS channel_brand,
   'STORE' AS selling_channel,
   wk0.week_num,
   wk0.week_start_date,
   SUBSTR(MAX(CASE
      WHEN LOWER(events0.rs_event_name) = LOWER('null')
      THEN NULL
      ELSE events0.rs_event_name
      END), 1, 100) AS regular_event_name,
   SUBSTR(MAX(CASE
      WHEN LOWER(events0.rs_loyalty_event) = LOWER('0')
      THEN NULL
      ELSE events0.rs_loyalty_event
      END), 1, 100) AS loyalty_event
  FROM `{{params.gcp_project_id}}`.t2dl_das_store_sales_forecasting.loyalty_events_sales_shipping AS events0
   INNER JOIN wk AS wk0 ON events0.event_date = wk0.day_date
  GROUP BY channel_brand,
   selling_channel,
   wk0.week_num,
   wk0.week_start_date)
 UNION DISTINCT
 SELECT 'NORDSTROM' AS channel_brand,
  'ONLINE' AS selling_channel,
  wk1.week_num,
  wk1.week_start_date,
  SUBSTR(MAX(CASE
     WHEN LOWER(events1.ncom_event_name) = LOWER('null')
     THEN NULL
     ELSE events1.ncom_event_name
     END), 1, 100) AS regular_event_name,
  SUBSTR(MAX(CASE
     WHEN LOWER(events1.ncom_loyalty_event) = LOWER('0')
     THEN NULL
     ELSE events1.ncom_loyalty_event
     END), 1, 100) AS loyalty_event
 FROM `{{params.gcp_project_id}}`.t2dl_das_store_sales_forecasting.loyalty_events_sales_shipping AS events1
  INNER JOIN wk AS wk1 ON events1.event_date = wk1.day_date
 GROUP BY channel_brand,
  selling_channel,
  wk1.week_num,
  wk1.week_start_date)
UNION DISTINCT
SELECT 'NORDSTROM' AS channel_brand,
 'STORE' AS selling_channel,
 wk2.week_num,
 wk2.week_start_date,
 SUBSTR(MAX(CASE
    WHEN LOWER(events2.fls_event_name) = LOWER('null')
    THEN NULL
    ELSE events2.fls_event_name
    END), 1, 100) AS regular_event_name,
 SUBSTR(MAX(CASE
    WHEN LOWER(events2.fls_loyalty_event) = LOWER('0')
    THEN NULL
    ELSE events2.fls_loyalty_event
    END), 1, 100) AS loyalty_event
FROM `{{params.gcp_project_id}}`.t2dl_das_store_sales_forecasting.loyalty_events_sales_shipping AS events2
 INNER JOIN wk AS wk2 ON events2.event_date = wk2.day_date
GROUP BY channel_brand,
 selling_channel,
 wk2.week_num,
 wk2.week_start_date)
SELECT channel_brand,
 selling_channel,
 week_num,
 week_start_date,
 SUBSTR(regular_event_name, 1, 100) AS regular_event_name,
 SUBSTR(loyalty_event, 1, 100) AS loyalty_event,
 CAST(TIMESTAMP(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('GMT')) AS DATETIME)) AS DATE) AS last_updated_utc,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM event;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


END;
