
/*SET QUERY_BAND = 'App_ID=app08818;
DAG_ID=trips_sandbox_weekly_cust_11521_ACE_ENG;
---     Task_Name=trips_sandbox_cust_single_attribute;'*/
---     FOR SESSION VOLATILE;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_lookup
AS
SELECT DISTINCT acp_id,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 region,
 dma,
 aec,
 predicted_segment,
 loyalty_level,
 loyalty_type,
 new_to_jwn,
 ROW_NUMBER() OVER (PARTITION BY acp_id, week_num_realigned ORDER BY week_num_realigned DESC) AS week_order,
 ROW_NUMBER() OVER (PARTITION BY acp_id, month_num_realigned ORDER BY week_num_realigned DESC) AS month_week_order,
 ROW_NUMBER() OVER (PARTITION BY acp_id, quarter_num_realigned ORDER BY week_num_realigned DESC) AS quarter_week_order,
 ROW_NUMBER() OVER (PARTITION BY acp_id, year_num_realigned ORDER BY week_num_realigned DESC) AS year_week_order
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_weekly_cust AS tswc;

CREATE TEMPORARY TABLE IF NOT EXISTS all_times
AS
SELECT 'WEEK    ' AS time_granularity,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 acp_id,
 MAX(CASE
   WHEN week_order = 1
   THEN region
   ELSE NULL
   END) AS region,
 MAX(CASE
   WHEN week_order = 1
   THEN dma
   ELSE NULL
   END) AS dma,
 MAX(CASE
   WHEN week_order = 1
   THEN aec
   ELSE NULL
   END) AS aec,
 MAX(CASE
   WHEN week_order = 1
   THEN predicted_segment
   ELSE NULL
   END) AS predicted_segment,
 MAX(CASE
   WHEN week_order = 1
   THEN loyalty_level
   ELSE NULL
   END) AS loyalty_level,
 MAX(CASE
   WHEN week_order = 1
   THEN loyalty_type
   ELSE NULL
   END) AS loyalty_type,
 MAX(new_to_jwn) AS new_to_jwn,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM customer_lookup AS cl
WHERE week_num_realigned >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
 AND week_num_realigned <= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = CURRENT_DATE('PST8PDT'))
GROUP BY time_granularity,
 week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 acp_id;

 
INSERT INTO all_times
(SELECT 'WEEK    ' AS time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id,
  MAX(CASE
    WHEN week_order = 1
    THEN region
    ELSE NULL
    END) AS region,
  MAX(CASE
    WHEN week_order = 1
    THEN dma
    ELSE NULL
    END) AS dma,
  MAX(CASE
    WHEN week_order = 1
    THEN aec
    ELSE NULL
    END) AS aec,
  MAX(CASE
    WHEN week_order = 1
    THEN predicted_segment
    ELSE NULL
    END) AS predicted_segment,
  MAX(CASE
    WHEN week_order = 1
    THEN loyalty_level
    ELSE NULL
    END) AS loyalty_level,
  MAX(CASE
    WHEN week_order = 1
    THEN loyalty_type
    ELSE NULL
    END) AS loyalty_type,
  MAX(new_to_jwn) AS new_to_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
 WHERE week_num_realigned >= (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
  AND week_num_realigned < (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
 GROUP BY time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id);

INSERT INTO all_times
(SELECT 'WEEK    ' AS time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id,
  MAX(CASE
    WHEN week_order = 1
    THEN region
    ELSE NULL
    END) AS region,
  MAX(CASE
    WHEN week_order = 1
    THEN dma
    ELSE NULL
    END) AS dma,
  MAX(CASE
    WHEN week_order = 1
    THEN aec
    ELSE NULL
    END) AS aec,
  MAX(CASE
    WHEN week_order = 1
    THEN predicted_segment
    ELSE NULL
    END) AS predicted_segment,
  MAX(CASE
    WHEN week_order = 1
    THEN loyalty_level
    ELSE NULL
    END) AS loyalty_level,
  MAX(CASE
    WHEN week_order = 1
    THEN loyalty_type
    ELSE NULL
    END) AS loyalty_type,
  MAX(new_to_jwn) AS new_to_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
 WHERE week_num_realigned >= (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH))
  AND week_num_realigned < (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
 GROUP BY time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id);

INSERT INTO all_times
(SELECT 'MONTH' AS time_granularity,
  0 AS week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id,
  MAX(CASE
    WHEN month_week_order = 1
    THEN region
    ELSE NULL
    END) AS region,
  MAX(CASE
    WHEN month_week_order = 1
    THEN dma
    ELSE NULL
    END) AS dma,
  MAX(CASE
    WHEN month_week_order = 1
    THEN aec
    ELSE NULL
    END) AS aec,
  MAX(CASE
    WHEN month_week_order = 1
    THEN predicted_segment
    ELSE NULL
    END) AS predicted_segment,
  MAX(CASE
    WHEN month_week_order = 1
    THEN loyalty_level
    ELSE NULL
    END) AS loyalty_level,
  MAX(CASE
    WHEN month_week_order = 1
    THEN loyalty_type
    ELSE NULL
    END) AS loyalty_type,
  MAX(new_to_jwn) AS new_to_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
 WHERE month_num_realigned >= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
  AND month_num_realigned <= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = CURRENT_DATE('PST8PDT'))
 GROUP BY time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id);

INSERT INTO all_times
(SELECT 'MONTH' AS time_granularity,
  0 AS week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id,
  MAX(CASE
    WHEN month_week_order = 1
    THEN region
    ELSE NULL
    END) AS region,
  MAX(CASE
    WHEN month_week_order = 1
    THEN dma
    ELSE NULL
    END) AS dma,
  MAX(CASE
    WHEN month_week_order = 1
    THEN aec
    ELSE NULL
    END) AS aec,
  MAX(CASE
    WHEN month_week_order = 1
    THEN predicted_segment
    ELSE NULL
    END) AS predicted_segment,
  MAX(CASE
    WHEN month_week_order = 1
    THEN loyalty_level
    ELSE NULL
    END) AS loyalty_level,
  MAX(CASE
    WHEN month_week_order = 1
    THEN loyalty_type
    ELSE NULL
    END) AS loyalty_type,
  MAX(new_to_jwn) AS new_to_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
 WHERE month_num_realigned >= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
  AND month_num_realigned < (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
 GROUP BY time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id);

INSERT INTO all_times
(SELECT 'MONTH' AS time_granularity,
  0 AS week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id,
  MAX(CASE
    WHEN month_week_order = 1
    THEN region
    ELSE NULL
    END) AS region,
  MAX(CASE
    WHEN month_week_order = 1
    THEN dma
    ELSE NULL
    END) AS dma,
  MAX(CASE
    WHEN month_week_order = 1
    THEN aec
    ELSE NULL
    END) AS aec,
  MAX(CASE
    WHEN month_week_order = 1
    THEN predicted_segment
    ELSE NULL
    END) AS predicted_segment,
  MAX(CASE
    WHEN month_week_order = 1
    THEN loyalty_level
    ELSE NULL
    END) AS loyalty_level,
  MAX(CASE
    WHEN month_week_order = 1
    THEN loyalty_type
    ELSE NULL
    END) AS loyalty_type,
  MAX(new_to_jwn) AS new_to_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
 WHERE month_num_realigned >= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH))
  AND month_num_realigned < (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
 GROUP BY time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id);

INSERT INTO all_times
(SELECT time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id,
  region,
  dma,
  aec,
  predicted_segment,
  loyalty_level,
  loyalty_type,
  new_to_jwn,
  dw_sys_load_tmstp
 FROM (SELECT 'QUARTER' AS time_granularity,
     0 AS week_num_realigned,
     0 AS month_num_realigned,
     quarter_num_realigned,
     year_num_realigned,
     acp_id,
     MAX(CASE
       WHEN month_week_order = 1
       THEN region
       ELSE NULL
       END) AS region,
     MAX(CASE
       WHEN month_week_order = 1
       THEN dma
       ELSE NULL
       END) AS dma,
     MAX(CASE
       WHEN month_week_order = 1
       THEN aec
       ELSE NULL
       END) AS aec,
     MAX(CASE
       WHEN month_week_order = 1
       THEN predicted_segment
       ELSE NULL
       END) AS predicted_segment,
     MAX(CASE
       WHEN month_week_order = 1
       THEN loyalty_level
       ELSE NULL
       END) AS loyalty_level,
     MAX(CASE
       WHEN month_week_order = 1
       THEN loyalty_type
       ELSE NULL
       END) AS loyalty_type,
     MAX(new_to_jwn) AS new_to_jwn,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM customer_lookup AS cl
    GROUP BY time_granularity,
     week_num_realigned,
     quarter_num_realigned,
     year_num_realigned,
     acp_id,
     month_num_realigned
    UNION DISTINCT
    SELECT 'YEAR' AS time_granularity,
     0 AS week_num_realigned,
     0 AS month_num_realigned,
     0 AS quarter_num_realigned,
     year_num_realigned,
     acp_id,
     MAX(CASE
       WHEN month_week_order = 1
       THEN region
       ELSE NULL
       END) AS region,
     MAX(CASE
       WHEN month_week_order = 1
       THEN dma
       ELSE NULL
       END) AS dma,
     MAX(CASE
       WHEN month_week_order = 1
       THEN aec
       ELSE NULL
       END) AS aec,
     MAX(CASE
       WHEN month_week_order = 1
       THEN predicted_segment
       ELSE NULL
       END) AS predicted_segment,
     MAX(CASE
       WHEN month_week_order = 1
       THEN loyalty_level
       ELSE NULL
       END) AS loyalty_level,
     MAX(CASE
       WHEN month_week_order = 1
       THEN loyalty_type
       ELSE NULL
       END) AS loyalty_type,
     MAX(new_to_jwn) AS new_to_jwn,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM customer_lookup AS cl
    GROUP BY time_granularity,
     week_num_realigned,
     quarter_num_realigned,
     year_num_realigned,
     acp_id,
     month_num_realigned) AS t5);

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute;

INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute
(SELECT TRIM(time_granularity) AS time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  acp_id,
  COALESCE(region, 'Z_UNKNOWN') AS region,
  COALESCE(dma, 'Z_UNKNOWN') AS dma,
  COALESCE(aec, 'UNDEFINED') AS aec,
  COALESCE(predicted_segment, 'Z_UNKNOWN') AS predicted_segment,
  COALESCE(loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  COALESCE(new_to_jwn, 0) AS new_to_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM all_times);

