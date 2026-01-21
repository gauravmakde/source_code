BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=app08818;
DAG_ID=trips_sandbox_weekly_cust_11521_ACE_ENG;
---     Task_Name=trips_sandbox_cust_count;'*/
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_lookup
AS
SELECT week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 acp_id,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
   THEN 1
   ELSE 0
   END) AS fls_shopper,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
   THEN 1
   ELSE 0
   END) AS ncom_shopper,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('3) Rack Stores')
   THEN 1
   ELSE 0
   END) AS rs_shopper,
 MAX(CASE
   WHEN LOWER(channel) = LOWER('4) Rack.com')
   THEN 1
   ELSE 0
   END) AS rcom_shopper,
 MAX(CASE
   WHEN LOWER(channel) IN (LOWER('1) Nordstrom Stores'), LOWER('3) Rack Stores'))
   THEN 1
   ELSE 0
   END) AS store_shopper,
 MAX(CASE
   WHEN LOWER(channel) IN (LOWER('2) Nordstrom.com'), LOWER('4) Rack.com'))
   THEN 1
   ELSE 0
   END) AS digital_shopper,
 MAX(CASE
   WHEN LOWER(banner) = LOWER('1) Nordstrom Banner')
   THEN 1
   ELSE 0
   END) AS nord_shopper,
 MAX(CASE
   WHEN LOWER(banner) = LOWER('2) Rack Banner')
   THEN 1
   ELSE 0
   END) AS rack_shopper
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_weekly_cust AS tswc
GROUP BY week_num_realigned,
 month_num_realigned,
 quarter_num_realigned,
 year_num_realigned,
 acp_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (acp_id, week_num_realigned) ON customer_lookup;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS all_times
AS
SELECT 'WEEK    ' AS time_granularity,
 cl.week_num_realigned,
 cl.month_num_realigned,
 cl.quarter_num_realigned,
 cl.year_num_realigned,
 tsca.region,
 tsca.dma,
 tsca.aec,
 tsca.predicted_segment,
 tsca.loyalty_level,
 tsca.loyalty_type,
 tsca.new_to_jwn,
 COUNT(DISTINCT CASE
   WHEN cl.fls_shopper = 1
   THEN cl.acp_id
   ELSE NULL
   END) AS cust_count_fls,
 COUNT(DISTINCT CASE
   WHEN cl.ncom_shopper = 1
   THEN cl.acp_id
   ELSE NULL
   END) AS cust_count_ncom,
 COUNT(DISTINCT CASE
   WHEN cl.rs_shopper = 1
   THEN cl.acp_id
   ELSE NULL
   END) AS cust_count_rs,
 COUNT(DISTINCT CASE
   WHEN cl.rcom_shopper = 1
   THEN cl.acp_id
   ELSE NULL
   END) AS cust_count_rcom,
 COUNT(DISTINCT CASE
   WHEN cl.store_shopper = 1
   THEN cl.acp_id
   ELSE NULL
   END) AS cust_count_stores,
 COUNT(DISTINCT CASE
   WHEN cl.digital_shopper = 1
   THEN cl.acp_id
   ELSE NULL
   END) AS cust_count_digital,
 COUNT(DISTINCT CASE
   WHEN cl.nord_shopper = 1
   THEN cl.acp_id
   ELSE NULL
   END) AS cust_count_nord,
 COUNT(DISTINCT CASE
   WHEN cl.rack_shopper = 1
   THEN cl.acp_id
   ELSE NULL
   END) AS cust_count_rack,
 COUNT(DISTINCT cl.acp_id) AS cust_count_jwn,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM customer_lookup AS cl
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute AS tsca ON LOWER(tsca.acp_id) = LOWER(cl.acp_id) AND cl.week_num_realigned
        = tsca.week_num_realigned AND cl.month_num_realigned = tsca.month_num_realigned AND cl.quarter_num_realigned =
     tsca.quarter_num_realigned AND cl.year_num_realigned = tsca.year_num_realigned AND LOWER(tsca.time_granularity) =
   LOWER(TRIM('WEEK    '))
WHERE cl.week_num_realigned >= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
 AND cl.week_num_realigned <= (SELECT DISTINCT week_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = CURRENT_DATE('PST8PDT'))
GROUP BY time_granularity,
 cl.week_num_realigned,
 cl.month_num_realigned,
 cl.quarter_num_realigned,
 cl.year_num_realigned,
 tsca.region,
 tsca.dma,
 tsca.aec,
 tsca.predicted_segment,
 tsca.loyalty_level,
 tsca.loyalty_type,
 tsca.new_to_jwn;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO all_times
(SELECT 'WEEK    ' AS time_granularity,
  cl.week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn,
  COUNT(DISTINCT CASE
    WHEN cl.fls_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_fls,
  COUNT(DISTINCT CASE
    WHEN cl.ncom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_ncom,
  COUNT(DISTINCT CASE
    WHEN cl.rs_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rs,
  COUNT(DISTINCT CASE
    WHEN cl.rcom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rcom,
  COUNT(DISTINCT CASE
    WHEN cl.store_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_stores,
  COUNT(DISTINCT CASE
    WHEN cl.digital_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_digital,
  COUNT(DISTINCT CASE
    WHEN cl.nord_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_nord,
  COUNT(DISTINCT CASE
    WHEN cl.rack_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rack,
  COUNT(DISTINCT cl.acp_id) AS cust_count_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute AS tsca ON LOWER(tsca.acp_id) = LOWER(cl.acp_id) AND cl.week_num_realigned
         = tsca.week_num_realigned AND cl.month_num_realigned = tsca.month_num_realigned AND cl.quarter_num_realigned =
      tsca.quarter_num_realigned AND cl.year_num_realigned = tsca.year_num_realigned AND LOWER(tsca.time_granularity) =
    LOWER(TRIM('WEEK    '))
 WHERE cl.week_num_realigned >= (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
  AND cl.week_num_realigned < (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
 GROUP BY time_granularity,
  cl.week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO all_times
(SELECT 'WEEK    ' AS time_granularity,
  cl.week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn,
  COUNT(DISTINCT CASE
    WHEN cl.fls_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_fls,
  COUNT(DISTINCT CASE
    WHEN cl.ncom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_ncom,
  COUNT(DISTINCT CASE
    WHEN cl.rs_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rs,
  COUNT(DISTINCT CASE
    WHEN cl.rcom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rcom,
  COUNT(DISTINCT CASE
    WHEN cl.store_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_stores,
  COUNT(DISTINCT CASE
    WHEN cl.digital_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_digital,
  COUNT(DISTINCT CASE
    WHEN cl.nord_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_nord,
  COUNT(DISTINCT CASE
    WHEN cl.rack_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rack,
  COUNT(DISTINCT cl.acp_id) AS cust_count_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute AS tsca ON LOWER(tsca.acp_id) = LOWER(cl.acp_id) AND cl.week_num_realigned
         = tsca.week_num_realigned AND cl.month_num_realigned = tsca.month_num_realigned AND cl.quarter_num_realigned =
      tsca.quarter_num_realigned AND cl.year_num_realigned = tsca.year_num_realigned AND LOWER(tsca.time_granularity) =
    LOWER(TRIM('WEEK    '))
 WHERE cl.week_num_realigned >= (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH))
  AND cl.week_num_realigned < (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
 GROUP BY time_granularity,
  cl.week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
--COLLECT STATISTICS COLUMN (region) ON all_times;
--COLLECT STATISTICS COLUMN (dma) ON all_times;
--COLLECT STATISTICS COLUMN (aec) ON all_times;
--COLLECT STATISTICS COLUMN (predicted_segment) ON all_times;
--COLLECT STATISTICS COLUMN (loyalty_level) ON all_times;
--COLLECT STATISTICS COLUMN (loyalty_type) ON all_times;
--COLLECT STATISTICS COLUMN (new_to_jwn) ON all_times;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO all_times
(SELECT 'MONTH' AS time_granularity,
  0 AS week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn,
  COUNT(DISTINCT CASE
    WHEN cl.fls_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_fls,
  COUNT(DISTINCT CASE
    WHEN cl.ncom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_ncom,
  COUNT(DISTINCT CASE
    WHEN cl.rs_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rs,
  COUNT(DISTINCT CASE
    WHEN cl.rcom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rcom,
  COUNT(DISTINCT CASE
    WHEN cl.store_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_stores,
  COUNT(DISTINCT CASE
    WHEN cl.digital_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_digital,
  COUNT(DISTINCT CASE
    WHEN cl.nord_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_nord,
  COUNT(DISTINCT CASE
    WHEN cl.rack_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rack,
  COUNT(DISTINCT cl.acp_id) AS cust_count_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute AS tsca ON LOWER(tsca.acp_id) = LOWER(cl.acp_id) AND tsca.week_num_realigned
         = 0 AND cl.month_num_realigned = tsca.month_num_realigned AND cl.quarter_num_realigned = tsca.quarter_num_realigned
       AND cl.year_num_realigned = tsca.year_num_realigned AND LOWER(tsca.time_granularity) = LOWER('MONTH')
 WHERE cl.month_num_realigned >= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
  AND cl.month_num_realigned <= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = CURRENT_DATE('PST8PDT'))
 GROUP BY time_granularity,
  week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO all_times
(SELECT 'MONTH' AS time_granularity,
  0 AS week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn,
  COUNT(DISTINCT CASE
    WHEN cl.fls_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_fls,
  COUNT(DISTINCT CASE
    WHEN cl.ncom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_ncom,
  COUNT(DISTINCT CASE
    WHEN cl.rs_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rs,
  COUNT(DISTINCT CASE
    WHEN cl.rcom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rcom,
  COUNT(DISTINCT CASE
    WHEN cl.store_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_stores,
  COUNT(DISTINCT CASE
    WHEN cl.digital_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_digital,
  COUNT(DISTINCT CASE
    WHEN cl.nord_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_nord,
  COUNT(DISTINCT CASE
    WHEN cl.rack_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rack,
  COUNT(DISTINCT cl.acp_id) AS cust_count_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute AS tsca ON LOWER(tsca.acp_id) = LOWER(cl.acp_id) AND tsca.week_num_realigned
         = 0 AND cl.month_num_realigned = tsca.month_num_realigned AND cl.quarter_num_realigned = tsca.quarter_num_realigned
       AND cl.year_num_realigned = tsca.year_num_realigned AND LOWER(tsca.time_granularity) = LOWER('MONTH')
 WHERE cl.month_num_realigned >= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
  AND cl.month_num_realigned < (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 8 MONTH))
 GROUP BY time_granularity,
  week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO all_times
(SELECT 'MONTH' AS time_granularity,
  0 AS week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn,
  COUNT(DISTINCT CASE
    WHEN cl.fls_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_fls,
  COUNT(DISTINCT CASE
    WHEN cl.ncom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_ncom,
  COUNT(DISTINCT CASE
    WHEN cl.rs_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rs,
  COUNT(DISTINCT CASE
    WHEN cl.rcom_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rcom,
  COUNT(DISTINCT CASE
    WHEN cl.store_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_stores,
  COUNT(DISTINCT CASE
    WHEN cl.digital_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_digital,
  COUNT(DISTINCT CASE
    WHEN cl.nord_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_nord,
  COUNT(DISTINCT CASE
    WHEN cl.rack_shopper = 1
    THEN cl.acp_id
    ELSE NULL
    END) AS cust_count_rack,
  COUNT(DISTINCT cl.acp_id) AS cust_count_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM customer_lookup AS cl
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute AS tsca ON LOWER(tsca.acp_id) = LOWER(cl.acp_id) AND tsca.week_num_realigned
         = 0 AND cl.month_num_realigned = tsca.month_num_realigned AND cl.quarter_num_realigned = tsca.quarter_num_realigned
       AND cl.year_num_realigned = tsca.year_num_realigned AND LOWER(tsca.time_granularity) = LOWER('MONTH')
 WHERE cl.month_num_realigned >= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH))
  AND cl.month_num_realigned < (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 16 MONTH))
  AND cl.week_num_realigned >= (SELECT DISTINCT week_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH))
 GROUP BY time_granularity,
  week_num_realigned,
  cl.month_num_realigned,
  cl.quarter_num_realigned,
  cl.year_num_realigned,
  tsca.region,
  tsca.dma,
  tsca.aec,
  tsca.predicted_segment,
  tsca.loyalty_level,
  tsca.loyalty_type,
  tsca.new_to_jwn);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
--COLLECT STATISTICS COLUMN (region) ON all_times;
--COLLECT STATISTICS COLUMN (dma) ON all_times;
--COLLECT STATISTICS COLUMN (aec) ON all_times;
--COLLECT STATISTICS COLUMN (predicted_segment) ON all_times;
--COLLECT STATISTICS COLUMN (loyalty_level) ON all_times;
--COLLECT STATISTICS COLUMN (loyalty_type) ON all_times;
--COLLECT STATISTICS COLUMN (new_to_jwn) ON all_times;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO all_times
(SELECT *
 FROM (SELECT 'QUARTER' AS time_granularity,
     0 AS week_num_realigned,
     0 AS month_num_realigned,
     cl.quarter_num_realigned,
     cl.year_num_realigned,
     tsca.region,
     tsca.dma,
     tsca.aec,
     tsca.predicted_segment,
     tsca.loyalty_level,
     tsca.loyalty_type,
     tsca.new_to_jwn,
     COUNT(DISTINCT CASE
       WHEN cl.fls_shopper = 1
       THEN cl.acp_id
       ELSE NULL
       END) AS cust_count_fls,
     COUNT(DISTINCT CASE
       WHEN cl.ncom_shopper = 1
       THEN cl.acp_id
       ELSE NULL
       END) AS cust_count_ncom,
     COUNT(DISTINCT CASE
       WHEN cl.rs_shopper = 1
       THEN cl.acp_id
       ELSE NULL
       END) AS cust_count_rs,
     COUNT(DISTINCT CASE
       WHEN cl.rcom_shopper = 1
       THEN cl.acp_id
       ELSE NULL
       END) AS cust_count_rcom,
     COUNT(DISTINCT CASE
       WHEN cl.store_shopper = 1
       THEN cl.acp_id
       ELSE NULL
       END) AS cust_count_stores,
     COUNT(DISTINCT CASE
       WHEN cl.digital_shopper = 1
       THEN cl.acp_id
       ELSE NULL
       END) AS cust_count_digital,
     COUNT(DISTINCT CASE
       WHEN cl.nord_shopper = 1
       THEN cl.acp_id
       ELSE NULL
       END) AS cust_count_nord,
     COUNT(DISTINCT CASE
       WHEN cl.rack_shopper = 1
       THEN cl.acp_id
       ELSE NULL
       END) AS cust_count_rack,
     COUNT(DISTINCT cl.acp_id) AS cust_count_jwn,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM customer_lookup AS cl
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute AS tsca ON LOWER(tsca.acp_id) = LOWER(cl.acp_id) AND
           tsca.week_num_realigned = 0 AND tsca.month_num_realigned = 0 AND cl.quarter_num_realigned = tsca.quarter_num_realigned
          AND cl.year_num_realigned = tsca.year_num_realigned AND LOWER(tsca.time_granularity) = LOWER('QUARTER')
    WHERE cl.week_num_realigned >= (SELECT DISTINCT week_num
       FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
       WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH))
    GROUP BY time_granularity,
     week_num_realigned,
     cl.quarter_num_realigned,
     cl.year_num_realigned,
     tsca.region,
     tsca.dma,
     tsca.aec,
     tsca.predicted_segment,
     tsca.loyalty_level,
     tsca.loyalty_type,
     tsca.new_to_jwn,
     month_num_realigned
    UNION DISTINCT
    SELECT 'YEAR' AS time_granularity,
     0 AS week_num_realigned,
     0 AS month_num_realigned,
     0 AS quarter_num_realigned,
     cl0.year_num_realigned,
     tsca0.region,
     tsca0.dma,
     tsca0.aec,
     tsca0.predicted_segment,
     tsca0.loyalty_level,
     tsca0.loyalty_type,
     tsca0.new_to_jwn,
     COUNT(DISTINCT CASE
       WHEN cl0.fls_shopper = 1
       THEN cl0.acp_id
       ELSE NULL
       END) AS cust_count_fls,
     COUNT(DISTINCT CASE
       WHEN cl0.ncom_shopper = 1
       THEN cl0.acp_id
       ELSE NULL
       END) AS cust_count_ncom,
     COUNT(DISTINCT CASE
       WHEN cl0.rs_shopper = 1
       THEN cl0.acp_id
       ELSE NULL
       END) AS cust_count_rs,
     COUNT(DISTINCT CASE
       WHEN cl0.rcom_shopper = 1
       THEN cl0.acp_id
       ELSE NULL
       END) AS cust_count_rcom,
     COUNT(DISTINCT CASE
       WHEN cl0.store_shopper = 1
       THEN cl0.acp_id
       ELSE NULL
       END) AS cust_count_stores,
     COUNT(DISTINCT CASE
       WHEN cl0.digital_shopper = 1
       THEN cl0.acp_id
       ELSE NULL
       END) AS cust_count_digital,
     COUNT(DISTINCT CASE
       WHEN cl0.nord_shopper = 1
       THEN cl0.acp_id
       ELSE NULL
       END) AS cust_count_nord,
     COUNT(DISTINCT CASE
       WHEN cl0.rack_shopper = 1
       THEN cl0.acp_id
       ELSE NULL
       END) AS cust_count_rack,
     COUNT(DISTINCT cl0.acp_id) AS cust_count_jwn,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM customer_lookup AS cl0
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_single_attribute AS tsca0 ON LOWER(tsca0.acp_id) = LOWER(cl0.acp_id) AND
           tsca0.week_num_realigned = 0 AND tsca0.month_num_realigned = 0 AND tsca0.quarter_num_realigned = 0 AND cl0.year_num_realigned
         = tsca0.year_num_realigned AND LOWER(tsca0.time_granularity) = LOWER('YEAR')
    WHERE cl0.week_num_realigned >= (SELECT DISTINCT week_num
       FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
       WHERE day_date = DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH))
    GROUP BY time_granularity,
     week_num_realigned,
     quarter_num_realigned,
     cl0.year_num_realigned,
     tsca0.region,
     tsca0.dma,
     tsca0.aec,
     tsca0.predicted_segment,
     tsca0.loyalty_level,
     tsca0.loyalty_type,
     tsca0.new_to_jwn,
     month_num_realigned) AS t15);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN (week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned, time_granularity) ON all_times;
--COLLECT STATISTICS COLUMN (region) ON all_times;
--COLLECT STATISTICS COLUMN (dma) ON all_times;
--COLLECT STATISTICS COLUMN (aec) ON all_times;
--COLLECT STATISTICS COLUMN (predicted_segment) ON all_times;
--COLLECT STATISTICS COLUMN (loyalty_level) ON all_times;
--COLLECT STATISTICS COLUMN (loyalty_type) ON all_times;
--COLLECT STATISTICS COLUMN (new_to_jwn) ON all_times;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_count;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_cust_count
(SELECT TRIM(time_granularity) AS time_granularity,
  week_num_realigned,
  month_num_realigned,
  quarter_num_realigned,
  year_num_realigned,
  COALESCE(region, 'Z_UNKNOWN') AS region,
  COALESCE(dma, 'Z_UNKNOWN') AS dma,
  COALESCE(aec, 'UNDEFINED') AS aec,
  COALESCE(predicted_segment, 'Z_UNKNOWN') AS predicted_segment,
  COALESCE(loyalty_level, '0) NOT A MEMBER') AS loyalty_level,
  COALESCE(loyalty_type, 'c) Non-Loyalty') AS loyalty_type,
  COALESCE(new_to_jwn, 0) AS new_to_jwn,
  cust_count_fls,
  cust_count_ncom,
  cust_count_rs,
  cust_count_rcom,
  cust_count_stores,
  cust_count_digital,
  cust_count_nord,
  cust_count_rack,
  cust_count_jwn,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM all_times);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

END;