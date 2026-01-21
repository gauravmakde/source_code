
/*SET QUERY_BAND = 'App_ID=app08818;
DAG_ID=trips_sandbox_yoy_11521_ACE_ENG;
---     Task_Name=trips_sandbox_yoy;'*/
---     FOR SESSION VOLATILE;

CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT dc.month_num,
 dc.quarter_num,
 dc.year_num,
  CASE
  WHEN dc.month_num >= WN.month_num - 100 AND dc.month_num < WN.month_num
  THEN 'TY'
  WHEN dc.month_num >= WN.month_num - 200 AND dc.month_num < WN.month_num - 100
  THEN 'LY'
  ELSE NULL
  END AS year_id,
 MIN(dc.day_date) AS ty_start_dt,
 MAX(dc.day_date) AS ty_end_dt
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
 LEFT JOIN (SELECT DISTINCT month_num
  FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
  WHERE day_date = CURRENT_DATE('PST8PDT')) AS WN ON TRUE
WHERE dc.month_num >= (SELECT DISTINCT month_num
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
    WHERE day_date = CURRENT_DATE('PST8PDT')) - 200
 AND dc.month_num < (SELECT DISTINCT month_num
   FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.usl_rolling_52wk_calendar AS dc
   WHERE day_date = CURRENT_DATE('PST8PDT'))
GROUP BY dc.month_num,
 dc.quarter_num,
 dc.year_num,
 year_id;

 
CREATE TEMPORARY TABLE IF NOT EXISTS customer_lookup
AS
SELECT DISTINCT tswc.acp_id,
 tswc.week_num_realigned,
 tswc.month_num_realigned,
 tswc.quarter_num_realigned,
 tswc.year_num_realigned,
 dc.year_id,
 tswc.region,
 tswc.dma,
 tswc.aec,
 tswc.predicted_segment,
 tswc.loyalty_level,
 tswc.loyalty_type,
 tswc.new_to_jwn,
 ROW_NUMBER() OVER (PARTITION BY tswc.acp_id, dc.year_id ORDER BY tswc.week_num_realigned DESC) AS year_id_week_order
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_weekly_cust AS tswc
 INNER JOIN date_lookup AS dc 
 ON tswc.month_num_realigned = dc.month_num;

CREATE TEMPORARY TABLE IF NOT EXISTS customer_single_attribute
AS
SELECT acp_id,
 year_id,
 COALESCE(MAX(CASE
    WHEN year_id_week_order = 1
    THEN region
    ELSE NULL
    END), 'Z_UNKNOWN') AS region,
 COALESCE(MAX(CASE
    WHEN year_id_week_order = 1
    THEN dma
    ELSE NULL
    END), 'Z_UNKNOWN') AS dma,
 COALESCE(MAX(CASE
    WHEN year_id_week_order = 1
    THEN aec
    ELSE NULL
    END), 'UNDEFINED') AS aec,
 COALESCE(MAX(CASE
    WHEN year_id_week_order = 1
    THEN predicted_segment
    ELSE NULL
    END), 'Z_UNKNOWN') AS predicted_segment,
 COALESCE(MAX(CASE
    WHEN year_id_week_order = 1
    THEN loyalty_level
    ELSE NULL
    END), '0) NOT A MEMBER') AS loyalty_level,
 COALESCE(MAX(CASE
    WHEN year_id_week_order = 1
    THEN loyalty_type
    ELSE NULL
    END), 'c) Non-Loyalty') AS loyalty_type,
 COALESCE(MAX(new_to_jwn), 0) AS new_to_jwn
FROM customer_lookup
GROUP BY acp_id,
 year_id;

CREATE TEMPORARY TABLE IF NOT EXISTS ty_data
AS
SELECT dc.year_id,
 csa.region,
 csa.dma,
 csa.aec,
 csa.predicted_segment,
 csa.loyalty_level,
 csa.loyalty_type,
 csa.new_to_jwn,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_fls,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_ncom,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_rs,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_rcom,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) IN (LOWER('1) Nordstrom Stores'), LOWER('3) Rack Stores'))
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_stores,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) IN (LOWER('2) Nordstrom.com'), LOWER('4) Rack.com'))
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_digital,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.banner) = LOWER('1) Nordstrom Banner')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_nord,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.banner) = LOWER('2) Rack Banner')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_rack,
 COUNT(DISTINCT scf.acp_id) AS cust_count_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.trips
   ELSE NULL
   END) AS trips_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.trips
   ELSE NULL
   END) AS trips_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.trips
   ELSE NULL
   END) AS trips_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.trips
   ELSE NULL
   END) AS trips_rcom,
 SUM(scf.trips) AS trips_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.net_spend
   ELSE NULL
   END) AS net_spend_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.net_spend
   ELSE NULL
   END) AS net_spend_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.net_spend
   ELSE NULL
   END) AS net_spend_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.net_spend
   ELSE NULL
   END) AS net_spend_rcom,
 SUM(scf.net_spend) AS net_spend_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.gross_spend
   ELSE NULL
   END) AS gross_spend_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.gross_spend
   ELSE NULL
   END) AS gross_spend_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.gross_spend
   ELSE NULL
   END) AS gross_spend_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.gross_spend
   ELSE NULL
   END) AS gross_spend_rcom,
 SUM(scf.gross_spend) AS gross_spend_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.net_units
   ELSE NULL
   END) AS net_units_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.net_units
   ELSE NULL
   END) AS net_units_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.net_units
   ELSE NULL
   END) AS net_units_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.net_units
   ELSE NULL
   END) AS net_units_rcom,
 SUM(scf.net_units) AS net_units_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.gross_units
   ELSE NULL
   END) AS gross_units_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.gross_units
   ELSE NULL
   END) AS gross_units_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.gross_units
   ELSE NULL
   END) AS gross_units_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.gross_units
   ELSE NULL
   END) AS gross_units_rcom,
 SUM(scf.gross_units) AS gross_units_jwn
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_weekly_cust AS scf
 INNER JOIN date_lookup AS dc 
 ON scf.month_num_realigned = dc.month_num 
 AND LOWER(dc.year_id) = LOWER('TY')
 LEFT JOIN customer_single_attribute AS csa 
 ON LOWER(csa.acp_id) = LOWER(scf.acp_id) 
 AND LOWER(csa.year_id) = LOWER(dc.year_id)
GROUP BY dc.year_id,
 csa.region,
 csa.dma,
 csa.aec,
 csa.predicted_segment,
 csa.loyalty_level,
 csa.loyalty_type,
 csa.new_to_jwn;

CREATE TEMPORARY TABLE IF NOT EXISTS ly_data
AS
SELECT dc.year_id,
 csa.region,
 csa.dma,
 csa.aec,
 csa.predicted_segment,
 csa.loyalty_level,
 csa.loyalty_type,
 csa.new_to_jwn,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_fls,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_ncom,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_rs,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_rcom,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) IN (LOWER('1) Nordstrom Stores'), LOWER('3) Rack Stores'))
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_stores,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.channel) IN (LOWER('2) Nordstrom.com'), LOWER('4) Rack.com'))
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_digital,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.banner) = LOWER('1) Nordstrom Banner')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_nord,
 COUNT(DISTINCT CASE
   WHEN LOWER(scf.banner) = LOWER('2) Rack Banner')
   THEN scf.acp_id
   ELSE NULL
   END) AS cust_count_rack,
 COUNT(DISTINCT scf.acp_id) AS cust_count_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.trips
   ELSE NULL
   END) AS trips_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.trips
   ELSE NULL
   END) AS trips_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.trips
   ELSE NULL
   END) AS trips_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.trips
   ELSE NULL
   END) AS trips_rcom,
 SUM(scf.trips) AS trips_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.net_spend
   ELSE NULL
   END) AS net_spend_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.net_spend
   ELSE NULL
   END) AS net_spend_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.net_spend
   ELSE NULL
   END) AS net_spend_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.net_spend
   ELSE NULL
   END) AS net_spend_rcom,
 SUM(scf.net_spend) AS net_spend_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.gross_spend
   ELSE NULL
   END) AS gross_spend_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.gross_spend
   ELSE NULL
   END) AS gross_spend_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.gross_spend
   ELSE NULL
   END) AS gross_spend_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.gross_spend
   ELSE NULL
   END) AS gross_spend_rcom,
 SUM(scf.gross_spend) AS gross_spend_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.net_units
   ELSE NULL
   END) AS net_units_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.net_units
   ELSE NULL
   END) AS net_units_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.net_units
   ELSE NULL
   END) AS net_units_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.net_units
   ELSE NULL
   END) AS net_units_rcom,
 SUM(scf.net_units) AS net_units_jwn,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('1) Nordstrom Stores')
   THEN scf.gross_units
   ELSE NULL
   END) AS gross_units_fls,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('2) Nordstrom.com')
   THEN scf.gross_units
   ELSE NULL
   END) AS gross_units_ncom,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('3) Rack Stores')
   THEN scf.gross_units
   ELSE NULL
   END) AS gross_units_rs,
 SUM(CASE
   WHEN LOWER(scf.channel) = LOWER('4) Rack.com')
   THEN scf.gross_units
   ELSE NULL
   END) AS gross_units_rcom,
 SUM(scf.gross_units) AS gross_units_jwn
FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_weekly_cust AS scf
 INNER JOIN date_lookup AS dc ON scf.month_num_realigned = dc.month_num AND LOWER(dc.year_id) = LOWER('LY')
 LEFT JOIN customer_single_attribute AS csa ON LOWER(csa.acp_id) = LOWER(scf.acp_id) AND LOWER(csa.year_id) = LOWER(dc.year_id
    )
GROUP BY dc.year_id,
 csa.region,
 csa.dma,
 csa.aec,
 csa.predicted_segment,
 csa.loyalty_level,
 csa.loyalty_type,
 csa.new_to_jwn;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_yoy;

INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.trips_sandbox_yoy
(SELECT year_id,
  region,
  dma,
  aec,
  predicted_segment,
  loyalty_level,
  loyalty_type,
  new_to_jwn,
  cust_count_fls_ty,
  cust_count_ncom_ty,
  cust_count_rs_ty,
  cust_count_rcom_ty,
  cust_count_stores_ty,
  cust_count_digital_ty,
  cust_count_nord_ty,
  cust_count_rack_ty,
  cust_count_jwn_ty,
  trips_fls_ty,
  trips_ncom_ty,
  trips_rs_ty,
  trips_rcom_ty,
  trips_jwn_ty,
  CAST(net_spend_fls_ty AS NUMERIC) AS net_spend_fls_ty,
  CAST(net_spend_ncom_ty AS NUMERIC) AS net_spend_ncom_ty,
  CAST(net_spend_rs_ty AS NUMERIC) AS net_spend_rs_ty,
  CAST(net_spend_rcom_ty AS NUMERIC) AS net_spend_rcom_ty,
  net_spend_jwn_ty,
  CAST(gross_spend_fls_ty AS NUMERIC) AS gross_spend_fls_ty,
  CAST(gross_spend_ncom_ty AS NUMERIC) AS gross_spend_ncom_ty,
  CAST(gross_spend_rs_ty AS NUMERIC) AS gross_spend_rs_ty,
  CAST(gross_spend_rcom_ty AS NUMERIC) AS gross_spend_rcom_ty,
  gross_spend_jwn_ty,
  CAST(net_units_fls_ty AS NUMERIC) AS net_units_fls_ty,
  CAST(net_units_ncom_ty AS NUMERIC) AS net_units_ncom_ty,
  CAST(net_units_rs_ty AS NUMERIC) AS net_units_rs_ty,
  CAST(net_units_rcom_ty AS NUMERIC) AS net_units_rcom_ty,
  net_units_jwn_ty,
  CAST(gross_units_fls_ty AS NUMERIC) AS gross_units_fls_ty,
  CAST(gross_units_ncom_ty AS NUMERIC) AS gross_units_ncom_ty,
  CAST(gross_units_rs_ty AS NUMERIC) AS gross_units_rs_ty,
  CAST(gross_units_rcom_ty AS NUMERIC) AS gross_units_rcom_ty,
  gross_units_jwn_ty,
  cust_count_fls_ly,
  cust_count_ncom_ly,
  cust_count_rs_ly,
  cust_count_rcom_ly,
  cust_count_stores_ly,
  cust_count_digital_ly,
  cust_count_nord_ly,
  cust_count_rack_ly,
  cust_count_jwn_ly,
  trips_fls_ly,
  trips_ncom_ly,
  trips_rs_ly,
  trips_rcom_ly,
  trips_jwn_ly,
  CAST(net_spend_fls_ly AS NUMERIC) AS net_spend_fls_ly,
  CAST(net_spend_ncom_ly AS NUMERIC) AS net_spend_ncom_ly,
  CAST(net_spend_rs_ly AS NUMERIC) AS net_spend_rs_ly,
  CAST(net_spend_rcom_ly AS NUMERIC) AS net_spend_rcom_ly,
  net_spend_jwn_ly,
  CAST(gross_spend_fls_ly AS NUMERIC) AS gross_spend_fls_ly,
  CAST(gross_spend_ncom_ly AS NUMERIC) AS gross_spend_ncom_ly,
  CAST(gross_spend_rs_ly AS NUMERIC) AS gross_spend_rs_ly,
  CAST(gross_spend_rcom_ly AS NUMERIC) AS gross_spend_rcom_ly,
  gross_spend_jwn_ly,
  CAST(net_units_fls_ly AS NUMERIC) AS net_units_fls_ly,
  CAST(net_units_ncom_ly AS NUMERIC) AS net_units_ncom_ly,
  CAST(net_units_rs_ly AS NUMERIC) AS net_units_rs_ly,
  CAST(net_units_rcom_ly AS NUMERIC) AS net_units_rcom_ly,
  net_units_jwn_ly,
  CAST(gross_units_fls_ly AS NUMERIC) AS gross_units_fls_ly,
  CAST(gross_units_ncom_ly AS NUMERIC) AS gross_units_ncom_ly,
  CAST(gross_units_rs_ly AS NUMERIC) AS gross_units_rs_ly,
  CAST(gross_units_rcom_ly AS NUMERIC) AS gross_units_rcom_ly,
  gross_units_jwn_ly,
  dw_sys_load_tmstp
 FROM (SELECT 'TY' AS year_id,
     COALESCE(tso.region, tso2.region) AS region,
     COALESCE(tso.dma, tso2.dma) AS dma,
     COALESCE(tso.aec, tso2.aec) AS aec,
     COALESCE(tso.predicted_segment, tso2.predicted_segment) AS predicted_segment,
     COALESCE(tso.loyalty_level, tso2.loyalty_level) AS loyalty_level,
     COALESCE(tso.loyalty_type, tso2.loyalty_type) AS loyalty_type,
     COALESCE(tso.new_to_jwn, tso2.new_to_jwn) AS new_to_jwn,
     COALESCE(tso.cust_count_fls, 0) AS cust_count_fls_ty,
     COALESCE(tso.cust_count_ncom, 0) AS cust_count_ncom_ty,
     COALESCE(tso.cust_count_rs, 0) AS cust_count_rs_ty,
     COALESCE(tso.cust_count_rcom, 0) AS cust_count_rcom_ty,
     COALESCE(tso.cust_count_stores, 0) AS cust_count_stores_ty,
     COALESCE(tso.cust_count_digital, 0) AS cust_count_digital_ty,
     COALESCE(tso.cust_count_nord, 0) AS cust_count_nord_ty,
     COALESCE(tso.cust_count_rack, 0) AS cust_count_rack_ty,
     COALESCE(tso.cust_count_jwn, 0) AS cust_count_jwn_ty,
     COALESCE(tso.trips_fls, 0) AS trips_fls_ty,
     COALESCE(tso.trips_ncom, 0) AS trips_ncom_ty,
     COALESCE(tso.trips_rs, 0) AS trips_rs_ty,
     COALESCE(tso.trips_rcom, 0) AS trips_rcom_ty,
     COALESCE(tso.trips_jwn, 0) AS trips_jwn_ty,
     COALESCE(tso.net_spend_fls, 0) AS net_spend_fls_ty,
     COALESCE(tso.net_spend_ncom, 0) AS net_spend_ncom_ty,
     COALESCE(tso.net_spend_rs, 0) AS net_spend_rs_ty,
     COALESCE(tso.net_spend_rcom, 0) AS net_spend_rcom_ty,
     COALESCE(tso.net_spend_jwn, 0) AS net_spend_jwn_ty,
     COALESCE(tso.gross_spend_fls, 0) AS gross_spend_fls_ty,
     COALESCE(tso.gross_spend_ncom, 0) AS gross_spend_ncom_ty,
     COALESCE(tso.gross_spend_rs, 0) AS gross_spend_rs_ty,
     COALESCE(tso.gross_spend_rcom, 0) AS gross_spend_rcom_ty,
     COALESCE(tso.gross_spend_jwn, 0) AS gross_spend_jwn_ty,
     COALESCE(tso.net_units_fls, 0) AS net_units_fls_ty,
     COALESCE(tso.net_units_ncom, 0) AS net_units_ncom_ty,
     COALESCE(tso.net_units_rs, 0) AS net_units_rs_ty,
     COALESCE(tso.net_units_rcom, 0) AS net_units_rcom_ty,
     COALESCE(tso.net_units_jwn, 0) AS net_units_jwn_ty,
     COALESCE(tso.gross_units_fls, 0) AS gross_units_fls_ty,
     COALESCE(tso.gross_units_ncom, 0) AS gross_units_ncom_ty,
     COALESCE(tso.gross_units_rs, 0) AS gross_units_rs_ty,
     COALESCE(tso.gross_units_rcom, 0) AS gross_units_rcom_ty,
     COALESCE(tso.gross_units_jwn, 0) AS gross_units_jwn_ty,
     COALESCE(tso2.cust_count_fls, 0) AS cust_count_fls_ly,
     COALESCE(tso2.cust_count_ncom, 0) AS cust_count_ncom_ly,
     COALESCE(tso2.cust_count_rs, 0) AS cust_count_rs_ly,
     COALESCE(tso2.cust_count_rcom, 0) AS cust_count_rcom_ly,
     COALESCE(tso2.cust_count_stores, 0) AS cust_count_stores_ly,
     COALESCE(tso2.cust_count_digital, 0) AS cust_count_digital_ly,
     COALESCE(tso2.cust_count_nord, 0) AS cust_count_nord_ly,
     COALESCE(tso2.cust_count_rack, 0) AS cust_count_rack_ly,
     COALESCE(tso2.cust_count_jwn, 0) AS cust_count_jwn_ly,
     COALESCE(tso2.trips_fls, 0) AS trips_fls_ly,
     COALESCE(tso2.trips_ncom, 0) AS trips_ncom_ly,
     COALESCE(tso2.trips_rs, 0) AS trips_rs_ly,
     COALESCE(tso2.trips_rcom, 0) AS trips_rcom_ly,
     COALESCE(tso2.trips_jwn, 0) AS trips_jwn_ly,
     COALESCE(tso2.net_spend_fls, 0) AS net_spend_fls_ly,
     COALESCE(tso2.net_spend_ncom, 0) AS net_spend_ncom_ly,
     COALESCE(tso2.net_spend_rs, 0) AS net_spend_rs_ly,
     COALESCE(tso2.net_spend_rcom, 0) AS net_spend_rcom_ly,
     COALESCE(tso2.net_spend_jwn, 0) AS net_spend_jwn_ly,
     COALESCE(tso2.gross_spend_fls, 0) AS gross_spend_fls_ly,
     COALESCE(tso2.gross_spend_ncom, 0) AS gross_spend_ncom_ly,
     COALESCE(tso2.gross_spend_rs, 0) AS gross_spend_rs_ly,
     COALESCE(tso2.gross_spend_rcom, 0) AS gross_spend_rcom_ly,
     COALESCE(tso2.gross_spend_jwn, 0) AS gross_spend_jwn_ly,
     COALESCE(tso2.net_units_fls, 0) AS net_units_fls_ly,
     COALESCE(tso2.net_units_ncom, 0) AS net_units_ncom_ly,
     COALESCE(tso2.net_units_rs, 0) AS net_units_rs_ly,
     COALESCE(tso2.net_units_rcom, 0) AS net_units_rcom_ly,
     COALESCE(tso2.net_units_jwn, 0) AS net_units_jwn_ly,
     COALESCE(tso2.gross_units_fls, 0) AS gross_units_fls_ly,
     COALESCE(tso2.gross_units_ncom, 0) AS gross_units_ncom_ly,
     COALESCE(tso2.gross_units_rs, 0) AS gross_units_rs_ly,
     COALESCE(tso2.gross_units_rcom, 0) AS gross_units_rcom_ly,
     COALESCE(tso2.gross_units_jwn, 0) AS gross_units_jwn_ly,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM ty_data AS tso
     FULL JOIN ly_data AS tso2 
	 ON LOWER(tso.region) = LOWER(tso2.region) 
	 AND LOWER(tso.dma) = LOWER(tso2.dma) 
	 AND LOWER(tso.aec) = LOWER(tso2.aec) 
	 AND LOWER(tso.predicted_segment) = LOWER(tso2.predicted_segment) 
	 AND LOWER(tso.loyalty_level) = LOWER(tso2.loyalty_level) 
	 AND LOWER(tso.loyalty_type) = LOWER(tso2.loyalty_type) 
	 AND tso.new_to_jwn = tso2.new_to_jwn
    UNION DISTINCT
    SELECT 'LY' AS year_id,
     region,
     dma,
     aec,
     predicted_segment,
     loyalty_level,
     loyalty_type,
     new_to_jwn,
     cust_count_fls AS cust_count_fls_ty,
     cust_count_ncom AS cust_count_ncom_ty,
     cust_count_rs AS cust_count_rs_ty,
     cust_count_rcom AS cust_count_rcom_ty,
     cust_count_stores AS cust_count_stores_ty,
     cust_count_digital AS cust_count_digital_ty,
     cust_count_nord AS cust_count_nord_ty,
     cust_count_rack AS cust_count_rack_ty,
     cust_count_jwn AS cust_count_jwn_ty,
     COALESCE(trips_fls, 0) AS trips_fls_ty,
     COALESCE(trips_ncom, 0) AS trips_ncom_ty,
     COALESCE(trips_rs, 0) AS trips_rs_ty,
     COALESCE(trips_rcom, 0) AS trips_rcom_ty,
     COALESCE(trips_jwn, 0) AS trips_jwn_ty,
     COALESCE(net_spend_fls, 0) AS net_spend_fls_ty,
     COALESCE(net_spend_ncom, 0) AS net_spend_ncom_ty,
     COALESCE(net_spend_rs, 0) AS net_spend_rs_ty,
     COALESCE(net_spend_rcom, 0) AS net_spend_rcom_ty,
     COALESCE(net_spend_jwn, 0) AS net_spend_jwn_ty,
     COALESCE(gross_spend_fls, 0) AS gross_spend_fls_ty,
     COALESCE(gross_spend_ncom, 0) AS gross_spend_ncom_ty,
     COALESCE(gross_spend_rs, 0) AS gross_spend_rs_ty,
     COALESCE(gross_spend_rcom, 0) AS gross_spend_rcom_ty,
     COALESCE(gross_spend_jwn, 0) AS gross_spend_jwn_ty,
     COALESCE(net_units_fls, 0) AS net_units_fls_ty,
     COALESCE(net_units_ncom, 0) AS net_units_ncom_ty,
     COALESCE(net_units_rs, 0) AS net_units_rs_ty,
     COALESCE(net_units_rcom, 0) AS net_units_rcom_ty,
     COALESCE(net_units_jwn, 0) AS net_units_jwn_ty,
     COALESCE(gross_units_fls, 0) AS gross_units_fls_ty,
     COALESCE(gross_units_ncom, 0) AS gross_units_ncom_ty,
     COALESCE(gross_units_rs, 0) AS gross_units_rs_ty,
     COALESCE(gross_units_rcom, 0) AS gross_units_rcom_ty,
     COALESCE(gross_units_jwn, 0) AS gross_units_jwn_ty,
     0 AS cust_count_fls_ly,
     0 AS cust_count_ncom_ly,
     0 AS cust_count_rs_ly,
     0 AS cust_count_rcom_ly,
     0 AS cust_count_stores_ly,
     0 AS cust_count_digital_ly,
     0 AS cust_count_nord_ly,
     0 AS cust_count_rack_ly,
     0 AS cust_count_jwn_ly,
     0 AS trips_fls_ly,
     0 AS trips_ncom_ly,
     0 AS trips_rs_ly,
     0 AS trips_rcom_ly,
     0 AS trips_jwn_ly,
     0 AS net_spend_fls_ly,
     0 AS net_spend_ncom_ly,
     0 AS net_spend_rs_ly,
     0 AS net_spend_rcom_ly,
     0 AS net_spend_jwn_ly,
     0 AS gross_spend_fls_ly,
     0 AS gross_spend_ncom_ly,
     0 AS gross_spend_rs_ly,
     0 AS gross_spend_rcom_ly,
     0 AS gross_spend_jwn_ly,
     0 AS net_units_fls_ly,
     0 AS net_units_ncom_ly,
     0 AS net_units_rs_ly,
     0 AS net_units_rcom_ly,
     0 AS net_units_jwn_ly,
     0 AS gross_units_fls_ly,
     0 AS gross_units_ncom_ly,
     0 AS gross_units_rs_ly,
     0 AS gross_units_rcom_ly,
     0 AS gross_units_jwn_ly,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM ly_data AS tso) AS t1);

