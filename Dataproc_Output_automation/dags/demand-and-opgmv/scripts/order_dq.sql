-- Zero Count Check



INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT dq_object_name,
  dq_metric_name,
  subject_area_nm,
  dw_batch_date,
  CAST(dq_metric_value AS FLOAT64) AS dq_metric_value,
  dq_metric_value_type,
  is_sensitive,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM (SELECT 'JWN_ORDER_LINE_DETAIL_FACT' AS dq_object_name,
     'order_zero_count_check' AS dq_metric_name,
     'JWN_CLARITY_ORDER' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_line_detail_fact_vw AS joldfv
    WHERE dw_batch_date >= DATE_SUB((SELECT extract_start_dt
        FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')), INTERVAL 5 DAY)
     AND dw_batch_date < (SELECT extract_end_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER'))
    UNION ALL
    SELECT 'JWN_ORDER_POR_WRK' AS dq_object_name,
     'order_zero_count_check' AS dq_metric_name,
     'JWN_CLARITY_ORDER' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_por_wrk AS jopw
    WHERE dw_batch_date IS NOT NULL) AS t13);


-- OLDF SOURCE-TARGET Count Check


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT dq_object_name,
  dq_metric_name,
  subject_area_nm,
  dw_batch_date,
  CAST(dq_metric_value AS FLOAT64) AS dq_metric_value,
  dq_metric_value_type,
  is_sensitive,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM (SELECT 'JWN_ORDER_LINE_DETAIL_FACT' AS dq_object_name,
     'order_record_count_check' AS dq_metric_name,
     'JWN_CLARITY_ORDER' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.order_line_detail_fact AS oldf
    WHERE dw_batch_date >= DATE_SUB((SELECT extract_start_dt
        FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')), INTERVAL 5 DAY)
     AND dw_batch_date < (SELECT extract_end_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER'))
    UNION ALL
    SELECT 'JWN_ORDER_LINE_DETAIL_FACT' AS dq_object_name,
     'order_record_count_check' AS dq_metric_name,
     'JWN_CLARITY_ORDER' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_line_detail_fact_vw AS joldfv
    WHERE dw_batch_date >= DATE_SUB((SELECT extract_start_dt
        FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')), INTERVAL 5 DAY)
     AND dw_batch_date < (SELECT extract_end_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER'))) AS t17);


-- OLDF SOURCE-TARGET Amount Check


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT dq_object_name,
  dq_metric_name,
  subject_area_nm,
  dw_batch_date,
  dq_metric_value,
  dq_metric_value_type,
  is_sensitive,
  dm__operand7,
  dm__operand8
 FROM (SELECT 'JWN_ORDER_LINE_DETAIL_FACT' AS dq_object_name,
     'order_record_amount_check' AS dq_metric_name,
     'JWN_CLARITY_ORDER' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     SUM(order_line_current_amount) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dm__operand7,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dm__operand8
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_base_vws.order_line_detail_fact AS oldf
    WHERE dw_batch_date >= DATE_SUB((SELECT extract_start_dt
        FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')), INTERVAL 5 DAY)
     AND dw_batch_date < (SELECT extract_end_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER'))
    UNION ALL
    SELECT 'JWN_ORDER_LINE_DETAIL_FACT' AS dq_object_name,
     'order_record_amount_check' AS dq_metric_name,
     'JWN_CLARITY_ORDER' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     SUM(line_net_amt) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_line_detail_fact_vw AS joldfv
    WHERE dw_batch_date >= DATE_SUB((SELECT extract_start_dt
        FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')), INTERVAL 5 DAY)
     AND dw_batch_date < (SELECT extract_end_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER'))) AS t17);


-- Order POR SOURCE-TARGET Count Check


-- Avoid high-date rows and partial day records


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT dq_object_name,
  dq_metric_name,
  subject_area_nm,
  dw_batch_date,
  CAST(dq_metric_value AS FLOAT64) AS dq_metric_value,
  dq_metric_value_type,
  is_sensitive,
  dm__operand7,
  dm__operand8
 FROM (SELECT 'JWN_ORDER_POR_WRK' AS dq_object_name,
     'order_record_count_check' AS dq_metric_name,
     'JWN_CLARITY_ORDER' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     COUNT(DISTINCT order_num) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dm__operand7,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dm__operand8
    FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.order_line_detail_fact
    WHERE order_date_pacific < CURRENT_DATE
     AND CAST(dw_sys_updt_tmstp AS DATE) >= DATE_SUB((SELECT extract_start_dt
        FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')), INTERVAL 5 DAY)
     AND CAST(dw_sys_updt_tmstp AS DATE) < (SELECT extract_end_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER'))
    UNION ALL
    SELECT 'JWN_ORDER_POR_WRK' AS dq_object_name,
     'order_record_count_check' AS dq_metric_name,
     'JWN_CLARITY_ORDER' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     COUNT(DISTINCT order_num) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_por_wrk AS jopw
    WHERE dw_batch_date IS NOT NULL) AS t13);


-- DAG Complete Time


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date,
 dag_completion_timestamp, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT 'jwn_clarity_order_line_detail_fact_por_wrk' AS dq_object_name,
  'order_sla_check' AS dq_metric_name,
  'JWN_CLARITY_ORDER' AS subject_area_nm,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dag_completion_timestamp,
  'SLA' AS dq_metric_value_type,
  'F' AS is_sensitive,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp);