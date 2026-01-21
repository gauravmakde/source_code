--Build a list of ORDER keys that we want to load.

TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_order_keys_wrk;

INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_order_keys_wrk (order_num, order_date_pacific, dw_batch_date)
(SELECT order_num,
  order_date_pacific,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date
 FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.order_line_detail_fact
 WHERE order_date_pacific < CURRENT_DATE('PST8PDT') -- Avoid high-date rows and partial day records
  AND CAST(dw_sys_updt_tmstp AS DATE) >= DATE_SUB((SELECT extract_start_dt
     FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')), INTERVAL 5 DAY)
--AND CAST(dw_sys_updt_tmstp AS DATE) < (SELECT EXTRACT_END_DT FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_NAP_JWN_METRICS_BASE_VWS.ETL_BATCH_DT_LKUP WHERE INTERFACE_CODE='JWN_CLARITY_ORDER') --:begin_date AND :end_date
 GROUP BY order_num,
  order_date_pacific,
  dw_batch_date);