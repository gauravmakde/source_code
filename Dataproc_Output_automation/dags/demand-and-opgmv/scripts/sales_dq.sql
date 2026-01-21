

-- Zero Count Check

INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT 'JWN_RETAILTRAN_LOYALTY_GIFTCARD_POR_FACT' AS dq_object_name,
  'sales_zero_count_check' AS dq_metric_name,
  'JWN_CLARITY_SALES' AS subject_area_nm,
  DATE_SUB((SELECT dw_batch_dt
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
  CAST(COUNT(*) AS FLOAT64) AS dq_metric_value,
  'TARGET' AS dq_metric_value_type,
  'F' AS is_sensitive,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv
 WHERE dw_batch_date = (SELECT extract_start_dt
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')));


-- SOURCE-TARGET Count Check


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT dq_object_name,
  dq_metric_name,
  subject_area_nm,
  dw_batch_date,
  CAST(dq_metric_value AS FLOAT64) AS dq_metric_value,
  dq_metric_value_type,
  is_sensitive,
  dw_record_load_tmstp,
  dw_record_updt_tmstp
 FROM (SELECT 'JWN_RETAILTRAN_LOYALTY_GIFTCARD_POR_FACT' AS dq_object_name,
     'sales_record_count_check' AS dq_metric_name,
     'JWN_CLARITY_SALES' AS subject_area_nm,
     DATE_SUB((SELECT dw_batch_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp
    FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_detail_fact AS rtdf
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw ON rtdf.global_tran_id = jctekw.global_tran_id
        AND rtdf.line_item_seq_num = jctekw.line_item_seq_num
    WHERE rtdf.business_day_date <> CURRENT_DATE('PST8PDT')
     AND LOWER(rtdf.tran_latest_version_ind) = LOWER('Y')
     AND LOWER(rtdf.error_flag) = LOWER('N')
     AND LOWER(rtdf.tran_type_code) IN (LOWER('EXCH'), LOWER('SALE'), LOWER('RETN'))
    UNION ALL
    SELECT 'JWN_RETAILTRAN_LOYALTY_GIFTCARD_POR_FACT' AS dq_object_name,
     'sales_record_count_check' AS dq_metric_name,
     'JWN_CLARITY_SALES' AS subject_area_nm,
     DATE_SUB((SELECT dw_batch_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw0 ON jrlgpfv.global_tran_id = jctekw0.global_tran_id
        AND jrlgpfv.line_item_seq_num = jctekw0.line_item_seq_num) AS t8);


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact
(
dq_object_name,
dq_metric_name,
subject_area_nm,
dw_batch_date,
dq_metric_value,
dq_metric_value_type,
is_sensitive,
dw_record_load_tmstp,
dw_record_updt_tmstp
)
WITH diff AS (SELECT SUM(net_amount) AS net_amount
FROM (SELECT SUM(rtdf.line_net_amt) AS net_amount
   FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.jwn_retail_tran_hdr_detail_fact_vw AS rtdf
    INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw ON rtdf.global_tran_id = jctekw.global_tran_id
        AND rtdf.line_item_seq_num = jctekw.line_item_seq_num AND LOWER(jctekw.status_code) = LOWER('+')
   UNION ALL
   SELECT SUM(- rtdf0.line_net_amt) AS net_amount
   FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.jwn_retail_tran_hdr_detail_fact_vw AS rtdf0
    INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw0 ON rtdf0.global_tran_id = jctekw0.global_tran_id
        AND rtdf0.line_item_seq_num = jctekw0.line_item_seq_num AND LOWER(jctekw0.status_code) = LOWER('+')
    LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.store_dim AS sd ON rtdf0.intent_store_num = sd.store_num
    LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.demand_product_detail_dim_vw AS sku ON LOWER(sku.rms_sku_num) = LOWER(CASE
          WHEN LOWER(rtdf0.rms_sku_num) <> LOWER('')
          THEN rtdf0.rms_sku_num
          ELSE '@' || TRIM(FORMAT('%20d', rtdf0.global_tran_id))
          END) AND (LOWER(sku.channel_country) = LOWER(sd.store_country_code) OR sku.country_count = 1) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf0.business_day_date AS DATETIME)) AS DATETIME)
       < CAST(sku.eff_end_tmstp AS DATETIME) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf0.business_day_date AS DATETIME)
        ) AS DATETIME) >= CAST(sku.eff_begin_tmstp AS DATETIME)
    LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.department_dim_hist AS dept ON dept.dept_num = CAST(rtdf0.merch_dept_num AS FLOAT64) AND CAST(dept.eff_begin_tmstp AS DATETIME)
       <= COALESCE(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf0.original_business_date AS DATETIME)) AS DATETIME),
        rtdf0.tran_time) AND CAST(dept.eff_end_tmstp AS DATETIME) > COALESCE(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(rtdf0.original_business_date AS DATETIME)
         ) AS DATETIME), rtdf0.tran_time)
   WHERE COALESCE(sku.division_num, dept.division_num) = 800 AND sku.class_num IN (90, 91, 92) AND COALESCE(sku.dept_num
       , CAST(TRIM(rtdf0.merch_dept_num) AS SMALLINT)) NOT IN (780, 695)
    OR LOWER(rtdf0.fee_code_desc) LIKE LOWER('%GIFT CARD%')
   UNION ALL
   SELECT SUM(rtdf1.line_net_amt) AS net_amount
   FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.jwn_retail_tran_hdr_detail_fact_vw AS rtdf1
    INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw1 ON rtdf1.global_tran_id = jctekw1.global_tran_id
        AND rtdf1.line_item_seq_num = jctekw1.line_item_seq_num AND LOWER(jctekw1.status_code) = LOWER('+')
   WHERE LOWER(rtdf1.fee_code_desc) LIKE LOWER('%GIFT CARD%')
    AND LOWER(rtdf1.fee_type_code) LIKE LOWER('%SH%')) AS line_net_amt)
SELECT 'JWN_RETAILTRAN_LOYALTY_GIFTCARD_POR_FACT' AS dq_object_name,
 'sales_record_amount_check' AS dq_metric_name,
 'JWN_CLARITY_SALES' AS subject_area_nm,
 DATE_SUB((SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
 net_amount AS dq_metric_value,
 'SOURCE' AS dq_metric_value_type,
 'T' AS is_sensitive,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp
FROM diff
UNION ALL
SELECT 'JWN_RETAILTRAN_LOYALTY_GIFTCARD_POR_FACT' AS dq_object_name,
 'sales_record_amount_check' AS dq_metric_name,
 'JWN_CLARITY_SALES' AS subject_area_nm,
 DATE_SUB((SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
 SUM(jrlgpfv.line_net_amt) AS dq_metric_value,
 'TARGET' AS dq_metric_value_type,
 'T' AS is_sensitive,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp
FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv
 INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw ON jrlgpfv.global_tran_id = jctekw.global_tran_id
     AND jrlgpfv.line_item_seq_num = jctekw.line_item_seq_num AND LOWER(jctekw.status_code) = LOWER('+');

-- DAG Complete Time


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date,
 dag_completion_timestamp, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT 'clarity_sales' AS dq_object_name,
  'sales_sla_check' AS dq_metric_name,
  'JWN_CLARITY_SALES' AS subject_area_nm,
  DATE_SUB((SELECT dw_batch_dt
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dag_completion_timestamp,
  'SLA' AS dq_metric_value_type,
  'F' AS is_sensitive,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp);