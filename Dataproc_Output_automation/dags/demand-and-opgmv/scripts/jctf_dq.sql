-- Zero Count Check
INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
  SUBSTR('tran_fct_zero_count_check', 1, 100) AS dq_metric_name,
  'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
  CAST(COUNT(*) AS FLOAT64) AS dq_metric_value,
  'TARGET' AS dq_metric_value_type,
  'F' AS is_sensitive,
  CURRENT_DATETIME('PST8PDT'),
  CURRENT_DATETIME('PST8PDT')
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact AS jctf
 WHERE dw_batch_date IS NOT NULL);


-- Order SOURCE-TARGET Count Check


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT dq_object_name,
  dq_metric_name,
  subject_area_nm,
  dw_batch_date,
  CAST(dq_metric_value AS FLOAT64) AS dq_metric_value,
  dq_metric_value_type,
  is_sensitive,
  CURRENT_DATETIME('PST8PDT'),
  CURRENT_DATETIME('PST8PDT')
 FROM (SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_order_record_count_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_por_wrk AS jopw
    WHERE dw_batch_date IS NOT NULL
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_order_record_count_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS jctw
    WHERE LOWER(record_source) = LOWER('O')
     AND dw_batch_date IS NOT NULL) AS t9);


-- Sales SOURCE-TARGET Count Check


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT dq_object_name,
  dq_metric_name,
  subject_area_nm,
  dw_batch_date,
  CAST(dq_metric_value AS FLOAT64) AS dq_metric_value,
  dq_metric_value_type,
  is_sensitive,
  CURRENT_DATETIME('PST8PDT'),
  CURRENT_DATETIME('PST8PDT')
 FROM (SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_sales_record_count_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
     DATE_SUB((SELECT dw_batch_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw ON jrlgpfv.global_tran_id = jctekw.global_tran_id
         AND jrlgpfv.line_item_seq_num = jctekw.line_item_seq_num AND LOWER(jctekw.status_code) = LOWER('+')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_sales_record_count_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
     COUNT(*) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'F' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS jctw
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw0 ON jctw.global_tran_id = jctekw0.global_tran_id
         AND jctw.transaction_line_seq_num = jctekw0.line_item_seq_num AND LOWER(jctekw0.status_code) = LOWER('+')
    WHERE LOWER(jctw.record_source) = LOWER('S')) AS t8);


-- Order SOURCE-TARGET Amount Check


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
 FROM (SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_order_line_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date,
     SUM(line_net_amt) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT') AS dm__operand7,
     CURRENT_DATETIME('PST8PDT') AS dm__operand8
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_order_por_wrk AS jopw
    WHERE CAST(dw_batch_date AS DATE) >= DATE_SUB((SELECT extract_start_dt
        FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')), INTERVAL 5 DAY)
     AND CAST(dw_batch_date AS DATE) < (SELECT extract_end_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT'))
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_order_line_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
     SUM(line_net_amt) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS jctw
    WHERE LOWER(record_source) = LOWER('O')
     AND dw_batch_date IS NOT NULL) AS t13);


-- Sales SOURCE-TARGET Amount Check


-- Line net amount


-- Cost of sales amount


-- Loyalty deferral amount


-- Giftcard expected breakage amount


-- Giftcard redeemed amount


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date
 , dq_metric_value, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT dq_object_name,
  dq_metric_name,
  subject_area_nm,
  dw_batch_date,
  dq_metric_value,
  dq_metric_value_type,
  is_sensitive,
  CURRENT_DATETIME('PST8PDT'),
  CURRENT_DATETIME('PST8PDT')
 FROM (SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_sales_line_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
     DATE_SUB((SELECT dw_batch_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
     SUM(jrlgpfv.line_net_usd_amt) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw ON jrlgpfv.global_tran_id = jctekw.global_tran_id
         AND jrlgpfv.line_item_seq_num = jctekw.line_item_seq_num AND LOWER(jctekw.status_code) = LOWER('+')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_sales_line_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
     SUM(jctw.line_net_usd_amt) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS jctw
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw0 ON jctw.global_tran_id = jctekw0.global_tran_id
         AND jctw.transaction_line_seq_num = jctekw0.line_item_seq_num AND LOWER(jctekw0.status_code) = LOWER('+')
    WHERE LOWER(jctw.record_source) = LOWER('S')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_cost_of_sales_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
     DATE_SUB((SELECT dw_batch_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
     SUM(jrlgpfv0.cost_of_sales_amt) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv0
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw1 ON jrlgpfv0.global_tran_id = jctekw1.global_tran_id
         AND jrlgpfv0.line_item_seq_num = jctekw1.line_item_seq_num AND LOWER(jctekw1.status_code) = LOWER('+')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_cost_of_sales_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
     SUM(jctw0.cost_of_sales_amt) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS jctw0
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw2 ON jctw0.global_tran_id = jctekw2.global_tran_id
         AND jctw0.transaction_line_seq_num = jctekw2.line_item_seq_num AND LOWER(jctekw2.status_code) = LOWER('+')
    WHERE LOWER(jctw0.record_source) = LOWER('S')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_loyalty_deferral_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
     DATE_SUB((SELECT dw_batch_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
     SUM(jrlgpfv1.loyalty_deferral_usd_amt) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv1
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw3 ON jrlgpfv1.global_tran_id = jctekw3.global_tran_id
         AND jrlgpfv1.line_item_seq_num = jctekw3.line_item_seq_num AND LOWER(jctekw3.status_code) = LOWER('+')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_loyalty_deferral_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
     SUM(jctw1.loyalty_deferral_usd_amt) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS jctw1
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw4 ON jctw1.global_tran_id = jctekw4.global_tran_id
         AND jctw1.transaction_line_seq_num = jctekw4.line_item_seq_num AND LOWER(jctekw4.status_code) = LOWER('+')
    WHERE LOWER(jctw1.record_source) = LOWER('S')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_gc_breakage_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
     DATE_SUB((SELECT dw_batch_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
     SUM(jrlgpfv2.gc_expected_breakage_usd_amt) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv2
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw5 ON jrlgpfv2.global_tran_id = jctekw5.global_tran_id
         AND jrlgpfv2.line_item_seq_num = jctekw5.line_item_seq_num AND LOWER(jctekw5.status_code) = LOWER('+')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_gc_breakage_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
     SUM(jctw2.gc_expected_breakage_usd_amt) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS jctw2
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw6 ON jctw2.global_tran_id = jctekw6.global_tran_id
         AND jctw2.transaction_line_seq_num = jctekw6.line_item_seq_num AND LOWER(jctekw6.status_code) = LOWER('+')
    WHERE LOWER(jctw2.record_source) = LOWER('S')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_gc_redeemed_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
     DATE_SUB((SELECT dw_batch_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 1 DAY) AS dw_batch_date,
     SUM(jrlgpfv3.gift_card_redeemed_usd_amt) AS dq_metric_value,
     'SOURCE' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS jrlgpfv3
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw7 ON jrlgpfv3.global_tran_id = jctekw7.global_tran_id
         AND jrlgpfv3.line_item_seq_num = jctekw7.line_item_seq_num AND LOWER(jctekw7.status_code) = LOWER('+')
    UNION ALL
    SELECT 'JWN_CLARITY_TRANSACTION_FACT' AS dq_object_name,
     SUBSTR('tran_fct_gc_redeemed_amount_check', 1, 100) AS dq_metric_name,
     'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
     SUM(jctw3.gift_card_redeemed_usd_amt) AS dq_metric_value,
     'TARGET' AS dq_metric_value_type,
     'T' AS is_sensitive,
     CURRENT_DATETIME('PST8PDT'),
     CURRENT_DATETIME('PST8PDT')
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_wrk AS jctw3
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS jctekw8 ON jctw3.global_tran_id = jctekw8.global_tran_id
         AND jctw3.transaction_line_seq_num = jctekw8.line_item_seq_num AND LOWER(jctekw8.status_code) = LOWER('+')
    WHERE LOWER(jctw3.record_source) = LOWER('S')) AS t44);


-- DAG Complete Time


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.dq_metrics_fact (dq_object_name, dq_metric_name, subject_area_nm, dw_batch_date,
 dag_completion_timestamp, dq_metric_value_type, is_sensitive, dw_record_load_tmstp, dw_record_updt_tmstp)
(SELECT 'jwn_clarity_transaction_fact' AS dq_object_name,
  'tran_fct_sla_check' AS dq_metric_name,
  'JWN_CLARITY_TRAN_FCT' AS subject_area_nm,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT')) AS dw_batch_date,
  CURRENT_DATETIME('PST8PDT') AS dag_completion_timestamp,
  'SLA' AS dq_metric_value_type,
  'F' AS is_sensitive,
  CURRENT_DATETIME('PST8PDT') AS dw_record_load_tmstp,
  CURRENT_DATETIME('PST8PDT') AS dw_record_updt_tmstp);