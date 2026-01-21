
TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk;

INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk
 (global_tran_id, business_day_date, line_item_seq_num
 , status_code, dw_batch_date)
(SELECT *
 FROM (SELECT DISTINCT lcf.global_tran_id,
     lcf.business_day_date,
     lcf.line_item_seq_num,
     RPAD('-', 1, ' ') AS status_code,
      (SELECT dw_batch_dt
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')) AS dw_batch_date
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS lcf
     LEFT JOIN {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_hdr_fact_vw AS rtf ON lcf.global_tran_id = rtf.global_tran_id AND lcf.business_day_date
        = rtf.business_day_date
    WHERE LOWER(lcf.record_source) = LOWER('S')
     AND rtf.global_tran_id IS NULL
    UNION DISTINCT
    SELECT DISTINCT global_tran_id,
     business_day_date,
     line_item_seq_num,
     RPAD('+', 1, ' ') AS status_code,
     dw_batch_date
    FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_detail_fact_vw
    WHERE business_day_date < CURRENT_DATE('PST8PDT')
     AND dw_batch_date BETWEEN DATE_SUB((SELECT extract_start_dt
        FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
        WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 5 DAY) AND (SELECT extract_start_dt
       FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
       WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES'))) AS t13);


TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_gcb_keys_wrk;


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_gcb_keys_wrk (tran_date)
(SELECT DISTINCT tran_date
 FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.jwn_gift_card_breakage_vw
 WHERE CAST(dw_sys_updt_tmstp AS DATE) BETWEEN DATE_SUB((SELECT extract_start_dt
     FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 5 DAY) AND 
     (SELECT extract_start_dt
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES'))
  AND tran_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 120 DAY));


TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_keys_wrk;


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_keys_wrk (business_day_date)
(SELECT DISTINCT business_day_date
 FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.loyalty_transaction_fact
 WHERE CAST(dw_sys_load_tmstp AS DATE) BETWEEN DATE_SUB((SELECT extract_start_dt
     FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
     WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')), INTERVAL 5 DAY) AND (SELECT extract_start_dt
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
    WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES'))
  AND business_day_date IS NOT NULL);

--COLLECT STATISTICS COLUMN(business_day_date) ON {{params.DBJWNENV}}_NAP_JWN_METRICS_STG.JWN_CLARITY_TRAN_LOYALTY_KEYS_WRK;

