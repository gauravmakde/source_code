TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_wrk;


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_wrk (tran_date, tran_amt_currency_code,
 gift_card_assumption_rate, gift_card_issued_amt, gift_card_reload_adj_amt, gift_card_redeemed_amt, total_tran_amt,
 gift_card_issued_usd_amt, gift_card_reload_adj_usd_amt, gift_card_redeemed_usd_amt, total_tran_usd_amt)
(SELECT tran_date,
  tran_amt_currency_code,
  CAST(MAX(gift_card_assumption_rate) AS NUMERIC) AS gift_card_assumption_rate,
  CAST(SUM(gift_card_issued_amt) AS NUMERIC) AS gift_card_issued_amt,
  CAST(SUM(gift_card_reload_adj_amt) AS NUMERIC) AS gift_card_reload_adj_amt,
  CAST(SUM(gift_card_redeemed_amt) AS NUMERIC) AS gift_card_redeemed_amt,
  SUM(tran_amt) AS total_tran_amt,
  CAST(SUM(gift_card_issued_usd_amt) AS NUMERIC) AS gift_card_issued_usd_amt,
  CAST(SUM(gift_card_reload_adj_usd_amt) AS NUMERIC) AS gift_card_reload_adj_usd_amt,
  CAST(SUM(gift_card_redeemed_usd_amt) AS NUMERIC) AS gift_card_redeemed_usd_amt,
  SUM(tran_usd_amt) AS total_tran_usd_amt
 FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.jwn_gift_card_breakage_vw
 WHERE tran_date IN (SELECT tran_date
    FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_gcb_keys_wrk)
 GROUP BY tran_date,
  tran_amt_currency_code);


TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_gmv_wrk;




INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_gmv_wrk (tran_date, tran_amt_currency_code,
 total_gmv_usd_amt, total_gmv_amt)
(SELECT ctw.ertm_tran_date,
  ctw.line_item_currency_code,
  SUM(ctw.operational_gmv_usd_amt) AS total_gmv_usd_amt,
  SUM(ctw.operational_gmv_amt) AS total_gmv_amt
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS ctw
  INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_wrk AS gcb ON ctw.ertm_tran_date = gcb.tran_date 
  AND LOWER(ctw.line_item_currency_code) = LOWER(gcb.tran_amt_currency_code)
  WHERE LOWER(ctw.record_source) = LOWER('S')
  AND LOWER(ctw.jwn_operational_gmv_ind) = LOWER('Y')
  AND LOWER(ctw.rms_sku_num) <> LOWER('')
  AND ctw.business_day_date >=(SELECT DATE_SUB(MIN(tran_date), INTERVAL 1 DAY) FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_wrk)
 GROUP BY ctw.line_item_currency_code,
  ctw.ertm_tran_date
 HAVING total_gmv_amt <> 0);



UPDATE
  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_retailtran_loyalty_giftcard_por_fact AS tgt
SET
  gift_card_issued_usd_amt = SRC.gift_card_issued_usd_amt,
  gift_card_issued_amt = SRC.gift_card_issued_amt,
  gift_card_reload_adj_usd_amt = SRC.gift_card_reload_adj_usd_amt,
  gift_card_reload_adj_amt = SRC.gift_card_reload_adj_amt,
  gc_expected_breakage_usd_amt = (SRC.gift_card_issued_usd_amt + SRC.gift_card_reload_adj_usd_amt) * SRC.gift_card_assumption_rate,
  gc_expected_breakage_amt = (SRC.gift_card_issued_amt + SRC.gift_card_reload_adj_amt) * SRC.gift_card_assumption_rate,
  gift_card_redeemed_usd_amt = SRC.gift_card_redeemed_usd_amt,
  gift_card_redeemed_amt = SRC.gift_card_redeemed_amt,
  dw_batch_date = (
  SELECT
    dw_batch_dt
  FROM
    {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
  WHERE
    LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')),
  dw_sys_load_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
FROM (
  SELECT
    ctw.record_source,
    ctw.line_item_seq_num,
    ctw.global_tran_id,
    ctw.line_item_currency_code,
    ctw.business_day_date,
    ctw.ertm_tran_date,
    ctw.tran_type_code,
    ctw.rms_sku_num,
    ctw.jwn_operational_gmv_ind,
    gcb.gift_card_assumption_rate,
    gcb.gift_card_issued_amt * ctw.operational_gmv_amt / gmv.total_gmv_amt AS gift_card_issued_amt,
    gcb.gift_card_reload_adj_amt * ctw.operational_gmv_amt / gmv.total_gmv_amt AS gift_card_reload_adj_amt,
    gcb.gift_card_redeemed_amt * ctw.operational_gmv_amt / gmv.total_gmv_amt AS gift_card_redeemed_amt,
    gcb.total_tran_amt * ctw.operational_gmv_amt / gmv.total_gmv_amt AS total_tran_amt,
    gcb.gift_card_issued_usd_amt * ctw.operational_gmv_usd_amt / gmv.total_gmv_usd_amt AS gift_card_issued_usd_amt,
    gcb.gift_card_reload_adj_usd_amt * ctw.operational_gmv_usd_amt / gmv.total_gmv_usd_amt AS gift_card_reload_adj_usd_amt,
    gcb.gift_card_redeemed_usd_amt * ctw.operational_gmv_usd_amt / gmv.total_gmv_usd_amt AS gift_card_redeemed_usd_amt,
    gcb.total_tran_usd_amt * ctw.operational_gmv_usd_amt / gmv.total_gmv_usd_amt AS total_tran_usd_amt
  FROM
    {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_wrk AS gcb
  INNER JOIN
    {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_gmv_wrk AS gmv
  ON
    gcb.tran_date = gmv.tran_date
    AND LOWER(gmv.tran_amt_currency_code) = LOWER(gcb.tran_amt_currency_code)
  INNER JOIN
    {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS ctw
  ON
    gcb.tran_date = ctw.ertm_tran_date
    AND LOWER(ctw.line_item_currency_code) = LOWER(gcb.tran_amt_currency_code)
  WHERE
    LOWER(ctw.record_source) = LOWER('S')
    AND LOWER(ctw.jwn_operational_gmv_ind) = LOWER('Y')
    AND LOWER(ctw.rms_sku_num) <> LOWER('')
    AND ctw.business_day_date >= (
    SELECT
      DATE_SUB(MIN(tran_date), INTERVAL 1 DAY)
    FROM
      {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_gift_card_breakage_wrk)) AS SRC
WHERE
  tgt.line_item_seq_num = SRC.line_item_seq_num
  AND LOWER(tgt.record_source) = LOWER(SRC.record_source)
  AND tgt.global_tran_id = SRC.global_tran_id
  AND LOWER(tgt.line_item_currency_code) = LOWER(SRC.line_item_currency_code)
  AND tgt.business_day_date = SRC.business_day_date
  AND LOWER(tgt.tran_type_code) = LOWER(SRC.tran_type_code)
  AND tgt.ertm_tran_date = SRC.ertm_tran_date
  AND LOWER(tgt.rms_sku_num) = LOWER(SRC.rms_sku_num)
  AND LOWER(tgt.jwn_operational_gmv_ind) = LOWER(SRC.jwn_operational_gmv_ind )
  AND (tgt.gift_card_issued_usd_amt <> SRC.gift_card_issued_usd_amt
    OR tgt.gift_card_issued_amt <> SRC.gift_card_issued_amt
    OR tgt.gift_card_reload_adj_usd_amt <> SRC.gift_card_reload_adj_usd_amt
    OR tgt.gift_card_reload_adj_amt <> SRC .gift_card_reload_adj_amt
    OR tgt.gift_card_redeemed_usd_amt <> SRC.gift_card_redeemed_usd_amt
    OR tgt.gift_card_redeemed_amt <> SRC.gift_card_redeemed_amt);