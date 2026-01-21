/*
Clarity tender type fact is a table used for item contribution data. The table creates a
concise data set to hold tender types for downstream processing
*/
--Delete records from work table



TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_tender_type_wrk;



INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_tender_type_wrk (transaction_id, global_tran_id,
 business_day_date, tender_type, tender_subtype)
(SELECT DISTINCT jctf.transaction_id,
  ttype.global_tran_id,
  ttype.business_day_date,
  ttype.tender_type,
  ttype.tender_subtype
 FROM (SELECT DISTINCT rttf.global_tran_id,
    rttf.business_day_date,
     CASE
     WHEN LOWER(rttf.tender_type_code) = LOWER('CREDIT_CARD')
     THEN SUBSTR('Credit Card', 1, 20)
     WHEN LOWER(rttf.tender_type_code) = LOWER('DEBIT_CARD') OR LOWER(rttf.card_type_code) = LOWER('NC') AND LOWER(rttf
         .card_subtype_code) IN (LOWER('MD'), LOWER('ND'), CAST(LOWER(NULL) AS STRING))
     THEN 'Debit Card'
     WHEN LOWER(rttf.tender_type_code) IN (LOWER('CA'), LOWER('CASH'))
     THEN 'Cash'
     WHEN LOWER(rttf.tender_type_code) = LOWER('NORDSTROM_NOTE')
     THEN 'Nordstrom Note'
     WHEN LOWER(rttf.tender_type_code) = LOWER('GIFT_CARD') OR LOWER(rttf.card_type_code) = LOWER('Interact')
     THEN 'Gift Card'
     WHEN LOWER(rttf.tender_type_code) = LOWER('CHECK')
     THEN 'Check'
     WHEN LOWER(rttf.tender_type_code) IN (LOWER('PAYPAL'), LOWER('PP'), LOWER('PAYPAL_BILLING_AGREEMENT'))
     THEN 'PayPal'
     WHEN LOWER(rttf.tender_type_code) = LOWER('AFTERPAY')
     THEN 'After Pay'
     WHEN LOWER(rttf.tender_type_code) = LOWER('AFFIRM')
     THEN 'Affirm'
     ELSE 'UNKNOWN_VALUE'
     END AS tender_type,
     CASE
     WHEN LOWER(rttf.card_type_code) = LOWER('AE')
     THEN SUBSTR('American Express', 1, 30)
     WHEN LOWER(rttf.card_type_code) = LOWER('DS')
     THEN 'Discover'
     WHEN LOWER(rttf.card_type_code) = LOWER('MC')
     THEN 'MasterCard'
     WHEN LOWER(rttf.card_type_code) IN (LOWER('VS'), LOWER('VC'))
     THEN 'Visa'
     WHEN LOWER(rttf.card_type_code) = LOWER('JC')
     THEN 'JCB'
     WHEN LOWER(rttf.card_type_code) = LOWER('NB')
     THEN 'Nordstrom Corporate Card'
     WHEN LOWER(rttf.card_type_code) = LOWER('NC') AND LOWER(rttf.card_subtype_code) IN (LOWER('MD'), LOWER('ND'), CAST(LOWER(NULL) AS STRING)
        )
     THEN 'Nordstrom Debit Card'
     WHEN LOWER(rttf.card_type_code) = LOWER('NC') AND LOWER(rttf.card_subtype_code) IN (LOWER('RR'), LOWER('RT'), LOWER('TR'
         ))
     THEN 'Nordstrom Retail Card'
     WHEN LOWER(rttf.card_type_code) = LOWER('NV')
     THEN 'Nordstrom Visa'
     WHEN LOWER(rttf.card_type_code) = LOWER('NG')
     THEN 'Nordstrom Gift Card'
     WHEN LOWER(rttf.tender_type_code) = LOWER('CASH')
     THEN 'Cash'
     WHEN LOWER(rttf.tender_type_code) = LOWER('CHECK')
     THEN 'Check'
     WHEN LOWER(rttf.tender_type_code) = LOWER('CREDIT_CARD')
     THEN 'Credit Card'
     WHEN LOWER(rttf.tender_type_code) = LOWER('DEBIT_CARD')
     THEN 'Debit Card'
     WHEN LOWER(rttf.tender_type_code) = LOWER('NORDSTROM_NOTE')
     THEN 'Nordstrom Note'
     WHEN LOWER(rttf.tender_type_code) = LOWER('GIFT_CARD') AND LOWER(rttf.card_type_code) IN (LOWER('Interact'), LOWER('UN'
         ), CAST(LOWER(NULL) AS STRING))
     THEN 'Third Party Gift Card'
     WHEN LOWER(rttf.tender_type_code) IN (LOWER('PAYPAL'), LOWER('PP'), LOWER('PAYPAL_BILLING_AGREEMENT'))
     THEN 'PayPal'
     WHEN LOWER(rttf.tender_type_code) = LOWER('AFTERPAY')
     THEN 'After Pay'
     WHEN LOWER(rttf.tender_type_code) = LOWER('AFFIRM')
     THEN 'Affirm'
     ELSE 'UNKNOWN_VALUE'
     END AS tender_subtype
   FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_tender_fact AS rttf
    INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_tran_ertm_keys_wrk AS keys 
    ON rttf.global_tran_id = keys.global_tran_id
        AND rttf.business_day_date = keys.business_day_date AND LOWER(keys.status_code) = LOWER('+')
   WHERE rttf.tender_type_code IS NOT NULL) AS ttype
  INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact AS jctf ON ttype.global_tran_id = jctf.global_tran_id
      AND ttype.business_day_date = jctf.business_day_date AND LOWER(jctf.record_source) = LOWER('S'));


--Push delta records to target table.


--Purge out any ERTM records that are no longer keyed to a visible transaction.


DELETE FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_tran_tender_type_fact AS tgt
WHERE EXISTS (SELECT 1
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_tran_ertm_keys_wrk AS kys
 WHERE tgt.global_tran_id = global_tran_id
  AND tgt.business_day_date = business_day_date
  AND LOWER(status_code) = LOWER('-'));


--Delete followed by re-insert of any ERTM keys in the latest load window


DELETE FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_tran_tender_type_fact AS tgt
WHERE EXISTS (SELECT 1
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_tran_tender_type_wrk AS wrk
 WHERE LOWER(tgt.transaction_id) = LOWER(transaction_id)
  AND tgt.global_tran_id = global_tran_id
  AND tgt.business_day_date = business_day_date);


--Insert into final tender type fact table


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_clarity_tran_tender_type_fact
 (transaction_id, global_tran_id,
 business_day_date, tender_type, tender_subtype, dw_batch_date)
(SELECT transaction_id,
  global_tran_id,
  business_day_date,
  tender_type,
  tender_subtype,
   (SELECT dw_batch_dt
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_TRAN_FCT'))
 FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_tran_tender_type_wrk);