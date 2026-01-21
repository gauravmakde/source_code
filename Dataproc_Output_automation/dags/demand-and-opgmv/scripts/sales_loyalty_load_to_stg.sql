--Seed Loyalty Points from Loyalty Transaction table for a business date range




TRUNCATE TABLE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_wrk;

--If we can find a match to OpGMV transaction then post the points there, else we
--aggregate up all of the points accumulated for the day that do not match so that
--they can be assigned to Nordstrom Visa card transactions in the next step.
--Legitimate global_tran_id keys will never be negative


INSERT INTO {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_wrk (global_tran_id, business_day_date, total_points)
(SELECT COALESCE(ctw.global_tran_id, DATE_DIFF(DATE '2000-01-01', ltf.business_day_date, DAY)) AS global_tran_id,
  ltf.business_day_date,
  SUM(ltf.total_points2) AS total_points
 FROM (SELECT loyalty_transaction_key AS global_tran_id,
    business_day_date,
    SUM(ROUND(CAST(total_points AS NUMERIC), 6)) AS total_points2
   FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.loyalty_transaction_fact
   WHERE business_day_date IN (SELECT business_day_date
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_keys_wrk)
   GROUP BY global_tran_id,
    business_day_date
   HAVING total_points2 <> 0) AS ltf
  LEFT JOIN (SELECT DISTINCT global_tran_id,
    business_day_date
   FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw
   WHERE LOWER(record_source) = LOWER('S')
    AND LOWER(jwn_operational_gmv_ind) = LOWER('Y')
    AND business_day_date IN (SELECT business_day_date
      FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_keys_wrk)) AS ctw ON ltf.global_tran_id = ctw.global_tran_id
     AND ltf.business_day_date = ctw.business_day_date
 GROUP BY global_tran_id,
  ltf.business_day_date);

--Update the loyalty deffered and breakage amounts in the POR work table
--Negative number since expected future cost
--Join in Loyalty points where the ERTM transaction can be matched
--Spread the unmapped Loyalty points across Nordstrom Visa Card transactions on the same day
--Only update rows that need it



UPDATE {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_fct.jwn_retailtran_loyalty_giftcard_por_fact AS tgt 
SET
 loyalty_deferral_usd_amt = SRC.loyalty_deferral_usd_amt2 - (SRC.loyalty_deferral_usd_amt2 * 0.07),
 loyalty_deferral_amt = SRC.loyalty_deferral_amt2 - (SRC.loyalty_deferral_amt2 * 0.07),
 loyalty_breakage_usd_amt = (SRC.loyalty_deferral_usd_amt2 * 0.07),
 loyalty_breakage_amt = (SRC.loyalty_deferral_amt2 * 0.07),
 dw_batch_date = (SELECT dw_batch_dt
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
  WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')),
 dw_sys_load_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
 FROM (SELECT ctw.record_source
   ,
   ctw.line_item_seq_num,
   ctw.global_tran_id,
   ctw.business_day_date,
     (COALESCE(CASE
        WHEN (ROW_NUMBER() OVER (PARTITION BY ctw.global_tran_id, ctw.business_day_date ORDER BY ctw.line_item_seq_num)
          ) = 1
        THEN loyalty.total_points * 0.01
        ELSE 0
        END, 0) + COALESCE(CASE
        WHEN ext_loyalty.global_tran_id IS NOT NULL AND (ROW_NUMBER() OVER (PARTITION BY ctw.global_tran_id, ctw.business_day_date
              ORDER BY ctw.line_item_seq_num)) = 1
        THEN ext_loyalty.tran_points * 0.01
        ELSE 0
        END, 0)) * - 1 AS loyalty_deferral_usd_amt2, --Negative number since expected future cost
     (COALESCE(CASE
        WHEN (ROW_NUMBER() OVER (PARTITION BY ctw.global_tran_id, ctw.business_day_date ORDER BY ctw.line_item_seq_num)
          ) = 1
        THEN loyalty.total_points * 0.01
        ELSE 0
        END, 0) + COALESCE(CASE
        WHEN ext_loyalty.global_tran_id IS NOT NULL AND (ROW_NUMBER() OVER (PARTITION BY ctw.global_tran_id, ctw.business_day_date
              ORDER BY ctw.line_item_seq_num)) = 1
        THEN ext_loyalty.tran_points * 0.01
        ELSE 0
        END, 0)) * - 1 AS loyalty_deferral_amt2
  FROM {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS ctw
   INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_keys_wrk AS d ON ctw.business_day_date = d.business_day_date
   --Join in Loyalty points where the ERTM transaction can be matched 
   LEFT JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_wrk AS loyalty ON ctw.global_tran_id = loyalty.global_tran_id
      AND ctw.business_day_date = loyalty.business_day_date
   --Spread the unmapped Loyalty points across Nordstrom Visa Card transactions on the same day 
   LEFT JOIN (SELECT nord_visa.global_tran_id,
     nord_visa.business_day_date,
      ext_tran.total_points / (SUM(1) OVER (PARTITION BY nord_visa.business_day_date RANGE BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING)) AS tran_points
    FROM (SELECT DISTINCT rttf.global_tran_id,
       rttf.business_day_date
      FROM {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_tender_fact AS rttf
       INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_retailtran_loyalty_giftcard_por_fact_vw AS ctw0 ON rttf.global_tran_id
            = ctw0.global_tran_id AND rttf.business_day_date = ctw0.business_day_date AND LOWER(ctw0.record_source) =
          LOWER('S') AND LOWER(ctw0.jwn_operational_gmv_ind) = LOWER('Y')
       INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_keys_wrk AS d0 ON ctw0.business_day_date = d0.business_day_date
        
      WHERE LOWER(rttf.card_type_code) = LOWER('NV')
       AND rttf.tender_type_code IS NOT NULL) AS nord_visa
     INNER JOIN {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_loyalty_wrk AS ext_tran ON nord_visa.business_day_date =
       ext_tran.business_day_date 
       AND ext_tran.global_tran_id < 0) AS ext_loyalty 
       ON ctw.global_tran_id = ext_loyalty.global_tran_id
      AND ctw.business_day_date = ext_loyalty.business_day_date
  WHERE LOWER(ctw.record_source) = LOWER('S')
   AND LOWER(ctw.jwn_operational_gmv_ind) = LOWER('Y')
   AND LOWER(ctw.zero_value_unit_ind) = LOWER('N')
   AND (loyalty.global_tran_id IS NOT NULL OR ext_loyalty.global_tran_id IS NOT NULL)
  QUALIFY (COALESCE(CASE
        WHEN (ROW_NUMBER() OVER (PARTITION BY ctw.global_tran_id, ctw.business_day_date ORDER BY ctw.line_item_seq_num)
          ) = 1
        THEN loyalty.total_points * 0.01
        ELSE 0
        END, 0) + COALESCE(CASE
        WHEN ext_loyalty.global_tran_id IS NOT NULL AND (ROW_NUMBER() OVER (PARTITION BY ctw.global_tran_id, ctw.business_day_date
              ORDER BY ctw.line_item_seq_num)) = 1
        THEN ext_loyalty.tran_points * 0.01
        ELSE 0
        END, 0)) * - 1 <> 0) AS SRC  --Only update rows that need it
WHERE tgt.line_item_seq_num = SRC.line_item_seq_num 
AND LOWER(tgt.record_source) = LOWER(SRC.record_source) 
AND tgt.global_tran_id = SRC.global_tran_id 
AND tgt.business_day_date = SRC.business_day_date;