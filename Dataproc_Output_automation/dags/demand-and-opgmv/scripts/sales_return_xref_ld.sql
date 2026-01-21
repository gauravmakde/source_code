-- BT;
/*This SQl loads the Sales return xref table which is meant
to aid in providing an alignment between sales and returns in the final transaction fact tables*/

--If any ERTM sales transactions had their key voided then NULL out those pointers in the table.
--Under normal conditions, we would expect that the replacement keys will be part of normal
--delta processing.

UPDATE   {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_dim.jwn_clarity_return_sale_xref AS tgt SET
    sale_global_tran_id = NULL,
    sale_line_item_seq_num = NULL,
    sale_business_day_date = NULL 
	FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS k
WHERE k.global_tran_id = tgt.sale_global_tran_id AND k.business_day_date = tgt.sale_business_day_date AND k.line_item_seq_num = tgt.sale_line_item_seq_num AND LOWER(k.status_code) = LOWER('-');


--Remove all ERTM return transactions that we will either process as "new" or where their key
--was voided so that we don't accidentally map keys redundantly.


DELETE FROM   {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_dim.jwn_clarity_return_sale_xref
WHERE (return_global_tran_id, return_business_day_date, return_line_item_seq_num) IN (SELECT (global_tran_id, business_day_date, line_item_seq_num)
        FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk);

--Insert the to be aligned sales and returns into the work table

TRUNCATE TABLE  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_return_sale_xref_wrk;

INSERT INTO   {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_return_sale_xref_wrk 
(return_global_tran_id,
 return_line_item_seq_num, 
 return_business_day_date, 
 order_num,
 rms_sku_num,
 tran_type_code,
line_item_activity_type_code,
nonmerch_fee_code, 
tran_time,
tran_time_tz, 
line_net_amt, 
line_item_currency_code,
original_line_item_amt,
original_line_item_amt_currency_code,
original_transaction_identifier, 
original_business_date,
original_ringing_store_num, 
original_register_num, 
original_tran_num, 
sale_global_tran_id)
(SELECT dtl.global_tran_id,
  dtl.line_item_seq_num,
  dtl.business_day_date,
  hdr.order_num,
   CASE
   WHEN LOWER(dtl.rms_sku_num) <> LOWER('')
   THEN dtl.rms_sku_num
   WHEN LOWER(SUBSTR(dtl.sku_num, 1, 1)) = LOWER('M')
   THEN dtl.sku_num
   ELSE NULL
   END AS rms_sku_num2,
  dtl.tran_type_code,
  dtl.line_item_activity_type_code,
  dtl.nonmerch_fee_code,
  hdr.tran_time_utc,
  hdr.tran_time_tz,
  dtl.line_net_amt,
  dtl.line_item_net_amt_currency_code AS line_item_currency_code,
  dtl.original_line_item_amt,
  COALESCE(dtl.original_line_item_amt_currency_code, dtl.line_item_net_amt_currency_code) AS original_line_item_amt_currency_code,
  dtl.original_transaction_identifier,
  dtl.original_business_date,
  dtl.original_ringing_store_num,
  dtl.original_register_num,
  ROUND(CAST(dtl.original_tran_num AS NUMERIC), 0) AS original_tran_num,
  hdrs.global_tran_id AS sale_global_tran_id
 FROM  {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_hdr_fact AS hdr
  INNER JOIN  {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_detail_fact AS dtl ON hdr.global_tran_id = dtl.global_tran_id AND hdr.business_day_date
     = dtl.business_day_date
  INNER JOIN (SELECT global_tran_id,
     business_day_date,
     line_item_seq_num
    FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk
    WHERE LOWER(status_code) = LOWER('+')
    UNION DISTINCT
    SELECT return_global_tran_id,
     return_business_day_date,
     return_line_item_seq_num
    FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_return_sale_xref
    WHERE sale_global_tran_id IS NULL) AS rtrn ON dtl.global_tran_id = rtrn.global_tran_id AND dtl.business_day_date =
     rtrn.business_day_date AND dtl.line_item_seq_num = rtrn.line_item_seq_num
  INNER JOIN  {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_hdr_fact AS hdrs ON LOWER(hdrs.transaction_identifier) = LOWER(dtl.original_transaction_identifier
       ) AND dtl.original_business_date = hdrs.business_day_date AND LOWER(hdrs.tran_latest_version_ind) = LOWER('Y')
   AND LOWER(hdrs.tran_type_code) IN (LOWER('EXCH'), LOWER('SALE'))
 WHERE LOWER(dtl.tran_latest_version_ind) = LOWER('Y')
  AND LOWER(hdr.tran_latest_version_ind) = LOWER('Y')
  AND LOWER(dtl.error_flag) = LOWER('N')
  AND LOWER(dtl.tran_type_code) IN (LOWER('EXCH'), LOWER('RETN'))
  AND dtl.original_transaction_identifier IS NOT NULL
  AND dtl.original_business_date IS NOT NULL
  AND dtl.original_ringing_store_num IS NOT NULL);


--This  MERGE INTO statement pairs up any identified Return records with corresponding Sales and
--then uses a QUALIFY clause to whittle down the potential matches to a precise pairing of line items.



MERGE INTO  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_dim.jwn_clarity_return_sale_xref AS tgt
USING (SELECT CASE WHEN LOWER(s.order_num) <> LOWER('') THEN SUBSTR(s.order_num, 1, 64) ELSE TRIM(FORMAT('%2d', s.ringing_store_num)) || '_' || COALESCE(TRIM(FORMAT('%2.0f', s.register_num)), 'sdm') || '_' || COALESCE(TRIM(FORMAT('%2.0f', s.tran_num)), 'sdm') || '_' || FORMAT_DATE('%F', s.business_day_date) END AS transaction_id, r.return_global_tran_id, r.return_line_item_seq_num, r.return_business_day_date, r.tran_type_code, r.line_item_activity_type_code, s.order_num, s.rms_sku_num2 AS rms_sku_num, s.nonmerch_fee_code, s.transaction_identifier, s.ringing_store_num, s.register_num, s.tran_num, s.line_item_currency_code, s.global_tran_id AS sale_global_tran_id, s.line_item_seq_num AS sale_line_item_seq_num, s.business_day_date AS sale_business_day_date, s.sale_tran_date, CURRENT_DATE AS dw_batch_date
    FROM (SELECT return_global_tran_id, return_line_item_seq_num, return_business_day_date, order_num, rms_sku_num, tran_type_code, line_item_activity_type_code, nonmerch_fee_code, tran_time, line_net_amt, line_item_currency_code, original_line_item_amt, original_line_item_amt_currency_code, original_transaction_identifier, original_business_date, original_ringing_store_num, original_register_num, original_tran_num, sale_global_tran_id, ROW_NUMBER() OVER (PARTITION BY original_transaction_identifier, original_ringing_store_num, CAST(original_register_num AS STRING), CAST(original_tran_num AS STRING), line_item_activity_type_code, CASE WHEN LOWER(line_item_activity_type_code) IN (LOWER('R'), LOWER('S')) THEN rms_sku_num ELSE nonmerch_fee_code END ORDER BY tran_time, ABS(line_net_amt), return_business_day_date, return_global_tran_id, return_line_item_seq_num) AS seq_num
            FROM  {{params.bq_project_id}}.{{params.DBENV}}_nap_jwn_metrics_stg.jwn_clarity_return_sale_xref_wrk) AS r
        INNER JOIN (SELECT dtl.global_tran_id, dtl.line_item_seq_num, dtl.business_day_date, hdr.order_num, hdr.tran_date AS sale_tran_date, CASE WHEN LOWER(dtl.rms_sku_num) <> LOWER('') THEN dtl.rms_sku_num WHEN LOWER(SUBSTR(dtl.sku_num, 1, 1)) = LOWER('M') THEN dtl.sku_num ELSE NULL END AS rms_sku_num2, dtl.line_item_activity_type_code, dtl.nonmerch_fee_code, dtl.line_net_amt, dtl.line_item_net_amt_currency_code AS line_item_currency_code, dtl.employee_discount_amt, hdr.transaction_identifier, hdr.ringing_store_num, hdr.register_num, hdr.tran_num, ROW_NUMBER() OVER (PARTITION BY hdr.transaction_identifier, hdr.ringing_store_num, CAST(hdr.register_num AS STRING), CAST(hdr.tran_num AS STRING), dtl.line_item_activity_type_code, CASE WHEN LOWER(dtl.line_item_activity_type_code) IN (LOWER('R'), LOWER('S')) THEN dtl.rms_sku_num ELSE dtl.nonmerch_fee_code END ORDER BY hdr.tran_time, ABS(dtl.line_net_amt), dtl.business_day_date, dtl.global_tran_id, dtl.line_item_seq_num) AS seq_num
            FROM  {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_hdr_fact AS hdr
                INNER JOIN (SELECT DISTINCT sale_global_tran_id, original_business_date
                    FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_return_sale_xref_wrk) AS k ON hdr.global_tran_id = k.sale_global_tran_id AND hdr.business_day_date = k.original_business_date
                INNER JOIN  {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_detail_fact AS dtl ON hdr.global_tran_id = dtl.global_tran_id AND hdr.business_day_date = dtl.business_day_date
            WHERE LOWER(dtl.tran_latest_version_ind) = LOWER('Y') AND LOWER(hdr.tran_latest_version_ind) = LOWER('Y') AND LOWER(dtl.error_flag) = LOWER('N') AND LOWER(dtl.tran_type_code) IN (LOWER('EXCH'), LOWER('SALE')) AND LOWER(dtl.line_item_activity_type_code) <> LOWER('R') AND NOT EXISTS (SELECT 1
                    FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_return_sale_xref AS xref
                    WHERE sale_global_tran_id = dtl.global_tran_id AND sale_line_item_seq_num = dtl.line_item_seq_num AND sale_business_day_date = dtl.business_day_date)) AS s ON LOWER(r.original_transaction_identifier) = LOWER(s.transaction_identifier) AND r.original_business_date = s.business_day_date AND LOWER(r.original_line_item_amt_currency_code) = LOWER(s.line_item_currency_code) AND (LOWER(r.rms_sku_num) = LOWER(s.rms_sku_num2) AND LOWER(r.line_item_activity_type_code) IN (LOWER('R'), LOWER('S')) AND LOWER(s.line_item_activity_type_code) = LOWER('S') OR LOWER(r.nonmerch_fee_code) = LOWER(s.nonmerch_fee_code) AND LOWER(r.line_item_activity_type_code) = LOWER('N') AND LOWER(s.line_item_activity_type_code) = LOWER('N')) AND (r.original_line_item_amt = s.line_net_amt OR r.original_line_item_amt = s.line_net_amt + ABS(COALESCE(s.employee_discount_amt, 0)) OR r.original_line_item_amt = 0 AND - 1 * r.line_net_amt = s.line_net_amt)
    QUALIFY (DENSE_RANK() OVER (PARTITION BY s.transaction_identifier, s.ringing_store_num, CAST(s.register_num AS STRING), CAST(s.tran_num AS STRING), CASE WHEN LOWER(s.line_item_activity_type_code) IN (LOWER('R'), LOWER('S')) THEN s.rms_sku_num2 ELSE s.nonmerch_fee_code END, s.line_item_activity_type_code, s.seq_num ORDER BY r.seq_num)) = (ROW_NUMBER() OVER (PARTITION BY s.transaction_identifier, s.ringing_store_num, CAST(s.register_num AS STRING), CAST(s.tran_num AS STRING), CASE WHEN LOWER(s.line_item_activity_type_code) IN (LOWER('R'), LOWER('S')) THEN s.rms_sku_num2 ELSE s.nonmerch_fee_code END, s.line_item_activity_type_code, r.seq_num ORDER BY s.seq_num))) AS SRC
ON LOWER(tgt.transaction_id) = LOWER(SRC.transaction_id) AND tgt.return_global_tran_id = SRC.return_global_tran_id AND tgt.return_line_item_seq_num = SRC.return_line_item_seq_num
WHEN MATCHED THEN UPDATE SET
    return_business_day_date = SRC.return_business_day_date,
    tran_type_code = SRC.tran_type_code,
    line_item_activity_type_code = SRC.line_item_activity_type_code,
    order_num = SRC.order_num,
    rms_sku_num = SRC.rms_sku_num,
    nonmerch_fee_code = SRC.nonmerch_fee_code,
    transaction_identifier = SRC.transaction_id,
    ringing_store_num = SRC.ringing_store_num,
    register_num = SRC.register_num,
    tran_num = SRC.tran_num,
    line_item_currency_code = SRC.line_item_currency_code,
    sale_global_tran_id = SRC.sale_global_tran_id,
    sale_line_item_seq_num = SRC.sale_line_item_seq_num,
    sale_business_day_date = SRC.sale_business_day_date,
    sale_tran_date = SRC.sale_tran_date,
    dw_sys_updt_tmstp = current_datetime('PST8PDT')
WHEN NOT MATCHED THEN INSERT (transaction_id, return_global_tran_id, return_line_item_seq_num, return_business_day_date, tran_type_code, line_item_activity_type_code, order_num, rms_sku_num, nonmerch_fee_code, transaction_identifier, ringing_store_num, register_num, tran_num, line_item_currency_code, sale_global_tran_id, sale_line_item_seq_num, sale_business_day_date, sale_tran_date, dw_batch_date) VALUES(SRC.transaction_id, SRC.return_global_tran_id, SRC.return_line_item_seq_num, SRC.return_business_day_date, SRC.tran_type_code, SRC.line_item_activity_type_code, SRC.order_num, SRC.rms_sku_num, SRC.nonmerch_fee_code, SRC.transaction_identifier, SRC.ringing_store_num, SRC.register_num, SRC.tran_num, SRC.line_item_currency_code, SRC.sale_global_tran_id, SRC.sale_line_item_seq_num, SRC.sale_business_day_date, SRC.sale_tran_date, SRC.dw_batch_date);



--Now that we have updated the target table, let's make sure that any keys to
--historical sale records are appended to the ERTM_KEYS_WRK table so that we can
--run those records thru the regular Clarity build process. The keys for return
--line items are (by definition) already in the ERTM_KEYS_WRK table.


INSERT INTO  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk (global_tran_id, business_day_date, line_item_seq_num
 , status_code, dw_batch_date)
(SELECT DISTINCT x.sale_global_tran_id,
  x.sale_business_day_date,
  x.sale_line_item_seq_num,
  RPAD('+', 1, ' ') AS status_code,
   (SELECT dw_batch_dt
   FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')) AS dw_batch_date
 FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_return_sale_xref_wrk AS xw
  INNER JOIN  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_return_sale_xref AS x ON xw.return_global_tran_id = x.return_global_tran_id
      AND xw.return_line_item_seq_num = x.return_line_item_seq_num AND xw.return_business_day_date = x.return_business_day_date
    
 WHERE NOT EXISTS (SELECT 1
   FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS k
   WHERE LOWER(status_code) = LOWER('+')
    AND global_tran_id = x.sale_global_tran_id
    AND business_day_date = x.sale_business_day_date
    AND line_item_seq_num = x.sale_line_item_seq_num)
  AND x.sale_global_tran_id IS NOT NULL);


--Also append any OLDF keys that are associated with Sale records into the
--OMS_KEYS_WRK table so that they are re-processed, too.

INSERT INTO  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_order_keys_wrk (order_num, order_date_pacific, dw_batch_date)
(SELECT DISTINCT ord.order_num,
  ord.order_date_pacific,
   (SELECT dw_batch_dt
   FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date
 FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_return_sale_xref_wrk AS xw
  INNER JOIN  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_return_sale_xref AS x ON xw.return_global_tran_id = x.return_global_tran_id
      AND xw.return_line_item_seq_num = x.return_line_item_seq_num AND xw.return_business_day_date = x.return_business_day_date
    
  INNER JOIN  {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.order_line_detail_fact AS ord ON LOWER(x.order_num) = LOWER(ord.order_num) AND ord.order_date_pacific
     <= CURRENT_DATE
 WHERE LOWER(x.order_num) <> LOWER('')
  AND NOT EXISTS (SELECT 1
   FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_order_keys_wrk AS k
   WHERE LOWER(order_num) = LOWER(ord.order_num)
    AND order_date_pacific = ord.order_date_pacific)
  AND x.sale_global_tran_id IS NOT NULL);



--Append any Orders corresponding to the sales being processed on a given day.

INSERT INTO  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_order_keys_wrk (
  order_num, 
  order_date_pacific, 
  dw_batch_date)
SELECT DISTINCT oldf.order_num,
  oldf.order_date_pacific,
(SELECT dw_batch_dt FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_ORDER')) AS dw_batch_date
 FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk AS sls_key
  INNER JOIN  {{params.bq_project_id}}.{{params.DBENV}}_nap_base_vws.retail_tran_hdr_fact AS rthf 
  ON sls_key.global_tran_id = rthf.global_tran_id
 AND sls_key.business_day_date = rthf.business_day_date
  INNER JOIN  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_base_vws.order_line_detail_fact AS oldf 
  ON LOWER(oldf.order_num) = LOWER(rthf.order_num) 
  AND oldf.order_date_pacific <= CURRENT_DATE
  WHERE NOT EXISTS (
SELECT 1 FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_NAP_JWN_METRICS_STG.JWN_ORDER_KEYS_WRK k
WHERE LOWER(k.order_num) = LOWER(oldf.order_num)
  AND k.order_date_pacific = oldf.order_date_pacific
);


--Append any ERTM keys corresponding to the orders being processed on a given day.
--Added as fix for PRC 903

INSERT INTO  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_clarity_tran_ertm_keys_wrk 
(global_tran_id
, business_day_date
, line_item_seq_num
 , status_code
 , dw_batch_date)
SELECT 
DISTINCT jctf.global_tran_id,
  jctf.business_day_date,
  jctf.transaction_line_seq_num,
  RPAD('+', 1, ' ') AS status_code,
   (SELECT max(dw_batch_dt)
   FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.etl_batch_dt_lkup
   WHERE LOWER(interface_code) = LOWER('JWN_CLARITY_SALES')) AS dw_batch_date
 FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_base_vws.jwn_clarity_transaction_fact AS jctf
  INNER JOIN  {{params.bq_project_id}}.{{params.DBJWNENV}}_nap_jwn_metrics_stg.jwn_order_keys_wrk AS o 
  ON LOWER(jctf.record_source) = LOWER('S') 
  AND LOWER(jctf.line_item_activity_type_code) = LOWER('S') 
  AND LOWER(jctf.tran_type_code) = LOWER('SALE') 
  AND LOWER(jctf.order_num) = LOWER(o.order_num)
WHERE NOT EXISTS (
SELECT 1 FROM  {{params.bq_project_id}}.{{params.DBJWNENV}}_NAP_JWN_METRICS_STG.JWN_CLARITY_TRAN_ERTM_KEYS_WRK  k
WHERE k.global_tran_id = jctf.global_tran_id
  AND k.business_day_date = jctf.business_day_date
  AND k.line_item_seq_num = jctf.transaction_line_seq_num)
;


-- ET;


