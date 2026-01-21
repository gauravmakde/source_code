
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_transfer_date_wrk;



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_transfer_date_wrk (rms_sku_num, store_num, first_receipt_date, last_receipt_date,
 dw_sys_load_tmstp, dw_sys_updt_tmstp, dw_batch_date)
(SELECT rms_sku_num,
  store_num,
  MIN(tran_date) AS min_receipt_date,
  MAX(tran_date) AS max_receipt_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
   (SELECT dw_batch_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
   WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')) AS dw_batch_dt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_poreceipt_sku_store_fact AS mpssf
 WHERE tran_date BETWEEN (DATE_SUB((SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
      WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')), INTERVAL CAST(CASE
       WHEN (SELECT config_value
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
         WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
          AND LOWER(config_key) = LOWER('REBUILD_DAYS')) = ''
       THEN '0'
       ELSE (SELECT config_value
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
         AND LOWER(config_key) = LOWER('REBUILD_DAYS'))
       END AS INTEGER) DAY)) AND (SELECT dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
    WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT'))
  AND LOWER(dropship_ind) = LOWER('N')
  AND (receipts_units > 0 OR receipts_crossdock_units > 0)
 GROUP BY store_num,
  rms_sku_num);



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_transfer_date_wrk (rms_sku_num, store_num, first_receipt_date, last_receipt_date,
 dw_sys_load_tmstp, dw_sys_updt_tmstp, dw_batch_date)
(SELECT rms_sku_num,
  store_num,
  MIN(tran_date) AS min_transfer_date,
  MAX(tran_date) AS max_transfer_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
   (SELECT dw_batch_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
   WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')) AS dw_batch_dt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_transfer_sku_store_day_columnar_vw AS mtsscv
 WHERE tran_date BETWEEN (DATE_SUB((SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
      WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')), INTERVAL CAST(CASE
       WHEN (SELECT config_value
         FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
         WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
          AND LOWER(config_key) = LOWER('REBUILD_DAYS')) = ''
       THEN '0'
       ELSE (SELECT config_value
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
         AND LOWER(config_key) = LOWER('REBUILD_DAYS'))
       END AS INTEGER) DAY)) AND (SELECT dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
    WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT'))
  AND (reservestock_transfer_in_units > 0 OR packandhold_transfer_in_units > 0 OR warehouse_transfer_in_units > 0)
 GROUP BY rms_sku_num,
  store_num);




TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk (rms_sku_num, store_num, first_receipt_date, last_receipt_date, reset_ind
 , reset_date, insert_record_ind, dw_sys_load_tmstp, dw_sys_updt_tmstp, dw_batch_date)
(SELECT po_rcpt.rms_sku_num,
  po_rcpt.store_num,
  po_rcpt.min_receipt_date,
  po_rcpt.max_receipt_date,
   CASE
   WHEN DATE_DIFF(COALESCE(po_rcpt.min_receipt_date, po_rcpt.dw_batch_dt), COALESCE(rcpt_dt.last_receipt_date, po_rcpt.dw_batch_dt
      ), DAY) > 120
   THEN 'Y'
   ELSE 'N'
   END AS reset_ind,
   CASE
   WHEN DATE_DIFF(COALESCE(po_rcpt.min_receipt_date, po_rcpt.dw_batch_dt), COALESCE(rcpt_dt.last_receipt_date, po_rcpt.dw_batch_dt
      ), DAY) > 120
   THEN po_rcpt.min_receipt_date
   ELSE NULL
   END AS reset_date,
   CASE
   WHEN rcpt_dt.rms_sku_num IS NULL
   THEN 'Y'
   ELSE 'N'
   END AS insert_record_ind,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  po_rcpt.dw_batch_dt
 FROM (SELECT rms_sku_num,
    store_num,
    MIN(first_receipt_date) AS min_receipt_date,
    MAX(last_receipt_date) AS max_receipt_date,
     (SELECT dw_batch_dt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
     WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')) AS dw_batch_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_transfer_date_wrk AS rcpt_dt_wrk
   GROUP BY rms_sku_num,
    store_num) AS po_rcpt
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS rcpt_dt ON LOWER(po_rcpt.rms_sku_num) = LOWER(rcpt_dt.rms_sku_num
     ) AND po_rcpt.store_num = rcpt_dt.store_num);




UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk AS tgt
SET
    reset_ind = src.style_level_reset_ind,
    reset_date = src.reset_date 
FROM (SELECT sub1.rms_sku_num, sub1.store_num, CASE WHEN LOWER(MIN(CASE WHEN DATE_DIFF(COALESCE(sub1.first_receipt_date, CURRENT_DATE('PST8PDT')), COALESCE(rcpt_dt.last_receipt_date, CURRENT_DATE('PST8PDT')), DAY) > 120 THEN 'Y' ELSE 'N' END) OVER (PARTITION BY sub1.rms_style_num, sub1.color_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) <> LOWER(MAX(CASE WHEN DATE_DIFF(COALESCE(sub1.first_receipt_date, CURRENT_DATE('PST8PDT')), COALESCE(rcpt_dt.last_receipt_date, CURRENT_DATE('PST8PDT')), DAY) > 120 THEN 'Y' ELSE 'N' END) OVER (PARTITION BY sub1.rms_style_num, sub1.color_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) THEN 'N' ELSE MIN(CASE WHEN DATE_DIFF(COALESCE(sub1.first_receipt_date, CURRENT_DATE('PST8PDT')), COALESCE(rcpt_dt.last_receipt_date, CURRENT_DATE('PST8PDT')), DAY) > 120 THEN 'Y' ELSE 'N' END) OVER (PARTITION BY sub1.rms_style_num, sub1.color_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) END AS style_level_reset_ind, CASE WHEN LOWER(MIN(CASE WHEN DATE_DIFF(COALESCE(sub1.first_receipt_date, CURRENT_DATE('PST8PDT')), COALESCE(rcpt_dt.last_receipt_date, CURRENT_DATE('PST8PDT')), DAY) > 120 THEN 'Y' ELSE 'N' END) OVER (PARTITION BY sub1.rms_style_num, sub1.color_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) <> LOWER(MAX(CASE WHEN DATE_DIFF(COALESCE(sub1.first_receipt_date, CURRENT_DATE('PST8PDT')), COALESCE(rcpt_dt.last_receipt_date, CURRENT_DATE('PST8PDT')), DAY) > 120 THEN 'Y' ELSE 'N' END) OVER (PARTITION BY sub1.rms_style_num, sub1.color_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) THEN NULL ELSE sub1.reset_date END AS reset_date, CASE WHEN LOWER(sub1.rms_sku_num) <> LOWER(rcpt_dt.rms_sku_num) THEN NULL ELSE sub1.first_receipt_date END AS sku_level_first_receipt_date, CASE WHEN LOWER(sub1.rms_sku_num) <> LOWER(rcpt_dt.rms_sku_num) THEN NULL ELSE sub1.last_receipt_date END AS sku_level_last_receipt_date
        FROM (SELECT psd.rms_style_num, psd.color_num, psdv.channel_brand, psdv.selling_channel, psdv.channel_country, psd1.rms_sku_num, psdv.store_num, rcpt_wrk.first_receipt_date, rcpt_wrk.last_receipt_date, COALESCE(rcpt_wrk.reset_date, etl.dw_batch_dt) AS reset_date, COALESCE(rcpt_wrk.reset_ind, 'Y') AS reset_ind
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk AS rcpt_wrk
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('SMD_CDS_RECEIPT_DT') AND rcpt_wrk.dw_batch_date = etl.dw_batch_dt
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON rcpt_wrk.store_num = psdv.store_num
                    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd ON LOWER(psd.rms_sku_num) = LOWER(rcpt_wrk.rms_sku_num) AND LOWER(psd.channel_country) = LOWER(psdv.channel_country)
					AND RANGE_CONTAINS(RANGE(psd.eff_begin_tmstp, psd.eff_end_tmstp),
    DATETIME(CAST(etl.dw_batch_dt AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND))
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd1 ON LOWER(psd.channel_country) = LOWER(psd1.channel_country) 
					AND LOWER(psd.rms_style_num) = LOWER(psd1.rms_style_num) AND LOWER(psd.color_num) = LOWER(psd1.color_num)
					AND RANGE_CONTAINS(RANGE(psd1.EFF_BEGIN_TMSTP, psd1.EFF_END_TMSTP) , DATETIME(CAST(etl.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND))
                WHERE LOWER(rcpt_wrk.reset_ind) = LOWER('Y') AND LOWER(rcpt_wrk.insert_record_ind) = LOWER('N')) AS sub1
            LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS rcpt_dt ON LOWER(sub1.rms_sku_num) = LOWER(rcpt_dt.rms_sku_num)
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY sub1.rms_sku_num, sub1.store_num ORDER BY NULL)) = 1) AS src
WHERE LOWER(tgt.rms_sku_num) = LOWER(src.rms_sku_num) AND tgt.store_num = src.store_num;



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_sku_store_gtt;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_sku_store_gtt (rms_sku_num, store_num)
(SELECT DISTINCT psd1.rms_sku_num,
  psdv1.store_num
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk AS rcpt_wrk
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('SMD_CDS_RECEIPT_DT') AND
    rcpt_wrk.dw_batch_date = etl.dw_batch_dt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON rcpt_wrk.store_num = psdv.store_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd ON LOWER(psd.rms_sku_num) = LOWER(rcpt_wrk.rms_sku_num) AND
    LOWER(psd.channel_country) = LOWER(psdv.channel_country)
	AND RANGE_CONTAINS(RANGE(psd.EFF_BEGIN_TMSTP, psd.EFF_END_TMSTP) , DATETIME(CAST(etl.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND ))
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd1 ON LOWER(psd.channel_country) = LOWER(psd1.channel_country)
    AND LOWER(psd.rms_style_num) = LOWER(psd1.rms_style_num) AND LOWER(psd.color_num) = LOWER(psd1.color_num)
	AND RANGE_CONTAINS(RANGE(psd1.EFF_BEGIN_TMSTP, psd1.EFF_END_TMSTP) , DATETIME(CAST(etl.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND ))
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv1 ON LOWER(psdv1.channel_brand) = LOWER(psdv.channel_brand) AND
    LOWER(psdv1.channel_country) = LOWER(psdv.channel_country)
 WHERE LOWER(rcpt_wrk.reset_ind) = LOWER('Y')
  AND LOWER(rcpt_wrk.insert_record_ind) = LOWER('N')
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY psd1.rms_sku_num, psdv1.store_num ORDER BY NULL)) = 1);



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS tgt
SET
    first_receipt_date = src.first_receipt_date,
    last_receipt_date = src.last_receipt_date,
    reset_date = src.reset_date,
    reset_ind = src.reset_ind,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) FROM (SELECT rcpt_dt_wrk.rms_sku_num, rcpt_dt_wrk.store_num, rcpt_dt_wrk.first_receipt_date, rcpt_dt_wrk.last_receipt_date, rcpt_dt_wrk.reset_ind, rcpt_dt_wrk.reset_date, rcpt_dt_wrk.insert_record_ind, rcpt_dt_wrk.dw_sys_load_tmstp, rcpt_dt_wrk.dw_sys_updt_tmstp, rcpt_dt_wrk.dw_batch_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_sku_store_gtt AS ss_temp
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk AS rcpt_dt_wrk ON LOWER(ss_temp.rms_sku_num) = LOWER(rcpt_dt_wrk.rms_sku_num) AND ss_temp.store_num = rcpt_dt_wrk.store_num) AS src
WHERE LOWER(tgt.rms_sku_num) = LOWER(src.rms_sku_num) AND tgt.store_num = src.store_num;




UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS tgt
SET
    first_receipt_date = src.first_receipt_date,
    last_receipt_date = src.last_receipt_date,
    reset_date = src.reset_date,
    reset_ind = src.reset_ind,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) FROM (SELECT ss_temp.rms_sku_num, ss_temp.store_num, rcpt_dt_wrk.first_receipt_date, rcpt_dt_wrk.last_receipt_date, (SELECT dw_batch_dt
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
                WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')) AS reset_date, 'Y' AS reset_ind
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_sku_store_gtt AS ss_temp
            LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk AS rcpt_dt_wrk ON LOWER(ss_temp.rms_sku_num) = LOWER(rcpt_dt_wrk.rms_sku_num) AND ss_temp.store_num = rcpt_dt_wrk.store_num
        WHERE rcpt_dt_wrk.rms_sku_num IS NULL) AS src
WHERE LOWER(tgt.rms_sku_num) = LOWER(src.rms_sku_num) AND tgt.store_num = src.store_num;



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS tgt 
SET
    first_receipt_date = src.min_receipt_date,
    last_receipt_date = src.max_receipt_date,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
FROM (SELECT rcpt_wrk.rms_sku_num, rcpt_wrk.store_num, CASE WHEN COALESCE(rcpt_date.first_receipt_date, DATE '4444-04-04') > COALESCE(rcpt_wrk.first_receipt_date, DATE '4444-04-04') THEN rcpt_wrk.first_receipt_date ELSE rcpt_date.first_receipt_date END AS min_receipt_date, CASE WHEN COALESCE(rcpt_date.last_receipt_date, DATE '2000-01-01') < COALESCE(rcpt_wrk.last_receipt_date, DATE '2000-01-01') THEN rcpt_wrk.last_receipt_date ELSE rcpt_date.last_receipt_date END AS max_receipt_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk AS rcpt_wrk
            INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS rcpt_date ON LOWER(rcpt_wrk.rms_sku_num) = LOWER(rcpt_date.rms_sku_num) AND rcpt_wrk.store_num = rcpt_date.store_num
        WHERE LOWER(rcpt_wrk.reset_ind) = LOWER('N') AND LOWER(rcpt_wrk.insert_record_ind) = LOWER('N')) AS src
WHERE LOWER(tgt.rms_sku_num) = LOWER(src.rms_sku_num) AND tgt.store_num = src.store_num;




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact (rms_sku_num, store_num, first_receipt_date,
 last_receipt_date, reset_ind, reset_date, first_rack_date, last_rack_date, rack_ind, dw_sys_load_tmstp,
 dw_sys_updt_tmstp, dw_batch_date)
(SELECT rms_sku_num,
  store_num,
  first_receipt_date,
  last_receipt_date,
  reset_ind,
  reset_date,
  CAST(NULL AS DATE),
  CAST(NULL AS DATE),
  'N',
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_receipt_date_wrk
 WHERE LOWER(reset_ind) = LOWER('N')
  AND LOWER(insert_record_ind) = LOWER('Y'));

