
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_clearance_markdown_receipt_date_load;
---Task_Name=receipt_date_rack_date_fact_load;'*/

BEGIN TRANSACTION;

BEGIN
SET ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transfer_rack_date_wrk;



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transfer_rack_date_wrk (rms_sku_num, store_num, first_receipt_date, last_receipt_date,
 first_rack_date, last_rack_date, rack_ind, reset_ind, reset_date, insert_record_ind, dw_sys_load_tmstp,
 dw_sys_updt_tmstp, dw_batch_date)
(SELECT trnsfr.rms_sku_num,
  trnsfr.store_num,
  MIN(trnsfr.min_transfer_date) AS first_receipt_date,
  MAX(trnsfr.max_transfer_date) AS last_receipt_date,
  MIN(trnsfr.min_transfer_date) AS first_rack_date,
  MAX(trnsfr.max_transfer_date) AS last_rack_date,
  MAX('Y'),
  MAX(CASE
    WHEN CAST(cmrf.effective_tmstp AS DATE) <= trnsfr.min_transfer_date AND rcpt_dt.last_receipt_date < CAST(cmrf.effective_tmstp AS DATE)
      
    THEN 'Y'
    ELSE 'N'
    END),
  MAX(CASE
    WHEN CAST(cmrf.effective_tmstp AS DATE) <= trnsfr.min_transfer_date AND rcpt_dt.last_receipt_date < CAST(cmrf.effective_tmstp AS DATE)
      
    THEN trnsfr.min_transfer_date
    ELSE NULL
    END),
  MAX(CASE
    WHEN rcpt_dt.rms_sku_num IS NULL
    THEN 'Y'
    ELSE 'N'
    END),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
   (SELECT dw_batch_dt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
   WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT'))
 FROM (SELECT mtdf.rms_sku_num,
    mtdf.store_num,
    psd.rms_style_num,
    psd.color_num_trimmed AS color_num,
    psd.channel_country,
    MIN(mtdf.tran_date) AS min_transfer_date,
    MAX(mtdf.tran_date) AS max_transfer_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_transfer_day_fact AS mtdf
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv_from ON mtdf.from_loc = psdv_from.store_num
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv_to ON mtdf.to_loc = psdv_to.store_num
    INNER JOIN (SELECT CASE
       WHEN LOWER(color_num) IN (LOWER('0'), LOWER('000'))
       THEN '0'
       ELSE LTRIM(color_num, '0')
       END AS color_num_trimmed,
      rms_sku_num,
      epm_sku_num,
      channel_country,
      sku_short_desc,
      sku_desc,
      web_sku_num,
      brand_label_num,
      brand_label_display_name,
      rms_style_num,
      epm_style_num,
      partner_relationship_num,
      partner_relationship_type_code,
      web_style_num,
      style_desc,
      epm_choice_num,
      supp_part_num,
      prmy_supp_num,
      manufacturer_num,
      sbclass_num,
      sbclass_desc,
      class_num,
      class_desc,
      dept_num,
      dept_desc,
      grp_num,
      grp_desc,
      div_num,
      div_desc,
      cmpy_num,
      cmpy_desc,
      color_num,
      color_desc,
      nord_display_color,
      nrf_size_code,
      size_1_num,
      size_1_desc,
      size_2_num,
      size_2_desc,
      supp_color,
      supp_size,
      brand_name,
      return_disposition_code,
      return_disposition_desc,
      selling_status_code,
      selling_status_desc,
      live_date,
      drop_ship_eligible_ind,
      sku_type_code,
      sku_type_desc,
      pack_orderable_code,
      pack_orderable_desc,
      pack_sellable_code,
      pack_sellable_desc,
      pack_simple_code,
      pack_simple_desc,
      display_seq_1,
      display_seq_2,
      hazardous_material_class_code,
      hazardous_material_class_desc,
      fulfillment_type_code,
      selling_channel_eligibility_list,
      smart_sample_ind,
      gwp_ind,
      msrp_amt,
      msrp_currency_code,
      npg_ind,
      order_quantity_multiple,
      fp_forecast_eligible_ind,
      fp_item_planning_eligible_ind,
      fp_replenishment_eligible_ind,
      op_forecast_eligible_ind,
      op_item_planning_eligible_ind,
      op_replenishment_eligible_ind,
      size_range_desc,
      size_sequence_num,
      size_range_code,
      eff_begin_tmstp_utc,
      eff_end_tmstp_utc,
      dw_batch_id,
      dw_batch_date,
      dw_sys_load_tmstp
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd_hist) AS psd ON LOWER(psd.rms_sku_num) = LOWER(mtdf.rms_sku_num)
     AND LOWER(psd.channel_country) = LOWER(psdv_from.channel_country)
	 AND RANGE_CONTAINS(RANGE(psd.eff_begin_tmstp_utc, psd.eff_end_tmstp_utc) , (CAST(etl.dw_batch_dt AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND))--- AT TIME ZONE 'GMT'
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_receipt_date_markdown_vw AS cmf ON LOWER(cmf.rms_style_num) = LOWER(psd.rms_style_num
         ) AND LOWER(cmf.color_num) = LOWER(psd.color_num_trimmed) AND LOWER(cmf.channel_country) = LOWER(psd.channel_country
        ) AND mtdf.tran_date BETWEEN CAST(cmf.effective_begin_tmstp AS DATE) AND (DATE_ADD(CAST(cmf.effective_begin_tmstp AS DATE)
        ,INTERVAL 10 DAY))
   WHERE LOWER(psdv_from.channel_brand) = LOWER('NORDSTROM')
    AND LOWER(psdv_to.channel_brand) = LOWER('NORDSTROM_RACK')
    AND LOWER(mtdf.event_type) = LOWER('TRANSFER_SHIP_TO_LOCATION_LEDGER_POSTED')
    AND mtdf.tran_date BETWEEN (DATE_SUB((SELECT dw_batch_dt
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
        WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')), INTERVAL CAST(TRUNC(CAST(CASE
         WHEN (SELECT config_value
           FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
           WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
            AND LOWER(config_key) = LOWER('REBUILD_DAYS')) = ''
         THEN '0'
         ELSE (SELECT config_value
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
          WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
           AND LOWER(config_key) = LOWER('REBUILD_DAYS'))
         END AS FLOAT64)) AS INTEGER) DAY)) AND (SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
      WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT'))
   GROUP BY mtdf.rms_sku_num,
    mtdf.store_num,
    psd.rms_style_num,
    color_num,
    psd.channel_country) AS trnsfr
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS rcpt_dt ON LOWER(trnsfr.rms_sku_num) = LOWER(rcpt_dt.rms_sku_num
     ) AND trnsfr.store_num = rcpt_dt.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_receipt_date_markdown_reset_vw AS cmrf ON LOWER(trnsfr.rms_style_num) = LOWER(cmrf.rms_style_num
      ) AND LOWER(trnsfr.color_num) = LOWER(cmrf.color_num) AND LOWER(trnsfr.channel_country) = LOWER(cmrf.channel_country
     )
 GROUP BY trnsfr.rms_sku_num,
  trnsfr.store_num);



  
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS tgt 
SET
    first_receipt_date = src.min_receipt_date,
    last_receipt_date = src.max_receipt_date,
    first_rack_date = src.min_rack_date,
    last_rack_date = src.max_rack_date,
    rack_ind = src.rack_ind,
    reset_ind = src.reset_ind,
    reset_date = src.reset_date,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
FROM (SELECT rms_sku_num, store_num, first_receipt_date AS min_receipt_date, last_receipt_date AS max_receipt_date, first_rack_date AS min_rack_date, last_rack_date AS max_rack_date, rack_ind, reset_ind, reset_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transfer_rack_date_wrk AS rcpt_wrk
        WHERE LOWER(reset_ind) = LOWER('Y') AND LOWER(insert_record_ind) = LOWER('N')) AS src
WHERE LOWER(tgt.rms_sku_num) = LOWER(src.rms_sku_num) AND tgt.store_num = src.store_num;


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_fact AS tgt
SET
    first_receipt_date = src.min_receipt_date,
    last_receipt_date = src.max_receipt_date,
    first_rack_date = src.min_rack_date,
    last_rack_date = src.max_rack_date,
    rack_ind = src.rack_ind,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) FROM (SELECT rcpt_wrk.rms_sku_num, rcpt_wrk.store_num, CASE WHEN COALESCE(rcpt_date.first_receipt_date, DATE '4444-04-04') > COALESCE(rcpt_wrk.first_receipt_date, DATE '4444-04-04') THEN rcpt_wrk.first_receipt_date ELSE rcpt_date.first_receipt_date END AS min_receipt_date, CASE WHEN COALESCE(rcpt_date.last_receipt_date, DATE '2000-01-01') < COALESCE(rcpt_wrk.last_receipt_date, DATE '2000-01-01') THEN rcpt_wrk.last_receipt_date ELSE rcpt_date.last_receipt_date END AS max_receipt_date, CASE WHEN COALESCE(rcpt_date.first_rack_date, DATE '4444-04-04') > COALESCE(rcpt_wrk.first_rack_date, DATE '4444-04-04') THEN rcpt_wrk.first_rack_date ELSE rcpt_date.first_rack_date END AS min_rack_date, CASE WHEN COALESCE(rcpt_date.last_rack_date, DATE '2000-01-01') < COALESCE(rcpt_wrk.last_rack_date, DATE '2000-01-01') THEN rcpt_wrk.last_rack_date ELSE rcpt_date.last_rack_date END AS max_rack_date, rcpt_wrk.rack_ind
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transfer_rack_date_wrk AS rcpt_wrk
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
  first_rack_date,
  last_rack_date,
  rack_ind,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transfer_rack_date_wrk
 WHERE LOWER(reset_ind) = LOWER('N')
  AND LOWER(insert_record_ind) = LOWER('Y'));

  
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
END;
