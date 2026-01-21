-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=merch_clearance_markdown_receipt_date_week_load;
-- Task_Name=receipt_date_reset_applied_week_fact_load;'
-- FOR SESSION VOLATILE;

-- ET;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_week_fact AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
    WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT') AND tgt.week_end_date = dw_batch_dt);
    
    INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_receipt_date_reset_applied_week_fact (week_end_date, rms_style_num, color_num,
 channel_country, channel_brand, selling_channel, first_receipt_date, last_receipt_date, reset_ind, reset_date,
 first_rack_date, last_rack_date, rack_ind, dw_sys_load_tmstp, dw_sys_updt_tmstp, dw_batch_date)
(SELECT etl.dw_batch_dt,
  psd.rms_style_num,
  psd.color_num,
  psd.channel_country,
  psdv.channel_brand,
  psdv.selling_channel,
  MIN(mrdwf.first_receipt_date),
  MAX(mrdwf.last_receipt_date),
  MAX(mrdwf.reset_ind),
  MIN(mrdwf.reset_date),
  MIN(mrdwf.first_rack_date),
  MAX(mrdwf.last_rack_date),
  MAX(mrdwf.rack_ind),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
  MIN(etl.dw_batch_dt)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_receipt_date_reset_applied_fact AS mrdwf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON LOWER(etl.interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv ON mrdwf.store_num = psdv.store_num
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd ON LOWER(psd.rms_sku_num) = LOWER(mrdwf.rms_sku_num) AND LOWER(psd
     .channel_country) = LOWER(psdv.channel_country)
     AND RANGE_CONTAINS(RANGE(psd.EFF_BEGIN_TMSTP_UTC, psd.EFF_END_TMSTP_UTC) , CAST(etl.DW_BATCH_DT AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL 1 MILLISECOND)

 WHERE psdv.store_type_code NOT IN (SELECT config_value
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
    WHERE LOWER(interface_code) = LOWER('SMD_CDS_RECEIPT_DT')
     AND LOWER(config_key) = LOWER('STORE_TYPE_EXCLUDE'))
 GROUP BY etl.dw_batch_dt,
  psd.rms_style_num,
  psd.color_num,
  psd.channel_country,
  psdv.channel_brand,
  psdv.selling_channel
  
  
  );

-- ET;

-- COLLECT STATS ON PRD_NAP_FCT.MERCH_RECEIPT_DATE_RESET_APPLIED_WEEK_FACT;

-- ET;

-- SET QUERY_BAND = NONE FOR SESSION;

-- ET;