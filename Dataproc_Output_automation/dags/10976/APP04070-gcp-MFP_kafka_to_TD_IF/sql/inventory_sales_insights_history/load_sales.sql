/* SET QUERY_BAND = 'App_ID=app04070;DAG_ID=inventory_sales_insights_history_10976_tech_nap_merch;Task_Name=execute_load_sales;'
FOR SESSION VOLATILE;*/

-- ET;

-- Load Sales Units & Amount metrics



MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_sales_insights_by_day_hist_fact AS tgt
USING (SELECT clearance_markdown_sales_vw.business_day_date,
  clearance_markdown_sales_vw.sku_num,
  clearance_markdown_sales_vw.rms_style_num,
  clearance_markdown_sales_vw.color_num,
  clearance_markdown_sales_vw.channel_brand,
  clearance_markdown_sales_vw.selling_channel,
  clearance_markdown_sales_vw.channel_country,
  TRIM(FORMAT('%11d', clearance_markdown_sales_vw.store_num)) AS store_num,
  clearance_markdown_sales_vw.line_item_currency_code,
  clearance_markdown_sales_vw.store_type_code,
  clearance_markdown_sales_vw.store_abbrev_name,
  SUM(clearance_markdown_sales_vw.line_item_quantity) AS total_sales_units,
  SUM(clearance_markdown_sales_vw.line_net_amt) AS total_sales_amt,
  SUM(clearance_markdown_sales_vw.line_net_usd_amt) AS total_sales_usd_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_sales_vw
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control AS elt ON LOWER(elt.subject_area_nm) = LOWER('METRICS_DATA_SERVICE_HIST')
 WHERE clearance_markdown_sales_vw.business_day_date BETWEEN CAST(elt.extract_from_tmstp AS DATE) AND (CAST(elt.extract_to_tmstp AS DATE)
    )
  AND clearance_markdown_sales_vw.store_num <> 5405
 GROUP BY clearance_markdown_sales_vw.business_day_date,
  clearance_markdown_sales_vw.sku_num,
  clearance_markdown_sales_vw.rms_style_num,
  clearance_markdown_sales_vw.color_num,
  clearance_markdown_sales_vw.channel_brand,
  clearance_markdown_sales_vw.selling_channel,
  clearance_markdown_sales_vw.channel_country,
  store_num,
  clearance_markdown_sales_vw.line_item_currency_code,
  clearance_markdown_sales_vw.store_type_code,
  clearance_markdown_sales_vw.store_abbrev_name) AS src
ON tgt.metrics_date = src.business_day_date AND LOWER(tgt.rms_sku_id) = LOWER(src.sku_num) AND LOWER(tgt.rms_style_num)
       = LOWER(src.rms_style_num) AND LOWER(tgt.color_num) = LOWER(src.color_num) AND LOWER(tgt.channel_brand) = LOWER(src
      .channel_brand) AND LOWER(tgt.selling_channel) = LOWER(src.selling_channel) AND LOWER(tgt.channel_country) = LOWER(src
    .channel_country) AND LOWER(tgt.location_id) = LOWER(src.store_num)
WHEN MATCHED THEN UPDATE SET
 total_sales_units = src.total_sales_units,
 total_sales_amt = src.total_sales_amt,
 total_sales_usd_amt = src.total_sales_usd_amt,
 dw_sys_updt_tmstp = cast(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) as TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`()
WHEN NOT MATCHED THEN INSERT (metrics_date, rms_sku_id, rms_style_num, color_num, channel_country, channel_brand,
 selling_channel, location_id, total_sales_units, total_sales_amt, total_sales_usd_amt, line_item_currency_code,
 store_type_code, store_abbrev_name, dw_sys_load_tmstp,dw_sys_load_tmstp_tz) VALUES(src.business_day_date, src.sku_num, src.rms_style_num,
 src.color_num, src.channel_country, src.channel_brand, src.selling_channel, src.store_num, src.total_sales_units, src.total_sales_amt
 , src.total_sales_usd_amt, src.line_item_currency_code, src.store_type_code, src.store_abbrev_name, CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) as TIMESTAMP),`{{params.gcp_project_id}}.JWN_UDF.DEFAULT_TZ_PST`()
 );


--COLLECT STATISTICS     COLUMN(RMS_SKU_ID),     COLUMN(RMS_STYLE_NUM),     COLUMN(COLOR_NUM),     COLUMN(CHANNEL_COUNTRY),     COLUMN(CHANNEL_BRAND),     COLUMN(SELLING_CHANNEL),     COLUMN(LOCATION_ID),     COLUMN(RMS_SKU_ID, RMS_STYLE_NUM, COLOR_NUM, CHANNEL_COUNTRY, CHANNEL_BRAND, SELLING_CHANNEL, LOCATION_ID),     COLUMN(PARTITION) ON `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.INVENTORY_SALES_INSIGHTS_BY_DAY_HIST_FACT