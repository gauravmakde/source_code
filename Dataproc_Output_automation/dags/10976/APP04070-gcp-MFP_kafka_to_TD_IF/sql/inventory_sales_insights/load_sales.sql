
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=app04070;DAG_ID=sales_amount_10976_tech_nap_merch;Task_Name=execute_load_sales;'*/


SET ERROR_CODE  =  0;


MERGE INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_sales_insights_by_day_fact
USING
  (
  SELECT
    clearance_markdown_sales_vw.business_day_date,
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
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_sales_vw
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS elt
  ON
    LOWER(elt.interface_code) = LOWER('SMD_SLS_DLY')
  WHERE
    clearance_markdown_sales_vw.business_day_date >= elt.extract_start_dt
    AND clearance_markdown_sales_vw.business_day_date <= elt.extract_end_dt
    AND clearance_markdown_sales_vw.store_num <> 5405
  GROUP BY
    clearance_markdown_sales_vw.business_day_date,
    clearance_markdown_sales_vw.sku_num,
    clearance_markdown_sales_vw.rms_style_num,
    clearance_markdown_sales_vw.color_num,
    clearance_markdown_sales_vw.channel_brand,
    clearance_markdown_sales_vw.selling_channel,
    clearance_markdown_sales_vw.channel_country,
    store_num,
    clearance_markdown_sales_vw.line_item_currency_code,
    clearance_markdown_sales_vw.store_type_code,
    clearance_markdown_sales_vw.store_abbrev_name) AS msv
ON
  inventory_sales_insights_by_day_fact.metrics_date = msv.business_day_date
  AND LOWER(inventory_sales_insights_by_day_fact.rms_sku_id) = LOWER(msv.sku_num)
  AND LOWER(inventory_sales_insights_by_day_fact.rms_style_num) = LOWER(msv.rms_style_num)
  AND LOWER(inventory_sales_insights_by_day_fact.color_num) = LOWER(msv.color_num)
  AND LOWER(inventory_sales_insights_by_day_fact.channel_brand) = LOWER(msv.channel_brand)
  AND LOWER(inventory_sales_insights_by_day_fact.selling_channel) = LOWER(msv.selling_channel)
  AND LOWER(inventory_sales_insights_by_day_fact.channel_country) = LOWER(msv.channel_country)
  AND LOWER(inventory_sales_insights_by_day_fact.location_id) = LOWER(msv.store_num)
  WHEN MATCHED THEN UPDATE SET total_sales_units = msv.total_sales_units, 
  total_sales_amt = msv.total_sales_amt, 
  total_sales_usd_amt = msv.total_sales_usd_amt, 
  dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
  WHEN NOT MATCHED
  THEN
INSERT
  (metrics_date,
    rms_sku_id,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    selling_channel,
    location_id,
    total_sales_units,
    total_sales_amt,
    total_sales_usd_amt,
    line_item_currency_code,
    store_type_code,
    store_abbrev_name,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz)
VALUES
  (msv.business_day_date, msv.sku_num, msv.rms_style_num, msv.color_num, msv.channel_country, msv.channel_brand, msv.selling_channel, msv.store_num, msv.total_sales_units, msv.total_sales_amt, msv.total_sales_usd_amt, msv.line_item_currency_code, msv.store_type_code, msv.store_abbrev_name, CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) , `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST());





END;
