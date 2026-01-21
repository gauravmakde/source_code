BEGIN
-- DECLARE
--   ERROR_CODE INT64;
-- DECLARE
--   ERROR_MESSAGE STRING; /*SET QUERY_BAND = 'App_ID=app04070;DAG_ID=smartmarkdown_insights_10976_tech_nap_merch;Task_Name=run_load_first_last_receipts;'*/ 
  
CREATE TEMPORARY TABLE IF NOT EXISTS my_params AS
SELECT
  dw_batch_dt AS last_sat
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
WHERE
  LOWER(interface_code) = LOWER('SMD_INSIGHTS_WKLY');
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_receipts_insights_fact (rms_sku_id,
    rms_style_num,
    color_num,
    channel_brand,
    selling_channel,
    channel_country,
    location_id,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz,
    dw_sys_updt_tmstp,
    dw_sys_updt_tmstp_tz) (
  SELECT
    DISTINCT psd.rms_sku_num,
    cmibwf.rms_style_num,
    cmibwf.color_num,
    cmibwf.channel_brand,
    cmibwf.selling_channel,
    cmibwf.channel_country,
    TRIM(FORMAT('%11d', psdv.store_num)) AS location_id,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  AS dw_sys_load_tmstp_tz,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
     `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_fact AS cmibwf
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_vw AS psd
  ON
    LOWER(cmibwf.rms_style_num) = LOWER(psd.rms_style_num)
    AND LOWER(cmibwf.color_num) = LOWER(psd.color_num)
    AND LOWER(cmibwf.channel_country) = LOWER(psd.channel_country)
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psdv
  ON
    LOWER(cmibwf.channel_country) = LOWER(psdv.channel_country)
    AND LOWER(cmibwf.channel_brand) = LOWER(psdv.channel_brand)
  WHERE
    LOWER(psdv.store_type_code) IN (LOWER('FC'),
      LOWER('LH'),
      LOWER('OC'),
      LOWER('FL'),
      LOWER('SS'),
      LOWER('OF'),
      LOWER('RK'))); 
   
 CREATE TEMPORARY TABLE IF NOT EXISTS clearance_markdown_first_last_receipt ( rms_sku_num STRING,
    rms_style_num STRING,
    color_num STRING,
    channel_brand STRING,
    selling_channel STRING,
    channel_country STRING,
    location_id STRING,
    first_received_date DATE,
    last_received_date DATE )

    ;
INSERT INTO
  clearance_markdown_first_last_receipt (
  SELECT
    prf.rms_sku_num,
    irif.rms_style_num,
    irif.color_num,
    irif.channel_brand,
    irif.selling_channel,
    irif.channel_country,
    irif.location_id,
    MIN(prf.received_date) AS first_received_date,
    MAX(prf.received_date) AS last_received_date
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.po_receipt_fact AS prf
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_fact AS pof
  ON
    LOWER(prf.purchase_order_num) = LOWER(pof.purchase_order_num )
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_receipts_insights_fact AS irif
  ON
    LOWER(prf.rms_sku_num) = LOWER(irif.rms_sku_id )
    AND prf.tological_id = CAST(irif.location_id AS FLOAT64)
  WHERE
    pof.dropship_ind IS NULL
    OR LOWER(pof.dropship_ind) = LOWER('f')
  GROUP BY
    prf.rms_sku_num,
    irif.rms_style_num,
    irif.color_num,
    irif.channel_brand,
    irif.selling_channel,
    irif.channel_country,
    irif.location_id);


INSERT INTO
  clearance_markdown_first_last_receipt (
  SELECT
    tsrf.rms_sku_num,
    irif.rms_style_num,
    irif.color_num,
    irif.channel_brand,
    irif.selling_channel,
    irif.channel_country,
    irif.location_id,
    MIN(tsrf.ship_date) AS first_received_date,
    MAX(tsrf.ship_date) AS last_received_date
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_shipment_receipt_logical_fact AS tsrf
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_receipts_insights_fact AS irif
  ON
    LOWER(tsrf.rms_sku_num) = LOWER(irif.rms_sku_id )
    AND LOWER(SUBSTR(CAST(tsrf.to_location_id AS STRING), 1, 20)) = LOWER(irif.location_id)
  WHERE
    LOWER(tsrf.operation_type) = LOWER('A')
  GROUP BY
    tsrf.rms_sku_num,
    irif.rms_style_num,
    irif.color_num,
    irif.channel_brand,
    irif.selling_channel,
    irif.channel_country,
    irif.location_id);


INSERT INTO
  clearance_markdown_first_last_receipt (
  SELECT
    tsrf.rms_sku_num,
    irif.rms_style_num,
    irif.color_num,
    irif.channel_brand,
    irif.selling_channel,
    irif.channel_country,
    irif.location_id,
    MIN(tsrf.ship_date) AS first_received_date,
    MAX(tsrf.ship_date) AS last_received_date
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.transfer_shipment_receipt_logical_fact AS tsrf
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.rms14_transfer_created_fact AS rtcf
  ON
    LOWER(tsrf.rms_sku_num) = LOWER(rtcf.rms_sku_num)
    AND LOWER(tsrf.operation_num) = LOWER(rtcf.operation_num)
    AND LOWER(tsrf.operation_type) = LOWER(rtcf.operation_type )
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_receipts_insights_fact AS irif
  ON
    LOWER(tsrf.rms_sku_num) = LOWER(irif.rms_sku_id )
    AND LOWER(SUBSTR(CAST(tsrf.to_location_id AS STRING), 1, 20)) = LOWER(irif.location_id)
  WHERE
    LOWER(tsrf.operation_type) IN (LOWER('T'))
    AND LOWER(rtcf.transfer_context_value) IN (LOWER('RKING'),
      LOWER('RS'),
      LOWER('ST'),
      LOWER('PH'))
  GROUP BY
    tsrf.rms_sku_num,
    irif.rms_style_num,
    irif.color_num,
    irif.channel_brand,
    irif.selling_channel,
    irif.channel_country,
    irif.location_id);



UPDATE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_receipts_insights_fact
SET
  first_received_date = mflr.first_received_date,
  last_received_date = mflr.last_received_date,
  dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz= `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM (
  SELECT
    rms_sku_num,
    rms_style_num,
    color_num,
    channel_brand,
    selling_channel,
    channel_country,
    location_id,
    MIN(first_received_date) AS first_received_date,
    MAX(last_received_date) AS last_received_date
  FROM
    clearance_markdown_first_last_receipt
  GROUP BY
    rms_sku_num,
    rms_style_num,
    color_num,
    channel_brand,
    selling_channel,
    channel_country,
    location_id) AS mflr
WHERE
  LOWER(inventory_receipts_insights_fact.rms_sku_id) = LOWER(mflr.rms_sku_num)
  AND LOWER(inventory_receipts_insights_fact.rms_style_num) = LOWER(mflr.rms_style_num)
  AND LOWER(inventory_receipts_insights_fact.color_num) = LOWER(mflr.color_num)
  AND LOWER(inventory_receipts_insights_fact.channel_brand) = LOWER(mflr.channel_brand)
  AND LOWER(inventory_receipts_insights_fact.selling_channel) = LOWER(mflr.selling_channel)
  AND LOWER(inventory_receipts_insights_fact.channel_country) = LOWER(mflr.channel_country)
  AND LOWER(inventory_receipts_insights_fact.location_id) = LOWER(mflr.location_id);


UPDATE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
  first_receipt_date = src.first_received_date_agg,
  last_receipt_date = src.last_received_date_agg,
  available_to_sell = CAST(TRUNC(src.available_to_sell) AS INTEGER),
  dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz= `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM (
  SELECT
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    selling_channel,
    first_received_date_agg,
    last_received_date_agg,
    CASE WHEN LOWER(channel_country) = LOWER('CA') AND LOWER(channel_brand) = LOWER('NORDSTROM_RACK') AND LOWER(selling_channel) = LOWER('STORE') THEN CAST(available_to_sell_temp AS FLOAT64) ELSE 0 END AS available_to_sell

  FROM (
    SELECT
      rms_style_num,
      color_num,
      channel_country,
      channel_brand,
      selling_channel,
      MIN(first_received_date) AS first_received_date_agg,
      MAX(last_received_date) AS last_received_date_agg,
      MAX(ROUND(DATE_DIFF((SELECT * FROM my_params), first_received_date, DAY) / 7.0)) as available_to_sell_temp
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_receipts_insights_fact
    GROUP BY
      rms_style_num,
      color_num,
      channel_country,
      channel_brand,
      selling_channel
    HAVING
      first_received_date_agg IS NOT NULL) AS t2)  AS src
WHERE
  clearance_markdown_insights_by_week_fact.snapshot_date = (
  SELECT
    *
  FROM
    my_params)
  AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA'))
  AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA'))
  AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country)
  AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand)
  AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);




UPDATE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
  first_receipt_date = src.first_received_date_agg,
  last_receipt_date = src.last_received_date_agg,
  available_to_sell = CAST(TRUNC(src.available_to_sell) AS INTEGER),
  dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz= `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM (
  SELECT
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    MIN(first_received_date) AS first_received_date_agg,
    MAX(last_received_date) AS last_received_date_agg,
    ROUND(DATE_DIFF((
        SELECT
          *
        FROM
          my_params), MIN(first_received_date), DAY) / 7.0) AS available_to_sell
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_receipts_insights_fact
  GROUP BY
    rms_style_num,
    color_num,
    channel_brand,
    channel_country
  HAVING
    first_received_date_agg IS NOT NULL) AS src
WHERE
  clearance_markdown_insights_by_week_fact.snapshot_date = (
  SELECT
    *
  FROM
    my_params)
  AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI')
    OR clearance_markdown_insights_by_week_fact.first_receipt_date IS NULL
    AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('ONLINE')
      OR LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('STORE')))
  AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA'))
  AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA'))
  AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country)
  AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand);
END
  ;