BEGIN
DECLARE
  _ERROR_CODE INT64;
DECLARE
  _ERROR_MESSAGE STRING; /*SET QUERY_BAND = 'App_ID=app04070;DAG_ID=smartmarkdown_insights_history_10976_tech_nap_merch;Task_Name=run_load_common_inventory;'*/
BEGIN
SET
  _ERROR_CODE = 0;

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact
   (snapshot_date,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    selling_channel,
    total_inv_qty,
    total_inv_dollars,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz,
    dw_sys_updt_tmstp,
     dw_sys_updt_tmstp_tz) 
     (
  SELECT
    cmsq.metrics_date AS snapshot_date,
    cmsq.rms_style_num,
    cmsq.color_num,
    cmsq.channel_country,
    cmsq.channel_brand,
    cmsq.selling_channel,
    cmsq.total_inv_qty,
    cmsq.total_inv_dollars,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as  dw_sys_updt_tmstp_tz

  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_stock_quantity_end_of_wk_hist_vw AS cmsq
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
  ON
    cmsq.metrics_date = etl.dw_batch_dt
    AND LOWER(etl.interface_code ) = LOWER('CMD_WKLY')); 

    EXCEPTION WHEN ERROR THEN SET _ERROR_CODE = 1;
     SET _ERROR_MESSAGE = @@error.message;
END
  ; --COLLECT STATISTICS COLUMN(SNAPSHOT_DATE),
--   COLUMN(RMS_STYLE_NUM),
--   COLUMN(COLOR_NUM),
--   COLUMN(CHANNEL_COUNTRY),
--   COLUMN(CHANNEL_BRAND),
--   COLUMN(SELLING_CHANNEL),
--   COLUMN(RMS_STYLE_NUM,
--     COLOR_NUM,
--     CHANNEL_COUNTRY,
--     CHANNEL_BRAND,
--     SELLING_CHANNEL),
--   COLUMN(PARTITION)
-- ON
--   `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_HIST_FACT;

BEGIN
SET
  _ERROR_CODE = 0;

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact (snapshot_date,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    selling_channel,
    total_inv_qty,
    total_inv_dollars,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz,
    dw_sys_updt_tmstp,
    dw_sys_updt_tmstp_tz)
     (
  SELECT
    DISTINCT cwibwf.snapshot_date,
    cwibwf.rms_style_num,
    cwibwf.color_num,
    cwibwf.channel_country,
    cwibwf.channel_brand,
    'ONLINE' AS selling_channel,
    0 AS total_inv_qty,
    0 AS total_inv_dollars,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp,
     `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_updt_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as  dw_sys_updt_tmstp_tz

  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cwibwf
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
  ON
    cwibwf.snapshot_date = etl.dw_batch_dt
    AND LOWER(etl.interface_code ) = LOWER('CMD_WKLY')
  LEFT JOIN (
    SELECT
      cmibh.channel_country,
      cmibh.channel_brand,
      cmibh.rms_style_num,
      cmibh.color_num
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cmibh
    INNER JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etlbd
    ON
      cmibh.snapshot_date = etlbd.dw_batch_dt
      AND LOWER(etlbd.interface_code ) = LOWER('CMD_WKLY')
    WHERE
      LOWER(cmibh.selling_channel) = LOWER('ONLINE')) AS online
  ON
    LOWER(COALESCE(cwibwf.rms_style_num, 'NA')) = LOWER(COALESCE(online.rms_style_num, 'NA'))
    AND LOWER(COALESCE(cwibwf.color_num, 'NA')) = LOWER(COALESCE(online.color_num, 'NA'))
    AND LOWER(cwibwf.channel_brand) = LOWER(online.channel_brand)
    AND LOWER(cwibwf.channel_country) = LOWER(online .channel_country)
  WHERE
    online.channel_country IS NULL); EXCEPTION
    WHEN ERROR THEN SET _ERROR_CODE = 1; SET _ERROR_MESSAGE = @@error.message;
END
  ; --COLLECT STATISTICS COLUMN(SNAPSHOT_DATE),
--   COLUMN(RMS_STYLE_NUM),
--   COLUMN(COLOR_NUM),
--   COLUMN(CHANNEL_COUNTRY),
--   COLUMN(CHANNEL_BRAND),
--   COLUMN(SELLING_CHANNEL),
--   COLUMN(RMS_STYLE_NUM,
--     COLOR_NUM,
--     CHANNEL_COUNTRY,
--     CHANNEL_BRAND,
--     SELLING_CHANNEL),
--   COLUMN(PARTITION)
-- ON
--   `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_HIST_FACT;
BEGIN
SET
  _ERROR_CODE = 0;
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact (snapshot_date,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    selling_channel,
    total_inv_qty,
    total_inv_dollars,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz,
    dw_sys_updt_tmstp,
    dw_sys_updt_tmstp_tz) 
    (
  SELECT
    DISTINCT cwibwf.snapshot_date,
    cwibwf.rms_style_num,
    cwibwf.color_num,
    cwibwf.channel_country,
    cwibwf.channel_brand,
    'STORE' AS selling_channel,
    0 AS total_inv_qty,
    0 AS total_inv_dollars,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp,
     `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_updt_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as  dw_sys_updt_tmstp_tz

  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cwibwf
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
  ON
    cwibwf.snapshot_date = etl.dw_batch_dt
    AND LOWER(etl.interface_code ) = LOWER('CMD_WKLY')
  LEFT JOIN (
    SELECT
      cmibh.channel_country,
      cmibh.channel_brand,
      cmibh.rms_style_num,
      cmibh.color_num
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cmibh
    INNER JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etlbd
    ON
      cmibh.snapshot_date = etlbd.dw_batch_dt
      AND LOWER(etlbd.interface_code ) = LOWER('CMD_WKLY')
    WHERE
      LOWER(cmibh.selling_channel) = LOWER('STORE')) AS store
  ON
    LOWER(COALESCE(cwibwf.rms_style_num, 'NA')) = LOWER(COALESCE(store .rms_style_num, 'NA'))
    AND LOWER(COALESCE(cwibwf.color_num, 'NA')) = LOWER(COALESCE(store.color_num, 'NA'))
    AND LOWER(cwibwf.channel_brand) = LOWER(store.channel_brand)
    AND LOWER(cwibwf.channel_country) = LOWER(store.channel_country )
  WHERE
    store.channel_country IS NULL); EXCEPTION
    WHEN ERROR THEN SET _ERROR_CODE = 1; SET _ERROR_MESSAGE = @@error.message;
END
  ; --COLLECT STATISTICS COLUMN(SNAPSHOT_DATE),
--   COLUMN(RMS_STYLE_NUM),
--   COLUMN(COLOR_NUM),
--   COLUMN(CHANNEL_COUNTRY),
--   COLUMN(CHANNEL_BRAND),
--   COLUMN(SELLING_CHANNEL),
--   COLUMN(RMS_STYLE_NUM,
--     COLOR_NUM,
--     CHANNEL_COUNTRY,
--     CHANNEL_BRAND,
--     SELLING_CHANNEL),
--   COLUMN(PARTITION)
-- ON
--   `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_HIST_FACT;
BEGIN
SET
  _ERROR_CODE = 0;
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact (snapshot_date,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    selling_channel,
    total_inv_qty,
    total_inv_dollars,
    dw_sys_load_tmstp,
        dw_sys_load_tmstp_tz,
            dw_sys_updt_tmstp,
             dw_sys_updt_tmstp_tz ) (
  SELECT
    cwibwf.snapshot_date,
    cwibwf.rms_style_num,
    cwibwf.color_num,
    cwibwf.channel_country,
    cwibwf.channel_brand,
    'OMNI' AS selling_channel,
    SUM(cwibwf.total_inv_qty) AS total_inv_qty,
    SUM(cwibwf.total_inv_dollars) AS total_inv_dollars,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_load_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
    CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS dw_sys_updt_tmstp,
    `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as  dw_sys_updt_tmstp_tz
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cwibwf
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
  ON
    cwibwf.snapshot_date = etl.dw_batch_dt
    AND LOWER(etl.interface_code ) = LOWER('CMD_WKLY')
  WHERE
    LOWER(cwibwf.channel_country) <> LOWER('CA')
    OR LOWER(cwibwf.channel_brand) <> LOWER('NORDSTROM_RACK')
  GROUP BY
    cwibwf.snapshot_date,
    cwibwf.rms_style_num,
    cwibwf.color_num,
    cwibwf.channel_country,
    cwibwf.channel_brand,
    selling_channel,
    dw_sys_load_tmstp,
    dw_sys_load_tmstp_tz,
    dw_sys_updt_tmstp,
    dw_sys_updt_tmstp_tz);

     EXCEPTION
     WHEN ERROR THEN SET _ERROR_CODE = 1; SET _ERROR_MESSAGE = @@error.message;
END
  ; --COLLECT STATISTICS COLUMN(SNAPSHOT_DATE),
--   COLUMN(RMS_STYLE_NUM),
--   COLUMN(COLOR_NUM),
--   COLUMN(CHANNEL_COUNTRY),
--   COLUMN(CHANNEL_BRAND),
--   COLUMN(SELLING_CHANNEL),
--   COLUMN(RMS_STYLE_NUM,
--     COLOR_NUM,
--     CHANNEL_COUNTRY,
--     CHANNEL_BRAND,
--     SELLING_CHANNEL),
--   COLUMN(PARTITION)
-- ON
--   `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_FCT.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_HIST_FACT; /*SET QUERY_BAND = NONE FOR SESSION;*/
END;