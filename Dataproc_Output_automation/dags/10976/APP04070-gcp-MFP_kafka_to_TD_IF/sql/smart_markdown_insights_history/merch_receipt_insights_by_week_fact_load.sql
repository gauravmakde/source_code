  CREATE TEMPORARY TABLE IF NOT EXISTS merch_smd_insights_by_week_vt ( snapshot_date DATE NOT NULL,
    rms_style_num STRING NOT NULL,
    color_num STRING NOT NULL,
    channel_country STRING NOT NULL,
    channel_brand STRING NOT NULL,
    selling_channel STRING NOT NULL,
    first_receipt_date DATE,
    last_receipt_date DATE,
    first_rack_date DATE,
    last_rack_date DATE,
    rack_ind STRING ) ;


INSERT INTO
  merch_smd_insights_by_week_vt (snapshot_date,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    selling_channel,
    first_receipt_date,
    last_receipt_date,
    first_rack_date,
    last_rack_date,
    rack_ind) (
  SELECT
    cmibwf.snapshot_date,
    cmibwf.rms_style_num,
    cmibwf.color_num,
    cmibwf.channel_country,
    cmibwf.channel_brand,
    cmibwf.selling_channel,
    MIN(mrdrwf.first_receipt_date) AS first_receipt_date,
    MAX(mrdrwf.last_receipt_date) AS last_receipt_date,
    MIN(mrdrwf.first_rack_date) AS first_rack_date,
    MAX(mrdrwf.last_rack_date) AS last_rack_date,
    MAX(mrdrwf.rack_ind) AS rack_ind
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cmibwf
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
  ON
    cmibwf.snapshot_date = etl.dw_batch_dt
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_receipt_date_reset_applied_week_fact AS mrdrwf
  ON
    LOWER(mrdrwf.rms_style_num) = LOWER(cmibwf.rms_style_num)
    AND LOWER(mrdrwf.color_num) = LOWER(cmibwf.color_num)
    AND LOWER(mrdrwf.channel_country ) = LOWER(cmibwf.channel_country)
    AND LOWER(mrdrwf.channel_brand) = LOWER(cmibwf.channel_brand)
    AND LOWER(mrdrwf .selling_channel) = LOWER(cmibwf.selling_channel)
    AND cmibwf.snapshot_date = mrdrwf.week_end_date
  WHERE
    LOWER(etl.interface_code) = LOWER('CMD_WKLY')
  GROUP BY
    cmibwf.snapshot_date,
    cmibwf.rms_style_num,
    cmibwf.color_num,
    cmibwf.channel_country,
    cmibwf.channel_brand,
    cmibwf.selling_channel);



UPDATE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt
SET
  first_receipt_date = src.first_receipt_date,
  last_receipt_date = src.last_receipt_date,
  first_rack_date = src.first_rack_date,
  rack_ind = src.rack_ind,
  available_to_sell = CAST(trunc(ROUND(DATE_DIFF((
        SELECT
          dw_batch_dt
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
        WHERE
          LOWER(interface_code) = LOWER('CMD_WKLY')), src.first_receipt_date, DAY) / 7.0)) AS INTEGER),
  dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    dw_sys_updt_tmstp_tz= `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 

FROM
  merch_smd_insights_by_week_vt AS src
WHERE
  src.snapshot_date = tgt.snapshot_date
  AND LOWER(src.rms_style_num) = LOWER(tgt.rms_style_num)
  AND LOWER(src.color_num) = LOWER(tgt.color_num)
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);



UPDATE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt
SET
  first_receipt_date = src.first_receipt_date,
  last_receipt_date = src.last_receipt_date,
  first_rack_date = src.first_rack_date,
  rack_ind = src.rack_ind,
  available_to_sell = CAST(trunc(src.available_to_sell) AS INTEGER),
  dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz= `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() 
FROM (
  SELECT
    snapshot_date,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand,
    MIN(first_receipt_date) AS first_receipt_date,
    MAX(last_receipt_date) AS last_receipt_date,
    MIN(first_rack_date) AS first_rack_date,
    MAX(last_rack_date) AS last_rack_date,
    MAX(rack_ind) AS rack_ind,
    ROUND(DATE_DIFF((
        SELECT
          dw_batch_dt
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
        WHERE
          LOWER(interface_code) = LOWER('CMD_WKLY')), MIN(first_receipt_date), DAY) / 7.0) AS available_to_sell
  FROM
    merch_smd_insights_by_week_vt AS mswvt
  GROUP BY
    snapshot_date,
    rms_style_num,
    color_num,
    channel_country,
    channel_brand) AS src
WHERE
  src.snapshot_date = tgt.snapshot_date
  AND LOWER(src.rms_style_num) = LOWER(tgt.rms_style_num)
  AND LOWER(src.color_num) = LOWER(tgt.color_num)
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand)
  AND (LOWER(tgt.selling_channel) = LOWER('OMNI')
    OR tgt.total_inv_qty = 0);