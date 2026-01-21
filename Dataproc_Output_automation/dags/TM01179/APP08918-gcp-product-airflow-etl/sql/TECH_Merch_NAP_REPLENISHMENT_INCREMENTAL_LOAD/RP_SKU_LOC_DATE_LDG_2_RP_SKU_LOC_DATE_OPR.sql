
BEGIN TRANSACTION;
UPDATE
`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS TARGET
SET
  rp_date_set_cancelled_ind = 'Y',
  rp_date_set_cancelled_desc = 'CANCELLED_FUTURE',
  rp_date_set_cancelled_event_tmstp = CAST(SOURCE.conv_src_event_tmstp AS TIMESTAMP),
  rp_date_set_cancelled_event_tmstp_tz = SOURCE.conv_src_event_tmstp_TZ,
  dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
FROM (
  SELECT
    dim.rms_sku_num,
    dim.location_num,
    dim.on_sale_date,
    dim.off_sale_date,
    LDG.conv_src_event_tmstp,
    LDG.conv_src_event_tmstp_tz
  FROM (
    SELECT
      rmsskuid AS rms_sku_num,
      CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS location_num,
      PARSE_DATE('%F', onsaledate) AS conv_on_sale_date,
      PARSE_DATE('%F', offsaledate) AS conv_off_sale_date,
      CAST(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedtime) AS TIMESTAMP) AS conv_src_event_tmstp,
      `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedtime)) AS conv_src_event_tmstp_tz,
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_ldg
    GROUP BY
      rms_sku_num,
      location_num,
      conv_on_sale_date,
      conv_off_sale_date,
      conv_src_event_tmstp,
      conv_src_event_tmstp_tz) AS LDG
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_date_dim AS dim
  ON
    LOWER(LDG.rms_sku_num) = LOWER(dim.rms_sku_num)
    AND LDG.location_num = dim.location_num
    AND CAST(dim.eff_end_tmstp AS DATE) > CURRENT_DATE('PST8PDT')
    AND dim.on_sale_date >= PARSE_DATE('%F', cast(LDG.conv_src_event_tmstp as string))) AS SOURCE
WHERE
  LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num)
  AND target.location_num = SOURCE.location_num
  AND target.on_sale_date = SOURCE.on_sale_date
  AND target.off_sale_date = SOURCE.off_sale_date
  AND target.dw_sys_load_date BETWEEN DATE_ADD(CURRENT_DATE('PST8PDT'),INTERVAL - 24 MONTH)
  AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 24 MONTH))
  AND LOWER(target.rp_date_set_cancelled_ind) <> LOWER('Y'); 
  

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr (rms_sku_num,
    location_num,
    on_sale_date,
    off_sale_date,
    rp_date_set_cancelled_ind,
    rp_date_set_cancelled_desc,
    rp_date_set_cancelled_event_tmstp,
    src_event_tmstp,
    src_event_tmstp_tz,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp) (
  SELECT
    LDG.rmsskuid AS rms_sku_num,
    CAST(TRUNC(CAST(LDG.locationid AS FLOAT64)) AS INTEGER) AS conv_location_num,
    PARSE_DATE('%F', LDG.onsaledate) AS conv_on_sale_date,
    PARSE_DATE('%F', LDG.offsaledate) AS conv_off_sale_date,
    'N' AS rp_date_set_cancelled_ind,
    '' AS rp_date_set_cancelled_desc,
    CAST(NULL AS TIMESTAMP) AS rp_date_set_cancelled_event_tmstp,
    CAST(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(ldg.lastupdatedtime) AS TIMESTAMP) AS conv_src_event_tmstp,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(ldg.lastupdatedtime)) as conv_src_event_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)),14, ' ') AS BIGINT) AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    -- PARSE_DATE('%F', `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(ldg.lastupdatedtime)) AS dw_sys_load_date,
    DATE( `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(LDG.LASTUPDATEDTIME)) AS DW_SYS_LOAD_DATE,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
  FROM (
    SELECT
      *
    FROM
     `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_ldg
    GROUP BY
      lastupdatedtime,
      rmsskuid,
      locationid,
      onsaledate,
      offsaledate) AS LDG
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS opr
  ON
    LOWER(LDG.rmsskuid) = LOWER(opr.rms_sku_num)
    AND CAST(TRUNC(CAST(LDG.locationid AS FLOAT64)) AS INTEGER) = opr.location_num
    AND PARSE_DATE('%F', LDG.onsaledate) = opr.on_sale_date
    AND CAST(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(ldg.lastupdatedtime) AS TIMESTAMP) = opr.src_event_tmstp
    AND opr.dw_sys_load_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 2 YEAR)
    AND (DATE_ADD(CURRENT_DATE('PST8PDT'),INTERVAL 2 YEAR))
  WHERE
    opr.rms_sku_num IS NULL);
    
UPDATE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS tgt
SET
  rp_date_set_cancelled_ind = 'Y',
  rp_date_set_cancelled_desc = 'CANCELLED_FUTURE',
  rp_date_set_cancelled_event_tmstp = CAST(SRC.conv_src_event_tmstp AS TIMESTAMP),
  rp_date_set_cancelled_event_tmstp_tz = SRC.conv_src_event_tmstp_tz,
  dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
FROM (
  SELECT
    rms_sku_num,
    location_num,
    on_sale_date,
    off_sale_date,
    conv_src_event_tmstp,
    conv_src_event_tmstp_tz,
  FROM (
    SELECT
      opr.rms_sku_num,
      opr.location_num,
      opr.on_sale_date,
      opr.off_sale_date,
      MAX(opr.dw_batch_id) OVER (PARTITION BY opr.rms_sku_num, opr.location_num ORDER BY opr.dw_batch_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_dw_batch_id,
      opr.dw_batch_id AS curr_dw_batch_id,
      LDG.conv_src_event_tmstp,
      LDG.conv_src_event_tmstp_tz,
      LDG.conv_on_sale_date,
      opr.src_event_tmstp
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_date_opr AS opr
    INNER JOIN (
      SELECT
        rmsskuid AS rms_sku_num,
        CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS conv_location_num,
        PARSE_DATE('%F', onsaledate) AS conv_on_sale_date,
        PARSE_DATE('%F', offsaledate) AS conv_off_sale_date,
        `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedtime) AS conv_src_event_tmstp,
        `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedtime)) AS conv_src_event_tmstp_tz
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_date_ldg
      GROUP BY
        rms_sku_num,
        conv_location_num,
        conv_on_sale_date,
        conv_off_sale_date,
        conv_src_event_tmstp,
        conv_src_event_tmstp_tz) AS LDG
    ON
      LOWER(opr.rms_sku_num) = LOWER(LDG.rms_sku_num)
      AND opr.location_num = LDG.conv_location_num
    QUALIFY
      opr.on_sale_date > LDG.conv_on_sale_date
      AND opr.src_event_tmstp < CAST(LDG.conv_src_event_tmstp AS TIMESTAMP)
      AND (MAX(opr.dw_batch_id) OVER (PARTITION BY opr.rms_sku_num, opr.location_num ORDER BY opr.dw_batch_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) <> opr.dw_batch_id) AS t2
  GROUP BY
    rms_sku_num,
    location_num,
    on_sale_date,
    off_sale_date,
    conv_src_event_tmstp,
    conv_src_event_tmstp_tz) AS SRC
WHERE
  LOWER(tgt.rms_sku_num) = LOWER(SRC.rms_sku_num)
  AND tgt.location_num = SRC.location_num
  AND tgt.on_sale_date = SRC.on_sale_date
  AND tgt.off_sale_date = SRC.off_sale_date
  AND LOWER(tgt.rp_date_set_cancelled_ind) <> LOWER('Y');
COMMIT TRANSACTION;