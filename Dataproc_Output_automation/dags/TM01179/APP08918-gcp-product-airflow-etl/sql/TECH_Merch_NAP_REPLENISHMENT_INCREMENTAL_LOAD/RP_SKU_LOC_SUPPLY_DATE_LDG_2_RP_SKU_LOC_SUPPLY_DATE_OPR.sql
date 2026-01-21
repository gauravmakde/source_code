BEGIN TRANSACTION;

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_supply_dateset_opr (rms_sku_num,
    location_num,
    on_supply_date,
    off_supply_date,
    rp_supply_date_set_cancelled_ind,
    rp_supply_date_set_cancelled_desc,
    rp_supply_date_set_cancelled_event_tmstp,
    src_event_tmstp,
    src_event_tmstp_tz,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp) (
  SELECT
    LDG.rmsskuid AS rms_sku_num,
    CAST(TRUNC(CAST(LDG.locationid AS FLOAT64))  AS INTEGER) AS conv_location_num,
    PARSE_DATE('%F', LDG.onsupplydate) AS conv_on_supply_date,
    PARSE_DATE('%F', LDG.offsupplydate) AS conv_off_supply_date,
    'N' AS rp_supply_date_set_cancelled_ind,
    '' AS rp_supply_date_set_cancelled_desc,
    CAST(NULL AS TIMESTAMP) AS rp_supply_date_set_cancelled_event_tmstp,
    CAST( `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg.lastupdatedtime) as timestamp) AS conv_src_event_tmstp,
     `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE( `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg.lastupdatedtime)) as conv_src_event_tmstp_tz,
    CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)), 14, ' ') AS BIGINT) AS dw_batch_id,
    CURRENT_DATE('PST8PDT') AS dw_batch_date,
    cast( `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg.lastupdatedtime)as date) AS dw_sys_load_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
  FROM (
    SELECT
      *
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_ldg
    GROUP BY
      lastupdatedtime,
      rmsskuid,
      locationid,
      onsupplydate,
      offsupplydate) AS LDG
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_supply_dateset_opr AS opr
  ON
    LOWER(LDG.rmsskuid) = LOWER(opr.rms_sku_num)
    AND CAST(TRUNC(CAST(LDG.locationid AS FLOAT64))AS INTEGER) = opr.location_num
    AND PARSE_DATE('%F', LDG.onsupplydate) = opr.on_supply_date
    AND PARSE_DATE('%F', LDG.offsupplydate ) = opr.off_supply_date
    AND cast( `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(ldg.lastupdatedtime) as timestamp) = opr.src_event_tmstp
    AND opr.dw_sys_load_date BETWEEN DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH)
    AND (DATE_ADD(CURRENT_DATE('PST8PDT'),INTERVAL 24 MONTH))
  WHERE
    opr.rms_sku_num IS NULL);


UPDATE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_sku_loc_supply_dateset_opr AS TARGET 
SET
  rp_supply_date_set_cancelled_ind = 'Y',
  rp_supply_date_set_cancelled_desc = 'CANCELLED_FUTURE',
  rp_supply_date_set_cancelled_event_tmstp = CAST(SOURCE.conv_src_event_tmstp AS TIMESTAMP),
  rp_supply_date_set_cancelled_event_tmstp_tz = SOURCE.conv_src_event_tmstp_tz,
  dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
FROM (
  SELECT
    dim.rms_sku_num,
    dim.location_num,
    dim.on_supply_date,
    dim.off_supply_date,
    LDG.conv_src_event_tmstp,
    LDG.conv_src_event_tmstp_tz
  FROM (
    SELECT
      rmsskuid AS rms_sku_num,
      CAST(TRUNC(CAST(locationid AS FLOAT64))AS INTEGER) AS location_num,
      PARSE_DATE('%F', onsupplydate) AS conv_on_supply_date,
      PARSE_DATE('%F', offsupplydate) AS conv_off_supply_date,


       `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime) AS conv_src_event_tmstp,

      `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE( `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedtime)) AS conv_src_event_tmstp_tz
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_sku_loc_supply_dateset_ldg
    GROUP BY
      rms_sku_num,
      location_num,
      conv_on_supply_date,
      conv_off_supply_date,
      conv_src_event_tmstp,
      conv_src_event_tmstp_tz) AS LDG
  INNER JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_supply_dateset_dim AS dim
  ON
    LOWER(LDG.rms_sku_num) = LOWER(dim.rms_sku_num)
    AND LDG.location_num = dim.location_num
    AND CAST(dim.eff_end_tmstp AS DATE) > CURRENT_DATE('PST8PDT')
    AND cast(dim.on_supply_date as string) >= LDG.conv_src_event_tmstp 
    ) AS SOURCE
WHERE
  LOWER(target.rms_sku_num) = LOWER(SOURCE.rms_sku_num)
  AND target.location_num = SOURCE.location_num
  AND target.on_supply_date = SOURCE.on_supply_date
  AND target.off_supply_date = SOURCE.off_supply_date
  AND target.dw_sys_load_date BETWEEN DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH)
  AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 24 MONTH))
  AND LOWER(target.rp_supply_date_set_cancelled_ind) <> LOWER('Y');


COMMIT TRANSACTION;