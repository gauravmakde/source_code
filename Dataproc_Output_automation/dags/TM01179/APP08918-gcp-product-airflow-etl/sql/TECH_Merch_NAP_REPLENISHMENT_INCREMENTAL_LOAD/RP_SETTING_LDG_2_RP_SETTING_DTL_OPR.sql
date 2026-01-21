
BEGIN TRANSACTION;

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_setting_dtl_opr (
    rms_sku_num,
    location_num,
    replenishment_setting_start_date,
    replenishment_setting_type_code,
    replenishment_setting_value_desc,
    replenishment_setting_level_code,
    replenishment_setting_cancelled_ind,
    replenishment_setting_cancelled_desc,
    replenishment_setting_cancelled_event_tmstp,
    replenishment_setting_cancelled_event_tmstp_tz,
    src_event_tmstp,
    src_event_tmstp_tz,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
  ) (
    SELECT
      LDG.rmsskuid AS rms_sku_num,
      CAST(TRUNC(CAST(LDG.locationid AS FLOAT64)) AS INTEGER) AS conv_location_num,
      PARSE_DATE ('%F', LDG.startdate) AS conv_replenishment_setting_start_date,
      LDG.replenishmentsettingtype AS replenishment_setting_type_code,
      LDG.replenishmentsettingvalue AS replenishment_setting_value_desc,
      LDG.replenishmentsettingsetlevel AS replenishment_setting_level_code,
      'N' AS replenishment_setting_cancelled_ind,
      '' AS replenishment_setting_cancelled_desc,
      CAST(NULL AS TIMESTAMP) AS replenishment_setting_cancelled_event_tmstp,
      CAST(NULL AS string) AS replenishment_setting_cancelled_event_tmstp_tz,
      CAST(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(LDG.lastupdatedtime) AS TIMESTAMP) AS conv_src_event_tmstp,
      `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(LDG.lastupdatedtime) AS src_updated_tmstp_tz,
      CAST( RPAD ( FORMAT_TIMESTAMP ('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP ('%F %H:%M:%E6S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME ) ), 14, ' ' ) AS BIGINT ) AS dw_batch_id,
      CURRENT_DATE AS dw_batch_date,
      -- PARSE_DATE ( '%F', `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(LDG.lastupdatedtime) ) AS dw_sys_load_date,
      DATE( `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(LDG.LASTUPDATEDTIME)) AS DW_SYS_LOAD_DATE,
      CAST( FORMAT_TIMESTAMP ('%F %H:%M:%S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME ) AS dw_sys_load_tmstp,
      CAST( FORMAT_TIMESTAMP ('%F %H:%M:%S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME ) AS dw_sys_updt_tmstp
    FROM
      (
        SELECT
          *
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_settings_ldg
        GROUP BY
          lastupdatedtime,
          rmsskuid,
          locationid,
          startdate,
          endtime,
          replenishmentsettingtype,
          replenishmentsettingvalue,
          replenishmentsettingsetlevel
      ) AS LDG
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_setting_dtl_opr AS opr ON LOWER(LDG.rmsskuid) = LOWER(opr.rms_sku_num)
      AND CAST(TRUNC(CAST(LDG.locationid AS FLOAT64)) AS INTEGER) = opr.location_num
      AND PARSE_DATE ('%F', LDG.startdate) = opr.replenishment_setting_start_date
      AND CAST( `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(LDG.lastupdatedtime) AS TIMESTAMP ) = opr.src_event_tmstp
      AND opr.dw_sys_load_date BETWEEN DATE_SUB (CURRENT_DATE, INTERVAL 2 YEAR) AND (DATE_ADD (CURRENT_DATE, INTERVAL 2 YEAR))
    WHERE
      opr.rms_sku_num IS NULL
      AND LDG.endtime IS NULL
  );


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_setting_dtl_opr AS tgt 
SET
  replenishment_setting_cancelled_ind = 'Y',
  replenishment_setting_cancelled_desc = SRC.replenishment_setting_cancelled_desc,
  replenishment_setting_cancelled_event_tmstp = CAST( SRC.replenishment_setting_cancelled_event_tmstp AS TIMESTAMP ),
  replenishment_setting_cancelled_event_tmstp_tz = `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(SRC.replenishment_setting_cancelled_event_tmstp),
  dw_sys_updt_tmstp = CAST( FORMAT_TIMESTAMP ('%F %H:%M:%S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME )
FROM
  (
    SELECT DISTINCT
      opr.rms_sku_num,
      opr.location_num,
      opr.replenishment_setting_type_code,
      opr.replenishment_setting_start_date,
      opr.src_event_tmstp,
      LDG.replenishment_setting_cancelled_event_tmstp,
      LDG.src_cancelled_event_tmstp,
      MAX(opr.dw_batch_id) OVER (
        PARTITION BY
          opr.rms_sku_num,
          opr.location_num
        ORDER BY
          opr.dw_batch_id RANGE BETWEEN UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      ) AS max_dw_batch_id,
      CASE
        WHEN opr.replenishment_setting_start_date > CAST(
          LDG.replenishment_setting_cancelled_event_tmstp AS DATE
        ) THEN 'CANCELLED_FUTURE'
        ELSE 'CANCELLED_CURRENT'
      END AS replenishment_setting_cancelled_desc
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.rp_setting_dtl_opr AS opr
      INNER JOIN (
        SELECT DISTINCT
          rmsskuid AS rms_sku_num,
          CAST(TRUNC(CAST(locationid AS FLOAT64)) AS INTEGER) AS conv_location_num,
          PARSE_DATE ('%F', startdate) AS conv_replenishment_setting_start_date,
          replenishmentsettingtype AS replenishment_setting_type_code,
          `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(endtime) AS replenishment_setting_cancelled_event_tmstp,
          `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedtime) AS src_cancelled_event_tmstp
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_settings_ldg AS ldg
        WHERE
          endtime IS NOT NULL QUALIFY (
            ROW_NUMBER() OVER (
              PARTITION BY
                rms_sku_num,
                conv_location_num,
                replenishment_setting_type_code,
                conv_replenishment_setting_start_date,
                replenishment_setting_cancelled_event_tmstp
              ORDER BY
                src_cancelled_event_tmstp DESC
            )
          ) = 1
      ) AS LDG ON LOWER(opr.rms_sku_num) = LOWER(LDG.rms_sku_num)
      AND opr.location_num = LDG.conv_location_num
      AND LOWER(opr.replenishment_setting_type_code) = LOWER(LDG.replenishment_setting_type_code)
      AND opr.replenishment_setting_start_date = LDG.conv_replenishment_setting_start_date
  ) AS SRC
WHERE
  LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND SRC.location_num = tgt.location_num
  AND LOWER(SRC.replenishment_setting_type_code) = LOWER(tgt.replenishment_setting_type_code)
  AND SRC.replenishment_setting_start_date = tgt.replenishment_setting_start_date
  AND SRC.src_event_tmstp = tgt.src_event_tmstp
  AND CAST(SRC.src_cancelled_event_tmstp AS TIMESTAMP) >= tgt.src_event_tmstp
  AND LOWER(tgt.replenishment_setting_cancelled_ind) <> LOWER('Y');


COMMIT TRANSACTION;