--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-opr;AppSubArea=CATALOG;' UPDATE FOR SESSION;
-- Insert records from PRODUCT_ITEM_CATALOG_LDG into the operational table
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_catalog_opr (
    catalog_num,
    catalog_name,
    catalog_desc,
    npin,
    rms_sku_num,
    is_deleted,
    del_reason,
    del_tmstp,
    del_tmstp_tz,
    event_time,
    event_time_tz,
    catalog_start_tmstp,
    catalog_start_tmstp_tz,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
  ) (
    SELECT
      pic.catalogid AS catalog_num,
      CAST(NULL AS STRING) AS catalog_name,
      CAST(NULL AS STRING) AS catalog_desc,
      pic.npin,
      pic.rmsskuid AS rms_sku_num,
      CASE
        WHEN LOWER(pic.isremoved) = LOWER('true') THEN 'Y'
        ELSE 'N'
      END AS is_deleted,
      CASE
        WHEN LOWER(pic.isremoved) = LOWER('true') THEN 'item catalog source event'
        ELSE NULL
      END AS del_reason,
      CAST(
        CAST(
          FORMAT_TIMESTAMP (
            '%F %H:%M:%E6S',
            CAST(
              CASE
                WHEN LOWER(pic.isremoved) = LOWER('true') THEN pic.eventtime
                ELSE NULL
              END AS DATETIME
            )
          ) AS DATETIME
        ) AS TIMESTAMP
      ) AS del_tmstp,
      `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(
        CAST(
          FORMAT_TIMESTAMP (
            '%F %H:%M:%E6S',
            CAST(
              CASE
                WHEN LOWER(pic.isremoved) = LOWER('true') THEN pic.eventtime
                ELSE NULL
              END AS DATETIME
            )
          ) AS DATETIME
        ) AS STRING
      )) as del_tmstp_tz,
      CAST(
        `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP` (pic.eventtime) AS TIMESTAMP
      ) AS event_time,
      `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE` (CAST(
        `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP` (pic.eventtime) AS STRING
      )) AS event_time_tz,
      CAST(NULL AS TIMESTAMP) AS catalog_start_tmstp,
      CAST(NULL AS STRING)  as catalog_start_tmstp_tz,
      CAST(
        RPAD (
          FORMAT_TIMESTAMP (
            '%Y%m%d%I%M%S',
            CAST(
              FORMAT_TIMESTAMP ('%F %H:%M:%E6S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME
            )
          ),
          14,
          ' '
        ) AS BIGINT
      ) AS dw_batch_id,
      CURRENT_DATE('PST8PDT') AS dw_batch_date,
      CURRENT_DATE('PST8PDT') AS dw_sys_load_date,
      CAST(
        FORMAT_TIMESTAMP ('%F %H:%M:%S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME
      ) AS dw_sys_load_tmstp,
      CAST(
        FORMAT_TIMESTAMP ('%F %H:%M:%E6S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME
      ) AS dw_sys_updt_tmstp
    FROM
      (
        SELECT
          eventtime,
          catalogid,
          npin,
          rmsskuid,
          isremoved
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_catalog_ldg
        GROUP BY
          catalogid,
          npin,
          rmsskuid,
          isremoved,
          eventtime
      ) AS pic
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_catalog_opr AS opr ON LOWER(pic.catalogid) = LOWER(opr.catalog_num)
      AND LOWER(pic.npin) = LOWER(opr.npin)
      AND LOWER(COALESCE(pic.rmsskuid, '-1')) = LOWER(COALESCE(opr.rms_sku_num, '-1'))
      AND CAST(
        `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP` (pic.eventtime) AS TIMESTAMP
      ) = opr.event_time
      AND opr.dw_sys_load_date BETWEEN DATE_ADD (CURRENT_DATE('PST8PDT'), INTERVAL - 24 MONTH) AND (
        DATE_ADD (CURRENT_DATE('PST8PDT'), INTERVAL 24 MONTH)
      )
    WHERE
      opr.npin IS NULL
  );

--COLLECT STATISTICS COLUMN(catalog_num), COLUMN(npin) ON PRD_NAP_OPR.PRODUCT_CATALOG_OPR