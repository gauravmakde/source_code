INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_catalog_opr
(
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
)
SELECT
 pc.catalogId AS catalog_num,
 pc.catalogName AS catalog_name,
 pc.catalogDescription AS catalog_desc,
 '-1' AS npin, -- only part of item catalog events
 '-1' AS rms_sku_num, -- only part of item catalog events
 CASE
  WHEN LOWER(pc.isDeleted)=LOWER('true') THEN 'Y'
  WHEN LOWER(pc.isDeleted)=LOWER('false') THEN 'N'
  ELSE 'N'
 END AS is_deleted,
 CASE WHEN LOWER(pc.isDeleted) = LOWER('true') THEN 'catalog source event' ELSE '' END AS del_reason,
 CAST(CASE WHEN LOWER(pc.isDeleted) = LOWER('true') THEN pc.eventTime ELSE NULL END AS TIMESTAMP) AS del_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(
        CAST(
          FORMAT_TIMESTAMP (
            '%F %H:%M:%E6S',
            CAST(
              CASE
                WHEN LOWER(pc.isDeleted) = LOWER('true') THEN pc.eventtime
                ELSE NULL
              END AS DATETIME
            )
          ) AS DATETIME
        ) AS STRING
      )) AS del_tmstp_tz,
 CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(pc.eventTime) AS TIMESTAMP) AS event_time,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE (CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(pc.eventTime) AS STRING)) AS event_time_tz,
 CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(pc.eventTime) AS TIMESTAMP) AS catalog_start_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE (CAST(`{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(pc.eventTime) AS STRING)) AS catalog_start_tmstp_tz,
 CAST(TRUNC(CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()) AS FLOAT64)) AS INT64) AS dw_batch_id,
 CURRENT_DATE('PST8PDT') AS dw_batch_date,
 CURRENT_DATE('PST8PDT') AS dw_sys_load_date,
 CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp,
 CURRENT_DATETIME('PST8PDT') AS dw_sys_updt_tmstp
 FROM
(SELECT eventTime
 ,catalogId
 ,catalogName
 ,catalogDescription
 ,isDeleted
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_catalog_ldg 
 GROUP BY 1,2,3,4,5 ) pc
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_catalog_opr opr
ON LOWER(pc.catalogId) = LOWER(OPR.catalog_num)
AND PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(cast(cast(pc.eventTime as timestamp)as string))) = OPR.event_time
AND OPR.dw_sys_load_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH) AND DATE_ADD(CURRENT_DATE(), INTERVAL 24 MONTH)
WHERE OPR.catalog_num IS NULL;

