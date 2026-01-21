
--COLLECT STATISTICS                    -- DEFAULT SYSTEM SAMPLE PERCENT                    -- DEFAULT SYSTEM THRESHOLD PERCENT             COLUMN ( RMSSKUID ,ORGCHANNELCODE ,STARTDATE ) ,             COLUMN ( RMSSKUID, ORGCHANNELCODE ) ,             COLUMN ( STARTDATE ) ,             COLUMN ( ENDTIME ) ,             COLUMN ( LASTUPDATEDTIME)                 ON PRD_NAP_STG.INV_DEPLOYMENT_RULE_LDG;
BEGIN TRANSACTION;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.inv_deployment_rule_opr (rms_sku_num, channel_num, deployment_rule_start_date,
 deployment_type_code, deployment_rule_set_level_code, changed_by_user_id, deployment_rule_cancelled_ind,
 deployment_rule_cancelled_desc, 
 deployment_rule_cancelled_event_tmstp,
 deployment_rule_cancelled_event_tmstp_tz, 
 src_event_tmstp, src_event_tmstp_tz,dw_batch_id, dw_batch_date,
 dw_sys_load_date, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT ldg.rmsskuid AS rms_sku_num,
  CAST(trunc(cast(ldg.orgchannelcode as float64)) AS INTEGER) AS conv_channel_num,
  PARSE_DATE('%F', ldg.startdate) AS conv_deployment_rule_start_date,
  ldg.deploymenttype AS deployment_type_code,
  ldg.deploymentrulesetlevel AS deployment_rule_set_level_code,
  ldg.createdbyid AS changed_by_user_id,
  'N' AS deployment_rule_cancelled_ind,
  '' AS deployment_rule_cancelled_desc,
  CAST(NULL AS TIMESTAMP) AS deployment_rule_cancelled_event_tmstp,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS deployment_rule_cancelled_event_tmstp_tz,
  CAST(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.lastupdatedtime) AS TIMESTAMP) AS conv_src_event_tmstp,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.lastupdatedtime)) AS src_event_tmstp_tz,
  CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)),
    14, ' ') AS BIGINT) AS dw_batch_id,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  -- PARSE_DATE('%F', `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.lastupdatedtime)) AS dw_sys_load_date,
  DATE( `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(LDG.LASTUPDATEDTIME)) AS DW_SYS_LOAD_DATE,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_ldg AS ldg
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.inv_deployment_rule_opr AS opr ON LOWER(ldg.rmsskuid) = LOWER(opr.rms_sku_num) AND CAST(trunc(cast(ldg.orgchannelcode as float64)) AS INTEGER)
       = opr.channel_num AND PARSE_DATE('%F', ldg.startdate) = opr.deployment_rule_start_date AND CAST(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.lastupdatedtime) AS TIMESTAMP)
     = opr.src_event_tmstp AND opr.dw_sys_load_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 2 YEAR) AND (DATE_ADD(CURRENT_DATE('PST8PDT')
      ,INTERVAL 2 YEAR))
 WHERE opr.rms_sku_num IS NULL
  AND ldg.endtime IS NULL);

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.inv_deployment_rule_opr AS tgt  SET
    deployment_rule_cancelled_ind = 'Y',
    deployment_rule_cancelled_desc = SRC.deployment_rule_cancelled_desc,
    deployment_rule_cancelled_event_tmstp = CAST(SRC.deployment_rule_cancelled_event_tmstp AS TIMESTAMP),
    deployment_rule_cancelled_event_tmstp_tz = SRC.deployment_rule_cancelled_event_tmstp_tz,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
    FROM (SELECT DISTINCT opr.rms_sku_num, opr.channel_num, opr.deployment_rule_start_date, opr.src_event_tmstp, LDG.deployment_rule_cancelled_event_tmstp,LDG.deployment_rule_cancelled_event_tmstp_tz, LDG.src_cancelled_event_tmstp, CASE WHEN opr.deployment_rule_start_date > CAST(LDG.deployment_rule_cancelled_event_tmstp AS DATE) THEN 'CANCELLED_FUTURE' ELSE 'CANCELLED_CURRENT' END AS deployment_rule_cancelled_desc
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.inv_deployment_rule_opr AS opr
            INNER JOIN (SELECT DISTINCT rmsskuid AS rms_sku_num, CAST(trunc(cast(orgchannelcode as float64)) AS INTEGER) AS conv_channel_num, PARSE_DATE('%F', startdate) AS conv_deployment_rule_start_date, `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(endtime) AS deployment_rule_cancelled_event_tmstp,`{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(endtime)) as deployment_rule_cancelled_event_tmstp_tz,   `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(lastupdatedtime) AS src_cancelled_event_tmstp
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.inv_deployment_rule_ldg AS ldg
                WHERE endtime IS NOT NULL
                QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, conv_channel_num, conv_deployment_rule_start_date, deployment_rule_cancelled_event_tmstp ORDER BY src_cancelled_event_tmstp DESC)) = 1) AS LDG ON LOWER(opr.rms_sku_num) = LOWER(LDG.rms_sku_num) AND opr.channel_num = LDG.conv_channel_num AND opr.deployment_rule_start_date = LDG.conv_deployment_rule_start_date) AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.channel_num = tgt.channel_num AND SRC.deployment_rule_start_date = tgt.deployment_rule_start_date AND SRC.src_event_tmstp = tgt.src_event_tmstp AND LOWER(tgt.deployment_rule_cancelled_ind) <> LOWER('Y');
COMMIT TRANSACTION;

--COLLECT STATISTICS                    -- DEFAULT SYSTEM SAMPLE PERCENT                    -- DEFAULT SYSTEM THRESHOLD PERCENT             COLUMN ( RMS_SKU_NUM ,CHANNEL_NUM ,DEPLOYMENT_RULE_START_DATE ) ,             COLUMN ( RMS_SKU_NUM, CHANNEL_NUM ) ,             COLUMN ( DEPLOYMENT_TYPE_CODE ) ,             COLUMN ( DEPLOYMENT_RULE_SET_LEVEL_CODE ) ,             COLUMN ( DEPLOYMENT_RULE_CANCELLED_EVENT_TMSTP ) ,             COLUMN ( SRC_EVENT_TMSTP),             COLUMN ( DW_BATCH_DATE )                 ON PRD_NAP_OPR.INV_DEPLOYMENT_RULE_OPR;

