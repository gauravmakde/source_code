
--COLLECT STATISTICS                    -- default SYSTEM SAMPLE PERCENT                    -- default SYSTEM THRESHOLD PERCENT             COLUMN ( rmsskunum,sellingcountry,sellingbrand,sellingchannel ) ,             COLUMN ( rmsskunum,sellingcountry,sellingbrand ) ,             COLUMN ( sellingchannel ) ,             COLUMN ( endtime ) ,             COLUMN ( lastupdatedtimestamp)                 ON PRD_NAP_STG.PRODUCT_ITEM_SELLING_RIGHTS_LDG;
BEGIN TRANSACTION;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_item_selling_rights_opr (rms_sku_num, channel_country, channel_brand, selling_channel,
 selling_rights_start_tmstp,selling_rights_start_tmstp_tz, store_num, sellable_audience, is_sellable_ind, selling_rights_cancelled_ind,
 selling_rights_cancelled_desc, selling_rights_cancelled_event_tmstp,selling_rights_cancelled_event_tmstp_tz, src_event_tmstp,src_event_tmstp_tz, dw_batch_id, dw_batch_date,
 dw_sys_load_date, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT ldg.rmsskunum AS rms_sku_num,
  ldg.sellingcountry AS channel_country,
  ldg.sellingbrand AS channel_brand,
  ldg.sellingchannel AS selling_channel,
  CAST(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.starttime) AS TIMESTAMP) AS conv_selling_rights_start_tmstp,
 `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.starttime)) as selling_rights_start_tmstp_tz,
  ldg.storenum AS store_num,
  ldg.sellableaudience AS sellable_audience,
  ldg.issellable AS is_sellable_ind,
  'N' AS selling_rights_cancelled_ind,
  '' AS selling_rights_cancelled_desc,
  CAST(NULL AS TIMESTAMP) AS selling_rights_cancelled_event_tmstp,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(CAST(CAST(NULL AS TIMESTAMP) AS STRING)) AS selling_rights_cancelled_event_tmstp_tz,
  CAST(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.lastupdatedtimestamp) AS TIMESTAMP) AS conv_src_event_tmstp,
  `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(CAST(CAST(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.lastupdatedtimestamp) AS TIMESTAMP) AS STRING)) AS conv_src_event_tmstp_tz,
  CAST(RPAD(FORMAT_TIMESTAMP('%Y%m%d%I%M%S', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)),
    14, ' ') AS BIGINT) AS dw_batch_id,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  PARSE_DATE('%F', `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.lastupdatedtimestamp)) AS dw_sys_load_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_ldg AS ldg
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_item_selling_rights_opr AS opr ON opr.dw_sys_load_date BETWEEN DATE_ADD(CURRENT_DATE('PST8PDT'),
          INTERVAL - 24 MONTH) AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 24 MONTH)) AND LOWER(ldg.rmsskunum) = LOWER(opr.rms_sku_num
          ) AND LOWER(ldg.sellingcountry) = LOWER(opr.channel_country) AND LOWER(ldg.sellingbrand) = LOWER(opr.channel_brand
        ) AND LOWER(ldg.sellingchannel) = LOWER(opr.selling_channel) AND CAST(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.starttime) AS TIMESTAMP)
     = opr.selling_rights_start_tmstp AND CAST(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(ldg.lastupdatedtimestamp) AS TIMESTAMP)
    = opr.src_event_tmstp
 WHERE opr.rms_sku_num IS NULL
  AND ldg.endtime IS NULL);
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_item_selling_rights_opr AS tgt SET
    selling_rights_cancelled_ind = 'Y',
    selling_rights_cancelled_desc = 'SELLING_EVENT_CANCELLED',
    selling_rights_cancelled_event_tmstp = CAST(SRC.src_cancelled_event_tmstp AS TIMESTAMP),
    selling_rights_cancelled_event_tmstp_tz = SRC.src_cancelled_event_tmstp_tz,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
    FROM (SELECT DISTINCT opr.rms_sku_num, opr.channel_country, opr.channel_brand, opr.selling_channel, opr.selling_rights_start_tmstp, opr.src_event_tmstp, LDG.src_cancelled_event_tmstp,LDG.src_cancelled_event_tmstp_tz
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_opr.product_item_selling_rights_opr AS opr
            INNER JOIN (SELECT DISTINCT rmsskunum AS rms_sku_num, sellingcountry AS channel_country, sellingbrand AS channel_brand, sellingchannel AS selling_channel, `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(starttime) AS selling_rights_cancelled_tmstp, `{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(lastupdatedtimestamp) AS src_cancelled_event_tmstp, `{{params.gcp_project_id}}.jwn_udf.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.jwn_udf.ISO8601_TMSTP`(lastupdatedtimestamp)) AS src_cancelled_event_tmstp_tz
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_item_selling_rights_ldg
                WHERE endtime IS NOT NULL
                QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, sellingcountry, sellingbrand, sellingchannel, selling_rights_cancelled_tmstp ORDER BY src_cancelled_event_tmstp DESC)) = 1) AS LDG ON LOWER(opr.rms_sku_num) = LOWER(LDG.rms_sku_num) AND LOWER(opr.channel_country) = LOWER(LDG.channel_country) AND LOWER(opr.channel_brand) = LOWER(LDG.channel_brand) AND LOWER(opr.selling_channel) = LOWER(LDG.selling_channel) AND opr.selling_rights_start_tmstp = CAST(LDG.selling_rights_cancelled_tmstp AS TIMESTAMP)) AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country) AND LOWER(SRC.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(SRC.selling_channel) = LOWER(tgt.selling_channel) AND SRC.selling_rights_start_tmstp = tgt.selling_rights_start_tmstp AND SRC.src_event_tmstp = tgt.src_event_tmstp AND LOWER(tgt.selling_rights_cancelled_ind) <> LOWER('Y');
COMMIT TRANSACTION;

--COLLECT STATISTICS       COLUMN ( RMS_SKU_NUM,CHANNEL_COUNTRY,CHANNEL_BRAND,SELLING_CHANNEL,SELLING_RIGHTS_START_TMSTP,SRC_EVENT_TMSTP ),       COLUMN ( RMS_SKU_NUM ,CHANNEL_COUNTRY ,CHANNEL_BRAND ,SELLING_CHANNEL ,SELLING_RIGHTS_START_TMSTP,IS_SELLABLE_IND),       COLUMN ( PARTITION,RMS_SKU_NUM,CHANNEL_COUNTRY,CHANNEL_BRAND,SELLING_CHANNEL ),       COLUMN ( RMS_SKU_NUM,CHANNEL_COUNTRY,CHANNEL_BRAND,SELLING_CHANNEL ),       COLUMN (IS_SELLABLE_IND),       COLUMN (SELLING_RIGHTS_CANCELLED_IND),       COLUMN (SELLING_RIGHTS_START_TMSTP),       COLUMN ( RMS_SKU_NUM )        ON PRD_NAP_OPR.PRODUCT_ITEM_SELLING_RIGHTS_OPR;

