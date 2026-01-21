
-- COMMIT TRANSACTION;

CREATE TEMPORARY TABLE IF NOT EXISTS merch_smd_insights_sales_dt_by_week_vt (
snapshot_date DATE NOT NULL,
rms_style_num STRING(10) NOT NULL,
color_num STRING(10) NOT NULL,
channel_country STRING(10) NOT NULL,
channel_brand STRING(20) NOT NULL,
selling_channel STRING(20) NOT NULL,
first_receipt_date DATE,
last_receipt_date DATE,
first_sales_date DATE
) ;

-- COMMIT TRANSACTION;

INSERT INTO merch_smd_insights_sales_dt_by_week_vt 
(snapshot_date, rms_style_num, color_num, channel_country,
 channel_brand, selling_channel, first_receipt_date, last_receipt_date, first_sales_date)
(SELECT cmibwf.snapshot_date,
  cmibwf.rms_style_num,
  cmibwf.color_num,
  cmibwf.channel_country,
  cmibwf.channel_brand,
  cmibwf.selling_channel,
  MIN(mrdrwf.first_receipt_date) AS first_receipt_date,
  MAX(mrdrwf.last_receipt_date) AS last_receipt_date,
  MIN(cmsv.business_day_date) AS first_sales_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS cmibwf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON cmibwf.snapshot_date = etl.dw_batch_dt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_sales_vw AS cmsv ON LOWER(cmibwf.rms_style_num) = LOWER(cmsv.rms_style_num
        ) AND LOWER(cmibwf.color_num) = LOWER(cmsv.color_num) AND LOWER(cmibwf.channel_country) = LOWER(cmsv.channel_country
       ) AND LOWER(cmibwf.channel_brand) = LOWER(cmsv.channel_brand) AND LOWER(cmibwf.selling_channel) = LOWER(cmsv.selling_channel
     )
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_receipt_date_reset_applied_week_fact AS mrdrwf ON LOWER(mrdrwf.rms_style_num) =
        LOWER(cmibwf.rms_style_num) AND LOWER(mrdrwf.color_num) = LOWER(cmibwf.color_num) AND LOWER(mrdrwf.channel_country
        ) = LOWER(cmibwf.channel_country) AND LOWER(mrdrwf.channel_brand) = LOWER(cmibwf.channel_brand) AND LOWER(mrdrwf
      .selling_channel) = LOWER(cmibwf.selling_channel) AND cmibwf.snapshot_date = mrdrwf.week_end_date
 WHERE LOWER(etl.interface_code) = LOWER('CMD_WKLY')
  AND cmsv.business_day_date <= etl.dw_batch_dt
  AND cmsv.business_day_date >= mrdrwf.first_receipt_date
 GROUP BY cmibwf.snapshot_date,
  cmibwf.rms_style_num,
  cmibwf.color_num,
  cmibwf.channel_country,
  cmibwf.channel_brand,
  cmibwf.selling_channel);

-- COMMIT TRANSACTION;

INSERT INTO merch_smd_insights_sales_dt_by_week_vt (snapshot_date, rms_style_num, color_num, channel_country,
 channel_brand, selling_channel, first_sales_date)
(SELECT cmibwf.snapshot_date,
  cmibwf.rms_style_num,
  cmibwf.color_num,
  cmibwf.channel_country,
  cmibwf.channel_brand,
  cmibwf.selling_channel,
  MIN(cmsv.business_day_date) AS first_sales_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS cmibwf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON cmibwf.snapshot_date = etl.dw_batch_dt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_sales_vw AS cmsv ON LOWER(cmibwf.rms_style_num) = LOWER(cmsv.rms_style_num
        ) AND LOWER(cmibwf.color_num) = LOWER(cmsv.color_num) AND LOWER(cmibwf.channel_country) = LOWER(cmsv.channel_country
       ) AND LOWER(cmibwf.channel_brand) = LOWER(cmsv.channel_brand) AND LOWER(cmibwf.selling_channel) = LOWER(cmsv.selling_channel
     )
  LEFT JOIN merch_smd_insights_sales_dt_by_week_vt AS mswsdvt ON LOWER(mswsdvt.rms_style_num) = LOWER(cmibwf.rms_style_num
         ) AND LOWER(mswsdvt.color_num) = LOWER(cmibwf.color_num) AND LOWER(mswsdvt.channel_country) = LOWER(cmibwf.channel_country
        ) AND LOWER(mswsdvt.channel_brand) = LOWER(cmibwf.channel_brand) AND LOWER(mswsdvt.selling_channel) = LOWER(cmibwf
      .selling_channel) AND cmibwf.snapshot_date = mswsdvt.snapshot_date
 WHERE LOWER(etl.interface_code) = LOWER('CMD_WKLY')
  AND cmsv.business_day_date <= etl.dw_batch_dt
  AND mswsdvt.rms_style_num IS NULL
 GROUP BY cmibwf.snapshot_date,
  cmibwf.rms_style_num,
  cmibwf.color_num,
  cmibwf.channel_country,
  cmibwf.channel_brand,
  cmibwf.selling_channel);




-- COMMIT TRANSACTION;
UPDATE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt
SET
  first_sales_date = src.first_sales_date,
  weeks_sales = cast(TRUNC((DATE_DIFF( (SELECT DATETIME_TRUNC(etl.DW_BATCH_DT,day) FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup etl
   WHERE etl.INTERFACE_CODE = 'CMD_WKLY'), DATETIME_TRUNC(src.FIRST_SALES_DATE,day),day)/7 ))as int64),
  dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
  dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM
  merch_smd_insights_sales_dt_by_week_vt AS src
WHERE
  src.snapshot_date = tgt.snapshot_date
  AND LOWER(src.rms_style_num) = LOWER(tgt.rms_style_num)
  AND LOWER(src.color_num ) = LOWER(tgt.color_num)
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand)
  AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);

-- COMMIT TRANSACTION;

UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact tgt
SET 
    FIRST_SALES_DATE  = src.FIRST_SALES_DATE,
    WEEKS_SALES       = src.WEEKS_SALES,
    DW_SYS_UPDT_TMSTP = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
    DW_SYS_UPDT_TMSTP_TZ = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM 
     (
       SELECT
           mswsdvt.SNAPSHOT_DATE
         , mswsdvt.RMS_STYLE_NUM
         , mswsdvt.COLOR_NUM
         , mswsdvt.CHANNEL_COUNTRY
         , mswsdvt.CHANNEL_BRAND
         , MIN(mswsdvt.FIRST_SALES_DATE) AS FIRST_SALES_DATE,
         CAST(TRUNC((DATE_DIFF((SELECT DATETIME_TRUNC(etl.DW_BATCH_DT,DAY) FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ETL_BATCH_DT_LKUP etl WHERE LOWER(etl.INTERFACE_CODE) = LOWER('CMD_WKLY')),
         DATETIME_TRUNC(MIN(mswsdvt.FIRST_SALES_DATE),DAY),DAY)/ 7)) AS INT64) AS WEEKS_SALES
         FROM merch_smd_insights_sales_dt_by_week_vt mswsdvt
        GROUP BY 1,2,3,4,5
     ) src
WHERE src.SNAPSHOT_DATE  = tgt.SNAPSHOT_DATE
AND LOWER(src.RMS_STYLE_NUM)    = LOWER(tgt.RMS_STYLE_NUM)
AND LOWER(src.COLOR_NUM)       = LOWER(tgt.COLOR_NUM)
AND LOWER(src.CHANNEL_COUNTRY)  = LOWER(tgt.CHANNEL_COUNTRY)
AND LOWER(src.CHANNEL_BRAND)   = LOWER(tgt.CHANNEL_BRAND)
AND (LOWER(tgt.SELLING_CHANNEL) = LOWER('OMNI') OR tgt.TOTAL_INV_QTY = 0)
;

-- COMMIT TRANSACTION;
-- COMMIT TRANSACTION;