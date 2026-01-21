BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS my_params AS
SELECT dw_batch_dt AS last_sat
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
  WHERE LOWER(interface_code) = LOWER('SMD_INSIGHTS_WKLY');

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS raw_data (
  rms_style_num STRING,
  color_num STRING,
  channel_country STRING NOT NULL,
  channel_brand STRING NOT NULL,
  selling_channel STRING NOT NULL,
  div_num INTEGER,
  div_desc STRING,
  grp_num INTEGER,
  grp_desc STRING,
  dept_num INTEGER,
  dept_desc STRING,
  class_num INTEGER,
  class_desc STRING,
  sbclass_num INTEGER,
  sbclass_desc STRING,
  style_group_num STRING,
  style_desc STRING
);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;

END;

INSERT INTO raw_data
SELECT DISTINCT
    isibdf.rms_style_num,
    isibdf.color_num,
    isibdf.channel_country,
    isibdf.channel_brand,
    isibdf.selling_channel,
    coalesce(pstd.div_num,psd.div_num),
    coalesce(pstd.div_desc,psd.div_desc),
    coalesce(pstd.grp_num,psd.grp_num),
    coalesce(pstd.grp_desc,psd.grp_desc),
    coalesce(pstd.dept_num,psd.dept_num),
    coalesce(pstd.dept_desc,psd.dept_desc),
    coalesce(pstd.class_num,psd.class_num),
    coalesce(pstd.class_desc,psd.class_desc),
    coalesce(pstd.sbclass_num,psd.sbclass_num),
    coalesce(pstd.sbclass_desc,psd.sbclass_desc),
    pstd.style_group_num,
    coalesce(pstd.style_desc,psd.style_desc)
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.INVENTORY_SALES_INSIGHTS_BY_DAY_FACT isibdf
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.CLEARANCE_MARKDOWN_INSIGHTS_BY_WEEK_FACT cmibwf
  ON COALESCE(cmibwf.rms_style_num, 'NA') = COALESCE(isibdf.rms_style_num, 'NA')
 AND COALESCE(cmibwf.color_num, 'NA')     = COALESCE(isibdf.color_num, 'NA')
 AND LOWER(cmibwf.channel_country) = LOWER(isibdf.channel_country)
 AND LOWER(cmibwf.channel_brand)   = LOWER(isibdf.channel_brand)
 AND LOWER(cmibwf.selling_channel) = LOWER(isibdf.selling_channel)
JOIN my_params as params
  ON  params.last_SAT = isibdf.metrics_date
  and params.last_SAT = cmibwf.snapshot_date
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST AS psd
  ON psd.RMS_SKU_NUM = isibdf.RMS_SKU_ID
  AND COALESCE(psd.RMS_STYLE_NUM, 'NA')    = COALESCE(isibdf.RMS_STYLE_NUM, 'NA')
  AND COALESCE(psd.COLOR_NUM, 'NA')        = COALESCE(isibdf.COLOR_NUM, 'NA')
  AND LOWER(psd.CHANNEL_COUNTRY)           = LOWER(isibdf.CHANNEL_COUNTRY)
  AND RANGE_CONTAINS(RANGE(psd.EFF_BEGIN_TMSTP_UTC, psd.EFF_END_TMSTP_UTC), CAST(timestamp(params.last_SAT) + INTERVAL '1' DAY  - interval '0.001' second AS TIMESTAMP )) 
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST AS pstd
  ON pstd.EPM_STYLE_NUM   = psd.EPM_STYLE_NUM
  AND pstd.CHANNEL_COUNTRY  = psd.CHANNEL_COUNTRY
  AND RANGE_CONTAINS(RANGE(pstd.EFF_BEGIN_TMSTP_UTC, pstd.EFF_END_TMSTP_UTC), CAST(timestamp(params.last_SAT) + INTERVAL '1' DAY  - interval '0.001' second AS TIMESTAMP) ) ;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    div_num = src.div_num,
    div_desc = src.div_desc,
    grp_num = src.grp_num,
    grp_desc = src.grp_desc,
    dept_num = src.dept_num,
    dept_desc = src.dept_desc,
    class_num = src.class_num,
    class_desc = src.class_desc,
    sbclass_num = src.sbclass_num,
    sbclass_desc = src.sbclass_desc,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
	dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM
(SELECT DISTINCT rms_style_num, color_num, channel_country, channel_brand, selling_channel, div_num, div_desc, grp_num, grp_desc, dept_num, dept_desc, class_num, class_desc, sbclass_num, sbclass_desc
    FROM raw_data) AS src 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT * FROM my_params) 
  AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) 
  AND LOWER(src.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) 
  AND LOWER(src.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) 
  AND LOWER(src.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    div_num = omni.div_num,
    div_desc = omni.div_desc,
    grp_num = omni.grp_num,
    grp_desc = omni.grp_desc,
    dept_num = omni.dept_num,
    dept_desc = omni.dept_desc,
    class_num = omni.class_num,
    class_desc = omni.class_desc,
    sbclass_num = omni.sbclass_num,
    sbclass_desc = omni.sbclass_desc,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
	dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM
 (SELECT DISTINCT rms_style_num, color_num, channel_country, channel_brand, div_num, div_desc, grp_num, grp_desc, dept_num, dept_desc, class_num, class_desc, sbclass_num, sbclass_desc
    FROM raw_data) AS omni 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT * FROM my_params) 
  AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) 
  AND LOWER(omni.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) 
  AND LOWER(omni.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) 
  AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') 
    OR clearance_markdown_insights_by_week_fact.div_num IS NULL 
    AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('ONLINE') 
    OR LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('STORE')));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    style_group_num = t1.style_group_num,
    style_desc = t1.style_desc,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
	dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM (
    SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, style_group_num, style_desc, COUNT(style_group_num) AS style_group_num_cnt
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, style_group_num, style_desc
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY style_group_num_cnt DESC, style_desc)) = 1
  ) AS t1 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT * FROM my_params) 
  AND LOWER(COALESCE(t1.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(t1.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) 
  AND LOWER(t1.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) 
  AND LOWER(t1.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) 
  AND LOWER(t1.selling_channel) = LOWER(clearance_markdown_insights_by_week_fact.selling_channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_fact
SET
    style_group_num = t1.style_group_num,
    style_desc = t1.style_desc,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
FROM 
(SELECT rms_style_num, color_num, channel_country, channel_brand, style_group_num, style_desc, COUNT(style_group_num) AS style_group_num_cnt
    FROM raw_data
    GROUP BY rms_style_num, color_num, channel_country, channel_brand, style_group_num, style_desc
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand ORDER BY style_group_num_cnt DESC, style_desc)) = 1
) AS t1 
WHERE clearance_markdown_insights_by_week_fact.snapshot_date = (SELECT * FROM my_params) 
  AND LOWER(COALESCE(t1.rms_style_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(t1.color_num, 'NA')) = LOWER(COALESCE(clearance_markdown_insights_by_week_fact.color_num, 'NA')) 
  AND LOWER(t1.channel_country) = LOWER(clearance_markdown_insights_by_week_fact.channel_country) 
  AND LOWER(t1.channel_brand) = LOWER(clearance_markdown_insights_by_week_fact.channel_brand) 
  AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('OMNI') 
    OR clearance_markdown_insights_by_week_fact.style_desc IS NULL 
    AND (LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('ONLINE') 
    OR LOWER(clearance_markdown_insights_by_week_fact.selling_channel) = LOWER('STORE')));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

END;
