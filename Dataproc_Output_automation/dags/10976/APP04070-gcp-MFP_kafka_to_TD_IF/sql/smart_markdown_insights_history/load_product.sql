

DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=app04070;DAG_ID=smartmarkdown_insights_history_10976_tech_nap_merch;Task_Name=run_load_product;'*/


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
) ;


INSERT INTO raw_data
(SELECT DISTINCT isibdf.rms_style_num,
  isibdf.color_num,
  isibdf.channel_country,
  isibdf.channel_brand,
  isibdf.selling_channel,
  COALESCE(pstd.div_num, psd.div_num) AS div_num,
  COALESCE(pstd.div_desc, psd.div_desc) AS div_desc,
  COALESCE(pstd.grp_num, psd.grp_num) AS grp_num,
  COALESCE(pstd.grp_desc, psd.grp_desc) AS grp_desc,
  COALESCE(pstd.dept_num, psd.dept_num) AS dept_num,
  COALESCE(pstd.dept_desc, psd.dept_desc) AS dept_desc,
  COALESCE(pstd.class_num, psd.class_num) AS class_num,
  COALESCE(pstd.class_desc, psd.class_desc) AS class_desc,
  COALESCE(pstd.sbclass_num, psd.sbclass_num) AS sbclass_num,
  COALESCE(pstd.sbclass_desc, psd.sbclass_desc) AS sbclass_desc,
  pstd.style_group_num,
  COALESCE(pstd.style_desc, psd.style_desc) AS style_desc
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_sales_insights_by_day_hist_fact AS isibdf
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl 
  ON LOWER(etl.interface_code) = LOWER('CMD_WKLY') AND isibdf.metrics_date
     = etl.dw_batch_dt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.clearance_markdown_insights_by_week_hist_fact AS cmibwf 
  ON LOWER(COALESCE(cmibwf.rms_style_num, 'NA')) = LOWER(COALESCE(isibdf.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(cmibwf.color_num, 'NA')) = LOWER(COALESCE(isibdf.color_num, 'NA')) 
  AND LOWER(cmibwf.channel_country) = LOWER(isibdf.channel_country) 
  AND LOWER(cmibwf.channel_brand) = LOWER(isibdf.channel_brand) 
  AND LOWER(cmibwf.selling_channel) = LOWER(isibdf.selling_channel) 
  AND etl.dw_batch_dt = cmibwf.snapshot_date
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS psd 
  ON LOWER(psd.rms_sku_num) = LOWER(isibdf.rms_sku_id) 
  AND LOWER(COALESCE(psd.rms_style_num, 'NA')) = LOWER(COALESCE(isibdf.rms_style_num, 'NA')) 
  AND LOWER(COALESCE(psd.color_num, 'NA')) = LOWER(COALESCE(isibdf.color_num, 'NA')) 
  AND LOWER(psd.channel_country) = LOWER(isibdf.channel_country)
  AND RANGE_CONTAINS(RANGE(psd.eff_begin_tmstp_utc, psd.eff_end_tmstp_utc), (CAST(etl.dw_batch_dt AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND ))
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS pstd 
  ON psd.epm_style_num = pstd.epm_style_num 
  AND LOWER(pstd.channel_country) = LOWER(psd.channel_country)
  AND RANGE_CONTAINS(RANGE(pstd.eff_begin_tmstp_utc, pstd.eff_end_tmstp_utc) , (CAST(etl.dw_batch_dt AS TIMESTAMP) + INTERVAL '1' DAY - INTERVAL '0.001' SECOND)));




UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt 
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
FROM (SELECT DISTINCT rms_style_num, color_num, channel_country, channel_brand, selling_channel, div_num, div_desc, grp_num, grp_desc, dept_num, dept_desc, class_num, class_desc, sbclass_num, sbclass_desc
        FROM raw_data) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt
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

FROM (SELECT DISTINCT rms_style_num, color_num, channel_country, channel_brand, div_num, div_desc, grp_num, grp_desc, dept_num, dept_desc, class_num, class_desc, sbclass_num, sbclass_desc
        FROM raw_data) AS omni
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(tgt.channel_country) AND LOWER(omni.channel_brand) = LOWER(tgt.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.div_num IS NULL AND (LOWER(tgt.selling_channel) = LOWER('ONLINE') OR LOWER(tgt.selling_channel) = LOWER('STORE')));



UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt
SET
    style_group_num = src.style_group_num,
    style_desc = src.style_desc,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()

FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, selling_channel, style_group_num, style_desc, COUNT(style_group_num) AS style_group_num_cnt
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, selling_channel, style_group_num, style_desc
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand, selling_channel ORDER BY style_group_num_cnt DESC, style_desc)) = 1) AS src
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(src.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(src.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(src.channel_country) = LOWER(tgt.channel_country) AND LOWER(src.channel_brand) = LOWER(tgt.channel_brand) AND LOWER(src.selling_channel) = LOWER(tgt.selling_channel);




UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.clearance_markdown_insights_by_week_hist_fact AS tgt
SET
    style_group_num = omni.style_group_num,
    style_desc = omni.style_desc,
    dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
    dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()

FROM (SELECT rms_style_num, color_num, channel_country, channel_brand, style_group_num, style_desc, COUNT(style_group_num) AS style_group_num_cnt
        FROM raw_data
        GROUP BY rms_style_num, color_num, channel_country, channel_brand, style_group_num, style_desc
        QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_style_num, color_num, channel_country, channel_brand ORDER BY style_group_num_cnt DESC, style_desc)) = 1) AS omni
WHERE tgt.snapshot_date = (SELECT dw_batch_dt
                            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
                            WHERE LOWER(interface_code) = LOWER('CMD_WKLY')) AND LOWER(COALESCE(omni.rms_style_num, 'NA')) = LOWER(COALESCE(tgt.rms_style_num, 'NA')) AND LOWER(COALESCE(omni.color_num, 'NA')) = LOWER(COALESCE(tgt.color_num, 'NA')) AND LOWER(omni.channel_country) = LOWER(tgt.channel_country) AND LOWER(omni.channel_brand) = LOWER(tgt.channel_brand) AND (LOWER(tgt.selling_channel) = LOWER('OMNI') OR tgt.style_desc IS NULL AND (LOWER(tgt.selling_channel) = LOWER('ONLINE') OR LOWER(tgt.selling_channel) = LOWER('STORE')));



