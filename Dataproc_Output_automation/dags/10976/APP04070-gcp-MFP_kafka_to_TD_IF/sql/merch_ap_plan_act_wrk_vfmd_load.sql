-- BEGIN
-- DECLARE _ERROR_CODE INT64;
-- DECLARE _ERROR_MESSAGE STRING;
-- /*SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=ap_plan_act_fct;
-- ---Task_Name=t0_4_ap_plan_act_wrk_vfmd_load;'*/
-- -- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- BEGIN
-- SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group, dept_num, banner_num, channel_country,
 month_num, fiscal_year_num, actual_vendor_funds_total_cost_amount, dw_batch_date, dw_sys_load_tmstp)
(SELECT COALESCE(g01.supplier_group, '-1') AS supplier_group,
  r01.department_num AS dept_num,
  COALESCE(b01.banner_id, - 1) AS banner_num,
  COALESCE(b01.channel_country, 'NA') AS channel_country,
  r01.month_idnt AS month_num,
  r01.year_idnt AS fiscal_year_num,
  SUM(r01.vendor_funds_cost) AS actual_vendor_funds_total_cost_amount,
  CURRENT_DATE('PST8PDT'),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_vendor_funds_sku_store_week_fact_vw AS r01
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS b01 ON r01.channel_num = b01.channel_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_dim AS g01 ON LOWER(r01.supplier_num) = LOWER(g01.supplier_num) AND r01.department_num
        = CAST(g01.dept_num AS FLOAT64) AND LOWER(b01.banner_code) = LOWER(g01.banner) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
     >= CAST(g01.eff_begin_tmstp AS DATETIME) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    < CAST(g01.eff_end_tmstp AS DATETIME)
 WHERE r01.week_num >= (SELECT MIN(week_idnt)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
    WHERE fiscal_year_num = (SELECT current_fiscal_year_num - 2
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
  AND r01.week_num <= (SELECT last_completed_fiscal_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
 GROUP BY supplier_group,
  dept_num,
  banner_num,
  channel_country,
  month_num,
  fiscal_year_num);
-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;
-- END;
-- BEGIN
-- SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group, dept_num, banner_num, channel_country,
 month_num, fiscal_year_num, actual_disc_terms_total_cost_amount, dw_batch_date, dw_sys_load_tmstp)
(SELECT COALESCE(g01.supplier_group, '-1') AS supplier_group,
  disc.department_num AS dept_num,
  COALESCE(b01.banner_id, - 1) AS banner_num,
  COALESCE(b01.channel_country, 'NA') AS channel_country,
  disc.month_num,
  disc.year_num AS fiscal_year_num,
  SUM(disc.disc_terms_cost) AS actual_disc_terms_total_cost_amount,
  CURRENT_DATE('PST8PDT'),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM (SELECT r01.month_num,
    r01.year_num,
    r01.supplier_num,
    r01.department_num,
    r01.week_num,
     CASE
     WHEN config.config_value IS NOT NULL AND r01.week_num < 202301
     THEN '110'
     ELSE FORMAT('%11d', r01.channel_num)
     END AS channel_num,
    r01.disc_terms_cost
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_disc_terms_sku_loc_week_agg_fact_vw AS r01
    LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup AS config ON CAST(config.config_value AS FLOAT64) = r01.channel_num AND LOWER(config
        .interface_code) = LOWER('MERCH_NAP_SPE_DLY') AND LOWER(config.config_key) = LOWER('REALIGN_CHANNEL_NUM')) AS
  disc
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS b01 ON CAST(disc.channel_num AS FLOAT64) = b01.channel_num
   
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_dim AS g01 ON LOWER(disc.supplier_num) = LOWER(g01.supplier_num) AND disc.department_num
        = CAST(g01.dept_num AS FLOAT64) AND LOWER(b01.banner_code) = LOWER(g01.banner) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
     >= CAST(g01.eff_begin_tmstp AS DATETIME) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    < CAST(g01.eff_end_tmstp AS DATETIME)
 WHERE disc.week_num >= (SELECT MIN(week_idnt)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
    WHERE fiscal_year_num = (SELECT current_fiscal_year_num - 2
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
  AND disc.week_num <= (SELECT last_completed_fiscal_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
 GROUP BY supplier_group,
  dept_num,
  banner_num,
  channel_country,
  disc.month_num,
  fiscal_year_num);
-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;
-- END;
-- COMMIT TRANSACTION;
-- -- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- /*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- -- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
-- END;
