DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.merch_nap_aggregate_rebuild_lkup
WHERE LOWER(config_key) IN (LOWER('MERCH_JWN_SALE_FACT_TY_LY_DAY_REBUILD_WEEKS'), LOWER('MERCH_INVENTORY_FACT_TY_LY_DAY_REBUILD_WEEKS'
   ), LOWER('MERCH_JWN_DEMAND_FACT_TY_LY_DAY_REBUILD_WEEKS'), LOWER('MERCH_TRAN_SKU_STORE_DAY_REBUILD_WEEKS'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.merch_nap_aggregate_rebuild_lkup (interface_code, config_key, rebuild_week_num,
 process_status_ind, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT 'MRIc_DAY_AGG_WKLY',
  'MERCH_JWN_SALE_FACT_TY_LY_DAY_REBUILD_WEEKS',
  CAST(week_num0 AS INTEGER) AS week_num,
  'N',
  a4,
  a5
 FROM (SELECT 'MRIc_DAY_AGG_WKLY',
    'MERCH_JWN_SALE_FACT_TY_LY_DAY_REBUILD_WEEKS',
    TRIM(FORMAT('%11d', week_num)) AS week_num0,
    'N',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS a4,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS a5,
    week_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_nap_aggregate_rebuild_weeks_vw
   WHERE LOWER(config_key) = LOWER('MERCH_TRAN_REBUILD_YEARS')
   ORDER BY week_num DESC
   LIMIT 10) AS t1);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.merch_nap_aggregate_rebuild_lkup (interface_code, config_key, rebuild_week_num,
 process_status_ind, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT 'MRIc_DAY_AGG_WKLY',
  'MERCH_JWN_DEMAND_FACT_TY_LY_DAY_REBUILD_WEEKS',
  CAST(TRUNC(CAST(week_num0 AS FLOAT64)) AS INTEGER) AS week_num,
  'N',
  a4,
  a5
 FROM (SELECT 'MRIc_DAY_AGG_WKLY',
    'MERCH_JWN_DEMAND_FACT_TY_LY_DAY_REBUILD_WEEKS',
    TRIM(FORMAT('%11d', week_num)) AS week_num0,
    'N',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS a4,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS a5,
    week_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_nap_aggregate_rebuild_weeks_vw
   WHERE LOWER(config_key) = LOWER('MERCH_TRAN_REBUILD_YEARS')
   ORDER BY week_num DESC
   LIMIT 10) AS t1);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.merch_nap_aggregate_rebuild_lkup (interface_code, config_key, rebuild_week_num,
 process_status_ind, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT 'MRIc_DAY_AGG_WKLY',
  'MERCH_INVENTORY_FACT_TY_LY_DAY_REBUILD_WEEKS',
    CAST(TRUNC(CAST(week_num0 AS FLOAT64)) AS INTEGER) AS week_num,
  'N',
  a4,
  a5
 FROM (SELECT 'MRIc_DAY_AGG_WKLY',
    'MERCH_INVENTORY_FACT_TY_LY_DAY_REBUILD_WEEKS',
    TRIM(FORMAT('%11d', week_num)) AS week_num0,
    'N',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS a4,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS a5,
    week_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_nap_aggregate_rebuild_weeks_vw
   WHERE LOWER(config_key) = LOWER('MERCH_TRAN_REBUILD_YEARS')
   ORDER BY week_num DESC
   LIMIT 10) AS t1);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.merch_nap_aggregate_rebuild_lkup (interface_code, config_key, rebuild_week_num,
 process_status_ind, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT 'MRIc_DAY_AGG_WKLY',
  'MERCH_TRAN_SKU_STORE_DAY_REBUILD_WEEKS',
  CAST(TRUNC(CAST(week_num0 AS FLOAT64)) AS INTEGER) AS week_num,
  'N',
  a4,
  a5
 FROM (SELECT 'MRIc_DAY_AGG_WKLY',
    'MERCH_TRAN_SKU_STORE_DAY_REBUILD_WEEKS',
    TRIM(FORMAT('%11d', week_num)) AS week_num0,
    'N',
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS a4,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS a5,
    week_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_nap_aggregate_rebuild_weeks_vw
   WHERE LOWER(config_key) = LOWER('MERCH_TRAN_REBUILD_YEARS')
   ORDER BY week_num DESC
   LIMIT 10) AS t1);