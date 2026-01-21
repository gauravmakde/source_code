
-- COMMIT TRANSACTION ;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_allocation_fact_sku_store_wrk 
(store_num, 
rms_sku_num, 
week_num, 
business_unit_num,
 dw_sys_load_dt, src_ind)
(SELECT CAST(TRUNC(CAST(RPAD(CAST(jrthd.store_num AS STRING), 4, ' ') AS FLOAT64)) AS INTEGER) AS store_num,
  SUBSTR(COALESCE(jrthd.rms_sku_num, 'NA'), 1, 10) AS rms_sku_num,
  jrthd.week_num,
  jrthd.business_unit_num,
  CURRENT_DATE('PST8PDT') AS dw_sys_load_dt,
  'SALES' AS src_ind
 FROM (SELECT a.rms_sku_num,
    a.intent_store_num AS store_num,
    b.week_idnt AS week_num,
    sd.business_unit_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact AS a
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON a.intent_store_num = sd.store_num
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.business_day_date = b.day_date
   WHERE a.business_day_date BETWEEN (SELECT dcd.week_start_day_date
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
       INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebd ON dcd.day_date = ebd.dw_batch_dt
      WHERE LOWER(ebd.interface_code) = LOWER('MERCH_ALLOCATION_DLY')) AND (SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_ALLOCATION_DLY'))
    AND a.dw_batch_date = (SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_ALLOCATION_DLY'))
    AND sd.business_unit_num IN (1000, 2000, 5000)
    AND LOWER(a.tran_latest_version_ind) = LOWER('Y')
    AND LOWER(a.error_flag) = LOWER('N')
    AND LOWER(a.tran_type_code) IN (LOWER('EXCH'), LOWER('SALE'), LOWER('RETN'))
    AND a.upc_num IS NOT NULL
    AND a.rms_sku_num IS NOT NULL
   GROUP BY a.rms_sku_num,
    store_num,
    week_num,
    sd.business_unit_num) AS jrthd
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_allocation_fact_sku_store_wrk AS alloc ON LOWER(jrthd.rms_sku_num) = LOWER(alloc.rms_sku_num
      ) AND jrthd.store_num = alloc.store_num AND jrthd.week_num = alloc.week_num
 WHERE alloc.store_num IS NULL);

-- COMMIT TRANSACTION ;
-- COMMIT TRANSACTION ;

