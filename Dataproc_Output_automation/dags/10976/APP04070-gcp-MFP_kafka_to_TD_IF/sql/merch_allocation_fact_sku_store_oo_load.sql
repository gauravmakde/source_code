
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_allocation_fact_sku_store_load;
---Task_Name=merch_allocation_fact_sku_store_oo_load;'*/
---FOR SESSION VOLATILE;

-- COMMIT TRANSACTION ;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_allocation_fact_sku_store_wrk 
(store_num, rms_sku_num, week_num, business_unit_num, dw_sys_load_dt, src_ind)
(SELECT CAST(TRUNC(CAST(RPAD(CAST(onordr.store_num AS STRING), 4, ' ') AS FLOAT64)) AS INTEGER) AS store_num,
  SUBSTR(onordr.rms_sku_num, 1, 10) AS rms_sku_num,
  onordr.week_num,
  onordr.business_unit_num,
  CURRENT_DATE('PST8PDT') AS dw_sys_load_dt,
  'ONORDER' AS src_ind
 FROM (SELECT oo.store_num,
    oo.rms_sku_num,
     (SELECT dcd.week_idnt
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
      INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl 
	  ON dcd.day_date = etl.dw_batch_dt
     WHERE LOWER(etl.interface_code) = LOWER('MERCH_ALLOCATION_DLY')) AS week_num,
    sd.business_unit_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS oo
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd 
	ON oo.store_num = sd.store_num
   WHERE oo.quantity_open > 0
    AND LOWER(oo.status) = LOWER('APPROVED')
    AND sd.business_unit_num IN (1000, 2000, 5000)
   GROUP BY oo.rms_sku_num,
    oo.store_num,
    sd.business_unit_num) AS onordr
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_allocation_fact_sku_store_wrk AS alloc 
  ON LOWER(onordr.rms_sku_num) = LOWER(TRIM(alloc.rms_sku_num)) 
  AND onordr.store_num = alloc.store_num 
  AND onordr.week_num = alloc.week_num
 WHERE alloc.store_num IS NULL);
 

-- COMMIT TRANSACTION ;
-- COMMIT TRANSACTION ;

