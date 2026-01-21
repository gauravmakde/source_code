
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=inventory_day_fact_load_dly;
---Task_Name=inv_day_fct_load;'*/

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_inventory_sku_store_day_fact AS inv
WHERE snapshot_date IN 
(SELECT cal.day_date FROM 
`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl
INNER JOIN 
`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal 
ON cal.day_date >= DATE_SUB(etl.dw_batch_dt, INTERVAL 4 DAY) 
AND etl.dw_batch_dt >= cal.day_date
WHERE LOWER(etl.interface_code) = LOWER('MERCH_NAP_INVDLY'));

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_inventory_sku_store_day_fact (rms_sku_num, store_num, day_num, snapshot_date,
 week_num, stock_on_hand_qty, in_transit_qty, location_type, dw_batch_date, dw_sys_load_tmstp)
(SELECT inv.rms_sku_id AS rms_sku_num,
  --FLOOR(INV.LOCATION_ID) AS STORE_NUM,
  cast(FLOOR(cast(INV.LOCATION_ID as float64)) as int64) AS STORE_NUM,
  cal.day_idnt AS day_num,
  inv.snapshot_date,
  cal.week_idnt AS week_num,
  inv.stock_on_hand_qty,
  inv.in_transit_qty,
  inv.location_type,
  etl.dw_batch_dt AS dw_batch_date,
  current_datetime('PST8PDT') AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_stock_quantity_by_day_logical_fact AS inv
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal 
  ON inv.snapshot_date = cal.day_date
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl 
  ON cal.day_date >= DATE_SUB(etl.dw_batch_dt, INTERVAL 4 DAY) AND
    cal.day_date <= etl.dw_batch_dt
 WHERE LOWER(etl.INTERFACE_CODE) = LOWER('MERCH_NAP_INVDLY')
  AND (inv.stock_on_hand_qty IS NOT NULL OR inv.in_transit_qty IS NOT NULL)
  AND COALESCE(inv.location_type, '-1') NOT IN (SELECT config_value
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_INVDLY')
     AND LOWER(config_key) = LOWER('MERCH_NAP_INV_LOC_TYP_FL')));

/*SET QUERY_BAND = NONE FOR SESSION;*/

