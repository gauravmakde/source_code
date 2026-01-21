-- Start by defining any session-level variables, if applicable. 
-- In BigQuery, you might use temporary tables for volatile data.

-- DELETE operation equivalent

DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;



DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk inv
WHERE inv.day_num IN (
    SELECT dcal.day_idnt 
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim cal
    JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup etl
      ON cal.day_date = etl.dw_batch_dt 
    JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim dcal
      ON dcal.week_idnt = cal.week_idnt
    WHERE LOWER(etl.interface_code) = LOWER('MRIc_DAY_AGG_DLY')
)
AND LOWER(src_ind) = LOWER('INV');

-- INSERT operation
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_transaction_sku_store_day_agg_wrk
(
    rms_sku_num,
    store_num,
    day_num,
    rp_ind,
    inventory_eoh_total_units_ty,
    inventory_eoh_total_retail_amt_ty, 
    inventory_eoh_total_cost_amt_ty,
    inventory_eoh_regular_units_ty,
    inventory_eoh_regular_retail_amt_ty,
    inventory_eoh_regular_cost_amt_ty,
    inventory_eoh_clearance_units_ty,
    inventory_eoh_clearance_retail_amt_ty,
    inventory_eoh_clearance_cost_amt_ty,
    inventory_eoh_total_units_ly,
    inventory_eoh_total_retail_amt_ly, 
    inventory_eoh_total_cost_amt_ly,
    inventory_eoh_regular_units_ly,
    inventory_eoh_regular_retail_amt_ly,
    inventory_eoh_regular_cost_amt_ly,
    inventory_eoh_clearance_units_ly,
    inventory_eoh_clearance_retail_amt_ly,
    inventory_eoh_clearance_cost_amt_ly,
    src_ind,
    dw_sys_load_tmstp,
    dw_sys_load_dt
)
SELECT 
    inv.rms_sku_num,
    CAST(TRUNC(CAST(inv.store_num AS FLOAT64)) AS INT64),
    inv.day_num,
    inv.rp_ind,
    inv.eoh_active_regular_units + inv.eoh_inactive_regular_units 
    + inv.eoh_active_clearance_units + inv.eoh_inactive_clearance_units AS inventory_eoh_total_units_ty,
    
    inv.eoh_active_regular_retail + inv.eoh_inactive_regular_retail 
    + inv.eoh_active_clearance_retail + inv.eoh_inactive_clearance_retail AS inventory_eoh_total_retail_amt_ty, 
    
    inv.eoh_active_regular_cost + inv.eoh_inactive_regular_cost 
    + inv.eoh_active_clearance_cost + inv.eoh_inactive_clearance_cost AS inventory_eoh_total_cost_amt_ty,

    inv.eoh_active_regular_units + inv.eoh_inactive_regular_units AS inventory_eoh_regular_units_ty,
    inv.eoh_active_regular_retail + inv.eoh_inactive_regular_retail AS inventory_eoh_regular_retail_amt_ty,
    inv.eoh_active_regular_cost + inv.eoh_inactive_regular_cost AS inventory_eoh_regular_cost_amt_ty,
    inv.eoh_active_clearance_units + inv.eoh_inactive_clearance_units AS inventory_eoh_clearance_units_ty,
    inv.eoh_active_clearance_retail + inv.eoh_inactive_clearance_retail AS inventory_eoh_clearance_retail_amt_ty,
    inv.eoh_active_clearance_cost + inv.eoh_inactive_clearance_cost AS inventory_eoh_clearance_cost_amt_ty,

    CAST(0 AS INT64) AS inventory_eoh_total_units_ly,
    CAST(0 AS NUMERIC) AS inventory_eoh_total_retail_amt_ly, 
    CAST(0 AS NUMERIC) AS inventory_eoh_total_cost_amt_ly,
    CAST(0 AS INT64) AS inventory_eoh_regular_units_ly,
    CAST(0 AS NUMERIC) AS inventory_eoh_regular_retail_amt_ly,
    CAST(0 AS NUMERIC) AS inventory_eoh_regular_cost_amt_ly,
    CAST(0 AS INT64) AS inventory_eoh_clearance_units_ly,
    CAST(0 AS NUMERIC) AS inventory_eoh_clearance_retail_amt_ly,
    CAST(0 AS NUMERIC) AS inventory_eoh_clearance_cost_amt_ly,
    'INV' AS src_ind,    
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
    CURRENT_DATE('PST8PDT') AS dw_sys_load_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inventory_sku_store_day_columnar_vw inv
WHERE inv.day_num IN (
    SELECT dcal.day_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim cal
    JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup etl
      ON cal.day_date = etl.dw_batch_dt
    JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim dcal
      ON dcal.week_idnt = cal.week_idnt
    WHERE LOWER(etl.interface_code) = LOWER('MRIc_DAY_AGG_DLY')
)

UNION ALL

SELECT 
    inv.rms_sku_num,
    CAST(TRUNC(CAST(inv.store_num AS FLOAT64)) AS INT64),
    cal.day_idnt,
    inv.rp_ind,
    
    CAST(0 AS INT64) AS inventory_eoh_total_units_ty,
    CAST(0 AS NUMERIC) AS inventory_eoh_total_retail_amt_ty, 
    CAST(0 AS NUMERIC) AS inventory_eoh_total_cost_amt_ty,
    CAST(0 AS INT64) AS inventory_eoh_regular_units_ty,
    CAST(0 AS NUMERIC) AS inventory_eoh_regular_retail_amt_ty,
    CAST(0 AS NUMERIC) AS inventory_eoh_regular_cost_amt_ty,
    CAST(0 AS INT64) AS inventory_eoh_clearance_units_ty,
    CAST(0 AS NUMERIC) AS inventory_eoh_clearance_retail_amt_ty,
    CAST(0 AS NUMERIC) AS inventory_eoh_clearance_cost_amt_ty,
    
    inv.eoh_active_regular_units + inv.eoh_inactive_regular_units 
    + inv.eoh_active_clearance_units + inv.eoh_inactive_clearance_units,
    
    inv.eoh_active_regular_retail + inv.eoh_inactive_regular_retail 
    + inv.eoh_active_clearance_retail + inv.eoh_inactive_clearance_retail,
    
    inv.eoh_active_regular_cost + inv.eoh_inactive_regular_cost 
    + inv.eoh_active_clearance_cost + inv.eoh_inactive_clearance_cost,
    
    inv.eoh_active_regular_units + inv.eoh_inactive_regular_units,
    inv.eoh_active_regular_retail + inv.eoh_inactive_regular_retail,
    inv.eoh_active_regular_cost + inv.eoh_inactive_regular_cost,
    inv.eoh_active_clearance_units + inv.eoh_inactive_clearance_units,
    inv.eoh_active_clearance_retail + inv.eoh_inactive_clearance_retail,
    inv.eoh_active_clearance_cost + inv.eoh_inactive_clearance_cost,
    'INV' AS src_ind,    
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
    CURRENT_DATE('PST8PDT') AS dw_sys_load_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inventory_sku_store_day_columnar_vw inv
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim cal
  ON cal.day_idnt_last_year_realigned = inv.day_num
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim dcal
  ON dcal.week_idnt = cal.week_idnt
JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup etl
  ON dcal.day_date = etl.dw_batch_dt
WHERE LOWER(etl.interface_code) = LOWER('MRIC_DAY_AGG_DLY');
