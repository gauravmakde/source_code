-- 
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_allocation_fact_sku_store_load;
---Task_Name=merch_allocation_fact_sku_store_inv_load;'*/
-- ET;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_allocation_fact_sku_store_wrk 
(store_num, 
rms_sku_num, 
week_num, 
business_unit_num,
dw_sys_load_dt, 
src_ind)
(SELECT CAST(trunc(cast(COALESCE(inv_curr.location_id, inv_prev.location_id) AS FLOAT64)) AS INTEGER) AS store_num,
  SUBSTR(COALESCE(inv_curr.rms_sku_id, inv_prev.rms_sku_id), 1, 10) AS rms_sku_num,
   (SELECT dcd.week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS dcd
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS etl ON dcd.day_date = etl.dw_batch_dt
   WHERE LOWER(etl.interface_code) = LOWER('MERCH_ALLOCATION_DLY')) AS week_num,
  COALESCE(inv_curr.business_unit_num, inv_prev.business_unit_num) AS business_unit_num,
  CURRENT_DATE('PST8PDT') AS dw_sys_load_dt,
  'INVENTORY' AS src_ind
 FROM (SELECT inv.snapshot_date,
    inv.value_updated_time,
    inv.rms_sku_id,
    inv.location_id,
    inv.immediately_sellable_qty,
    inv.stock_on_hand_qty,
    inv.unavailable_qty,
    inv.cust_order_return,
    inv.pre_inspect_customer_return,
    inv.unusable_qty,
    inv.rtv_reqst_resrv,
    inv.met_reqst_resrv,
    inv.tc_preview,
    inv.tc_clubhouse,
    inv.tc_mini_1,
    inv.tc_mini_2,
    inv.tc_mini_3,
    inv.problem,
    inv.damaged_return,
    inv.damaged_cosmetic_return,
    inv.ertm_holds,
    inv.nd_unavailable_qty,
    inv.pb_holds_qty,
    inv.com_co_holds,
    inv.wm_holds,
    inv.store_reserve,
    inv.tc_holds,
    inv.returns_holds,
    inv.fp_holds,
    inv.transfers_reserve_qty,
    inv.return_to_vendor_qty,
    inv.in_transit_qty,
    inv.store_transfer_reserved_qty,
    inv.store_transfer_expected_qty,
    inv.back_order_reserve_qty,
    inv.oms_back_order_reserve_qty,
    inv.on_replenishment,
    inv.last_received_date,
    inv.location_type,
    inv.epm_id,
    inv.upc,
    inv.return_inspection_qty,
    inv.receiving_qty,
    inv.damage_qty,
    inv.hold_qty,
    inv.dw_batch_id,
    inv.dw_batch_date,
    inv.dw_sys_load_tmstp,

    inv.dw_sys_updt_tmstp,

    sd.business_unit_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_logical_fact AS inv
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd ON CAST(inv.location_id AS FLOAT64) = sd.store_num
   WHERE inv.snapshot_date = (SELECT dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_ALLOCATION_DLY'))
    AND sd.business_unit_num IN (1000, 2000, 5000)) AS inv_curr
  FULL JOIN (SELECT inv0.snapshot_date,
    inv0.value_updated_time,
    inv0.rms_sku_id,
    inv0.location_id,
    inv0.immediately_sellable_qty,
    inv0.stock_on_hand_qty,
    inv0.unavailable_qty,
    inv0.cust_order_return,
    inv0.pre_inspect_customer_return,
    inv0.unusable_qty,
    inv0.rtv_reqst_resrv,
    inv0.met_reqst_resrv,
    inv0.tc_preview,
    inv0.tc_clubhouse,
    inv0.tc_mini_1,
    inv0.tc_mini_2,
    inv0.tc_mini_3,
    inv0.problem,
    inv0.damaged_return,
    inv0.damaged_cosmetic_return,
    inv0.ertm_holds,
    inv0.nd_unavailable_qty,
    inv0.pb_holds_qty,
    inv0.com_co_holds,
    inv0.wm_holds,
    inv0.store_reserve,
    inv0.tc_holds,
    inv0.returns_holds,
    inv0.fp_holds,
    inv0.transfers_reserve_qty,
    inv0.return_to_vendor_qty,
    inv0.in_transit_qty,
    inv0.store_transfer_reserved_qty,
    inv0.store_transfer_expected_qty,
    inv0.back_order_reserve_qty,
    inv0.oms_back_order_reserve_qty,
    inv0.on_replenishment,
    inv0.last_received_date,
    inv0.location_type,
    inv0.epm_id,
    inv0.upc,
    inv0.return_inspection_qty,
    inv0.receiving_qty,
    inv0.damage_qty,
    inv0.hold_qty,
    inv0.dw_batch_id,
    inv0.dw_batch_date,
    inv0.dw_sys_load_tmstp,

    inv0.dw_sys_updt_tmstp,
    
    sd0.business_unit_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_logical_fact AS inv0
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS sd0 ON CAST(inv0.location_id AS FLOAT64) = sd0.store_num
   WHERE inv0.snapshot_date = (SELECT DATE_SUB(dw_batch_dt, INTERVAL 1 DAY)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup
      WHERE LOWER(interface_code) = LOWER('MERCH_ALLOCATION_DLY'))
    AND sd0.business_unit_num IN (1000, 2000, 5000)) AS inv_prev ON LOWER(inv_curr.rms_sku_id) = LOWER(inv_prev.rms_sku_id
     ) AND LOWER(inv_curr.location_id) = LOWER(inv_prev.location_id)
 WHERE inv_curr.stock_on_hand_qty - inv_curr.store_transfer_reserved_qty - inv_curr.return_to_vendor_qty - (inv_curr.unavailable_qty
                  + inv_curr.pre_inspect_customer_return + inv_curr.unusable_qty + inv_curr.rtv_reqst_resrv + inv_curr.met_reqst_resrv
               + inv_curr.tc_preview + inv_curr.tc_clubhouse + inv_curr.tc_mini_1 + inv_curr.tc_mini_2 + inv_curr.tc_mini_3
          + inv_curr.nd_unavailable_qty + inv_curr.pb_holds_qty + inv_curr.store_reserve) <> inv_prev.stock_on_hand_qty
      - inv_prev.store_transfer_reserved_qty - inv_prev.return_to_vendor_qty - (inv_prev.unavailable_qty + inv_prev.pre_inspect_customer_return
                  + inv_prev.unusable_qty + inv_prev.rtv_reqst_resrv + inv_prev.met_reqst_resrv + inv_prev.tc_preview +
            inv_prev.tc_clubhouse + inv_prev.tc_mini_1 + inv_prev.tc_mini_2 + inv_prev.tc_mini_3 + inv_prev.nd_unavailable_qty
         + inv_prev.pb_holds_qty + inv_prev.store_reserve)
  OR inv_curr.in_transit_qty + inv_curr.store_transfer_expected_qty <> inv_prev.in_transit_qty + inv_prev.store_transfer_expected_qty
    
  OR inv_curr.rms_sku_id IS NULL
  OR inv_prev.rms_sku_id IS NULL);

-- ET;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- ET;
