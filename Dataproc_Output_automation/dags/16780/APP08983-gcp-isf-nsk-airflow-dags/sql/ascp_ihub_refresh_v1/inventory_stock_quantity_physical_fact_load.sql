
/*

-- Script Details

--*************************************************************************************************************************************

-- File              : inventory_stock_quantity_physical_fact_load.sql

-- Author            : Bharat Mahajan

-- Description       : Merge INVENTORY_STOCK_QUANTITY_PHYSICAL_FACT table with delta from staging table

-- Source topic      : ascp-inventory-stock-quantity-eis-object-model-avro

-- Object model      : InventoryStockQuantity

-- ETL Run Frequency : Every day

-- Version :         : 0.1

--*************************************************************************************************************************************

-- Change Log: Date Author Description

--*************************************************************************************************************************************

-- 2023-11-10  Andrew Ivchuk    FA-10494: migration from 15850 to 15850

-- 2024-03-01  Chaichenko Oleksandr       FA-11683: Migrate IHUB_REFRESH to Airflow NSK
--*/


-- Update control metrics table with spark counts


CREATE TEMPORARY TABLE isf_dag_control_log_wrk AS
SELECT isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp_utc,
metric_tmstp_tz,
metric_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.isf_dag_control_log;


CREATE TEMPORARY TABLE IF NOT EXISTS inventory_stock_quantity_physical_ldg_tpt_info AS
SELECT jobstarttime,
 jobendtime,
 rowsinserted
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.tpt_job_log
WHERE LOWER(job_name) = LOWER('{{params.dbenv}}_NAP_STG_INVENTORY_STOCK_QUANTITY_PHYSICAL_LDG_JOB')
QUALIFY (ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY rcd_load_tmstp DESC)) = 1;


INSERT INTO isf_dag_control_log_wrk
(
isf_dag_nm,
step_nm,
batch_id,
tbl_nm,
metric_nm,
metric_value,
metric_tmstp_utc,
metric_tmstp_tz,
metric_date
)
WITH dummy AS (SELECT 1 AS num)
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 CAST(NULL AS STRING) AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{{params.fct_table_name}}', 1, 100) AS tbl_nm,
 SUBSTR('TERADATA_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST(CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 NULL AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{{params.ldg_table_name}}', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_CSV_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
    AND LOWER(tbl_nm) = LOWER('INVENTORY_STOCK_QUANTITY_PHYSICAL_LDG')
    AND LOWER(metric_nm) = LOWER('PROCESSED_ROWS_CNT')), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 NULL AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR(NULL, 1, 100) AS tbl_nm,
 SUBSTR('SPARK_PROCESSED_MESSAGE_CNT', 1, 100) AS metric_nm,
 SUBSTR((SELECT metric_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.processed_data_spark_log
   WHERE LOWER(isf_dag_nm) = LOWER('{{params.dag_name}}')
    AND LOWER(metric_nm) = LOWER('SPARK_PROCESSED_MESSAGE_CNT')), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 NULL AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{{params.ldg_table_name}}', 1, 100) AS tbl_nm,
 SUBSTR('LOADED_TO_LDG_CNT', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT rowsinserted
    FROM inventory_stock_quantity_physical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 NULL AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{{params.ldg_table_name}}', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_START_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobstarttime
    FROM inventory_stock_quantity_physical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy
UNION ALL
SELECT '{{params.dag_name}}' AS isf_dag_nm,
 NULL AS step_nm,
  (SELECT batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
 SUBSTR('{{params.dbenv}}_{{params.ldg_table_name}}', 1, 100) AS tbl_nm,
 SUBSTR('TPT_JOB_END_TMSTP', 1, 100) AS metric_nm,
 SUBSTR(CAST((SELECT jobendtime
    FROM inventory_stock_quantity_physical_ldg_tpt_info) AS STRING), 1, 60) AS metric_value,
 CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS metric_tmstp,
 'GMT' AS metric_tmstp_tz,
 CURRENT_DATE('GMT') AS metric_date
FROM dummy;
-- DATA_TIMELINESS_METRIC

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('INVENTORY_STOCK_QUANTITY_PHYSICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  '{{params.dag_name}}',  'teradata_load_teradata',  0,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '{{params.subject_area}}');

/*

-- Merge into the target fact table

*/

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_physical_fact AS fact
USING (SELECT CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(REPLACE(REPLACE(value_updated_time, 'Z', '+00:00'), 'T', ' ') AS DATETIME)) AS TIMESTAMP) AS value_updated_time, rms_sku_id, location_id, CAST(TRUNC(CAST(TRIM(immediately_sellable_qty) AS FLOAT64)) AS INTEGER) AS immediately_sellable_qty, CAST(TRUNC(CAST(TRIM(stock_on_hand_qty) AS FLOAT64)) AS INTEGER) AS stock_on_hand_qty, CAST(TRUNC(CAST(TRIM(unavailable_qty) AS FLOAT64)) AS INTEGER) AS unavailable_qty, CAST(TRUNC(CAST(TRIM(cust_order_return) AS FLOAT64)) AS INTEGER) AS cust_order_return, CAST(TRUNC(CAST(TRIM(pre_inspect_customer_return) AS FLOAT64)) AS INTEGER) AS pre_inspect_customer_return, CAST(TRUNC(CAST(TRIM(unusable_qty) AS FLOAT64)) AS INTEGER) AS unusable_qty, CAST(TRUNC(CAST(TRIM(rtv_reqst_resrv) AS FLOAT64)) AS INTEGER) AS rtv_reqst_resrv, CAST(TRUNC(CAST(TRIM(met_reqst_resrv) AS FLOAT64)) AS INTEGER) AS met_reqst_resrv, CAST(TRUNC(CAST(TRIM(tc_preview) AS FLOAT64)) AS INTEGER) AS tc_preview, CAST(TRUNC(CAST(TRIM(tc_clubhouse) AS FLOAT64)) AS INTEGER) AS tc_clubhouse, CAST(TRUNC(CAST(TRIM(tc_mini_1) AS FLOAT64)) AS INTEGER) AS tc_mini_1, CAST(TRUNC(CAST(TRIM(tc_mini_2) AS FLOAT64)) AS INTEGER) AS tc_mini_2, CAST(TRUNC(CAST(TRIM(tc_mini_3) AS FLOAT64)) AS INTEGER) AS tc_mini_3, CAST(TRUNC(CAST(TRIM(problem) AS FLOAT64)) AS INTEGER) AS problem, CAST(TRUNC(CAST(TRIM(damaged_return) AS FLOAT64)) AS INTEGER) AS damaged_return, CAST(TRUNC(CAST(TRIM(damaged_cosmetic_return) AS FLOAT64)) AS INTEGER) AS damaged_cosmetic_return, CAST(TRUNC(CAST(TRIM(ertm_holds) AS FLOAT64)) AS INTEGER) AS ertm_holds, CAST(TRUNC(CAST(TRIM(nd_unavailable_qty) AS FLOAT64)) AS INTEGER) AS nd_unavailable_qty, CAST(TRUNC(CAST(TRIM(pb_holds_qty) AS FLOAT64)) AS INTEGER) AS pb_holds_qty, CAST(TRUNC(CAST(TRIM(com_co_holds) AS FLOAT64)) AS INTEGER) AS com_co_holds, CAST(TRUNC(CAST(TRIM(wm_holds) AS FLOAT64)) AS INTEGER) AS wm_holds, CAST(TRUNC(CAST(TRIM(store_reserve) AS FLOAT64)) AS INTEGER) AS store_reserve, CAST(TRUNC(CAST(TRIM(tc_holds) AS FLOAT64)) AS INTEGER) AS tc_holds, CAST(TRUNC(CAST(TRIM(returns_holds) AS FLOAT64)) AS INTEGER) AS returns_holds, CAST(TRUNC(CAST(TRIM(fp_holds) AS FLOAT64)) AS INTEGER) AS fp_holds, CAST(TRUNC(CAST(TRIM(transfers_reserve_qty) AS FLOAT64)) AS INTEGER) AS transfers_reserve_qty, CAST(TRUNC(CAST(TRIM(return_to_vendor_qty) AS FLOAT64)) AS INTEGER) AS return_to_vendor_qty, CAST(TRUNC(CAST(TRIM(in_transit_qty) AS FLOAT64)) AS INTEGER) AS in_transit_qty, CAST(TRUNC(CAST(TRIM(store_transfer_reserved_qty) AS FLOAT64)) AS INTEGER) AS store_transfer_reserved_qty, CAST(TRUNC(CAST(TRIM(store_transfer_expected_qty) AS FLOAT64)) AS INTEGER) AS store_transfer_expected_qty, CAST(TRUNC(CAST(TRIM(back_order_reserve_qty) AS FLOAT64)) AS INTEGER) AS back_order_reserve_qty, CAST(TRUNC(CAST(TRIM(oms_back_order_reserve_qty) AS FLOAT64)) AS INTEGER) AS oms_back_order_reserve_qty, on_replenishment, CASE WHEN last_received_date IS NULL OR LOWER(last_received_date) = LOWER('') THEN NULL WHEN LOWER(last_received_date) LIKE LOWER('%.%') THEN CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(CONCAT(last_received_date, '+00:00') AS DATETIME)) AS TIMESTAMP) ELSE CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(CONCAT(last_received_date, '.000+00:00') AS DATETIME)) AS TIMESTAMP) END AS last_received_date, location_type, epm_id, upc, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', return_inspection_qty)) AS FLOAT64)) AS INTEGER) AS return_inspection_qty, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', receiving_qty)) AS FLOAT64)) AS INTEGER) AS receiving_qty, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', damage_qty)) AS FLOAT64)) AS INTEGER) AS damage_qty, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', hold_qty)) AS FLOAT64)) AS INTEGER) AS hold_qty, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', offsite_events)) AS FLOAT64)) AS INTEGER) AS offsite_events, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', alterations_and_repairs)) AS FLOAT64)) AS INTEGER) AS alterations_and_repairs, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', studio_services)) AS FLOAT64)) AS INTEGER) AS studio_services, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', designer_approval)) AS FLOAT64)) AS INTEGER) AS designer_approval, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', single_shoe)) AS FLOAT64)) AS INTEGER) AS single_shoe, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', pickable_hold)) AS FLOAT64)) AS INTEGER) AS pickable_hold, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', pickable_repairable_soh)) AS FLOAT64)) AS INTEGER) AS pickable_repairable_soh, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', pickable_repairable)) AS FLOAT64)) AS INTEGER) AS pickable_repairable, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', pickable_used_soh)) AS FLOAT64)) AS INTEGER) AS pickable_used_soh, CAST(TRUNC(CAST(TRIM(FORMAT('%11d', pickable_used)) AS FLOAT64)) AS INTEGER) AS pickable_used, (SELECT batch_id
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_INVENTORY_STOCK_QUANTITY_PHYSICAL')) AS dw_batch_id, (SELECT curr_batch_date
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_INVENTORY_STOCK_QUANTITY_PHYSICAL')) AS dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_stock_quantity_physical_ldg
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_id, location_id, location_type ORDER BY CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(REPLACE(REPLACE(CAST(value_updated_time AS STRING), 'Z', '+00:00'), 'T', ' ') AS DATETIME)) AS DATETIME) DESC)) = 1) AS t4
ON LOWER(t4.rms_sku_id) = LOWER(fact.rms_sku_id) AND LOWER(t4.location_id) = LOWER(fact.location_id) AND (LOWER(t4.location_type) = LOWER(fact.location_type) OR t4.location_type IS NULL AND fact.location_type IS NULL)
WHEN MATCHED THEN UPDATE SET
    value_updated_time = t4.value_updated_time,
    value_updated_time_tz= '+00:00',
    immediately_sellable_qty = t4.immediately_sellable_qty,
    stock_on_hand_qty = t4.stock_on_hand_qty,
    unavailable_qty = t4.unavailable_qty,
    cust_order_return = t4.cust_order_return,
    pre_inspect_customer_return = t4.pre_inspect_customer_return,
    unusable_qty = t4.unusable_qty,
    rtv_reqst_resrv = t4.rtv_reqst_resrv,
    met_reqst_resrv = t4.met_reqst_resrv,
    tc_preview = t4.tc_preview,
    tc_clubhouse = t4.tc_clubhouse,
    tc_mini_1 = t4.tc_mini_1,
    tc_mini_2 = t4.tc_mini_2,
    tc_mini_3 = t4.tc_mini_3,
    problem = t4.problem,
    damaged_return = t4.damaged_return,
    damaged_cosmetic_return = t4.damaged_cosmetic_return,
    ertm_holds = t4.ertm_holds,
    nd_unavailable_qty = t4.nd_unavailable_qty,
    pb_holds_qty = t4.pb_holds_qty,
    com_co_holds = t4.com_co_holds,
    wm_holds = t4.wm_holds,
    store_reserve = t4.store_reserve,
    tc_holds = t4.tc_holds,
    returns_holds = t4.returns_holds,
    fp_holds = t4.fp_holds,
    transfers_reserve_qty = t4.transfers_reserve_qty,
    return_to_vendor_qty = t4.return_to_vendor_qty,
    in_transit_qty = t4.in_transit_qty,
    store_transfer_reserved_qty = t4.store_transfer_reserved_qty,
    store_transfer_expected_qty = t4.store_transfer_expected_qty,
    back_order_reserve_qty = t4.back_order_reserve_qty,
    oms_back_order_reserve_qty = t4.oms_back_order_reserve_qty,
    on_replenishment = t4.on_replenishment,
    last_received_date = t4.last_received_date,
    last_received_date_tz = '+00:00',
    location_type = t4.location_type,
    epm_id = t4.epm_id,
    upc = t4.upc,
    return_inspection_qty = t4.return_inspection_qty,
    receiving_qty = t4.receiving_qty,
    damage_qty = t4.damage_qty,
    hold_qty = t4.hold_qty,
    offsite_events = t4.offsite_events,
    alterations_and_repairs = t4.alterations_and_repairs,
    studio_services = t4.studio_services,
    designer_approval = t4.designer_approval,
    single_shoe = t4.single_shoe,
    pickable_hold = t4.pickable_hold,
    pickable_repairable_soh = t4.pickable_repairable_soh,
    pickable_repairable = t4.pickable_repairable,
    pickable_used_soh = t4.pickable_used_soh,
    pickable_used = t4.pickable_used,
    dw_batch_id = t4.dw_batch_id,
    dw_batch_date = t4.dw_batch_date,
    dw_sys_updt_tmstp = CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP),
    dw_sys_updt_tmstp_tz='PST8PDT'
WHEN NOT MATCHED THEN INSERT VALUES(t4.value_updated_time,'+00:00', t4.rms_sku_id, t4.location_id, t4.immediately_sellable_qty, t4.stock_on_hand_qty, t4.unavailable_qty, t4.cust_order_return, t4.pre_inspect_customer_return, t4.unusable_qty, t4.rtv_reqst_resrv, t4.met_reqst_resrv, t4.tc_preview, t4.tc_clubhouse, t4.tc_mini_1, t4.tc_mini_2, t4.tc_mini_3, t4.problem, t4.damaged_return, t4.damaged_cosmetic_return, t4.ertm_holds, t4.nd_unavailable_qty, t4.pb_holds_qty, t4.com_co_holds, t4.wm_holds, t4.store_reserve, t4.tc_holds, t4.returns_holds, t4.fp_holds, t4.transfers_reserve_qty, t4.return_to_vendor_qty, t4.in_transit_qty, t4.store_transfer_reserved_qty, t4.store_transfer_expected_qty, t4.back_order_reserve_qty, t4.oms_back_order_reserve_qty, t4.on_replenishment, t4.last_received_date,'+00:00', t4.location_type, t4.epm_id, t4.upc, t4.dw_batch_id, t4.dw_batch_date, CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP),'PST8PDT', CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP),'PST8PDT', t4.return_inspection_qty, t4.receiving_qty, t4.damage_qty, t4.hold_qty, t4.offsite_events, t4.alterations_and_repairs, t4.studio_services, t4.designer_approval, t4.single_shoe, t4.pickable_hold, t4.pickable_repairable_soh, t4.pickable_repairable, t4.pickable_used_soh, t4.pickable_used);

/*

-- Delete snapshot for current date if exist

*/

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_by_day_physical_fact
WHERE snapshot_date = (SELECT curr_batch_date
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_INVENTORY_STOCK_QUANTITY_PHYSICAL'));

/*

-- Create inventory daily snapshot

*/
--SET DM_RETAIN_COMMENT = "-- OR NON_SELLABLE_QTY<>0";

INSERT INTO`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_stock_quantity_by_day_physical_fact (snapshot_date, value_updated_time,value_updated_time_tz, rms_sku_id,
 location_id, immediately_sellable_qty, stock_on_hand_qty, unavailable_qty, cust_order_return,
 pre_inspect_customer_return, unusable_qty, rtv_reqst_resrv, met_reqst_resrv, tc_preview, tc_clubhouse, tc_mini_1,
 tc_mini_2, tc_mini_3, problem, damaged_return, damaged_cosmetic_return, ertm_holds, nd_unavailable_qty, pb_holds_qty,
 com_co_holds, wm_holds, store_reserve, tc_holds, returns_holds, fp_holds, transfers_reserve_qty, return_to_vendor_qty,
 in_transit_qty, store_transfer_reserved_qty, store_transfer_expected_qty, back_order_reserve_qty,
 oms_back_order_reserve_qty, on_replenishment, last_received_date,last_received_date_tz, location_type, epm_id, upc, dw_batch_id,
 dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz, return_inspection_qty, receiving_qty, damage_qty, hold_qty,
 offsite_events, alterations_and_repairs, studio_services, designer_approval, single_shoe, pickable_hold,
 pickable_repairable_soh, pickable_repairable, pickable_used_soh, pickable_used)
(SELECT (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_INVENTORY_STOCK_QUANTITY_PHYSICAL')) AS snapshot_date,
  value_updated_time_utc AS value_updated_time,
  value_updated_time_tz,
  rms_sku_id,
  location_id,
  immediately_sellable_qty,
  stock_on_hand_qty,
  unavailable_qty,
  cust_order_return,
  pre_inspect_customer_return,
  unusable_qty,
  rtv_reqst_resrv,
  met_reqst_resrv,
  tc_preview,
  tc_clubhouse,
  tc_mini_1,
  tc_mini_2,
  tc_mini_3,
  problem,
  damaged_return,
  damaged_cosmetic_return,
  ertm_holds,
  nd_unavailable_qty,
  pb_holds_qty,
  com_co_holds,
  wm_holds,
  store_reserve,
  tc_holds,
  returns_holds,
  fp_holds,
  transfers_reserve_qty,
  return_to_vendor_qty,
  in_transit_qty,
  store_transfer_reserved_qty,
  store_transfer_expected_qty,
  back_order_reserve_qty,
  oms_back_order_reserve_qty,
  on_replenishment,
  last_received_date_utc as last_received_date,
  last_received_date_tz,
  location_type,
  epm_id,
  upc,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp_utc as dw_sys_load_tmstp,
  dw_sys_load_tmstp_tz,
  dw_sys_updt_tmstp_utc as dw_sys_updt_tmstp,
  dw_sys_updt_tmstp_tz,
  return_inspection_qty,
  receiving_qty,
  damage_qty,
  hold_qty,
  offsite_events,
  alterations_and_repairs,
  studio_services,
  designer_approval,
  single_shoe,
  pickable_hold,
  pickable_repairable_soh,
  pickable_repairable,
  pickable_used_soh,
  pickable_used
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_stock_quantity_physical_v1_fact
 WHERE stock_on_hand_qty <> 0
  OR in_transit_qty <> 0
  OR store_transfer_reserved_qty <> 0
  OR store_transfer_expected_qty <> 0
  OR rtv_reqst_resrv <> 0
  OR offsite_events <> 0
  OR alterations_and_repairs <> 0
  OR studio_services <> 0
  OR designer_approval <> 0
  OR single_shoe <> 0
  OR pickable_hold <> 0
  OR pickable_repairable_soh <> 0
  OR pickable_repairable <> 0
  OR pickable_used_soh <> 0
  OR pickable_used <> 0);

/*

-- DATA_TIMELINESS_METRIC

*/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('INVENTORY_STOCK_QUANTITY_PHYSICAL_FACT',  '{{params.dbenv}}_NAP_FCT',  '{{params.dag_name}}',  'teradata_load_teradata',  1,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '{{params.subject_area}}');

/*

-- Collect stats on fact tables

*/
--COLLECT STATS ON {dbenv}_NAP_FCT.INVENTORY_STOCK_QUANTITY_PHYSICAL_FACT INDEX ( rms_sku_id, location_id );
/*

-- Perform audit between stg and fct tables

*/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1('INVENTORY_STOCK_QUANTITY_PHYSICAL_LDG_TO_BASE',  'NAP_ASCP_INVENTORY_STOCK_QUANTITY_PHYSICAL',  '{{params.dbenv}}_NAP_STG',  'INVENTORY_STOCK_QUANTITY_PHYSICAL_LDG',  '{{params.dbenv}}_NAP_FCT',  'INVENTORY_STOCK_QUANTITY_PHYSICAL_FACT',  'Count_Distinct',  0,  'T-S',  'concat( rms_sku_id, location_id, location_type )',  'concat( rms_sku_id, location_id, location_type )',  NULL,  NULL,  'Y');

/*

-- Update control metrics table with temp table

*/

INSERT INTO isf_dag_control_log_wrk
(SELECT '{{params.dag_name}}' AS isf_dag_nm,
  CAST(NULL AS STRING) AS step_nm,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('{{params.subject_area}}')) AS batch_id,
  SUBSTR('{{params.dbenv}}_{{params.fct_table_name}}', 1, 100) AS tbl_nm,
  SUBSTR('TERADATA_JOB_END_TMSTP', 1, 100) AS metric_nm,
  SUBSTR(CAST(CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS STRING), 1, 60) AS metric_value,
  CAST(CAST(CURRENT_DATETIME('GMT') AS TIMESTAMP)  AS TIMESTAMP) AS metric_tmstp,
  'GMT' AS metric_tmstp_tz,
  CURRENT_DATE('GMT') AS metric_date);


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_utl.isf_dag_control_log  t0 SET
    t0.metric_value = wrk.metric_value,
    t0.metric_tmstp = wrk.metric_tmstp_utc,
    t0.metric_tmstp_tz = wrk.metric_tmstp_tz 
    FROM 
    isf_dag_control_log_wrk AS wrk
WHERE LOWER(t0.isf_dag_nm) = LOWER(wrk.isf_dag_nm) 
AND LOWER(COALESCE(t0.tbl_nm, '')) = LOWER(COALESCE(wrk.tbl_nm, ''))
AND LOWER(t0.metric_nm) = LOWER(wrk.metric_nm)
AND t0.batch_id = wrk.batch_id;
