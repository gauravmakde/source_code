--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : jwn_dq_metrics_fact_complete.sql
-- Author                  : Oleksandr Chaichenko
-- Description             : Provide information -  Zero count loaded DQ and SRC/TGT checks
-- Data Source             : NAP_JWN_METRICS_BASE views
-- ETL Run Frequency       : Daily
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-06-19  Oleksandr Chaichenko     FA-8789: Code Refactor - for Ongoing Delta Load in Production
--*************************************************************************************************************************************


/*Zero COUNT CHECK*/
MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT SUBSTR('JWN_PURCHASE_ORDER_RMS_RECEIPTS_FACT', 1, 100) AS dq_object_name,
   'records_loaded' AS dq_metric_name,
   'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    (SELECT curr_batch_date
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
   CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    (SELECT CASE
      WHEN COUNT(1) = 0
      THEN 0
      ELSE 1
      END AS cnt1
    FROM (SELECT purchase_order_num,
       carton_num,
       location_num,
       rms_sku_num,
       reporting_date,
       store_inbound_receipt_flag,
       received_qty,
       dw_batch_id,
       dw_batch_date,
       dw_sys_load_tmstp,
       dw_sys_updt_tmstp
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact
      WHERE dw_batch_date = (SELECT curr_batch_date
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
      LIMIT 1) AS t) AS dq_metric_value,
   'TARGET' AS dq_metric_value_type,
   'F' AS is_sensitive,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp
  UNION ALL
  SELECT SUBSTR('JWN_EXTERNAL_IN_TRANSIT_FACT', 1, 100) AS dq_object_name,
   'records_loaded' AS dq_metric_name,
   'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    (SELECT curr_batch_date
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
   CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    (SELECT CASE
      WHEN COUNT(1) = 0
      THEN 0
      ELSE 1
      END AS cnt1
    FROM (SELECT purchase_order_num,
       rms_sku_num,
       po_distributed_location_num,
       in_transit_activity_date,
       cumulative_external_in_transit_qty,
       dw_batch_id,
       dw_batch_date,
       dw_sys_load_tmstp,
       dw_sys_updt_tmstp
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_external_in_transit_fact
      WHERE dw_batch_date = (SELECT curr_batch_date
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
      LIMIT 1) AS t) AS dq_metric_value,
   'TARGET' AS dq_metric_value_type,
   'F' AS is_sensitive,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp
  UNION ALL
  SELECT SUBSTR('JWN_TRANSFERS_RMS_FACT', 1, 100) AS dq_object_name,
   'records_loaded' AS dq_metric_name,
   'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    (SELECT curr_batch_date
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
   CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    (SELECT CASE
      WHEN COUNT(1) = 0
      THEN 0
      ELSE 1
      END AS cnt1
    FROM (SELECT rms_sku_num,
       operation_type,
       from_location_num,
       to_location_num,
       transfer_in_date,
       transfer_out_date,
       transfer_type,
       transfer_context_type,
       transfer_in_qty,
       transfer_out_qty,
       dw_batch_id,
       dw_batch_date,
       dw_sys_load_tmstp,
       dw_sys_updt_tmstp
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact
      WHERE dw_batch_date = (SELECT curr_batch_date
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
      LIMIT 1) AS t) AS dq_metric_value,
   'TARGET' AS dq_metric_value_type,
   'F' AS is_sensitive,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp
  UNION ALL
  SELECT SUBSTR('JWN_INVENTORY_SKU_LOC_DAY_FACT', 1, 100) AS dq_object_name,
   'records_loaded' AS dq_metric_name,
   'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    (SELECT curr_batch_date
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
   CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    (SELECT CASE
      WHEN COUNT(1) = 0
      THEN 0
      ELSE 1
      END AS cnt1
    FROM (SELECT rms_sku_num,
       location_num,
       location_type,
       reporting_date,
       unit_price_amt,
       price_type_code,
       boh_qty,
       eoh_qty,
       physical_sellable_qty,
       unavailable_qty,
       damaged_qty,
       problem_qty,
       damaged_return_qty,
       hold_qty,
       reserved_qty,
       in_transit_qty,
       received_qty,
       direct_to_store_receipt_qty,
       last_receipt_date,
       sku_network_last_receipt_date,
       last_transfer_exposed_date,
       po_transfer_in_qty,
       reserve_stock_transfer_in_qty,
       pack_and_hold_transfer_in_qty,
       racking_transfer_in_qty,
       ghost_transfer_in_qty,
       po_transfer_out_qty,
       reserve_stock_transfer_out_qty,
       pack_and_hold_transfer_out_qty,
       racking_transfer_out_qty,
       ghost_transfer_out_qty,
       rtv_qty,
       inventory_availability_flag,
       inventory_source,
       last_transfer_channel_exposed_date,
       other_transfer_in_qty,
       other_transfer_out_qty,
       channel_num,
       channel_desc,
       store_country_code,
       po_transfer_in_retail_amt,
       reserve_stock_transfer_in_retail_amt,
       pack_and_hold_transfer_in_retail_amt,
       racking_transfer_in_retail_amt,
       ghost_transfer_in_retail_amt,
       other_transfer_in_retail_amt,
       vendor_product_num,
       sku_desc,
       color_num,
       color_desc,
       nord_display_color,
       rms_style_num,
       style_desc,
       style_group_num,
       style_group_desc,
       sbclass_num,
       sbclass_name,
       class_num,
       class_name,
       dept_num,
       dept_name,
       subdivision_num,
       subdivision_name,
       division_num,
       division_name,
       reclass_ind,
       return_disposition_code,
       selling_status_desc,
       selling_channel_eligibility_list,
       prmy_supp_num,
       vendor_brand_name,
       supplier_num,
       supplier_name,
       payto_vendor_num,
       payto_vendor_name,
       npg_ind,
       gwp_ind,
       smart_sample_ind,
       dw_batch_id,
       dw_batch_date,
       dw_sys_load_tmstp,
       dw_sys_updt_tmstp
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact
      WHERE dw_batch_date = (SELECT curr_batch_date
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
       AND reporting_date >= DATE_SUB((SELECT curr_batch_date
          FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
          WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')), INTERVAL 2 DAY)
      LIMIT 1) AS t) AS dq_metric_value,
   'TARGET' AS dq_metric_value_type,
   'F' AS is_sensitive,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
   LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT tbl.dq_object_name,
  tbl.subject_area_nm,
  tbl.dw_batch_date,
  tbl.dag_completion_timestamp,
  tbl.dq_metric_value_type,
  tbl.is_sensitive,
  tbl.dw_record_load_tmstp,
  tbl.dw_record_updt_tmstp,
  t5.dq_metric_name,
  CAST(CASE
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.eoh_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.reserved_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.physical_sellable_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.unavailable_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.damaged_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.problem_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.damaged_return_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.hold_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.in_transit_qty
    ELSE NULL
    END AS BIGINT) AS dq_metric_value
 FROM (SELECT 'JWN_INVENTORY_SKU_LOC_DAY_FACT' AS dq_object_name,
    'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    snapshot_date AS dw_batch_date,
    CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    'SOURCE' AS dq_metric_value_type,
    'F' AS is_sensitive,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp,
    IFNULL(SUM(stock_on_hand_qty), 0) AS eoh_qty,
    IFNULL(SUM(IFNULL(store_transfer_reserved_qty, 0) + IFNULL(back_order_reserve_qty, 0)), 0) AS reserved_qty,
    IFNULL(SUM(immediately_sellable_qty), 0) AS physical_sellable_qty,
    IFNULL(SUM(unavailable_qty), 0) AS unavailable_qty,
    IFNULL(SUM(damage_qty), 0) AS damaged_qty,
    IFNULL(SUM(problem), 0) AS problem_qty,
    IFNULL(SUM(damaged_return), 0) AS damaged_return_qty,
    IFNULL(SUM(hold_qty), 0) AS hold_qty,
    SUM(in_transit_qty) AS in_transit_qty
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.inventory_stock_quantity_by_day_logical_fact
   WHERE snapshot_date = (SELECT curr_batch_date
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
   GROUP BY dq_object_name,
    subject_area_nm,
    dw_batch_date,
    dag_completion_timestamp) AS tbl
  INNER JOIN (SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name) AS t5 ON TRUE
 WHERE CASE
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.eoh_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.reserved_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.physical_sellable_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.unavailable_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.damaged_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.problem_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.damaged_return_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.hold_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.in_transit_qty
   ELSE NULL
   END IS NOT NULL) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
    LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND LOWER(tgt.dq_metric_value_type) = LOWER(SRC.dq_metric_value_type
    ) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT tbl.dq_object_name,
  tbl.subject_area_nm,
  tbl.dw_batch_date,
  tbl.dag_completion_timestamp,
  tbl.dq_metric_value_type,
  tbl.is_sensitive,
  tbl.dw_record_load_tmstp,
  tbl.dw_record_updt_tmstp,
  t5.dq_metric_name,
  CAST(CASE
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.eoh_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.reserved_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.physical_sellable_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.unavailable_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.damaged_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.problem_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.damaged_return_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.hold_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.in_transit_qty
    ELSE NULL
    END AS BIGINT) AS dq_metric_value
 FROM (SELECT 'JWN_INVENTORY_SKU_LOC_DAY_FACT' AS dq_object_name,
    'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    reporting_date AS dw_batch_date,
    CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    'TARGET' AS dq_metric_value_type,
    'F' AS is_sensitive,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp,
    IFNULL(SUM(eoh_qty), 0) AS eoh_qty,
    IFNULL(SUM(reserved_qty), 0) AS reserved_qty,
    IFNULL(SUM(physical_sellable_qty), 0) AS physical_sellable_qty,
    IFNULL(SUM(unavailable_qty), 0) AS unavailable_qty,
    IFNULL(SUM(damaged_qty), 0) AS damaged_qty,
    IFNULL(SUM(problem_qty), 0) AS problem_qty,
    IFNULL(SUM(damaged_return_qty), 0) AS damaged_return_qty,
    IFNULL(SUM(hold_qty), 0) AS hold_qty,
    SUM(in_transit_qty) AS in_transit_qty
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact
   WHERE reporting_date = (SELECT curr_batch_date
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
   GROUP BY dq_object_name,
    subject_area_nm,
    dw_batch_date,
    dag_completion_timestamp) AS tbl
  INNER JOIN (SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name) AS t5 ON TRUE
 WHERE CASE
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.eoh_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.reserved_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.physical_sellable_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.unavailable_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.damaged_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.problem_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.damaged_return_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.hold_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.in_transit_qty
   ELSE NULL
   END IS NOT NULL) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
    LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND LOWER(tgt.dq_metric_value_type) = LOWER(SRC.dq_metric_value_type
    ) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT tbl.dq_object_name,
  tbl.subject_area_nm,
  tbl.dw_batch_date,
  tbl.dag_completion_timestamp,
  tbl.dq_metric_value_type,
  tbl.is_sensitive,
  tbl.dw_record_load_tmstp,
  tbl.dw_record_updt_tmstp,
  t6.dq_metric_name,
  CAST(trunc(cast(CASE
    WHEN t6.dq_metric_name = 'null'
    THEN tbl.received_qty
    WHEN t6.dq_metric_name = 'null'
    THEN tbl.direct_to_store_receipt_qty
    ELSE NULL
    END as float64)) AS INTEGER) AS dq_metric_value
 FROM (SELECT 'JWN_PURCHASE_ORDER_RMS_RECEIPTS_FACT' AS dq_object_name,
    'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    a.received_date AS dw_batch_date,
    CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    'SOURCE' AS dq_metric_value_type,
    'F' AS is_sensitive,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp,
    SUM(a.shipment_qty) AS received_qty,
    SUM(CASE
      WHEN LOWER(a.tofacility_type) = LOWER('S')
      THEN a.shipment_qty
      ELSE 0
      END) AS direct_to_store_receipt_qty
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.po_receipt_fact AS a
    INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.purchase_order_fact AS b ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_num)
   WHERE a.received_date = (SELECT curr_batch_date
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    AND LOWER(b.dropship_ind) <> LOWER('t')
    AND CASE
      WHEN a.tofacility_id < 1000
      THEN COALESCE(a.tofacility_id, 0)
      ELSE COALESCE(a.tofacility_id - 10000, 0)
      END NOT IN (828, 808)
   GROUP BY dq_object_name,
    subject_area_nm,
    dw_batch_date
   HAVING received_qty <> 0) AS tbl
  INNER JOIN (SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name) AS t6 ON TRUE
 WHERE CASE
   WHEN t6.dq_metric_name = 'null'
   THEN tbl.received_qty
   WHEN t6.dq_metric_name = 'null'
   THEN tbl.direct_to_store_receipt_qty
   ELSE NULL
   END IS NOT NULL) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
    LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND LOWER(tgt.dq_metric_value_type) = LOWER(SRC.dq_metric_value_type
    ) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT tbl.dq_object_name,
  tbl.subject_area_nm,
  tbl.dw_batch_date,
  tbl.dag_completion_timestamp,
  tbl.dq_metric_value_type,
  tbl.is_sensitive,
  tbl.dw_record_load_tmstp,
  tbl.dw_record_updt_tmstp,
  t6.dq_metric_name,
  CAST(trunc(cast(CASE
    WHEN t6.dq_metric_name = 'null'
    THEN tbl.received_qty
    WHEN t6.dq_metric_name = 'null'
    THEN tbl.direct_to_store_receipt_qty
    ELSE NULL
    END as float64)) AS INTEGER) AS dq_metric_value
 FROM (SELECT 'JWN_PURCHASE_ORDER_RMS_RECEIPTS_FACT' AS dq_object_name,
    'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    reporting_date AS dw_batch_date,
    CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    'TARGET' AS dq_metric_value_type,
    'F' AS is_sensitive,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp,
    SUM(received_qty) AS received_qty,
    SUM(CASE
      WHEN LOWER(store_inbound_receipt_flag) = LOWER('Y')
      THEN received_qty
      ELSE 0
      END) AS direct_to_store_receipt_qty
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
   WHERE reporting_date = (SELECT curr_batch_date
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
   GROUP BY dq_object_name,
    subject_area_nm,
    dw_batch_date
   HAVING received_qty <> 0) AS tbl
  INNER JOIN (SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name) AS t6 ON TRUE
 WHERE CASE
   WHEN t6.dq_metric_name = 'null'
   THEN tbl.received_qty
   WHEN t6.dq_metric_name = 'null'
   THEN tbl.direct_to_store_receipt_qty
   ELSE NULL
   END IS NOT NULL) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
    LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND LOWER(tgt.dq_metric_value_type) = LOWER(SRC.dq_metric_value_type
    ) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT tbl.dq_object_name,
  tbl.subject_area_nm,
  tbl.dw_batch_date,
  tbl.dag_completion_timestamp,
  tbl.dq_metric_value_type,
  tbl.is_sensitive,
  tbl.dw_record_load_tmstp,
  tbl.dw_record_updt_tmstp,
  t6.dq_metric_name,
  CAST(trunc(cast(CASE
    WHEN t6.dq_metric_name = 'null'
    THEN tbl.transfer_in_qty
    WHEN t6.dq_metric_name = 'null'
    THEN tbl.transfer_out_qty
    ELSE NULL
    END as float64)) AS INTEGER) AS dq_metric_value
 FROM (SELECT 'JWN_TRANSFERS_RMS_FACT' AS dq_object_name,
    'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    ship_date AS dw_batch_date,
    CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    'SOURCE' AS dq_metric_value_type,
    'F' AS is_sensitive,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp,
    SUM(receipt_qty) AS transfer_in_qty,
    SUM(ship_qty) AS transfer_out_qty
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.transfer_shipment_receipt_logical_fact AS a
   WHERE ship_date = (SELECT curr_batch_date
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    AND from_location_id IS NOT NULL
   GROUP BY dq_object_name,
    subject_area_nm,
    dw_batch_date
   HAVING transfer_out_qty <> 0) AS tbl
  INNER JOIN (SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name) AS t6 ON TRUE
 WHERE CASE
   WHEN t6.dq_metric_name = 'null'
   THEN tbl.transfer_in_qty
   WHEN t6.dq_metric_name = 'null'
   THEN tbl.transfer_out_qty
   ELSE NULL
   END IS NOT NULL) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
    LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND LOWER(tgt.dq_metric_value_type) = LOWER(SRC.dq_metric_value_type
    ) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT tbl.dq_object_name,
  tbl.subject_area_nm,
  tbl.dw_batch_date,
  tbl.dag_completion_timestamp,
  tbl.dq_metric_value_type,
  tbl.is_sensitive,
  tbl.dw_record_load_tmstp,
  tbl.dw_record_updt_tmstp,
  t5.dq_metric_name,
  CAST(trunc(cast(CASE
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.transfer_in_qty
    WHEN t5.dq_metric_name = 'null'
    THEN tbl.transfer_out_qty
    ELSE NULL
    END as float64)) AS INTEGER) AS dq_metric_value
 FROM (SELECT 'JWN_TRANSFERS_RMS_FACT' AS dq_object_name,
    'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
    transfer_out_date AS dw_batch_date,
    CAST(NULL AS DATETIME) AS dag_completion_timestamp,
    'TARGET' AS dq_metric_value_type,
    'F' AS is_sensitive,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp,
    SUM(transfer_in_qty) AS transfer_in_qty,
    SUM(transfer_out_qty) AS transfer_out_qty
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_transfers_rms_fact AS a
   WHERE transfer_out_date = (SELECT curr_batch_date
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
      WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
   GROUP BY dq_object_name,
    subject_area_nm,
    dw_batch_date) AS tbl
  INNER JOIN (SELECT 'null' AS dq_metric_name
    UNION ALL
    SELECT 'null' AS dq_metric_name) AS t5 ON TRUE
 WHERE CASE
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.transfer_in_qty
   WHEN t5.dq_metric_name = 'null'
   THEN tbl.transfer_out_qty
   ELSE NULL
   END IS NOT NULL) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
    LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND LOWER(tgt.dq_metric_value_type) = LOWER(SRC.dq_metric_value_type
    ) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT 'JWN_EXTERNAL_IN_TRANSIT_FACT' AS dq_object_name,
  'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
   (SELECT curr_batch_date
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
  CAST(NULL AS DATETIME) AS dag_completion_timestamp,
  'SOURCE' AS dq_metric_value_type,
  'F' AS is_sensitive,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp,
  'purchase_order_num' AS dq_metric_name,
  COUNT(purchase_order_num) AS dq_metric_value
 FROM (SELECT LTRIM(purchase_order_num, '0') AS purchase_order_num
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.vendor_asn_fact
    WHERE dw_sys_updt_tmstp BETWEEN (SELECT extract_from_tmstp
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AND (SELECT extract_to_tmstp
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
    UNION DISTINCT
    SELECT purchase_order_num
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS t
    WHERE dw_batch_date = (SELECT curr_batch_date
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
     AND EXISTS (SELECT 1
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.vendor_asn_fact AS a
      WHERE edi_date >= DATE '2022-10-30'
       AND LOWER(LTRIM(purchase_order_num, '0')) = LOWER(LTRIM(purchase_order_num, '0'))
       AND LOWER(LTRIM(rms_sku_num, '0')) = LOWER(rms_sku_num))) AS tbl) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
    LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND LOWER(tgt.dq_metric_value_type) = LOWER(SRC.dq_metric_value_type
    ) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_fct.dq_metrics_fact AS tgt
USING (SELECT 'JWN_EXTERNAL_IN_TRANSIT_FACT' AS dq_object_name,
  'NAP_ASCP_CLARITY_LOAD' AS subject_area_nm,
  dw_batch_date,
  CAST(NULL AS DATETIME) AS dag_completion_timestamp,
  'TARGET' AS dq_metric_value_type,
  'F' AS is_sensitive,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_record_updt_tmstp,
  'purchase_order_num' AS dq_metric_name,
  COUNT(DISTINCT purchase_order_num) AS dq_metric_value
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_external_in_transit_fact
 WHERE dw_batch_id = (SELECT batch_id
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')
     AND curr_batch_date = (SELECT curr_batch_date
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')))
 GROUP BY dw_batch_date) AS SRC
ON LOWER(tgt.subject_area_nm) = LOWER(SRC.subject_area_nm) AND LOWER(tgt.dq_object_name) = LOWER(SRC.dq_object_name) AND
    LOWER(tgt.dq_metric_name) = LOWER(SRC.dq_metric_name) AND LOWER(tgt.dq_metric_value_type) = LOWER(SRC.dq_metric_value_type
    ) AND tgt.dw_batch_date = SRC.dw_batch_date
WHEN MATCHED THEN UPDATE SET
 dw_record_updt_tmstp = SRC.dw_record_updt_tmstp,
 dq_metric_value = SRC.dq_metric_value
WHEN NOT MATCHED THEN INSERT VALUES(SRC.dq_object_name, SRC.dq_metric_name, SRC.subject_area_nm, SRC.dw_batch_date, SRC
 .dag_completion_timestamp, SRC.dq_metric_value, SRC.dq_metric_value_type, SRC.is_sensitive, SRC.dw_record_load_tmstp,
 SRC.dw_record_updt_tmstp);