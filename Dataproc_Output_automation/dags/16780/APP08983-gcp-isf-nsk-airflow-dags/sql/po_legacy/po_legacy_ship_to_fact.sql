/***********************************************************************************
-- Insert The Timeliness Load Data
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT',  '{{params.dbenv}}_nap_fct',  'po_legacy_kafka_to_orc_td',  'ldg_to_fact',  1,  'LOAD_START',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'SCOI_PO_LEGACY');


/***********************************************************************************
-- Collect all validation failure records in error tables
************************************************************************************/
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.purchase_order_item_shiplocation_fact_err (purchase_order_num, rms_sku_num, ship_location_id,
 case_pack_ind, cancel_code, cancel_date, canceledby_id, close_date, distribution_description, distribution_id,
 distribution_method, distribution_status, estimatedin_stock_date, external_distribution_id, last_received_ind,
 non_scaling_ind, origin_code, original_replenish_qty, canceled_qty, ordered_qty, prescaled_qty, received_qty,
 allocated_notreleased_qty, release_date, earliest_ship_date, epm_sku_num, initial_unit_cost, latest_ship_date,
 item_nonscaling_ind, origin_country, reference_item, unit_retail_amt, supplier_packsize, unit_cost_amt,
 universal_product_code, universal_product_code_supplement, error_table, error_code, error_desc, dw_batch_id,
 dw_batch_date)
(SELECT TRIM(purchaseorder_externalid) AS purchaseorder_externalid,
  TRIM(purchaseorder_items_externalitemid) AS purchaseorder_items_externalitemid,
  purchaseorder_items_shiplocations_id,
  purchaseorder_items_casepackindicator,
  purchaseorder_items_shiplocations_cancelcode,
  CAST(purchaseorder_items_shiplocations_canceldate AS DATE) AS purchaseorder_items_shiplocations_canceldate,
  purchaseorder_items_shiplocations_canceledbyid,
  CAST(purchaseorder_items_shiplocations_closedate AS DATE) AS purchaseorder_items_shiplocations_closedate,
  purchaseorder_items_shiplocations_distributiondescription,
  purchaseorder_items_shiplocations_distributionid,
  purchaseorder_items_shiplocations_distributionmethod,
  purchaseorder_items_shiplocations_distributionstatus,
  CAST(purchaseorder_items_shiplocations_estimatedinstockdate AS DATE) AS
  purchaseorder_items_shiplocations_estimatedinstockdate,
  purchaseorder_items_shiplocations_externaldistributionid,
  CAST(purchaseorder_items_shiplocations_lastreceived AS STRING) AS purchaseorder_items_shiplocations_lastreceived,
  purchaseorder_items_shiplocations_nonscalingindicator,
  purchaseorder_items_shiplocations_originindicator,
  purchaseorder_items_shiplocations_originalreplenishquantity,
  purchaseorder_items_shiplocations_quantitycanceled,
  purchaseorder_items_shiplocations_quantityordered,
  purchaseorder_items_shiplocations_quantityprescaled,
  purchaseorder_items_shiplocations_quantityreceived,
  purchaseorder_items_shiplocations_quantityreceivedallocatednotreleased,
  CAST(purchaseorder_items_shiplocations_releasedate AS DATE) AS purchaseorder_items_shiplocations_releasedate,
  CAST(purchaseorder_items_earliestshipdate AS DATE) AS purchaseorder_items_earliestshipdate,
  CAST(purchaseorder_items_itemid AS STRING) AS purchaseorder_items_itemid,
  ROUND(CAST(purchaseorder_items_initialunitcost AS NUMERIC), 2) AS purchaseorder_items_initialunitcost,
  CAST(purchaseorder_items_latestshipdate AS DATE) AS purchaseorder_items_latestshipdate,
  purchaseorder_items_nonscalingindicator,
  purchaseorder_items_origincountry,
  purchaseorder_items_referenceitem,
  ROUND(CAST(purchaseorder_items_shiplocations_unitretail AS NUMERIC), 2) AS
  purchaseorder_items_shiplocations_unitretail,
  purchaseorder_items_supplierpacksize,
  ROUND(CAST(purchaseorder_items_unitcost AS NUMERIC), 2) AS purchaseorder_items_unitcost,
  purchaseorder_items_universalproductcode,
  purchaseorder_items_universalproductcodesupplement,
  'PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT' AS error_table,
  2 AS error_code,
  'Date/Number/Mandatory Field Validation Failed in PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT Table' AS error_desc,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER')) AS dw_batch_date
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_item_shiplocation_ldg AS ldg
 WHERE purchaseorder_externalid IS NULL
  OR purchaseorder_items_externalitemid IS NULL
  OR purchaseorder_items_shiplocations_id IS NULL
  OR LENGTH(TRIM(purchaseorder_items_externalitemid)) > 16);


/***********************************************************************************
-- Insert The Timeliness Load Data
************************************************************************************/

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT',  '{{params.dbenv}}_NAP_FCT',  'po_legacy_kafka_to_orc_td',  'ldg_to_fact',  2,  'INTERMEDIATE',  'Load the PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT_ERR with records containing errors',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'SCOI_PO_LEGACY');

/***********************************************************************************
-- Delete purchase order items that was modified
************************************************************************************/

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.purchase_order_item_shiplocation_fact AS fact
WHERE purchase_order_num IN (SELECT DISTINCT TRIM(purchaseorder_externalid) 
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_ldg AS ldg);

/***********************************************************************************
-- Insert The Timeliness Load Data
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT',  '{{params.dbenv}}_NAP_FCT',  'po_legacy_kafka_to_orc_td',  'ldg_to_fact',  3,  'INTERMEDIATE',  'Deleting new POs from the target table if they had existed there',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'SCOI_PO_LEGACY');

/***********************************************************************************
-- Insert to the target fact table
************************************************************************************/
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.purchase_order_item_shiplocation_fact (purchase_order_num, rms_sku_num, ship_location_id,
 case_pack_ind, cancel_code, cancel_date, canceledby_id, close_date, distribution_description, distribution_id,
 distribution_method, distribution_status, estimatedin_stock_date, external_distribution_id, last_received_ind,
 non_scaling_ind, origin_code, original_replenish_qty, canceled_qty, ordered_qty, prescaled_qty, received_qty,
 allocated_notreleased_qty, release_date, earliest_ship_date, epm_sku_num, initial_unit_cost, latest_ship_date,
 item_nonscaling_ind, origin_country, reference_item, unit_retail_amt, supplier_packsize, unit_cost_amt,
 universal_product_code, universal_product_code_supplement, dw_batch_id, dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz,
 dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz)
(SELECT DISTINCT TRIM(purchaseorder_externalid) AS purchaseorder_externalid,
  TRIM(purchaseorder_items_externalitemid) AS purchaseorder_items_externalitemid,
  purchaseorder_items_shiplocations_id,
  purchaseorder_items_casepackindicator,
  purchaseorder_items_shiplocations_cancelcode,
  CAST(purchaseorder_items_shiplocations_canceldate AS DATE) AS purchaseorder_items_shiplocations_canceldate,
  purchaseorder_items_shiplocations_canceledbyid,
  CAST(purchaseorder_items_shiplocations_closedate AS DATE) AS purchaseorder_items_shiplocations_closedate,
  purchaseorder_items_shiplocations_distributiondescription,
  cast(trunc(cast(purchaseorder_items_shiplocations_distributionid as  FLOAT64)) as BIGINT) AS purchaseorder_items_shiplocations_distributionid,
  purchaseorder_items_shiplocations_distributionmethod,
  purchaseorder_items_shiplocations_distributionstatus,
  CAST(purchaseorder_items_shiplocations_estimatedinstockdate AS DATE) AS
  purchaseorder_items_shiplocations_estimatedinstockdate,
  cast(trunc(cast(purchaseorder_items_shiplocations_externaldistributionid as FLOAT64)) as BIGINT) AS
  purchaseorder_items_shiplocations_externaldistributionid,
  CAST(purchaseorder_items_shiplocations_lastreceived AS STRING) AS purchaseorder_items_shiplocations_lastreceived,
  purchaseorder_items_shiplocations_nonscalingindicator,
  purchaseorder_items_shiplocations_originindicator,
  purchaseorder_items_shiplocations_originalreplenishquantity,
  purchaseorder_items_shiplocations_quantitycanceled,
  purchaseorder_items_shiplocations_quantityordered,
  purchaseorder_items_shiplocations_quantityprescaled,
  purchaseorder_items_shiplocations_quantityreceived,
  purchaseorder_items_shiplocations_quantityreceivedallocatednotreleased,
  CAST(purchaseorder_items_shiplocations_releasedate AS DATE) AS purchaseorder_items_shiplocations_releasedate,
  CAST(purchaseorder_items_earliestshipdate AS DATE) AS purchaseorder_items_earliestshipdate,
  TRIM(SUBSTR(CAST(purchaseorder_items_itemid AS STRING), 1, 60)) AS purchaseorder_items_itemid,
  ROUND(CAST(purchaseorder_items_initialunitcost AS NUMERIC), 2) AS purchaseorder_items_initialunitcost,
  CAST(purchaseorder_items_latestshipdate AS DATE) AS purchaseorder_items_latestshipdate,
  purchaseorder_items_nonscalingindicator,
  purchaseorder_items_origincountry,
  purchaseorder_items_referenceitem,
  ROUND(CAST(purchaseorder_items_shiplocations_unitretail AS NUMERIC), 2) AS
  purchaseorder_items_shiplocations_unitretail,
  purchaseorder_items_supplierpacksize,
  ROUND(CAST(purchaseorder_items_unitcost AS NUMERIC), 2) AS purchaseorder_items_unitcost,
  purchaseorder_items_universalproductcode,
  purchaseorder_items_universalproductcodesupplement,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_PURCHASE_ORDER')) AS dw_batch_date,
  CAST(current_datetime('PST8PDT') AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_load_tmstp_tz,
  CAST(current_datetime('PST8PDT') AS TIMESTAMP) AS dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as dw_sys_updt_tmstp_tz,
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_item_shiplocation_ldg AS sdc
 WHERE purchaseorder_externalid IS NOT NULL
  AND purchaseorder_items_externalitemid IS NOT NULL
  AND purchaseorder_items_shiplocations_id IS NOT NULL
 QUALIFY (MAX(CAST(trunc(cast(metadata_revisionid as float64)) AS INT64)) OVER (PARTITION BY purchaseorder_externalid RANGE BETWEEN
    UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = CAST(trunc(cast(metadata_revisionid as float64)) AS INT64));



/***********************************************************************************
-- Insert The Timeliness Load Data
************************************************************************************/
CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT',  '{{params.dbenv}}_nap_fct',  'po_legacy_kafka_to_orc_td',  'ldg_to_fact',  4,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  'SCOI_PO_LEGACY');


/***********************************************************************************
-- Collect stats on fact tables
************************************************************************************/

--COLLECT STATISTICS ON {db_env}_NAP_FCT.PURCHASE_ORDER_ITEM_SHIPLOCATION_FACT COLUMN( purchase_order_num,rms_sku_num,ship_location_id);
