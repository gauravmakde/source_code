--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : jwn_inventory_sku_loc_day_fact_ld.sql
-- Author                  : Rishi Nair
-- Description             : Calculate and update boh and eoh quantities JWN_INVENTORY_SKU_LOC_DAY_FACT
-- Data Source             : INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT and JWN_INVENTORY_SKU_LOC_DAY_FACT
-- ETL Run Frequency       : Daily
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-04-22  Rishi Nair 			Clarity prototype scripts
-- 2023-04-27  Sergii Porfiriev     FA-8598: Code Refactor - for Ongoing Delta Load in Production
--*************************************************************************************************************************************




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact AS tgt
WHERE reporting_date = (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AND LOWER(inventory_source) = LOWER('LOGICAL');


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact AS tgt
USING (SELECT a.rms_sku_id AS rms_sku_num,
  CAST(TRUNC(CAST(a.location_id AS FLOAT64)) AS INTEGER) AS location_num,
  COALESCE(b.store_type_code, a.location_type) AS location_type,
  a.snapshot_date AS reporting_date,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
  MAX(b.channel_num) AS channel_num,
  MAX(b.channel_desc) AS channel_desc,
  MAX(b.store_country_code) AS store_country_code,
  MAX(COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0)) AS
  unit_price_amt,
  MAX(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code)) AS price_type_code,
  SUM(IFNULL(a.stock_on_hand_qty, 0)) AS eoh_qty,
  SUM(IFNULL(a.immediately_sellable_qty, 0)) AS physical_sellable_qty,
  SUM(IFNULL(a.unavailable_qty, 0)) AS unavailable_qty,
  SUM(IFNULL(a.damage_qty, 0)) AS damaged_qty,
  SUM(IFNULL(a.problem, 0)) AS problem_qty,
  SUM(IFNULL(a.damaged_return, 0)) AS damaged_return_qty,
  SUM(IFNULL(a.hold_qty, 0)) AS hold_qty,
  SUM(IFNULL(a.store_transfer_reserved_qty, 0) + IFNULL(a.back_order_reserve_qty, 0)) AS reserved_qty,
  SUM(IFNULL(a.in_transit_qty, 0)) AS in_transit_qty,
  'LOGICAL' AS inventory_source
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_stock_quantity_by_day_logical_fact AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS b ON CAST(a.location_id AS FLOAT64) = b.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON CAST(a.location_id AS FLOAT64) = psd.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pro ON psd.price_store_num = CAST(pro.store_num AS FLOAT64)
     AND LOWER(a.rms_sku_id) = LOWER(pro.rms_sku_num) AND a.snapshot_date >= pro.eff_begin_tmstp 
      AND a.snapshot_date < pro.eff_end_tmstp
 WHERE a.snapshot_date = (SELECT curr_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
 GROUP BY rms_sku_num,
  location_num,
  location_type,
  reporting_date,
  dw_batch_id,
  dw_batch_date
 HAVING eoh_qty <> 0 OR physical_sellable_qty <> 0 OR unavailable_qty <> 0 OR damaged_qty <> 0 OR problem_qty <> 0 OR
      damaged_return_qty <> 0 OR hold_qty <> 0 OR reserved_qty <> 0 OR in_transit_qty <> 0) AS t9
ON t9.location_num = tgt.location_num AND LOWER(t9.location_type) = LOWER(tgt.location_type) AND LOWER(t9.rms_sku_num) =
   LOWER(tgt.rms_sku_num) AND t9.reporting_date = tgt.reporting_date
WHEN MATCHED THEN UPDATE SET
 eoh_qty = t9.eoh_qty,
 unit_price_amt = t9.unit_price_amt,
 price_type_code = t9.price_type_code,
 physical_sellable_qty = t9.physical_sellable_qty,
 unavailable_qty = t9.unavailable_qty,
 damaged_qty = t9.damaged_qty,
 problem_qty = t9.problem_qty,
 damaged_return_qty = t9.damaged_return_qty,
 hold_qty = t9.hold_qty,
 reserved_qty = t9.reserved_qty,
 in_transit_qty = t9.in_transit_qty,
 inventory_source = t9.inventory_source,
 dw_batch_id = t9.dw_batch_id,
 dw_batch_date = t9.dw_batch_date,
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN INSERT (
  location_num, location_type, channel_num, channel_desc, store_country_code, rms_sku_num, reporting_date, unit_price_amt, price_type_code, eoh_qty, physical_sellable_qty, unavailable_qty, damaged_qty, problem_qty, damaged_return_qty, hold_qty, reserved_qty, in_transit_qty, inventory_source, dw_batch_id, dw_batch_date,dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz) VALUES(t9.location_num ,t9.location_type,t9.channel_num, t9.channel_desc,t9.store_country_code, t9.rms_sku_num, t9.reporting_date, t9.unit_price_amt, t9.price_type_code
 , t9.eoh_qty, t9.physical_sellable_qty, t9.unavailable_qty, t9.damaged_qty, t9.problem_qty, t9.damaged_return_qty, t9.hold_qty
 , t9.reserved_qty, t9.in_transit_qty, t9.inventory_source, t9.dw_batch_id , 
   t9.dw_batch_date
 ,CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST(), 
 CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
  );


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact AS tgt
USING (SELECT a.rms_sku_num,
  a.location_num,
  COALESCE(b.store_type_code, a.location_type) AS location_type,
  DATE_ADD(a.reporting_date, INTERVAL 1 DAY) AS reporting_date,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
  MAX(b.channel_num) AS channel_num,
  MAX(b.channel_desc) AS channel_desc,
  MAX(b.store_country_code) AS store_country_code,
  MAX(COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0)) AS
  unit_price_amt,
  MAX(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code)) AS price_type_code,
  SUM(IFNULL(a.eoh_qty, 0)) AS eoh_qty,
  'BOH' AS inventory_source
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS b ON a.location_num = b.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON a.location_num = psd.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pro ON psd.price_store_num = CAST(pro.store_num AS FLOAT64)
     AND LOWER(a.rms_sku_num) = LOWER(pro.rms_sku_num) 
     AND DATE_ADD(a.reporting_date, INTERVAL 1 DAY) >= pro.eff_begin_tmstp
      AND DATE_ADD(a.reporting_date, INTERVAL 1 DAY) < pro.eff_end_tmstp
 WHERE a.reporting_date = (SELECT DATE_SUB(curr_batch_date, INTERVAL 1 DAY)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
 GROUP BY a.rms_sku_num,
  a.location_num,
  location_type,
  reporting_date,
  dw_batch_id,
  dw_batch_date
 HAVING eoh_qty <> 0) AS t9
ON t9.location_num = tgt.location_num AND LOWER(t9.rms_sku_num) = LOWER(tgt.rms_sku_num) AND t9.reporting_date = tgt.reporting_date
  
WHEN MATCHED THEN UPDATE SET
 boh_qty = t9.eoh_qty,
 dw_batch_id = t9.dw_batch_id,
 dw_batch_date = t9.dw_batch_date,
 dw_sys_updt_tmstp = CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz = `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN INSERT (location_num, location_type, channel_num, channel_desc, store_country_code, rms_sku_num, reporting_date, unit_price_amt, price_type_code, eoh_qty, boh_qty, inventory_source, dw_batch_id, dw_batch_date, dw_sys_load_tmstp,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz) VALUES
 (t9.location_num, t9.location_type, t9.channel_num
 , t9.channel_desc, t9.store_country_code, t9.rms_sku_num,t9.reporting_date 
 , t9.unit_price_amt, t9.price_type_code, 0, t9.eoh_qty,t9.inventory_source , t9.dw_batch_id
 ,t9.dw_batch_date, CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP),
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
 ,CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) ,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST());


