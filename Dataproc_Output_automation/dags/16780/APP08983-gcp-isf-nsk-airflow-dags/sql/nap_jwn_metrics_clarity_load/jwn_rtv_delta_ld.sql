--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : jwn_rtv_delta_ld.sql
-- Author                  : Rishi Nair
-- Description             : Merge data from current batch of RMS14_RTV_FACT to JWN_INVENTORY_SKU_LOC_DAY_FACT
-- Data Source             : current batch  of RMS14_RTV_FACT table
-- ETL Run Frequency       : Daily
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-04-22  Rishi Nair 			Clarity prototype scripts
-- 2023-04-27  Sergii Porfiriev     FA-8598: Code Refactor - for Ongoing Delta Load in Production
-- 2023-08-29  Oleksandr Chaichenko FA-9891: rtv metrics sometimes double-counted
-- 2023-09-27  Oleksandr Chaichenko FA-10200: Code Refactor
--*************************************************************************************************************************************


CREATE OR REPLACE TEMPORARY TABLE  delta_all_rtv_skus
AS
SELECT rms_sku_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rtv_logical_fact
WHERE dw_batch_date = DATE_ADD((SELECT curr_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')), INTERVAL 1 DAY)
GROUP BY rms_sku_num;

-- delta  join for incremental load


CREATE OR REPLACE TEMPORARY TABLE  delta_rtv_full
AS
SELECT rms_sku_num,
 CAST(TRUNC(CAST(from_location_id AS FLOAT64)) AS INTEGER) AS location_num,
 ship_date AS reporting_date,
 SUM(rtv_qty) AS rtv_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rtv_logical_fact AS a
WHERE ship_date >= DATE '2022-10-30'
 AND EXISTS (SELECT 1
  FROM delta_all_rtv_skus AS delta
  WHERE LOWER(a.rms_sku_num) = LOWER(rms_sku_num))
 AND from_location_id IS NOT NULL
GROUP BY rms_sku_num,
 location_num,
 reporting_date;


/*delta for delete*/


CREATE OR REPLACE TEMPORARY TABLE  delta_rtv_4delete_wrk
AS
SELECT fct.rms_sku_num,
 fct.location_num,
 fct.reporting_date,
 fct.rtv_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_rtv_logical_fact AS fct
 INNER JOIN delta_all_rtv_skus AS w ON LOWER(fct.rms_sku_num) = LOWER(w.rms_sku_num)
 LEFT JOIN delta_rtv_full AS wrk ON LOWER(fct.rms_sku_num) = LOWER(wrk.rms_sku_num) AND fct.location_num = wrk.location_num
     AND fct.reporting_date = wrk.reporting_date
WHERE wrk.rms_sku_num IS NULL;


--CLEAN UP JWN_RTV_LOGICAL_FACT


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_rtv_logical_fact AS fct
WHERE EXISTS (SELECT 1
 FROM delta_rtv_4delete_wrk AS wrk
 WHERE LOWER(fct.rms_sku_num) = LOWER(rms_sku_num)
  AND fct.location_num = location_num
  AND fct.reporting_date = reporting_date);


--CLEAN UP JWN_INVENTORY_SKU_LOC_DAY_FACT


UPDATE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact SET
 rtv_qty = 0
WHERE rtv_qty <> 0 AND EXISTS (SELECT 1
  FROM delta_rtv_4delete_wrk AS wrk
  WHERE LOWER(jwn_inventory_sku_loc_day_fact.rms_sku_num) = LOWER(rms_sku_num)
   AND jwn_inventory_sku_loc_day_fact.location_num = location_num
   AND jwn_inventory_sku_loc_day_fact.reporting_date = reporting_date);


--drop VT


DROP TABLE IF EXISTS delta_all_rtv_skus;

DROP TABLE IF EXISTS delta_rtv_4delete_wrk;


-- CLEAN UP delta_full


DELETE FROM delta_rtv_full wrk
WHERE EXISTS (SELECT 1
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_rtv_logical_fact AS fct
 WHERE LOWER(wrk.rms_sku_num) = LOWER(rms_sku_num)
  AND wrk.location_num = location_num
  AND wrk.reporting_date = reporting_date
  AND wrk.rtv_qty = rtv_qty);


/*Merge data from current batch of RMS14_RTV_FACT to JWN_RTV_LOGICAL_FACT*/


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_rtv_logical_fact AS tgt
USING (SELECT rms_sku_num,
  location_num,
  reporting_date,
  rtv_qty,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_load_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp_tz,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_updt_tmstp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp_tz
 FROM delta_rtv_full AS wrk) AS SRC
ON SRC.location_num = tgt.location_num AND LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.reporting_date = tgt
  .reporting_date
WHEN MATCHED THEN UPDATE SET
 rtv_qty = SRC.rtv_qty,
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz =  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN INSERT
VALUES
(
 CAST(TRUNC(CAST(SRC.location_num AS FLOAT64)) AS STRING),
 CAST(TRUNC(CAST(SRC.rms_sku_num AS FLOAT64)) AS INTEGER), 
 SRC.reporting_date,
 SRC.rtv_qty, 
 SRC.dw_batch_id, 
 SRC.dw_batch_date, 
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
 dw_sys_load_tmstp_tz,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz
 );


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact AS tgt
USING (SELECT a.location_num,
  a.rms_sku_num,
  a.reporting_date,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
  MAX(b.store_type_code) AS location_type,
  MAX(b.channel_num) AS channel_num,
  MAX(b.channel_desc) AS channel_desc,
  MAX(b.store_country_code) AS store_country_code,
  MAX(COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0)) AS
  unit_price_amt,
  MAX(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code)) AS price_type_code,
  SUM(a.rtv_qty) AS rtv_qty,
  'RTV' AS inventory_source
 FROM delta_rtv_full AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS b ON a.location_num = b.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON a.location_num = psd.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pro ON psd.price_store_num = CAST(pro.store_num AS FLOAT64)
     AND LOWER(a.rms_sku_num) = LOWER(pro.rms_sku_num) AND a.reporting_date >= CAST(pro.eff_begin_tmstp AS DATE) AND a.reporting_date
     < CAST(pro.eff_end_tmstp AS DATE)
 GROUP BY a.location_num,
  a.rms_sku_num,
  a.reporting_date) AS SRC
ON SRC.location_num = tgt.location_num AND LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.reporting_date = tgt
  .reporting_date
WHEN MATCHED THEN UPDATE SET
 price_type_code = SRC.price_type_code,
 unit_price_amt = SRC.unit_price_amt,
 rtv_qty = SRC.rtv_qty,
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP),
 dw_sys_updt_tmstp_tz =  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()
WHEN NOT MATCHED THEN INSERT 
(rms_sku_num,
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
 reserved_qty) 
 VALUES(
CAST(SRC.location_num AS STRING),
CAST(TRUNC(CAST(SRC.location_type AS FLOAT64)) AS INTEGER), 
CAST(SRC.channel_num AS STRING),
CAST(SRC.channel_desc AS DATE), 
CAST(ROUND(CAST(SRC.store_country_code AS BIGNUMERIC), 0) AS NUMERIC),
SRC.rms_sku_num,
CAST(CAST(SRC.reporting_date AS STRING) AS INTEGER),
SRC.rtv_qty, 
CAST(TRUNC(CAST(SRC.price_type_code AS FLOAT64)) AS INTEGER), 
CAST(TRUNC(CAST(SRC.unit_price_amt AS FLOAT64)) AS INTEGER), 
CAST(TRUNC(CAST(SRC.inventory_source AS FLOAT64)) AS INTEGER),
SRC.dw_batch_id, 
CAST(CAST(SRC.dw_batch_date AS STRING) AS INTEGER), 
CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS STRING)AS INTEGER), 
CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS STRING) AS INTEGER)
);


