--*************************************************************************************************************************************
-- Script Details
--*************************************************************************************************************************************
-- File                    : jwn_purchase_order_rms_receipts_fact_ld.sql
-- Author                  : Rishi Nair
-- Description             : Insert data from current batch of RMS14_INBOUND_SHIPMENT_FACT and PURCHASE_ORDER_FACT and merg result to JWN_INVENTORY_SKU_LOC_DAY_FACT
-- Data Source             : current batch  of RMS14_RTV_FACT and PURCHASE_ORDER_FACT tables
-- ETL Run Frequency       : Daily
-- Reference Documentation :
--*************************************************************************************************************************************
-- Change Log: Date Author Description
--*************************************************************************************************************************************
-- 2023-04-22  Rishi Nair 			Clarity prototype scripts
-- 2023-04-27  Sergii Porfiriev     FA-8598: Code Refactor - for Ongoing Delta Load in Production
-- 2023-09-27  Oleksandr Chaichenko FA-10200: Code Refactor
-- 2023-12-07  Oleksandr Chaichenko FA-10018:Exclude negative qty from calculation last received date
--*************************************************************************************************************************************


/*Delta volatile table with all purchase orders affected as part of the current days batch*/
/*TRIM applied for purchase orders as some POs have a 0 appended before the PO number*/


CREATE TEMPORARY TABLE IF NOT EXISTS delta_all_receipts_skus AS
SELECT purchase_order_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.po_receipt_fact
WHERE dw_batch_date = DATE_ADD((SELECT curr_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
    WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')), INTERVAL 1 DAY)
GROUP BY purchase_order_num
UNION DISTINCT
SELECT a.purchase_order_num FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.po_receipt_fact AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_fact AS b 
 ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_num)
WHERE (LOWER(b.dropship_ind) <> LOWER('t') OR b.dropship_ind IS NULL)
 AND b.dw_batch_date = DATE_ADD((SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')), INTERVAL 1 DAY)

GROUP BY a.purchase_order_num;


CREATE TEMPORARY TABLE IF NOT EXISTS delta_receipts_full
AS SELECT a.purchase_order_num AS purchase_order_number,
  CASE WHEN LOWER(a.carton_id) = LOWER('null') THEN NULL WHEN LOWER(a.carton_id) = LOWER('UNKNOWN') THEN NULL ELSE a.carton_id END AS carton_num,
  CASE WHEN COALESCE(a.tofacility_id, 0) < 10000 THEN COALESCE(a.tofacility_id, 0) WHEN COALESCE(a.tofacility_id, 0) > 10000 AND COALESCE(a.tofacility_id, 0) < 100000 THEN COALESCE(a.tofacility_id - 10000, 0) ELSE COALESCE(a.tofacility_id - 100000, 0) END AS location_num,
 a.rms_sku_num,
 a.received_date AS reporting_date,
  CASE WHEN LOWER(a.tofacility_type) = LOWER('S') THEN 'Y' ELSE 'N' END AS store_inbound_receipt_flag, SUM(a.shipment_qty) AS received_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.po_receipt_fact AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.purchase_order_fact AS b ON LOWER(a.purchase_order_num) = LOWER(b.purchase_order_num)
 INNER JOIN delta_all_receipts_skus AS c ON LOWER(a.purchase_order_num) = LOWER(c.purchase_order_num)
WHERE LOWER(b.dropship_ind) <> LOWER('t')
 OR b.dropship_ind IS NULL
GROUP BY purchase_order_number,
 carton_num,
 location_num,
 a.rms_sku_num,
 reporting_date,
 store_inbound_receipt_flag;


CREATE TEMPORARY TABLE IF NOT EXISTS delta_receipts_4delete_wrk
AS
SELECT fct.purchase_order_num,
 fct.carton_num,
 fct.rms_sku_num,
 fct.location_num,
 fct.reporting_date,
 fct.store_inbound_receipt_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS fct
 INNER JOIN delta_all_receipts_skus AS w ON LOWER(fct.purchase_order_num) = LOWER(w.purchase_order_num)
 LEFT JOIN delta_receipts_full AS wrk ON LOWER(fct.purchase_order_num) = LOWER(wrk.purchase_order_number) AND LOWER(fct
        .carton_num) = LOWER(wrk.carton_num) AND LOWER(fct.rms_sku_num) = LOWER(wrk.rms_sku_num) AND fct.location_num =
     wrk.location_num AND fct.reporting_date = wrk.reporting_date AND LOWER(fct.store_inbound_receipt_flag) = LOWER(wrk
    .store_inbound_receipt_flag)
WHERE wrk.rms_sku_num IS NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS jwn_purchase_order_rms_receipts_fact_before
AS
SELECT a.location_num,
 a.rms_sku_num,
 a.reporting_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
 INNER JOIN delta_all_receipts_skus AS wrk ON LOWER(a.purchase_order_num) = LOWER(wrk.purchase_order_num)
WHERE a.reporting_date >= DATE '2022-10-30'
GROUP BY a.location_num,
 a.rms_sku_num,
 a.reporting_date;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_purchase_order_rms_receipts_fact AS fct
WHERE EXISTS (SELECT 1
 FROM delta_receipts_4delete_wrk AS wrk
 WHERE LOWER(fct.purchase_order_num) = LOWER(purchase_order_num)
  AND LOWER(fct.carton_num) = LOWER(carton_num)
  AND LOWER(fct.rms_sku_num) = LOWER(rms_sku_num)
  AND fct.location_num = location_num
  AND fct.reporting_date = reporting_date
  AND LOWER(fct.store_inbound_receipt_flag) = LOWER(store_inbound_receipt_flag));


DELETE FROM delta_receipts_full wrk
WHERE EXISTS (SELECT 1
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS fct
 WHERE LOWER(wrk.purchase_order_number) = LOWER(fct.purchase_order_num)
  AND LOWER(wrk.carton_num) = LOWER(fct.carton_num)
  AND LOWER(wrk.rms_sku_num) = LOWER(fct.rms_sku_num)
  AND wrk.location_num = fct.location_num
  AND wrk.reporting_date = fct.reporting_date
  AND LOWER(wrk.store_inbound_receipt_flag) = LOWER(fct.store_inbound_receipt_flag)
  AND wrk.received_qty = fct.received_qty);


DROP TABLE IF EXISTS delta_all_receipts_skus;


DROP TABLE IF EXISTS delta_receipts_4delete_wrk;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_purchase_order_rms_receipts_fact AS tgt
USING (
  SELECT 
  purchase_order_number,
  carton_num,
  location_num,
  rms_sku_num,
  reporting_date,
  store_inbound_receipt_flag,
  SUM(received_qty) AS received_qty,
   (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_id,
   (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
   current_datetime('PST8PDT') AS dw_sys_load_tmstp, 
  'PST8PDT' as   dw_sys_load_tmstp_tz,
   current_datetime('PST8PDT') AS dw_sys_updt_tmstp,
  'PST8PDT' as  dw_sys_updt_tmstp_tz
 FROM delta_receipts_full AS wrk
 GROUP BY purchase_order_number, carton_num, location_num, rms_sku_num, reporting_date, store_inbound_receipt_flag) AS SRC
ON LOWER(SRC.purchase_order_number) = LOWER(tgt.purchase_order_num) AND LOWER(SRC.carton_num) = LOWER(tgt.carton_num)
    AND SRC.location_num = tgt.location_num AND LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.reporting_date =
   tgt.reporting_date AND LOWER(SRC.store_inbound_receipt_flag) = LOWER(tgt.store_inbound_receipt_flag)
WHEN MATCHED THEN UPDATE SET
 location_num = SRC.location_num,
 reporting_date = SRC.reporting_date,
 store_inbound_receipt_flag = SRC.store_inbound_receipt_flag,
 received_qty = SRC.received_qty,
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp =  timestamp(current_datetime('PST8PDT')),
 dw_sys_updt_tmstp_tz = SRC.dw_sys_updt_tmstp_tz
WHEN NOT MATCHED THEN INSERT 
(
    purchase_order_num  , carton_num  , rms_sku_num  , location_num  , reporting_date  , store_inbound_receipt_flag  , received_qty  , dw_batch_id  , dw_batch_date  , dw_sys_load_tmstp  ,dw_sys_load_tmstp_tz, dw_sys_updt_tmstp,dw_sys_updt_tmstp_tz )
VALUES(
  SRC.purchase_order_number ,
  SRC.carton_num, 
  SRC.rms_sku_num , 
  SRC.location_num,
  SRC.reporting_date,
  SRC.store_inbound_receipt_flag,
	SRC.received_qty, 
  SRC.dw_batch_id,
  SRC.dw_batch_date,
  timestamp(current_datetime('PST8PDT')),
  dw_sys_load_tmstp_tz,
  timestamp(current_datetime('PST8PDT')),
  dw_sys_updt_tmstp_tz
  );


DROP TABLE IF EXISTS delta_receipts_full;


CREATE TEMPORARY TABLE IF NOT EXISTS delta_rms_wrk 
AS
SELECT rms_sku_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact
WHERE dw_batch_date = (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD'))
GROUP BY rms_sku_num;


CREATE TEMPORARY TABLE IF NOT EXISTS jwn_purchase_order_rms_receipts_fact_wrk
AS
SELECT a.location_num,
 a.rms_sku_num,
 a.reporting_date,
 SUM(a.received_qty) AS received_qty,
 SUM(CASE
   WHEN LOWER(a.store_inbound_receipt_flag) = LOWER('Y')
   THEN a.received_qty
   ELSE 0
   END) AS direct_to_store_receipt_qty
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_purchase_order_rms_receipts_fact AS a
 INNER JOIN delta_rms_wrk AS wrk ON LOWER(a.rms_sku_num) = LOWER(wrk.rms_sku_num)
WHERE a.reporting_date >= DATE '2022-10-30'
GROUP BY a.location_num,
 a.rms_sku_num,
 a.reporting_date;


--Cleanup: set all transfers qty and amt for delta to 0 to handle cases when dates has changed in the source
-- based on data in today's JWN_PURCHASE_ORDER_RMS_RECEIPTS_FACT

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact AS tgt
USING (
  SELECT b.rms_sku_num,
  b.location_num,
  b.reporting_date
 FROM jwn_purchase_order_rms_receipts_fact_before AS b
  INNER JOIN delta_rms_wrk AS wrk 
  ON LOWER(b.rms_sku_num) = LOWER(wrk.rms_sku_num)
  LEFT JOIN jwn_purchase_order_rms_receipts_fact_wrk AS w 
  ON b.reporting_date = w.reporting_date
   AND b.location_num = w.location_num 
   AND LOWER(b.rms_sku_num) = LOWER(w.rms_sku_num)
 WHERE 
 w.rms_sku_num IS NULL
 ) AS SRC
ON SRC.location_num = tgt.location_num 
AND LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND SRC.reporting_date = tgt.reporting_date
WHEN MATCHED THEN UPDATE SET
 received_qty = 0,
 direct_to_store_receipt_qty = 0,
 last_receipt_date = NULL;


DELETE 
FROM jwn_purchase_order_rms_receipts_fact_wrk wrk
WHERE 
EXISTS 
( SELECT 1
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_day_fact AS fct
 WHERE LOWER(fct.rms_sku_num) = LOWER(wrk.rms_sku_num)
  AND fct.location_num = wrk.location_num
  AND fct.reporting_date = wrk.reporting_date
  AND fct.received_qty = wrk.received_qty
  AND fct.direct_to_store_receipt_qty = wrk.direct_to_store_receipt_qty
  );


/*MErge the changed and inserted receipts into the JWN inventory table
*/
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_fct.jwn_inventory_sku_loc_day_fact AS tgt
USING (
  SELECT 
  a.location_num,
  a.rms_sku_num,
  a.reporting_date,
   (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_id,
   (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_ASCP_CLARITY_LOAD')) AS dw_batch_date,
  MAX(b.store_type_code) AS location_type,
  MAX(b.channel_num) AS channel_num,
  MAX(b.channel_desc) AS channel_desc,
  MAX(b.store_country_code) AS store_country_code,
  MAX(COALESCE(pro.ownership_retail_price_amt, pro.selling_retail_price_amt, pro.regular_price_amt, 0)) AS
  unit_price_amt,
  MAX(COALESCE(pro.ownership_retail_price_type_code, pro.selling_retail_price_type_code)) AS price_type_code,
  SUM(a.received_qty) AS received_qty,
  SUM(a.direct_to_store_receipt_qty) AS direct_to_store_receipt_qty,
  SUM(CASE
    WHEN a.received_qty > 0
    THEN 1
    ELSE 0
    END) AS positive_qty_ind,
   CASE
   WHEN SUM(a.received_qty) > 0 OR SUM(a.received_qty) < 0 AND SUM(CASE
        WHEN a.received_qty > 0
        THEN 1
        ELSE 0
        END) > 0
   THEN a.reporting_date
   ELSE NULL
   END AS last_receipt_date,
  'RECEIPTS' AS inventory_source
 FROM 
 jwn_purchase_order_rms_receipts_fact_wrk AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS b 
  ON a.location_num = b.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd 
  ON a.location_num = psd.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS pro 
  ON psd.price_store_num = CAST(pro.store_num AS FLOAT64)
     AND LOWER(a.rms_sku_num) = LOWER(pro.rms_sku_num) 
     AND a.reporting_date >= CAST(pro.eff_begin_tmstp_utc AS DATE) 
     AND a.reporting_date < CAST(pro.eff_end_tmstp_utc AS DATE)
 GROUP BY a.location_num,a.rms_sku_num,a.reporting_date
  --HAVING SUM(a.received_qty) <> 0
  ) AS SRC
ON SRC.location_num = tgt.location_num 
AND LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND SRC.reporting_date = tgt.reporting_date
WHEN MATCHED THEN UPDATE SET
 price_type_code = SRC.price_type_code,
 unit_price_amt = SRC.unit_price_amt,
 received_qty = SRC.received_qty,
 direct_to_store_receipt_qty = SRC.direct_to_store_receipt_qty,
 last_receipt_date = COALESCE(SRC.last_receipt_date, tgt.last_receipt_date),
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp =  timestamp(current_datetime('PST8PDT')),
 dw_sys_updt_tmstp_tz ='PST8PDT' 
WHEN NOT MATCHED THEN INSERT (
  location_num, location_type,channel_num   ,channel_desc ,store_country_code ,rms_sku_num  ,reporting_date  ,last_receipt_date  ,received_qty               ,direct_to_store_receipt_qty,price_type_code            ,unit_price_amt             ,inventory_source  ,dw_batch_id                ,dw_batch_date              ,dw_sys_load_tmstp          ,dw_sys_load_tmstp_tz,dw_sys_updt_tmstp ,dw_sys_updt_tmstp_tz) 
 VALUES(SRC.location_num
,SRC.location_type
,SRC.channel_num
,SRC.channel_desc
,SRC.store_country_code
,SRC.rms_sku_num
,SRC.reporting_date
,SRC.last_receipt_date
,SRC.received_qty
,SRC.direct_to_store_receipt_qty
,SRC.price_type_code
,SRC.unit_price_amt
,SRC.inventory_source	
,SRC.dw_batch_id
,SRC.dw_batch_date
,timestamp(current_datetime('PST8PDT')),
'PST8PDT',
timestamp(current_datetime('PST8PDT')),
'PST8PDT');