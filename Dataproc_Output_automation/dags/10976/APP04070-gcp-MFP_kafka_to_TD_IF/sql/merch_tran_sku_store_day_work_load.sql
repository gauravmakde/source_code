BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_gds_ongoing_load;
---Task_Name=gds_tran_work_load;'*/
---FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_tran_sku_store_day_wrk;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_tran_sku_store_day_wrk (rms_sku_num, day_date, store_num, receipt_units,
 receipt_retail_amt, receipt_cost_amt, receipt_po_units, receipt_po_retail_amt, receipt_po_cost_amt,
 receipt_dropship_units, receipt_dropship_retail_amt, receipt_dropship_cost_amt)
(SELECT rms_sku_num,
  tran_date AS day_date,
  store_num,
  SUM(receipts_crossdock_units) AS receipt_units,
  SUM(receipts_crossdock_retail) AS receipt_retail_amt,
  SUM(receipts_crossdock_cost) AS receipt_cost_amt,
  SUM(receipts_units) AS receipt_po_units,
  SUM(receipts_retail) AS receipt_po_retail_amt,
  SUM(receipts_cost) AS receipt_po_cost_amt,
  SUM(CASE
    WHEN LOWER(dropship_ind) = LOWER('Y')
    THEN receipts_units
    ELSE 0
    END) AS receipt_dropship_units,
  CAST(SUM(CASE
     WHEN LOWER(dropship_ind) = LOWER('Y')
     THEN receipts_retail
     ELSE 0
     END) AS NUMERIC) AS receipt_dropship_retail_amt,
  CAST(SUM(CASE
     WHEN LOWER(dropship_ind) = LOWER('Y')
     THEN receipts_cost
     ELSE 0
     END) AS NUMERIC) AS receipt_dropship_cost_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_poreceipt_sku_store_fact AS r01
 WHERE tran_date BETWEEN (SELECT start_rebuild_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
    WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY')) AND (SELECT end_rebuild_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
    WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY'))
 GROUP BY rms_sku_num,
  day_date,
  store_num);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_tran_sku_store_day_wrk AS tgt 
USING
    (
       SELECT tsf.rms_sku_num,
 tsf.tran_date AS day_date,
 tsf.store_num,
 SUM(CASE
   WHEN ph_config_from.config_value IS NOT NULL AND ph_config_to.config_value IS NULL
   THEN tsf.units
   ELSE 0
   END) AS receipt_pah_units,
 SUM(CASE
   WHEN ph_config_from.config_value IS NOT NULL AND ph_config_to.config_value IS NULL
   THEN tsf.total_retail
   ELSE 0
   END) AS receipt_pah_retail_amt,
 SUM(CASE
   WHEN ph_config_from.config_value IS NOT NULL AND ph_config_to.config_value IS NULL
   THEN tsf.total_cost
   ELSE 0
   END) AS receipt_pah_cost_amt,
 SUM(CASE
   WHEN rs_config_from.config_value IS NOT NULL AND rs_config_to.config_value IS NULL
   THEN tsf.units
   ELSE 0
   END) AS receipt_reservestock_units,
 SUM(CASE
   WHEN rs_config_from.config_value IS NOT NULL AND rs_config_to.config_value IS NULL
   THEN tsf.total_retail
   ELSE 0
   END) AS receipt_reservestock_retail_amt,
 SUM(CASE
   WHEN rs_config_from.config_value IS NOT NULL AND rs_config_to.config_value IS NULL
   THEN tsf.total_cost
   ELSE 0
   END) AS receipt_reservestock_cost_amt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_transfer_day_fact AS tsf
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS orgstore_from ON tsf.from_loc = orgstore_from.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS orgstore_to ON tsf.to_loc = orgstore_to.store_num
 LEFT JOIN (SELECT CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INTEGER) AS config_value
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
  WHERE LOWER(interface_code) = LOWER('MERCH_NAP_TRNSFR_DLY')
   AND LOWER(config_key) = LOWER('RS_CHANNELS')) AS rs_config_from ON orgstore_from.channel_num = rs_config_from.config_value
  
 LEFT JOIN (SELECT CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INTEGER) AS config_value
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
  WHERE LOWER(interface_code) = LOWER('MERCH_NAP_TRNSFR_DLY')
   AND LOWER(config_key) = LOWER('RS_CHANNELS')) AS rs_config_to ON orgstore_to.channel_num = rs_config_to.config_value
 LEFT JOIN (SELECT CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INTEGER) AS config_value
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
  WHERE LOWER(interface_code) = LOWER('MERCH_NAP_TRNSFR_DLY')
   AND LOWER(config_key) = LOWER('PH_CHANNELS')) AS ph_config_from ON orgstore_from.channel_num = ph_config_from.config_value
  
 LEFT JOIN (SELECT CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INTEGER) AS config_value
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
  WHERE LOWER(interface_code) = LOWER('MERCH_NAP_TRNSFR_DLY')
   AND LOWER(config_key) = LOWER('PH_CHANNELS')) AS ph_config_to ON orgstore_to.channel_num = ph_config_to.config_value
WHERE LOWER(tsf.event_type) = LOWER('TRANSFER_SHIP_TO_LOCATION_LEDGER_POSTED')
 AND tsf.tran_date BETWEEN (SELECT start_rebuild_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
   WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY')) AND (SELECT end_rebuild_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
   WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY'))
GROUP BY tsf.rms_sku_num,
 day_date,
 tsf.store_num
    ) src
ON LOWER(src.rms_sku_num)  = LOWER(tgt.rms_sku_num)
	AND src.day_date  = tgt.day_date
	AND src.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE 
SET receipt_pah_units = src.receipt_pah_units ,
	receipt_pah_retail_amt = src.receipt_pah_retail_amt ,
	receipt_pah_cost_amt = src.receipt_pah_cost_amt ,
	receipt_reservestock_units = src.receipt_reservestock_units ,
	receipt_reservestock_retail_amt = src.receipt_reservestock_retail_amt ,
	receipt_reservestock_cost_amt = src.receipt_reservestock_cost_amt ,	
	dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN 
INSERT (rms_sku_num,day_date,store_num,receipt_pah_units,receipt_pah_retail_amt,receipt_pah_cost_amt,receipt_reservestock_units,receipt_reservestock_retail_amt,receipt_reservestock_cost_amt,dw_sys_load_tmstp,dw_sys_updt_tmstp)
VALUES
(
	src.rms_sku_num ,
	src.day_date ,
	src.store_num ,
	src.receipt_pah_units ,
	src.receipt_pah_retail_amt ,
	src.receipt_pah_cost_amt ,
	src.receipt_reservestock_units ,
	src.receipt_reservestock_retail_amt ,
	src.receipt_reservestock_cost_amt ,
	CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
	CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) 
);


-- MERGE DEMAND
MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_tran_sku_store_day_wrk AS tgt 
USING 
	(SELECT rms_sku_num,
 demand_date_pacific AS day_date,
 store_num,
 SUM(demand_qty) AS reported_demand_units,
 SUM(demand_amt) AS reported_demand_retail_amt,
 SUM(shipped_qty) AS fulfilled_demand_units,
 SUM(shipped_amt) AS fulfilled_demand_retail_amt,
 SUM(canceled_total_qty) AS demand_canceled_units,
 SUM(canceled_total_amt) AS demand_canceled_retail_amt,
 SUM(CASE
   WHEN LOWER(fulfill_type_code) IN (LOWER('DS'))
   THEN demand_qty
   ELSE 0
   END) AS demand_dropship_units,
 SUM(CASE
   WHEN LOWER(fulfill_type_code) IN (LOWER('DS'))
   THEN demand_amt
   ELSE 0
   END) AS demand_dropship_retail_amt,
 SUM(CASE
   WHEN LOWER(fulfill_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'))
   THEN demand_qty
   ELSE 0
   END) AS store_fulfill_units,
 SUM(CASE
   WHEN LOWER(fulfill_type_code) IN (LOWER('FL'), LOWER('RK'), LOWER('SS'))
   THEN demand_amt
   ELSE 0
   END) AS store_fulfill_retail_amt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.demand_sku_store_fact AS d01
WHERE demand_date_pacific BETWEEN (SELECT start_rebuild_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
   WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY')) AND (SELECT end_rebuild_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
   WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY'))
GROUP BY rms_sku_num,
 day_date,
 store_num
) src
ON LOWER(src.rms_sku_num)  = LOWER(tgt.rms_sku_num)
	AND src.day_date  = tgt.day_date
	AND src.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE 
SET reported_demand_units = src.reported_demand_units ,
	reported_demand_retail_amt = src.reported_demand_retail_amt ,
	fulfilled_demand_units = src.fulfilled_demand_units ,
	fulfilled_demand_retail_amt = src.fulfilled_demand_retail_amt ,
	demand_canceled_units = src.demand_canceled_units ,
	demand_canceled_retail_amt = src.demand_canceled_retail_amt ,
	demand_dropship_units = src.demand_dropship_units,
	demand_dropship_retail_amt = src.demand_dropship_retail_amt,
	store_fulfill_units = src.store_fulfill_units,
	store_fulfill_retail_amt = src.store_fulfill_retail_amt,	
	dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN 
INSERT(rms_sku_num,day_date,store_num,reported_demand_units,reported_demand_retail_amt,fulfilled_demand_units,fulfilled_demand_retail_amt,demand_canceled_units,demand_canceled_retail_amt,demand_dropship_units,demand_dropship_retail_amt,store_fulfill_units,store_fulfill_retail_amt,dw_sys_load_tmstp,dw_sys_updt_tmstp)
VALUES
(
	src.rms_sku_num ,
	src.day_date ,
	src.store_num ,
	src.reported_demand_units ,
	src.reported_demand_retail_amt ,
	src.fulfilled_demand_units ,
	src.fulfilled_demand_retail_amt ,
	src.demand_canceled_units ,
	src.demand_canceled_retail_amt ,
	src.demand_dropship_units,
	src.demand_dropship_retail_amt,
	src.store_fulfill_units,
	src.store_fulfill_retail_amt,
	CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),
	CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
);


BEGIN
SET _ERROR_CODE  =  0;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_tran_sku_store_day_wrk AS tgt
USING (SELECT w01.rms_sku_num, w01.day_date, w01.store_num, prc.selling_retail_price_amt, prc.regular_price_amt, prc.ownership_retail_price_amt, prc.compare_at_retail_price_amt, 
    COALESCE(prc.ownership_retail_price_amt, prc.selling_retail_price_amt, 0) AS inv_retail_price_amt, 
    CASE WHEN LOWER(COALESCE(prc.ownership_retail_price_type_code, prc.selling_retail_price_type_code, 'REGULAR')) = LOWER('CLEARANCE') 
    THEN 'C' ELSE 'R' END AS price_type
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_sku_store_day_wrk AS w01
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 ON w01.store_num = s01.store_num
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON w01.store_num = psd.store_num
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS prc 
        ON LOWER(prc.rms_sku_num) = LOWER(w01.rms_sku_num)
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt ON LOWER(dt.interface_code) = LOWER('MERCH_GDS_TRAN_DLY')
	    AND LOWER(prc.channel_brand) = LOWER(PSD.channel_brand)
	    AND LOWER(prc.channel_country) = LOWER(PSD.channel_country)
	    AND LOWER(prc.selling_channel) = LOWER(PSD.selling_channel)
	    AND RANGE_CONTAINS(RANGE(prc.eff_begin_tmstp_utc ,prc.eff_end_tmstp_utc) ,
        (TIMESTAMP_SUB(CAST(DATE_ADD(w01.day_date,INTERVAL 1 DAY) AS TIMESTAMP), INTERVAL CAST(TRUNC(CAST(0.001 AS FLOAT64)) AS INT64) SECOND)))
	    AND prc.eff_end_tmstp_utc >= CAST(dt.start_rebuild_date AS TIMESTAMP) 
) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND SRC.day_date = tgt.day_date 
AND SRC.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE SET
    selling_retail_price_amt = SRC.selling_retail_price_amt,
    ownership_retail_price_amt = SRC.ownership_retail_price_amt,
    compare_at_retail_price_amt = SRC.compare_at_retail_price_amt,
    regular_price_amt = SRC.regular_price_amt;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_tran_sku_store_day_wrk AS tgt
USING (SELECT rms_sku_num, day_date, store_num, price_type, sales_units, sales_retail_amt, sales_cost_amt, return_units, return_retail_amt, return_cost_amt, selling_retail_price_amt, ownership_retail_price_amt, regular_price_amt, compare_at_retail_price_amt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sale_sku_store_day_wrk) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.day_date = tgt.day_date AND LOWER(SRC.price_type) = LOWER(tgt.price_type) AND SRC.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE SET
    sales_units = SRC.sales_units,
    sales_retail_amt = SRC.sales_retail_amt,
    sales_cost_amt = SRC.sales_cost_amt,
    return_units = SRC.return_units,
    return_retail_amt = SRC.return_retail_amt,
    return_cost_amt = SRC.return_cost_amt,
    selling_retail_price_amt = SRC.selling_retail_price_amt,
    ownership_retail_price_amt = SRC.ownership_retail_price_amt,
    regular_price_amt = SRC.regular_price_amt,
    compare_at_retail_price_amt = SRC.compare_at_retail_price_amt,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT (rms_sku_num,day_date,store_num,price_type,sales_units,sales_retail_amt,sales_cost_amt,return_units,return_retail_amt,return_cost_amt,selling_retail_price_amt,ownership_retail_price_amt,regular_price_amt,compare_at_retail_price_amt,dw_sys_load_tmstp,dw_sys_updt_tmstp) 
VALUES(SRC.rms_sku_num, SRC.day_date, SRC.store_num, SRC.price_type, SRC.sales_units, SRC.sales_retail_amt, SRC.sales_cost_amt, SRC.return_units, SRC.return_retail_amt, SRC.return_cost_amt, SRC.selling_retail_price_amt, SRC.ownership_retail_price_amt, SRC.regular_price_amt, SRC.compare_at_retail_price_amt, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) , CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) );

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


--COLLECT STATS ON PRD_NAP_STG.MERCH_TRAN_SKU_STORE_DAY_WRK;


BEGIN
SET _ERROR_CODE  =  0;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_tran_sku_store_day_wrk AS tgt
USING (SELECT w01.rms_sku_num, w01.day_date, w01.store_num, w01.price_type, 
        COALESCE(wacd.weighted_average_cost, CAST(wacc.weighted_average_cost_currency_code AS NUMERIC), 0) AS weighted_average_cost_amt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_sku_store_day_wrk AS w01
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 ON w01.store_num = s01.store_num
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
        ON LOWER(dt.interface_code) = LOWER('MERCH_GDS_TRAN_DLY')
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim AS wacd
			ON wacd.sku_num = w01.rms_sku_num
				AND wacd.location_num = CAST(w01.store_num AS STRING) 
				AND RANGE_CONTAINS(RANGE(wacd.eff_begin_dt, wacd.eff_end_dt) , w01.day_date)
				AND wacd.eff_end_dt >= dt.start_rebuild_date 
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim AS wacc 
			ON LOWER(wacc.sku_num) = LOWER(w01.rms_sku_num)
				AND wacc.channel_num =  s01.channel_num
				AND RANGE_CONTAINS(RANGE(wacc.eff_begin_dt, wacc.eff_end_dt) , w01.day_date)
				AND wacd.eff_end_dt >= dt.start_rebuild_date 
        
                     ) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND SRC.day_date = tgt.day_date 
AND SRC.store_num = tgt.store_num 
AND LOWER(SRC.price_type) = LOWER(tgt.price_type)
WHEN MATCHED THEN UPDATE SET
    weighted_average_cost_amt = CAST(ROUND(CAST(SRC.weighted_average_cost_amt AS BIGNUMERIC), 0) AS NUMERIC);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
END;
