BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_gds_ongoing_load;
---Task_Name=gds_inv_eoh_load;'*/
---FOR SESSION VOLATILE;

BEGIN
SET _ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk (rms_sku_num, day_date, store_num, eoh_units, in_transit_units)
(SELECT rms_sku_num,
  snapshot_date AS day_date,
  store_num,
  COALESCE(stock_on_hand_qty, 0) AS eoh_units,
  COALESCE(in_transit_qty, 0) AS in_transit_units
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inventory_sku_store_day_fact AS i

 WHERE i.snapshot_date BETWEEN (select dw_batch_dt-(select CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INTEGER) from  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup  
		WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY') AND LOWER(config_key) = LOWER('REBUILD_DAYS'))
 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl 
where 
LOWER(interface_code)=LOWER('MERCH_GDS_INV_DLY')) 
and   (select dw_batch_dt  from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl 
where 
LOWER(interface_code)=LOWER('MERCH_GDS_INV_DLY')))
;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;



BEGIN
SET _ERROR_CODE  =  0;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk AS tgt
USING (SELECT w01.rms_sku_num, w01.day_date, w01.store_num, COALESCE(wacd.weighted_average_cost, wacc.weighted_average_cost, 0) AS weighted_average_cost_amt,
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inv_sku_store_day_wrk AS w01
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 
        ON w01.store_num = s01.store_num
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim AS wacd 
        ON LOWER(wacd.sku_num) = LOWER(w01.rms_sku_num) 
        AND LOWER(wacd.location_num) = LOWER(TRIM(FORMAT('%11d', w01.store_num)))
        				AND RANGE_CONTAINS(RANGE(wacd.eff_begin_dt, wacd.eff_end_dt), w01.day_date)
				 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim AS wacc 
        ON LOWER(wacc.sku_num) = LOWER(w01.rms_sku_num) 
        AND s01.channel_num = wacc.channel_num 
        AND RANGE_CONTAINS(RANGE(wacc.eff_begin_dt, wacc.eff_end_dt), w01.day_date)
  where wacd.eff_end_dt >= (select max(dw_batch_dt-(select CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INTEGER) from  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup  
		WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY') AND LOWER(config_key) = LOWER('REBUILD_DAYS')))
 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl 
where 
LOWER(interface_code)=LOWER('MERCH_GDS_INV_DLY'))
AND WACD.EFF_END_DT >= (select max(dw_batch_dt-(select CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INTEGER) from  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup  
		WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY') AND LOWER(config_key) = LOWER('REBUILD_DAYS')))
 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl 
 where LOWER(interface_code)=LOWER('MERCH_GDS_INV_DLY'))



) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND SRC.day_date = tgt.day_date 
AND SRC.store_num = tgt.store_num
		
WHEN MATCHED THEN UPDATE SET
    weighted_average_cost_amt = SRC.weighted_average_cost_amt,
    eoh_cost_amt = tgt.eoh_units * SRC.weighted_average_cost_amt,
    in_transit_cost_amt = tgt.in_transit_units * SRC.weighted_average_cost_amt;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk AS tgt
USING (SELECT w01.rms_sku_num, w01.day_date, w01.store_num, prc.selling_retail_price_amt, prc.regular_price_amt, prc.ownership_retail_price_amt, prc.compare_at_retail_price_amt, COALESCE(prc.ownership_retail_price_amt, prc.selling_retail_price_amt, 0) AS inv_retail_price_amt, CASE WHEN LOWER(COALESCE(prc.ownership_retail_price_type_code, prc.selling_retail_price_type_code, 'REGULAR')) = LOWER('CLEARANCE') THEN 'C' ELSE 'R' END AS price_type
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inv_sku_store_day_wrk AS w01
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 ON w01.store_num = s01.store_num
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd ON w01.store_num = psd.store_num
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS prc 
        ON LOWER(prc.rms_sku_num) = LOWER(w01.rms_sku_num) 
        AND LOWER(prc.channel_brand) = LOWER(psd.channel_brand) 
        AND LOWER(prc.channel_country) = LOWER(psd.channel_country) 
        AND LOWER(prc.selling_channel) = LOWER(psd.selling_channel)
        AND RANGE_CONTAINS(RANGE(prc.eff_begin_tmstp_utc,prc.eff_end_tmstp_utc), (CAST(w01.DAY_DATE + 1 AS TIMESTAMP) - INTERVAL '0.001' SECOND))
        WHERE CAST(prc.eff_end_tmstp_utc AS DATE) >= (select dw_batch_dt-(select CAST(TRUNC(CAST(config_value AS FLOAT64)) AS INTEGER) from`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup  
		WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY') AND LOWER(config_key) = LOWER('REBUILD_DAYS'))
 from `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl 
where LOWER(interface_code)=LOWER('MERCH_GDS_INV_DLY'))
) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.day_date = tgt.day_date AND SRC.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE SET
    selling_retail_price_amt = SRC.selling_retail_price_amt,
    ownership_retail_price_amt = SRC.ownership_retail_price_amt,
    compare_at_retail_price_amt = SRC.compare_at_retail_price_amt,
    regular_price_amt = SRC.regular_price_amt,
    price_type = SRC.price_type,
    eoh_retail_amt = COALESCE(tgt.eoh_units * SRC.inv_retail_price_amt, 0),
    in_transit_retail_amt = COALESCE(tgt.in_transit_units * SRC.inv_retail_price_amt, 0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_inv_tran_sku_store_day_fact AS tgt
USING (SELECT rms_sku_num, day_date, store_num, MAX(selling_retail_price_amt) AS selling_retail_price_amt, MAX(ownership_retail_price_amt) AS ownership_retail_price_amt, MAX(regular_price_amt) AS regular_price_amt, MAX(compare_at_retail_price_amt) AS compare_at_retail_price_amt, MAX(weighted_average_cost_amt) AS weighted_average_cost_amt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inv_sku_store_day_wrk AS w01
    GROUP BY rms_sku_num, day_date, store_num) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.day_date = tgt.day_date AND SRC.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE SET
    eoh_units = 0,
    eoh_retail_amt = 0,
    eoh_cost_amt = 0,
    boh_units = 0,
    boh_retail_amt = 0,
    boh_cost_amt = 0,
    in_transit_units = 0,
    in_transit_retail_amt = 0,
    in_transit_cost_amt = 0,
    selling_retail_price_amt = SRC.selling_retail_price_amt,
    ownership_retail_price_amt = SRC.ownership_retail_price_amt,
    regular_price_amt = SRC.regular_price_amt,
    compare_at_retail_price_amt = SRC.compare_at_retail_price_amt,
    weighted_average_cost_amt = SRC.weighted_average_cost_amt,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_inv_tran_sku_store_day_fact AS tgt
USING (SELECT rms_sku_num, day_date, price_type, store_num, eoh_units, eoh_retail_amt, eoh_cost_amt, in_transit_units, in_transit_retail_amt, in_transit_cost_amt, selling_retail_price_amt, ownership_retail_price_amt, regular_price_amt, compare_at_retail_price_amt, weighted_average_cost_amt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inv_sku_store_day_wrk AS w01) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.day_date = tgt.day_date AND LOWER(SRC.price_type) = LOWER(tgt.price_type) AND SRC.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE SET
    eoh_units = SRC.eoh_units,
    eoh_retail_amt = SRC.eoh_retail_amt,
    eoh_cost_amt = SRC.eoh_cost_amt,
    boh_units = 0,
    boh_retail_amt = 0,
    boh_cost_amt = 0,
    in_transit_units = SRC.in_transit_units,
    in_transit_retail_amt = SRC.in_transit_retail_amt,
    in_transit_cost_amt = SRC.in_transit_cost_amt,
    selling_retail_price_amt = SRC.selling_retail_price_amt,
    ownership_retail_price_amt = SRC.ownership_retail_price_amt,
    regular_price_amt = SRC.regular_price_amt,
    compare_at_retail_price_amt = SRC.compare_at_retail_price_amt,
    weighted_average_cost_amt = SRC.weighted_average_cost_amt,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT (rms_sku_num,day_date,price_type,store_num,eoh_units,eoh_retail_amt,eoh_cost_amt,boh_units,boh_retail_amt,boh_cost_amt,in_transit_units,in_transit_retail_amt,in_transit_cost_amt,selling_retail_price_amt,ownership_retail_price_amt,regular_price_amt,compare_at_retail_price_amt,weighted_average_cost_amt,dw_sys_load_tmstp,dw_sys_updt_tmstp) VALUES(SRC.rms_sku_num, SRC.day_date, SRC.price_type, SRC.store_num, SRC.eoh_units, SRC.eoh_retail_amt, SRC.eoh_cost_amt, 0, 0, 0, SRC.in_transit_units, SRC.in_transit_retail_amt, CAST(TRUNC(SRC.in_transit_cost_amt) AS INT64), SRC.selling_retail_price_amt, CAST(TRUNC(SRC.ownership_retail_price_amt) AS INT64), SRC.regular_price_amt, CAST(TRUNC(SRC.compare_at_retail_price_amt) AS INT64), SRC.weighted_average_cost_amt, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) , CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;



/*SET QUERY_BAND = NONE FOR SESSION;*/


END;
