
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk (rms_sku_num, day_date, price_type, store_num, boh_units, boh_retail_amt, boh_cost_amt)
(SELECT rms_sku_num,
  DATE_ADD(day_date, INTERVAL 1 DAY),
  price_type,
  store_num,
  eoh_units AS boh_units,
  eoh_retail_amt AS boh_retail_amt,
  eoh_cost_amt AS boh_cost_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inv_tran_sku_store_day_fact AS f01
 WHERE day_date BETWEEN (SELECT DATE_SUB(dw_batch_dt, INTERVAL (SELECT CAST(trunc(cast(config_value as float64)) AS INTEGER) + 1
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
       WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY')
        AND LOWER(config_key) = LOWER('REBUILD_DAYS')) DAY)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl
    WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY')) 
AND (SELECT DATE_SUB(dw_batch_dt, INTERVAL 1 DAY)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl
    WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY'))
  AND eoh_units <> 0);




MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk TGT
USING(
SELECT w01.rms_sku_num,
 w01.day_date,
 w01.store_num,
 COALESCE(WACD.WEIGHTED_AVERAGE_COST ,WACC.WEIGHTED_AVERAGE_COST,0 ) AS WEIGHTED_AVERAGE_COST_AMT
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inv_sku_store_day_wrk AS w01
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 
 ON w01.store_num = s01.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim AS wacd 
 ON LOWER(wacd.sku_num) = LOWER(w01.rms_sku_num)
 AND WACD.LOCATION_NUM = TRIM(CAST(w01.STORE_NUM AS STRING) )
 AND RANGE_CONTAINS(RANGE(WACD.EFF_BEGIN_DT, WACD.EFF_END_DT), w01.DAY_DATE)

LEFT OUTER JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_channel_dim  WACC
ON lower(WACC.SKU_NUM) = lower(w01.RMS_SKU_NUM)
AND WACC.CHANNEL_NUM =s01.CHANNEL_NUM
AND  RANGE_CONTAINS(RANGE(WACC.EFF_BEGIN_DT, WACC.EFF_END_DT),w01.DAY_DATE)
WHERE WACD.EFF_END_DT >= (SELECT DATE_SUB(dw_batch_dt, INTERVAL (SELECT CAST(trunc(cast(config_value as float64)) AS INTEGER) AS config_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
   WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY')
    AND LOWER(config_key) = LOWER('REBUILD_DAYS')) DAY)
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl
WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY'))

) SRC
ON LOWER(SRC.RMS_SKU_NUM )= LOWER(TGT.RMS_SKU_NUM)
AND SRC.DAY_DATE = TGT.DAY_DATE
AND SRC.STORE_NUM=TGT.STORE_NUM
WHEN MATCHED THEN UPDATE 
SET WEIGHTED_AVERAGE_COST_AMT = SRC.WEIGHTED_AVERAGE_COST_AMT;




MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk TGT
USING(
SELECT w01.rms_sku_num,
 w01.day_date,
 w01.store_num,
 prc.selling_retail_price_amt,
 prc.regular_price_amt,
 prc.ownership_retail_price_amt,
 prc.compare_at_retail_price_amt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inv_sku_store_day_wrk AS w01
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 
 ON w01.store_num = s01.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd 
 ON w01.store_num = psd.store_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS prc 
 ON LOWER(prc.rms_sku_num) = LOWER(w01.rms_sku_num) 
 AND LOWER(prc.channel_brand) = LOWER(psd.channel_brand) 
AND LOWER(prc.channel_country) = LOWER(psd.channel_country) 
AND LOWER(prc.selling_channel) = LOWER(psd.selling_channel)
AND RANGE_CONTAINS(RANGE(prc.eff_begin_tmstp_utc ,prc.eff_end_tmstp_utc),(CAST(w01.DAY_DATE + 1 AS TIMESTAMP) - INTERVAL '0.001' SECOND))

WHERE prc.EFF_END_TMSTP >= (SELECT DATE_SUB(dw_batch_dt, INTERVAL (SELECT CAST(trunc(cast(config_value as float64)) AS INTEGER) AS config_value
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
   WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY')
    AND LOWER(config_key) = LOWER('REBUILD_DAYS')) DAY)
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.etl_batch_dt_lkup AS ebdl
WHERE LOWER(interface_code) = LOWER('MERCH_GDS_INV_DLY'))
) SRC
ON LOWER(SRC.RMS_SKU_NUM) = LOWER(TGT.RMS_SKU_NUM)
AND SRC.DAY_DATE = TGT.DAY_DATE
AND SRC.STORE_NUM = TGT.STORE_NUM
WHEN MATCHED THEN UPDATE SET
SELLING_RETAIL_PRICE_AMT = SRC.SELLING_RETAIL_PRICE_AMT,
OWNERSHIP_RETAIL_PRICE_AMT = SRC.OWNERSHIP_RETAIL_PRICE_AMT,
COMPARE_AT_RETAIL_PRICE_AMT = SRC.COMPARE_AT_RETAIL_PRICE_AMT,
REGULAR_PRICE_AMT = SRC.REGULAR_PRICE_AMT;




MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_inv_tran_sku_store_day_fact AS tgt
USING (SELECT rms_sku_num,
day_date,
price_type,
store_num,
boh_units,
boh_retail_amt,
boh_cost_amt,
CAST(trunc(cast(selling_retail_price_amt as float64)) AS INT64) AS selling_retail_price_amt,
ownership_retail_price_amt,
regular_price_amt,
CAST(trunc(cast(compare_at_retail_price_amt as float64)) AS INT64) AS compare_at_retail_price_amt,
weighted_average_cost_amt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_inv_sku_store_day_wrk AS w01) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND SRC.day_date = tgt.day_date AND LOWER(SRC.price_type) = LOWER(tgt.price_type) AND SRC.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE SET
    boh_units = SRC.boh_units,
    boh_retail_amt = SRC.boh_retail_amt,
    boh_cost_amt = SRC.boh_cost_amt,
    selling_retail_price_amt = SRC.selling_retail_price_amt,
    ownership_retail_price_amt = SRC.ownership_retail_price_amt,
    regular_price_amt = SRC.regular_price_amt,
    compare_at_retail_price_amt = SRC.compare_at_retail_price_amt,
    weighted_average_cost_amt = SRC.weighted_average_cost_amt,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S',
CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT (rms_sku_num,
day_date,
price_type,
store_num,
sales_units,
sales_retail_amt,
sales_cost_amt,
selling_retail_price_amt,
ownership_retail_price_amt,
regular_price_amt,
compare_at_retail_price_amt,
weighted_average_cost_amt,
dw_sys_updt_tmstp,
dw_sys_load_tmstp) 
VALUES(SRC.rms_sku_num,
SRC.day_date,
SRC.price_type,
SRC.store_num,
SRC.boh_units,
SRC.boh_retail_amt,
SRC.boh_cost_amt,
SRC.selling_retail_price_amt,
SRC.ownership_retail_price_amt,
SRC.regular_price_amt,
SRC.compare_at_retail_price_amt,
SRC.weighted_average_cost_amt,
CAST(FORMAT_TIMESTAMP('%F %H:%M:%S',
CURRENT_DATETIME('PST8PDT')) AS DATETIME),
CAST(FORMAT_TIMESTAMP('%F %H:%M:%S',
CURRENT_DATETIME('PST8PDT')) AS DATETIME));



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_inv_sku_store_day_wrk;
