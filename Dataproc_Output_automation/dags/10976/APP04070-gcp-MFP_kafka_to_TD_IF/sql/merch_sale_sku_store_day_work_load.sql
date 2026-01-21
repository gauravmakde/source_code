
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=merch_gds_ongoing_load;
---Task_Name=gds_sales_load;'*/
---FOR SESSION VOLATILE;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_sale_sku_store_day_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_sale_sku_store_day_wrk 
(rms_sku_num, day_date, price_type, store_num, sales_units,
 sales_retail_amt, sales_cost_amt, return_units, return_retail_amt, return_cost_amt)
(SELECT s01.rms_sku_num,
  cal.day_date,
  SUBSTR(s01.price_type, 1, 1) AS price_type,
  s01.store_num,
  SUM(s01.net_sales_units) AS sales_units,
  SUM(s01.net_sales_retl) AS sales_retail_amt,
  SUM(s01.net_sales_cost) AS sales_cost_amt,
  SUM(s01.returns_units) AS return_units,
  SUM(s01.returns_retl) AS return_retail_amt,
  SUM(s01.returns_cost) AS return_cost_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sale_return_sku_store_day_fact AS s01
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim AS cal 
  ON s01.day_num = cal.day_idnt
 WHERE cal.day_date BETWEEN (SELECT start_rebuild_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
    WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY')) 
	AND (SELECT end_rebuild_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
    WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY'))
  AND LOWER(s01.merch_ownership_dept_ind) = LOWER('Y')
 GROUP BY s01.rms_sku_num,
  s01.store_num,
  s01.price_type,
  cal.day_date);

MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_sale_sku_store_day_wrk AS tgt
USING (SELECT w01.rms_sku_num, w01.day_date, w01.store_num, MAX(prc.selling_retail_price_amt) AS selling_retail_price_amt, MAX(prc.regular_price_amt) AS regular_price_amt, MAX(prc.ownership_retail_price_amt) AS ownership_retail_price_amt, MAX(prc.compare_at_retail_price_amt) AS compare_at_retail_price_amt, MAX(CASE WHEN LOWER(COALESCE(prc.ownership_retail_price_type_code, prc.selling_retail_price_type_code, 'REGULAR')) = LOWER('CLEARANCE') THEN 'C' ELSE 'R' END) AS price_type
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sale_sku_store_day_wrk AS w01
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS s01 
		ON w01.store_num = s01.store_num
        INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.price_store_dim_vw AS psd 
		ON w01.store_num = psd.store_num
        INNER JOIN (`{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS prc 
		LEFT JOIN (SELECT start_rebuild_date AS A1088370191
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS dt
                WHERE LOWER(interface_code) = LOWER('MERCH_GDS_TRAN_DLY')) AS t0 ON TRUE) 
				ON LOWER(prc.rms_sku_num) = LOWER(w01.rms_sku_num) 
				AND LOWER(prc.channel_brand) = LOWER(psd.channel_brand) 
				AND LOWER(prc.channel_country) = LOWER(psd.channel_country) 
				AND LOWER(prc.selling_channel) = LOWER(psd.selling_channel)
        AND  RANGE_CONTAINS(RANGE(prc.eff_begin_tmstp_utc ,prc.eff_end_tmstp_utc),(CAST(w01.DAY_DATE + 1 AS TIMESTAMP) - INTERVAL '0.001' SECOND))
				AND CAST(prc.eff_end_tmstp_utc AS DATE) >= t0.A1088370191
    GROUP BY w01.rms_sku_num, w01.day_date, w01.store_num) AS SRC
ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) 
AND SRC.day_date = tgt.day_date 
AND SRC.store_num = tgt.store_num
WHEN MATCHED THEN UPDATE SET
    selling_retail_price_amt = SRC.selling_retail_price_amt,
    ownership_retail_price_amt = SRC.ownership_retail_price_amt,
    compare_at_retail_price_amt = SRC.compare_at_retail_price_amt,
    regular_price_amt = SRC.regular_price_amt;

