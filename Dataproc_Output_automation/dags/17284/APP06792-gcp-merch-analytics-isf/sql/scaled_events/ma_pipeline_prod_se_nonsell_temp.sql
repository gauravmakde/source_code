/*
--DROP TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.ANNIVERSARY_NONSELL;
 CREATE MULTISET TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.ANNIVERSARY_NONSELL
			 , FALLBACK
			 , NO BEFORE JOURNAL
			 , NO AFTER JOURNAL
			 , CHECKSUM = DEFAULT
			 , DEFAULT MERGEBLOCKRATIO
(
		rms_sku_id  			VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL
		, snapshot_date 		DATE NOT NULL
	, snapshot_date_BOH 	DATE NOT NULL
	, value_updated_time	TIMESTAMP WITH TIME ZONE
		, location_id       VARCHAR(30)
		, nonsellable_qty		INTEGER
		, current_price_type VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC

)
PRIMARY INDEX (rms_sku_id,snapshot_date,location_id);
GRANT SELECT ON `{{params.gcp_project_id}}`.{{params.environment_schema}}.ANNIVERSARY_NONSELL  TO PUBLIC;
*/





CREATE TEMPORARY TABLE IF NOT EXISTS store_01
AS
SELECT store_num,
 price_store_num,
 store_type_code,
  CASE
  WHEN price_store_num IN (808, 867, 835, 1) OR price_store_num = - 1 AND channel_num = 120
  THEN 'FL'
  WHEN price_store_num IN (844, 828, 338) OR price_store_num = - 1 AND channel_num = 250
  THEN 'RK'
  ELSE NULL
  END AS store_type_code_new,
  CASE
  WHEN price_store_num IN (808, 828, 338, 1)
  THEN 'USA'
  WHEN price_store_num = - 1 AND LOWER(channel_country) = LOWER('US')
  THEN 'USA'
  WHEN price_store_num IN (844, 867, 835)
  THEN 'CAN'
  WHEN price_store_num = - 1 AND LOWER(channel_country) = LOWER('CA')
  THEN 'CAN'
  ELSE NULL
  END AS store_country_code,
  CASE
  WHEN (price_store_num = - 1 AND channel_num = 120) OR (price_store_num = - 1 AND channel_num = 250)
  THEN 'ONLINE'
  ELSE selling_channel
  END AS selling_channel,
 channel_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
GROUP BY store_num,
 price_store_num,
 store_type_code,
 store_type_code_new,
 store_country_code,
 selling_channel,
 channel_num;


--COLLECT STATS 	 PRIMARY INDEX (STORE_NUM) 	,COLUMN (store_num, store_type_code, store_country_code, selling_channel) 	,COLUMN (store_num) 	,COLUMN (selling_channel) 		ON STORE_01


CREATE TEMPORARY TABLE IF NOT EXISTS price_01
AS
SELECT b.rms_sku_num,
 a.store_type_code,
 a.store_country_code,
 a.selling_channel,
 b.channel_country,
 b.ownership_price_type,
 b.ownership_price_amt,
 b.regular_price_amt,
 b.current_price_amt,
 b.current_price_currency_code,
 b.current_price_type,
 b.eff_begin_tmstp,
 b.eff_end_tmstp
FROM (SELECT price_store_num,
   store_type_code_new AS store_type_code,
   store_country_code,
   selling_channel
  FROM store_01
  GROUP BY price_store_num,
   store_type_code,
   store_country_code,
   selling_channel) AS a
 INNER JOIN (SELECT rms_sku_num,
   store_num,
    CASE
    WHEN LOWER(channel_country) = LOWER('US')
    THEN 'USA'
    WHEN LOWER(channel_country) = LOWER('CA')
    THEN 'CAN'
    ELSE NULL
    END AS channel_country,
    CASE
    WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(ownership_retail_price_type_code) = LOWER('REGULAR')
    THEN 'R'
    ELSE ownership_retail_price_type_code
    END AS ownership_price_type,
   ownership_retail_price_amt AS ownership_price_amt,
   regular_price_amt,
   selling_retail_price_amt AS current_price_amt,
   selling_retail_currency_code AS current_price_currency_code,
    CASE
    WHEN LOWER(selling_retail_price_type_code) = LOWER('CLEARANCE')
    THEN 'C'
    WHEN LOWER(selling_retail_price_type_code) = LOWER('REGULAR')
    THEN 'R'
    WHEN LOWER(selling_retail_price_type_code) = LOWER('PROMOTION')
    THEN 'P'
    ELSE selling_retail_price_type_code
    END AS current_price_type,
   eff_begin_tmstp_utc as eff_begin_tmstp,
   eff_end_tmstp_utc as eff_end_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim
  WHERE CAST(eff_begin_tmstp_utc AS DATE) < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS b ON a.price_store_num = CAST(b.store_num AS FLOAT64)
GROUP BY b.rms_sku_num,
 a.store_type_code,
 a.store_country_code,
 a.selling_channel,
 b.channel_country,
 b.ownership_price_type,
 b.ownership_price_amt,
 b.regular_price_amt,
 b.current_price_amt,
 b.current_price_currency_code,
 b.current_price_type,
 b.eff_begin_tmstp,
 b.eff_end_tmstp;


--COLLECT STATS 	PRIMARY INDEX (RMS_SKU_NUM,STORE_TYPE_CODE,CHANNEL_COUNTRY,selling_channel,eff_begin_tmstp, eff_end_tmstp) 	,COLUMN (RMS_SKU_NUM) 	,COLUMN(EFF_BEGIN_TMSTP, EFF_END_TMSTP) 		ON PRICE_01


CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_01
AS
SELECT rms_sku_id,
 snapshot_date,
 DATE_ADD(snapshot_date, INTERVAL 1 DAY) AS snapshot_date_boh,
 value_updated_time_utc,
 value_updated_time_tz,
 location_id,
 COALESCE(unavailable_qty, 0) AS nonsellable_qty,
 location_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact
WHERE snapshot_date BETWEEN {{params.start_date_soh}} AND {{params.end_date}}
 AND COALESCE(unavailable_qty, 0) <> 0
GROUP BY rms_sku_id,
 snapshot_date,
 snapshot_date_boh,
 value_updated_time_utc,
 value_updated_time_tz,
 location_id,
 nonsellable_qty,
 location_type;


--COLLECT STATS 	PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) 	,COLUMN (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) 	,COLUMN (RMS_SKU_ID) 	,COLUMN (LOCATION_ID) 		ON SKU_SOH_01


CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_02
AS
SELECT a.rms_sku_id,
 b.store_type_code,
 b.store_type_code_new,
 b.store_country_code,
 b.selling_channel,
 a.snapshot_date,
 a.snapshot_date_boh,
 a.value_updated_time_utc,
 a.value_updated_time_tz,
 a.location_id,
 a.nonsellable_qty,
 a.location_type
FROM sku_soh_01 AS a
 LEFT JOIN store_01 AS b ON CAST(a.location_id AS FLOAT64) = b.store_num
GROUP BY a.rms_sku_id,
 b.store_type_code,
 b.store_type_code_new,
 b.store_country_code,
 b.selling_channel,
 a.snapshot_date,
 a.snapshot_date_boh,
 a.value_updated_time_utc,
 a.value_updated_time_tz,
 a.location_id,
 a.nonsellable_qty,
 a.location_type;


--collect stats 	primary index ( rms_sku_id, store_type_code, store_country_code, snapshot_date) 	,column ( rms_sku_id, store_type_code, store_country_code, snapshot_date) 		on sku_soh_02


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_nonsell
WHERE snapshot_date BETWEEN {{params.start_date_soh}} AND {{params.end_date}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.anniversary_nonsell
(SELECT a.rms_sku_id,
  a.snapshot_date,
  a.snapshot_date_boh,
  a.value_updated_time_utc,
  a.value_updated_time_tz,
  a.location_id,
  a.nonsellable_qty,
  MIN(b.ownership_price_type) AS current_price_type
 FROM sku_soh_02 AS a
  LEFT JOIN price_01 AS b 
  ON LOWER(a.rms_sku_id) = LOWER(b.rms_sku_num) 
  AND LOWER(a.store_type_code_new) = LOWER(b.store_type_code) 
  AND LOWER(a.store_country_code) = LOWER(b.channel_country) 
  AND LOWER(a.selling_channel) = LOWER(b.selling_channel)
  AND cast(a.snapshot_date as TIMESTAMP) between b.eff_begin_tmstp AND DATE_SUB(b.eff_end_tmstp, INTERVAL 1 MILLISECOND)
 WHERE a.nonsellable_qty <> 0
 GROUP BY a.rms_sku_id,
  a.snapshot_date,
  a.snapshot_date_boh,
  a.value_updated_time_utc,
  a.value_updated_time_tz,
  a.location_id,
  a.nonsellable_qty);


--collect stats 	primary index (rms_sku_id,snapshot_date,location_id) 	,column (rms_sku_id) 	,column (snapshot_date) 	,column (location_id) 		on `{{params.gcp_project_id}}`.{{params.environment_schema}}.ANNIVERSARY_NONSELL