/*------------------------------------------------------
 SKU Daily Backorder History Table
 Purpose: Provide SKU/Day/Loc/Price level information on historical and current backorders.
 Last Update: 4/1/22 Jonathan Popescu
 Contact for logic/code: Jonathan Popescu (Merch Analytics)
 Contact for ETL: Jonathan Popescu (Merch Analytics)
 */

-- Calculate location and current backorder flag



CREATE TEMPORARY TABLE IF NOT EXISTS backorder_base_01
AS
SELECT rms_sku_num,
 source_channel_country_code AS channel_country,
  CASE
  WHEN LOWER(source_channel_code) = LOWER('FULL_LINE')
  THEN 'NORDSTROM'
  WHEN LOWER(source_channel_code) = LOWER('RACK')
  THEN 'NORDSTROM_RACK'
  ELSE NULL
  END AS channel_brand,
  CASE
  WHEN LOWER(source_platform_code) = LOWER('POS')
  THEN source_store_num
  WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('US')
  THEN 808
  WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('CA')
  THEN 867
  WHEN LOWER(source_channel_code) = LOWER('RACK')
  THEN 828
  ELSE NULL
  END AS price_store_num,
  CASE
  WHEN LOWER(source_platform_code) = LOWER('POS')
  THEN 'STORE'
  ELSE 'ONLINE'
  END AS selling_channel,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_amount,
 order_line_quantity,
  CASE
  WHEN shipped_date_pacific IS NULL 
  AND canceled_date_pacific IS NULL 
  AND arrived_at_order_pickup_date_pacific IS NULL
  AND fulfilled_date_pacific IS NULL 
  AND requested_max_promise_date_pacific > CURRENT_DATE('PST8PDT')
  THEN 'Y'
  ELSE 'N'
  END AS current_backorder_flag,
  CASE
  WHEN COALESCE(order_line_promotion_discount_amount_usd, 0) - COALESCE(order_line_employee_discount_amount_usd, 0) > 0
  THEN 1
  ELSE 0
  END AS promo_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE order_date_pacific BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 365 DAY) AND (CURRENT_DATE('PST8PDT'))
 AND (LOWER(backorder_ind) = LOWER('Y') OR LOWER(backorder_display_ind) = LOWER('Y'))
GROUP BY rms_sku_num,
 channel_country,
 channel_brand,
 price_store_num,
 selling_channel,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_amount,
 order_line_quantity,
 current_backorder_flag,
 promo_flag;


--COLLECT STATS 	 PRIMARY INDEX (RMS_SKU_NUM, PRICE_STORE_NUM, ORDER_TMSTP_PACIFIC) 	,COLUMN (RMS_SKU_NUM) 		ON BACKORDER_BASE_01


CREATE TEMPORARY TABLE IF NOT EXISTS backorder_base_02
AS
SELECT a.rms_sku_num,
 a.price_store_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.channel_country,
 a.channel_brand,
 a.order_line_amount,
 a.order_line_quantity,
 a.current_backorder_flag,
  CASE
  WHEN a.promo_flag = 1 AND LOWER(b.selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'P'
  WHEN LOWER(b.ownership_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'C'
  WHEN LOWER(b.selling_retail_price_type_code) = LOWER('PROMOTION')
  THEN 'P'
  ELSE 'R'
  END AS current_price_type,
 b.regular_price_amt,
 b.selling_retail_price_amt AS current_price_amt
FROM backorder_base_01 AS a
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND a.price_store_num
      = CAST(b.store_num AS FLOAT64) AND LOWER(a.selling_channel) = LOWER(b.selling_channel) AND a.order_tmstp_pacific
   BETWEEN cast(b.eff_begin_tmstp_utc as datetime) AND cast(b.eff_end_tmstp_utc as datetime)
GROUP BY a.rms_sku_num,
 a.price_store_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.channel_country,
 a.channel_brand,
 a.order_line_amount,
 a.order_line_quantity,
 a.current_backorder_flag,
 current_price_type,
 b.regular_price_amt,
 current_price_amt;


--COLLECT STATS 	 PRIMARY INDEX (RMS_SKU_NUM, PRICE_STORE_NUM, CURRENT_PRICE_TYPE, ORDER_TMSTP_PACIFIC, CURRENT_PRICE_AMT) 	,COLUMN (RMS_SKU_NUM) 		ON BACKORDER_BASE_02


CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline
AS
SELECT rms_sku_num,
 price_store_num AS store_num,
 current_price_type AS price_type,
 order_date_pacific,
 channel_country,
 channel_brand,
 current_price_amt AS current_price,
 regular_price_amt AS regular_price,
 SUM(order_line_quantity) AS backorder_units,
 SUM(order_line_amount) AS backorder_dollars,
 SUM(CASE
   WHEN LOWER(current_backorder_flag) = LOWER('Y')
   THEN order_line_quantity
   ELSE 0
   END) AS current_backorder_units,
 SUM(CASE
   WHEN LOWER(current_backorder_flag) = LOWER('Y')
   THEN order_line_amount
   ELSE 0
   END) AS current_backorder_dollars
FROM backorder_base_02
GROUP BY rms_sku_num,
 store_num,
 price_type,
 order_date_pacific,
 channel_country,
 channel_brand,
 current_price,
 regular_price;


--COLLECT STATS 	 PRIMARY INDEX (RMS_SKU_NUM, STORE_NUM, PRICE_TYPE, ORDER_DATE_PACIFIC, CURRENT_PRICE) 	,COLUMN (RMS_SKU_NUM) 		ON FINAL_BASELINE


MERGE INTO  `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.sku_bo_history AS a
USING final_baseline AS b
ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) 
AND a.store_num = b.store_num 
AND a.order_date_pacific = b.order_date_pacific
AND LOWER(a.price_type) = LOWER(b.price_type)
AND a.current_price = b.current_price 
AND a.regular_price = b.regular_price
  
WHEN MATCHED THEN UPDATE SET
 channel_country = b.channel_country,
 channel_brand = b.channel_brand,
 backorder_units = b.backorder_units,
 backorder_dollars = b.backorder_dollars,
 current_backorder_units = b.current_backorder_units,
 current_backorder_dollars = CAST(b.current_backorder_dollars AS NUMERIC)
WHEN NOT MATCHED THEN INSERT (rms_sku_num, store_num, price_type, order_date_pacific, current_price, regular_price,
 channel_country, channel_brand, backorder_units, backorder_dollars, current_backorder_units, current_backorder_dollars
 ) VALUES(b.rms_sku_num, b.store_num, b.price_type, b.order_date_pacific, b.current_price, b.regular_price, b.channel_country
 , b.channel_brand, b.backorder_units, b.backorder_dollars, b.current_backorder_units, CAST(b.current_backorder_dollars AS NUMERIC)
 );