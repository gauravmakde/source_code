-- Delete rows from SKU_LOC_PRICETYPE_DAY where DAY_DT is between '2024-01-01' and '2025-01-01'
DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day
WHERE DAY_DT BETWEEN {{params.start_date}} AND {{params.end_date}};

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day{{params.env_suffix}};
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs{{params.env_suffix}} ;




CREATE TEMPORARY TABLE IF NOT EXISTS sku_01
----CLUSTER BY rms_sku_num
AS
SELECT rms_sku_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
GROUP BY rms_sku_num;


CREATE TEMPORARY TABLE IF NOT EXISTS store_01
----CLUSTER BY store_num
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
  WHEN price_store_num = - 1 AND channel_num = 120 OR price_store_num = - 1 AND channel_num = 250
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


--,b.pricing_start_tmstp


--,b.pricing_end_tmstp


--,pricing_start_tmstp


--,pricing_end_tmstp


--,14,15


CREATE TEMPORARY TABLE IF NOT EXISTS price_01
----CLUSTER BY rms_sku_num, store_type_code, channel_country, selling_channel
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
   eff_begin_tmstp,
   eff_end_tmstp
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim
  WHERE CAST(eff_begin_tmstp AS DATE) < DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS b ON a.price_store_num = CAST(b.store_num AS FLOAT64)

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


CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_01a
----CLUSTER BY sku_num, tran_time, intent_store_num
AS
SELECT sku_num,
 business_day_date,
 tran_date,
  CASE
  WHEN LOWER(tran_type_code) = LOWER('RETN')
  THEN COALESCE(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(original_business_date AS DATETIME)) AS DATETIME), tran_time
   )
  ELSE CAST(tran_time AS DATETIME)
  END AS tran_time,
 tran_time AS tran_time_new,
 intent_store_num,
  CASE
  WHEN LOWER(tran_type_code) = LOWER('RETN')
  THEN COALESCE(original_ringing_store_num, intent_store_num)
  ELSE intent_store_num
  END AS intent_store_num_new,
  CASE
  WHEN LOWER(tran_type_code) IN (LOWER('RETN'), LOWER('EXCH'))
  THEN COALESCE(original_ringing_store_num, ringing_store_num, intent_store_num)
  ELSE COALESCE(fulfilling_store_num, ringing_store_num, intent_store_num)
  END AS intent_store_num_cogs,
 tran_type_code,
  CASE
  WHEN line_item_promo_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS promo_flag,
 line_net_usd_amt,
 line_net_amt,
 line_item_quantity,
 line_item_seq_num,
  CASE
  WHEN LOWER(line_item_fulfillment_type) = LOWER('VendorDropShip')
  THEN 1
  ELSE 0
  END AS dropship_fulfilled
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS a
WHERE CAST(upc_num AS FLOAT64) > 0
 AND business_day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND LOWER(line_item_merch_nonmerch_ind) = LOWER('MERCH')
 AND intent_store_num NOT IN (260, 770)
 AND sku_num IS NOT NULL
GROUP BY sku_num,
 business_day_date,
 tran_date,
 tran_time,
 tran_time_new,
 intent_store_num,
 intent_store_num_new,
 intent_store_num_cogs,
 tran_type_code,
 promo_flag,
 line_net_usd_amt,
 line_net_amt,
 line_item_quantity,
 line_item_seq_num,
 dropship_fulfilled;

 CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_01 AS
(
  SELECT
    a.sku_num,
    a.business_day_date,
    a.tran_date,
    a.tran_time,
    a.intent_store_num,
    a.intent_store_num_new,
    a.tran_type_code,
    a.promo_flag,
    a.line_net_usd_amt,
    a.line_net_amt,
    a.line_item_quantity,
    a.line_item_seq_num,
    c.weighted_average_cost_currency_code,
    CASE
      WHEN a.tran_type_code = 'SALE' AND dropship_fulfilled = 1 THEN COALESCE(c.weighted_average_cost, d.weighted_average_cost)
      WHEN a.tran_type_code = 'SALE' THEN c.weighted_average_cost
      WHEN a.tran_type_code = 'RETN' THEN c.weighted_average_cost * -1
      WHEN a.tran_type_code = 'EXCH' AND a.line_net_amt <= 0 THEN c.weighted_average_cost * -1
      WHEN a.tran_type_code = 'EXCH' AND a.line_net_amt > 0 THEN c.weighted_average_cost
      ELSE c.weighted_average_cost
    END AS weighted_average_cost
  FROM
    trans_base_01a a
  LEFT JOIN
   `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_dim c
    ON a.sku_num = c.sku_num
    AND TIMESTAMP_ADD(a.tran_time, INTERVAL 8 HOUR) BETWEEN c.weighted_average_cost_changed_tmstp AND TIMESTAMP_SUB(c.eff_end_tmstp, INTERVAL  1 MILLISECOND)
    AND a.intent_store_num_cogs    = cast(trunc(cast(c.location_num as float64))  as int64)
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim d
    ON a.sku_num = d.sku_num
    AND a.business_day_date BETWEEN d.eff_begin_dt AND DATE_SUB(d.eff_end_dt, INTERVAL 1 DAY)
    AND a.intent_store_num_cogs =  cast(trunc(cast(d.location_num as float64))  as int64)
);



--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, TRAN_TIME) 	,COLUMN (SKU_NUM, TRAN_TIME) 		ON TRANS_BASE_01A


INSERT INTO trans_base_01a
(SELECT b.rms_sku_num,
  a.business_day_date,
  a.tran_date,
   CASE
   WHEN LOWER(a.tran_type_code) = LOWER('RETN')
   THEN COALESCE(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(a.original_business_date AS DATETIME)) AS DATETIME), a.tran_time
    )
   ELSE CAST(a.tran_time AS DATETIME)
   END AS tran_time,
  a.tran_time AS tran_time_new,
  a.intent_store_num,
   CASE
   WHEN LOWER(a.tran_type_code) = LOWER('RETN')
   THEN COALESCE(a.original_ringing_store_num, a.intent_store_num)
   ELSE a.intent_store_num
   END AS intent_store_num_new,
   CASE
   WHEN LOWER(a.tran_type_code) IN (LOWER('RETN'), LOWER('EXCH'))
   THEN COALESCE(a.original_ringing_store_num, a.ringing_store_num, a.intent_store_num)
   ELSE COALESCE(a.fulfilling_store_num, a.ringing_store_num, a.intent_store_num)
   END AS intent_store_num_cogs,
  a.tran_type_code,
   CASE
   WHEN a.line_item_promo_id IS NOT NULL
   THEN 1
   ELSE 0
   END AS promo_flag,
  a.line_net_usd_amt,
  a.line_net_amt,
  a.line_item_quantity,
  a.line_item_seq_num,
   CASE
   WHEN LOWER(a.line_item_fulfillment_type) = LOWER('VendorDropShip')
   THEN 1
   ELSE 0
   END AS dropship_fulfilled
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS a
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_upc_dim AS b ON LOWER(a.upc_num) = LOWER(b.upc_num)
 WHERE a.sku_num IS NULL
  AND a.business_day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
  AND LOWER(a.line_item_merch_nonmerch_ind) = LOWER('MERCH')
  AND a.intent_store_num NOT IN (260, 770)
 GROUP BY b.rms_sku_num,
  a.business_day_date,
  a.tran_date,
  tran_time,
  tran_time_new,
  a.intent_store_num,
  intent_store_num_new,
  intent_store_num_cogs,
  a.tran_type_code,
  promo_flag,
  a.line_net_usd_amt,
  a.line_net_amt,
  a.line_item_quantity,
  a.line_item_seq_num,
  dropship_fulfilled);


--COLLECT STATS 	PRIMARY INDEX (SKU_NUM,TRAN_TIME), 	INDEX (INTENT_STORE_NUM), 	COLUMN (SKU_NUM), 	COLUMN (SKU_NUM,TRAN_TIME), 	COLUMN







CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_02
----CLUSTER BY sku_num, intent_store_num, store_country_code, tran_time
AS
SELECT a.sku_num,
 a.business_day_date,
 a.tran_date,
 a.tran_time,
 a.intent_store_num,
 b.price_store_num,
 b.selling_channel,
 a.tran_type_code,
 SUM(a.line_net_usd_amt) AS line_net_usd_amt,
 SUM(a.line_net_amt) AS line_net_amt,
 SUM(a.line_item_quantity) AS line_item_quantity,
 a.line_item_seq_num,
 a.promo_flag,
 b.store_type_code,
 b.store_country_code,
 a.weighted_average_cost
FROM trans_base_01 AS a
 LEFT JOIN store_01 AS b ON a.intent_store_num = b.store_num
GROUP BY a.sku_num,
 a.business_day_date,
 a.tran_date,
 a.tran_time,
 a.intent_store_num,
 b.price_store_num,
 b.selling_channel,
 a.tran_type_code,
 a.line_item_seq_num,
 a.promo_flag,
 b.store_type_code,
 b.store_country_code,
 a.weighted_average_cost;




CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_03 AS
SELECT
    a.SKU_NUM,
    a.BUSINESS_DAY_DATE,
    a.TRAN_DATE,
    a.TRAN_TIME,
    a.INTENT_STORE_NUM,
    a.price_store_num,
    a.TRAN_TYPE_CODE,
    a.LINE_NET_USD_AMT,
    a.LINE_NET_AMT,
    a.LINE_ITEM_QUANTITY,
    a.LINE_ITEM_SEQ_NUM,
    a.STORE_TYPE_CODE,
    b.rms_sku_num,
    b.regular_price_amt,
    b.selling_retail_price_amt AS current_price_amt,
    b.selling_retail_currency_code AS current_price_currency_code,
    CASE
        WHEN b.ownership_retail_price_type_code = 'CLEARANCE' THEN 'C'
        WHEN a.PROMO_FLAG = 1 THEN 'P'
        ELSE COALESCE(
            CASE
                WHEN any_value(selling_retail_price_type_code) = 'CLEARANCE' THEN 'C'
                WHEN any_value (selling_retail_price_type_code )= 'REGULAR' THEN 'R'
                WHEN any_value(selling_retail_price_type_code) = 'PROMOTION' THEN 'P'
                ELSE any_value(selling_retail_price_type_code)
            END, 'N/A'
        )
    END AS current_price_type,
    a.WEIGHTED_AVERAGE_COST,
    a.WEIGHTED_AVERAGE_COST * a.LINE_ITEM_QUANTITY AS COST_OF_GOODS_SOLD
FROM trans_base_02 a
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim b
    ON a.SKU_NUM = b.RMS_SKU_NUM
    AND a.price_store_num = cast(trunc(cast(b.store_num as float64)) as integer)
    AND a.selling_channel = b.selling_channel
    AND a.TRAN_TIME BETWEEN b.eff_begin_tmstp AND TIMESTAMP_SUB(b.eff_end_tmstp, INTERVAL 1 MILLISECOND)
GROUP BY
    a.SKU_NUM,
    a.BUSINESS_DAY_DATE,
    a.TRAN_DATE,
    a.TRAN_TIME,
    a.INTENT_STORE_NUM,
    a.price_store_num,
    a.TRAN_TYPE_CODE,
    a.LINE_NET_USD_AMT,
    a.LINE_NET_AMT,
    a.LINE_ITEM_QUANTITY,
    a.LINE_ITEM_SEQ_NUM,
    a.STORE_TYPE_CODE,
    b.rms_sku_num,
    b.regular_price_amt,
    b.selling_retail_price_amt,
    b.selling_retail_currency_code,
    b.ownership_retail_price_type_code,
    a.PROMO_FLAG,
    a.WEIGHTED_AVERAGE_COST;




CREATE TEMPORARY TABLE IF NOT EXISTS trans_base_final
CLUSTER BY row_id
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', business_day_date) || '_' || TRIM(FORMAT('%11d', intent_store_num)) || '_' ||
      TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || current_price_type AS row_id,
 sku_num,
 business_day_date,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(line_net_amt) AS net_sls,
  CASE
  WHEN MIN(line_net_amt) < 0
  THEN MIN(line_net_amt) * - 1
  ELSE NULL
  END AS return,
 SUM(CASE
   WHEN LOWER(tran_type_code) = LOWER('RETN')
   THEN line_item_quantity * - 1
   ELSE line_item_quantity
   END) AS sls_units,
 MAX(CASE
   WHEN LOWER(tran_type_code) = LOWER('RETN')
   THEN line_item_quantity
   ELSE NULL
   END) AS return_units,
 SUM(cost_of_goods_sold) AS cost_of_goods_sold
FROM (SELECT sku_num,
   business_day_date,
   intent_store_num,
   store_type_code,
    CASE
    WHEN LOWER(tran_type_code) = LOWER('EXCH') AND line_net_amt >= 0
    THEN 'SALE'
    WHEN LOWER(tran_type_code) = LOWER('EXCH') AND line_net_amt < 0
    THEN 'RETN'
    ELSE tran_type_code
    END AS tran_type_code,
   regular_price_amt,
   current_price_amt,
   current_price_currency_code,
   current_price_type,
   SUM(line_net_amt) AS line_net_amt,
   SUM(line_item_quantity) AS line_item_quantity,
   SUM(cost_of_goods_sold) AS cost_of_goods_sold
  FROM trans_base_03
  GROUP BY sku_num,
   business_day_date,
   intent_store_num,
   store_type_code,
   tran_type_code,
   regular_price_amt,
   current_price_amt,
   current_price_currency_code,
   current_price_type) AS a
GROUP BY row_id,
 sku_num,
 business_day_date,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;


--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 	,COLUMN (SKU_NUM) 		ON TRANS_BASE_FINAL


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'TRANS_BASE_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM trans_base_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt,
cast(b.process_timestamp as timestamp),
 b.process_timestamp_tz);


------ DROP INTERMEDIATE TRANS TABLES -----


DROP TABLE IF EXISTS trans_base_01a;


DROP TABLE IF EXISTS trans_base_01;


DROP TABLE IF EXISTS trans_base_02;


DROP TABLE IF EXISTS trans_base_03;


--------------------------------------------


-- 2023.11.07 - Fraud Transactions identification


-- 2023.11.07 - Same day cancellations identification


CREATE TEMPORARY TABLE IF NOT EXISTS demand_base_01
CLUSTER BY sku_num, order_tmstp_pacific, destination_node_num
AS
SELECT sku_num,
 rms_sku_num,
 sku_type,
 delivery_method_code,
  CASE
  WHEN LOWER(delivery_method_code) = LOWER('PICK')
  THEN destination_node_num
  ELSE NULL
  END AS destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
  CASE
  WHEN COALESCE(order_line_promotion_discount_amount, 0) - COALESCE(order_line_employee_discount_amount, 0) > 0
  THEN 1
  ELSE 0
  END AS promo_flag,
 source_channel_code,
 source_store_num,
 source_channel_country_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 order_line_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE order_date_pacific BETWEEN {{params.start_date}}
 AND {{params.end_date}}
 AND LOWER(COALESCE(fraud_cancel_ind, 'N')) <> LOWER('Y')
 AND order_date_pacific <> COALESCE(canceled_date_pacific, DATE '1900-01-01')
GROUP BY sku_num,
 rms_sku_num,
 sku_type,
 delivery_method_code,
 destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
 promo_flag,
 source_channel_code,
 source_store_num,
 source_channel_country_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 order_line_id;


--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,INDEX (DESTINATION_NODE_NUM) 		ON DEMAND_BASE_01


CREATE TEMPORARY TABLE IF NOT EXISTS demand_base_02
CLUSTER BY sku_num, price_store_num, selling_channel, order_tmstp_pacific
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.prc_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.shipped_date_pacific,
 a.promo_flag,
 a.order_line_id,
 b.price_store_num,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   order_tmstp_pacific,
   order_date_pacific,
   destination_node_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 5405
    WHEN destination_node_num IS NOT NULL
    THEN destination_node_num
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('US')
    THEN 808
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('CA')
    THEN 867
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS intent_store_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 808
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('US')
    THEN 808
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('CA')
    THEN 867
    WHEN LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS prc_store_num,
   delivery_method_code,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   canceled_date_pacific,
   shipped_date_pacific,
   promo_flag,
   order_line_id
  FROM demand_base_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.prc_store_num = b.store_num;




CREATE TEMPORARY TABLE IF NOT EXISTS demand_base_03 AS
SELECT
    a.SKU_NUM,
    a.ORDER_TMSTP_PACIFIC,
    a.ORDER_DATE_PACIFIC,
    a.DESTINATION_NODE_NUM,
    a.INTENT_STORE_NUM,
    a.DELIVERY_METHOD_CODE,
    a.ORDER_LINE_QUANTITY,
    a.ORDER_LINE_CURRENT_AMOUNT,
    a.SOURCE_CHANNEL_CODE,
    a.CANCELED_DATE_PACIFIC,
    a.SHIPPED_DATE_PACIFIC,
    a.PRICE_STORE_NUM,
    a.ORDER_LINE_ID,
    b.RMS_SKU_NUM,
    b.REGULAR_PRICE_AMT,
    b.selling_retail_price_amt AS current_price_amt,
    b.selling_retail_currency_code AS current_price_currency_code,
    CASE
        WHEN PROMO_FLAG = 1 AND any_value(b.selling_retail_price_type_code) = 'REGULAR' THEN 'P'
        WHEN b.ownership_retail_price_type_code = 'CLEARANCE' THEN 'C'
        ELSE
            CASE
                WHEN any_value(selling_retail_price_type_code) = 'CLEARANCE' THEN 'C'
                WHEN any_value(selling_retail_price_type_code) = 'REGULAR' THEN 'R'
                WHEN any_value(selling_retail_price_type_code) = 'PROMOTION' THEN 'P'
                ELSE any_value(selling_retail_price_type_code)
            END
    END AS current_price_type
FROM demand_base_02 a
LEFT JOIN   `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim b
    ON a.SKU_NUM = b.RMS_SKU_NUM
    AND a.price_store_num =   cast(trunc(cast(b.store_num as float64) )as int64)
    AND a.selling_channel = b.selling_channel
    AND a.ORDER_TMSTP_PACIFIC BETWEEN b.EFF_BEGIN_TMSTP AND TIMESTAMP_SUB(b.EFF_END_TMSTP, INTERVAL 1 MILLISECOND)
GROUP BY
    a.SKU_NUM,
    a.ORDER_TMSTP_PACIFIC,
    a.ORDER_DATE_PACIFIC,
    a.DESTINATION_NODE_NUM,
    a.INTENT_STORE_NUM,
    a.DELIVERY_METHOD_CODE,
    a.ORDER_LINE_QUANTITY,
    a.ORDER_LINE_CURRENT_AMOUNT,
    a.SOURCE_CHANNEL_CODE,
    a.CANCELED_DATE_PACIFIC,
    a.SHIPPED_DATE_PACIFIC,
    a.PRICE_STORE_NUM,
    a.ORDER_LINE_ID,
    b.RMS_SKU_NUM,
    b.REGULAR_PRICE_AMT,
    b.selling_retail_price_amt,
    b.selling_retail_currency_code,
    b.ownership_retail_price_type_code,
    a.PROMO_FLAG;






--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, INTENT_STORE_NUM, ORDER_TMSTP_PACIFIC) 		ON DEMAND_BASE_03


---2024-03-05 to filter blank SKUs


CREATE TEMPORARY TABLE IF NOT EXISTS demand_base_final
--CLUSTER BY row_id
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', order_date_pacific) || '_' || TRIM(FORMAT('%11d', intent_store_num)) || '_'
      || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 order_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS demand_dollars,
 SUM(order_line_quantity) AS demand_units
FROM demand_base_03
WHERE LOWER(sku_num) <> LOWER('')
GROUP BY row_id,
 sku_num,
 order_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;


--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 		ON DEMAND_BASE_FINAL


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'DEMAND_BASE_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM demand_base_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt,
cast(b.process_timestamp as timestamp),
process_timestamp_tz);


------ DROP INTERMEDIATE DEMAND BASE TABLES -----


DROP TABLE IF EXISTS demand_base_01;


DROP TABLE IF EXISTS demand_base_02;


DROP TABLE IF EXISTS demand_base_03;


--------------------------------------------------


-- 2023.11.07 - Fraud transactions identification


-- 2023.11.07 - Same day cancellations identification


CREATE TEMPORARY TABLE IF NOT EXISTS demand_dropship_01
---CLUSTER BY sku_num, order_tmstp_pacific, destination_node_num
AS
SELECT sku_num,
 rms_sku_num,
 sku_type,
 delivery_method_code,
  CASE
  WHEN LOWER(delivery_method_code) = LOWER('PICK')
  THEN destination_node_num
  ELSE NULL
  END AS destination_node_num,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
  CASE
  WHEN COALESCE(order_line_promotion_discount_amount, 0) - COALESCE(order_line_employee_discount_amount, 0) > 0
  THEN 1
  ELSE 0
  END AS promo_flag,
 source_channel_code,
 source_store_num,
 source_channel_country_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 order_line_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE order_date_pacific BETWEEN  {{params.start_date}}
 AND {{params.end_date}}
 AND LOWER(first_released_node_type_code) = LOWER('DS')
 AND LOWER(COALESCE(fraud_cancel_ind, 'N')) <> LOWER('Y')
 AND order_date_pacific <> COALESCE(canceled_date_pacific, DATE '1900-01-01')
GROUP BY sku_num,
 rms_sku_num,
 sku_type,
 delivery_method_code,
 destination_node_num,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
 promo_flag,
 source_channel_code,
 source_store_num,
 source_channel_country_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 order_line_id;


--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,INDEX (DESTINATION_NODE_NUM) 		ON DEMAND_DROPSHIP_01


CREATE TEMPORARY TABLE IF NOT EXISTS demand_dropship_02
---CLUSTER BY sku_num, price_store_num, selling_channel, order_tmstp_pacific
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.order_date_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.prc_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.shipped_date_pacific,
 a.promo_flag,
 a.order_line_id,
 b.price_store_num,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   order_tmstp_pacific,
   order_date_pacific,
   destination_node_num,
    CASE
    WHEN destination_node_num IS NOT NULL
    THEN destination_node_num
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('US')
    THEN 808
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('CA')
    THEN 867
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS intent_store_num,
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
    END AS prc_store_num,
   delivery_method_code,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   canceled_date_pacific,
   shipped_date_pacific,
   promo_flag,
   order_line_id
  FROM demand_dropship_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.prc_store_num = b.store_num;




CREATE TEMPORARY TABLE IF NOT EXISTS demand_dropship_03 AS
SELECT
    a.SKU_NUM,
    a.ORDER_TMSTP_PACIFIC,
    a.ORDER_DATE_PACIFIC,
    a.DESTINATION_NODE_NUM,
    a.INTENT_STORE_NUM,
    a.DELIVERY_METHOD_CODE,
    a.ORDER_LINE_QUANTITY,
    a.ORDER_LINE_CURRENT_AMOUNT,
    a.SOURCE_CHANNEL_CODE,
    a.CANCELED_DATE_PACIFIC,
    a.SHIPPED_DATE_PACIFIC,
    a.PRICE_STORE_NUM,
    a.ORDER_LINE_ID,
    b.RMS_SKU_NUM,
    b.REGULAR_PRICE_AMT,
    b.selling_retail_price_amt AS current_price_amt,
    b.selling_retail_currency_code AS current_price_currency_code,
    CASE
        WHEN PROMO_FLAG = 1 AND any_value(b.selling_retail_price_type_code) = 'REGULAR' THEN 'P'
        WHEN b.ownership_retail_price_type_code = 'CLEARANCE' THEN 'C'
        ELSE
            CASE
                WHEN any_value(selling_retail_price_type_code) = 'CLEARANCE' THEN 'C'
                WHEN any_value(selling_retail_price_type_code) = 'REGULAR' THEN 'R'
                WHEN any_value(selling_retail_price_type_code) = 'PROMOTION' THEN 'P'
                ELSE any_value(selling_retail_price_type_code)
            END
    END AS current_price_type
FROM demand_dropship_02 a
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim b
    ON a.SKU_NUM = b.RMS_SKU_NUM
    AND a.price_store_num =  cast(trunc(cast(b.store_num as float64)) as integer)
    AND a.selling_channel = b.selling_channel
    AND a.ORDER_TMSTP_PACIFIC BETWEEN b.EFF_BEGIN_TMSTP AND TIMESTAMP_SUB(b.EFF_END_TMSTP, INTERVAL 1 MILLISECOND)
GROUP BY
    a.SKU_NUM,
    a.ORDER_TMSTP_PACIFIC,
    a.ORDER_DATE_PACIFIC,
    a.DESTINATION_NODE_NUM,
    a.INTENT_STORE_NUM,
    a.DELIVERY_METHOD_CODE,
    a.ORDER_LINE_QUANTITY,
    a.ORDER_LINE_CURRENT_AMOUNT,
    a.SOURCE_CHANNEL_CODE,
    a.CANCELED_DATE_PACIFIC,
    a.SHIPPED_DATE_PACIFIC,
    a.PRICE_STORE_NUM,
    a.ORDER_LINE_ID,
    b.RMS_SKU_NUM,
    b.REGULAR_PRICE_AMT,
    b.selling_retail_price_amt,
    b.selling_retail_currency_code,
    b.ownership_retail_price_type_code,
    a.PROMO_FLAG;






CREATE TEMPORARY TABLE IF NOT EXISTS demand_dropship_final
--CLUSTER BY row_id
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', order_date_pacific) || '_' || TRIM(FORMAT('%11d', intent_store_num)) || '_'
      || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 order_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS demand_dropship_dollars,
 SUM(order_line_quantity) AS demand_dropship_units
FROM demand_dropship_03
WHERE LOWER(sku_num) <> LOWER('')
GROUP BY row_id,
 sku_num,
 order_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;


--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 		ON DEMAND_DROPSHIP_FINAL


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'DEMAND_DROPSHIP_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  as process_timestamp_tz
 FROM demand_dropship_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt,
 cast(b.process_timestamp as timestamp),
 process_timestamp_tz
 );


------ DROP INTERMEDIATE DEMAND DS TABLES -----


DROP TABLE IF EXISTS demand_dropship_01;


DROP TABLE IF EXISTS demand_dropship_02;


DROP TABLE IF EXISTS demand_dropship_03;


-----------------------------------------------


CREATE TEMPORARY TABLE IF NOT EXISTS demand_cancel_01
CLUSTER BY sku_num, order_tmstp_pacific, destination_node_num
AS
SELECT sku_num,
 rms_sku_num,
 order_tmstp_pacific,
 canceled_tmstp_pacific,
 canceled_date_pacific,
 sku_type,
 delivery_method_code,
 destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_line_quantity,
 order_line_current_amount,
 source_channel_code,
 source_channel_country_code,
 source_platform_code,
 cancel_reason_code,
 fraud_cancel_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE canceled_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND canceled_date_pacific <> order_date_pacific
 AND LOWER(cancel_reason_code) NOT LIKE LOWER('%FRAUD%')
GROUP BY sku_num,
 rms_sku_num,
 order_tmstp_pacific,
 canceled_tmstp_pacific,
 canceled_date_pacific,
 sku_type,
 delivery_method_code,
 destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_line_quantity,
 order_line_current_amount,
 source_channel_code,
 source_channel_country_code,
 source_platform_code,
 cancel_reason_code,
 fraud_cancel_ind;


--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,COLUMN (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,INDEX (DESTINATION_NODE_NUM) 		ON DEMAND_CANCEL_01


CREATE TEMPORARY TABLE IF NOT EXISTS demand_cancel_02
CLUSTER BY sku_num, price_store_num, selling_channel, order_tmstp_pacific
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.canceled_tmstp_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 b.price_store_num,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   order_tmstp_pacific,
   canceled_tmstp_pacific,
   destination_node_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 5405
    WHEN destination_node_num IS NOT NULL
    THEN destination_node_num
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('US')
    THEN 808
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('CA')
    THEN 867
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS intent_store_num,
   delivery_method_code,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   canceled_date_pacific
  FROM demand_cancel_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.intent_store_num = b.store_num;



CREATE TEMPORARY TABLE IF NOT EXISTS demand_cancel_03 AS
SELECT
    a.SKU_NUM,
    a.ORDER_TMSTP_PACIFIC,
    a.CANCELED_TMSTP_PACIFIC,
    a.DESTINATION_NODE_NUM,
    a.INTENT_STORE_NUM,
    a.DELIVERY_METHOD_CODE,
    a.ORDER_LINE_QUANTITY,
    a.ORDER_LINE_CURRENT_AMOUNT,
    a.SOURCE_CHANNEL_CODE,
    a.CANCELED_DATE_PACIFIC,
    a.PRICE_STORE_NUM,
    b.RMS_SKU_NUM,
    b.REGULAR_PRICE_AMT,
    b.selling_retail_price_amt AS current_price_amt,
    b.selling_retail_currency_code AS current_price_currency_code,
    CASE
        WHEN b.selling_retail_price_type_code = 'CLEARANCE' THEN 'C'
        WHEN b.selling_retail_price_type_code = 'REGULAR' THEN 'R'
        WHEN b.selling_retail_price_type_code = 'PROMOTION' THEN 'P'
        ELSE b.selling_retail_price_type_code
    END AS CURRENT_PRICE_TYPE
FROM demand_cancel_02 a
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim b
    ON a.SKU_NUM = b.RMS_SKU_NUM
    AND a.price_store_num = cast(trunc (cast(b.store_num as float64)) as int64)
    AND a.selling_channel = b.selling_channel
    AND a.ORDER_TMSTP_PACIFIC BETWEEN b.EFF_BEGIN_TMSTP AND TIMESTAMP_SUB(b.EFF_END_TMSTP, INTERVAL 1 MILLISECOND)
GROUP BY
    a.SKU_NUM,
    a.ORDER_TMSTP_PACIFIC,
    a.CANCELED_TMSTP_PACIFIC,
    a.DESTINATION_NODE_NUM,
    a.INTENT_STORE_NUM,
    a.DELIVERY_METHOD_CODE,
    a.ORDER_LINE_QUANTITY,
    a.ORDER_LINE_CURRENT_AMOUNT,
    a.SOURCE_CHANNEL_CODE,
    a.CANCELED_DATE_PACIFIC,
    a.PRICE_STORE_NUM,
    b.RMS_SKU_NUM,
    b.REGULAR_PRICE_AMT,
    b.selling_retail_price_amt,
    b.selling_retail_currency_code,
    b.selling_retail_price_type_code;






CREATE TEMPORARY TABLE IF NOT EXISTS demand_cancel_final
---CLUSTER BY row_id
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', canceled_date_pacific) || '_' || TRIM(FORMAT('%11d', intent_store_num)) ||
       '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 canceled_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS demand_cancel_dollars,
 SUM(order_line_quantity) AS demand_cancel_units
FROM demand_cancel_03
GROUP BY row_id,
 sku_num,
 canceled_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;


--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 		ON DEMAND_CANCEL_FINAL


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'DEMAND_CANCEL_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM demand_cancel_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, cast(b.process_timestamp as timestamp),
process_timestamp_tz);


------ DROP INTERMEDIATE DEMAND CANCEL TABLES -----


DROP TABLE IF EXISTS demand_cancel_01;


DROP TABLE IF EXISTS demand_cancel_02;


DROP TABLE IF EXISTS demand_cancel_03;


--------------------------------------------------


CREATE TEMPORARY TABLE IF NOT EXISTS store_fulfill_01
CLUSTER BY sku_num, fulfilled_tmstp_pacific
AS
SELECT sku_num,
 rms_sku_num,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
 source_channel_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 fulfilled_node_num,
 fulfilled_node_type_code
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE fulfilled_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND fulfilled_node_num IS NOT NULL
GROUP BY sku_num,
 rms_sku_num,
 order_num,
 order_line_num,
 order_tmstp_pacific,
 order_date_pacific,
 order_line_quantity,
 order_line_current_amount,
 source_channel_code,
 source_platform_code,
 canceled_date_pacific,
 cancel_reason_code,
 fraud_cancel_ind,
 fulfilled_tmstp_pacific,
 fulfilled_date_pacific,
 fulfilled_node_num,
 fulfilled_node_type_code;


--collect stats 	primary index ( sku_num, fulfilled_tmstp_pacific ) 	,column ( sku_num, fulfilled_tmstp_pacific ) 		on store_fulfill_01


CREATE TEMPORARY TABLE IF NOT EXISTS store_fulfill_02
----CLUSTER BY sku_num, price_store_num, selling_channel, fulfilled_tmstp_pacific
AS
SELECT a.sku_num,
 a.fulfilled_tmstp_pacific,
 a.fulfilled_date_pacific,
 a.fulfilled_node_num,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.canceled_date_pacific,
 a.order_tmstp_pacific,
 b.price_store_num,
 b.store_type_code,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   fulfilled_tmstp_pacific,
   fulfilled_date_pacific,
   fulfilled_node_num,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   canceled_date_pacific,
   order_tmstp_pacific
  FROM store_fulfill_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.fulfilled_node_num = b.store_num;




CREATE TEMPORARY TABLE IF NOT EXISTS store_fulfill_03 AS
SELECT
    a.sku_num,
    a.fulfilled_tmstp_pacific,
    a.fulfilled_date_pacific,
    a.fulfilled_node_num,
    a.order_line_quantity,
    a.order_line_current_amount,
    a.source_channel_code,
    a.canceled_date_pacific,
    a.price_store_num,
    b.rms_sku_num,
    b.regular_price_amt,
    b.selling_retail_price_amt AS current_price_amt,
    b.selling_retail_currency_code AS current_price_currency_code,
    CASE
        WHEN b.selling_retail_price_type_code = 'CLEARANCE' THEN 'C'
        WHEN b.selling_retail_price_type_code = 'REGULAR' THEN 'R'
        WHEN b.selling_retail_price_type_code = 'PROMOTION' THEN 'P'
        ELSE b.selling_retail_price_type_code
    END AS CURRENT_PRICE_TYPE
FROM store_fulfill_02 a
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_USR_VWS.PRODUCT_PRICE_TIMELINE_DIM b
    ON a.sku_num = b.rms_sku_num
    AND a.price_store_num =    cast(trunc(cast(b.store_num as float64)) as int64)
    AND a.selling_channel = b.selling_channel
    AND a.order_tmstp_pacific BETWEEN b.eff_begin_tmstp AND TIMESTAMP_SUB(b.eff_end_tmstp, INTERVAL 1 MILLISECOND);






CREATE TEMPORARY TABLE IF NOT EXISTS store_fulfill_final
---CLUSTER BY row_id
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', fulfilled_date_pacific) || '_' || TRIM(FORMAT('%11d', fulfilled_node_num)) ||
       '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 fulfilled_date_pacific,
 fulfilled_node_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS store_fulfill_dollars,
 SUM(order_line_quantity) AS store_fulfill_units
FROM store_fulfill_03
GROUP BY row_id,
 sku_num,
 fulfilled_date_pacific,
 fulfilled_node_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;


--collect stats 	primary index ( row_id ) 	,column ( row_id ) 		on store_fulfill_final


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'STORE_FULFILL_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM store_fulfill_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt,
cast(b.process_timestamp as timestamp),
b. process_timestamp_tz);


------ DROP INTERMEDIATE STORE FULFILL TABLES -----


DROP TABLE IF EXISTS store_fulfill_01;


DROP TABLE IF EXISTS store_fulfill_02;


DROP TABLE IF EXISTS store_fulfill_03;


--------------------------------------------------


CREATE TEMPORARY TABLE IF NOT EXISTS order_ship_01
CLUSTER BY sku_num, order_tmstp_pacific, destination_node_num
AS
SELECT sku_num,
 rms_sku_num,
 order_tmstp_pacific,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 sku_type,
 delivery_method_code,
  CASE
  WHEN LOWER(delivery_method_code) = LOWER('PICK')
  THEN destination_node_num
  ELSE NULL
  END AS destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_line_quantity,
 order_line_current_amount,
  CASE
  WHEN COALESCE(order_line_promotion_discount_amount, 0) - COALESCE(order_line_employee_discount_amount, 0) > 0
  THEN 1
  ELSE 0
  END AS promo_flag,
 source_store_num,
 source_channel_code,
 source_channel_country_code,
 source_platform_code,
 order_line_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE shipped_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY sku_num,
 rms_sku_num,
 order_tmstp_pacific,
 shipped_tmstp_pacific,
 shipped_date_pacific,
 sku_type,
 delivery_method_code,
 destination_node_num,
 partner_relationship_type_code,
 order_num,
 order_line_num,
 order_line_quantity,
 order_line_current_amount,
 promo_flag,
 source_store_num,
 source_channel_code,
 source_channel_country_code,
 source_platform_code,
 order_line_id;


--COLLECT STATS 	PRIMARY INDEX (SKU_NUM, ORDER_TMSTP_PACIFIC) 	,INDEX (DESTINATION_NODE_NUM) 		ON ORDER_SHIP_01


CREATE TEMPORARY TABLE IF NOT EXISTS order_ship_02
---CLUSTER BY sku_num, price_store_num, selling_channel, order_tmstp_pacific
AS
SELECT a.sku_num,
 a.order_tmstp_pacific,
 a.shipped_tmstp_pacific,
 a.destination_node_num,
 a.intent_store_num,
 a.prc_store_num,
 a.delivery_method_code,
 a.order_line_quantity,
 a.order_line_current_amount,
 a.source_channel_code,
 a.shipped_date_pacific,
 a.promo_flag,
 a.order_line_id,
 b.price_store_num,
 b.selling_channel
FROM (SELECT rms_sku_num AS sku_num,
   order_tmstp_pacific,
   shipped_tmstp_pacific,
   destination_node_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 5405
    WHEN destination_node_num IS NOT NULL
    THEN destination_node_num
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('US')
    THEN 808
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code
       ) = LOWER('CA')
    THEN 867
    WHEN destination_node_num IS NULL AND LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS intent_store_num,
    CASE
    WHEN LOWER(partner_relationship_type_code) = LOWER('ECONCESSION')
    THEN 808
    WHEN LOWER(source_platform_code) = LOWER('POS')
    THEN source_store_num
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('US')
    THEN 808
    WHEN LOWER(source_channel_code) = LOWER('FULL_LINE') AND LOWER(source_channel_country_code) = LOWER('CA')
    THEN 867
    WHEN LOWER(source_channel_code) = LOWER('RACK')
    THEN 828
    ELSE NULL
    END AS prc_store_num,
   delivery_method_code,
   order_line_quantity,
   order_line_current_amount,
   source_channel_code,
   shipped_date_pacific,
   promo_flag,
   order_line_id
  FROM order_ship_01 AS a) AS a
 LEFT JOIN store_01 AS b ON a.prc_store_num = b.store_num;




CREATE TEMPORARY TABLE IF NOT EXISTS order_ship_03 AS
SELECT
    a.SKU_NUM,
    a.ORDER_TMSTP_PACIFIC,
    a.SHIPPED_TMSTP_PACIFIC,
    a.DESTINATION_NODE_NUM,
    a.INTENT_STORE_NUM,
    a.DELIVERY_METHOD_CODE,
    a.ORDER_LINE_QUANTITY,
    a.ORDER_LINE_CURRENT_AMOUNT,
    a.SOURCE_CHANNEL_CODE,
    a.SHIPPED_DATE_PACIFIC,
    a.PRICE_STORE_NUM,
    a.ORDER_LINE_ID,
    b.RMS_SKU_NUM,
    b.REGULAR_PRICE_AMT,
    b.selling_retail_price_amt AS current_price_amt,
    b.selling_retail_currency_code AS current_price_currency_code,
    CASE
        WHEN PROMO_FLAG = 1 AND b.selling_retail_price_type_code = 'REGULAR' THEN 'P'
        WHEN b.ownership_retail_price_type_code = 'CLEARANCE' THEN 'C'
        ELSE
            CASE
                WHEN b.selling_retail_price_type_code = 'CLEARANCE' THEN 'C'
                WHEN b.selling_retail_price_type_code = 'REGULAR' THEN 'R'
                WHEN b.selling_retail_price_type_code = 'PROMOTION' THEN 'P'
                ELSE b.selling_retail_price_type_code
            END
    END AS current_price_type
FROM order_ship_02 a
LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_price_timeline_dim b
    ON a.SKU_NUM = b.RMS_SKU_NUM
    AND a.price_store_num =   cast(trunc(cast(b.store_num as float64)) as int64)
    AND a.selling_channel = b.selling_channel
    AND a.ORDER_TMSTP_PACIFIC BETWEEN b.EFF_BEGIN_TMSTP AND TIMESTAMP_SUB(b.EFF_END_TMSTP, INTERVAL 1 MILLISECOND)
GROUP BY
    a.SKU_NUM,
    a.ORDER_TMSTP_PACIFIC,
    a.SHIPPED_TMSTP_PACIFIC,
    a.DESTINATION_NODE_NUM,
    a.INTENT_STORE_NUM,
    a.DELIVERY_METHOD_CODE,
    a.ORDER_LINE_QUANTITY,
    a.ORDER_LINE_CURRENT_AMOUNT,
    a.SOURCE_CHANNEL_CODE,
    a.SHIPPED_DATE_PACIFIC,
    a.PRICE_STORE_NUM,
    a.ORDER_LINE_ID,
    b.RMS_SKU_NUM,
    b.REGULAR_PRICE_AMT,
    b.selling_retail_price_amt,
    b.selling_retail_currency_code,
    b.selling_retail_price_type_code,
    b.ownership_retail_price_type_code,
    a.PROMO_FLAG;




CREATE TEMPORARY TABLE IF NOT EXISTS order_ship_final
CLUSTER BY row_id
AS
SELECT sku_num || '_' || FORMAT_DATE('%F', shipped_date_pacific) || '_' || TRIM(FORMAT('%11d', intent_store_num)) || '_'
        || TRIM(COALESCE(SUBSTR(FORMAT('%.2f', regular_price_amt), 1, 10), '')) || '_' || TRIM(COALESCE(SUBSTR(FORMAT('%.2f'
        , current_price_amt), 1, 10), '')) || '_' || COALESCE(current_price_type, '') AS row_id,
 sku_num,
 shipped_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type,
 SUM(order_line_current_amount) AS shipped_dollars,
 SUM(order_line_quantity) AS shipped_units
FROM order_ship_03
WHERE LOWER(sku_num) <> LOWER('')
GROUP BY row_id,
 sku_num,
 shipped_date_pacific,
 intent_store_num,
 regular_price_amt,
 current_price_amt,
 current_price_currency_code,
 current_price_type;


--COLLECT STATS 	PRIMARY INDEX (ROW_ID) 	,COLUMN (ROW_ID) 		ON ORDER_SHIP_FINAL


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'ORDER_SHIP_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM order_ship_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp =  cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt,
 cast(b.process_timestamp as timestamp),
 b.process_timestamp_tz);


------ DROP INTERMEDIATE ORDER SHIP TABLES -----


DROP TABLE IF EXISTS order_ship_01;


DROP TABLE IF EXISTS order_ship_02;


DROP TABLE IF EXISTS order_ship_03;


--------------------------------------------------


--2023-12-14 derived field from Logical fact table


--2023-12-14 direct field from Logical fact table


-- Added location_type to associate dropship inventory


--2023-12-14 changed PHYSICAL_FACT to LOGICAL_FACT table


--2024-02-01 Logical Fact has data from 2022-10-26


CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_log
----CLUSTER BY rms_sku_id, snapshot_date, location_id
AS
SELECT rms_sku_id,
 snapshot_date,
 DATE_ADD(snapshot_date, INTERVAL 1 DAY) AS snapshot_date_boh,
 value_updated_time,
 location_id,
  COALESCE(stock_on_hand_qty, 0) - COALESCE(unavailable_qty, 0) AS immediately_sellable_qty,
 COALESCE(stock_on_hand_qty, 0) AS stock_on_hand_qty,
 COALESCE(unavailable_qty, 0) AS nonsellable_qty,
 location_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_logical_fact
WHERE snapshot_date BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND snapshot_date >= DATE '2022-10-26'
GROUP BY rms_sku_id,
 snapshot_date,
 snapshot_date_boh,
 value_updated_time,
 location_id,
 immediately_sellable_qty,
 stock_on_hand_qty,
 nonsellable_qty,
 location_type;


--COLLECT STATS 	PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) 	,COLUMN (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) 	,COLUMN (RMS_SKU_ID) 	,COLUMN (LOCATION_ID) 		ON SKU_SOH_LOG


---2024-02-01 Using Physical Fact table for data before 2022-10-26


--2024-02-01 Using Physical Fact table for data before 2022-10-26


CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_phy
----CLUSTER BY rms_sku_id, snapshot_date, location_id
AS
SELECT rms_sku_id,
 snapshot_date,
 DATE_ADD(snapshot_date, INTERVAL 1 DAY) AS snapshot_date_boh,
 value_updated_time,
 location_id,
 COALESCE(immediately_sellable_qty, 0) AS immediately_sellable_qty,
 COALESCE(stock_on_hand_qty, 0) AS stock_on_hand_qty,
  COALESCE(stock_on_hand_qty, 0) - COALESCE(immediately_sellable_qty, 0) AS nonsellable_qty,
 location_type
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.inventory_stock_quantity_by_day_physical_fact
WHERE snapshot_date BETWEEN  {{params.start_date}} AND {{params.end_date}}
 AND snapshot_date < DATE '2022-10-26'
GROUP BY rms_sku_id,
 snapshot_date,
 snapshot_date_boh,
 value_updated_time,
 location_id,
 immediately_sellable_qty,
 stock_on_hand_qty,
 nonsellable_qty,
 location_type;


--COLLECT STATS PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) ,COLUMN (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) ,COLUMN (RMS_SKU_ID) ,COLUMN (LOCATION_ID) ON SKU_SOH_PHY


-----------sku_soh_01------------


--2024-02-01 Combining Physical & Logical Fact to create SKU_SOH_01 table


CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_01
---CLUSTER BY rms_sku_id, snapshot_date, location_id
AS
SELECT *
FROM sku_soh_log
UNION DISTINCT
SELECT *
FROM sku_soh_phy AS a
WHERE location_type IS NULL;


--COLLECT STATS PRIMARY INDEX (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) ,COLUMN (RMS_SKU_ID,SNAPSHOT_DATE,LOCATION_ID) ,COLUMN (RMS_SKU_ID) ,COLUMN (LOCATION_ID) ON SKU_SOH_01


-- NEW --


--WHERE a.location_type IS NULL                            ----2023-12-14 location_type has no Null values in logical_fact table


CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_02
---CLUSTER BY rms_sku_id, store_type_code, store_country_code, snapshot_date
AS
SELECT a.rms_sku_id,
 b.store_type_code,
 b.store_type_code_new,
 b.store_country_code,
 b.selling_channel,
 a.snapshot_date,
 a.snapshot_date_boh,
 a.value_updated_time,
 a.location_id,
 a.immediately_sellable_qty,
 a.stock_on_hand_qty,
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
 a.value_updated_time,
 a.location_id,
 a.immediately_sellable_qty,
 a.stock_on_hand_qty,
 a.nonsellable_qty,
 a.location_type;




CREATE TEMPORARY TABLE IF NOT EXISTS sku_soh_final AS
SELECT
    CONCAT(
        a.rms_sku_id, '_', FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(a.snapshot_date)), '_', TRIM(a.location_id), '_',
        TRIM(COALESCE(CAST(a.REGULAR_PRICE_AMT AS STRING), '')), '_',
        TRIM(COALESCE(CAST(a.CURRENT_PRICE_AMT AS STRING), '')), '_',
        COALESCE(a.current_price_type, '')
    ) AS row_id,
    CONCAT(
        a.rms_sku_id, '_', FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(a.snapshot_date_boh)), '_', TRIM(a.location_id), '_',
        TRIM(COALESCE(CAST(a.REGULAR_PRICE_AMT AS STRING), '')), '_',
        TRIM(COALESCE(CAST(a.CURRENT_PRICE_AMT AS STRING), '')), '_',
        COALESCE(a.current_price_type, '')
    ) AS row_id_boh,
    a.rms_sku_id,
    a.snapshot_date,
    a.snapshot_date_boh,
    a.value_updated_time,
    a.location_id,
    a.immediately_sellable_qty,
    a.immediately_sellable_qty * b.ownership_price_amt AS immediately_sellable_dollars,
    a.stock_on_hand_qty,
    a.stock_on_hand_qty * b.ownership_price_amt AS stock_on_hand_dollars,
    a.nonsellable_qty,
    a.store_type_code,
    a.store_country_code,
    a.current_price_type,
    a.CURRENT_PRICE_AMT,
    a.REGULAR_PRICE_AMT,
    a.location_type
FROM (
    SELECT
        a.rms_sku_id,
        a.snapshot_date,
        a.snapshot_date_boh,
        a.value_updated_time,
        a.location_id,
        a.immediately_sellable_qty,
        a.stock_on_hand_qty,
        a.nonsellable_qty,
        a.store_type_code,
        a.store_country_code,
        MIN(b.ownership_price_type) AS current_price_type,
        MIN(b.current_price_amt) AS CURRENT_PRICE_AMT,
        b.REGULAR_PRICE_AMT,
        a.location_type
    FROM sku_soh_02 a
    LEFT JOIN price_01 b
        ON a.rms_sku_id = b.rms_sku_num
        AND a.store_type_code_new = b.store_type_code
        AND a.store_country_code = b.channel_country
        AND a.selling_channel = b.selling_channel
        AND TIMESTAMP(a.snapshot_date) BETWEEN timestamp( b.EFF_BEGIN_TMSTP) AND cast(TIMESTAMP_SUB(b.EFF_END_TMSTP, INTERVAL 1 MILLISECOND) as timestamp)
    WHERE a.immediately_sellable_qty <> 0
       OR a.stock_on_hand_qty <> 0
       OR a.nonsellable_qty <> 0
    GROUP BY
        a.rms_sku_id,
        a.snapshot_date,
        a.snapshot_date_boh,
        a.value_updated_time,
        a.location_id,
        a.immediately_sellable_qty,
        a.stock_on_hand_qty,
        a.nonsellable_qty,
        a.store_type_code,
        a.store_country_code,
        b.REGULAR_PRICE_AMT,
        a.location_type
) a left join price_01 b  ON a.rms_sku_id = b.rms_sku_num;







MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'SKU_SOH_FINAL' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM sku_soh_final
 GROUP BY row_id
 HAVING COUNT(row_id) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt,
cast( b.process_timestamp as timestamp),
b.process_timestamp_tz);


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id_boh AS row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id_boh , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'SKU_SOH_FINAL_BOH' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
   `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM sku_soh_final
 GROUP BY row_id
 HAVING COUNT(row_id_boh) > 1) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt,
cast(b.process_timestamp as timestamp),
b.process_timestamp_tz);


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'SKU_SOH_FINAL_BAD' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
   `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM sku_soh_final
 WHERE stock_on_hand_dollars > 9999999999.99) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt,
cast( b.process_timestamp as timestamp),
b.process_timestamp_tz);


MERGE INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs AS a
USING (SELECT row_id_boh AS row_id,
  PARSE_DATE('%F', REGEXP_EXTRACT_ALL(row_id_boh , r'[^_]+') [OFFSET ( 1 ) ]) AS bus_dt,
  'SKU_SOH_FINAL_BAD_BOH' AS dataset_issue,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS DATE) AS process_dt,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS process_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_timestamp_tz
 FROM sku_soh_final
 WHERE stock_on_hand_dollars > 9999999999.99) AS b
ON LOWER(a.row_id) = LOWER(b.row_id) AND a.bus_dt = b.bus_dt AND LOWER(a.dataset_issue) = LOWER(b.dataset_issue)
WHEN MATCHED THEN UPDATE SET
 process_dt = b.process_dt,
 process_timestamp = cast(b.process_timestamp as timestamp)
WHEN NOT MATCHED THEN INSERT VALUES(b.row_id, b.bus_dt, b.dataset_issue, b.process_dt, cast(b.process_timestamp as timestamp),
b.process_timestamp_tz);


DROP TABLE IF EXISTS sku_soh_log;


DROP TABLE IF EXISTS sku_soh_phy;


DROP TABLE IF EXISTS sku_soh_01;


DROP TABLE IF EXISTS sku_soh_02;


--------------------------------------------------


CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline_union
--CLUSTER BY row_id
AS
((((((SELECT row_id
      FROM trans_base_final
      UNION DISTINCT
      SELECT row_id
      FROM demand_base_final)
     UNION DISTINCT
     SELECT row_id
     FROM demand_dropship_final)
    UNION DISTINCT
    SELECT row_id
    FROM demand_cancel_final)
   UNION DISTINCT
   SELECT row_id
   FROM order_ship_final)
  UNION DISTINCT
  SELECT row_id
  FROM sku_soh_final)
 UNION DISTINCT
 SELECT row_id_boh AS row_id
 FROM sku_soh_final)
UNION DISTINCT
SELECT row_id
FROM store_fulfill_final;


--COLLECT STATS COLUMN(row_id) ON final_baseline_union


CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline_01 AS
SELECT
    c.row_id,
    CASE
        WHEN SPLIT(c.row_id, '_')[OFFSET(0)] IS NULL
             OR SPLIT(c.row_id, '_')[OFFSET(1)] IS NULL
             OR SPLIT(c.row_id, '_')[OFFSET(2)] IS NULL
             OR SPLIT(c.row_id, '_')[OFFSET(5)] IS NULL
        THEN 'Y'
        ELSE 'N'
    END AS INCOMPLETE_IND
FROM (
    SELECT a.row_id
    FROM final_baseline_union a
    LEFT JOIN (
        SELECT ROW_ID
        FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day_error_logs
        WHERE PROCESS_DT = CURRENT_DATE()
        GROUP BY ROW_ID
    ) b
    ON a.ROW_ID = b.ROW_ID
    WHERE b.ROW_ID IS NULL
) c left join  final_baseline_union a ON a.ROW_ID = c.ROW_ID
WHERE c.ROW_ID IS NOT NULL
    AND DATE(SPLIT(a.row_id, '_')[OFFSET(1)]) BETWEEN {{params.start_date}} AND {{params.end_date}};


	-- Create temporary table final_baseline_02
CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline_02 AS (
  SELECT
    a.row_id,
    SPLIT(a.row_id, '_')[OFFSET(0)] AS sku_num,
    CAST(SPLIT(a.row_id, '_')[OFFSET(1)] AS DATE) AS day_dt,
    SPLIT(a.row_id, '_')[OFFSET(5)] AS price_type,
    SPLIT(a.row_id, '_')[OFFSET(2)] AS loc_idnt,
    a.incomplete_ind
  FROM final_baseline_01 a
);

-- Create final_baseline table
CREATE TEMPORARY TABLE IF NOT EXISTS final_baseline AS (
  SELECT
    a.row_id,
    a.sku_num,
    a.day_dt,
    a.price_type,
    a.loc_idnt,
    b.sls_units AS sales_units,
    b.net_sls AS sales_dollars,
    b.return_units AS return_units,
    b.return AS return_dollars,
    b.cost_of_goods_sold,
    CASE
      WHEN any_value(st.channel_num ) IN (110, 111, 210, 211)
        THEN COALESCE(b.sls_units, 0) + COALESCE(b.return_units, 0)
      ELSE h.demand_units
    END AS demand_units,
    CASE
      WHEN any_value(st.channel_num) IN (110, 111, 210, 211)
        THEN COALESCE(b.net_sls, 0) + COALESCE(b.return, 0)
      ELSE h.demand_dollars
    END AS demand_dollars,
    i.demand_cancel_units,
    i.demand_cancel_dollars,
    j.demand_dropship_units,
    j.demand_dropship_dollars,
    k.shipped_units,
    k.shipped_dollars,
    m.store_fulfill_units,
    m.store_fulfill_dollars,
    c.stock_on_hand_qty AS eoh_units,
    c.stock_on_hand_dollars AS eoh_dollars,
    n.stock_on_hand_qty AS boh_units,
    n.stock_on_hand_dollars AS boh_dollars,
    c.nonsellable_qty AS nonsellable_units,
    NULL AS receipt_units,
    NULL AS receipt_dollars,
    NULL AS receipt_po_units,
    NULL AS receipt_po_dollars,
    NULL AS receipt_pah_units,
    NULL AS receipt_pah_dollars,
    NULL AS receipt_dropship_units,
    NULL AS receipt_dropship_dollars,
    NULL AS receipt_reservestock_units,
    NULL AS receipt_reservestock_dollars,
    COALESCE(
     any_value( b.current_price_amt),
     any_value( c.current_price_amt),
     any_value( h.current_price_amt),
     any_value( i.current_price_amt),
      any_value(j.current_price_amt),
      any_value(k.current_price_amt),
     any_value( m.current_price_amt),
     any_value( n.current_price_amt)
    ) AS current_price,
    COALESCE(
      any_value(b.regular_price_amt),
     any_value( c.regular_price_amt),
      any_value(h.regular_price_amt),
      any_value(i.regular_price_amt),
     any_value( j.regular_price_amt),
      any_value(k.regular_price_amt),
      any_value(m.regular_price_amt),
     any_value( n.regular_price_amt)
    ) AS regular_price,
    a.incomplete_ind,
    TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS update_timestamp,
    TIMESTAMP(CURRENT_DATETIME('PST8PDT')) AS process_timestamp
  FROM final_baseline_02 a
  LEFT JOIN trans_base_final b
    ON a.row_id = b.row_id
  LEFT JOIN sku_soh_final c
    ON a.row_id = c.row_id
  LEFT JOIN demand_base_final h
    ON a.row_id = h.row_id
  LEFT JOIN demand_cancel_final i
    ON a.row_id = i.row_id
  LEFT JOIN demand_dropship_final j
    ON a.row_id = j.row_id
  LEFT JOIN order_ship_final k
    ON a.row_id = k.row_id
  LEFT JOIN store_fulfill_final m
    ON a.row_id = m.row_id
  LEFT JOIN sku_soh_final n
    ON a.row_id = n.row_id_boh
  LEFT JOIN prd_nap_usr_vws.store_dim st
    ON
	--CAST(a.loc_idnt AS INT64)
cast(trunc(cast(a.loc_idnt as float64)) as int64)	= st.store_num
  GROUP BY a.row_id, a.sku_num, a.day_dt, a.price_type, a.loc_idnt,
           b.sls_units, b.net_sls, b.return_units, b.return,
           b.cost_of_goods_sold, h.demand_units, h.demand_dollars,
           i.demand_cancel_units, i.demand_cancel_dollars,
           j.demand_dropship_units, j.demand_dropship_dollars,
           k.shipped_units, k.shipped_dollars, m.store_fulfill_units,
           m.store_fulfill_dollars, c.stock_on_hand_qty, c.stock_on_hand_dollars,
           n.stock_on_hand_qty, n.stock_on_hand_dollars,
           c.nonsellable_qty, a.incomplete_ind
);

-- Perform a MERGE operation into SKU_LOC_PRICETYPE_DAY
MERGE INTO
`{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day AS a
USING
  final_baseline AS b
ON
  a.ROW_ID = b.ROW_ID
  AND a.DAY_DT = b.DAY_DT
WHEN MATCHED THEN
  UPDATE SET
    SALES_UNITS = 	cast(trunc(cast(b.SALES_UNITS as float64)) as int64),
    SALES_DOLLARS = b.SALES_DOLLARS,
    RETURN_UNITS =
	cast(trunc(cast(b.RETURN_UNITS as float64)) as int64),
    RETURN_DOLLARS = b.RETURN_DOLLARS,
    DEMAND_UNITS =
	cast(trunc(cast(b.DEMAND_UNITS as float64)) as int64),
    DEMAND_DOLLARS = b.DEMAND_DOLLARS,
    DEMAND_CANCEL_UNITS =
	cast(trunc(cast(b.DEMAND_CANCEL_UNITS as float64)) as int64),
    DEMAND_CANCEL_DOLLARS = b.DEMAND_CANCEL_DOLLARS,
    DEMAND_DROPSHIP_UNITS =
	cast(trunc(cast(b.DEMAND_DROPSHIP_UNITS as float64)) as int64),
    DEMAND_DROPSHIP_DOLLARS = b.DEMAND_DROPSHIP_DOLLARS,
    SHIPPED_UNITS =
	cast(trunc(cast(b.SHIPPED_UNITS as float64)) as int64)
	,
    SHIPPED_DOLLARS = b.SHIPPED_DOLLARS,
    STORE_FULFILL_UNITS =
	cast(trunc(cast(b.STORE_FULFILL_UNITS as float64)) as int64),
    STORE_FULFILL_DOLLARS = b.STORE_FULFILL_DOLLARS,
    EOH_UNITS =
	cast(trunc(cast(b.EOH_UNITS as float64)) as int64),
    EOH_DOLLARS = b.EOH_DOLLARS,
    BOH_UNITS =
	cast(trunc(cast(b.BOH_UNITS as float64)) as int64),
    BOH_DOLLARS = b.BOH_DOLLARS,
    NONSELLABLE_UNITS =
	cast(trunc(cast(b.NONSELLABLE_UNITS as float64)) as int64),
    RECEIPT_UNITS =
	cast(trunc(cast(b.RECEIPT_UNITS as float64)) as int64)
	,
    RECEIPT_DOLLARS = b.RECEIPT_DOLLARS,
    RECEIPT_PO_UNITS = b.RECEIPT_PO_UNITS,
    RECEIPT_PO_DOLLARS = b.RECEIPT_PO_DOLLARS,
    RECEIPT_PAH_UNITS = b.RECEIPT_PAH_UNITS,
    RECEIPT_PAH_DOLLARS = b.RECEIPT_PAH_DOLLARS,
    RECEIPT_DROPSHIP_UNITS = b.RECEIPT_DROPSHIP_UNITS,
    RECEIPT_DROPSHIP_DOLLARS = b.RECEIPT_DROPSHIP_DOLLARS,
    RECEIPT_RESERVESTOCK_UNITS = b.RECEIPT_RESERVESTOCK_UNITS,
    RECEIPT_RESERVESTOCK_DOLLARS = b.RECEIPT_RESERVESTOCK_DOLLARS,
    CURRENT_PRICE = b.CURRENT_PRICE,
    REGULAR_PRICE = b.REGULAR_PRICE,
    INCOMPLETE_IND = b.INCOMPLETE_IND,
    UPDATE_TIMESTAMP = b.UPDATE_TIMESTAMP,
    COST_OF_GOODS_SOLD = b.COST_OF_GOODS_SOLD
WHEN NOT MATCHED THEN
  INSERT (
    ROW_ID,
    sku_idnt,
    DAY_DT,
    PRICE_TYPE,
    LOC_IDNT,
    SALES_UNITS,
    SALES_DOLLARS,
    RETURN_UNITS,
    RETURN_DOLLARS,
    DEMAND_UNITS,
    DEMAND_DOLLARS,
    DEMAND_CANCEL_UNITS,
    DEMAND_CANCEL_DOLLARS,
    DEMAND_DROPSHIP_UNITS,
    DEMAND_DROPSHIP_DOLLARS,
    SHIPPED_UNITS,
    SHIPPED_DOLLARS,
    STORE_FULFILL_UNITS,
    STORE_FULFILL_DOLLARS,
    EOH_UNITS,
    EOH_DOLLARS,
    BOH_UNITS,
    BOH_DOLLARS,
    NONSELLABLE_UNITS,
    RECEIPT_UNITS,
    RECEIPT_DOLLARS,
    RECEIPT_PO_UNITS,
    RECEIPT_PO_DOLLARS,
    RECEIPT_PAH_UNITS,
    RECEIPT_PAH_DOLLARS,
    RECEIPT_DROPSHIP_UNITS,
    RECEIPT_DROPSHIP_DOLLARS,
    RECEIPT_RESERVESTOCK_UNITS,
    RECEIPT_RESERVESTOCK_DOLLARS,
    CURRENT_PRICE,
    REGULAR_PRICE,
    INCOMPLETE_IND,
    UPDATE_TIMESTAMP,
    PROCESS_TIMESTAMP,
    COST_OF_GOODS_SOLD
  ) VALUES (
    b.ROW_ID,
    b.SKU_NUM,
    b.DAY_DT,
    b.PRICE_TYPE,
    ---cast(b.LOC_IDNT as int64),
	cast(trunc(cast(b.LOC_IDNT as float64)) as int64),
    --cast(b.SALES_UNITS as int64),
	cast(trunc(cast(b.SALES_UNITS as float64)) as int64),
    b.SALES_DOLLARS,
    --cast(b.RETURN_UNITS as int64),
	cast(trunc(cast(b.RETURN_UNITS as float64)) as int64),
    b.RETURN_DOLLARS,
    --cast(b.DEMAND_UNITS as int64),
	cast(trunc(cast(b.DEMAND_UNITS as float64)) as int64),

    b.DEMAND_DOLLARS,
    ---cast(b.DEMAND_CANCEL_UNITS as int64),
	cast(trunc(cast(b.DEMAND_CANCEL_UNITS as float64)) as int64) ,

    b.DEMAND_CANCEL_DOLLARS,
    cast(b.DEMAND_DROPSHIP_UNITS as int64),
    b.DEMAND_DROPSHIP_DOLLARS,
	cast(trunc(cast(b.SHIPPED_UNITS as float64)) as int64) ,
   --cast( b.SHIPPED_UNITS as int64),
    b.SHIPPED_DOLLARS,
	cast(trunc(cast(b.STORE_FULFILL_UNITS as float64)) as int64) ,
    --cast(b.STORE_FULFILL_UNITS as int64),
    b.STORE_FULFILL_DOLLARS,
    --cast(b.EOH_UNITS as int64),

	cast(trunc(cast(b.EOH_UNITS as float64)) as int64),
    b.EOH_DOLLARS,
    b.BOH_UNITS,
    b.BOH_DOLLARS,
    b.NONSELLABLE_UNITS,
    b.RECEIPT_UNITS,
    b.RECEIPT_DOLLARS,
    b.RECEIPT_PO_UNITS,
    b.RECEIPT_PO_DOLLARS,
    b.RECEIPT_PAH_UNITS,
    b.RECEIPT_PAH_DOLLARS,
    b.RECEIPT_DROPSHIP_UNITS,
    b.RECEIPT_DROPSHIP_DOLLARS,
    b.RECEIPT_RESERVESTOCK_UNITS,
    b.RECEIPT_RESERVESTOCK_DOLLARS,
    b.CURRENT_PRICE,
    b.REGULAR_PRICE,
    b.INCOMPLETE_IND,
    b.UPDATE_TIMESTAMP,
    b.PROCESS_TIMESTAMP,
   b.COST_OF_GOODS_SOLD

  );


  -- Drop tables in BigQuery
DROP TABLE IF EXISTS trans_base_final;
DROP TABLE IF EXISTS demand_base_final;
DROP TABLE IF EXISTS demand_dropship_final;
DROP TABLE IF EXISTS demand_cancel_final;
DROP TABLE IF EXISTS store_fulfill_final;
DROP TABLE IF EXISTS order_ship_final;
DROP TABLE IF EXISTS sku_soh_final;
DROP TABLE IF EXISTS final_baseline_union;
DROP TABLE IF EXISTS final_baseline_01;
DROP TABLE IF EXISTS final_baseline_02;
DROP TABLE IF EXISTS final_baseline;
DROP TABLE IF EXISTS sku_01;
DROP TABLE IF EXISTS store_01;
DROP TABLE IF EXISTS price_01;


-- Create a temporary table in BigQuery
CREATE TEMPORARY TABLE IF NOT EXISTS wac_01 AS
SELECT
    sku_num,
    location_num,
    weighted_average_cost_currency_code,
    weighted_average_cost,
    eff_begin_dt,
    eff_end_dt,
    ROW_NUMBER() OVER (PARTITION BY sku_num, location_num ORDER BY eff_end_dt DESC) AS rn
FROM
   `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.weighted_average_cost_date_dim
WHERE
    eff_end_dt >= DATE('2024-02-01');


-- Create a temporary table in BigQuery
CREATE TEMPORARY TABLE IF NOT EXISTS sku_loc_01 AS
SELECT
    sku_idnt,
    day_dt,
    loc_idnt,
    row_id
FROM
    `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day slpdv
WHERE
    day_dt BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY
    sku_idnt, day_dt, loc_idnt, row_id;

-- Create a temporary table (similar to Teradata's volatile table)
CREATE TEMPORARY TABLE IF NOT EXISTS sku_loc_final AS
SELECT
    sku_idnt,
    day_dt,
    loc_idnt,
    row_id
FROM
    sku_loc_01
GROUP BY
    sku_idnt, day_dt, loc_idnt, row_id;


	-- Create a temporary table (equivalent to Teradata's volatile table)
CREATE TEMPORARY TABLE IF NOT EXISTS wac_final AS
SELECT
    a.row_id,
    a.sku_idnt,
    a.day_dt,
    a.loc_idnt,
    b.weighted_average_cost,
    b.weighted_average_cost AS weighted_average_cost_new
FROM
    sku_loc_final a
INNER JOIN
    wac_01 b
    ON a.sku_idnt = b.sku_num
    AND a.loc_idnt =
	cast(trunc(cast(b.location_num as float64)) as int64)
WHERE
    a.day_dt BETWEEN b.eff_begin_dt AND b.eff_end_dt - INTERVAL 1 DAY
GROUP BY
    a.row_id, a.sku_idnt, a.day_dt, a.loc_idnt, b.weighted_average_cost
;

UPDATE
    `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day AS sku_day
SET
    sku_day.weighted_average_cost = b.weighted_average_cost_new,
    sku_day.update_timestamp = CURRENT_TIMESTAMP()
FROM
    wac_final b
WHERE
    sku_day.day_dt BETWEEN {{params.start_date}} AND {{params.end_date}}
    AND sku_day.row_id = b.row_id;


    CREATE TEMPORARY TABLE IF NOT EXISTS wac_ending AS
SELECT *
FROM wac_01
WHERE rn = 1
AND eff_end_dt <> '9999-12-31';



	CREATE TEMPORARY TABLE IF NOT EXISTS wac_ending_final AS
SELECT
    a.row_id,
    a.sku_idnt,
    a.day_dt,
    a.loc_idnt,
    b.weighted_average_cost,
    b.weighted_average_cost AS weighted_average_cost_new
FROM
    sku_loc_final a
INNER JOIN
    wac_ending b
    ON a.sku_idnt = b.sku_num
    AND a.loc_idnt =
	cast(trunc(cast(b.location_num as float64)) as int64)
WHERE
    a.day_dt BETWEEN b.eff_begin_dt AND b.eff_end_dt
GROUP BY
    1, 2, 3, 4, 5, 6;

	UPDATE
   `{{params.gcp_project_id}}`.{{params.environment_schema}}.sku_loc_pricetype_day AS sku_day
SET
    sku_day.weighted_average_cost = b.weighted_average_cost,
    sku_day.update_timestamp = TIMESTAMP(CURRENT_DATETIME('PST8PDT'))
FROM
    wac_ending_final b
WHERE
    sku_day.day_dt BETWEEN {{params.start_date}} AND {{params.end_date}}
    AND sku_day.row_id = b.row_id;


DROP TABLE IF EXISTS wac_01;
DROP TABLE IF EXISTS sku_loc_01;
DROP TABLE IF EXISTS sku_loc_final;
DROP TABLE IF EXISTS wac_final;
DROP TABLE IF EXISTS wac_ending;
DROP TABLE IF EXISTS wac_ending_final;
