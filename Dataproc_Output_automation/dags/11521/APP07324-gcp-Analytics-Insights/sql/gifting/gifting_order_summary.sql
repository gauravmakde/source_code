 BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP07324;
DAG_ID=gifting_order_summary_11521_ACE_ENG;
ask_Name=gifting_order_summary;'*/

SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS order_detail

AS
SELECT order_num,
 order_date_pacific,
 upc_code,
 source_platform_code,
 source_channel_country_code,
 source_channel_code,
 promise_type_code,
 delivery_method_code,
  CASE
  WHEN LOWER(UPPER(promise_type_code)) = LOWER('SAME_DAY_COURIER')
  THEN 'SHIP_TO_HOME'
  WHEN LOWER(UPPER(delivery_method_code)) = LOWER('PICK')
  THEN 'BOPUS'
  WHEN destination_node_num > 0
  THEN 'SHIP_TO_STORE'
  ELSE 'SHIP_TO_HOME'
  END AS ship_type,
 destination_node_num,
  CASE
  WHEN LOWER(UPPER(gift_option_type)) = LOWER('UNKNOWN')
  THEN NULL
  ELSE UPPER(gift_option_type)
  END AS gift_option_type,
 order_line_quantity,
 order_line_amount_usd,
 gift_option_amount_usd,
 order_line_amount,
 gift_option_amount,
 ROW_NUMBER() OVER (PARTITION BY order_num, upc_code ORDER BY upc_code) AS order_seq_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact
WHERE order_date_pacific >= {{params.start_date}}
AND   order_date_pacific <= {{params.end_date}}
 AND LOWER(COALESCE(fraud_cancel_ind, '9999')) <> LOWER('Y');

-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;

-- SET _ERROR_CODE  =  0;

CREATE TEMPORARY TABLE IF NOT EXISTS gifting_detail

AS
SELECT od.order_date_pacific,
 od.order_num,
 od.source_platform_code,
 od.source_channel_country_code,
 od.source_channel_code,
 od.ship_type,
 od.destination_node_num,
 od.gift_option_type,
 SUM(od.order_line_quantity) AS order_items,
 SUM(tr.shipped_qty) AS shipped_items,
 SUM(tr.return_qty_30_day) AS return_items_30_day,
 SUM(od.order_line_amount_usd) AS order_amount_usd,
 SUM(tr.shipped_usd_sales) AS shipped_usd_sales,
 SUM(tr.return_usd_amt_30_day) AS return_usd_amount_30_day,
 SUM(od.gift_option_amount_usd) AS gift_option_amount_usd,
 SUM(od.order_line_amount) AS order_amount,
 SUM(tr.shipped_sales) AS shipped_sales,
 SUM(tr.return_amt_30_day) AS return_amount_30_day,
 SUM(od.gift_option_amount) AS gift_option_amount
FROM order_detail AS od
 LEFT JOIN (SELECT
   order_num,
   line_item_seq_num,
   upc_num,
   shipped_qty,
   shipped_usd_sales,
   shipped_sales,
    CASE
    WHEN days_to_return BETWEEN 0 AND 30
    THEN return_qty
    ELSE 0
    END AS return_qty_30_day,
    CASE
    WHEN days_to_return BETWEEN 0 AND 30
    THEN return_usd_amt
    ELSE 0
    END AS return_usd_amt_30_day,
    CASE
    WHEN days_to_return BETWEEN 0 AND 30
    THEN return_amt
    ELSE 0
    END AS return_amt_30_day,
   ROW_NUMBER() OVER (PARTITION BY order_num, upc_num ORDER BY upc_num, line_item_seq_num) AS order_seq_num
  FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact
  WHERE order_date >= {{params.start_date}}
        AND order_date <= {{params.end_date}}
) AS tr
   ON LOWER(od.order_num) = LOWER(tr.order_num) AND LOWER(LPAD(od.upc_code,
      15, '0')) = LOWER(LPAD(tr.upc_num, 15, '0')) AND od.order_seq_num = tr.order_seq_num
GROUP BY od.order_num,
 od.order_date_pacific,
 od.source_platform_code,
 od.source_channel_country_code,
 od.source_channel_code,
 od.ship_type,
 od.destination_node_num,
 od.gift_option_type;

-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;

-- SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.gifting_digital_exp_t2_schema}}.gifting_order_summary
WHERE order_date_pacific >= {{params.start_date}}
AND order_date_pacific <= {{params.end_date}};

-- EXCEPTION WHEN ERROR THEN
-- SET _ERROR_CODE  =  1;
-- SET _ERROR_MESSAGE  =  @@error.message;

-- SET _ERROR_CODE  =  0;

INSERT INTO `{{params.gcp_project_id}}`.{{params.gifting_digital_exp_t2_schema}}.gifting_order_summary
(SELECT order_date_pacific,
  order_num,
  source_platform_code,
  SUBSTR(source_channel_country_code, 1, 2) AS source_channel_country_code,
  source_channel_code,
  ship_type,
  destination_node_num,
  gift_option_type,
  order_items,
  shipped_items,
  return_items_30_day,
  ROUND(CAST(order_amount_usd AS NUMERIC), 2) AS order_amount_usd,
  shipped_usd_sales,
  CAST(return_usd_amount_30_day AS NUMERIC) AS return_usd_amount_30_day,
  ROUND(CAST(gift_option_amount_usd AS NUMERIC), 2) AS gift_option_amount_usd,
  ROUND(CAST(order_amount AS NUMERIC), 2) AS order_amount,
  shipped_sales,
  CAST(return_amount_30_day AS NUMERIC) AS return_amount_30_day,
  ROUND(CAST(gift_option_amount AS NUMERIC), 2) AS gift_option_amount,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
  
 FROM gifting_detail);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;

END;
