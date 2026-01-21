CREATE TEMPORARY TABLE IF NOT EXISTS sf_start_date
AS
SELECT CASE
  WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE('PST8PDT')) = {{params.backfill_day_of_week}} 
  THEN 18658
  ELSE
   (EXTRACT(YEAR FROM CURRENT_DATE('PST8PDT')) - 1900) * 10000 + EXTRACT(MONTH FROM CURRENT_DATE('PST8PDT')) * 100 + EXTRACT(DAY FROM
     CURRENT_DATE('PST8PDT')) - {{params.incremental_look_back}}
  END AS start_date,
  CASE
  WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE('PST8PDT')) =  {{params.backfill_day_of_week}} 
  THEN 'backfill'
  ELSE 'incremental'
  END AS delete_range;

/*create the sales fact temp table based on the above time period*/

CREATE TEMPORARY TABLE IF NOT EXISTS sarf
CLUSTER BY global_tran_id, line_item_seq_num
AS
SELECT global_tran_id,
 line_item_seq_num,
 order_num,
 has_employee_discount,
 source_platform_code,
 promise_type_code,
 price_type,
 transaction_type,
 return_qty,
 return_usd_amt
FROM `{{params.gcp_project_id}}`.t2dl_das_sales_returns.sales_and_returns_fact
WHERE CAST(((EXTRACT(YEAR FROM business_day_date) - 1900) * 10000 + EXTRACT(MONTH FROM business_day_date) * 100) + EXTRACT(DAY FROM business_day_date) AS FLOAT64)
  BETWEEN (SELECT start_date
   FROM sf_start_date) AND (CAST((EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) - 1900) * 10000 + EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) * 100 + EXTRACT(DAY FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS FLOAT64)
   );

--load 

CREATE TEMPORARY TABLE IF NOT EXISTS vt2 AS
SELECT 
 base.business_day_date,
 base.global_tran_id,
 base.line_item_seq_num,
  CASE
  WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND base.intent_store_num
     = 828
  THEN 828
  WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND LOWER(base.line_item_net_amt_currency_code
     ) = LOWER('CAD')
  THEN 867
  WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) OR base.intent_store_num
     = 5405
  THEN 808
  ELSE base.intent_store_num
  END AS store_number,
 base.acp_id,
CASE 
  WHEN base.line_net_usd_amt >= 0 THEN COALESCE(base.order_date, base.tran_date)
  ELSE base.tran_date 
END AS sale_date,
  CASE 
  WHEN base.line_net_usd_amt > 0 
  THEN CAST(TRIM(base.acp_id) || '_' || TRIM(CAST(CASE
  WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND base.intent_store_num
     = 828
  THEN 828
  WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND LOWER(base.line_item_net_amt_currency_code
     ) = LOWER('CAD')
  THEN 867
  WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) OR base.intent_store_num
     = 5405
  THEN 808
  ELSE base.intent_store_num
  END  AS STRING)) || '_' || TRIM(CAST(CASE 
  WHEN base.line_net_usd_amt >= 0 THEN COALESCE(base.order_date, base.tran_date)
  ELSE base.tran_date 
END AS STRING)) AS STRING)
  ELSE NULL 
END AS trip_id,
 base.employee_discount_flag,
 base.line_item_activity_type_desc,
 base.tran_type_code,
 base.reversal_flag,
 base.line_item_quantity,
 base.line_item_net_amt_currency_code,
 base.original_line_item_amt_currency_code,
 base.original_line_item_amt,
 base.line_net_amt,
 base.line_net_usd_amt,
  CASE
  WHEN LOWER(base.nonmerch_fee_code) = LOWER('6666')
  THEN 0
  ELSE base.line_net_usd_amt
  END AS non_gc_line_net_usd_amt,
 base.line_item_regular_price,
 base.line_item_merch_nonmerch_ind,
  CASE
  WHEN LOWER(COALESCE(base.nonmerch_fee_code, '-999')) = LOWER('6666')
  THEN 1
  ELSE 0
  END AS giftcard_flag,
 base.sku_num,
 base.upc_num,
 st.store_country_code,
  CASE
  WHEN base.intent_store_num = 5405
  THEN 1
  ELSE 0
  END AS marketplace_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS base
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CASE
   WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND base.intent_store_num
      = 828
   THEN 828
   WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) AND LOWER(base.line_item_net_amt_currency_code
      ) = LOWER('CAD')
   THEN 867
   WHEN LOWER(base.line_item_order_type) IN (LOWER('CustInitWebOrder'), LOWER('CustInitPhoneOrder')) OR base.intent_store_num
      = 5405
   THEN 808
   ELSE base.intent_store_num
   END = st.store_num
WHERE CAST(((EXTRACT(YEAR FROM base.business_day_date) - 1900) * 10000 + EXTRACT(MONTH FROM base.business_day_date) * 100) + EXTRACT(DAY FROM base.business_day_date) AS FLOAT64)
  BETWEEN (SELECT MAX(start_date)
   FROM sf_start_date) AND (CAST((EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) - 1900) * 10000 + EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) * 100 + EXTRACT(DAY FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS FLOAT64)
   );

--load 1

CREATE TEMPORARY TABLE IF NOT EXISTS usl_sales_fact
CLUSTER BY global_tran_id, line_item_seq_num
AS
SELECT base.business_day_date AS date_id,
 base.global_tran_id,
 base.line_item_seq_num,
 base.store_number,
 base.acp_id,
 psd.rms_sku_num AS sku_id,
 base.upc_num,
 base.sale_date,
 base.trip_id,
 base.employee_discount_flag,
 COALESCE(tt.transaction_type_id, 1) AS transaction_type_id,
 COALESCE(devices.device_id, 1) AS device_id,
 COALESCE(sm.ship_method_id, 1) AS ship_method_id,
 COALESCE(pt.price_type_id, 1) AS price_type_id,
 base.line_item_activity_type_desc,
 base.tran_type_code,
 base.reversal_flag,
 base.line_item_quantity,
 base.line_item_net_amt_currency_code,
 base.original_line_item_amt_currency_code,
 base.original_line_item_amt,
 base.line_net_amt,
 base.line_net_usd_amt,
 dpt.merch_dept_ind,
 base.non_gc_line_net_usd_amt,
 base.line_item_regular_price,
 COALESCE(sarf.return_qty, 0) AS units_returned,
 COALESCE(sarf.return_usd_amt, 0) AS sales_returned,
 base.giftcard_flag,
  CASE
  WHEN LOWER(dpt.merch_dept_ind) = LOWER('Y') AND LOWER(base.line_item_merch_nonmerch_ind) = LOWER('MERCH') AND dpt.division_num
     NOT IN (800, 900, 100)
  THEN 1
  ELSE 0
  END AS merch_flag,
 base.marketplace_flag
FROM vt2 AS base
 LEFT JOIN (SELECT channel_country,
   rms_sku_num,
   epm_sku_num,
   epm_style_num,
   rms_style_num,
   dept_num,
   div_desc
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw) AS psd ON LOWER(base.sku_num) = LOWER(psd.rms_sku_num) AND LOWER(base.store_country_code
    ) = LOWER(psd.channel_country)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt ON psd.dept_num = dpt.dept_num
 LEFT JOIN sarf AS sarf ON base.global_tran_id = sarf.global_tran_id AND base.line_item_seq_num = sarf.line_item_seq_num
   
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.transaction_type_dim AS tt ON LOWER(sarf.transaction_type) = LOWER(tt.transaction_type)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.price_type_dim AS pt ON LOWER(sarf.price_type) = LOWER(pt.price_type)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.ship_method_dim AS sm ON LOWER(sarf.promise_type_code) = LOWER(sm.promise_type_code)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.device_dim AS devices ON LOWER(sarf.source_platform_code) = LOWER(devices.source_platform_code)
WHERE CAST(((EXTRACT(YEAR FROM base.business_day_date) - 1900) * 10000 + EXTRACT(MONTH FROM base.business_day_date) * 100) + EXTRACT(DAY FROM base.business_day_date) AS FLOAT64)
  BETWEEN (SELECT start_date
   FROM sf_start_date) AND (CAST((EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) - 1900) * 10000 + EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) * 100 + EXTRACT(DAY FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS FLOAT64)
   )
 AND LOWER(base.sku_num) <> LOWER('151')
 AND base.sku_num IS NOT NULL
UNION ALL
SELECT base0.business_day_date AS date_id,
 base0.global_tran_id,
 base0.line_item_seq_num,
 base0.store_number,
 base0.acp_id,
 psd.rms_sku_num AS sku_id,
 base0.upc_num,
 base0.sale_date,
 base0.trip_id,
 base0.employee_discount_flag,
 COALESCE(tt0.transaction_type_id, 1) AS transaction_type_id,
 COALESCE(devices0.device_id, 1) AS device_id,
 COALESCE(sm0.ship_method_id, 1) AS ship_method_id,
 COALESCE(pt0.price_type_id, 1) AS price_type_id,
 base0.line_item_activity_type_desc,
 base0.tran_type_code,
 base0.reversal_flag,
 base0.line_item_quantity,
 base0.line_item_net_amt_currency_code,
 base0.original_line_item_amt_currency_code,
 base0.original_line_item_amt,
 base0.line_net_amt,
 base0.line_net_usd_amt,
 dpt0.merch_dept_ind,
 base0.non_gc_line_net_usd_amt,
 base0.line_item_regular_price,
 COALESCE(sarf0.return_qty, 0) AS units_returned,
 COALESCE(sarf0.return_usd_amt, 0) AS sales_returned,
 base0.giftcard_flag,
  CASE
  WHEN LOWER(dpt0.merch_dept_ind) = LOWER('Y') AND LOWER(base0.line_item_merch_nonmerch_ind) = LOWER('MERCH') AND dpt0.division_num
     NOT IN (800, 900, 100)
  THEN 1
  ELSE 0
  END AS merch_flag,
 base0.marketplace_flag
FROM vt2 AS base0
 LEFT JOIN (SELECT channel_country,
   rms_sku_num,
   epm_sku_num,
   epm_style_num,
   rms_style_num,
   dept_num,
   div_desc
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw) AS psd ON LOWER(base0.sku_num) = LOWER(psd.rms_sku_num) AND LOWER(base0.store_country_code
    ) = LOWER(psd.channel_country)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt0 ON psd.dept_num = dpt0.dept_num
 LEFT JOIN sarf AS sarf0 ON base0.global_tran_id = sarf0.global_tran_id AND base0.line_item_seq_num = sarf0.line_item_seq_num
   
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.transaction_type_dim AS tt0 ON LOWER(sarf0.transaction_type) = LOWER(tt0.transaction_type)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.price_type_dim AS pt0 ON LOWER(sarf0.price_type) = LOWER(pt0.price_type)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.ship_method_dim AS sm0 ON LOWER(sarf0.promise_type_code) = LOWER(sm0.promise_type_code)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.device_dim AS devices0 ON LOWER(sarf0.source_platform_code) = LOWER(devices0.source_platform_code
   )
WHERE CAST(((EXTRACT(YEAR FROM base0.business_day_date) - 1900) * 10000 + EXTRACT(MONTH FROM base0.business_day_date) * 100) + EXTRACT(DAY FROM base0.business_day_date) AS FLOAT64)
  BETWEEN (SELECT start_date
   FROM sf_start_date) AND (CAST((EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) - 1900) * 10000 + EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) * 100 + EXTRACT(DAY FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS FLOAT64)
   )
 AND base0.sku_num IS NULL
UNION ALL
SELECT base1.business_day_date AS date_id,
 base1.global_tran_id,
 base1.line_item_seq_num,
 base1.store_number,
 base1.acp_id,
 psd.rms_sku_num AS sku_id,
 base1.upc_num,
 base1.sale_date,
 base1.trip_id,
 base1.employee_discount_flag,
 COALESCE(tt1.transaction_type_id, 1) AS transaction_type_id,
 COALESCE(devices1.device_id, 1) AS device_id,
 COALESCE(sm1.ship_method_id, 1) AS ship_method_id,
 COALESCE(pt1.price_type_id, 1) AS price_type_id,
 base1.line_item_activity_type_desc,
 base1.tran_type_code,
 base1.reversal_flag,
 base1.line_item_quantity,
 base1.line_item_net_amt_currency_code,
 base1.original_line_item_amt_currency_code,
 base1.original_line_item_amt,
 base1.line_net_amt,
 base1.line_net_usd_amt,
 dpt1.merch_dept_ind,
 base1.non_gc_line_net_usd_amt,
 base1.line_item_regular_price,
 COALESCE(sarf1.return_qty, 0) AS units_returned,
 COALESCE(sarf1.return_usd_amt, 0) AS sales_returned,
 base1.giftcard_flag,
  CASE
  WHEN LOWER(dpt1.merch_dept_ind) = LOWER('Y') AND LOWER(base1.line_item_merch_nonmerch_ind) = LOWER('MERCH') AND dpt1.division_num
     NOT IN (800, 900, 100)
  THEN 1
  ELSE 0
  END AS merch_flag,
 base1.marketplace_flag
FROM vt2 AS base1
 LEFT JOIN (SELECT channel_country,
   rms_sku_num,
   epm_sku_num,
   epm_style_num,
   rms_style_num,
   dept_num,
   div_desc
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw) AS psd ON LOWER(base1.sku_num) = LOWER(psd.rms_sku_num) AND LOWER(base1.store_country_code
    ) = LOWER(psd.channel_country)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt1 ON psd.dept_num = dpt1.dept_num
 LEFT JOIN sarf AS sarf1 ON base1.global_tran_id = sarf1.global_tran_id AND base1.line_item_seq_num = sarf1.line_item_seq_num
   
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.transaction_type_dim AS tt1 ON LOWER(sarf1.transaction_type) = LOWER(tt1.transaction_type)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.price_type_dim AS pt1 ON LOWER(sarf1.price_type) = LOWER(pt1.price_type)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.ship_method_dim AS sm1 ON LOWER(sarf1.promise_type_code) = LOWER(sm1.promise_type_code)
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_usl.device_dim AS devices1 ON LOWER(sarf1.source_platform_code) = LOWER(devices1.source_platform_code
   )
WHERE CAST(((EXTRACT(YEAR FROM base1.business_day_date) - 1900) * 10000 + EXTRACT(MONTH FROM base1.business_day_date) * 100) + EXTRACT(DAY FROM base1.business_day_date) AS FLOAT64)
  BETWEEN (SELECT start_date
   FROM sf_start_date) AND (CAST((EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) - 1900) * 10000 + EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) * 100 + EXTRACT(DAY FROM DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY)) AS FLOAT64)
   )
 AND LOWER(base1.sku_num) = LOWER('151');


DELETE FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_fact
WHERE CAST((EXTRACT(YEAR FROM date_id) - 1900) * 10000 + EXTRACT(MONTH FROM date_id) * 100 + EXTRACT(DAY FROM date_id) AS FLOAT64) >= (SELECT start_date
            FROM sf_start_date) AND EXISTS (SELECT 1
        FROM sf_start_date
        WHERE LOWER(delete_range) = LOWER('incremental'));
        

DELETE FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_fact
WHERE EXISTS (SELECT 1
    FROM sf_start_date
    WHERE LOWER(delete_range) = LOWER('backfill'));



INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sales_fact
(SELECT date_id,
  global_tran_id,
  line_item_seq_num,
  store_number,
  acp_id,
  sku_id,
  upc_num,
  sale_date,
  trip_id,
  employee_discount_flag,
  transaction_type_id,
  device_id,
  ship_method_id,
  price_type_id,
  line_item_activity_type_desc,
  tran_type_code,
  reversal_flag,
  line_item_quantity,
  line_item_net_amt_currency_code,
  original_line_item_amt_currency_code,
  original_line_item_amt,
  line_net_usd_amt,
  merch_dept_ind,
  CAST(non_gc_line_net_usd_amt AS NUMERIC) AS non_gc_line_net_usd_amt,
  line_item_regular_price,
  units_returned,
  sales_returned,
  giftcard_flag,
  merch_flag,
  marketplace_flag,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM usl_sales_fact);