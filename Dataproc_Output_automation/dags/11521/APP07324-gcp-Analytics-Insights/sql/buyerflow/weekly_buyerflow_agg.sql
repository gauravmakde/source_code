BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = 'App_ID=APP09058;
DAG_ID=weekly_buyerflow_agg_11521_ACE_ENG;
---     Task_Name=weekly_buyerflow_agg;'*/
---     FOR SESSION VOLATILE;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup AS
SELECT week_idnt AS report_period,
 MIN(day_date) AS ty_start_dt,
 MAX(day_date) AS ty_end_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1461 DAY) AS pre_start_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1 DAY) AS pre_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT MAX(week_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date <= CURRENT_DATE('PST8PDT'))
GROUP BY report_period;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_purchase AS
SELECT DISTINCT CASE
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp') AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
  THEN 808
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp') AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
  THEN 867
  ELSE intent_store_num
  END AS store_num,
 acp_id,
 COALESCE(order_date, tran_date) AS sale_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
WHERE COALESCE(order_date, tran_date) >= (SELECT pre_start_dt FROM date_lookup)
 AND COALESCE(order_date, tran_date) <= (SELECT pre_end_dt FROM date_lookup)
 AND business_day_date BETWEEN (SELECT pre_start_dt FROM date_lookup) AND (SELECT DATE_ADD(pre_end_dt, INTERVAL 14 DAY) FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
 AND acp_id IS NOT NULL;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_purchase_channel AS
SELECT CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'NCOM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS channel,
 pre_purchase.acp_id,
 MAX(pre_purchase.sale_date) AS latest_sale_date
FROM pre_purchase
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str 
 ON pre_purchase.store_num = str.store_num 
 AND LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
GROUP BY channel,
 pre_purchase.acp_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_purchase AS
SELECT DISTINCT CASE
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') 
  AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp' ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
  THEN 808
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp') AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
  THEN 867
  ELSE intent_store_num
  END AS store_num,
 acp_id,
 COALESCE(order_date, tran_date) AS sale_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
WHERE COALESCE(order_date, tran_date) >= (SELECT ty_start_dt FROM date_lookup)
 AND COALESCE(order_date, tran_date) <= (SELECT ty_end_dt FROM date_lookup)
 AND business_day_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT DATE_ADD(ty_end_dt, INTERVAL 14 DAY)  FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
 AND acp_id IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_purchase_channel AS
SELECT CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'NCOM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS channel,
 ty_purchase.acp_id,
 MIN(ty_purchase.sale_date) AS first_sale_date
FROM ty_purchase
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON ty_purchase.store_num = str.store_num AND LOWER(str.business_unit_desc)
   IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
GROUP BY channel,
 ty_purchase.acp_id;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS existing_cust AS
SELECT ty.acp_id,
 ty.channel,
  CASE
  WHEN acq.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS new_customer_flg,
  CASE
  WHEN pre.latest_sale_date IS NOT NULL AND DATE_DIFF(ty.first_sale_date, pre.latest_sale_date, DAY) < 1461
  THEN 1
  ELSE 0
  END AS old_channel_flg
FROM ty_purchase_channel AS ty
 LEFT JOIN (SELECT acp_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
  WHERE aare_status_date BETWEEN (SELECT ty_start_dt  FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup)) AS acq 
  ON LOWER(ty.acp_id) = LOWER(acq.acp_id)
 LEFT JOIN pre_purchase_channel AS pre ON LOWER(ty.acp_id) = LOWER(pre.acp_id) AND LOWER(ty.channel) = LOWER(pre.channel );

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS new_cust AS
SELECT ty.acp_id,
 ty.channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS acq
 INNER JOIN ty_purchase_channel AS ty ON LOWER(acq.acp_id) = LOWER(ty.acp_id) AND LOWER(acq.aare_chnl_code) <> LOWER(ty.channel)
 INNER JOIN (SELECT acp_id
  FROM ty_purchase_channel
  GROUP BY acp_id
  HAVING COUNT(DISTINCT channel) > 1) AS multi ON LOWER(acq.acp_id) = LOWER(multi.acp_id)
WHERE acq.aare_status_date BETWEEN (SELECT ty_start_dt  FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS week_num_engaged_customer AS
SELECT (SELECT report_period  FROM date_lookup) AS report_period,
 acp_id,
 channel AS engaged_to_channel
FROM existing_cust
WHERE old_channel_flg = 0
 AND new_customer_flg = 0
UNION ALL
SELECT (SELECT report_period  FROM date_lookup),
 acp_id,
 channel AS engaged_to_channel
FROM new_cust;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--collect statistics on week_num_engaged_customer column (acp_id);
BEGIN
SET _ERROR_CODE  =  0;

DROP TABLE IF EXISTS date_lookup;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup AS
SELECT week_idnt AS report_period,
 MIN(day_date) AS ty_start_dt,
 MAX(day_date) AS ty_end_dt,
 MIN(day_date_last_year_realigned) AS ly_start_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1 DAY) AS ly_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT MAX(week_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date <= CURRENT_DATE('PST8PDT'))
GROUP BY report_period;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty AS
SELECT dtl.acp_id,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'NCOM'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS channel,
  CASE
  WHEN LOWER(dtl.channel) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'FP'
  ELSE 'OP'
  END AS brand,
 SUM(CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS gross_spend,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt < 0
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS return_amount,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666')
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS net_spend,
 COUNT(DISTINCT CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN FORMAT('%11d', st.store_num) || CAST(COALESCE(dtl.order_date, dtl.tran_date) AS STRING)
   ELSE NULL
   END) AS trips,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt > 0
   THEN dtl.line_item_quantity
   ELSE 0
   END) AS items,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt < 0
   THEN - 1 * dtl.line_item_quantity
   ELSE 0
   END) AS return_items,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt < 0
   THEN - 1 * dtl.line_item_quantity
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt > 0
   THEN dtl.line_item_quantity
   ELSE 0
   END) AS net_items
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CASE
   WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp') AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('CAD')
   THEN 867
   WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp' ) AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('USD')
   THEN 808
   ELSE dtl.intent_store_num
   END = st.store_num
WHERE CASE
   WHEN COALESCE(dtl.line_net_usd_amt, 0) >= 0
   THEN COALESCE(dtl.order_date, dtl.tran_date)
   ELSE dtl.tran_date
   END BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup)
 AND dtl.business_day_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT DATE_ADD(ty_end_dt, INTERVAL 14 DAY)
   FROM date_lookup)
 AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
 AND dtl.acp_id IS NOT NULL
GROUP BY dtl.acp_id,
 channel,
 brand;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly AS
SELECT dtl.acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON dtl.intent_store_num = st.store_num
WHERE CASE
   WHEN COALESCE(dtl.line_net_usd_amt, 0) >= 0
   THEN COALESCE(dtl.order_date, dtl.tran_date)
   ELSE dtl.tran_date
   END BETWEEN (SELECT ly_start_dt FROM date_lookup) AND (SELECT ly_end_dt FROM date_lookup)
 AND dtl.business_day_date BETWEEN (SELECT ly_start_dt FROM date_lookup) AND (SELECT DATE_ADD(ly_end_dt, INTERVAL 30 DAY)
   FROM date_lookup)
 AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
 AND dtl.acp_id IS NOT NULL;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS new_ty AS
SELECT acp_id, aare_chnl_code AS acquired_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
WHERE aare_status_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT ty_end_dt  FROM date_lookup);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS activated_ty AS
SELECT acp_id,
 activated_chnl_code AS activated_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_activated_fact
WHERE activated_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_summary AS
SELECT ty.brand,
 ty.channel,
 ty.acp_id,
 ty.gross_spend,
 ty.net_spend,
 ty.trips,
 ty.items,
  CASE
  WHEN acq.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS new_flg,
  CASE
  WHEN ly.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS retained_flg,
  CASE
  WHEN eng.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS eng_flg,
  CASE
  WHEN act.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS act_flg
FROM ty
 LEFT JOIN ly ON LOWER(ty.acp_id) = LOWER(ly.acp_id)
 LEFT JOIN week_num_engaged_customer AS eng ON LOWER(ty.acp_id) = LOWER(eng.acp_id) AND LOWER(ty.channel) = LOWER(eng.engaged_to_channel )
 LEFT JOIN new_ty AS acq ON LOWER(ty.acp_id) = LOWER(acq.acp_id)
 LEFT JOIN activated_ty AS act ON LOWER(ty.acp_id) = LOWER(act.acp_id) AND LOWER(ty.channel) = LOWER(act.activated_channel );

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
WHERE report_period = (SELECT report_period FROM date_lookup);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'all' AS report_type,
  channel,
  '' AS predicted_segment,
  '' AS country,
  '' AS loyalty_status,
  '' AS nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary
 GROUP BY channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'all' AS report_type,
  brand,
  '' AS predicted_segment,
  '' AS country,
  '' AS loyalty_status,
  '' AS nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary
 GROUP BY brand);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'all' AS report_type,
  'JWN',
  '' AS predicted_segment,
  '' AS country,
  '' AS loyalty_status,
  '' AS nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS lyl_status AS
SELECT acp_id,
  CASE
  WHEN cardmember_fl = 1
  THEN 'cardmember'
  WHEN member_fl = 1
  THEN 'member'
  ELSE 'non-member'
  END AS loyalty_status,
 rewards_level
FROM (SELECT ac.acp_id,
    CASE
    WHEN lmd.cardmember_enroll_date <= (SELECT ty_end_dt FROM date_lookup) AND (lmd.cardmember_close_date >= (SELECT ty_end_dt FROM date_lookup) OR lmd.cardmember_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS cardmember_fl,
    CASE
    WHEN CASE
        WHEN lmd.cardmember_enroll_date <= (SELECT ty_end_dt FROM date_lookup) AND (lmd.cardmember_close_date >= (SELECT ty_end_dt FROM date_lookup) OR lmd.cardmember_close_date IS NULL)
        THEN 1
        ELSE 0
        END = 0 AND lmd.member_enroll_date <= (SELECT ty_end_dt  FROM date_lookup) AND (lmd.member_close_date >= (SELECT ty_end_dt  FROM date_lookup) OR lmd.member_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS member_fl,
   MAX(CASE
     WHEN LOWER(t1.rewards_level) IN (LOWER('MEMBER'))
     THEN 1
     WHEN LOWER(t1.rewards_level) IN (LOWER('INSIDER'), LOWER('L1'))
     THEN 2
     WHEN LOWER(t1.rewards_level) IN (LOWER('INFLUENCER'), LOWER('L2'))
     THEN 3
     WHEN LOWER(t1.rewards_level) IN (LOWER('AMBASSADOR'), LOWER('L3'))
     THEN 4
     WHEN LOWER(t1.rewards_level) IN (LOWER('ICON'), LOWER('L4'))
     THEN 5
     ELSE 0
     END) AS rewards_level
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd ON LOWER(ac.acp_loyalty_id) = LOWER(lmd.loyalty_id)
   LEFT JOIN (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
    WHERE start_day_date <= (SELECT ty_end_dt  FROM date_lookup)   AND end_day_date > (SELECT ty_end_dt
       FROM date_lookup)) AS t1 ON LOWER(ac.acp_loyalty_id) = LOWER(t1.loyalty_id)
  GROUP BY ac.acp_id,
   cardmember_fl,
   member_fl) AS lyl;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_summary_seg
AS
SELECT ty.brand,
 ty.channel,
 ty.acp_id,
  CASE
  WHEN x.predicted_ct_segment IS NULL
  THEN 'unknown'
  ELSE x.predicted_ct_segment
  END AS predicted_segment,
 ac.country,
  CASE
  WHEN lyl.loyalty_status IS NULL
  THEN 'non-member'
  ELSE lyl.loyalty_status
  END AS loyalty_status,
  CASE
  WHEN lyl.rewards_level = 1
  THEN '1-member'
  WHEN lyl.rewards_level = 2
  THEN '2-insider'
  WHEN lyl.rewards_level = 3
  THEN '3-influencer'
  WHEN lyl.rewards_level = 4
  THEN '4-ambassador'
  WHEN lyl.rewards_level = 5
  THEN '5-icon'
  WHEN lyl.rewards_level = 0
  THEN CASE
   WHEN LOWER(lyl.loyalty_status) = LOWER('cardmember')
   THEN '1-member'
   WHEN LOWER(lyl.loyalty_status) = LOWER('member')
   THEN '1-member'
   WHEN LOWER(lyl.loyalty_status) = LOWER('non-member')
   THEN '0-nonmember'
   ELSE NULL
   END
  ELSE '0-nonmember'
  END AS nordy_level,
 ty.gross_spend,
 ty.net_spend,
 ty.trips,
 ty.items,
 ty.new_flg,
 ty.retained_flg,
 ty.eng_flg,
 ty.act_flg
FROM customer_summary AS ty
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment AS x ON LOWER(ty.acp_id) = LOWER(x.acp_id)
 LEFT JOIN lyl_status AS lyl ON LOWER(ty.acp_id) = LOWER(lyl.acp_id)
 LEFT JOIN (SELECT acp_id,
    CASE
    WHEN us_dma_code = - 1
    THEN NULL
    ELSE us_dma_desc
    END AS us_dma,
    CASE
    WHEN CASE
     WHEN us_dma_code = - 1
     THEN NULL
     ELSE us_dma_desc
     END IS NOT NULL
    THEN 'US'
    WHEN ca_dma_code IS NOT NULL
    THEN 'CA'
    ELSE 'unknown'
    END AS country
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer) AS ac ON LOWER(ty.acp_id) = LOWER(ac.acp_id);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--collect statistics on customer_summary_seg column (acp_id);
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'segment' AS report_type,
  channel,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary_seg
 GROUP BY channel,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'segment' AS report_type,
  brand,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary_seg
 GROUP BY brand,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'segment' AS report_type,
  'JWN',
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary_seg
 GROUP BY predicted_segment,
  country,
  loyalty_status,
  nordy_level);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS date_lookup;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT week_idnt AS report_period,
 MIN(day_date) AS ty_start_dt,
 MAX(day_date) AS ty_end_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1461 DAY) AS pre_start_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1 DAY) AS pre_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT MAX(week_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date <= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 7 DAY))
GROUP BY report_period;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS pre_purchase;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_purchase
AS
SELECT DISTINCT CASE
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
  THEN 808
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'
      ) AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
  THEN 867
  ELSE intent_store_num
  END AS store_num,
 acp_id,
 COALESCE(order_date, tran_date) AS sale_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
WHERE COALESCE(order_date, tran_date) >= (SELECT pre_start_dt  FROM date_lookup)
 AND COALESCE(order_date, tran_date) <= (SELECT pre_end_dt  FROM date_lookup)
 AND business_day_date BETWEEN (SELECT pre_start_dt  FROM date_lookup) AND (SELECT DATE_ADD(pre_end_dt, INTERVAL 14 DAY)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
 AND acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS pre_purchase_channel;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_purchase_channel
AS
SELECT CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'NCOM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS channel,
 pre_purchase.acp_id,
 MAX(pre_purchase.sale_date) AS latest_sale_date
FROM pre_purchase
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON pre_purchase.store_num = str.store_num AND LOWER(str.business_unit_desc ) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK' ), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
GROUP BY channel,
 pre_purchase.acp_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty_purchase;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_purchase
AS
SELECT DISTINCT CASE
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp'  ) AND LOWER(line_item_net_amt_currency_code) = LOWER('USD')
  THEN 808
  WHEN LOWER(line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(line_item_fulfillment_type) = LOWER('StorePickUp' ) AND LOWER(line_item_net_amt_currency_code) = LOWER('CAD')
  THEN 867
  ELSE intent_store_num
  END AS store_num,
 acp_id,
 COALESCE(order_date, tran_date) AS sale_date
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
WHERE COALESCE(order_date, tran_date) >= (SELECT ty_start_dt  FROM date_lookup)
 AND COALESCE(order_date, tran_date) <= (SELECT ty_end_dt FROM date_lookup)
 AND business_day_date BETWEEN (SELECT ty_start_dt  FROM date_lookup) AND (SELECT DATE_ADD(ty_end_dt, INTERVAL 14 DAY)
   FROM date_lookup)
 AND line_net_usd_amt > 0
 AND LOWER(tran_type_code) IN (LOWER('SALE'), LOWER('EXCH'))
 AND acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty_purchase_channel;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty_purchase_channel
AS
SELECT CASE
  WHEN LOWER(str.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'NCOM'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(str.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS channel,
 ty_purchase.acp_id,
 MIN(ty_purchase.sale_date) AS first_sale_date
FROM ty_purchase
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS str ON ty_purchase.store_num = str.store_num AND LOWER(str.business_unit_desc)
   IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'), LOWER('OFFPRICE ONLINE'), LOWER('RACK'
     ), LOWER('RACK CANADA'), LOWER('TRUNK CLUB'))
GROUP BY channel,
 ty_purchase.acp_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS existing_cust;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS existing_cust
AS
SELECT ty.acp_id,
 ty.channel,
  CASE
  WHEN acq.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS new_customer_flg,
  CASE
  WHEN pre.latest_sale_date IS NOT NULL AND DATE_DIFF(ty.first_sale_date, pre.latest_sale_date, DAY) < 1461
  THEN 1
  ELSE 0
  END AS old_channel_flg
FROM ty_purchase_channel AS ty
 LEFT JOIN (SELECT acp_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
  WHERE aare_status_date BETWEEN (SELECT ty_start_dt  FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup)) AS acq ON LOWER(ty.acp_id) = LOWER(acq.acp_id)
 LEFT JOIN pre_purchase_channel AS pre ON LOWER(ty.acp_id) = LOWER(pre.acp_id) AND LOWER(ty.channel) = LOWER(pre.channel );
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS new_cust;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS new_cust
AS
SELECT ty.acp_id,
 ty.channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS acq
 INNER JOIN ty_purchase_channel AS ty ON LOWER(acq.acp_id) = LOWER(ty.acp_id) AND LOWER(acq.aare_chnl_code) <> LOWER(ty.channel)
 INNER JOIN (SELECT acp_id
  FROM ty_purchase_channel
  GROUP BY acp_id
  HAVING COUNT(DISTINCT channel) > 1) AS multi ON LOWER(acq.acp_id) = LOWER(multi.acp_id)
WHERE acq.aare_status_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS week_num_engaged_customer;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS week_num_engaged_customer
AS
SELECT (SELECT report_period
  FROM date_lookup) AS report_period,
 acp_id,
 channel AS engaged_to_channel
FROM existing_cust
WHERE old_channel_flg = 0
 AND new_customer_flg = 0
UNION ALL
SELECT (SELECT report_period
  FROM date_lookup),
 acp_id,
 channel AS engaged_to_channel
FROM new_cust;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


--collect statistics on week_num_engaged_customer column (acp_id);
BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS date_lookup;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT week_idnt AS report_period,
 MIN(day_date) AS ty_start_dt,
 MAX(day_date) AS ty_end_dt,
 MIN(day_date_last_year_realigned) AS ly_start_dt,
 DATE_SUB(MIN(day_date), INTERVAL 1 DAY) AS ly_end_dt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_idnt = (SELECT MAX(week_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date <= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 7 DAY))
GROUP BY report_period;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ty;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ty
AS
SELECT dtl.acp_id,
  CASE
  WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'))
  THEN 'FLS'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB'))
  THEN 'NCOM'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA'))
  THEN 'RACK'
  WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE'))
  THEN 'NRHL'
  ELSE NULL
  END AS channel,
  CASE
  WHEN LOWER(dtl.channel) IN (LOWER('FLS'), LOWER('NCOM'))
  THEN 'FP'
  ELSE 'OP'
  END AS brand,
 SUM(CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS gross_spend,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt < 0
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS return_amount,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666')
   THEN dtl.line_net_usd_amt
   ELSE 0
   END) AS net_spend,
 COUNT(DISTINCT CASE
   WHEN dtl.line_net_usd_amt > 0
   THEN FORMAT('%11d', st.store_num) || CAST(COALESCE(dtl.order_date, dtl.tran_date) AS STRING)
   ELSE NULL
   END) AS trips,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt > 0
   THEN dtl.line_item_quantity
   ELSE 0
   END) AS items,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt < 0
   THEN - 1 * dtl.line_item_quantity
   ELSE 0
   END) AS return_items,
 SUM(CASE
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt < 0
   THEN - 1 * dtl.line_item_quantity
   WHEN LOWER(COALESCE(dtl.nonmerch_fee_code, '-999')) <> LOWER('6666') AND dtl.line_net_usd_amt > 0
   THEN dtl.line_item_quantity
   ELSE 0
   END) AS net_items,
 MAX(CASE
   WHEN LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA')) AND LOWER(TRIM(nts.aare_chnl_code
       )) = LOWER('FLS')
   THEN 1
   WHEN LOWER(st.business_unit_desc) IN (LOWER('N.CA'), LOWER('N.COM'), LOWER('TRUNK CLUB')) AND LOWER(TRIM(nts.aare_chnl_code
       )) = LOWER('NCOM')
   THEN 1
   WHEN LOWER(st.business_unit_desc) IN (LOWER('RACK'), LOWER('RACK CANADA')) AND LOWER(TRIM(nts.aare_chnl_code)) =
     LOWER('RACK')
   THEN 1
   WHEN LOWER(st.business_unit_desc) IN (LOWER('OFFPRICE ONLINE')) AND LOWER(TRIM(nts.aare_chnl_code)) = LOWER('NRHL')
   THEN 1
   ELSE 0
   END) AS acq_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON CASE
   WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp' ) AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('CAD')
   THEN 867
   WHEN LOWER(dtl.line_item_order_type) = LOWER('CustInitWebOrder') AND LOWER(dtl.line_item_fulfillment_type) = LOWER('StorePickUp') AND LOWER(dtl.line_item_net_amt_currency_code) = LOWER('USD')
   THEN 808
   ELSE dtl.intent_store_num
   END = st.store_num
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact AS nts ON dtl.global_tran_id = nts.aare_global_tran_id
WHERE CASE
   WHEN COALESCE(dtl.line_net_usd_amt, 0) >= 0
   THEN COALESCE(dtl.order_date, dtl.tran_date)
   ELSE dtl.tran_date
   END BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT ty_end_dt FROM date_lookup)
 AND dtl.business_day_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT DATE_ADD(ty_end_dt, INTERVAL 14 DAY)
   FROM date_lookup)
 AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
 AND dtl.acp_id IS NOT NULL
GROUP BY dtl.acp_id,
 channel,
 brand;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS ly;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS ly
AS
SELECT dtl.acp_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS dtl
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON dtl.intent_store_num = st.store_num
WHERE CASE
   WHEN COALESCE(dtl.line_net_usd_amt, 0) >= 0
   THEN COALESCE(dtl.order_date, dtl.tran_date)
   ELSE dtl.tran_date
   END BETWEEN (SELECT ly_start_dt  FROM date_lookup) AND (SELECT ly_end_dt FROM date_lookup)
 AND dtl.business_day_date BETWEEN (SELECT ly_start_dt FROM date_lookup) AND (SELECT DATE_ADD(ly_end_dt, INTERVAL 30 DAY)
   FROM date_lookup)
 AND LOWER(st.business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('N.COM'),
   LOWER('TRUNK CLUB'), LOWER('RACK'), LOWER('RACK CANADA'), LOWER('OFFPRICE ONLINE'))
 AND dtl.acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS new_ty;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS new_ty
AS
SELECT acp_id,
 aare_chnl_code AS acquired_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_ntn_status_fact
WHERE aare_status_date BETWEEN (SELECT ty_start_dt FROM date_lookup) AND (SELECT ty_end_dt  FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS activated_ty;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS activated_ty
AS
SELECT acp_id,
 activated_chnl_code AS activated_channel
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.customer_activated_fact
WHERE activated_date BETWEEN (SELECT ty_start_dt
   FROM date_lookup) AND (SELECT ty_end_dt
   FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS customer_summary;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_summary
AS
SELECT ty.brand,
 ty.channel,
 ty.acp_id,
 ty.gross_spend,
 ty.net_spend,
 ty.trips,
 ty.items,
  CASE
  WHEN acq.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS new_flg,
  CASE
  WHEN ly.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS retained_flg,
  CASE
  WHEN eng.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS eng_flg,
  CASE
  WHEN act.acp_id IS NOT NULL
  THEN 1
  ELSE 0
  END AS act_flg
FROM ty
 LEFT JOIN ly ON LOWER(ty.acp_id) = LOWER(ly.acp_id)
 LEFT JOIN week_num_engaged_customer AS eng ON LOWER(ty.acp_id) = LOWER(eng.acp_id) AND LOWER(ty.channel) = LOWER(eng.engaged_to_channel
    )
 LEFT JOIN new_ty AS acq ON LOWER(ty.acp_id) = LOWER(acq.acp_id)
 LEFT JOIN activated_ty AS act ON LOWER(ty.acp_id) = LOWER(act.acp_id) AND LOWER(ty.channel) = LOWER(act.activated_channel
    );
    
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
WHERE report_period = (SELECT report_period  FROM date_lookup);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period FROM date_lookup),
  'all' AS report_type,
  channel,
  '' AS predicted_segment,
  '' AS country,
  '' AS loyalty_status,
  '' AS nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary
 GROUP BY channel);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'all' AS report_type,
  brand,
  '' AS predicted_segment,
  '' AS country,
  '' AS loyalty_status,
  '' AS nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary
 GROUP BY brand);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period FROM date_lookup),
  'all' AS report_type,
  'JWN',
  '' AS predicted_segment,
  '' AS country,
  '' AS loyalty_status,
  '' AS nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS lyl_status;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS lyl_status
AS
SELECT acp_id,
  CASE
  WHEN cardmember_fl = 1
  THEN 'cardmember'
  WHEN member_fl = 1
  THEN 'member'
  ELSE 'non-member'
  END AS loyalty_status,
 rewards_level
FROM (SELECT ac.acp_id,
    CASE
    WHEN lmd.cardmember_enroll_date <= (SELECT ty_end_dt FROM date_lookup) AND (lmd.cardmember_close_date >= (SELECT ty_end_dt FROM date_lookup) OR lmd.cardmember_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS cardmember_fl,
    CASE
    WHEN CASE
        WHEN lmd.cardmember_enroll_date <= (SELECT ty_end_dt FROM date_lookup) AND (lmd.cardmember_close_date >= (SELECT ty_end_dt FROM date_lookup) OR lmd.cardmember_close_date IS NULL)
        THEN 1
        ELSE 0
        END = 0 AND lmd.member_enroll_date <= (SELECT ty_end_dt FROM date_lookup) AND (lmd.member_close_date >= (SELECT ty_end_dt FROM date_lookup) OR lmd.member_close_date IS NULL)
    THEN 1
    ELSE 0
    END AS member_fl,
   MAX(CASE
     WHEN LOWER(t1.rewards_level) IN (LOWER('MEMBER'))
     THEN 1
     WHEN LOWER(t1.rewards_level) IN (LOWER('INSIDER'), LOWER('L1'))
     THEN 2
     WHEN LOWER(t1.rewards_level) IN (LOWER('INFLUENCER'), LOWER('L2'))
     THEN 3
     WHEN LOWER(t1.rewards_level) IN (LOWER('AMBASSADOR'), LOWER('L3'))
     THEN 4
     WHEN LOWER(t1.rewards_level) IN (LOWER('ICON'), LOWER('L4'))
     THEN 5
     ELSE 0
     END) AS rewards_level
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer AS ac
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_member_dim_vw AS lmd ON LOWER(ac.acp_loyalty_id) = LOWER(lmd.loyalty_id)
   LEFT JOIN (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.loyalty_level_lifecycle_fact_vw AS rwd
    WHERE start_day_date <= (SELECT ty_end_dt
       FROM date_lookup)
     AND end_day_date > (SELECT ty_end_dt FROM date_lookup)) AS t1 ON LOWER(ac.acp_loyalty_id) = LOWER(t1.loyalty_id)
  GROUP BY ac.acp_id,
   cardmember_fl,
   member_fl) AS lyl;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DROP TABLE IF EXISTS customer_summary_seg;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS customer_summary_seg
AS
SELECT ty.brand,
 ty.channel,
 ty.acp_id,
  CASE
  WHEN x.predicted_ct_segment IS NULL
  THEN 'unknown'
  ELSE x.predicted_ct_segment
  END AS predicted_segment,
 ac.country,
  CASE
  WHEN lyl.loyalty_status IS NULL
  THEN 'non-member'
  ELSE lyl.loyalty_status
  END AS loyalty_status,
  CASE
  WHEN lyl.rewards_level = 1
  THEN '1-member'
  WHEN lyl.rewards_level = 2
  THEN '2-insider'
  WHEN lyl.rewards_level = 3
  THEN '3-influencer'
  WHEN lyl.rewards_level = 4
  THEN '4-ambassador'
  WHEN lyl.rewards_level = 5
  THEN '5-icon'
  WHEN lyl.rewards_level = 0
  THEN CASE
   WHEN LOWER(lyl.loyalty_status) = LOWER('cardmember')
   THEN '1-member'
   WHEN LOWER(lyl.loyalty_status) = LOWER('member')
   THEN '1-member'
   WHEN LOWER(lyl.loyalty_status) = LOWER('non-member')
   THEN '0-nonmember'
   ELSE NULL
   END
  ELSE '0-nonmember'
  END AS nordy_level,
 ty.gross_spend,
 ty.net_spend,
 ty.trips,
 ty.items,
 ty.new_flg,
 ty.retained_flg,
 ty.eng_flg,
 ty.act_flg
FROM customer_summary AS ty
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_customber_model_attribute_productionalization.customer_prediction_core_target_segment AS x ON LOWER(ty.acp_id) = LOWER(x.acp_id)
 LEFT JOIN lyl_status AS lyl ON LOWER(ty.acp_id) = LOWER(lyl.acp_id)
 LEFT JOIN (SELECT acp_id,
    CASE
    WHEN us_dma_code = - 1
    THEN NULL
    ELSE us_dma_desc
    END AS us_dma,
    CASE
    WHEN CASE
     WHEN us_dma_code = - 1
     THEN NULL
     ELSE us_dma_desc
     END IS NOT NULL
    THEN 'US'
    WHEN ca_dma_code IS NOT NULL
    THEN 'CA'
    ELSE 'unknown'
    END AS country
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.analytical_customer) AS ac ON LOWER(ty.acp_id) = LOWER(ac.acp_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

--collect statistics on customer_summary_seg column (acp_id);
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'segment' AS report_type,
  channel,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary_seg
 GROUP BY channel,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'segment' AS report_type,
  brand,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary_seg
 GROUP BY brand,
  predicted_segment,
  country,
  loyalty_status,
  nordy_level);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg
(SELECT (SELECT report_period
   FROM date_lookup),
  'segment' AS report_type,
  'JWN',
  predicted_segment,
  country,
  loyalty_status,
  nordy_level,
  COUNT(DISTINCT acp_id) AS total_customer,
  CAST(SUM(gross_spend) AS NUMERIC) AS total_gross_spend,
  CAST(SUM(net_spend) AS NUMERIC) AS total_net_spend,
  CAST(SUM(trips) AS NUMERIC) AS total_trips,
  CAST(SUM(items) AS NUMERIC) AS total_items,
  COUNT(DISTINCT CASE
    WHEN retained_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS retained_customer,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_retained_spend,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS retained_trips,
  CAST(SUM(CASE
     WHEN retained_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS retained_items,
  COUNT(DISTINCT CASE
    WHEN new_flg = 1
    THEN acp_id
    ELSE NULL
    END) AS new_customer,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_new_spend,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS new_trips,
  CAST(SUM(CASE
     WHEN new_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS new_items,
    COUNT(DISTINCT acp_id) - COUNT(DISTINCT CASE
      WHEN new_flg = 1
      THEN acp_id
      ELSE NULL
      END) - COUNT(DISTINCT CASE
     WHEN retained_flg = 1
     THEN acp_id
     ELSE NULL
     END) AS react_customer,
  CAST(SUM(gross_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN gross_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN gross_spend
      ELSE 0
      END) AS NUMERIC) AS total_react_spend,
  CAST(SUM(net_spend) - SUM(CASE
       WHEN retained_flg = 1
       THEN net_spend
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN net_spend
      ELSE 0
      END) AS NUMERIC) AS net_react_spend,
  CAST(SUM(trips) - SUM(CASE
       WHEN retained_flg = 1
       THEN CAST(trips AS INTEGER)
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN CAST(trips AS INTEGER)
      ELSE 0
      END) AS NUMERIC) AS react_trips,
  CAST(SUM(items) - SUM(CASE
       WHEN retained_flg = 1
       THEN items
       ELSE 0
       END) - SUM(CASE
      WHEN new_flg = 1
      THEN items
      ELSE 0
      END) AS NUMERIC) AS react_items,
  SUM(act_flg) AS activated_customer,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_activated_spend,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS activated_trips,
  CAST(SUM(CASE
     WHEN act_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS activated_items,
  SUM(eng_flg) AS eng_customer,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN gross_spend
     ELSE 0
     END) AS NUMERIC) AS total_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN net_spend
     ELSE 0
     END) AS NUMERIC) AS net_eng_spend,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN CAST(trips AS INTEGER)
     ELSE 0
     END) AS NUMERIC) AS eng_trips,
  CAST(SUM(CASE
     WHEN eng_flg = 1
     THEN items
     ELSE 0
     END) AS NUMERIC) AS eng_items,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS update_tmstp
 FROM customer_summary_seg
 GROUP BY predicted_segment,
  country,
  loyalty_status,
  nordy_level);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*grant select on `{{params.gcp_project_id}}`.{{params.bfl_t2_schema}}.weekly_buyerflow_agg to public;*/
--COLLECT STATISTICS  COLUMN (report_period, box) on t2dl_das_ea_bfl.weekly_buyerflow_agg;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
