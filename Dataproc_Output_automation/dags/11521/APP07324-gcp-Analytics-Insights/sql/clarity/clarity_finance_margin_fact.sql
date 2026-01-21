
BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;

BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS buyerflow

AS
SELECT CASE
  WHEN LOWER(channel) = LOWER('1) Nordstrom Stores')
  THEN 'FULL LINE'
  WHEN LOWER(channel) = LOWER('2) Nordstrom.com')
  THEN 'N.COM'
  WHEN LOWER(channel) = LOWER('3) Rack Stores')
  THEN 'RACK'
  WHEN LOWER(channel) = LOWER('4) Rack.com')
  THEN 'OFFPRICE ONLINE'
  ELSE NULL
  END AS business_unit_desc,
  CAST(SUBSTR(fiscal_year_shopped, 2 * -1) AS FLOAT64) + 2000 AS year_num,
 acp_id,
 buyer_flow AS buyerflow_code,
  CASE
  WHEN aare_acquired = 1
  THEN UPPER('Y')
  ELSE UPPER('N')
  END AS aare_acquired_ind,
  CASE
  WHEN aare_activated = 1
  THEN UPPER('Y')
  ELSE UPPER('N')
  END AS aare_activated_ind,
  CASE
  WHEN aare_retained = 1
  THEN UPPER('Y')
  ELSE UPPER('N')
  END AS aare_retained_ind,
  CASE
  WHEN aare_engaged = 1
  THEN UPPER('Y')
  ELSE UPPER('N')
  END AS aare_engaged_ind
FROM `{{params.gcp_project_id}}`.t2dl_das_strategy.cco_buyer_flow_fy
WHERE LOWER(channel) IN (LOWER('1) Nordstrom Stores'), LOWER('2) Nordstrom.com'), LOWER('3) Rack Stores'), LOWER('4) Rack.com'
    ))
 AND CAST(SUBSTR(fiscal_year_shopped, 2 * -1) AS FLOAT64) + 2000 IN (SELECT DISTINCT year_num
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal
   WHERE day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 60 DAY) AND (CURRENT_DATE('PST8PDT')));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(acp_id, year_num, business_unit_desc) ON buyerflow;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS aec
--CLUSTER BY acp_id, execution_qtr
AS
SELECT acp_id,
 execution_qtr,
 engagement_cohort
FROM `{{params.gcp_project_id}}`.t2dl_das_aec.audience_engagement_cohorts
WHERE execution_qtr_end_dt >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 60 DAY)
 AND execution_qtr_start_dt <= CURRENT_DATE('PST8PDT');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(acp_id, execution_qtr) ON aec;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS tender
--CLUSTER BY purchase_id
AS
SELECT transaction_id AS purchase_id,
 MIN(business_day_date) AS business_day_date,
 SUM(CASE
   WHEN LOWER(tender_type) = LOWER('Cash')
   THEN 1
   ELSE 0
   END) AS tran_tender_cash_flag,
 SUM(CASE
   WHEN LOWER(tender_type) = LOWER('Check')
   THEN 1
   ELSE 0
   END) AS tran_tender_check_flag,
 SUM(CASE
   WHEN LOWER(tender_type) = LOWER('Credit Card') AND LOWER(tender_subtype) LIKE LOWER('Nordstrom%')
   THEN 1
   ELSE 0
   END) AS tran_tender_nordstrom_card_flag,
 SUM(CASE
   WHEN LOWER(tender_type) = LOWER('Credit Card') AND LOWER(tender_subtype) NOT LIKE LOWER('Nordstrom%')
   THEN 1
   ELSE 0
   END) AS tran_tender_non_nordstrom_credit_flag,
 SUM(CASE
   WHEN LOWER(tender_type) = LOWER('Debit Card')
   THEN 1
   ELSE 0
   END) AS tran_tender_non_nordstrom_debit_flag,
 SUM(CASE
   WHEN LOWER(tender_type) = LOWER('Gift Card')
   THEN 1
   ELSE 0
   END) AS tran_tender_nordstrom_gift_card_flag,
 SUM(CASE
   WHEN LOWER(tender_type) = LOWER('Nordstrom Note')
   THEN 1
   ELSE 0
   END) AS tran_tender_nordstrom_note_flag,
 SUM(CASE
   WHEN LOWER(tender_type) = LOWER('PayPal')
   THEN 1
   ELSE 0
   END) AS tran_tender_paypal_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_clarity_tran_tender_type_fact AS jctttf
WHERE business_day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 60 DAY) AND (CURRENT_DATE('PST8PDT'))
 AND business_day_date BETWEEN DATE '2021-01-31' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
GROUP BY purchase_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(purchase_id) ON tender;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS pre_sessions
--CLUSTER BY session_id
AS
SELECT activity_date_pacific,
 mrkt_type,
 finance_rollup,
 finance_detail,
 session_id
FROM `{{params.gcp_project_id}}`.t2dl_das_sessions.dior_session_fact AS dsfd
WHERE activity_date_pacific BETWEEN DATE_SUB(DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 60 DAY), INTERVAL 30 DAY) AND (CURRENT_DATE('PST8PDT')
   )
 AND activity_date_pacific >= DATE '2022-01-31'
 AND LOWER(experience) IN (LOWER('ANDROID_APP'), LOWER('DESKTOP_WEB'), LOWER('IOS_APP'), LOWER('MOBILE_WEB'))
 AND (web_orders >= 1 OR activity_date_pacific < DATE '2022-02-09');
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(session_id) ON pre_sessions;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS sess_order_map
--CLUSTER BY session_id
AS
SELECT activity_date,
 order_id,
 session_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_session_order_fact
WHERE activity_date BETWEEN DATE_SUB(DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 60 DAY), INTERVAL 30 DAY) AND (CURRENT_DATE('PST8PDT'))
 AND activity_date >= DATE '2022-01-31';
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(session_id) ON sess_order_map;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS channel
--CLUSTER BY order_id, activity_date_pacific
AS
SELECT ps.activity_date_pacific,
 som.order_id,
 ps.mrkt_type,
 ps.finance_rollup,
 ps.finance_detail
FROM pre_sessions AS ps
 INNER JOIN sess_order_map AS som ON LOWER(som.session_id) = LOWER(ps.session_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--COLLECT STATISTICS COLUMN(order_id,activity_date_pacific) ON channel;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS jcmmv (
business_day_date DATE NOT NULL,
demand_date DATE,
purchase_id STRING(64),
business_unit_desc STRING(50),
intent_store_num INTEGER,
bill_zip_code STRING(15),
line_item_currency_code STRING(8),
inventory_business_model STRING(30),
division_name STRING(150),
subdivision_name STRING(150),
price_type STRING(20),
record_source STRING(1),
platform_code STRING(40),
remote_selling_ind STRING(30),
fulfilled_from_location_type STRING(32),
delivery_method STRING(30),
delivery_method_subtype STRING(40),
loyalty_status STRING(20),
acp_id STRING(50),
order_num STRING(40),
jwn_reported_gmv_ind STRING(1),
jwn_merch_net_sales_ind STRING(1),
jwn_reported_net_sales_ind STRING(1),
line_item_activity_type_code STRING(1),
jwn_reported_gmv_usd_amt NUMERIC(25,5),
line_item_quantity INTEGER,
jwn_merch_net_sales_usd_amt NUMERIC(25,5),
jwn_reported_net_sales_usd_amt NUMERIC(25,5),
jwn_variable_expense_usd_amt NUMERIC(25,5),
jwn_contribution_margin_usd_amt NUMERIC(25,5)
) ;
--CLUSTER BY purchase_id;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO jcmmv
(SELECT business_day_date,
  demand_date,
  transaction_id AS purchase_id,
  business_unit_desc,
  intent_store_num,
  bill_zip_code,
  line_item_currency_code,
  inventory_business_model,
   CASE
   WHEN LOWER(division_name) LIKE LOWER('INACT%')
   THEN 'OTHER'
   WHEN LOWER(division_name) IN (LOWER('DIRECT'), LOWER('DISCONTINUED DEPTS'), LOWER('INVENTORY INTEGRITY TEST'), LOWER('NPG NEW BUSINESS'
      ), LOWER('MERCH PROJECTS'))
   THEN 'OTHER'
   WHEN division_name IS NULL OR LOWER(division_name) = LOWER('CORPORATE')
   THEN 'OTHER/NON-MERCH'
   ELSE division_name
   END AS division_name,
   CASE
   WHEN subdivision_num IN (770, 790, 775, 780, 710, 705, 700, 785)
   THEN CASE
    WHEN LOWER(subdivision_name) = LOWER('KIDS WEAR')
    THEN 'KIDS APPAREL'
    ELSE subdivision_name
    END
   ELSE NULL
   END AS subdivision_name,
  price_type,
  record_source,
   CASE
   WHEN LOWER(order_platform_type) = LOWER('Mobile App')
   THEN CASE
    WHEN LOWER(platform_subtype) IN (LOWER('UNKNOWN'), LOWER('UNKNOWN: Price Adjustment'))
    THEN 'Other'
    ELSE platform_subtype
    END
   WHEN LOWER(order_platform_type) IN (LOWER('BorderFree'), LOWER('Phone (Customer Care)'), LOWER('UNKNOWN'), LOWER('UNKNOWN: Price Adjustment'
      ), LOWER('UNKNOWN_VALUE'), LOWER('UNKNOWN_VALUE: Platform=UNKNOW'))
   THEN 'Other'
   ELSE order_platform_type
   END AS platform_code,
  remote_selling_ind,
  fulfilled_from_location_type,
  delivery_method,
   CASE
   WHEN LOWER(delivery_method_subtype) = LOWER('UNKNOWN_VALUE')
   THEN 'Other'
   ELSE delivery_method_subtype
   END AS delivery_method_subtype,
  loyalty_status,
  acp_id,
  order_num,
  jwn_reported_gmv_ind,
  jwn_merch_net_sales_ind,
  jwn_reported_net_sales_ind,
  line_item_activity_type_code,
  jwn_reported_gmv_usd_amt,
  line_item_quantity,
  ROUND(CAST(jwn_merch_net_sales_usd_amt AS NUMERIC), 5) AS jwn_merch_net_sales_usd_amt,
  ROUND(CAST(jwn_reported_net_sales_usd_amt AS NUMERIC), 5) AS jwn_reported_net_sales_usd_amt,
  CAST(jwn_variable_expense_usd_amt AS NUMERIC) AS jwn_variable_expense_usd_amt,
  ROUND(CAST(jwn_contribution_margin_usd_amt AS NUMERIC), 5) AS jwn_contribution_margin_usd_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_contribution_margin_metric_vw AS jcmmv
 WHERE business_day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 60 DAY) AND (CURRENT_DATE('PST8PDT'))
  AND business_day_date BETWEEN DATE '2021-01-31' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
  AND LOWER(zero_value_unit_ind) = LOWER('N')
  AND (LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('N.COM'), LOWER('RACK'), LOWER('OFFPRICE ONLINE')) OR
      LOWER(business_unit_desc) IN (LOWER('FULL LINE CANADA'), LOWER('N.CA'), LOWER('RACK CANADA')) AND
      business_day_date <= DATE '2023-02-25')
  AND LOWER(record_source) <> LOWER('O'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (purchase_id) on jcmmv;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin_tender AS
SELECT jcmmv.business_day_date,
 jcmmv.demand_date,
 dc.year_num,
 dc.quarter_num,
 jcmmv.business_unit_desc,
 jcmmv.intent_store_num,
 jcmmv.bill_zip_code,
 jcmmv.line_item_currency_code,
 jcmmv.inventory_business_model,
 jcmmv.division_name,
 jcmmv.subdivision_name,
 jcmmv.price_type,
 jcmmv.record_source,
 jcmmv.platform_code,
 jcmmv.remote_selling_ind,
 jcmmv.fulfilled_from_location_type,
 jcmmv.delivery_method,
 jcmmv.delivery_method_subtype,
 jcmmv.loyalty_status,
 jcmmv.acp_id,
 jcmmv.order_num,
  CASE
  WHEN tender.tran_tender_cash_flag >= 1
  THEN 'Y'
  ELSE 'N'
  END AS tran_tender_cash_flag,
  CASE
  WHEN tender.tran_tender_check_flag >= 1
  THEN 'Y'
  ELSE 'N'
  END AS tran_tender_check_flag,
  CASE
  WHEN tender.tran_tender_nordstrom_card_flag >= 1
  THEN 'Y'
  ELSE 'N'
  END AS tran_tender_nordstrom_card_flag,
  CASE
  WHEN tender.tran_tender_non_nordstrom_credit_flag >= 1
  THEN 'Y'
  ELSE 'N'
  END AS tran_tender_non_nordstrom_credit_flag,
  CASE
  WHEN tender.tran_tender_non_nordstrom_debit_flag >= 1
  THEN 'Y'
  ELSE 'N'
  END AS tran_tender_non_nordstrom_debit_flag,
  CASE
  WHEN tender.tran_tender_nordstrom_gift_card_flag >= 1
  THEN 'Y'
  ELSE 'N'
  END AS tran_tender_nordstrom_gift_card_flag,
  CASE
  WHEN tender.tran_tender_nordstrom_note_flag >= 1
  THEN 'Y'
  ELSE 'N'
  END AS tran_tender_nordstrom_note_flag,
  CASE
  WHEN tender.tran_tender_paypal_flag >= 1
  THEN 'Y'
  ELSE 'N'
  END AS tran_tender_paypal_flag,
 jcmmv.jwn_reported_gmv_ind,
 jcmmv.jwn_merch_net_sales_ind,
 jcmmv.jwn_reported_net_sales_ind,
 jcmmv.line_item_activity_type_code,
 jcmmv.jwn_reported_gmv_usd_amt,
 jcmmv.line_item_quantity,
 jcmmv.jwn_merch_net_sales_usd_amt,
 jcmmv.jwn_reported_net_sales_usd_amt,
 jcmmv.jwn_variable_expense_usd_amt,
 jcmmv.jwn_contribution_margin_usd_amt
FROM jcmmv
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS dc ON jcmmv.business_day_date = dc.day_date
 LEFT JOIN tender ON LOWER(tender.purchase_id) = LOWER(jcmmv.purchase_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin_tender_order_null AS
SELECT business_day_date,
 year_num,
 quarter_num,
 business_unit_desc,
 intent_store_num,
 bill_zip_code,
 line_item_currency_code,
 inventory_business_model,
 division_name,
 subdivision_name,
 price_type,
 record_source,
 platform_code,
 remote_selling_ind,
 fulfilled_from_location_type,
 delivery_method,
 delivery_method_subtype,
 loyalty_status,
 acp_id,
 order_num,
 tran_tender_cash_flag,
 tran_tender_check_flag,
 tran_tender_nordstrom_card_flag,
 tran_tender_non_nordstrom_credit_flag,
 tran_tender_non_nordstrom_debit_flag,
 tran_tender_nordstrom_gift_card_flag,
 tran_tender_nordstrom_note_flag,
 tran_tender_paypal_flag,
 jwn_reported_gmv_ind,
 jwn_merch_net_sales_ind,
 jwn_reported_net_sales_ind,
 line_item_activity_type_code,
 jwn_reported_gmv_usd_amt,
 line_item_quantity,
 jwn_merch_net_sales_usd_amt,
 jwn_reported_net_sales_usd_amt,
 jwn_variable_expense_usd_amt,
 jwn_contribution_margin_usd_amt
FROM margin_tender AS jcmmv
WHERE order_num IS NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin_tender_order_not_null
--CLUSTER BY order_num, demand_date
AS
SELECT *
FROM margin_tender AS jcmmv
WHERE order_num IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (order_num,demand_date) on margin_tender_order_not_null;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin_channel AS
SELECT jcmmv.business_day_date,
 jcmmv.year_num,
 jcmmv.quarter_num,
 jcmmv.business_unit_desc,
 jcmmv.intent_store_num,
 jcmmv.bill_zip_code,
 jcmmv.line_item_currency_code,
 jcmmv.inventory_business_model,
 jcmmv.division_name,
 jcmmv.subdivision_name,
 jcmmv.price_type,
 jcmmv.record_source,
 jcmmv.platform_code,
 jcmmv.remote_selling_ind,
 jcmmv.fulfilled_from_location_type,
 jcmmv.delivery_method,
 jcmmv.delivery_method_subtype,
 jcmmv.loyalty_status,
 jcmmv.acp_id,
 mc.mrkt_type AS mrtk_chnl_type_code,
 mc.finance_rollup AS mrtk_chnl_finance_rollup_code,
 mc.finance_detail AS mrtk_chnl_finance_detail_code,
 jcmmv.tran_tender_cash_flag,
 jcmmv.tran_tender_check_flag,
 jcmmv.tran_tender_nordstrom_card_flag,
 jcmmv.tran_tender_non_nordstrom_credit_flag,
 jcmmv.tran_tender_non_nordstrom_debit_flag,
 jcmmv.tran_tender_nordstrom_gift_card_flag,
 jcmmv.tran_tender_nordstrom_note_flag,
 jcmmv.tran_tender_paypal_flag,
 jcmmv.jwn_reported_gmv_ind,
 jcmmv.jwn_merch_net_sales_ind,
 jcmmv.jwn_reported_net_sales_ind,
 jcmmv.line_item_activity_type_code,
 jcmmv.jwn_reported_gmv_usd_amt,
 jcmmv.line_item_quantity,
 jcmmv.jwn_merch_net_sales_usd_amt,
 jcmmv.jwn_reported_net_sales_usd_amt,
 jcmmv.jwn_variable_expense_usd_amt,
 jcmmv.jwn_contribution_margin_usd_amt
FROM margin_tender_order_not_null AS jcmmv
 LEFT JOIN channel AS mc ON LOWER(jcmmv.order_num) = LOWER(mc.order_id) AND jcmmv.demand_date = mc.activity_date_pacific
   
UNION ALL
SELECT business_day_date,
 year_num,
 quarter_num,
 business_unit_desc,
 intent_store_num,
 bill_zip_code,
 line_item_currency_code,
 inventory_business_model,
 division_name,
 subdivision_name,
 price_type,
 record_source,
 platform_code,
 remote_selling_ind,
 fulfilled_from_location_type,
 delivery_method,
 delivery_method_subtype,
 loyalty_status,
 acp_id,
 SUBSTR(NULL, 1, 40) AS mrtk_chnl_type_code,
 SUBSTR(NULL, 1, 40) AS mrtk_chnl_finance_rollup_code,
 SUBSTR(NULL, 1, 40) AS mrtk_chnl_finance_detail_code,
 tran_tender_cash_flag,
 tran_tender_check_flag,
 tran_tender_nordstrom_card_flag,
 tran_tender_non_nordstrom_credit_flag,
 tran_tender_non_nordstrom_debit_flag,
 tran_tender_nordstrom_gift_card_flag,
 tran_tender_nordstrom_note_flag,
 tran_tender_paypal_flag,
 jwn_reported_gmv_ind,
 jwn_merch_net_sales_ind,
 jwn_reported_net_sales_ind,
 line_item_activity_type_code,
 jwn_reported_gmv_usd_amt,
 line_item_quantity,
 jwn_merch_net_sales_usd_amt,
 jwn_reported_net_sales_usd_amt,
 jwn_variable_expense_usd_amt,
 jwn_contribution_margin_usd_amt
FROM margin_tender_order_null AS jcmmv;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin_acp_null AS
SELECT *
FROM margin_channel AS jcmmv
WHERE acp_id IS NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin_acp_not_null
--CLUSTER BY acp_id, year_num, business_unit_desc
AS
SELECT *
FROM margin_channel AS jcmmv
WHERE acp_id IS NOT NULL;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, year_num, business_unit_desc) on margin_acp_not_null;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin_buyerflow
--CLUSTER BY acp_id, quarter_num
AS
SELECT jcmmv.business_day_date,
 jcmmv.business_unit_desc,
 jcmmv.intent_store_num AS store_num,
 jcmmv.bill_zip_code,
 jcmmv.line_item_currency_code,
 jcmmv.inventory_business_model,
 jcmmv.division_name,
 jcmmv.subdivision_name,
 jcmmv.price_type,
 jcmmv.record_source,
 jcmmv.platform_code,
 jcmmv.remote_selling_ind,
 jcmmv.fulfilled_from_location_type,
 jcmmv.delivery_method,
 jcmmv.delivery_method_subtype,
 jcmmv.loyalty_status,
 jcmmv.quarter_num,
 bf.buyerflow_code,
 bf.aare_acquired_ind,
 bf.aare_activated_ind,
 bf.aare_retained_ind,
 bf.aare_engaged_ind,
 jcmmv.acp_id,
 jcmmv.mrtk_chnl_type_code,
 jcmmv.mrtk_chnl_finance_rollup_code,
 jcmmv.mrtk_chnl_finance_detail_code,
 jcmmv.tran_tender_cash_flag,
 jcmmv.tran_tender_check_flag,
 jcmmv.tran_tender_nordstrom_card_flag,
 jcmmv.tran_tender_non_nordstrom_credit_flag,
 jcmmv.tran_tender_non_nordstrom_debit_flag,
 jcmmv.tran_tender_nordstrom_gift_card_flag,
 jcmmv.tran_tender_nordstrom_note_flag,
 jcmmv.tran_tender_paypal_flag,
 jcmmv.jwn_reported_gmv_ind,
 jcmmv.jwn_merch_net_sales_ind,
 jcmmv.jwn_reported_net_sales_ind,
 jcmmv.line_item_activity_type_code,
 jcmmv.jwn_reported_gmv_usd_amt,
 jcmmv.line_item_quantity,
 jcmmv.jwn_merch_net_sales_usd_amt,
 jcmmv.jwn_reported_net_sales_usd_amt,
 jcmmv.jwn_variable_expense_usd_amt,
 jcmmv.jwn_contribution_margin_usd_amt
FROM margin_acp_not_null AS jcmmv
 LEFT JOIN buyerflow AS bf ON LOWER(jcmmv.business_unit_desc) = LOWER(bf.business_unit_desc) AND jcmmv.year_num = bf.year_num
     AND LOWER(jcmmv.acp_id) = LOWER(bf.acp_id);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
--collect statistics column (acp_id, quarter_num) on margin_buyerflow;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin_aec AS
SELECT jcmmv.business_day_date,
 jcmmv.business_unit_desc,
 jcmmv.store_num,
 jcmmv.bill_zip_code,
 jcmmv.line_item_currency_code,
 jcmmv.inventory_business_model,
 jcmmv.division_name,
 jcmmv.subdivision_name,
 jcmmv.price_type,
 jcmmv.record_source,
 jcmmv.platform_code,
 jcmmv.remote_selling_ind,
 jcmmv.fulfilled_from_location_type,
 jcmmv.delivery_method,
 jcmmv.delivery_method_subtype,
 jcmmv.loyalty_status,
 jcmmv.buyerflow_code,
 jcmmv.aare_acquired_ind,
 jcmmv.aare_activated_ind,
 jcmmv.aare_retained_ind,
 jcmmv.aare_engaged_ind,
 jcmmv.mrtk_chnl_type_code,
 jcmmv.mrtk_chnl_finance_rollup_code,
 jcmmv.mrtk_chnl_finance_detail_code,
 aec.engagement_cohort,
 jcmmv.tran_tender_cash_flag,
 jcmmv.tran_tender_check_flag,
 jcmmv.tran_tender_nordstrom_card_flag,
 jcmmv.tran_tender_non_nordstrom_credit_flag,
 jcmmv.tran_tender_non_nordstrom_debit_flag,
 jcmmv.tran_tender_nordstrom_gift_card_flag,
 jcmmv.tran_tender_nordstrom_note_flag,
 jcmmv.tran_tender_paypal_flag,
 jcmmv.jwn_reported_gmv_ind,
 jcmmv.jwn_merch_net_sales_ind,
 jcmmv.jwn_reported_net_sales_ind,
 jcmmv.line_item_activity_type_code,
 jcmmv.jwn_reported_gmv_usd_amt,
 jcmmv.line_item_quantity,
 jcmmv.jwn_merch_net_sales_usd_amt,
 jcmmv.jwn_reported_net_sales_usd_amt,
 jcmmv.jwn_variable_expense_usd_amt,
 jcmmv.jwn_contribution_margin_usd_amt
FROM margin_buyerflow AS jcmmv
 LEFT JOIN aec ON LOWER(jcmmv.acp_id) = LOWER(aec.acp_id) AND jcmmv.quarter_num = aec.execution_qtr;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
CREATE TEMPORARY TABLE IF NOT EXISTS margin (
business_day_date DATE NOT NULL,
business_unit_desc STRING(50),
store_num INTEGER,
bill_zip_code STRING(15),
line_item_currency_code STRING(8),
inventory_business_model STRING(30),
division_name STRING(150),
subdivision_name STRING(150),
price_type STRING(20),
record_source STRING(1),
platform_code STRING(40),
remote_selling_ind STRING(30),
fulfilled_from_location_type STRING(32),
delivery_method STRING(30),
delivery_method_subtype STRING(40),
fulfillment_journey STRING(40),
loyalty_status STRING(20),
buyerflow_code STRING(27),
aare_acquired_ind STRING(1),
aare_activated_ind STRING(1),
aare_retained_ind STRING(1),
aare_engaged_ind STRING(1),
mrtk_chnl_type_code STRING(40),
mrtk_chnl_finance_rollup_code STRING(40),
mrtk_chnl_finance_detail_code STRING(40),
engagement_cohort STRING(30),
tran_tender_cash_flag STRING(1),
tran_tender_check_flag STRING(1),
tran_tender_nordstrom_card_flag STRING(1),
tran_tender_non_nordstrom_credit_flag STRING(1),
tran_tender_non_nordstrom_debit_flag STRING(1),
tran_tender_nordstrom_gift_card_flag STRING(1),
tran_tender_nordstrom_note_flag STRING(1),
tran_tender_paypal_flag STRING(1),
reported_gmv_usd_amt NUMERIC(25,5),
reported_gmv_units INTEGER,
merch_net_sales_usd_amt NUMERIC(25,5),
merch_net_sales_units INTEGER,
reported_net_sales_usd_amt NUMERIC(25,5),
reported_net_sales_units INTEGER,
variable_cost_usd_amt NUMERIC(25,5),
gross_contribution_margin_usd_amt NUMERIC(25,5),
net_contribution_margin_usd_amt NUMERIC(25,5),
dw_sys_load_tmstp DATETIME NOT NULL
);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO margin
(SELECT business_day_date,
  business_unit_desc,
  store_num,
  bill_zip_code,
  line_item_currency_code,
  inventory_business_model,
  division_name,
  subdivision_name,
  price_type,
  record_source,
  platform_code,
  remote_selling_ind,
  fulfilled_from_location_type,
  delivery_method,
  delivery_method_subtype,
  CAST(fulfillment_journey AS STRING) AS fulfillment_journey,
  loyalty_status,
  buyerflow_code,
  aare_acquired_ind,
  aare_activated_ind,
  aare_retained_ind,
  aare_engaged_ind,
  mrtk_chnl_type_code,
  mrtk_chnl_finance_rollup_code,
  mrtk_chnl_finance_detail_code,
  engagement_cohort,
  tran_tender_cash_flag,
  tran_tender_check_flag,
  tran_tender_nordstrom_card_flag,
  tran_tender_non_nordstrom_credit_flag,
  tran_tender_non_nordstrom_debit_flag,
  tran_tender_nordstrom_gift_card_flag,
  tran_tender_nordstrom_note_flag,
  tran_tender_paypal_flag,
  CAST(reported_gmv_usd_amt AS NUMERIC) AS reported_gmv_usd_amt,
  reported_gmv_units,
  CAST(merch_net_sales_usd_amt AS NUMERIC) AS merch_net_sales_usd_amt,
  merch_net_sales_units,
  CAST(reported_net_sales_usd_amt AS NUMERIC) AS reported_net_sales_usd_amt,
  reported_net_sales_units,
  variable_cost_usd_amt,
  CAST(gross_contribution_margin_usd_amt AS NUMERIC) AS gross_contribution_margin_usd_amt,
  net_contribution_margin_usd_amt,
  dw_sys_load_tmstp
 FROM (SELECT business_day_date,
     business_unit_desc,
     store_num,
     bill_zip_code,
     line_item_currency_code,
     inventory_business_model,
     division_name,
     subdivision_name,
     price_type,
     record_source,
     platform_code,
     remote_selling_ind,
     fulfilled_from_location_type,
     delivery_method,
     delivery_method_subtype,
     NULL AS fulfillment_journey,
     loyalty_status,
     buyerflow_code,
     aare_acquired_ind,
     aare_activated_ind,
     aare_retained_ind,
     aare_engaged_ind,
     mrtk_chnl_type_code,
     mrtk_chnl_finance_rollup_code,
     mrtk_chnl_finance_detail_code,
     engagement_cohort,
     tran_tender_cash_flag,
     tran_tender_check_flag,
     tran_tender_nordstrom_card_flag,
     tran_tender_non_nordstrom_credit_flag,
     tran_tender_non_nordstrom_debit_flag,
     tran_tender_nordstrom_gift_card_flag,
     tran_tender_nordstrom_note_flag,
     tran_tender_paypal_flag,
     SUM(CASE
       WHEN LOWER(jwn_reported_gmv_ind) = LOWER('Y')
       THEN jwn_reported_gmv_usd_amt
       ELSE 0
       END) AS reported_gmv_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_reported_gmv_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS reported_gmv_units,
     SUM(CASE
       WHEN LOWER(jwn_merch_net_sales_ind) = LOWER('Y')
       THEN jwn_merch_net_sales_usd_amt
       ELSE 0
       END) AS merch_net_sales_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_merch_net_sales_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS merch_net_sales_units,
     SUM(CASE
       WHEN LOWER(jwn_reported_net_sales_ind) = LOWER('Y')
       THEN jwn_reported_net_sales_usd_amt
       ELSE 0
       END) AS reported_net_sales_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_reported_net_sales_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS reported_net_sales_units,
     SUM(jwn_variable_expense_usd_amt) AS variable_cost_usd_amt,
     SUM(CASE
       WHEN LOWER(line_item_activity_type_code) = LOWER('S')
       THEN jwn_contribution_margin_usd_amt
       ELSE 0
       END) AS gross_contribution_margin_usd_amt,
     SUM(jwn_contribution_margin_usd_amt) AS net_contribution_margin_usd_amt,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME(('PST8PDT'))) AS DATETIME) AS dw_sys_load_tmstp
    FROM margin_aec AS jcmmv
    GROUP BY business_day_date,
     business_unit_desc,
     store_num,
     bill_zip_code,
     line_item_currency_code,
     inventory_business_model,
     division_name,
     subdivision_name,
     price_type,
     record_source,
     platform_code,
     remote_selling_ind,
     fulfilled_from_location_type,
     delivery_method,
     delivery_method_subtype,
     fulfillment_journey,
     loyalty_status,
     buyerflow_code,
     aare_acquired_ind,
     aare_activated_ind,
     aare_retained_ind,
     aare_engaged_ind,
     mrtk_chnl_type_code,
     mrtk_chnl_finance_rollup_code,
     mrtk_chnl_finance_detail_code,
     engagement_cohort,
     tran_tender_cash_flag,
     tran_tender_check_flag,
     tran_tender_nordstrom_card_flag,
     tran_tender_non_nordstrom_credit_flag,
     tran_tender_non_nordstrom_debit_flag,
     tran_tender_nordstrom_gift_card_flag,
     tran_tender_nordstrom_note_flag,
     tran_tender_paypal_flag
    UNION ALL
    SELECT business_day_date,
     business_unit_desc,
     intent_store_num AS store_num,
     bill_zip_code,
     line_item_currency_code,
     inventory_business_model,
     division_name,
     subdivision_name,
     price_type,
     record_source,
     platform_code,
     remote_selling_ind,
     fulfilled_from_location_type,
     delivery_method,
     delivery_method_subtype,
     NULL AS fulfillment_journey,
     loyalty_status,
     SUBSTR(NULL, 1, 27) AS buyerflow_code,
     'N' AS aare_acquired_ind,
     'N' AS aare_activated_ind,
     'N' AS aare_retained_ind,
     'N' AS aare_engaged_ind,
     mrtk_chnl_type_code,
     mrtk_chnl_finance_rollup_code,
     mrtk_chnl_finance_detail_code,
     SUBSTR(NULL, 1, 30) AS engagement_cohort,
     tran_tender_cash_flag,
     tran_tender_check_flag,
     tran_tender_nordstrom_card_flag,
     tran_tender_non_nordstrom_credit_flag,
     tran_tender_non_nordstrom_debit_flag,
     tran_tender_nordstrom_gift_card_flag,
     tran_tender_nordstrom_note_flag,
     tran_tender_paypal_flag,
     SUM(CASE
       WHEN LOWER(jwn_reported_gmv_ind) = LOWER('Y')
       THEN jwn_reported_gmv_usd_amt
       ELSE 0
       END) AS reported_gmv_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_reported_gmv_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS reported_gmv_units,
     SUM(CASE
       WHEN LOWER(jwn_merch_net_sales_ind) = LOWER('Y')
       THEN jwn_merch_net_sales_usd_amt
       ELSE 0
       END) AS merch_net_sales_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_merch_net_sales_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS merch_net_sales_units,
     SUM(CASE
       WHEN LOWER(jwn_reported_net_sales_ind) = LOWER('Y')
       THEN jwn_reported_net_sales_usd_amt
       ELSE 0
       END) AS reported_net_sales_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_reported_net_sales_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS reported_net_sales_units,
     SUM(jwn_variable_expense_usd_amt) AS variable_cost_usd_amt,
     SUM(CASE
       WHEN LOWER(line_item_activity_type_code) = LOWER('S')
       THEN jwn_contribution_margin_usd_amt
       ELSE 0
       END) AS gross_contribution_margin_usd_amt,
     SUM(jwn_contribution_margin_usd_amt) AS net_contribution_margin_usd_amt,
     CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME(('PST8PDT'))) AS DATETIME) AS dw_sys_load_tmstp
    FROM margin_acp_null AS man
    GROUP BY business_day_date,
     business_unit_desc,
     store_num,
     bill_zip_code,
     line_item_currency_code,
     inventory_business_model,
     division_name,
     subdivision_name,
     price_type,
     record_source,
     platform_code,
     remote_selling_ind,
     fulfilled_from_location_type,
     delivery_method,
     delivery_method_subtype,
     fulfillment_journey,
     loyalty_status,
     buyerflow_code,
     aare_acquired_ind,
     mrtk_chnl_type_code,
     mrtk_chnl_finance_rollup_code,
     mrtk_chnl_finance_detail_code,
     tran_tender_cash_flag,
     tran_tender_check_flag,
     tran_tender_nordstrom_card_flag,
     tran_tender_non_nordstrom_credit_flag,
     tran_tender_non_nordstrom_debit_flag,
     tran_tender_nordstrom_gift_card_flag,
     tran_tender_nordstrom_note_flag,
     tran_tender_paypal_flag,
     aare_activated_ind,
     aare_retained_ind,
     aare_engaged_ind,
     engagement_cohort) AS t5);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dsa_ai_base_vws.finance_margin_fact
WHERE business_day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 60 DAY) AND (CURRENT_DATE('PST8PDT'));
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dsa_ai_fct.finance_margin_fact (business_day_date, business_unit_desc, store_num, bill_zip_code,
 line_item_currency_code, inventory_business_model, division_name, subdivision_name, price_type, record_source,
 platform_code, remote_selling_ind, fulfilled_from_location_type, delivery_method, delivery_method_subtype,
 fulfillment_journey, loyalty_status, buyerflow_code, aare_acquired_ind, aare_activated_ind, aare_retained_ind,
 aare_engaged_ind, engagement_cohort, mrtk_chnl_type_code, mrtk_chnl_finance_rollup_code, mrtk_chnl_finance_detail_code
 , tran_tender_cash_flag, tran_tender_check_flag, tran_tender_nordstrom_card_flag, tran_tender_non_nordstrom_credit_flag
 , tran_tender_non_nordstrom_debit_flag, tran_tender_nordstrom_gift_card_flag, tran_tender_nordstrom_note_flag,
 tran_tender_paypal_flag, reported_gmv_usd_amt, reported_gmv_units, merch_net_sales_usd_amt, merch_net_sales_units,
 reported_net_sales_usd_amt, reported_net_sales_units, variable_cost_usd_amt, gross_contribution_margin_usd_amt,
 net_contribution_margin_usd_amt, dw_sys_load_tmstp)
(SELECT business_day_date,
  business_unit_desc,
  store_num,
  bill_zip_code,
  line_item_currency_code,
  inventory_business_model,
  division_name,
  subdivision_name,
  price_type,
  record_source,
  platform_code,
  remote_selling_ind,
  fulfilled_from_location_type,
  delivery_method,
  delivery_method_subtype,
  fulfillment_journey,
  loyalty_status,
  buyerflow_code,
  aare_acquired_ind,
  aare_activated_ind,
  aare_retained_ind,
  aare_engaged_ind,
  engagement_cohort,
  mrtk_chnl_type_code,
  mrtk_chnl_finance_rollup_code,
  mrtk_chnl_finance_detail_code,
  tran_tender_cash_flag,
  tran_tender_check_flag,
  tran_tender_nordstrom_card_flag,
  tran_tender_non_nordstrom_credit_flag,
  tran_tender_non_nordstrom_debit_flag,
  tran_tender_nordstrom_gift_card_flag,
  tran_tender_nordstrom_note_flag,
  tran_tender_paypal_flag,
  reported_gmv_usd_amt,
  reported_gmv_units,
  merch_net_sales_usd_amt,
  merch_net_sales_units,
  reported_net_sales_usd_amt,
  reported_net_sales_units,
  variable_cost_usd_amt,
  gross_contribution_margin_usd_amt,
  net_contribution_margin_usd_amt,
  dw_sys_load_tmstp
 FROM margin);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
