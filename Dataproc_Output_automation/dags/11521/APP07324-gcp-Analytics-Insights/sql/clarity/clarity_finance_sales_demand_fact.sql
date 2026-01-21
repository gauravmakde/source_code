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
   WHERE day_date BETWEEN {{params.start_date}} AND {{params.end_date}});


--COLLECT STATISTICS COLUMN(acp_id, year_num, business_unit_desc) ON buyerflow


--index this table one time to de-skew the joins later


CREATE TEMPORARY TABLE IF NOT EXISTS aec
AS
SELECT acp_id,
 execution_qtr,
 engagement_cohort
FROM `{{params.gcp_project_id}}`.t2dl_das_aec.audience_engagement_cohorts
WHERE execution_qtr_end_dt >= {{params.start_date}}
 AND execution_qtr_start_dt <= {{params.end_date}};


--COLLECT STATISTICS COLUMN(acp_id, execution_qtr) ON aec


--aggregate tender table (which has multiple rows per order) for later join


CREATE TEMPORARY TABLE IF NOT EXISTS tender
AS
SELECT transaction_id AS purchase_id,
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
WHERE business_day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND business_day_date BETWEEN DATE '2021-01-31' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
GROUP BY purchase_id;

CREATE TEMPORARY TABLE IF NOT EXISTS pre_sessions
AS
SELECT activity_date_pacific,
 mrkt_type,
 finance_rollup,
 finance_detail,
 session_id
FROM `{{params.gcp_project_id}}`.t2dl_das_sessions.dior_session_fact AS dsfd
WHERE activity_date_pacific BETWEEN DATE_SUB({{params.start_date}}, INTERVAL 30 DAY) AND {{params.end_date}}
 AND activity_date_pacific >= DATE '2022-01-31'
 AND LOWER(experience) IN (LOWER('ANDROID_APP'), LOWER('DESKTOP_WEB'), LOWER('IOS_APP'), LOWER('MOBILE_WEB'))
 AND (web_orders >= 1 OR activity_date_pacific < DATE '2022-02-09');


CREATE TEMPORARY TABLE IF NOT EXISTS sess_order_map
AS
SELECT activity_date,
 order_id,
 session_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_session_order_fact
WHERE activity_date BETWEEN DATE_SUB({{params.start_date}}, INTERVAL 30 DAY) AND {{params.end_date}}
 AND activity_date >= DATE '2022-01-31';

CREATE TEMPORARY TABLE IF NOT EXISTS channel
AS
SELECT ps.activity_date_pacific,
 som.order_id,
 ps.mrkt_type,
 ps.finance_rollup,
 ps.finance_detail
FROM pre_sessions AS ps
 INNER JOIN sess_order_map AS som ON LOWER(som.session_id) = LOWER(ps.session_id);


CREATE TEMPORARY TABLE IF NOT EXISTS jdmv (
demand_date DATE NOT NULL,
purchase_id STRING,
year_num INTEGER,
quarter_num INTEGER,
business_unit_desc STRING,
intent_store_num INTEGER,
ringing_store_num INTEGER,
bill_zip_code STRING,
line_item_currency_code STRING,
inventory_business_model STRING,
division_name STRING,
subdivision_name STRING,
price_type STRING,
record_source STRING,
platform_code STRING,
remote_selling_ind STRING,
fulfilled_from_location_type STRING,
delivery_method STRING,
delivery_method_subtype STRING,
loyalty_status STRING,
rms_sku_num STRING,
demand_tmstp_pacific TIMESTAMP,
demand_tmstp_pacific_tz STRING,
acp_id STRING,
cancel_reason_code STRING,
fulfillment_status STRING,
order_num STRING,
jwn_reported_demand_ind STRING,
employee_discount_ind STRING,
jwn_gross_demand_usd_amt NUMERIC(20,2),
demand_units INTEGER,
jwn_reported_demand_usd_amt NUMERIC(20,2),
employee_discount_usd_amt NUMERIC(20,2)
);

INSERT INTO jdmv
(SELECT jdmv.demand_date,
  jdmv.purchase_id,
  dc.year_num,
  dc.quarter_num,
  jdmv.business_unit_desc,
  jdmv.intent_store_num,
  jdmv.ringing_store_num,
  jdmv.bill_zip_code,
  jdmv.line_item_currency_code,
  jdmv.inventory_business_model,
   CASE
   WHEN LOWER(jdmv.division_name) LIKE LOWER('INACT%')
   THEN 'OTHER'
   WHEN LOWER(jdmv.division_name) IN (LOWER('DIRECT'), LOWER('DISCONTINUED DEPTS'), LOWER('INVENTORY INTEGRITY TEST'),
     LOWER('NPG NEW BUSINESS'), LOWER('MERCH PROJECTS'))
   THEN 'OTHER'
   WHEN jdmv.division_name IS NULL OR LOWER(jdmv.division_name) = LOWER('CORPORATE')
   THEN 'OTHER/NON-MERCH'
   ELSE jdmv.division_name
   END AS division_name,
   CASE
   WHEN jdmv.subdivision_num IN (770, 790, 775, 780, 710, 705, 700, 785)
   THEN CASE
    WHEN LOWER(jdmv.subdivision_name) = LOWER('KIDS WEAR')
    THEN 'KIDS APPAREL'
    ELSE jdmv.subdivision_name
    END
   ELSE NULL
   END AS subdivision_name,
  jdmv.price_type,
  jdmv.record_source,
   CASE
   WHEN LOWER(jdmv.order_platform_type) = LOWER('Mobile App')
   THEN CASE
    WHEN LOWER(jdmv.platform_subtype) IN (LOWER('UNKNOWN'), LOWER('UNKNOWN: Price Adjustment'))
    THEN 'Other'
    ELSE jdmv.platform_subtype
    END
   WHEN LOWER(jdmv.order_platform_type) IN (LOWER('BorderFree'), LOWER('Phone (Customer Care)'), LOWER('UNKNOWN'), LOWER('UNKNOWN: Price Adjustment'
      ), LOWER('UNKNOWN_VALUE'), LOWER('UNKNOWN_VALUE: Platform=UNKNOW'))
   THEN 'Other'
   ELSE jdmv.order_platform_type
   END AS platform_code,
  jdmv.remote_selling_ind,
  jdmv.fulfilled_from_location_type,
  jdmv.delivery_method,
   CASE
   WHEN LOWER(jdmv.delivery_method_subtype) = LOWER('UNKNOWN_VALUE')
   THEN 'Other'
   ELSE jdmv.delivery_method_subtype
   END AS delivery_method_subtype,
  jdmv.loyalty_status,
  jdmv.rms_sku_num,
  jdmv.demand_tmstp_pacific_utc,
  jdmv.demand_tmstp_pacific_tz,
  jdmv.acp_id,
  jdmv.cancel_reason_code,
  jdmv.fulfillment_status,
  jdmv.order_num,
  jdmv.jwn_reported_demand_ind,
  jdmv.employee_discount_ind,
  jdmv.jwn_gross_demand_usd_amt,
  jdmv.demand_units,
  CAST(jdmv.jwn_reported_demand_usd_amt AS NUMERIC) AS jwn_reported_demand_usd_amt,
  jdmv.employee_discount_usd_amt
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_demand_metric_vw AS jdmv
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS dc ON jdmv.demand_date = dc.day_date
 WHERE jdmv.demand_date BETWEEN {{params.start_date}} AND {{params.end_date}}
  AND jdmv.demand_date BETWEEN DATE '2021-01-31' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
  AND LOWER(jdmv.zero_value_unit_ind) = LOWER('N')
  AND (LOWER(jdmv.business_unit_country) <> LOWER('CA') OR jdmv.business_day_date <= DATE '2023-02-25'));

CREATE TEMPORARY TABLE IF NOT EXISTS price_type
AS
SELECT rms_sku_num,
 eff_begin_tmstp_utc as eff_begin_tmstp,
 eff_begin_tmstp_tz,
 eff_end_tmstp_utc as eff_end_tmstp,
 eff_end_tmstp_tz,
  CASE
  WHEN LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'Clearance'
  WHEN LOWER(selling_retail_price_type_code) = LOWER('CLEARANCE')
  THEN 'Clearance'
  WHEN LOWER(selling_retail_price_type_code) = LOWER('PROMOTION')
  THEN 'Promotion'
  WHEN LOWER(selling_retail_price_type_code) = LOWER('REGULAR')
  THEN 'Regular Price'
  ELSE NULL
  END AS price_type,
 store_num,
  CASE
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK') AND LOWER(selling_channel) = LOWER('STORE') AND LOWER(channel_country
     ) = LOWER('CA')
  THEN 'RACK CANADA'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK') AND LOWER(selling_channel) = LOWER('STORE') AND LOWER(channel_country
     ) = LOWER('US')
  THEN 'RACK'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM_RACK') AND LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(channel_country
     ) = LOWER('US')
  THEN 'OFFPRICE ONLINE'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(selling_channel) = LOWER('STORE') AND LOWER(channel_country)
    = LOWER('CA')
  THEN 'FULL LINE CANADA'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(selling_channel) = LOWER('STORE') AND LOWER(channel_country)
    = LOWER('US')
  THEN 'FULL LINE'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(channel_country
     ) = LOWER('CA')
  THEN 'N.CA'
  WHEN LOWER(channel_brand) = LOWER('NORDSTROM') AND LOWER(selling_channel) = LOWER('ONLINE') AND LOWER(channel_country
     ) = LOWER('US')
  THEN 'N.COM'
  ELSE NULL
  END AS business_unit_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_price_timeline_dim AS ppd
WHERE CAST(eff_end_tmstp AS DATE) >= CAST({{params.start_date}} as date)
 AND CAST(eff_begin_tmstp AS DATE) <= CAST({{params.end_date}} as date)
 AND (LOWER(ownership_retail_price_type_code) = LOWER('CLEARANCE') OR LOWER(selling_retail_price_type_code) IN (LOWER('CLEARANCE'
      ), LOWER('PROMOTION')))
 AND rms_sku_num IN (SELECT DISTINCT rms_sku_num
   FROM jdmv
   WHERE LOWER(price_type) = LOWER('NOT_APPLICABLE'));


--COLLECT STATISTICS COLUMN(rms_sku_num, eff_begin_tmstp, eff_end_tmstp, business_unit_desc) ON price_type


CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_tender AS
SELECT jdmv.demand_date,
 jdmv.year_num,
 jdmv.quarter_num,
 jdmv.business_unit_desc,
 jdmv.intent_store_num,
 jdmv.ringing_store_num,
 jdmv.bill_zip_code,
 jdmv.line_item_currency_code,
 jdmv.inventory_business_model,
 jdmv.division_name,
 jdmv.subdivision_name,
 SUBSTR(CASE
   WHEN LOWER(jdmv.price_type) <> LOWER('NOT_APPLICABLE')
   THEN jdmv.price_type
   WHEN ptc.price_type IS NOT NULL
   THEN ptc.price_type
   ELSE 'Regular Price'
   END, 1, 20) AS price_type,
 jdmv.record_source,
 jdmv.platform_code,
 jdmv.remote_selling_ind,
 jdmv.fulfilled_from_location_type,
 jdmv.delivery_method,
 jdmv.delivery_method_subtype,
 jdmv.loyalty_status,
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
 jdmv.acp_id,
 jdmv.cancel_reason_code,
 jdmv.fulfillment_status,
 jdmv.order_num,
 jdmv.jwn_reported_demand_ind,
 jdmv.employee_discount_ind,
 jdmv.jwn_gross_demand_usd_amt,
 jdmv.demand_units,
 jdmv.jwn_reported_demand_usd_amt,
 jdmv.employee_discount_usd_amt
FROM jdmv
 LEFT JOIN tender ON LOWER(tender.purchase_id) = LOWER(jdmv.purchase_id)
 LEFT JOIN price_type AS ptc ON LOWER(jdmv.rms_sku_num) = LOWER(ptc.rms_sku_num) AND LOWER(jdmv.business_unit_desc) =
     LOWER(ptc.business_unit_desc) AND jdmv.demand_tmstp_pacific BETWEEN ptc.eff_begin_tmstp AND ptc.eff_end_tmstp AND
   LOWER(jdmv.price_type) = LOWER('NOT_APPLICABLE');


CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_tender_order_null AS
SELECT demand_date,
 year_num,
 quarter_num,
 business_unit_desc,
 intent_store_num,
 ringing_store_num,
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
 tran_tender_cash_flag,
 tran_tender_check_flag,
 tran_tender_nordstrom_card_flag,
 tran_tender_non_nordstrom_credit_flag,
 tran_tender_non_nordstrom_debit_flag,
 tran_tender_nordstrom_gift_card_flag,
 tran_tender_nordstrom_note_flag,
 tran_tender_paypal_flag,
 acp_id,
 cancel_reason_code,
 fulfillment_status,
 order_num,
 jwn_reported_demand_ind,
 employee_discount_ind,
 jwn_gross_demand_usd_amt,
 demand_units,
 jwn_reported_demand_usd_amt,
 employee_discount_usd_amt
FROM jdmv_tender AS jdmv
WHERE order_num IS NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_tender_order_not_null
AS
SELECT demand_date,
 year_num,
 quarter_num,
 business_unit_desc,
 intent_store_num,
 ringing_store_num,
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
 tran_tender_cash_flag,
 tran_tender_check_flag,
 tran_tender_nordstrom_card_flag,
 tran_tender_non_nordstrom_credit_flag,
 tran_tender_non_nordstrom_debit_flag,
 tran_tender_nordstrom_gift_card_flag,
 tran_tender_nordstrom_note_flag,
 tran_tender_paypal_flag,
 acp_id,
 cancel_reason_code,
 fulfillment_status,
 order_num,
 jwn_reported_demand_ind,
 employee_discount_ind,
 jwn_gross_demand_usd_amt,
 demand_units,
 jwn_reported_demand_usd_amt,
 employee_discount_usd_amt
FROM jdmv_tender AS jdmv
WHERE order_num IS NOT NULL;


--COLLECT STATISTICS COLUMN(order_num,demand_date) ON jdmv_tender_order_not_null


CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_channel AS
SELECT jdmv.demand_date,
 jdmv.year_num,
 jdmv.quarter_num,
 jdmv.business_unit_desc,
 jdmv.intent_store_num,
 jdmv.ringing_store_num,
 jdmv.bill_zip_code,
 jdmv.line_item_currency_code,
 jdmv.inventory_business_model,
 jdmv.division_name,
 jdmv.subdivision_name,
 jdmv.price_type,
 jdmv.record_source,
 jdmv.platform_code,
 jdmv.remote_selling_ind,
 jdmv.fulfilled_from_location_type,
 jdmv.delivery_method,
 jdmv.delivery_method_subtype,
 jdmv.loyalty_status,
 mc.mrkt_type AS mrtk_chnl_type_code,
 mc.finance_rollup AS mrtk_chnl_finance_rollup_code,
 mc.finance_detail AS mrtk_chnl_finance_detail_code,
 jdmv.tran_tender_cash_flag,
 jdmv.tran_tender_check_flag,
 jdmv.tran_tender_nordstrom_card_flag,
 jdmv.tran_tender_non_nordstrom_credit_flag,
 jdmv.tran_tender_non_nordstrom_debit_flag,
 jdmv.tran_tender_nordstrom_gift_card_flag,
 jdmv.tran_tender_nordstrom_note_flag,
 jdmv.tran_tender_paypal_flag,
 jdmv.acp_id,
 jdmv.cancel_reason_code,
 jdmv.fulfillment_status,
 jdmv.order_num,
 jdmv.jwn_reported_demand_ind,
 jdmv.employee_discount_ind,
 jdmv.jwn_gross_demand_usd_amt,
 jdmv.demand_units,
 jdmv.jwn_reported_demand_usd_amt,
 jdmv.employee_discount_usd_amt
FROM jdmv_tender_order_not_null AS jdmv
 LEFT JOIN channel AS mc ON LOWER(jdmv.order_num) = LOWER(mc.order_id) AND jdmv.demand_date = mc.activity_date_pacific
UNION ALL
SELECT demand_date,
 year_num,
 quarter_num,
 business_unit_desc,
 intent_store_num,
 ringing_store_num,
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
 acp_id,
 cancel_reason_code,
 fulfillment_status,
 order_num,
 jwn_reported_demand_ind,
 employee_discount_ind,
 jwn_gross_demand_usd_amt,
 demand_units,
 jwn_reported_demand_usd_amt,
 employee_discount_usd_amt
FROM jdmv_tender_order_null AS jdmv;

CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_acp_null AS
SELECT demand_date,
 year_num,
 quarter_num,
 business_unit_desc,
 intent_store_num,
 ringing_store_num,
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
 acp_id,
 cancel_reason_code,
 fulfillment_status,
 order_num,
 jwn_reported_demand_ind,
 employee_discount_ind,
 jwn_gross_demand_usd_amt,
 demand_units,
 jwn_reported_demand_usd_amt,
 employee_discount_usd_amt
FROM jdmv_channel AS jdmv
WHERE acp_id IS NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_acp_not_null
AS
SELECT demand_date,
 year_num,
 quarter_num,
 business_unit_desc,
 intent_store_num,
 ringing_store_num,
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
 acp_id,
 cancel_reason_code,
 fulfillment_status,
 order_num,
 jwn_reported_demand_ind,
 employee_discount_ind,
 jwn_gross_demand_usd_amt,
 demand_units,
 jwn_reported_demand_usd_amt,
 employee_discount_usd_amt
FROM jdmv_channel AS jdmv
WHERE acp_id IS NOT NULL;


--collect statistics column (acp_id, quarter_num) on jdmv_acp_not_null


CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_aec
AS
SELECT jdmv.demand_date,
 jdmv.year_num,
 jdmv.business_unit_desc,
 jdmv.intent_store_num,
 jdmv.ringing_store_num,
 jdmv.bill_zip_code,
 jdmv.line_item_currency_code,
 jdmv.inventory_business_model,
 jdmv.division_name,
 jdmv.subdivision_name,
 jdmv.price_type,
 jdmv.record_source,
 jdmv.platform_code,
 jdmv.remote_selling_ind,
 jdmv.fulfilled_from_location_type,
 jdmv.delivery_method,
 jdmv.delivery_method_subtype,
 jdmv.loyalty_status,
 aec.engagement_cohort,
 jdmv.mrtk_chnl_type_code,
 jdmv.mrtk_chnl_finance_rollup_code,
 jdmv.mrtk_chnl_finance_detail_code,
 jdmv.tran_tender_cash_flag,
 jdmv.tran_tender_check_flag,
 jdmv.tran_tender_nordstrom_card_flag,
 jdmv.tran_tender_non_nordstrom_credit_flag,
 jdmv.tran_tender_non_nordstrom_debit_flag,
 jdmv.tran_tender_nordstrom_gift_card_flag,
 jdmv.tran_tender_nordstrom_note_flag,
 jdmv.tran_tender_paypal_flag,
 jdmv.acp_id,
 jdmv.cancel_reason_code,
 jdmv.fulfillment_status,
 jdmv.order_num,
 jdmv.jwn_reported_demand_ind,
 jdmv.employee_discount_ind,
 jdmv.jwn_gross_demand_usd_amt,
 jdmv.demand_units,
 jdmv.jwn_reported_demand_usd_amt,
 jdmv.employee_discount_usd_amt
FROM jdmv_acp_not_null AS jdmv
 LEFT JOIN aec ON LOWER(jdmv.acp_id) = LOWER(aec.acp_id) AND jdmv.quarter_num = aec.execution_qtr;


--collect statistics column (acp_id, year_num, business_unit_desc) on jdmv_aec


CREATE TEMPORARY TABLE IF NOT EXISTS jdmv_buyerflow AS
SELECT jdmv.demand_date,
 jdmv.business_unit_desc,
 jdmv.intent_store_num,
 jdmv.ringing_store_num,
 jdmv.bill_zip_code,
 jdmv.line_item_currency_code,
 jdmv.inventory_business_model,
 jdmv.division_name,
 jdmv.subdivision_name,
 jdmv.price_type,
 jdmv.record_source,
 jdmv.platform_code,
 jdmv.remote_selling_ind,
 jdmv.fulfilled_from_location_type,
 jdmv.delivery_method,
 jdmv.delivery_method_subtype,
 jdmv.loyalty_status,
 jdmv.engagement_cohort,
 bf.buyerflow_code,
 bf.aare_acquired_ind,
 bf.aare_activated_ind,
 bf.aare_retained_ind,
 bf.aare_engaged_ind,
 jdmv.mrtk_chnl_type_code,
 jdmv.mrtk_chnl_finance_rollup_code,
 jdmv.mrtk_chnl_finance_detail_code,
 jdmv.tran_tender_cash_flag,
 jdmv.tran_tender_check_flag,
 jdmv.tran_tender_nordstrom_card_flag,
 jdmv.tran_tender_non_nordstrom_credit_flag,
 jdmv.tran_tender_non_nordstrom_debit_flag,
 jdmv.tran_tender_nordstrom_gift_card_flag,
 jdmv.tran_tender_nordstrom_note_flag,
 jdmv.tran_tender_paypal_flag,
 jdmv.cancel_reason_code,
 jdmv.fulfillment_status,
 jdmv.order_num,
 jdmv.jwn_reported_demand_ind,
 jdmv.employee_discount_ind,
 jdmv.jwn_gross_demand_usd_amt,
 jdmv.demand_units,
 jdmv.jwn_reported_demand_usd_amt,
 jdmv.employee_discount_usd_amt
FROM jdmv_aec AS jdmv
 LEFT JOIN buyerflow AS bf ON LOWER(jdmv.business_unit_desc) = LOWER(bf.business_unit_desc) AND jdmv.year_num = bf.year_num
     AND LOWER(jdmv.acp_id) = LOWER(bf.acp_id);


CREATE TEMPORARY TABLE IF NOT EXISTS demand (
demand_date DATE NOT NULL,
business_unit_desc STRING,
store_num INTEGER,
bill_zip_code STRING,
line_item_currency_code STRING,
inventory_business_model STRING,
division_name STRING,
subdivision_name STRING,
price_type STRING,
record_source STRING,
platform_code STRING,
remote_selling_ind STRING,
fulfilled_from_location_type STRING,
delivery_method STRING,
delivery_method_subtype STRING,
loyalty_status STRING,
buyerflow_code STRING,
aare_acquired_ind STRING,
aare_activated_ind STRING,
aare_retained_ind STRING,
aare_engaged_ind STRING,
mrtk_chnl_type_code STRING,
mrtk_chnl_finance_rollup_code STRING,
mrtk_chnl_finance_detail_code STRING,
engagement_cohort STRING,
tran_tender_cash_flag STRING,
tran_tender_check_flag STRING,
tran_tender_nordstrom_card_flag STRING,
tran_tender_non_nordstrom_credit_flag STRING,
tran_tender_non_nordstrom_debit_flag STRING,
tran_tender_nordstrom_gift_card_flag STRING,
tran_tender_nordstrom_note_flag STRING,
tran_tender_paypal_flag STRING,
gross_demand_usd_amt NUMERIC(20,2),
gross_demand_units INTEGER,
canceled_gross_demand_usd_amt NUMERIC(20,2),
canceled_gross_demand_units INTEGER,
reported_demand_usd_amt NUMERIC(20,2),
reported_demand_units INTEGER,
canceled_reported_demand_usd_amt NUMERIC(20,2),
canceled_reported_demand_units INTEGER,
emp_disc_gross_demand_usd_amt NUMERIC(20,2),
emp_disc_gross_demand_units INTEGER,
emp_disc_reported_demand_usd_amt NUMERIC(20,2),
emp_disc_reported_demand_units INTEGER
);


INSERT INTO demand
(SELECT demand_date,
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
  gross_demand_usd_amt,
  gross_demand_units,
  CAST(canceled_gross_demand_usd_amt AS NUMERIC) AS canceled_gross_demand_usd_amt,
  canceled_gross_demand_units,
  reported_demand_usd_amt,
  reported_demand_units,
  CAST(canceled_reported_demand_usd_amt AS NUMERIC) AS canceled_reported_demand_usd_amt,
  canceled_reported_demand_units,
  emp_disc_gross_demand_usd_amt,
  emp_disc_gross_demand_units,
  CAST(emp_disc_reported_demand_usd_amt AS NUMERIC) AS emp_disc_reported_demand_usd_amt,
  emp_disc_reported_demand_units
 FROM (SELECT demand_date,
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
     SUM(jwn_gross_demand_usd_amt) AS gross_demand_usd_amt,
     SUM(demand_units) AS gross_demand_units,
     SUM(CASE
       WHEN cancel_reason_code IS NOT NULL AND LOWER(cancel_reason_code) <> LOWER('UNKNOWN_VALUE')
       THEN jwn_gross_demand_usd_amt
       ELSE 0
       END) AS canceled_gross_demand_usd_amt,
     SUM(CASE
       WHEN cancel_reason_code IS NOT NULL AND LOWER(cancel_reason_code) <> LOWER('UNKNOWN_VALUE')
       THEN demand_units
       ELSE 0
       END) AS canceled_gross_demand_units,
     SUM(jwn_reported_demand_usd_amt) AS reported_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_reported_demand_ind) = LOWER('Y')
       THEN demand_units
       ELSE 0
       END) AS reported_demand_units,
     SUM(CASE
       WHEN cancel_reason_code IS NOT NULL AND LOWER(cancel_reason_code) <> LOWER('UNKNOWN_VALUE')
       THEN jwn_reported_demand_usd_amt
       ELSE 0
       END) AS canceled_reported_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(cancel_reason_code) <> LOWER('UNKNOWN_VALUE') AND LOWER(COALESCE(jwn_reported_demand_ind, 'N')) =
          LOWER('Y') AND cancel_reason_code IS NOT NULL
       THEN demand_units
       ELSE 0
       END) AS canceled_reported_demand_units,
     SUM(employee_discount_usd_amt) AS emp_disc_gross_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(employee_discount_ind) = LOWER('Y')
       THEN demand_units
       ELSE 0
       END) AS emp_disc_gross_demand_units,
     SUM(CASE
       WHEN LOWER(jwn_reported_demand_ind) = LOWER('Y')
       THEN employee_discount_usd_amt
       ELSE 0
       END) AS emp_disc_reported_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_reported_demand_ind) = LOWER('Y') AND LOWER(employee_discount_ind) = LOWER('Y')
       THEN demand_units
       ELSE 0
       END) AS emp_disc_reported_demand_units
    FROM jdmv_buyerflow AS jdmv
    GROUP BY demand_date,
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
    SELECT demand_date,
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
     SUM(jwn_gross_demand_usd_amt) AS gross_demand_usd_amt,
     SUM(demand_units) AS gross_demand_units,
     SUM(CASE
       WHEN cancel_reason_code IS NOT NULL AND LOWER(cancel_reason_code) <> LOWER('UNKNOWN_VALUE')
       THEN jwn_gross_demand_usd_amt
       ELSE 0
       END) AS canceled_gross_demand_usd_amt,
     SUM(CASE
       WHEN cancel_reason_code IS NOT NULL AND LOWER(cancel_reason_code) <> LOWER('UNKNOWN_VALUE')
       THEN demand_units
       ELSE 0
       END) AS canceled_gross_demand_units,
     SUM(jwn_reported_demand_usd_amt) AS reported_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_reported_demand_ind) = LOWER('Y')
       THEN demand_units
       ELSE 0
       END) AS reported_demand_units,
     SUM(CASE
       WHEN cancel_reason_code IS NOT NULL AND LOWER(cancel_reason_code) <> LOWER('UNKNOWN_VALUE')
       THEN jwn_reported_demand_usd_amt
       ELSE 0
       END) AS canceled_reported_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(cancel_reason_code) <> LOWER('UNKNOWN_VALUE') AND LOWER(COALESCE(jwn_reported_demand_ind, 'N')) =
          LOWER('Y') AND cancel_reason_code IS NOT NULL
       THEN demand_units
       ELSE 0
       END) AS canceled_reported_demand_units,
     SUM(employee_discount_usd_amt) AS emp_disc_gross_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(employee_discount_ind) = LOWER('Y')
       THEN demand_units
       ELSE 0
       END) AS emp_disc_gross_demand_units,
     SUM(CASE
       WHEN LOWER(jwn_reported_demand_ind) = LOWER('Y')
       THEN employee_discount_usd_amt
       ELSE 0
       END) AS emp_disc_reported_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_reported_demand_ind) = LOWER('Y') AND LOWER(employee_discount_ind) = LOWER('Y')
       THEN demand_units
       ELSE 0
       END) AS emp_disc_reported_demand_units
    FROM jdmv_acp_null AS jdmv
    GROUP BY demand_date,
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


CREATE TEMPORARY TABLE IF NOT EXISTS preorders_buyerflow AS
SELECT jdmv.demand_date,
  CASE
  WHEN CASE
    WHEN LOWER(jdmv.platform_code) = LOWER('Direct to Customer (DTC)')
    THEN 808
    ELSE jdmv.ringing_store_num
    END = 808
  THEN 'N.COM'
  WHEN CASE
    WHEN LOWER(jdmv.platform_code) = LOWER('Direct to Customer (DTC)')
    THEN 808
    ELSE jdmv.ringing_store_num
    END = 828
  THEN 'OFFPRICE ONLINE'
  ELSE NULL
  END AS business_unit_desc_ringing,
  CASE
  WHEN LOWER(jdmv.platform_code) = LOWER('Direct to Customer (DTC)')
  THEN 808
  ELSE jdmv.ringing_store_num
  END AS store_num,
 jdmv.bill_zip_code,
 jdmv.line_item_currency_code,
 jdmv.platform_code,
 jdmv.loyalty_status,
 bf.buyerflow_code,
 bf.aare_acquired_ind,
 bf.aare_activated_ind,
 bf.aare_retained_ind,
 bf.aare_engaged_ind,
 jdmv.engagement_cohort,
 jdmv.mrtk_chnl_type_code,
 jdmv.mrtk_chnl_finance_rollup_code,
 jdmv.mrtk_chnl_finance_detail_code,
 jdmv.tran_tender_cash_flag,
 jdmv.tran_tender_check_flag,
 jdmv.tran_tender_nordstrom_card_flag,
 jdmv.tran_tender_non_nordstrom_credit_flag,
 jdmv.tran_tender_non_nordstrom_debit_flag,
 jdmv.tran_tender_nordstrom_gift_card_flag,
 jdmv.tran_tender_nordstrom_note_flag,
 jdmv.tran_tender_paypal_flag,
 jdmv.fulfillment_status,
 jdmv.acp_id,
 jdmv.cancel_reason_code,
 jdmv.employee_discount_ind,
 jdmv.order_num
FROM jdmv_aec AS jdmv
 LEFT JOIN buyerflow AS bf ON LOWER(CASE
      WHEN CASE
        WHEN LOWER(jdmv.platform_code) = LOWER('Direct to Customer (DTC)')
        THEN 808
        ELSE jdmv.ringing_store_num
        END = 808
      THEN 'N.COM'
      WHEN CASE
        WHEN LOWER(jdmv.platform_code) = LOWER('Direct to Customer (DTC)')
        THEN 808
        ELSE jdmv.ringing_store_num
        END = 828
      THEN 'OFFPRICE ONLINE'
      ELSE NULL
      END) = LOWER(bf.business_unit_desc) AND jdmv.year_num = bf.year_num AND LOWER(jdmv.acp_id) = LOWER(bf.acp_id)
WHERE LOWER(jdmv.record_source) = LOWER('O')
 AND jdmv.order_num IS NOT NULL
UNION ALL
SELECT demand_date,
  CASE
  WHEN CASE
    WHEN LOWER(platform_code) = LOWER('Direct to Customer (DTC)')
    THEN 808
    ELSE ringing_store_num
    END = 808
  THEN 'N.COM'
  WHEN CASE
    WHEN LOWER(platform_code) = LOWER('Direct to Customer (DTC)')
    THEN 808
    ELSE ringing_store_num
    END = 828
  THEN 'OFFPRICE ONLINE'
  ELSE NULL
  END AS business_unit_desc_ringing,
  CASE
  WHEN LOWER(platform_code) = LOWER('Direct to Customer (DTC)')
  THEN 808
  ELSE ringing_store_num
  END AS store_num,
 bill_zip_code,
 line_item_currency_code,
 platform_code,
 loyalty_status,
 SUBSTR(NULL, 1, 27) AS buyerflow_code,
 'N' AS aare_acquired_ind,
 'N' AS aare_activated_ind,
 'N' AS aare_retained_ind,
 'N' AS aare_engaged_ind,
 SUBSTR(NULL, 1, 30) AS engagement_cohort,
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
 fulfillment_status,
 acp_id,
 cancel_reason_code,
 employee_discount_ind,
 order_num
FROM jdmv_acp_null AS jdmv
WHERE LOWER(record_source) = LOWER('O')
 AND order_num IS NOT NULL;

CREATE TEMPORARY TABLE IF NOT EXISTS preorders AS
SELECT demand_date,
 business_unit_desc_ringing,
 store_num,
 bill_zip_code,
 line_item_currency_code,
 platform_code,
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
 cancel_reason_code,
 employee_discount_ind,
 order_num,
 RANK() OVER (PARTITION BY order_num, business_unit_desc_ringing ORDER BY fulfillment_status DESC, acp_id DESC,
    loyalty_status) AS rank_col
FROM preorders_buyerflow AS jdmv;


CREATE TEMPORARY TABLE IF NOT EXISTS orders AS
SELECT demand_date AS tran_date,
 business_unit_desc_ringing AS business_unit_desc,
 store_num,
 bill_zip_code,
 line_item_currency_code,
 platform_code,
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
 COUNT(DISTINCT order_num) AS orders_count,
 COUNT(DISTINCT CASE
   WHEN LOWER(cancel_reason_code) = LOWER('Fraud Cancel')
   THEN order_num
   ELSE NULL
   END) AS canceled_fraud_orders_count,
 COUNT(DISTINCT CASE
   WHEN LOWER(employee_discount_ind) = LOWER('Y')
   THEN order_num
   ELSE NULL
   END) AS emp_disc_orders_count
FROM preorders
WHERE rank_col = 1
GROUP BY tran_date,
 business_unit_desc,
 store_num,
 bill_zip_code,
 line_item_currency_code,
 platform_code,
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
 tran_tender_paypal_flag;

CREATE TEMPORARY TABLE IF NOT EXISTS jogmv
AS
SELECT jogmv.business_day_date,
 jogmv.demand_date,
 jogmv.purchase_id,
 dc.year_num,
 dc.quarter_num,
 jogmv.business_unit_desc,
 jogmv.intent_store_num,
 jogmv.bill_zip_code,
 jogmv.line_item_currency_code,
 jogmv.inventory_business_model,
  CASE
  WHEN LOWER(jogmv.division_name) LIKE LOWER('INACT%')
  THEN 'OTHER'
  WHEN LOWER(jogmv.division_name) IN (LOWER('DIRECT'), LOWER('DISCONTINUED DEPTS'), LOWER('INVENTORY INTEGRITY TEST'),
    LOWER('NPG NEW BUSINESS'), LOWER('MERCH PROJECTS'))
  THEN 'OTHER'
  WHEN jogmv.division_name IS NULL OR LOWER(jogmv.division_name) = LOWER('CORPORATE')
  THEN 'OTHER/NON-MERCH'
  ELSE jogmv.division_name
  END AS division_name,
  CASE
  WHEN jogmv.subdivision_num IN (770, 790, 775, 780, 710, 705, 700, 785)
  THEN CASE
   WHEN LOWER(jogmv.subdivision_name) = LOWER('KIDS WEAR')
   THEN 'KIDS APPAREL'
   ELSE jogmv.subdivision_name
   END
  ELSE NULL
  END AS subdivision_name,
 jogmv.price_type,
 jogmv.record_source,
  CASE
  WHEN LOWER(jogmv.order_platform_type) = LOWER('Mobile App')
  THEN CASE
   WHEN LOWER(jogmv.platform_subtype) IN (LOWER('UNKNOWN'), LOWER('UNKNOWN: Price Adjustment'))
   THEN 'Other'
   ELSE jogmv.platform_subtype
   END
  WHEN LOWER(jogmv.order_platform_type) IN (LOWER('BorderFree'), LOWER('Phone (Customer Care)'), LOWER('UNKNOWN'), LOWER('UNKNOWN: Price Adjustment'
     ), LOWER('UNKNOWN_VALUE'), LOWER('UNKNOWN_VALUE: Platform=UNKNOW'))
  THEN 'Other'
  ELSE jogmv.order_platform_type
  END AS platform_code,
 jogmv.remote_selling_ind,
 jogmv.fulfilled_from_location_type,
 jogmv.delivery_method,
  CASE
  WHEN LOWER(jogmv.delivery_method_subtype) = LOWER('UNKNOWN_VALUE')
  THEN 'Other'
  ELSE jogmv.delivery_method_subtype
  END AS delivery_method_subtype,
 jogmv.loyalty_status,
 jogmv.acp_id,
 jogmv.order_num,
 jogmv.employee_discount_ind,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.price_adjustment_ind,
 jogmv.product_return_ind,
 jogmv.same_day_store_return_ind,
 jogmv.service_type,
 jogmv.employee_discount_usd_amt,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_jwn_metrics_base_vws.jwn_operational_gmv_metric_vw AS jogmv
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal AS dc ON jogmv.business_day_date = dc.day_date
WHERE jogmv.business_day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
 AND jogmv.business_day_date BETWEEN DATE '2021-01-31' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
 AND LOWER(jogmv.zero_value_unit_ind) = LOWER('N')
 AND (LOWER(jogmv.business_unit_country) <> LOWER('CA') OR jogmv.business_day_date <= DATE '2023-02-25');


--collect statistics column (purchase_id) on jogmv


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_tender AS
SELECT jogmv.business_day_date,
 jogmv.demand_date,
 jogmv.year_num,
 jogmv.quarter_num,
 jogmv.business_unit_desc,
 jogmv.intent_store_num AS store_num,
 jogmv.bill_zip_code,
 jogmv.line_item_currency_code,
 jogmv.inventory_business_model,
 jogmv.division_name,
 jogmv.subdivision_name,
 jogmv.price_type,
 jogmv.record_source,
 jogmv.platform_code,
 jogmv.remote_selling_ind,
 jogmv.fulfilled_from_location_type,
 jogmv.delivery_method,
 jogmv.delivery_method_subtype,
 jogmv.loyalty_status,
 jogmv.acp_id,
 jogmv.order_num,
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
 jogmv.employee_discount_ind,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.price_adjustment_ind,
 jogmv.product_return_ind,
 jogmv.same_day_store_return_ind,
 jogmv.service_type,
 jogmv.employee_discount_usd_amt,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM jogmv
 LEFT JOIN tender ON LOWER(tender.purchase_id) = LOWER(jogmv.purchase_id);


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_tender_order_null AS
SELECT business_day_date,
 year_num,
 quarter_num,
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
 loyalty_status,
 acp_id,
 tran_tender_cash_flag,
 tran_tender_check_flag,
 tran_tender_nordstrom_card_flag,
 tran_tender_non_nordstrom_credit_flag,
 tran_tender_non_nordstrom_debit_flag,
 tran_tender_nordstrom_gift_card_flag,
 tran_tender_nordstrom_note_flag,
 tran_tender_paypal_flag,
 employee_discount_ind,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 price_adjustment_ind,
 product_return_ind,
 same_day_store_return_ind,
 service_type,
 employee_discount_usd_amt,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM gmv_tender AS jogmv
WHERE order_num IS NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_tender_order_not_null
AS
SELECT business_day_date,
 demand_date,
 year_num,
 quarter_num,
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
 employee_discount_ind,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 price_adjustment_ind,
 product_return_ind,
 same_day_store_return_ind,
 service_type,
 employee_discount_usd_amt,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM gmv_tender AS jogmv
WHERE order_num IS NOT NULL;


--COLLECT STATISTICS COLUMN(order_num,demand_date) ON gmv_tender_order_not_null


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_channel AS
SELECT jogmv.business_day_date,
 jogmv.year_num,
 jogmv.quarter_num,
 jogmv.business_unit_desc,
 jogmv.store_num,
 jogmv.bill_zip_code,
 jogmv.line_item_currency_code,
 jogmv.inventory_business_model,
 jogmv.division_name,
 jogmv.subdivision_name,
 jogmv.price_type,
 jogmv.record_source,
 jogmv.platform_code,
 jogmv.remote_selling_ind,
 jogmv.fulfilled_from_location_type,
 jogmv.delivery_method,
 jogmv.delivery_method_subtype,
 jogmv.loyalty_status,
 jogmv.acp_id,
 mc.mrkt_type AS mrtk_chnl_type_code,
 mc.finance_rollup AS mrtk_chnl_finance_rollup_code,
 mc.finance_detail AS mrtk_chnl_finance_detail_code,
 jogmv.tran_tender_cash_flag,
 jogmv.tran_tender_check_flag,
 jogmv.tran_tender_nordstrom_card_flag,
 jogmv.tran_tender_non_nordstrom_credit_flag,
 jogmv.tran_tender_non_nordstrom_debit_flag,
 jogmv.tran_tender_nordstrom_gift_card_flag,
 jogmv.tran_tender_nordstrom_note_flag,
 jogmv.tran_tender_paypal_flag,
 jogmv.employee_discount_ind,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.price_adjustment_ind,
 jogmv.product_return_ind,
 jogmv.same_day_store_return_ind,
 jogmv.service_type,
 jogmv.employee_discount_usd_amt,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM gmv_tender_order_not_null AS jogmv
 LEFT JOIN channel AS mc ON LOWER(jogmv.order_num) = LOWER(mc.order_id) AND jogmv.demand_date = mc.activity_date_pacific
   
UNION ALL
SELECT business_day_date,
 year_num,
 quarter_num,
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
 employee_discount_ind,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 price_adjustment_ind,
 product_return_ind,
 same_day_store_return_ind,
 service_type,
 employee_discount_usd_amt,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM gmv_tender_order_null AS jogmv;


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_acp_null AS
SELECT business_day_date,
 year_num,
 quarter_num,
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
 loyalty_status,
 acp_id,
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
 employee_discount_ind,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 price_adjustment_ind,
 product_return_ind,
 same_day_store_return_ind,
 service_type,
 employee_discount_usd_amt,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM gmv_channel AS jogmv
WHERE acp_id IS NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_acp_not_null
AS
SELECT business_day_date,
 year_num,
 quarter_num,
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
 loyalty_status,
 acp_id,
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
 employee_discount_ind,
 jwn_fulfilled_demand_ind,
 jwn_operational_gmv_ind,
 price_adjustment_ind,
 product_return_ind,
 same_day_store_return_ind,
 service_type,
 employee_discount_usd_amt,
 jwn_fulfilled_demand_usd_amt,
 line_item_quantity,
 operational_gmv_usd_amt,
 operational_gmv_units
FROM gmv_channel AS jogmv
WHERE acp_id IS NOT NULL;


--collect statistics column (acp_id, year_num, business_unit_desc) on gmv_acp_not_null


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_buyerflow
AS
SELECT jogmv.business_day_date,
 jogmv.quarter_num,
 jogmv.business_unit_desc,
 jogmv.store_num,
 jogmv.bill_zip_code,
 jogmv.line_item_currency_code,
 jogmv.inventory_business_model,
 jogmv.division_name,
 jogmv.subdivision_name,
 jogmv.price_type,
 jogmv.record_source,
 jogmv.platform_code,
 jogmv.remote_selling_ind,
 jogmv.fulfilled_from_location_type,
 jogmv.delivery_method,
 jogmv.delivery_method_subtype,
 jogmv.loyalty_status,
 jogmv.acp_id,
 bf.buyerflow_code,
 bf.aare_acquired_ind,
 bf.aare_activated_ind,
 bf.aare_retained_ind,
 bf.aare_engaged_ind,
 jogmv.mrtk_chnl_type_code,
 jogmv.mrtk_chnl_finance_rollup_code,
 jogmv.mrtk_chnl_finance_detail_code,
 jogmv.tran_tender_cash_flag,
 jogmv.tran_tender_check_flag,
 jogmv.tran_tender_nordstrom_card_flag,
 jogmv.tran_tender_non_nordstrom_credit_flag,
 jogmv.tran_tender_non_nordstrom_debit_flag,
 jogmv.tran_tender_nordstrom_gift_card_flag,
 jogmv.tran_tender_nordstrom_note_flag,
 jogmv.tran_tender_paypal_flag,
 jogmv.employee_discount_ind,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.price_adjustment_ind,
 jogmv.product_return_ind,
 jogmv.same_day_store_return_ind,
 jogmv.service_type,
 jogmv.employee_discount_usd_amt,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM gmv_acp_not_null AS jogmv
 LEFT JOIN buyerflow AS bf ON LOWER(jogmv.business_unit_desc) = LOWER(bf.business_unit_desc) AND jogmv.year_num = bf.year_num
     AND LOWER(jogmv.acp_id) = LOWER(bf.acp_id);


--collect statistics column (acp_id, quarter_num) on gmv_buyerflow


CREATE TEMPORARY TABLE IF NOT EXISTS gmv_aec AS
SELECT jogmv.business_day_date,
 jogmv.business_unit_desc,
 jogmv.store_num,
 jogmv.bill_zip_code,
 jogmv.line_item_currency_code,
 jogmv.inventory_business_model,
 jogmv.division_name,
 jogmv.subdivision_name,
 jogmv.price_type,
 jogmv.record_source,
 jogmv.platform_code,
 jogmv.remote_selling_ind,
 jogmv.fulfilled_from_location_type,
 jogmv.delivery_method,
 jogmv.delivery_method_subtype,
 jogmv.loyalty_status,
 jogmv.buyerflow_code,
 jogmv.aare_acquired_ind,
 jogmv.aare_activated_ind,
 jogmv.aare_retained_ind,
 jogmv.aare_engaged_ind,
 aec.engagement_cohort,
 jogmv.mrtk_chnl_type_code,
 jogmv.mrtk_chnl_finance_rollup_code,
 jogmv.mrtk_chnl_finance_detail_code,
 jogmv.tran_tender_cash_flag,
 jogmv.tran_tender_check_flag,
 jogmv.tran_tender_nordstrom_card_flag,
 jogmv.tran_tender_non_nordstrom_credit_flag,
 jogmv.tran_tender_non_nordstrom_debit_flag,
 jogmv.tran_tender_nordstrom_gift_card_flag,
 jogmv.tran_tender_nordstrom_note_flag,
 jogmv.tran_tender_paypal_flag,
 jogmv.employee_discount_ind,
 jogmv.jwn_fulfilled_demand_ind,
 jogmv.jwn_operational_gmv_ind,
 jogmv.price_adjustment_ind,
 jogmv.product_return_ind,
 jogmv.same_day_store_return_ind,
 jogmv.service_type,
 jogmv.employee_discount_usd_amt,
 jogmv.jwn_fulfilled_demand_usd_amt,
 jogmv.line_item_quantity,
 jogmv.operational_gmv_usd_amt,
 jogmv.operational_gmv_units
FROM gmv_buyerflow AS jogmv
 LEFT JOIN aec ON LOWER(jogmv.acp_id) = LOWER(aec.acp_id) AND jogmv.quarter_num = aec.execution_qtr;


CREATE TEMPORARY TABLE IF NOT EXISTS gmv (
business_day_date DATE NOT NULL,
business_unit_desc STRING,
store_num INTEGER,
bill_zip_code STRING,
line_item_currency_code STRING,
inventory_business_model STRING,
division_name STRING,
subdivision_name STRING,
price_type STRING,
record_source STRING,
platform_code STRING,
remote_selling_ind STRING,
fulfilled_from_location_type STRING,
delivery_method STRING,
delivery_method_subtype STRING,
loyalty_status STRING,
buyerflow_code STRING,
aare_acquired_ind STRING,
aare_activated_ind STRING,
aare_retained_ind STRING,
aare_engaged_ind STRING,
engagement_cohort STRING,
mrtk_chnl_type_code STRING,
mrtk_chnl_finance_rollup_code STRING,
mrtk_chnl_finance_detail_code STRING,
tran_tender_cash_flag STRING,
tran_tender_check_flag STRING,
tran_tender_nordstrom_card_flag STRING,
tran_tender_non_nordstrom_credit_flag STRING,
tran_tender_non_nordstrom_debit_flag STRING,
tran_tender_nordstrom_gift_card_flag STRING,
tran_tender_nordstrom_note_flag STRING,
tran_tender_paypal_flag STRING,
emp_disc_fulfilled_demand_usd_amt NUMERIC(20,2),
emp_disc_fulfilled_demand_units INTEGER,
emp_disc_op_gmv_usd_amt NUMERIC(20,2),
emp_disc_op_gmv_units INTEGER,
fulfilled_demand_usd_amt NUMERIC(20,2),
fulfilled_demand_units INTEGER,
post_fulfill_price_adj_usd_amt NUMERIC(20,2),
same_day_store_return_usd_amt NUMERIC(20,2),
same_day_store_return_units INTEGER,
last_chance_usd_amt NUMERIC(20,2),
last_chance_units INTEGER,
actual_product_returns_usd_amt NUMERIC(20,2),
actual_product_returns_units INTEGER,
op_gmv_usd_amt NUMERIC(20,2),
op_gmv_units INTEGER
);


INSERT INTO gmv
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
  CAST(emp_disc_fulfilled_demand_usd_amt AS NUMERIC) AS emp_disc_fulfilled_demand_usd_amt,
  emp_disc_fulfilled_demand_units,
  CAST(emp_disc_op_gmv_usd_amt AS NUMERIC) AS emp_disc_op_gmv_usd_amt,
  emp_disc_op_gmv_units,
  CAST(fulfilled_demand_usd_amt AS NUMERIC) AS fulfilled_demand_usd_amt,
  fulfilled_demand_units,
  CAST(post_fulfill_price_adj_usd_amt AS NUMERIC) AS post_fulfill_price_adj_usd_amt,
  CAST(same_day_store_return_usd_amt AS NUMERIC) AS same_day_store_return_usd_amt,
  same_day_store_return_units,
  CAST(last_chance_usd_amt AS NUMERIC) AS last_chance_usd_amt,
  last_chance_units,
  CAST(actual_product_returns_usd_amt AS NUMERIC) AS actual_product_returns_usd_amt,
  actual_product_returns_units,
  CAST(op_gmv_usd_amt AS NUMERIC) AS op_gmv_usd_amt,
  op_gmv_units
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
     SUM(CASE
       WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
       THEN employee_discount_usd_amt
       ELSE 0
       END) AS emp_disc_fulfilled_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y') AND LOWER(employee_discount_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS emp_disc_fulfilled_demand_units,
     SUM(CASE
       WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y')
       THEN employee_discount_usd_amt
       ELSE 0
       END) AS emp_disc_op_gmv_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y') AND LOWER(employee_discount_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS emp_disc_op_gmv_units,
     SUM(CASE
       WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
       THEN jwn_fulfilled_demand_usd_amt
       ELSE 0
       END) AS fulfilled_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS fulfilled_demand_units,
     SUM(CASE
       WHEN LOWER(price_adjustment_ind) = LOWER('Y') AND LOWER(product_return_ind) = LOWER('N')
       THEN operational_gmv_usd_amt - jwn_fulfilled_demand_usd_amt
       ELSE 0
       END) AS post_fulfill_price_adj_usd_amt,
     SUM(CASE
       WHEN LOWER(same_day_store_return_ind) = LOWER('Y') AND LOWER(service_type) <> LOWER('Last Chance')
       THEN operational_gmv_usd_amt
       ELSE 0
       END) AS same_day_store_return_usd_amt,
     SUM(CASE
       WHEN LOWER(same_day_store_return_ind) = LOWER('Y') AND LOWER(service_type) <> LOWER('Last Chance')
       THEN line_item_quantity
       ELSE 0
       END) AS same_day_store_return_units,
     SUM(CASE
       WHEN LOWER(service_type) = LOWER('Last Chance')
       THEN operational_gmv_usd_amt
       ELSE 0
       END) AS last_chance_usd_amt,
     SUM(CASE
       WHEN LOWER(service_type) = LOWER('Last Chance')
       THEN line_item_quantity
       ELSE 0
       END) AS last_chance_units,
     SUM(CASE
       WHEN LOWER(product_return_ind) = LOWER('Y') AND LOWER(service_type) <> LOWER('Last Chance')
       THEN operational_gmv_usd_amt
       ELSE 0
       END) AS actual_product_returns_usd_amt,
     SUM(CASE
       WHEN LOWER(product_return_ind) = LOWER('Y') AND LOWER(service_type) <> LOWER('Last Chance')
       THEN line_item_quantity
       ELSE 0
       END) AS actual_product_returns_units,
     SUM(CASE
       WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y')
       THEN operational_gmv_usd_amt
       ELSE 0
       END) AS op_gmv_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y')
       THEN operational_gmv_units
       ELSE 0
       END) AS op_gmv_units
    FROM gmv_aec AS jogmv
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
     tran_tender_paypal_flag
    UNION ALL
    SELECT business_day_date,
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
     loyalty_status,
     SUBSTR(NULL, 1, 27) AS buyerflow_code,
     'N' AS aare_acquired_ind,
     'N' AS aare_activated_ind,
     'N' AS aare_retained_ind,
     'N' AS aare_engaged_ind,
     SUBSTR(NULL, 1, 230) AS engagement_cohort,
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
     SUM(CASE
       WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
       THEN employee_discount_usd_amt
       ELSE 0
       END) AS emp_disc_fulfilled_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y') AND LOWER(employee_discount_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS emp_disc_fulfilled_demand_units,
     SUM(CASE
       WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y')
       THEN employee_discount_usd_amt
       ELSE 0
       END) AS emp_disc_op_gmv_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y') AND LOWER(employee_discount_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS emp_disc_op_gmv_units,
     SUM(CASE
       WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
       THEN jwn_fulfilled_demand_usd_amt
       ELSE 0
       END) AS fulfilled_demand_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_fulfilled_demand_ind) = LOWER('Y')
       THEN line_item_quantity
       ELSE 0
       END) AS fulfilled_demand_units,
     SUM(CASE
       WHEN LOWER(price_adjustment_ind) = LOWER('Y') AND LOWER(product_return_ind) = LOWER('N')
       THEN operational_gmv_usd_amt - jwn_fulfilled_demand_usd_amt
       ELSE 0
       END) AS post_fulfill_price_adj_usd_amt,
     SUM(CASE
       WHEN LOWER(same_day_store_return_ind) = LOWER('Y') AND LOWER(service_type) <> LOWER('Last Chance')
       THEN operational_gmv_usd_amt
       ELSE 0
       END) AS same_day_store_return_usd_amt,
     SUM(CASE
       WHEN LOWER(same_day_store_return_ind) = LOWER('Y') AND LOWER(service_type) <> LOWER('Last Chance')
       THEN line_item_quantity
       ELSE 0
       END) AS same_day_store_return_units,
     SUM(CASE
       WHEN LOWER(service_type) = LOWER('Last Chance')
       THEN operational_gmv_usd_amt
       ELSE 0
       END) AS last_chance_usd_amt,
     SUM(CASE
       WHEN LOWER(service_type) = LOWER('Last Chance')
       THEN line_item_quantity
       ELSE 0
       END) AS last_chance_units,
     SUM(CASE
       WHEN LOWER(product_return_ind) = LOWER('Y') AND LOWER(service_type) <> LOWER('Last Chance')
       THEN operational_gmv_usd_amt
       ELSE 0
       END) AS actual_product_returns_usd_amt,
     SUM(CASE
       WHEN LOWER(product_return_ind) = LOWER('Y') AND LOWER(service_type) <> LOWER('Last Chance')
       THEN line_item_quantity
       ELSE 0
       END) AS actual_product_returns_units,
     SUM(CASE
       WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y')
       THEN operational_gmv_usd_amt
       ELSE 0
       END) AS op_gmv_usd_amt,
     SUM(CASE
       WHEN LOWER(jwn_operational_gmv_ind) = LOWER('Y')
       THEN operational_gmv_units
       ELSE 0
       END) AS op_gmv_units
    FROM gmv_acp_null AS jogmv
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


CREATE TEMPORARY TABLE IF NOT EXISTS combo_sum AS WITH combo AS (SELECT demand_date AS tran_date,
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
    gross_demand_usd_amt,
    gross_demand_units,
    canceled_gross_demand_usd_amt,
    canceled_gross_demand_units,
    reported_demand_usd_amt,
    reported_demand_units,
    canceled_reported_demand_usd_amt,
    canceled_reported_demand_units,
    emp_disc_gross_demand_usd_amt,
    emp_disc_gross_demand_units,
    emp_disc_reported_demand_usd_amt,
    emp_disc_reported_demand_units,
    CAST(NULL AS NUMERIC) AS emp_disc_fulfilled_demand_usd_amt,
    CAST(NULL AS INTEGER) AS emp_disc_fulfilled_demand_units,
    CAST(NULL AS NUMERIC) AS emp_disc_op_gmv_usd_amt,
    CAST(NULL AS INTEGER) AS emp_disc_op_gmv_units,
    CAST(NULL AS NUMERIC) AS fulfilled_demand_usd_amt,
    CAST(NULL AS INTEGER) AS fulfilled_demand_units,
    CAST(NULL AS NUMERIC) AS post_fulfill_price_adj_usd_amt,
    CAST(NULL AS NUMERIC) AS same_day_store_return_usd_amt,
    CAST(NULL AS INTEGER) AS same_day_store_return_units,
    CAST(NULL AS NUMERIC) AS last_chance_usd_amt,
    CAST(NULL AS INTEGER) AS last_chance_units,
    CAST(NULL AS NUMERIC) AS actual_product_returns_usd_amt,
    CAST(NULL AS INTEGER) AS actual_product_returns_units,
    CAST(NULL AS NUMERIC) AS op_gmv_usd_amt,
    CAST(NULL AS INTEGER) AS op_gmv_units,
    CAST(NULL AS INTEGER) AS orders_count,
    CAST(NULL AS INTEGER) AS canceled_fraud_orders_count,
    CAST(NULL AS INTEGER) AS emp_disc_orders_count
   FROM demand
   UNION ALL
   SELECT business_day_date AS tran_date,
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
    CAST(NULL AS NUMERIC) AS gross_demand_usd_amt,
    CAST(NULL AS INTEGER) AS gross_demand_units,
    CAST(NULL AS NUMERIC) AS canceled_gross_demand_usd_amt,
    CAST(NULL AS INTEGER) AS canceled_gross_demand_units,
    CAST(NULL AS NUMERIC) AS reported_demand_usd_amt,
    CAST(NULL AS INTEGER) AS reported_demand_units,
    CAST(NULL AS NUMERIC) AS canceled_reported_demand_usd_amt,
    CAST(NULL AS INTEGER) AS canceled_reported_demand_units,
    CAST(NULL AS NUMERIC) AS emp_disc_gross_demand_usd_amt,
    CAST(NULL AS INTEGER) AS emp_disc_gross_demand_units,
    CAST(NULL AS NUMERIC) AS emp_disc_reported_demand_usd_amt,
    CAST(NULL AS INTEGER) AS emp_disc_reported_demand_units,
    emp_disc_fulfilled_demand_usd_amt,
    emp_disc_fulfilled_demand_units,
    emp_disc_op_gmv_usd_amt,
    emp_disc_op_gmv_units,
    fulfilled_demand_usd_amt,
    fulfilled_demand_units,
    post_fulfill_price_adj_usd_amt,
    same_day_store_return_usd_amt,
    same_day_store_return_units,
    last_chance_usd_amt,
    last_chance_units,
    actual_product_returns_usd_amt,
    actual_product_returns_units,
    op_gmv_usd_amt,
    op_gmv_units,
    CAST(NULL AS INTEGER) AS orders_count,
    CAST(NULL AS INTEGER) AS canceled_fraud_orders_count,
    CAST(NULL AS INTEGER) AS emp_disc_orders_count
   FROM gmv
   UNION ALL
   SELECT tran_date,
    business_unit_desc,
    store_num,
    bill_zip_code,
    line_item_currency_code,
    SUBSTR(NULL, 1, 4) AS inventory_business_model,
    SUBSTR(NULL, 1, 4) AS division_name,
    SUBSTR(NULL, 1, 4) AS subdivision_name,
    SUBSTR(NULL, 1, 4) AS price_type,
    'O' AS record_source,
    platform_code,
    SUBSTR(NULL, 1, 4) AS remote_selling_ind,
    SUBSTR(NULL, 1, 4) AS fulfilled_from_location_type,
    SUBSTR(NULL, 1, 4) AS delivery_method,
    SUBSTR(NULL, 1, 4) AS delivery_method_subtype,
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
    CAST(NULL AS NUMERIC) AS gross_demand_usd_amt,
    CAST(NULL AS INTEGER) AS gross_demand_units,
    CAST(NULL AS NUMERIC) AS canceled_gross_demand_usd_amt,
    CAST(NULL AS INTEGER) AS canceled_gross_demand_units,
    CAST(NULL AS NUMERIC) AS reported_demand_usd_amt,
    CAST(NULL AS INTEGER) AS reported_demand_units,
    CAST(NULL AS NUMERIC) AS canceled_reported_demand_usd_amt,
    CAST(NULL AS INTEGER) AS canceled_reported_demand_units,
    CAST(NULL AS NUMERIC) AS emp_disc_gross_demand_usd_amt,
    CAST(NULL AS INTEGER) AS emp_disc_gross_demand_units,
    CAST(NULL AS NUMERIC) AS emp_disc_reported_demand_usd_amt,
    CAST(NULL AS INTEGER) AS emp_disc_reported_demand_units,
    CAST(NULL AS NUMERIC) AS emp_disc_fulfilled_demand_usd_amt,
    CAST(NULL AS INTEGER) AS emp_disc_fulfilled_demand_units,
    CAST(NULL AS NUMERIC) AS emp_disc_op_gmv_usd_amt,
    CAST(NULL AS INTEGER) AS emp_disc_op_gmv_units,
    CAST(NULL AS NUMERIC) AS fulfilled_demand_usd_amt,
    CAST(NULL AS INTEGER) AS fulfilled_demand_units,
    CAST(NULL AS NUMERIC) AS post_fulfill_price_adj_usd_amt,
    CAST(NULL AS NUMERIC) AS same_day_store_return_usd_amt,
    CAST(NULL AS INTEGER) AS same_day_store_return_units,
    CAST(NULL AS NUMERIC) AS last_chance_usd_amt,
    CAST(NULL AS INTEGER) AS last_chance_units,
    CAST(NULL AS NUMERIC) AS actual_product_returns_usd_amt,
    CAST(NULL AS INTEGER) AS actual_product_returns_units,
    CAST(NULL AS NUMERIC) AS op_gmv_usd_amt,
    CAST(NULL AS INTEGER) AS op_gmv_units,
    orders_count,
    canceled_fraud_orders_count,
    emp_disc_orders_count
   FROM orders) (SELECT tran_date,
   business_unit_desc,
   store_num,
    CASE
    WHEN LOWER(bill_zip_code) = LOWER('UNKNOWN_VALUE')
    THEN 'other'
    WHEN LOWER(SUBSTR(bill_zip_code, 6, 1)) = LOWER('-')
    THEN SUBSTR(bill_zip_code, 0, 5)
    WHEN LOWER(bill_zip_code) LIKE LOWER('%-%')
    THEN 'other'
    WHEN IF(REGEXP_CONTAINS(bill_zip_code , r'^[0-9]+$'), 1, 0) = 1 AND CAST(SUBSTR(bill_zip_code, 0, 5) AS INTEGER)
      BETWEEN 501 AND 99950
    THEN SUBSTR(bill_zip_code, 0, 5)
    ELSE 'other'
    END AS bill_zip_code,
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
   SUM(gross_demand_usd_amt) AS gross_demand_usd_amt,
   SUM(gross_demand_units) AS gross_demand_units,
   SUM(canceled_gross_demand_usd_amt) AS canceled_gross_demand_usd_amt,
   SUM(canceled_gross_demand_units) AS canceled_gross_demand_units,
   SUM(reported_demand_usd_amt) AS reported_demand_usd_amt,
   SUM(reported_demand_units) AS reported_demand_units,
   SUM(canceled_reported_demand_usd_amt) AS canceled_reported_demand_usd_amt,
   SUM(canceled_reported_demand_units) AS canceled_reported_demand_units,
   SUM(emp_disc_gross_demand_usd_amt) AS emp_disc_gross_demand_usd_amt,
   SUM(emp_disc_gross_demand_units) AS emp_disc_gross_demand_units,
   SUM(emp_disc_reported_demand_usd_amt) AS emp_disc_reported_demand_usd_amt,
   SUM(emp_disc_reported_demand_units) AS emp_disc_reported_demand_units,
   SUM(emp_disc_fulfilled_demand_usd_amt) AS emp_disc_fulfilled_demand_usd_amt,
   SUM(emp_disc_fulfilled_demand_units) AS emp_disc_fulfilled_demand_units,
   SUM(emp_disc_op_gmv_usd_amt) AS emp_disc_op_gmv_usd_amt,
   SUM(emp_disc_op_gmv_units) AS emp_disc_op_gmv_units,
   SUM(fulfilled_demand_usd_amt) AS fulfilled_demand_usd_amt,
   SUM(fulfilled_demand_units) AS fulfilled_demand_units,
   SUM(post_fulfill_price_adj_usd_amt) AS post_fulfill_price_adj_usd_amt,
   SUM(same_day_store_return_usd_amt) AS same_day_store_return_usd_amt,
   SUM(same_day_store_return_units) AS same_day_store_return_units,
   SUM(last_chance_usd_amt) AS last_chance_usd_amt,
   SUM(last_chance_units) AS last_chance_units,
   SUM(actual_product_returns_usd_amt) AS actual_product_returns_usd_amt,
   SUM(actual_product_returns_units) AS actual_product_returns_units,
   SUM(op_gmv_usd_amt) AS op_gmv_usd_amt,
   SUM(op_gmv_units) AS op_gmv_units,
   SUM(orders_count) AS orders_count,
   SUM(canceled_fraud_orders_count) AS canceled_fraud_orders_count,
   SUM(emp_disc_orders_count) AS emp_disc_orders_count,
   CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
  FROM combo
  GROUP BY tran_date,
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
   tran_tender_paypal_flag);


DELETE FROM `{{params.gcp_project_id}}`.dev_nap_dsa_ai_fct.finance_sales_demand_fact
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}};


INSERT INTO `{{params.gcp_project_id}}`.dev_nap_dsa_ai_fct.finance_sales_demand_fact (row_id, tran_date, business_unit_desc, store_num,
 bill_zip_code, line_item_currency_code, inventory_business_model, division_name, subdivision_name, price_type,
 record_source, platform_code, remote_selling_ind, fulfilled_from_location_type, delivery_method,
 delivery_method_subtype, fulfillment_journey, loyalty_status, buyerflow_code, aare_acquired_ind, aare_activated_ind,
 aare_retained_ind, aare_engaged_ind, engagement_cohort, mrtk_chnl_type_code, mrtk_chnl_finance_rollup_code,
 mrtk_chnl_finance_detail_code, tran_tender_cash_flag, tran_tender_check_flag, tran_tender_nordstrom_card_flag,
 tran_tender_non_nordstrom_credit_flag, tran_tender_non_nordstrom_debit_flag, tran_tender_nordstrom_gift_card_flag,
 tran_tender_nordstrom_note_flag, tran_tender_paypal_flag, gross_demand_usd_amt, gross_demand_units,
 canceled_gross_demand_usd_amt, canceled_gross_demand_units, reported_demand_usd_amt, reported_demand_units,
 canceled_reported_demand_usd_amt, canceled_reported_demand_units, emp_disc_gross_demand_usd_amt,
 emp_disc_gross_demand_units, emp_disc_reported_demand_usd_amt, emp_disc_reported_demand_units,
 emp_disc_fulfilled_demand_usd_amt, emp_disc_fulfilled_demand_units, emp_disc_op_gmv_usd_amt, emp_disc_op_gmv_units,
 fulfilled_demand_usd_amt, fulfilled_demand_units, post_fulfill_price_adj_usd_amt, same_day_store_return_usd_amt,
 same_day_store_return_units, last_chance_usd_amt, last_chance_units, actual_product_returns_usd_amt,
 actual_product_returns_units, op_gmv_usd_amt, op_gmv_units, orders_count, canceled_fraud_orders_count,
 emp_disc_orders_count, dw_sys_load_tmstp)
(SELECT COALESCE((SELECT MAX(row_id)
     FROM `{{params.gcp_project_id}}`.{{params.clarity_schema}}.finance_sales_demand_fact), 0) + (ROW_NUMBER() OVER ()) AS row_id,
  tran_date,
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
  gross_demand_usd_amt,
  gross_demand_units,
  canceled_gross_demand_usd_amt,
  canceled_gross_demand_units,
  reported_demand_usd_amt,
  reported_demand_units,
  canceled_reported_demand_usd_amt,
  canceled_reported_demand_units,
  emp_disc_gross_demand_usd_amt,
  emp_disc_gross_demand_units,
  emp_disc_reported_demand_usd_amt,
  emp_disc_reported_demand_units,
  ROUND(CAST(emp_disc_fulfilled_demand_usd_amt AS NUMERIC), 2) AS emp_disc_fulfilled_demand_usd_amt,
  emp_disc_fulfilled_demand_units,
  ROUND(CAST(emp_disc_op_gmv_usd_amt AS NUMERIC), 2) AS emp_disc_op_gmv_usd_amt,
  emp_disc_op_gmv_units,
  ROUND(CAST(fulfilled_demand_usd_amt AS NUMERIC), 2) AS fulfilled_demand_usd_amt,
  fulfilled_demand_units,
  ROUND(CAST(post_fulfill_price_adj_usd_amt AS NUMERIC), 2) AS post_fulfill_price_adj_usd_amt,
  ROUND(CAST(same_day_store_return_usd_amt AS NUMERIC), 2) AS same_day_store_return_usd_amt,
  same_day_store_return_units,
  ROUND(CAST(last_chance_usd_amt AS NUMERIC), 2) AS last_chance_usd_amt,
  last_chance_units,
  ROUND(CAST(actual_product_returns_usd_amt AS NUMERIC), 2) AS actual_product_returns_usd_amt,
  actual_product_returns_units,
  ROUND(CAST(op_gmv_usd_amt AS NUMERIC), 2) AS op_gmv_usd_amt,
  op_gmv_units,
  orders_count,
  canceled_fraud_orders_count,
  emp_disc_orders_count,
  dw_sys_load_tmstp
 FROM combo_sum);