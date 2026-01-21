

/*
PRD_NAP_JWN_METRICS_USR_VWS.finance_sales_demand_margin_traffic_fact
Description - This ddl creates a 7 box (n.com, r.com, n.ca, FULL LINE, FULL LINE CANADA, RACK, RACK CANADA)
table of daily sales, demand, margin, and traffic split by store, platform (device), and customer categories
Full documenation: https://confluence.nordstrom.com/x/M4fITg
Contacts: Matthew Bond, Analytics

SLA:
I would like an SLA on this of 6am (9am EST)
but I don't know how to set that up since this job has no schedule since it's downstream of FSDF and that is dependent on raw clarity data
*/


CREATE TEMPORARY TABLE IF NOT EXISTS combo_sum AS (
WITH sales_demand AS ( --reassign bopus to store or digital
SELECT tran_date,
 business_unit_desc,
 store_num,
 bill_zip_code,
 line_item_currency_code,
 record_source,
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
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN gross_demand_usd_amt
   ELSE 0
   END) AS gross_demand_usd_amt_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN gross_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_store_gross_demand_usd_amt,
 0.00 AS bopus_attr_digital_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN gross_demand_units
   ELSE 0
   END) AS gross_demand_units_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN gross_demand_units
   ELSE 0
   END) AS bopus_attr_store_gross_demand_units,
 0 AS bopus_attr_digital_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_gross_demand_usd_amt
   ELSE 0
   END) AS canceled_gross_demand_usd_amt_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_gross_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_store_canceled_gross_demand_usd_amt,
 0.00 AS bopus_attr_digital_canceled_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_gross_demand_units
   ELSE 0
   END) AS canceled_gross_demand_units_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_gross_demand_units
   ELSE 0
   END) AS bopus_attr_store_canceled_gross_demand_units,
 0 AS bopus_attr_digital_canceled_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN reported_demand_usd_amt
   ELSE 0
   END) AS reported_demand_usd_amt_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN reported_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_store_reported_demand_usd_amt,
 0.00 AS bopus_attr_digital_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN reported_demand_units
   ELSE 0
   END) AS reported_demand_units_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN reported_demand_units
   ELSE 0
   END) AS bopus_attr_store_reported_demand_units,
 0 AS bopus_attr_digital_reported_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_reported_demand_usd_amt
   ELSE 0
   END) AS canceled_reported_demand_usd_amt_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_reported_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_store_canceled_reported_demand_usd_amt,
 0.00 AS bopus_attr_digital_canceled_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_reported_demand_units
   ELSE 0
   END) AS canceled_reported_demand_units_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_reported_demand_units
   ELSE 0
   END) AS bopus_attr_store_canceled_reported_demand_units,
 0 AS bopus_attr_digital_canceled_reported_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_gross_demand_usd_amt
   ELSE 0
   END) AS emp_disc_gross_demand_usd_amt_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_gross_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_store_emp_disc_gross_demand_usd_amt,
 0.00 AS bopus_attr_digital_emp_disc_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_gross_demand_units
   ELSE 0
   END) AS emp_disc_gross_demand_units_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_gross_demand_units
   ELSE 0
   END) AS bopus_attr_store_emp_disc_gross_demand_units,
 0 AS bopus_attr_digital_emp_disc_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_reported_demand_usd_amt
   ELSE 0
   END) AS emp_disc_reported_demand_usd_amt_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_reported_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_store_emp_disc_reported_demand_usd_amt,
 0.00 AS bopus_attr_digital_emp_disc_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_reported_demand_units
   ELSE 0
   END) AS emp_disc_reported_demand_units_excl_bopus,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_reported_demand_units
   ELSE 0
   END) AS bopus_attr_store_emp_disc_reported_demand_units,
 0 AS bopus_attr_digital_emp_disc_reported_demand_units,
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
 0 AS orders_count,
 0 AS canceled_fraud_orders_count,
 0 AS emp_disc_orders_count
FROM `{{params.gcp_project_id}}`.{{params.clarity_schema}}.finance_sales_demand_fact AS fsdf
WHERE LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('FULL LINE CANADA'), LOWER('RACK'), LOWER('RACK CANADA'),
   LOWER('MARKETPLACE'))
 AND tran_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tran_date,
 business_unit_desc,
 store_num,
 bill_zip_code,
 line_item_currency_code,
 record_source,
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
 tran_tender_paypal_flag
UNION ALL
SELECT tran_date,
  CASE
  WHEN LOWER(business_unit_desc) = LOWER('FULL LINE') AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code)
    NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
  THEN 'N.COM'
  WHEN LOWER(business_unit_desc) = LOWER('RACK') AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code) NOT IN (LOWER('Direct to Customer (DTC)'
      ), LOWER('Store POS'))
  THEN 'OFFPRICE ONLINE'
  ELSE business_unit_desc
  END AS business_unit_desc,
  CASE
  WHEN LOWER(business_unit_desc) = LOWER('FULL LINE') AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code)
    NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
  THEN 808
  WHEN LOWER(business_unit_desc) = LOWER('RACK') AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code) NOT IN (LOWER('Direct to Customer (DTC)'
      ), LOWER('Store POS'))
  THEN 828
  ELSE store_num
  END AS store_num,
 bill_zip_code,
 line_item_currency_code,
 record_source,
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
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN gross_demand_usd_amt
   ELSE 0
   END) AS gross_demand_usd_amt_excl_bopus,
 0.00 AS bopus_attr_store_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN gross_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_digital_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN gross_demand_units
   ELSE 0
   END) AS gross_demand_units_excl_bopus,
 0 AS bopus_attr_store_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN gross_demand_units
   ELSE 0
   END) AS bopus_attr_digital_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_gross_demand_usd_amt
   ELSE 0
   END) AS canceled_gross_demand_usd_amt_excl_bopus,
 0.00 AS bopus_attr_store_canceled_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_gross_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_digital_canceled_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_gross_demand_units
   ELSE 0
   END) AS canceled_gross_demand_units_excl_bopus,
 0 AS bopus_attr_store_canceled_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_gross_demand_units
   ELSE 0
   END) AS bopus_attr_digital_canceled_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN reported_demand_usd_amt
   ELSE 0
   END) AS reported_demand_usd_amt_excl_bopus,
 0.00 AS bopus_attr_store_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN reported_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_digital_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN reported_demand_units
   ELSE 0
   END) AS reported_demand_units_excl_bopus,
 0 AS bopus_attr_store_reported_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN reported_demand_units
   ELSE 0
   END) AS bopus_attr_digital_reported_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_reported_demand_usd_amt
   ELSE 0
   END) AS canceled_reported_demand_usd_amt_excl_bopus,
 0.00 AS bopus_attr_store_canceled_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_reported_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_digital_canceled_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_reported_demand_units
   ELSE 0
   END) AS canceled_reported_demand_units_excl_bopus,
 0 AS bopus_attr_store_canceled_reported_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN canceled_reported_demand_units
   ELSE 0
   END) AS bopus_attr_digital_canceled_reported_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_gross_demand_usd_amt
   ELSE 0
   END) AS emp_disc_gross_demand_usd_amt_excl_bopus,
 0.00 AS bopus_attr_store_emp_disc_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_gross_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_digital_emp_disc_gross_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_gross_demand_units
   ELSE 0
   END) AS emp_disc_gross_demand_units_excl_bopus,
 0 AS bopus_attr_store_emp_disc_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_gross_demand_units
   ELSE 0
   END) AS bopus_attr_digital_emp_disc_gross_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_reported_demand_usd_amt
   ELSE 0
   END) AS emp_disc_reported_demand_usd_amt_excl_bopus,
 0.00 AS bopus_attr_store_emp_disc_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_reported_demand_usd_amt
   ELSE 0
   END) AS bopus_attr_digital_emp_disc_reported_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) NOT IN (LOWER('FULL LINE'), LOWER('RACK')) OR LOWER(record_source) <> LOWER('O') OR
     LOWER(platform_code) IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_reported_demand_units
   ELSE 0
   END) AS emp_disc_reported_demand_units_excl_bopus,
 0 AS bopus_attr_store_emp_disc_reported_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code
      ) NOT IN (LOWER('Direct to Customer (DTC)'), LOWER('Store POS'))
   THEN emp_disc_reported_demand_units
   ELSE 0
   END) AS bopus_attr_digital_emp_disc_reported_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN emp_disc_fulfilled_demand_usd_amt
   ELSE 0
   END) AS emp_disc_fulfilled_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN emp_disc_fulfilled_demand_units
   ELSE 0
   END) AS emp_disc_fulfilled_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN emp_disc_op_gmv_usd_amt
   ELSE 0
   END) AS emp_disc_op_gmv_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN emp_disc_op_gmv_units
   ELSE 0
   END) AS emp_disc_op_gmv_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN fulfilled_demand_usd_amt
   ELSE 0
   END) AS fulfilled_demand_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN fulfilled_demand_units
   ELSE 0
   END) AS fulfilled_demand_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN post_fulfill_price_adj_usd_amt
   ELSE 0
   END) AS post_fulfill_price_adj_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN same_day_store_return_usd_amt
   ELSE 0
   END) AS same_day_store_return_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN same_day_store_return_units
   ELSE 0
   END) AS same_day_store_return_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN last_chance_usd_amt
   ELSE 0
   END) AS last_chance_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN last_chance_units
   ELSE 0
   END) AS last_chance_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN actual_product_returns_usd_amt
   ELSE 0
   END) AS actual_product_returns_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN actual_product_returns_units
   ELSE 0
   END) AS actual_product_returns_units,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN op_gmv_usd_amt
   ELSE 0
   END) AS op_gmv_usd_amt,
 SUM(CASE
   WHEN LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA'))
   THEN op_gmv_units
   ELSE 0
   END) AS op_gmv_units,
 SUM(orders_count) AS orders_count,
 SUM(canceled_fraud_orders_count) AS canceled_fraud_orders_count,
 SUM(emp_disc_orders_count) AS emp_disc_orders_count
FROM `{{params.gcp_project_id}}`.{{params.clarity_schema}}.finance_sales_demand_fact AS fsdf
WHERE (LOWER(business_unit_desc) IN (LOWER('OFFPRICE ONLINE'), LOWER('N.COM'), LOWER('N.CA')) OR LOWER(business_unit_desc
       ) IN (LOWER('FULL LINE'), LOWER('RACK')) AND LOWER(record_source) = LOWER('O') AND LOWER(platform_code) NOT IN (LOWER('Direct to Customer (DTC)'
       ), LOWER('Store POS')))
 AND tran_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY tran_date,
 business_unit_desc,
 store_num,
 bill_zip_code,
 line_item_currency_code,
 record_source,
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
 tran_tender_paypal_flag
)

, visitor_bots as
(
SELECT day_date,
  CASE
  WHEN LOWER(box) = LOWER('rcom')
  THEN 'OFFPRICE ONLINE'
  WHEN LOWER(box) = LOWER('ncom')
  THEN 'N.COM'
  ELSE NULL
  END AS business_unit,
 device_type,
 SUM(suspicious_visitors) AS suspicious_visitors,
 SUM(suspicious_viewing_visitors) AS suspicious_viewing_visitors,
 SUM(suspicious_adding_visitors) AS suspicious_adding_visitors
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.bot_traffic_fix
GROUP BY day_date,
 business_unit,
 device_type
)
, pre_store_traffic as
(
SELECT store_num,
 day_date,
 channel,
 SUM(purchase_trips) AS purchase_trips,
 SUM(CASE
   WHEN LOWER(traffic_source) IN (LOWER('retailnext_cam'))
   THEN traffic
   ELSE 0
   END) AS traffic_rn_cam,
 SUM(CASE
   WHEN LOWER(traffic_source) IN (LOWER('placer_channel_scaled_to_retailnext'), LOWER('placer_closed_store_scaled_to_retailnext'
      ), LOWER('placer_vertical_interference_scaled_to_retailnext'), LOWER('placer_vertical_interference_scaled_to_est_retailnext'
      ), LOWER('placer_store_scaled_to_retailnext'))
   THEN traffic
   ELSE 0
   END) AS traffic_scaled_placer
FROM `{{params.gcp_project_id}}`.{{params.fls_traffic_model_t2_schema}}.store_traffic_daily AS stdn
WHERE day_date BETWEEN DATE '2021-01-31' AND (DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 1 DAY))
 AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}}
GROUP BY store_num,
 day_date,
 channel
)
, combo AS ( --reassign bopus to store AND digital
SELECT
tran_date
,business_unit_desc
,store_num
,bill_zip_code
,line_item_currency_code
,record_source
,platform_code
,loyalty_status
,buyerflow_code
,aare_acquired_ind
,aare_activated_ind
,aare_retained_ind
,aare_engaged_ind
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,SUM(gross_demand_usd_amt_excl_bopus) AS gross_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_gross_demand_usd_amt) AS bopus_attr_store_gross_demand_usd_amt
,SUM(bopus_attr_digital_gross_demand_usd_amt) AS bopus_attr_digital_gross_demand_usd_amt
,SUM(gross_demand_units_excl_bopus) AS gross_demand_units_excl_bopus
,SUM(bopus_attr_store_gross_demand_units) AS bopus_attr_store_gross_demand_units
,SUM(bopus_attr_digital_gross_demand_units) AS bopus_attr_digital_gross_demand_units
,SUM(canceled_gross_demand_usd_amt_excl_bopus) AS canceled_gross_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_canceled_gross_demand_usd_amt) AS bopus_attr_store_canceled_gross_demand_usd_amt
,SUM(bopus_attr_digital_canceled_gross_demand_usd_amt) AS bopus_attr_digital_canceled_gross_demand_usd_amt
,SUM(canceled_gross_demand_units_excl_bopus) AS canceled_gross_demand_units_excl_bopus
,SUM(bopus_attr_store_canceled_gross_demand_units) AS bopus_attr_store_canceled_gross_demand_units
,SUM(bopus_attr_digital_canceled_gross_demand_units) AS bopus_attr_digital_canceled_gross_demand_units
,SUM(reported_demand_usd_amt_excl_bopus) AS reported_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_reported_demand_usd_amt) AS bopus_attr_store_reported_demand_usd_amt
,SUM(bopus_attr_digital_reported_demand_usd_amt) AS bopus_attr_digital_reported_demand_usd_amt
,SUM(reported_demand_units_excl_bopus) AS reported_demand_units_excl_bopus
,SUM(bopus_attr_store_reported_demand_units) AS bopus_attr_store_reported_demand_units
,SUM(bopus_attr_digital_reported_demand_units) AS bopus_attr_digital_reported_demand_units
,SUM(canceled_reported_demand_usd_amt_excl_bopus) AS canceled_reported_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_canceled_reported_demand_usd_amt) AS bopus_attr_store_canceled_reported_demand_usd_amt
,SUM(bopus_attr_digital_canceled_reported_demand_usd_amt) AS bopus_attr_digital_canceled_reported_demand_usd_amt
,SUM(canceled_reported_demand_units_excl_bopus) AS canceled_reported_demand_units_excl_bopus
,SUM(bopus_attr_store_canceled_reported_demand_units) AS bopus_attr_store_canceled_reported_demand_units
,SUM(bopus_attr_digital_canceled_reported_demand_units) AS bopus_attr_digital_canceled_reported_demand_units
,SUM(emp_disc_gross_demand_usd_amt_excl_bopus) AS emp_disc_gross_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_emp_disc_gross_demand_usd_amt) AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,SUM(bopus_attr_digital_emp_disc_gross_demand_usd_amt) AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,SUM(emp_disc_gross_demand_units_excl_bopus) AS emp_disc_gross_demand_units_excl_bopus
,SUM(bopus_attr_store_emp_disc_gross_demand_units) AS bopus_attr_store_emp_disc_gross_demand_units
,SUM(bopus_attr_digital_emp_disc_gross_demand_units) AS bopus_attr_digital_emp_disc_gross_demand_units
,SUM(emp_disc_reported_demand_usd_amt_excl_bopus) AS emp_disc_reported_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_emp_disc_reported_demand_usd_amt) AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,SUM(bopus_attr_digital_emp_disc_reported_demand_usd_amt) AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,SUM(emp_disc_reported_demand_units_excl_bopus) AS emp_disc_reported_demand_units_excl_bopus
,SUM(bopus_attr_store_emp_disc_reported_demand_units) AS bopus_attr_store_emp_disc_reported_demand_units
,SUM(bopus_attr_digital_emp_disc_reported_demand_units) AS bopus_attr_digital_emp_disc_reported_demand_units
,SUM(emp_disc_fulfilled_demand_usd_amt) AS emp_disc_fulfilled_demand_usd_amt
,SUM(emp_disc_fulfilled_demand_units) AS emp_disc_fulfilled_demand_units
,SUM(emp_disc_op_gmv_usd_amt) AS emp_disc_op_gmv_usd_amt
,SUM(emp_disc_op_gmv_units) AS emp_disc_op_gmv_units
,SUM(fulfilled_demand_usd_amt) AS fulfilled_demand_usd_amt
,SUM(fulfilled_demand_units) AS fulfilled_demand_units
,SUM(post_fulfill_price_adj_usd_amt) AS post_fulfill_price_adj_usd_amt
,SUM(same_day_store_return_usd_amt) AS same_day_store_return_usd_amt
,SUM(same_day_store_return_units) AS same_day_store_return_units
,SUM(last_chance_usd_amt) AS last_chance_usd_amt
,SUM(last_chance_units) AS last_chance_units
,SUM(actual_product_returns_usd_amt) AS actual_product_returns_usd_amt
,SUM(actual_product_returns_units) AS actual_product_returns_units
,SUM(op_gmv_usd_amt) AS op_gmv_usd_amt
,SUM(op_gmv_units) AS op_gmv_units
,CAST(NULL AS NUMERIC) AS reported_gmv_usd_amt
,NULL AS reported_gmv_units
,CAST(NULL AS NUMERIC) AS merch_net_sales_usd_amt
,NULL AS merch_net_sales_units
,CAST(NULL AS NUMERIC) AS reported_net_sales_usd_amt
,NULL AS reported_net_sales_units
,CAST(NULL AS NUMERIC) AS variable_cost_usd_amt
,CAST(NULL AS NUMERIC) AS gross_contribution_margin_usd_amt
,CAST(NULL AS NUMERIC) AS net_contribution_margin_usd_amt
,SUM(orders_count) AS orders_count
,SUM(canceled_fraud_orders_count) AS canceled_fraud_orders_count
,SUM(emp_disc_orders_count) AS emp_disc_orders_count
,NULL AS visitors
,NULL AS viewing_visitors
,NULL AS adding_visitors
,NULL AS ordering_visitors
,NULL AS sessions
,NULL AS viewing_sessions
,NULL AS adding_sessions
,NULL AS checkout_sessions
,NULL AS ordering_sessions
,NULL AS acp_count
,NULL AS unique_cust_count
,NULL AS total_pages_visited
,NULL AS total_unique_pages_visited
,NULL AS bounced_sessions
,NULL AS browse_sessions
,NULL AS search_sessions
,NULL AS wishlist_sessions
,NULL AS store_purchase_trips
,NULL AS store_traffic
FROM sales_demand
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag,tran_tender_paypal_flag
UNION ALL

SELECT
activity_date as tran_date
,CASE WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.COM'
    WHEN LOWER(channel_country) = LOWER('CA') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.CA'
    WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('RACK') THEN 'OFFPRICE ONLINE'
    ELSE NULL END AS business_unit_desc
,CASE WHEN LOWER(CASE WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.COM'
    WHEN LOWER(channel_country) = LOWER('CA') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.CA'
    WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('RACK') THEN 'OFFPRICE ONLINE'
    ELSE NULL END) = LOWER('N.COM') THEN 808
	WHEN LOWER(CASE WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.COM'
    WHEN LOWER(channel_country) = LOWER('CA') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.CA'
    WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('RACK') THEN 'OFFPRICE ONLINE'
    ELSE NULL END) = LOWER('N.CA') THEN 867
	WHEN LOWER(CASE WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.COM'
    WHEN LOWER(channel_country) = LOWER('CA') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.CA'
    WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('RACK') THEN 'OFFPRICE ONLINE'
    ELSE NULL END) = LOWER('OFFPRICE ONLINE') THEN 828
	ELSE NULL END AS store_num
,'other' AS bill_zip_code
,CASE WHEN LOWER(channel_country) = LOWER('US') THEN 'USD'
    WHEN LOWER(channel_country) = LOWER('CA') THEN 'CAD'
    ELSE NULL END AS line_item_currency_code
,'Z' AS record_source
,CASE WHEN LOWER(platform) = LOWER('CSR_PHONE') THEN 'Other'
	WHEN LOWER(platform) = LOWER('ANDROID') THEN 'Android'
	WHEN LOWER(platform) = LOWER('MOW') THEN 'Mobile Web'
	WHEN LOWER(platform) = LOWER('WEB') THEN 'Desktop/Tablet'
	WHEN LOWER(platform) = LOWER('POS') THEN 'Store POS'
	WHEN LOWER(platform) = LOWER('IOS') THEN 'IOS'
	WHEN LOWER(platform) = LOWER('THIRD_PARTY_VENDOR') THEN 'Other'
	ELSE platform END AS platform_code
,'total_digital' AS loyalty_status
,'total_digital' AS buyerflow_code
,'N' AS aare_acquired_ind
,'N' AS aare_activated_ind
,'N' AS aare_retained_ind
,'N' AS aare_engaged_ind
,'total_digital' AS mrtk_chnl_type_code
,'total_digital' AS mrtk_chnl_finance_rollup_code
,'total_digital' AS mrtk_chnl_finance_detail_code
,'total_digital' AS engagement_cohort
,'N' AS tran_tender_cash_flag
,'N' AS tran_tender_check_flag
,'N' AS tran_tender_nordstrom_card_flag
,'N' AS tran_tender_non_nordstrom_credit_flag
,'N' AS tran_tender_non_nordstrom_debit_flag
,'N' AS tran_tender_nordstrom_gift_card_flag
,'N' AS tran_tender_nordstrom_note_flag
,'N' AS tran_tender_paypal_flag
,NULL AS gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_gross_demand_usd_amt
,NULL AS bopus_attr_digital_gross_demand_usd_amt
,NULL AS gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_gross_demand_units
,NULL AS bopus_attr_digital_gross_demand_units
,NULL AS canceled_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_gross_demand_usd_amt
,NULL AS canceled_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_units
,NULL AS bopus_attr_digital_canceled_gross_demand_units
,NULL AS reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_reported_demand_usd_amt
,NULL AS bopus_attr_digital_reported_demand_usd_amt
,NULL AS reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_reported_demand_units
,NULL AS bopus_attr_digital_reported_demand_units
,NULL AS canceled_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_reported_demand_usd_amt
,NULL AS canceled_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_units
,NULL AS bopus_attr_digital_canceled_reported_demand_units
,NULL AS emp_disc_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,NULL AS emp_disc_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_units
,NULL AS bopus_attr_digital_emp_disc_gross_demand_units
,NULL AS emp_disc_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,NULL AS emp_disc_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_units
,NULL AS bopus_attr_digital_emp_disc_reported_demand_units
,NULL AS emp_disc_fulfilled_demand_usd_amt
,NULL AS emp_disc_fulfilled_demand_units
,NULL AS emp_disc_op_gmv_usd_amt
,NULL AS emp_disc_op_gmv_units
,NULL AS fulfilled_demand_usd_amt
,NULL AS fulfilled_demand_units
,NULL AS post_fulfill_price_adj_usd_amt
,NULL AS same_day_store_return_usd_amt
,NULL AS same_day_store_return_units
,NULL AS last_chance_usd_amt
,NULL AS last_chance_units
,NULL AS actual_product_returns_usd_amt
,NULL AS actual_product_returns_units
,NULL AS op_gmv_usd_amt
,NULL AS op_gmv_units
,NULL AS reported_gmv_usd_amt
,NULL AS reported_gmv_units
,NULL AS merch_net_sales_usd_amt
,NULL AS merch_net_sales_units
,NULL AS reported_net_sales_usd_amt
,NULL AS reported_net_sales_units
,NULL AS variable_cost_usd_amt
,NULL AS gross_contribution_margin_usd_amt
,NULL AS net_contribution_margin_usd_amt
,NULL AS orders_count
,NULL AS canceled_fraud_orders_count
,NULL AS emp_disc_orders_count
,SUM(visitor_count)-MAX(COALESCE(b.suspicious_visitors,0)) AS visitors
,SUM(product_view_visitors)-MAX(COALESCE(b.suspicious_viewing_visitors,0)) AS viewing_visitors
,SUM(cart_add_visitors)-MAX(COALESCE(b.suspicious_adding_visitors,0)) AS adding_visitors
,SUM(ordering_visitors) AS ordering_visitors
,NULL AS sessions
,NULL AS viewing_sessions
,NULL AS adding_sessions
,NULL AS checkout_sessions
,NULL AS ordering_sessions
,NULL AS acp_count
,NULL AS unique_cust_count
,NULL AS total_pages_visited
,NULL AS total_unique_pages_visited
,NULL AS bounced_sessions
,NULL AS browse_sessions
,NULL AS search_sessions
,NULL AS wishlist_sessions
,NULL AS store_purchase_trips
,NULL AS store_traffic
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.visitor_funnel_fact AS vff
LEFT JOIN visitor_bots b ON vff.activity_date = b.day_date AND LOWER(CASE WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.COM'
    WHEN LOWER(channel_country) = LOWER('CA') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.CA'
    WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('RACK') THEN 'OFFPRICE ONLINE'
    ELSE NULL END) = LOWER(b.business_unit) AND LOWER(vff.platform) = LOWER(b.device_type)
WHERE activity_date BETWEEN '2021-01-31' AND DATE_SUB(CURRENT_DATE('PST8PDT'),INTERVAL 1 DAY)
AND activity_date BETWEEN {{params.start_date}} AND {{params.end_date}}
AND ((LOWER(CASE WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.COM'
    WHEN LOWER(channel_country) = LOWER('CA') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.CA'
    WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('RACK') THEN 'OFFPRICE ONLINE'
    ELSE NULL END) IN (LOWER('N.COM'),LOWER('N.CA'))) OR (LOWER(CASE WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.COM'
    WHEN LOWER(channel_country) = LOWER('CA') AND LOWER(purchase_channel) = LOWER('FULL_LINE') THEN 'N.CA'
    WHEN LOWER(channel_country) = LOWER('US') AND LOWER(purchase_channel) = LOWER('RACK') THEN 'OFFPRICE ONLINE'
    ELSE NULL END) = LOWER('OFFPRICE ONLINE') AND activity_date >= '2021-06-13')) --r.com traffic before END of project rocket isn't valid
AND LOWER(bot_traffic_ind) <> LOWER('Y')
AND LOWER(platform) <> LOWER('POS') --remove DTC, it should be accounted for in store traffic
--ideally move bopus too, but it's not able to be split by orders either- at least that's consistent by conversion rate (orders/visitors)
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag,tran_tender_paypal_flag

UNION ALL
SELECT  --add sessions
activity_date_pacific AS tran_date
,CASE WHEN LOWER(channel) = LOWER('NORDSTROM') THEN 'N.COM'
    WHEN LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK')) THEN 'OFFPRICE ONLINE'
    ELSE NULL END AS business_unit_desc
,CASE WHEN LOWER(CASE WHEN LOWER(channel) = LOWER('NORDSTROM') THEN 'N.COM'
    WHEN LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK')) THEN 'OFFPRICE ONLINE'
    ELSE NULL END ) = LOWER('N.COM') THEN 808
	WHEN LOWER(CASE WHEN LOWER(channel) = LOWER('NORDSTROM') THEN 'N.COM'
    WHEN LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK')) THEN 'OFFPRICE ONLINE'
    ELSE NULL END ) = LOWER('OFFPRICE ONLINE') THEN 828
	ELSE NULL END AS store_num
,'other' AS bill_zip_code
,'USD' AS line_item_currency_code
,'Z' AS record_source
,CASE WHEN LOWER(experience) = LOWER('CARE_PHONE') THEN 'Other'
	WHEN LOWER(experience) = LOWER('ANDROID_APP') THEN 'Android'
	WHEN LOWER(experience) = LOWER('MOBILE_WEB') THEN 'Mobile Web'
	WHEN LOWER(experience) = LOWER('DESKTOP_WEB') THEN 'Desktop/Tablet'
	WHEN LOWER(experience) = LOWER('POINT_OF_SALE') THEN 'Store POS'
	WHEN LOWER(experience) = LOWER('IOS_APP') THEN 'IOS'
	WHEN LOWER(experience) = LOWER('VENDOR') THEN 'Other'
	ELSE experience END AS platform_code
,'total_digital' AS loyalty_status
,'total_digital' AS buyerflow_code
,'N' AS aare_acquired_ind
,'N' AS aare_activated_ind
,'N' AS aare_retained_ind
,'N' AS aare_engaged_ind
,mrkt_type AS mrtk_chnl_type_code
,finance_rollup AS mrtk_chnl_finance_rollup_code
,finance_detail AS mrtk_chnl_finance_detail_code
,'total_digital' AS engagement_cohort
,'N' AS tran_tender_cash_flag
,'N' AS tran_tender_check_flag
,'N' AS tran_tender_nordstrom_card_flag
,'N' AS tran_tender_non_nordstrom_credit_flag
,'N' AS tran_tender_non_nordstrom_debit_flag
,'N' AS tran_tender_nordstrom_gift_card_flag
,'N' AS tran_tender_nordstrom_note_flag
,'N' AS tran_tender_paypal_flag
,NULL AS gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_gross_demand_usd_amt
,NULL AS bopus_attr_digital_gross_demand_usd_amt
,NULL AS gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_gross_demand_units
,NULL AS bopus_attr_digital_gross_demand_units
,NULL AS canceled_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_gross_demand_usd_amt
,NULL AS canceled_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_units
,NULL AS bopus_attr_digital_canceled_gross_demand_units
,NULL AS reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_reported_demand_usd_amt
,NULL AS bopus_attr_digital_reported_demand_usd_amt
,NULL AS reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_reported_demand_units
,NULL AS bopus_attr_digital_reported_demand_units
,NULL AS canceled_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_reported_demand_usd_amt
,NULL AS canceled_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_units
,NULL AS bopus_attr_digital_canceled_reported_demand_units
,NULL AS emp_disc_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,NULL AS emp_disc_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_units
,NULL AS bopus_attr_digital_emp_disc_gross_demand_units
,NULL AS emp_disc_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,NULL AS emp_disc_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_units
,NULL AS bopus_attr_digital_emp_disc_reported_demand_units
,NULL AS emp_disc_fulfilled_demand_usd_amt
,NULL AS emp_disc_fulfilled_demand_units
,NULL AS emp_disc_op_gmv_usd_amt
,NULL AS emp_disc_op_gmv_units
,NULL AS fulfilled_demand_usd_amt
,NULL AS fulfilled_demand_units
,NULL AS post_fulfill_price_adj_usd_amt
,NULL AS same_day_store_return_usd_amt
,NULL AS same_day_store_return_units
,NULL AS last_chance_usd_amt
,NULL AS last_chance_units
,NULL AS actual_product_returns_usd_amt
,NULL AS actual_product_returns_units
,NULL AS op_gmv_usd_amt
,NULL AS op_gmv_units
,NULL AS reported_gmv_usd_amt
,NULL AS reported_gmv_units
,NULL AS merch_net_sales_usd_amt
,NULL AS merch_net_sales_units
,NULL AS reported_net_sales_usd_amt
,NULL AS reported_net_sales_units
,NULL AS variable_cost_usd_amt
,NULL AS gross_contribution_margin_usd_amt
,NULL AS net_contribution_margin_usd_amt
,NULL AS orders_count
,NULL AS canceled_fraud_orders_count
,NULL AS emp_disc_orders_count
,NULL AS visitors
,NULL AS viewing_visitors
,NULL AS adding_visitors
,NULL AS ordering_visitors
,COUNT(DISTINCT (session_id)) AS sessions
,COUNT(DISTINCT CASE WHEN product_views >= 1 THEN session_id ELSE NULL END) AS viewing_sessions
,COUNT(DISTINCT CASE WHEN cart_adds >= 1 THEN session_id ELSE NULL END) AS adding_sessions
,NULL AS checkout_sessions
,COUNT(DISTINCT CASE WHEN web_orders >= 1 THEN session_id ELSE NULL END) AS ordering_sessions
,NULL AS acp_count
,NULL AS unique_cust_count
,SUM(product_views) AS total_pages_visited
,NULL AS total_unique_pages_visited
,COUNT(DISTINCT CASE WHEN bounce_flag = 1 THEN session_id ELSE NULL END) AS bounced_sessions
,NULL AS browse_sessions
,NULL AS search_sessions
,NULL AS wishlist_sessions
,NULL AS store_purchase_trips
,NULL AS store_traffic
FROM `{{params.gcp_project_id}}`.t2dl_das_sessions.dior_session_fact
WHERE activity_date_pacific BETWEEN '2022-01-30' AND DATE_SUB(CURRENT_DATE('PST8PDT'),INTERVAL 1 DAY)
--'2022-01-31' is first date of complete sessions data. Adding sessions is empty until 2022-02-09
AND activity_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
AND LOWER(CASE WHEN LOWER(channel) = LOWER('NORDSTROM') THEN 'N.COM'
    WHEN LOWER(channel) IN (LOWER('NORDSTROM_RACK'), LOWER('HAUTELOOK')) THEN 'OFFPRICE ONLINE'
    ELSE NULL END) NOT IN (LOWER('TRUNK_CLUB'))
AND ((COALESCE(active_session_flag,0) = 1 or web_orders >= 1) or --remove inactive sessions (mostly bots) but add back a few inactive sessions that do place orders (maybe using an adblocker)
    ((web_orders >= 1 OR cart_adds >= 1 OR product_views >= 1) AND activity_date_pacific BETWEEN '2022-01-30' AND '2022-03-22') OR
    ((web_orders >= 1 OR cart_adds >= 1 OR product_views >= 1) AND activity_date_pacific BETWEEN '2022-05-15' AND '2022-05-17') OR
    ((web_orders >= 1 OR cart_adds >= 1 OR product_views >= 1) AND activity_date_pacific BETWEEN '2022-08-28' AND '2022-09-08') OR
    ((web_orders >= 1 OR cart_adds >= 1 OR product_views >= 1) AND activity_date_pacific BETWEEN '2022-11-06' AND '2022-11-07'))
--for fy 2022 dates with bad data (Sessionizer broke or p0) estimate sessions using view/add/order sessions- backfill may come eventually from session team
AND LOWER(CASE WHEN LOWER(experience) = LOWER('CARE_PHONE') THEN 'Other'
	WHEN LOWER(experience) = LOWER('ANDROID_APP') THEN 'Android'
	WHEN LOWER(experience) = LOWER('MOBILE_WEB') THEN 'Mobile Web'
	WHEN LOWER(experience) = LOWER('DESKTOP_WEB') THEN 'Desktop/Tablet'
	WHEN LOWER(experience) = LOWER('POINT_OF_SALE') THEN 'Store POS'
	WHEN LOWER(experience) = LOWER('IOS_APP') THEN 'IOS'
	WHEN LOWER(experience) = LOWER('VENDOR') THEN 'Other'
	ELSE experience END) <> LOWER('Store POS')
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag,tran_tender_paypal_flag
    UNION ALL
SELECT   --add store traffic
day_date AS tran_date
,channel AS business_unit_desc
,store_num
,'other' AS bill_zip_code
,CASE WHEN LOWER(channel) LIKE LOWER('%Canada') THEN 'CAD' ELSE 'USD' END AS line_item_currency_code
,'Z' AS record_source
,'total_store' AS platform_code
,'total_store' AS loyalty_status
,'total_store' AS buyerflow_code
,'N' AS aare_acquired_ind
,'N' AS aare_activated_ind
,'N' AS aare_retained_ind
,'N' AS aare_engaged_ind
,'total_store' AS mrtk_chnl_type_code
,'total_store' AS mrtk_chnl_finance_rollup_code
,'total_store' AS mrtk_chnl_finance_detail_code
,'total_store' AS engagement_cohort
,'N' AS tran_tender_cash_flag
,'N' AS tran_tender_check_flag
,'N' AS tran_tender_nordstrom_card_flag
,'N' AS tran_tender_non_nordstrom_credit_flag
,'N' AS tran_tender_non_nordstrom_debit_flag
,'N' AS tran_tender_nordstrom_gift_card_flag
,'N' AS tran_tender_nordstrom_note_flag
,'N' AS tran_tender_paypal_flag
,NULL AS gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_gross_demand_usd_amt
,NULL AS bopus_attr_digital_gross_demand_usd_amt
,NULL AS gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_gross_demand_units
,NULL AS bopus_attr_digital_gross_demand_units
,NULL AS canceled_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_gross_demand_usd_amt
,NULL AS canceled_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_units
,NULL AS bopus_attr_digital_canceled_gross_demand_units
,NULL AS reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_reported_demand_usd_amt
,NULL AS bopus_attr_digital_reported_demand_usd_amt
,NULL AS reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_reported_demand_units
,NULL AS bopus_attr_digital_reported_demand_units
,NULL AS canceled_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_reported_demand_usd_amt
,NULL AS canceled_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_units
,NULL AS bopus_attr_digital_canceled_reported_demand_units
,NULL AS emp_disc_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,NULL AS emp_disc_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_units
,NULL AS bopus_attr_digital_emp_disc_gross_demand_units
,NULL AS emp_disc_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,NULL AS emp_disc_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_units
,NULL AS bopus_attr_digital_emp_disc_reported_demand_units
,NULL AS emp_disc_fulfilled_demand_usd_amt
,NULL AS emp_disc_fulfilled_demand_units
,NULL AS emp_disc_op_gmv_usd_amt
,NULL AS emp_disc_op_gmv_units
,NULL AS fulfilled_demand_usd_amt
,NULL AS fulfilled_demand_units
,NULL AS post_fulfill_price_adj_usd_amt
,NULL AS same_day_store_return_usd_amt
,NULL AS same_day_store_return_units
,NULL AS last_chance_usd_amt
,NULL AS last_chance_units
,NULL AS actual_product_returns_usd_amt
,NULL AS actual_product_returns_units
,NULL AS op_gmv_usd_amt
,NULL AS op_gmv_units
,NULL AS reported_gmv_usd_amt
,NULL AS reported_gmv_units
,NULL AS merch_net_sales_usd_amt
,NULL AS merch_net_sales_units
,NULL AS reported_net_sales_usd_amt
,NULL AS reported_net_sales_units
,NULL AS variable_cost_usd_amt
,NULL AS gross_contribution_margin_usd_amt
,NULL AS net_contribution_margin_usd_amt
,NULL AS orders_count
,NULL AS canceled_fraud_orders_count
,NULL AS emp_disc_orders_count
,NULL AS visitors
,NULL AS viewing_visitors
,NULL AS adding_visitors
,NULL AS ordering_visitors
,NULL AS sessions
,NULL AS viewing_sessions
,NULL AS adding_sessions
,NULL AS checkout_sessions
,NULL AS ordering_sessions
,NULL AS acp_count
,NULL AS unique_cust_count
,NULL AS total_pages_visited
,NULL AS total_unique_pages_visited
,NULL AS bounced_sessions
,NULL AS browse_sessions
,NULL AS search_sessions
,NULL AS wishlist_sessions
,SUM(purchase_trips) AS store_purchase_trips
,SUM(CASE WHEN traffic_rn_cam > 0 AND store_num NOT IN (4,5,358) THEN traffic_rn_cam ELSE traffic_scaled_placer END) AS store_traffic
FROM pre_store_traffic AS st
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag,tran_tender_paypal_flag
	UNION ALL
--r.com hard coded historical funnel
SELECT
day_date AS tran_date
,'OFFPRICE ONLINE' AS business_unit_desc
,828 AS store_num
,'other' AS bill_zip_code
,'USD' AS line_item_currency_code
,'Z' AS record_source
,'Android' AS platform_code
,'total_digital' AS loyalty_status
,'total_digital' AS buyerflow_code
,'N' AS aare_acquired_ind
,'N' AS aare_activated_ind
,'N' AS aare_retained_ind
,'N' AS aare_engaged_ind
,'total_digital' AS mrtk_chnl_type_code
,'total_digital' AS mrtk_chnl_finance_rollup_code
,'total_digital' AS mrtk_chnl_finance_detail_code
,'total_digital' AS engagement_cohort
,'N' AS tran_tender_cash_flag
,'N' AS tran_tender_check_flag
,'N' AS tran_tender_nordstrom_card_flag
,'N' AS tran_tender_non_nordstrom_credit_flag
,'N' AS tran_tender_non_nordstrom_debit_flag
,'N' AS tran_tender_nordstrom_gift_card_flag
,'N' AS tran_tender_nordstrom_note_flag
,'N' AS tran_tender_paypal_flag
,NULL AS gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_gross_demand_usd_amt
,NULL AS bopus_attr_digital_gross_demand_usd_amt
,NULL AS gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_gross_demand_units
,NULL AS bopus_attr_digital_gross_demand_units
,NULL AS canceled_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_gross_demand_usd_amt
,NULL AS canceled_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_units
,NULL AS bopus_attr_digital_canceled_gross_demand_units
,NULL AS reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_reported_demand_usd_amt
,NULL AS bopus_attr_digital_reported_demand_usd_amt
,NULL AS reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_reported_demand_units
,NULL AS bopus_attr_digital_reported_demand_units
,NULL AS canceled_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_reported_demand_usd_amt
,NULL AS canceled_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_units
,NULL AS bopus_attr_digital_canceled_reported_demand_units
,NULL AS emp_disc_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,NULL AS emp_disc_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_units
,NULL AS bopus_attr_digital_emp_disc_gross_demand_units
,NULL AS emp_disc_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,NULL AS emp_disc_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_units
,NULL AS bopus_attr_digital_emp_disc_reported_demand_units
,NULL AS emp_disc_fulfilled_demand_usd_amt
,NULL AS emp_disc_fulfilled_demand_units
,NULL AS emp_disc_op_gmv_usd_amt
,NULL AS emp_disc_op_gmv_units
,NULL AS fulfilled_demand_usd_amt
,NULL AS fulfilled_demand_units
,NULL AS post_fulfill_price_adj_usd_amt
,NULL AS same_day_store_return_usd_amt
,NULL AS same_day_store_return_units
,NULL AS last_chance_usd_amt
,NULL AS last_chance_units
,NULL AS actual_product_returns_usd_amt
,NULL AS actual_product_returns_units
,NULL AS op_gmv_usd_amt
,NULL AS op_gmv_units
,NULL AS reported_gmv_usd_amt
,NULL AS reported_gmv_units
,NULL AS merch_net_sales_usd_amt
,NULL AS merch_net_sales_units
,NULL AS reported_net_sales_usd_amt
,NULL AS reported_net_sales_units
,NULL AS variable_cost_usd_amt
,NULL AS gross_contribution_margin_usd_amt
,NULL AS net_contribution_margin_usd_amt
,NULL AS orders_count
,NULL AS canceled_fraud_orders_count
,NULL AS emp_disc_orders_count
,SUM(visitors_android) AS visitors
,SUM(viewing_visitors_android) AS viewing_visitors
,SUM(adding_visitors_android) AS adding_visitors
,SUM(ordering_visitors_android) AS ordering_visitors
,NULL AS sessions
,NULL AS viewing_sessions
,NULL AS adding_sessions
,NULL AS checkout_sessions
,NULL AS ordering_sessions
,NULL AS acp_count
,NULL AS unique_cust_count
,NULL AS total_pages_visited
,NULL AS total_unique_pages_visited
,NULL AS bounced_sessions
,NULL AS browse_sessions
,NULL AS search_sessions
,NULL AS wishlist_sessions
,NULL AS store_purchase_trips
,NULL AS store_traffic
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.daily_vistors_ga_vff AS fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun
WHERE day_date BETWEEN '2021-01-31' AND '2021-06-12' --hard code for historical data
AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}} --so historical doesn't run in daily job
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag,tran_tender_paypal_flag
UNION ALL

SELECT
day_date AS tran_date
,'OFFPRICE ONLINE' AS business_unit_desc
,828 AS store_num
,'other' AS bill_zip_code
,'USD' AS line_item_currency_code
,'Z' AS record_source
,'Mobile Web' AS platform_code
,'total_digital' AS loyalty_status
,'total_digital' AS buyerflow_code
,'N' AS aare_acquired_ind
,'N' AS aare_activated_ind
,'N' AS aare_retained_ind
,'N' AS aare_engaged_ind
,'total_digital' AS mrtk_chnl_type_code
,'total_digital' AS mrtk_chnl_finance_rollup_code
,'total_digital' AS mrtk_chnl_finance_detail_code
,'total_digital' AS engagement_cohort
,'N' AS tran_tender_cash_flag
,'N' AS tran_tender_check_flag
,'N' AS tran_tender_nordstrom_card_flag
,'N' AS tran_tender_non_nordstrom_credit_flag
,'N' AS tran_tender_non_nordstrom_debit_flag
,'N' AS tran_tender_nordstrom_gift_card_flag
,'N' AS tran_tender_nordstrom_note_flag
,'N' AS tran_tender_paypal_flag
,NULL AS gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_gross_demand_usd_amt
,NULL AS bopus_attr_digital_gross_demand_usd_amt
,NULL AS gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_gross_demand_units
,NULL AS bopus_attr_digital_gross_demand_units
,NULL AS canceled_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_gross_demand_usd_amt
,NULL AS canceled_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_units
,NULL AS bopus_attr_digital_canceled_gross_demand_units
,NULL AS reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_reported_demand_usd_amt
,NULL AS bopus_attr_digital_reported_demand_usd_amt
,NULL AS reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_reported_demand_units
,NULL AS bopus_attr_digital_reported_demand_units
,NULL AS canceled_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_reported_demand_usd_amt
,NULL AS canceled_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_units
,NULL AS bopus_attr_digital_canceled_reported_demand_units
,NULL AS emp_disc_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,NULL AS emp_disc_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_units
,NULL AS bopus_attr_digital_emp_disc_gross_demand_units
,NULL AS emp_disc_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,NULL AS emp_disc_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_units
,NULL AS bopus_attr_digital_emp_disc_reported_demand_units
,NULL AS emp_disc_fulfilled_demand_usd_amt
,NULL AS emp_disc_fulfilled_demand_units
,NULL AS emp_disc_op_gmv_usd_amt
,NULL AS emp_disc_op_gmv_units
,NULL AS fulfilled_demand_usd_amt
,NULL AS fulfilled_demand_units
,NULL AS post_fulfill_price_adj_usd_amt
,NULL AS same_day_store_return_usd_amt
,NULL AS same_day_store_return_units
,NULL AS last_chance_usd_amt
,NULL AS last_chance_units
,NULL AS actual_product_returns_usd_amt
,NULL AS actual_product_returns_units
,NULL AS op_gmv_usd_amt
,NULL AS op_gmv_units
,NULL AS reported_gmv_usd_amt
,NULL AS reported_gmv_units
,NULL AS merch_net_sales_usd_amt
,NULL AS merch_net_sales_units
,NULL AS reported_net_sales_usd_amt
,NULL AS reported_net_sales_units
,NULL AS variable_cost_usd_amt
,NULL AS gross_contribution_margin_usd_amt
,NULL AS net_contribution_margin_usd_amt
,NULL AS orders_count
,NULL AS canceled_fraud_orders_count
,NULL AS emp_disc_orders_count
,SUM(visitors_mow) AS visitors
,SUM(viewing_visitors_mow) AS viewing_visitors
,SUM(adding_visitors_mow) AS adding_visitors
,SUM(ordering_visitors_mow) AS ordering_visitors
,NULL AS sessions
,NULL AS viewing_sessions
,NULL AS adding_sessions
,NULL AS checkout_sessions
,NULL AS ordering_sessions
,NULL AS acp_count
,NULL AS unique_cust_count
,NULL AS total_pages_visited
,NULL AS total_unique_pages_visited
,NULL AS bounced_sessions
,NULL AS browse_sessions
,NULL AS search_sessions
,NULL AS wishlist_sessions
,NULL AS store_purchase_trips
,NULL AS store_traffic
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.daily_vistors_ga_vff AS fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun
WHERE day_date BETWEEN '2021-01-31' AND '2021-06-12' --hard code for historical data
AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}} --so historical doesn't run in daily job
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag

UNION ALL
SELECT
day_date AS tran_date
,'OFFPRICE ONLINE' AS business_unit_desc
,828 AS store_num
,'other' AS bill_zip_code
,'USD' AS line_item_currency_code
,'Z' AS record_source
,'IOS' AS platform_code
,'total_digital' AS loyalty_status
,'total_digital' AS buyerflow_code
,'N' AS aare_acquired_ind
,'N' AS aare_activated_ind
,'N' AS aare_retained_ind
,'N' AS aare_engaged_ind
,'total_digital' AS mrtk_chnl_type_code
,'total_digital' AS mrtk_chnl_finance_rollup_code
,'total_digital' AS mrtk_chnl_finance_detail_code
,'total_digital' AS engagement_cohort
,'N' AS tran_tender_cash_flag
,'N' AS tran_tender_check_flag
,'N' AS tran_tender_nordstrom_card_flag
,'N' AS tran_tender_non_nordstrom_credit_flag
,'N' AS tran_tender_non_nordstrom_debit_flag
,'N' AS tran_tender_nordstrom_gift_card_flag
,'N' AS tran_tender_nordstrom_note_flag
,'N' AS tran_tender_paypal_flag
,NULL AS gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_gross_demand_usd_amt
,NULL AS bopus_attr_digital_gross_demand_usd_amt
,NULL AS gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_gross_demand_units
,NULL AS bopus_attr_digital_gross_demand_units
,NULL AS canceled_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_gross_demand_usd_amt
,NULL AS canceled_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_units
,NULL AS bopus_attr_digital_canceled_gross_demand_units
,NULL AS reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_reported_demand_usd_amt
,NULL AS bopus_attr_digital_reported_demand_usd_amt
,NULL AS reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_reported_demand_units
,NULL AS bopus_attr_digital_reported_demand_units
,NULL AS canceled_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_reported_demand_usd_amt
,NULL AS canceled_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_units
,NULL AS bopus_attr_digital_canceled_reported_demand_units
,NULL AS emp_disc_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,NULL AS emp_disc_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_units
,NULL AS bopus_attr_digital_emp_disc_gross_demand_units
,NULL AS emp_disc_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,NULL AS emp_disc_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_units
,NULL AS bopus_attr_digital_emp_disc_reported_demand_units
,NULL AS emp_disc_fulfilled_demand_usd_amt
,NULL AS emp_disc_fulfilled_demand_units
,NULL AS emp_disc_op_gmv_usd_amt
,NULL AS emp_disc_op_gmv_units
,NULL AS fulfilled_demand_usd_amt
,NULL AS fulfilled_demand_units
,NULL AS post_fulfill_price_adj_usd_amt
,NULL AS same_day_store_return_usd_amt
,NULL AS same_day_store_return_units
,NULL AS last_chance_usd_amt
,NULL AS last_chance_units
,NULL AS actual_product_returns_usd_amt
,NULL AS actual_product_returns_units
,NULL AS op_gmv_usd_amt
,NULL AS op_gmv_units
,NULL AS reported_gmv_usd_amt
,NULL AS reported_gmv_units
,NULL AS merch_net_sales_usd_amt
,NULL AS merch_net_sales_units
,NULL AS reported_net_sales_usd_amt
,NULL AS reported_net_sales_units
,NULL AS variable_cost_usd_amt
,NULL AS gross_contribution_margin_usd_amt
,NULL AS net_contribution_margin_usd_amt
,NULL AS orders_count
,NULL AS canceled_fraud_orders_count
,NULL AS emp_disc_orders_count
,SUM(visitors_ios) AS visitors
,SUM(viewing_visitors_ios) AS viewing_visitors
,SUM(adding_visitors_ios) AS adding_visitors
,SUM(ordering_visitors_ios) AS ordering_visitors
,NULL AS sessions
,NULL AS viewing_sessions
,NULL AS adding_sessions
,NULL AS checkout_sessions
,NULL AS ordering_sessions
,NULL AS acp_count
,NULL AS unique_cust_count
,NULL AS total_pages_visited
,NULL AS total_unique_pages_visited
,NULL AS bounced_sessions
,NULL AS browse_sessions
,NULL AS search_sessions
,NULL AS wishlist_sessions
,NULL AS store_purchase_trips
,NULL AS store_traffic
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.daily_vistors_ga_vff AS fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun
WHERE day_date BETWEEN '2021-01-31' AND '2021-06-12' --hard code for historical data
AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}} --so historical doesn't run in daily job
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag,tran_tender_paypal_flag

UNION ALL
SELECT
day_date AS tran_date
,'OFFPRICE ONLINE' AS business_unit_desc
,828 AS store_num
,'other' AS bill_zip_code
,'USD' AS line_item_currency_code
,'Z' AS record_source
,'Desktop/Tablet' AS platform_code
,'total_digital' AS loyalty_status
,'total_digital' AS buyerflow_code
,'N' AS aare_acquired_ind
,'N' AS aare_activated_ind
,'N' AS aare_retained_ind
,'N' AS aare_engaged_ind
,'total_digital' AS mrtk_chnl_type_code
,'total_digital' AS mrtk_chnl_finance_rollup_code
,'total_digital' AS mrtk_chnl_finance_detail_code
,'total_digital' AS engagement_cohort
,'N' AS tran_tender_cash_flag
,'N' AS tran_tender_check_flag
,'N' AS tran_tender_nordstrom_card_flag
,'N' AS tran_tender_non_nordstrom_credit_flag
,'N' AS tran_tender_non_nordstrom_debit_flag
,'N' AS tran_tender_nordstrom_gift_card_flag
,'N' AS tran_tender_nordstrom_note_flag
,'N' AS tran_tender_paypal_flag
,NULL AS gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_gross_demand_usd_amt
,NULL AS bopus_attr_digital_gross_demand_usd_amt
,NULL AS gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_gross_demand_units
,NULL AS bopus_attr_digital_gross_demand_units
,NULL AS canceled_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_gross_demand_usd_amt
,NULL AS canceled_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_gross_demand_units
,NULL AS bopus_attr_digital_canceled_gross_demand_units
,NULL AS reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_reported_demand_usd_amt
,NULL AS bopus_attr_digital_reported_demand_usd_amt
,NULL AS reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_reported_demand_units
,NULL AS bopus_attr_digital_reported_demand_units
,NULL AS canceled_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_usd_amt
,NULL AS bopus_attr_digital_canceled_reported_demand_usd_amt
,NULL AS canceled_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_canceled_reported_demand_units
,NULL AS bopus_attr_digital_canceled_reported_demand_units
,NULL AS emp_disc_gross_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,NULL AS emp_disc_gross_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_gross_demand_units
,NULL AS bopus_attr_digital_emp_disc_gross_demand_units
,NULL AS emp_disc_reported_demand_usd_amt_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,NULL AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,NULL AS emp_disc_reported_demand_units_excl_bopus
,NULL AS bopus_attr_store_emp_disc_reported_demand_units
,NULL AS bopus_attr_digital_emp_disc_reported_demand_units
,NULL AS emp_disc_fulfilled_demand_usd_amt
,NULL AS emp_disc_fulfilled_demand_units
,NULL AS emp_disc_op_gmv_usd_amt
,NULL AS emp_disc_op_gmv_units
,NULL AS fulfilled_demand_usd_amt
,NULL AS fulfilled_demand_units
,NULL AS post_fulfill_price_adj_usd_amt
,NULL AS same_day_store_return_usd_amt
,NULL AS same_day_store_return_units
,NULL AS last_chance_usd_amt
,NULL AS last_chance_units
,NULL AS actual_product_returns_usd_amt
,NULL AS actual_product_returns_units
,NULL AS op_gmv_usd_amt
,NULL AS op_gmv_units
,NULL AS reported_gmv_usd_amt
,NULL AS reported_gmv_units
,NULL AS merch_net_sales_usd_amt
,NULL AS merch_net_sales_units
,NULL AS reported_net_sales_usd_amt
,NULL AS reported_net_sales_units
,NULL AS variable_cost_usd_amt
,NULL AS gross_contribution_margin_usd_amt
,NULL AS net_contribution_margin_usd_amt
,NULL AS orders_count
,NULL AS canceled_fraud_orders_count
,NULL AS emp_disc_orders_count
,SUM(visitors_web) AS visitors
,SUM(viewing_visitors_web) AS viewing_visitors
,SUM(adding_visitors_web) AS adding_visitors
,SUM(ordering_visitors_web) AS ordering_visitors
,NULL AS sessions
,NULL AS viewing_sessions
,NULL AS adding_sessions
,NULL AS checkout_sessions
,NULL AS ordering_sessions
,NULL AS acp_count
,NULL AS unique_cust_count
,NULL AS total_pages_visited
,NULL AS total_unique_pages_visited
,NULL AS bounced_sessions
,NULL AS browse_sessions
,NULL AS search_sessions
,NULL AS wishlist_sessions
,NULL AS store_purchase_trips
,NULL AS store_traffic
FROM `{{params.gcp_project_id}}`.t2dl_das_mothership.daily_vistors_ga_vff AS fun --historical data from T3DL_ACE_OP.daily_vistors_GA_VFF fun
WHERE day_date BETWEEN '2021-01-31' AND '2021-06-12' --hard code for historical data
AND day_date BETWEEN {{params.start_date}} AND {{params.end_date}} --so historical doesn't run in daily job
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag,tran_tender_paypal_flag
)

SELECT
tran_date
,business_unit_desc
,store_num
,bill_zip_code
,line_item_currency_code
,record_source
,platform_code
,loyalty_status
,buyerflow_code
,aare_acquired_ind
,aare_activated_ind
,aare_retained_ind
,aare_engaged_ind
,mrtk_chnl_type_code
,mrtk_chnl_finance_rollup_code
,mrtk_chnl_finance_detail_code
,engagement_cohort
,tran_tender_cash_flag
,tran_tender_check_flag
,tran_tender_nordstrom_card_flag
,tran_tender_non_nordstrom_credit_flag
,tran_tender_non_nordstrom_debit_flag
,tran_tender_nordstrom_gift_card_flag
,tran_tender_nordstrom_note_flag
,tran_tender_paypal_flag
,SUM(gross_demand_usd_amt_excl_bopus) AS gross_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_gross_demand_usd_amt) AS bopus_attr_store_gross_demand_usd_amt
,SUM(bopus_attr_digital_gross_demand_usd_amt) AS bopus_attr_digital_gross_demand_usd_amt
,SUM(gross_demand_units_excl_bopus) AS gross_demand_units_excl_bopus
,SUM(bopus_attr_store_gross_demand_units) AS bopus_attr_store_gross_demand_units
,SUM(bopus_attr_digital_gross_demand_units) AS bopus_attr_digital_gross_demand_units
,SUM(canceled_gross_demand_usd_amt_excl_bopus) AS canceled_gross_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_canceled_gross_demand_usd_amt) AS bopus_attr_store_canceled_gross_demand_usd_amt
,SUM(bopus_attr_digital_canceled_gross_demand_usd_amt) AS bopus_attr_digital_canceled_gross_demand_usd_amt
,SUM(canceled_gross_demand_units_excl_bopus) AS canceled_gross_demand_units_excl_bopus
,SUM(bopus_attr_store_canceled_gross_demand_units) AS bopus_attr_store_canceled_gross_demand_units
,SUM(bopus_attr_digital_canceled_gross_demand_units) AS bopus_attr_digital_canceled_gross_demand_units
,SUM(reported_demand_usd_amt_excl_bopus) AS reported_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_reported_demand_usd_amt) AS bopus_attr_store_reported_demand_usd_amt
,SUM(bopus_attr_digital_reported_demand_usd_amt) AS bopus_attr_digital_reported_demand_usd_amt
,SUM(reported_demand_units_excl_bopus) AS reported_demand_units_excl_bopus
,SUM(bopus_attr_store_reported_demand_units) AS bopus_attr_store_reported_demand_units
,SUM(bopus_attr_digital_reported_demand_units) AS bopus_attr_digital_reported_demand_units
,SUM(canceled_reported_demand_usd_amt_excl_bopus) AS canceled_reported_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_canceled_reported_demand_usd_amt) AS bopus_attr_store_canceled_reported_demand_usd_amt
,SUM(bopus_attr_digital_canceled_reported_demand_usd_amt) AS bopus_attr_digital_canceled_reported_demand_usd_amt
,SUM(canceled_reported_demand_units_excl_bopus) AS canceled_reported_demand_units_excl_bopus
,SUM(bopus_attr_store_canceled_reported_demand_units) AS bopus_attr_store_canceled_reported_demand_units
,SUM(bopus_attr_digital_canceled_reported_demand_units) AS bopus_attr_digital_canceled_reported_demand_units
,SUM(emp_disc_gross_demand_usd_amt_excl_bopus) AS emp_disc_gross_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_emp_disc_gross_demand_usd_amt) AS bopus_attr_store_emp_disc_gross_demand_usd_amt
,SUM(bopus_attr_digital_emp_disc_gross_demand_usd_amt) AS bopus_attr_digital_emp_disc_gross_demand_usd_amt
,SUM(emp_disc_gross_demand_units_excl_bopus) AS emp_disc_gross_demand_units_excl_bopus
,SUM(bopus_attr_store_emp_disc_gross_demand_units) AS bopus_attr_store_emp_disc_gross_demand_units
,SUM(bopus_attr_digital_emp_disc_gross_demand_units) AS bopus_attr_digital_emp_disc_gross_demand_units
,SUM(emp_disc_reported_demand_usd_amt_excl_bopus) AS emp_disc_reported_demand_usd_amt_excl_bopus
,SUM(bopus_attr_store_emp_disc_reported_demand_usd_amt) AS bopus_attr_store_emp_disc_reported_demand_usd_amt
,SUM(bopus_attr_digital_emp_disc_reported_demand_usd_amt) AS bopus_attr_digital_emp_disc_reported_demand_usd_amt
,SUM(emp_disc_reported_demand_units_excl_bopus) AS emp_disc_reported_demand_units_excl_bopus
,SUM(bopus_attr_store_emp_disc_reported_demand_units) AS bopus_attr_store_emp_disc_reported_demand_units
,SUM(bopus_attr_digital_emp_disc_reported_demand_units) AS bopus_attr_digital_emp_disc_reported_demand_units
,SUM(emp_disc_fulfilled_demand_usd_amt) AS emp_disc_fulfilled_demand_usd_amt
,SUM(emp_disc_fulfilled_demand_units) AS emp_disc_fulfilled_demand_units
,SUM(emp_disc_op_gmv_usd_amt) AS emp_disc_op_gmv_usd_amt
,SUM(emp_disc_op_gmv_units) AS emp_disc_op_gmv_units
,SUM(fulfilled_demand_usd_amt) AS fulfilled_demand_usd_amt
,SUM(fulfilled_demand_units) AS fulfilled_demand_units
,SUM(post_fulfill_price_adj_usd_amt) AS post_fulfill_price_adj_usd_amt
,SUM(same_day_store_return_usd_amt) AS same_day_store_return_usd_amt
,SUM(same_day_store_return_units) AS same_day_store_return_units
,SUM(last_chance_usd_amt) AS last_chance_usd_amt
,SUM(last_chance_units) AS last_chance_units
,SUM(actual_product_returns_usd_amt) AS actual_product_returns_usd_amt
,SUM(actual_product_returns_units) AS actual_product_returns_units
,SUM(op_gmv_usd_amt) AS op_gmv_usd_amt
,SUM(op_gmv_units) AS op_gmv_units
,SUM(reported_gmv_usd_amt) AS reported_gmv_usd_amt
,SUM(reported_gmv_units) AS reported_gmv_units
,SUM(merch_net_sales_usd_amt) AS merch_net_sales_usd_amt
,SUM(merch_net_sales_units) AS merch_net_sales_units
,SUM(reported_net_sales_usd_amt) AS reported_net_sales_usd_amt
,SUM(reported_net_sales_units) AS reported_net_sales_units
,SUM(variable_cost_usd_amt) AS variable_cost_usd_amt
,SUM(gross_contribution_margin_usd_amt) AS gross_contribution_margin_usd_amt
,SUM(net_contribution_margin_usd_amt) AS net_contribution_margin_usd_amt
,SUM(orders_count) AS orders_count
,SUM(canceled_fraud_orders_count) AS canceled_fraud_orders_count
,SUM(emp_disc_orders_count) AS emp_disc_orders_count
,SUM(visitors) AS visitors
,SUM(viewing_visitors) AS viewing_visitors
,SUM(adding_visitors) AS adding_visitors
,SUM(ordering_visitors) AS ordering_visitors
,SUM(sessions) AS sessions
,SUM(viewing_sessions) AS viewing_sessions
,SUM(adding_sessions) AS adding_sessions
,SUM(checkout_sessions) AS checkout_sessions
,SUM(ordering_sessions) AS ordering_sessions
,SUM(acp_count) AS acp_count
,SUM(unique_cust_count) AS unique_cust_count
,SUM(total_pages_visited) AS total_pages_visited
,SUM(total_unique_pages_visited) AS total_unique_pages_visited
,SUM(bounced_sessions) AS bounced_sessions
,SUM(browse_sessions) AS browse_sessions
,SUM(search_sessions) AS search_sessions
,SUM(wishlist_sessions) AS wishlist_sessions
,SUM(store_purchase_trips) AS store_purchase_trips
,SUM(store_traffic) AS store_traffic
,CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM combo
WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}}
AND tran_date BETWEEN '2021-01-31' AND DATE_SUB(CURRENT_DATE('PST8PDT'),INTERVAL 1 DAY)
GROUP BY tran_date,business_unit_desc,store_num,bill_zip_code,line_item_currency_code,record_source,platform_code,loyalty_status,buyerflow_code,aare_acquired_ind,aare_activated_ind,aare_retained_ind,aare_engaged_ind,mrtk_chnl_type_code,mrtk_chnl_finance_rollup_code,mrtk_chnl_finance_detail_code,engagement_cohort,tran_tender_cash_flag,tran_tender_check_flag,tran_tender_nordstrom_card_flag,tran_tender_non_nordstrom_credit_flag,tran_tender_non_nordstrom_debit_flag,tran_tender_nordstrom_gift_card_flag,tran_tender_nordstrom_note_flag,tran_tender_paypal_flag
)
;


DELETE FROM `{{params.gcp_project_id}}`.{{params.clarity_schema}}.finance_sales_demand_margin_traffic_fact WHERE tran_date BETWEEN {{params.start_date}} AND {{params.end_date}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.clarity_schema}}.finance_sales_demand_margin_traffic_fact
(
  tran_date
  ,business_unit_desc
  ,store_num
  ,bill_zip_code
  ,line_item_currency_code
  ,record_source
  ,platform_code
  ,loyalty_status
  ,buyerflow_code
  ,aare_acquired_ind
  ,aare_activated_ind
  ,aare_retained_ind
  ,aare_engaged_ind
  ,engagement_cohort
  ,mrtk_chnl_type_code
  ,mrtk_chnl_finance_rollup_code
  ,mrtk_chnl_finance_detail_code
  ,tran_tender_cash_flag
  ,tran_tender_check_flag
  ,tran_tender_nordstrom_card_flag
  ,tran_tender_non_nordstrom_credit_flag
  ,tran_tender_non_nordstrom_debit_flag
  ,tran_tender_nordstrom_gift_card_flag
  ,tran_tender_nordstrom_note_flag
  ,tran_tender_paypal_flag
  ,gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_gross_demand_usd_amt
  ,bopus_attr_digital_gross_demand_usd_amt
  ,gross_demand_units_excl_bopus
  ,bopus_attr_store_gross_demand_units
  ,bopus_attr_digital_gross_demand_units
  ,canceled_gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_canceled_gross_demand_usd_amt
  ,bopus_attr_digital_canceled_gross_demand_usd_amt
  ,canceled_gross_demand_units_excl_bopus
  ,bopus_attr_store_canceled_gross_demand_units
  ,bopus_attr_digital_canceled_gross_demand_units
  ,reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_reported_demand_usd_amt
  ,bopus_attr_digital_reported_demand_usd_amt
  ,reported_demand_units_excl_bopus
  ,bopus_attr_store_reported_demand_units
  ,bopus_attr_digital_reported_demand_units
  ,canceled_reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_canceled_reported_demand_usd_amt
  ,bopus_attr_digital_canceled_reported_demand_usd_amt
  ,canceled_reported_demand_units_excl_bopus
  ,bopus_attr_store_canceled_reported_demand_units
  ,bopus_attr_digital_canceled_reported_demand_units
  ,emp_disc_gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_emp_disc_gross_demand_usd_amt
  ,bopus_attr_digital_emp_disc_gross_demand_usd_amt
  ,emp_disc_gross_demand_units_excl_bopus
  ,bopus_attr_store_emp_disc_gross_demand_units
  ,bopus_attr_digital_emp_disc_gross_demand_units
  ,emp_disc_reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_emp_disc_reported_demand_usd_amt
  ,bopus_attr_digital_emp_disc_reported_demand_usd_amt
  ,emp_disc_reported_demand_units_excl_bopus
  ,bopus_attr_store_emp_disc_reported_demand_units
  ,bopus_attr_digital_emp_disc_reported_demand_units
  ,emp_disc_fulfilled_demand_usd_amt
  ,emp_disc_fulfilled_demand_units
  ,emp_disc_op_gmv_usd_amt
  ,emp_disc_op_gmv_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,post_fulfill_price_adj_usd_amt
  ,same_day_store_return_usd_amt
  ,same_day_store_return_units
  ,last_chance_usd_amt
  ,last_chance_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,reported_gmv_usd_amt
  ,reported_gmv_units
  ,merch_net_sales_usd_amt
  ,merch_net_sales_units
  ,reported_net_sales_usd_amt
  ,reported_net_sales_units
  ,variable_cost_usd_amt
  ,gross_contribution_margin_usd_amt
  ,net_contribution_margin_usd_amt
  ,orders_count
  ,canceled_fraud_orders_count
  ,emp_disc_orders_count
  ,visitors
  ,viewing_visitors
  ,adding_visitors
  ,ordering_visitors
  ,sessions
  ,viewing_sessions
  ,adding_sessions
  ,checkout_sessions
  ,ordering_sessions
  ,acp_count
  ,unique_cust_count
  ,total_pages_visited
  ,total_unique_pages_visited
  ,bounced_sessions
  ,browse_sessions
  ,search_sessions
  ,wishlist_sessions
  ,store_purchase_trips
  ,store_traffic
  ,dw_sys_load_tmstp
  )
  SELECT
  tran_date
  ,business_unit_desc
  ,store_num
  ,bill_zip_code
  ,line_item_currency_code
  ,record_source
  ,platform_code
  ,loyalty_status
  ,buyerflow_code
  ,aare_acquired_ind
  ,aare_activated_ind
  ,aare_retained_ind
  ,aare_engaged_ind
  ,engagement_cohort
  ,mrtk_chnl_type_code
  ,mrtk_chnl_finance_rollup_code
  ,mrtk_chnl_finance_detail_code
  ,tran_tender_cash_flag
  ,tran_tender_check_flag
  ,tran_tender_nordstrom_card_flag
  ,tran_tender_non_nordstrom_credit_flag
  ,tran_tender_non_nordstrom_debit_flag
  ,tran_tender_nordstrom_gift_card_flag
  ,tran_tender_nordstrom_note_flag
  ,tran_tender_paypal_flag
  ,gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_gross_demand_usd_amt
  ,bopus_attr_digital_gross_demand_usd_amt
  ,gross_demand_units_excl_bopus
  ,bopus_attr_store_gross_demand_units
  ,bopus_attr_digital_gross_demand_units
  ,canceled_gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_canceled_gross_demand_usd_amt
  ,bopus_attr_digital_canceled_gross_demand_usd_amt
  ,canceled_gross_demand_units_excl_bopus
  ,bopus_attr_store_canceled_gross_demand_units
  ,bopus_attr_digital_canceled_gross_demand_units
  ,reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_reported_demand_usd_amt
  ,bopus_attr_digital_reported_demand_usd_amt
  ,reported_demand_units_excl_bopus
  ,bopus_attr_store_reported_demand_units
  ,bopus_attr_digital_reported_demand_units
  ,canceled_reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_canceled_reported_demand_usd_amt
  ,bopus_attr_digital_canceled_reported_demand_usd_amt
  ,canceled_reported_demand_units_excl_bopus
  ,bopus_attr_store_canceled_reported_demand_units
  ,bopus_attr_digital_canceled_reported_demand_units
  ,emp_disc_gross_demand_usd_amt_excl_bopus
  ,bopus_attr_store_emp_disc_gross_demand_usd_amt
  ,bopus_attr_digital_emp_disc_gross_demand_usd_amt
  ,emp_disc_gross_demand_units_excl_bopus
  ,bopus_attr_store_emp_disc_gross_demand_units
  ,bopus_attr_digital_emp_disc_gross_demand_units
  ,emp_disc_reported_demand_usd_amt_excl_bopus
  ,bopus_attr_store_emp_disc_reported_demand_usd_amt
  ,bopus_attr_digital_emp_disc_reported_demand_usd_amt
  ,emp_disc_reported_demand_units_excl_bopus
  ,bopus_attr_store_emp_disc_reported_demand_units
  ,bopus_attr_digital_emp_disc_reported_demand_units
  ,emp_disc_fulfilled_demand_usd_amt
  ,emp_disc_fulfilled_demand_units
  ,emp_disc_op_gmv_usd_amt
  ,emp_disc_op_gmv_units
  ,fulfilled_demand_usd_amt
  ,fulfilled_demand_units
  ,post_fulfill_price_adj_usd_amt
  ,same_day_store_return_usd_amt
  ,same_day_store_return_units
  ,last_chance_usd_amt
  ,last_chance_units
  ,actual_product_returns_usd_amt
  ,actual_product_returns_units
  ,op_gmv_usd_amt
  ,op_gmv_units
  ,reported_gmv_usd_amt
  ,reported_gmv_units
  ,merch_net_sales_usd_amt
  ,merch_net_sales_units
  ,reported_net_sales_usd_amt
  ,reported_net_sales_units
  ,variable_cost_usd_amt
  ,gross_contribution_margin_usd_amt
  ,net_contribution_margin_usd_amt
  ,orders_count
  ,canceled_fraud_orders_count
  ,emp_disc_orders_count
  ,CAST(TRUNC(CAST(visitors AS FLOAT64)) AS INT64)
  ,CAST(TRUNC(CAST(viewing_visitors AS FLOAT64)) AS INT64)
  ,CAST(TRUNC(CAST(adding_visitors AS FLOAT64)) AS INT64)
  ,ordering_visitors
  ,sessions
  ,viewing_sessions
  ,adding_sessions
  ,checkout_sessions
  ,ordering_sessions
  ,acp_count
  ,unique_cust_count
  ,total_pages_visited
  ,total_unique_pages_visited
  ,bounced_sessions
  ,browse_sessions
  ,search_sessions
  ,wishlist_sessions
  ,CAST(TRUNC(CAST(store_purchase_trips AS FLOAT64)) AS INT64)
  ,CAST(TRUNC(CAST(store_traffic AS FLOAT64)) AS INT64)
  ,dw_sys_load_tmstp
 FROM
combo_sum;
