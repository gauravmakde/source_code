CREATE TEMPORARY TABLE IF NOT EXISTS employee_purchase_audit_tender_order

AS
SELECT DISTINCT hr.first_name,
 hr.last_name,
 hr.discount_percent,
 rtdf.business_day_date AS business_date,
 rtdf.intent_store_num AS intent_store,
 rtdf.ringing_store_num AS ringing_store,
 rtdf.tran_date AS ringing_date,
 rtdf.followup_slsprsn_num AS sales_person,
 rtdf.original_register_num,
 rtdf.global_tran_id AS original_transaction_id,
 rtdf.sa_tran_status_code AS status_code,
 rtdf.merch_dept_num AS dept,
 rtdf.employee_discount_flag AS emp_discount_flag,
 ABS(rtdf.employee_discount_usd_amt) AS emp_discount_amount,
 rtdf.employee_discount_num AS emp_discount_number,
 rtdf.upc_num AS upc_no,
 rtdf.nonmerch_fee_code AS fee_code,
 rtdf.line_item_seq_num AS line_item_num,
 rtdf.line_net_amt AS line_net_amount,
 rthd.total_amt AS tran_total_amount,
 rtdf.line_item_tax_amt AS item_tax_amt,
 rthd.total_manual_tax_usd_amt AS tran_total_tax_amt,
 rtdf.line_item_tax_exempt_flag AS tax_exempt_flag,
 rtdf.original_business_date AS original_bus_date,
 rtdf.original_ringing_store_num AS original_store,
 rtdf.tran_type_code AS tran_type,
 rtdf.employee_discount_usd_amt AS tran_total_emp_disc_amt,
 rtdf.original_ringing_store_num AS tax_store,
 rthd.total_manual_tax_usd_amt AS tran_total_manual_tax_amt,
 sku.sku_desc,
 sku.rms_sku_num AS sku_num,
 rtdf.line_item_fulfillment_type AS fulfillment_type,
 rthd.total_amt_currency_code AS tran_currency,
 rtdf.original_line_item_amt_currency_code AS original_currency,
 rtdf.line_item_order_type AS order_type,
 oldf.destination_city AS city,
 oldf.destination_state AS state,
 oldf.destination_zip_code AS zip,
 SUBSTR(oldf.destination_zip_code, 1, 3) AS zip_3,
 rtdf.original_tran_num,
 oldf.original_destination_city,
 oldf.original_destination_state,
 oldf.original_destination_zip_code AS original_destination_zip,
 rtdf.line_item_activity_type_code,
 rtdf.item_source,
 rtdf.banner,
 st.business_unit_desc,
 rttf.tender_item_account_number_v2,
  CASE
  WHEN LOWER(rttf.tender_type_code) IN (LOWER('CASH'), LOWER('CA'))
  THEN 'Cash'
  WHEN LOWER(rttf.tender_type_code) IN (LOWER('PAYPAL'), LOWER('PP'))
  THEN 'PayPal'
  WHEN LOWER(rttf.tender_type_code) = LOWER('CHECK')
  THEN 'Check'
  WHEN LOWER(rttf.card_type_code) = LOWER('NC') AND LOWER(rttf.card_subtype_code) IN (LOWER('MD'), LOWER('ND'))
  THEN 'Nordstrom Debit'
  WHEN LOWER(rttf.card_type_code) = LOWER('NV') AND LOWER(rttf.card_subtype_code) IN (LOWER('TP'), LOWER('TV'), LOWER('RV'
       )) OR LOWER(rttf.card_type_code) = LOWER('NV')
  THEN 'Nordstrom Visa'
  WHEN LOWER(rttf.card_type_code) = LOWER('NC') AND LOWER(rttf.card_subtype_code) IN (LOWER('RT'), LOWER('TR'), LOWER('RR'
      ))
  THEN 'Nordstrom Retail'
  WHEN LOWER(rttf.card_type_code) = LOWER('NB')
  THEN 'Nordstrom Corporate'
  WHEN LOWER(rttf.tender_type_code) = LOWER('NORDSTROM_NOTE') OR LOWER(rttf.card_subtype_code) = LOWER('NN')
  THEN 'Nordstrom Note'
  WHEN LOWER(rttf.tender_type_code) = LOWER('GIFT CARD') OR LOWER(rttf.card_subtype_code) = LOWER('GC')
  THEN 'Gift Card'
  WHEN LOWER(rttf.card_type_code) = LOWER('VC')
  THEN 'Third-party Visa'
  WHEN LOWER(rttf.card_type_code) = LOWER('MC')
  THEN 'Master Card'
  WHEN LOWER(rttf.card_type_code) = LOWER('DS')
  THEN 'Discover'
  WHEN LOWER(rttf.card_type_code) = LOWER('AE')
  THEN 'American Express'
  WHEN LOWER(rttf.card_type_code) = LOWER('JC')
  THEN 'JCB'
  WHEN LOWER(rttf.card_type_code) = LOWER('DC')
  THEN 'Diners'
  WHEN LOWER(rttf.card_type_code) = LOWER('IA')
  THEN 'Interact Debit'
  ELSE 'Other'
  END AS tender,
  CASE
  WHEN epat.tender_item_account_number_v2 IS NOT NULL
  THEN 'verified'
  ELSE 'not_verified'
  END AS tender_verification_flag,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS dw_sys_load_tmstp,
`{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST()  as dw_sys_load_tmstp_tz


FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_detail_fact_vw AS rtdf
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_hdr_fact AS rthd ON rtdf.global_tran_id = rthd.global_tran_id AND rtdf.business_day_date
    = rthd.business_day_date
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf ON LOWER(rtdf.order_num) = LOWER(oldf.order_num) AND rtdf.business_day_date
      = oldf.order_date_pacific AND rtdf.tran_line_id = oldf.order_line_num AND LOWER(rtdf.sku_num) = LOWER(oldf.sku_num
    )
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim AS st ON rtdf.ringing_store_num = st.store_num
 LEFT JOIN (SELECT rms_sku_num,
   sku_desc
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num ORDER BY channel_country DESC, dw_batch_date DESC)) = 1) AS sku
 ON LOWER(rtdf.sku_num) = LOWER(sku.rms_sku_num)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_hr_usr_vws.hr_worker_v2_dim AS hr ON LOWER(rtdf.employee_discount_num) = LOWER(hr.worker_number)
 LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.retail_tran_tender_fact AS rttf ON rtdf.global_tran_id = rttf.global_tran_id
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_trust_emp.employee_purchase_audit_look_up AS epat ON LOWER(rttf.tender_item_account_number_v2) =
  LOWER(epat.tender_item_account_number_v2)
WHERE rtdf.business_day_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 90 DAY)
 AND CAST(rtdf.employee_discount_flag AS FLOAT64) = 1
 AND LOWER(rtdf.employee_discount_num) IN (LOWER('10381911'), LOWER('11405024'), LOWER('30176067'), LOWER('30176088'),
   LOWER('10555621'), LOWER('9688193'), LOWER('10874634'), LOWER('30326208'), LOWER('30326211'), LOWER('30658407'),
   LOWER('30797349'), LOWER('30824733'), LOWER('30875907'), LOWER('1004100'), LOWER('7319320'), LOWER('2629285'), LOWER('8827750'
    ), LOWER('10771715'), LOWER('3202165'), LOWER('8256075'), LOWER('4027678'), LOWER('8955379'), LOWER('7008436'),
   LOWER('1001742'), LOWER('4027793'), LOWER('1002153'), LOWER('1404334'), LOWER('1003268'), LOWER('1002906'), LOWER('1510296'
    ), LOWER('1510494'), LOWER('8309668'), LOWER('1398320'), LOWER('8309650'), LOWER('10348696'), LOWER('7350846'),
   LOWER('8365041'), LOWER('4027769'), LOWER('1398353'), LOWER('1510577'), LOWER('1398338'), LOWER('1398346'), LOWER('20073'
    ), LOWER('8859829'));


DELETE FROM `{{params.gcp_project_id}}`.{{params.trust_emp_t2_schema}}.employee_purchase_audit_tender
WHERE business_date >= {{params.start_date}} AND business_date <= {{params.end_date}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.trust_emp_t2_schema}}.employee_purchase_audit_tender
(SELECT first_name,
  last_name,
  discount_percent,
  business_date,
  intent_store,
  ringing_store,
  ringing_date,
  sales_person,
  original_register_num,
  original_transaction_id,
  status_code,
  dept,
  emp_discount_flag,
  emp_discount_amount,
  emp_discount_number,
  upc_no,
  fee_code,
  line_item_num,
  line_net_amount,
  tran_total_amount,
  item_tax_amt,
  tran_total_tax_amt,
  tax_exempt_flag,
  original_bus_date,
  original_store,
  tran_type,
  tran_total_emp_disc_amt,
  tax_store,
  tran_total_manual_tax_amt,
  sku_desc,
  sku_num,
  fulfillment_type,
  tran_currency,
  original_currency,
  order_type,
  city,
  state,
  zip,
  zip_3,
  original_tran_num,
  original_destination_city,
  original_destination_state,
  original_destination_zip,
  line_item_activity_type_code,
  item_source,
  banner,
  business_unit_desc,
  tender_item_account_number_v2,
  tender,
  tender_verification_flag,
  dw_sys_load_tmstp,
  dw_sys_load_tmstp_tz
 FROM employee_purchase_audit_tender_order
 WHERE business_date >= {{params.start_date}}
  AND business_date <= {{params.end_date}});