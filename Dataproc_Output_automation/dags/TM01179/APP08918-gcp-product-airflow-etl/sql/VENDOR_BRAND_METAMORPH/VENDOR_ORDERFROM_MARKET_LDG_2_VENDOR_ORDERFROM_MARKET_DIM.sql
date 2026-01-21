--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_ORDERFROM_MARKET;' UPDATE FOR SESSION;





MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.vendor_orderfrom_market_dim AS tgt
USING (SELECT vendormarket_vendornumber AS vendor_num,
  vendormarket_marketcode AS market_code,
  vendormarket_currencyusagetype AS currency_usage_type,
  vendormarket_dischargeport AS discharge_port,
  vendormarket_freightpaymentmethod AS frieght_payment_method,
  vendormarket_importstatus AS import_status,
  vendormarket_importstatusdetail AS import_status_detail,
   CASE
   WHEN LOWER(vendormarket_isdonotrefusedelivery) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS donot_refuse_delivery_flag,
   CASE
   WHEN LOWER(vendormarket_ispreticket) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS preticket_flag,
   CASE
   WHEN LOWER(vendormarket_ispremark) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS premark_flag,
   CASE
   WHEN LOWER(vendormarket_isrestrictedfromintnlshipping) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS restricted_intnl_shipping_flag,
   CASE
   WHEN LOWER(vendormarket_isreturnallowed) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS return_allowed_flag,
   CASE
   WHEN LOWER(vendormarket_isreturnauthorizedrequired) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS return_authorization_required_flag,
   CASE
   WHEN LOWER(vendormarket_isreturnonly) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS return_only_flag,
  vendormarket_marketstatus AS market_status,
  vendormarket_purchasetype AS purchase_type,
  vendormarket_shippingmethod AS shipping_method_code,
  vendormarket_titlepass AS title_pass_code,
  vendormarket_transportationresponsibility AS transportation_responsibility_code,
  vendormarket_edicharacteristics_edisalesreportfrequency AS edi_sales_report_freq_flag,
  vendormarket_edicharacteristics_edipastcompliantdate AS edi_past_compliant_date,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isediadvancedshipmentnotification) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_advanced_shipment_notification_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isediavailable) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_available_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedicontract) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_contract_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedidestroyinfield) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_destroy_in_field_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedidomesticpurchaseorderformat) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_domestic_po_format_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isediinventory) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_inventory_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isediinvoice) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_invoice_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedipartnerpurchaseorder) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_partner_po_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedipartnerpurchaseorderchange) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_partner_po_change_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedipurchaseorder) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_po_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedipurchaseorderchange) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_po_change_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedipurchaseorderconfirmed) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_po_confirmed_flag,
   CASE
   WHEN LOWER(vendormarket_edicharacteristics_isedipurchaseorderfullreplacementsetup) = LOWER('true')
   THEN 'Y'
   ELSE 'N'
   END AS edi_po_full_replacement_setup_flag,
  vendormarket_authorizationnumber AS authorization_number,
  vendormarket_orderpaymentmethod AS order_pymnt_method_code,
  vendormarket_htsclassificationstatus AS hts_classification_status_code,
  vendormarket_freightterms AS freight_terms_code,
   (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_ORDERFROM_MARKET')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_ORDERFROM_MARKET')) AS dw_batch_date
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.vendor_orderfrom_market_ldg
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY vendormarket_vendornumber, vendormarket_marketcode ORDER BY eventcreatedate
      DESC, eventsequence DESC)) = 1) AS SRC
ON LOWER(SRC.vendor_num) = LOWER(tgt.vendor_num) AND LOWER(SRC.market_code) = LOWER(tgt.market_code)
WHEN MATCHED THEN UPDATE SET
 currency_usage_type = SRC.currency_usage_type,
 discharge_port = SRC.discharge_port,
 frieght_payment_method = SRC.frieght_payment_method,
 import_status = SRC.import_status,
 import_status_detail = SRC.import_status_detail,
 donot_refuse_delivery_flag = SRC.donot_refuse_delivery_flag,
 preticket_flag = SRC.preticket_flag,
 premark_flag = SRC.premark_flag,
 restricted_intnl_shipping_flag = SRC.restricted_intnl_shipping_flag,
 return_allowed_flag = SRC.return_allowed_flag,
 return_authorization_required_flag = SRC.return_authorization_required_flag,
 return_only_flag = SRC.return_only_flag,
 market_status = SRC.market_status,
 purchase_type = SRC.purchase_type,
 shipping_method_code = SRC.shipping_method_code,
 title_pass_code = SRC.title_pass_code,
 transportation_responsibility_code = SRC.transportation_responsibility_code,
 edi_sales_report_freq_flag = SRC.edi_sales_report_freq_flag,
 edi_past_compliant_date = SRC.edi_past_compliant_date,
 edi_advanced_shipment_notification_flag = SRC.edi_advanced_shipment_notification_flag,
 edi_available_flag = SRC.edi_available_flag,
 edi_contract_flag = SRC.edi_contract_flag,
 edi_destroy_in_field_flag = SRC.edi_destroy_in_field_flag,
 edi_domestic_po_format_flag = SRC.edi_domestic_po_format_flag,
 edi_inventory_flag = SRC.edi_inventory_flag,
 edi_invoice_flag = SRC.edi_invoice_flag,
 edi_partner_po_flag = SRC.edi_partner_po_flag,
 edi_partner_po_change_flag = SRC.edi_partner_po_change_flag,
 edi_po_flag = SRC.edi_po_flag,
 edi_po_change_flag = SRC.edi_po_change_flag,
 edi_po_confirmed_flag = SRC.edi_po_confirmed_flag,
 edi_po_full_replacement_setup_flag = SRC.edi_po_full_replacement_setup_flag,
 authorization_number = SRC.authorization_number,
 order_pymnt_method_code = SRC.order_pymnt_method_code,
 hts_classification_status_code = SRC.hts_classification_status_code,
 freight_terms_code = SRC.freight_terms_code,
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(SRC.vendor_num, SRC.market_code, SRC.currency_usage_type, SRC.discharge_port, SRC.frieght_payment_method
 , SRC.import_status, SRC.import_status_detail, SRC.donot_refuse_delivery_flag, SRC.preticket_flag, SRC.premark_flag,
 SRC.restricted_intnl_shipping_flag, SRC.return_allowed_flag, SRC.return_authorization_required_flag, SRC.return_only_flag
 , SRC.market_status, SRC.purchase_type, SRC.shipping_method_code, SRC.title_pass_code, SRC.transportation_responsibility_code
 , SRC.edi_sales_report_freq_flag, SRC.edi_past_compliant_date, SRC.edi_advanced_shipment_notification_flag, SRC.edi_available_flag
 , SRC.edi_contract_flag, SRC.edi_destroy_in_field_flag, SRC.edi_domestic_po_format_flag, SRC.edi_inventory_flag, SRC.edi_invoice_flag
 , SRC.edi_partner_po_flag, SRC.edi_partner_po_change_flag, SRC.edi_po_flag, SRC.edi_po_change_flag, SRC.edi_po_confirmed_flag
 , SRC.edi_po_full_replacement_setup_flag, SRC.authorization_number, SRC.order_pymnt_method_code, SRC.hts_classification_status_code
 , SRC.freight_terms_code, SRC.dw_batch_id, SRC.dw_batch_date, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 , CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));