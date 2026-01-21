--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_ORDERFROM_PAYMENTTERMS_XREF;' UPDATE FOR SESSION;




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.vendor_orderfrom_paymentterms_xref
WHERE vendor_num IN (SELECT orgretaildepartmentpaymentterm_vendornumber
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_orderfrom_paymentterms_xref_ldg);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.vendor_orderfrom_paymentterms_xref (vendor_num, order_from_resource_id, org_retail_dept_code,
 start_date, market_code, payment_terms_code, dw_batch_id, dw_batch_date, dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT DISTINCT orgretaildepartmentpaymentterm_vendornumber AS vendor_num,
  orgretaildepartmentpaymentterm_resourceid AS order_from_resource_id,
  orgretaildepartmentpaymentterm_orgretaildepartmentcode AS org_retail_dept_code,
  orgretaildepartmentpaymentterm_startdate AS start_date,
  orgretaildepartmentpaymentterm_marketcode AS market_code,
  orgretaildepartmentpaymentterm_paymentterms AS payment_terms_code,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_ORDERFROM_PAYMENTTERMS_XREF')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_ORDERFROM_PAYMENTTERMS_XREF')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_orderfrom_paymentterms_xref_ldg AS src
 QUALIFY (DENSE_RANK() OVER (PARTITION BY orgretaildepartmentpaymentterm_vendornumber ORDER BY eventcreatedate DESC,
      eventsequence DESC)) = 1);