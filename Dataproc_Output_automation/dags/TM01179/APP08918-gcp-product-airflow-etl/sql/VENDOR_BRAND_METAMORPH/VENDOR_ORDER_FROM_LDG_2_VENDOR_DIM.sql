--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_ORDERFROM;'  UPDATE FOR SESSION;
--COLLECT STATS ON PRD_NAP_STG.VENDOR_ORDERFROM_LDG
MERGE INTO {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_dim AS tgt USING (
  SELECT
    vendornumber AS vendor_num,
    resourceid AS order_from_num,
    vendorname AS vendor_name,
    vendorrole_rolename AS vendor_role_name,
    vendoraltname_altname AS vendor_altname,
    vendoraltname_vendornametype AS vendor_alt_name_type,
    vendorstatus AS vendor_status_code,
    vendorrole_vendortype AS vendor_type_code,
    vendorrole_vendorcategory AS vendor_category_code,
    vendorlanguage AS vendor_lang_code,
    classificationtype AS classification_type_code,
    inventorymanagementinfolevel AS inventory_mgmt_level_code,
    CASE
      WHEN LOWER(isnpg) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS npg_flag,
    CASE
      WHEN LOWER(isdefectiveclaimagreement) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS defect_claim_agrmt_flag,
    debitmemo AS debit_memo_code,
    CASE
      WHEN LOWER(isautomaticinvoiceapproval) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS auto_invoice_approval_flag,
    CASE
      WHEN LOWER(isfreightcharge) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS freight_charge_flag,
    CASE
      WHEN LOWER(isprepaidinvoice) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS prepaid_invoice_flag,
    paymentmethod AS payment_method_code,
    paymentterms AS payment_terms_code,
    paymenttermsdatebasis AS payment_terms_date_basis,
    invoicecurrency AS vendor_invoice_currency_code,
    manufacturerprinciplecountry AS mnfctr_principal_cntry_code,
    partnergroup AS partner_group_code,
    inactivedate AS order_from_inactive_date,
    ladingport AS lading_port,
    eventsequence AS event_seq_num,
    (
      SELECT
        batch_id
      FROM
        {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
      WHERE
        LOWER(subject_area_nm) = LOWER('VENDOR_ORDERFROM')
    ) AS dw_batch_id,
    (
      SELECT
        curr_batch_date
      FROM
        {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
      WHERE
        LOWER(subject_area_nm) = LOWER('VENDOR_ORDERFROM')
    ) AS dw_batch_date
  FROM
    {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.vendor_orderfrom_ldg AS src0 QUALIFY (
      ROW_NUMBER() OVER (
        PARTITION BY
          vendornumber
        ORDER BY
          eventcreatedate DESC,
          eventsequence DESC
      )
    ) = 1
) AS t5 ON LOWER(t5.vendor_num) = LOWER(tgt.vendor_num) WHEN MATCHED THEN
UPDATE
SET
  order_from_num = t5.order_from_num,
  vendor_name = t5.vendor_name,
  vendor_role_name = t5.vendor_role_name,
  vendor_altname = t5.vendor_altname,
  vendor_alt_name_type = t5.vendor_alt_name_type,
  vendor_status_code = t5.vendor_status_code,
  vendor_type_code = t5.vendor_type_code,
  vendor_category_code = t5.vendor_category_code,
  vendor_lang_code = t5.vendor_lang_code,
  classification_type_code = t5.classification_type_code,
  inventory_mgmt_level_code = t5.inventory_mgmt_level_code,
  npg_flag = t5.npg_flag,
  defect_claim_agrmt_flag = t5.defect_claim_agrmt_flag,
  debit_memo_code = t5.debit_memo_code,
  auto_invoice_approval_flag = t5.auto_invoice_approval_flag,
  freight_charge_flag = t5.freight_charge_flag,
  prepaid_invoice_flag = t5.prepaid_invoice_flag,
  payment_method_code = t5.payment_method_code,
  payment_terms_code = t5.payment_terms_code,
  payment_terms_date_basis = t5.payment_terms_date_basis,
  vendor_invoice_currency_code = t5.vendor_invoice_currency_code,
  mnfctr_principal_cntry_code = t5.mnfctr_principal_cntry_code,
  partner_group_code = t5.partner_group_code,
  order_from_inactive_date = t5.order_from_inactive_date,
  lading_port = t5.lading_port,
  event_seq_num = t5.event_seq_num,
  dw_batch_id = t5.dw_batch_id,
  dw_batch_date = t5.dw_batch_date,
  dw_sys_updt_tmstp = current_datetime('PST8PDT') WHEN NOT MATCHED THEN INSERT (
    vendor_num,
    order_from_num,
    vendor_name,
    vendor_role_name,
    vendor_status_code,
    vendor_type_code,
    vendor_category_code,
    vendor_lang_code,
    vendor_altname,
    vendor_alt_name_type,
    classification_type_code,
    inventory_mgmt_level_code,
    npg_flag,
    defect_claim_agrmt_flag,
    debit_memo_code,
    auto_invoice_approval_flag,
    freight_charge_flag,
    prepaid_invoice_flag,
    payment_method_code,
    payment_terms_code,
    payment_terms_date_basis,
    vendor_invoice_currency_code,
    mnfctr_principal_cntry_code,
    partner_group_code,
    order_from_inactive_date,
    lading_port,
    event_seq_num,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
  )
VALUES
  (
    t5.vendor_num,
    t5.order_from_num,
    t5.vendor_name,
    t5.vendor_role_name,
    t5.vendor_status_code,
    t5.vendor_type_code,
    t5.vendor_category_code,
    t5.vendor_lang_code,
    t5.vendor_altname,
    t5.vendor_alt_name_type,
    t5.classification_type_code,
    t5.inventory_mgmt_level_code,
    t5.npg_flag,
    t5.defect_claim_agrmt_flag,
    t5.debit_memo_code,
    t5.auto_invoice_approval_flag,
    t5.freight_charge_flag,
    t5.prepaid_invoice_flag,
    t5.payment_method_code,
    t5.payment_terms_code,
    t5.payment_terms_date_basis,
    t5.vendor_invoice_currency_code,
    t5.mnfctr_principal_cntry_code,
    t5.partner_group_code,
    t5.order_from_inactive_date,
    t5.lading_port,
    t5.event_seq_num,
    t5.dw_batch_id,
    t5.dw_batch_date,
    current_datetime ('PST8PDT'),
    current_datetime ('PST8PDT')
  );

/*.IF ERRORCODE <> 0 THEN .QUIT 1*/
--COLLECT STATISTICS COLUMN(vendor_num) ON PRD_NAP_DIM.VENDOR_DIM;