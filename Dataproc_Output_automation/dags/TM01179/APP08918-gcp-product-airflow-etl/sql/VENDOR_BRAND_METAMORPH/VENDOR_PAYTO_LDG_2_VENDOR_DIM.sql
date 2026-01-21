--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_PAYTO;'  UPDATE FOR SESSION;
--COLLECT STATS ON PRD_NAP_STG.VENDOR_PAYTO_LDG
MERGE INTO {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_dim AS tgt USING (
  SELECT
    src0.vendornumber AS vendor_num,
    src0.resourceid AS payto_num,
    src0.vendorname AS vendor_name,
    src0.vendorrole_rolename AS vendor_role_name,
    src0.vendoraltname_altname AS vendor_altname,
    src0.vendoraltname_vendornametype AS vendor_alt_name_type,
    src0.vendorstatus AS vendor_status_code,
    src0.vendorrole_vendortype AS vendor_type_code,
    src0.vendorrole_vendorcategory AS vendor_category_code,
    src0.vendorlanguage AS vendor_lang_code,
    CASE
      WHEN LOWER(src0.codeofconduct) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS code_of_conduct_flag,
    src0.codeofconductupdatedate AS code_of_conduct_update_date,
    src0.debitmemocode AS debit_memo_code,
    CASE
      WHEN LOWER(src0.isfreightcharge) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS freight_charge_flag,
    CASE
      WHEN LOWER(src0.isprepaidinvoice) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS prepaid_invoice_flag,
    src0.paymentmethod AS payment_method_code,
    src0.paymentterms AS payment_terms_code,
    src0.paymenttermsdatebasis AS payment_terms_date_basis,
    src0.invoicecurrency AS vendor_invoice_currency_code,
    src0.dropshippaymentterms AS drop_ship_payment_terms,
    CASE
      WHEN LOWER(tgt0.vendor_status_code) <> LOWER('I')
      AND LOWER(src0.vendorstatus) = LOWER('I') THEN PARSE_DATE ('%F', SUBSTR (src0.eventcreatedate, 1, 10))
      ELSE NULL
    END AS payto_inactive_date,
    src0.eventsequence AS event_seq_num,
    (
      SELECT
        batch_id
      FROM
        {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
      WHERE
        LOWER(subject_area_nm) = LOWER('VENDOR_PAYTO')
    ) AS dw_batch_id,
    (
      SELECT
        curr_batch_date
      FROM
        {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
      WHERE
        LOWER(subject_area_nm) = LOWER('VENDOR_PAYTO')
    ) AS dw_batch_date
  FROM
    {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.vendor_payto_ldg AS src0
    LEFT JOIN {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_dim AS tgt0 ON LOWER(tgt0.vendor_num) = LOWER(src0.vendornumber) QUALIFY (
      ROW_NUMBER() OVER (
        PARTITION BY
          src0.vendornumber
        ORDER BY
          src0.eventcreatedate DESC,
          src0.eventsequence DESC
      )
    ) = 1
) AS t5 ON LOWER(t5.vendor_num) = LOWER(tgt.vendor_num) WHEN MATCHED THEN
UPDATE
SET
  payto_num = t5.payto_num,
  vendor_name = t5.vendor_name,
  vendor_role_name = t5.vendor_role_name,
  vendor_altname = t5.vendor_altname,
  vendor_alt_name_type = t5.vendor_alt_name_type,
  vendor_status_code = t5.vendor_status_code,
  vendor_type_code = t5.vendor_type_code,
  vendor_category_code = t5.vendor_category_code,
  vendor_lang_code = t5.vendor_lang_code,
  debit_memo_code = t5.debit_memo_code,
  freight_charge_flag = t5.freight_charge_flag,
  prepaid_invoice_flag = t5.prepaid_invoice_flag,
  payment_method_code = t5.payment_method_code,
  payment_terms_code = t5.payment_terms_code,
  payment_terms_date_basis = t5.payment_terms_date_basis,
  vendor_invoice_currency_code = t5.vendor_invoice_currency_code,
  drop_ship_payment_terms = t5.drop_ship_payment_terms,
  code_of_conduct_flag = t5.code_of_conduct_flag,
  code_of_conduct_update_date = t5.code_of_conduct_update_date,
  payto_inactive_date = CAST(t5.payto_inactive_date AS STRING),
  event_seq_num = t5.event_seq_num,
  dw_batch_id = t5.dw_batch_id,
  dw_batch_date = t5.dw_batch_date,
  dw_sys_updt_tmstp = CAST(
    FORMAT_TIMESTAMP ('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME
  ) WHEN NOT MATCHED THEN INSERT (
    vendor_num,
    payto_num,
    vendor_name,
    vendor_role_name,
    vendor_status_code,
    vendor_type_code,
    vendor_category_code,
    vendor_lang_code,
    vendor_altname,
    vendor_alt_name_type,
    code_of_conduct_flag,
    code_of_conduct_update_date,
    debit_memo_code,
    freight_charge_flag,
    prepaid_invoice_flag,
    payment_method_code,
    payment_terms_code,
    payment_terms_date_basis,
    vendor_invoice_currency_code, 
    drop_ship_payment_terms, 
    payto_inactive_date, 
    event_seq_num, 
    dw_batch_id, 
    dw_batch_date, 
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp

  )
VALUES
  (
    t5.vendor_num,
    t5.payto_num,
    t5.vendor_name,
    t5.vendor_role_name,
    t5.vendor_status_code,
    t5.vendor_type_code,
    t5.vendor_category_code,
    t5.vendor_lang_code,
    t5.vendor_altname, 
    t5.vendor_alt_name_type, 
    t5.code_of_conduct_flag, 
    t5.code_of_conduct_update_date, 
    t5.debit_memo_code,
    t5.freight_charge_flag,
    t5.prepaid_invoice_flag,
    t5.payment_method_code,
    t5.payment_terms_code,
    t5.payment_terms_date_basis,
    t5.vendor_invoice_currency_code,
    t5.drop_ship_payment_terms,
    CAST(t5.payto_inactive_date AS STRING),
    t5.event_seq_num, 
    t5.dw_batch_id, 
    t5.dw_batch_date, 
    current_datetime('PST8PDT'), 
    current_datetime('PST8PDT')
  );

--COLLECT STATISTICS COLUMN(vendor_num) ON PRD_NAP_DIM.VENDOR_DIM