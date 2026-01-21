--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_PARNTER;'  UPDATE FOR SESSION;
--COLLECT STATS ON PRD_NAP_STG.VENDOR_PARTNER_LDG
MERGE INTO {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_dim AS tgt USING (
  SELECT
    src0.vendornumber AS vendor_num,
    src0.resourceid AS partner_num,
    src0.vendorname AS vendor_name,
    src0.vendorrole_rolename AS vendor_role_name,
    src0.vendoraltname_altname AS vendor_altname,
    src0.vendoraltname_vendornametype AS vendor_alt_name_type,
    src0.vendorstatus AS vendor_status_code,
    src0.vendorrole_vendortype AS vendor_type_code,
    src0.vendorrole_vendorcategory AS vendor_category_code,
    src0.vendorlanguage AS vendor_lang_code,
    'CHECK' AS payment_method_code,
    src0.paymentterms AS payment_terms_code,
    src0.paymenttermsdatebasis AS payment_terms_date_basis,
    src0.invoicecurrency AS vendor_invoice_currency_code,
    src0.partnergroup AS partner_group_code,
    src0.ladingport AS lading_port,
    CASE
      WHEN LOWER(src0.isnordstromlabel) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS nord_label_flag,
    src0.nordstromlabelmodificationdate AS nord_label_modify_date,
    CASE
      WHEN LOWER(tgt0.vendor_status_code) <> LOWER('I')
      AND LOWER(src0.vendorstatus) = LOWER('I') THEN PARSE_DATE ('%F', SUBSTR (src0.eventcreatedate, 1, 10))
      ELSE NULL
    END AS partner_inactive_date,
    src0.principlecountry AS principal_country_code,
    src0.eventsequence AS event_seq_num,
    (
      SELECT
        batch_id
      FROM
        {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
      WHERE
        LOWER(subject_area_nm) = LOWER('VENDOR_PARNTER')
    ) AS dw_batch_id,
    (
      SELECT
        curr_batch_date
      FROM
        {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
      WHERE
        LOWER(subject_area_nm) = LOWER('VENDOR_PARNTER')
    ) AS dw_batch_date
  FROM
    {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.vendor_partner_ldg AS src0
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
  partner_num = t5.partner_num,
  vendor_name = t5.vendor_name,
  vendor_role_name = t5.vendor_role_name,
  vendor_altname = t5.vendor_altname,
  vendor_alt_name_type = t5.vendor_alt_name_type,
  vendor_status_code = t5.vendor_status_code,
  vendor_type_code = t5.vendor_type_code,
  vendor_category_code = t5.vendor_category_code,
  vendor_lang_code = t5.vendor_lang_code,
  payment_method_code = t5.payment_method_code,
  payment_terms_code = t5.payment_terms_code,
  payment_terms_date_basis = t5.payment_terms_date_basis,
  vendor_invoice_currency_code = t5.vendor_invoice_currency_code,
  partner_group_code = t5.partner_group_code,
  lading_port = t5.lading_port,
  nord_label_flag = t5.nord_label_flag,
  nord_label_modify_date = t5.nord_label_modify_date,
  partner_inactive_date = CAST(t5.partner_inactive_date AS STRING),
  principal_country_code = t5.principal_country_code,
  event_seq_num = t5.event_seq_num,
  dw_batch_id = t5.dw_batch_id,
  dw_batch_date = t5.dw_batch_date,
  dw_sys_updt_tmstp = current_datetime ('PST8PDT') WHEN NOT MATCHED THEN INSERT (
    vendor_num,
    partner_num,
    vendor_name,
    vendor_role_name,
    vendor_status_code,
    vendor_type_code,
    vendor_category_code,
    vendor_lang_code,
    vendor_altname,
    vendor_alt_name_type,
    principal_country_code,
    payment_method_code,
    payment_terms_code,
    payment_terms_date_basis,
    vendor_invoice_currency_code,
    partner_group_code,
    lading_port,
    nord_label_flag,
    nord_label_modify_date,
    partner_inactive_date,
    event_seq_num,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
  )
VALUES
  (
    t5.vendor_num,
    t5.partner_num,
    t5.vendor_name,
    t5.vendor_role_name,
    t5.vendor_status_code,
    t5.vendor_type_code,
    t5.vendor_category_code,
    t5.vendor_lang_code,
    t5.vendor_altname,
    t5.vendor_alt_name_type,
    t5.principal_country_code,
    t5.payment_method_code,
    t5.payment_terms_code,
    t5.payment_terms_date_basis,
    t5.vendor_invoice_currency_code,
    t5.partner_group_code,
    t5.lading_port,
    t5.nord_label_flag,
    t5.nord_label_modify_date,
    CAST(t5.partner_inactive_date AS STRING),
    t5.event_seq_num,
    t5.dw_batch_id,
    t5.dw_batch_date,
    current_datetime ('PST8PDT'),
    current_datetime ('PST8PDT')
  );

--COLLECT STATISTICS COLUMN (vendor_num) ON PRD_NAP_DIM.VENDOR_DIM