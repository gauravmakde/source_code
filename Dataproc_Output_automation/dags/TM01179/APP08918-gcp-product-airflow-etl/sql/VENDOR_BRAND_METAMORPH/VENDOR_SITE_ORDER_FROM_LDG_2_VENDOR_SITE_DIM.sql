--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_SITE;' UPDATE FOR SESSION;
DELETE FROM {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_site_dim
WHERE
  vendor_num IN (
    SELECT
      vendornumber
    FROM
      {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.vendor_site_order_from_ldg
  )
  AND LOWER(vendor_role_name) = LOWER('ORDER_FROM');

--COLLECT STATS ON PRD_NAP_STG.VENDOR_SITE_ORDER_FROM_LDG
INSERT INTO
  {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_site_dim (
    vendor_num,
    vendor_site_resource_id,
    vendor_postal_standard_code,
    vendor_primary_postal_address_flag,
    vendor_postal_resource_id,
    vendor_postal_original_code,
    country_code,
    vendor_currency_code,
    vendor_operating_unit,
    payment_method_code,
    payment_terms_code,
    vendor_site_name,
    vendor_language,
    vendor_site_status_code,
    vendor_site_usage_code,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp,
    vendor_role_name
  ) (
    SELECT DISTINCT
      vendornumber AS vendor_num,
      vendorsite_resourceid AS vendor_site_resource_id,
      vendorsite_vendorpostaladdress_postalstandardcode AS vendor_postal_standard_code,
      CASE
        WHEN LOWER(
          vendorsite_vendorpostaladdress_isprimaryvendorpostaladdress
        ) = LOWER('true') THEN 'Y'
        ELSE 'N'
      END AS vendor_primary_postal_address_flag,
      vendorsite_vendorpostaladdress_resourceid AS vendor_postal_resource_id,
      vendorsite_vendorpostaladdress_postaloriginalcode AS vendor_postal_original_code,
      vendorsite_vendorpostaladdress_country AS country_code,
      vendorsite_currency AS vendor_currency_code,
      vendorsite_operatingunit AS vendor_operating_unit,
      vendorsite_paymentmethod AS payment_method_code,
      vendorsite_paymentterms AS payment_terms_code,
      vendorsite_sitename AS vendor_site_name,
      vendorsite_vendorlanguage AS vendor_language,
      vendorsite_vendorsitestatus AS vendor_site_status_code,
      vendorsite_vendorsiteusage AS vendor_site_usage_code,
      (
        SELECT
          batch_id
        FROM
          {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
        WHERE
          LOWER(subject_area_nm) = LOWER('VENDOR_SITE_ORDERFROM')
      ) AS dw_batch_id,
      (
        SELECT
          curr_batch_date
        FROM
          {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
        WHERE
          LOWER(subject_area_nm) = LOWER('VENDOR_SITE_ORDERFROM')
      ) AS dw_batch_date,
      CAST(
        FORMAT_TIMESTAMP ('%F %H:%M:%S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME
      ) AS dw_sys_load_tmstp,
      CAST(
        FORMAT_TIMESTAMP ('%F %H:%M:%S', CURRENT_DATETIME ('PST8PDT')) AS DATETIME
      ) AS dw_sys_updt_tmstp,
      'ORDER_FROM' AS vendor_role_name
    FROM
      {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.vendor_site_order_from_ldg AS src QUALIFY (
        DENSE_RANK() OVER (
          PARTITION BY
            vendornumber
          ORDER BY
            eventcreatedate DESC,
            eventsequence DESC
        )
      ) = 1
  );

--COLLECT STATISTICS ON PRD_NAP_DIM.VENDOR_SITE_DIM