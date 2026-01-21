--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_RELATIONSHIP;' UPDATE FOR SESSION;
DELETE FROM {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_relationship_dim
WHERE
  vendor_num IN (
    SELECT
      vendorrelationship_vendornumber
    FROM
      {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.vendor_relationship_order_from_ldg
  )
  AND LOWER(vendor_role_name) = LOWER('ORDER_FROM');

--COLLECT STATS ON PRD_NAP_STG.VENDOR_RELATIONSHIP_ORDER_FROM_LDG
INSERT INTO
  {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_relationship_dim (
    vendor_num,
    from_vendor_category,
    from_vendor_type,
    relationship_associated_flag,
    relationship_primary_flag,
    vendor_relationship_type,
    related_vendor_id,
    related_vendor_name,
    to_vendor_category,
    to_vendor_type,
    related_vendor_number,
    relationship_ticket_type_code,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp,
    vendor_role_name
  ) (
    SELECT DISTINCT
      vendorrelationship_vendornumber AS vendor_num,
      vendorrelationship_fromvendorcategory AS from_vendor_category,
      vendorrelationship_fromvendortype AS from_vendor_type,
      CASE
        WHEN LOWER(vendorrelationship_isrelationshipassociated) = LOWER('true') THEN 'Y'
        ELSE 'N'
      END AS relationship_associated_flag,
      CASE
        WHEN LOWER(vendorrelationship_isrelationshipprimary) = LOWER('true') THEN 'Y'
        ELSE 'N'
      END AS relationship_primary_flag,
      vendorrelationship_vendorrelationshiptype AS vendor_relationship_type,
      vendorrelationship_relatedvendorid AS related_vendor_id,
      vendorrelationship_relatedvendorname AS related_vendor_name,
      vendorrelationship_tovendorcategory AS to_vendor_category,
      vendorrelationship_tovendortype AS to_vendor_type,
      vendorrelationship_relatedvendornumber AS related_vendor_number,
      vendorrelationship_vendorrelationshiptickettype AS relationship_ticket_type_code,
      (
        SELECT
          batch_id
        FROM
          {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
        WHERE
          LOWER(subject_area_nm) = LOWER('VENDOR_RELATIONSHIP_ORDERFROM')
      ) AS dw_batch_id,
      (
        SELECT
          curr_batch_date
        FROM
          {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.elt_control
        WHERE
          LOWER(subject_area_nm) = LOWER('VENDOR_RELATIONSHIP_ORDERFROM')
      ) AS dw_batch_date,
      current_datetime ('PST8PDT') AS dw_sys_load_tmstp,
      current_datetime ('PST8PDT') AS dw_sys_updt_tmstp,
      'ORDER_FROM' AS vendor_role_name
    FROM
      {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.vendor_relationship_order_from_ldg AS src
    WHERE
      LOWER(TRIM(vendorrelationship_vendornumber)) <> LOWER('') QUALIFY (
        DENSE_RANK() OVER (
          PARTITION BY
            vendorrelationship_vendornumber
          ORDER BY
            eventcreatedate DESC,
            eventsequence DESC
        )
      ) = 1
  );

--COLLECT STATISTICS ON PRD_NAP_DIM.VENDOR_RELATIONSHIP_DIM