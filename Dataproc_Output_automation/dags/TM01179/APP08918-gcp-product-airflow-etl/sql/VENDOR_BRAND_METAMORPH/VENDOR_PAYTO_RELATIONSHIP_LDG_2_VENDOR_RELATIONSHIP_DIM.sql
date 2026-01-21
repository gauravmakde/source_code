--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_PAYTO_RELATIONSHIP;'  UPDATE FOR SESSION;
--COLLECT STATS ON PRD_NAP_STG.VENDOR_PAYTO_RELATIONSHIP_LDG
MERGE INTO {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_payto_relationship_dim AS tgt USING (
  SELECT
    src0.vendornumber AS payto_vendor_num,
    src0.vendorname AS payto_vendor_name,
    src0.vendorrelationship_relatedvendornumber AS order_from_vendor_num,
    src0.vendorrelationship_relatedvendorname AS order_from_vendor_name,
    src0.vendorstatus AS payto_vendor_status_code,
    src0.vendorrelationship_relatedpaytostatus AS order_from_vendor_status_code,
    jwn_udf.iso8601_tmstp(src0.vendorrelationship_relatedpaytocreatedate) AS related_payto_create_date,
    jwn_udf.udf_time_zone (jwn_udf.iso8601_tmstp(COLLATE(src0.vendorrelationship_relatedpaytocreatedate,''))) AS related_payto_create_date_tz, 
    src0.vendorrelationship_relatedpaytodropshippaymentterms AS related_payto_drop_ship_payment_terms,
    CASE
      WHEN LOWER(src0.vendorrelationship_isrelationshipprimary) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS is_relationship_primary,
    CASE
      WHEN LOWER(src0.vendorrelationship_isrelationshipassociated) = LOWER('true') THEN 'Y'
      ELSE 'N'
    END AS is_relationship_associated,
    src0.vendorrelationship_fromvendorcategory AS from_vendor_category_code,
    src0.vendorrelationship_fromvendortype AS from_vendor_type_code,
    src0.vendorrelationship_tovendorcategory AS to_vendor_category_code,
    src0.vendorrelationship_tovendortype AS to_vendor_type_code,
    src0.vendorrelationship_vendorrelationshiptype AS vendor_relationship_type,
    src0.vendorrelationship_vendorrelationshiptickettype AS vendor_relationship_ticket_type,
    src0.vendorrelationship_vendorrelationshipstatus AS vendor_relationship_status_code,
    CASE
      WHEN LOWER(tgt0.payto_vendor_status_code) <> LOWER('I')
      AND LOWER(src0.vendorstatus) = LOWER('I') THEN PARSE_DATE ('%F', SUBSTR (src0.eventcreatedate, 1, 10))
      ELSE NULL
    END AS payto_inactive_date,
    CASE
      WHEN LOWER(tgt0.order_from_vendor_status_code) <> LOWER('I')
      AND LOWER(src0.vendorrelationship_relatedpaytostatus) = LOWER('I') THEN PARSE_DATE ('%F', SUBSTR (src0.eventcreatedate, 1, 10))
      ELSE NULL
    END AS order_from_vendor_inactive_date,
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
    {{params.gcp_project_id}}.{{params.db_env}}_nap_base_vws.vendor_payto_relationship_ldg AS src0
    LEFT JOIN {{params.gcp_project_id}}.{{params.db_env}}_nap_dim.vendor_payto_relationship_dim AS tgt0 ON LOWER(tgt0.payto_vendor_num) = LOWER(src0.vendornumber)
    AND LOWER(tgt0.order_from_vendor_num) = LOWER(src0.vendorrelationship_relatedvendornumber)
  WHERE
    LOWER(TRIM(src0.vendorrelationship_relatedvendornumber)) <> LOWER('')
    AND src0.vendorrelationship_relatedvendornumber IS NOT NULL QUALIFY (
      ROW_NUMBER() OVER (
        PARTITION BY
          src0.vendornumber,
          src0.vendorrelationship_relatedvendornumber
        ORDER BY
          src0.eventcreatedate DESC,
          src0.eventsequence DESC
      )
    ) = 1
) AS SRC ON LOWER(SRC.payto_vendor_num) = LOWER(tgt.payto_vendor_num)
AND LOWER(SRC.order_from_vendor_num) = LOWER(tgt.order_from_vendor_num) WHEN MATCHED THEN
UPDATE
SET
  payto_vendor_name = SRC.payto_vendor_name,
  order_from_vendor_name = SRC.order_from_vendor_name,
  payto_vendor_status_code = SRC.payto_vendor_status_code,
  order_from_vendor_status_code = SRC.order_from_vendor_status_code,
  related_payto_create_date = CAST(SRC.related_payto_create_date AS TIMESTAMP),
  related_payto_create_date_tz = SRC.related_payto_create_date_tz,
  related_payto_drop_ship_payment_terms = SRC.related_payto_drop_ship_payment_terms,
  is_relationship_primary = SRC.is_relationship_primary,
  is_relationship_associated = SRC.is_relationship_associated,
  from_vendor_category_code = SRC.from_vendor_category_code,
  from_vendor_type_code = SRC.from_vendor_type_code,
  to_vendor_category_code = SRC.to_vendor_category_code,
  to_vendor_type_code = SRC.to_vendor_type_code,
  vendor_relationship_type = SRC.vendor_relationship_type,
  vendor_relationship_ticket_type = SRC.vendor_relationship_ticket_type,
  vendor_relationship_status_code = SRC.vendor_relationship_status_code,
  payto_inactive_date = SRC.payto_inactive_date,
  order_from_vendor_inactive_date = SRC.order_from_vendor_inactive_date,
  event_seq_num = SRC.event_seq_num,
  dw_batch_id = SRC.dw_batch_id,
  dw_batch_date = SRC.dw_batch_date,
  dw_sys_updt_tmstp = CAST(
    FORMAT_TIMESTAMP ('%F %H:%M:%S', CURRENT_DATETIME ()) AS DATETIME
  ) WHEN NOT MATCHED THEN INSERT
VALUES
  (
    SRC.payto_vendor_num,
    SRC.order_from_vendor_num,
    SRC.payto_vendor_name,
    SRC.order_from_vendor_name,
    SRC.payto_vendor_status_code,
    SRC.order_from_vendor_status_code,
    CAST(SRC.related_payto_create_date AS TIMESTAMP),
    SRC.related_payto_create_date_tz,
    SRC.related_payto_drop_ship_payment_terms,
    SRC.is_relationship_primary,
    SRC.is_relationship_associated,
    SRC.from_vendor_category_code,
    SRC.from_vendor_type_code,
    SRC.to_vendor_category_code,
    SRC.to_vendor_type_code,
    SRC.vendor_relationship_type,
    SRC.vendor_relationship_ticket_type,
    SRC.vendor_relationship_status_code,
    SRC.payto_inactive_date,
    SRC.order_from_vendor_inactive_date,
    SRC.event_seq_num,
    SRC.dw_batch_id,
    SRC.dw_batch_date,
    current_datetime('PST8PDT'), 
    current_datetime('PST8PDT')
  );

--COLLECT STATISTICS COLUMN(payto_vendor_num) , COLUMN(order_from_vendor_num) ON PRD_NAP_DIM.VENDOR_PAYTO_RELATIONSHIP_DIM