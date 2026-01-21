--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_LABEL;'  UPDATE FOR SESSION;






MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.vendor_label_dim AS tgt
USING (SELECT vendorbrand_vendorlabel_code AS vendor_label_code,
  vendorbrand_vendorlabel_name AS vendor_label_name,
  vendorbrand_vendorlabel_description AS vendor_label_desc,
  vendorbrand_vendorlabel_labelstatus AS vendor_label_status,
  vendorbrand_vendorlabel_labelinactivedate AS label_inactive_date,
  vendorbrand_code AS vendor_brand_code,
  vendorbrand_name AS vendor_brand_name,
  vendorbrand_brandstatus AS vendor_brand_status,
   (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_LABEL')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_LABEL')) AS dw_batch_date
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.vendor_label_dim_ldg AS src0
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY vendorbrand_vendorlabel_code ORDER BY eventcreatedate DESC, eventsequence DESC
      )) = 1) AS SRC
ON SRC.vendor_label_code = tgt.vendor_label_code
WHEN MATCHED THEN UPDATE SET
 vendor_label_name = SRC.vendor_label_name,
 vendor_label_desc = SRC.vendor_label_desc,
 vendor_label_status = SRC.vendor_label_status,
 label_inactive_date = SRC.label_inactive_date,
 vendor_brand_code = CAST(SRC.vendor_brand_code AS STRING),
 vendor_brand_name = SRC.vendor_brand_name,
 vendor_brand_status = SRC.vendor_brand_status,
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(SRC.vendor_label_code, SRC.vendor_label_name, SRC.vendor_label_desc, SRC.vendor_label_status
 , SRC.label_inactive_date, CAST(SRC.vendor_brand_code AS STRING), SRC.vendor_brand_name, SRC.vendor_brand_status, SRC.dw_batch_id
 , SRC.dw_batch_date, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 );