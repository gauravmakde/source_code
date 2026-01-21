--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_BRAND_XREF;'  UPDATE FOR SESSION;


MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.vendor_brand_xref AS tgt
USING (SELECT vendorbrandorderfromassociation_vendorbrand_code AS vendor_brand_code,
  vendorbrandorderfromassociation_vendorbrand_name AS vendor_brand_name,
  vendorbrandorderfromassociation_vendornumber AS vendor_num,
  vendorbrandorderfromassociation_associationstatus AS association_status,
   (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_BRAND_XREF')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_BRAND_XREF')) AS dw_batch_date
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.vendor_brand_xref_ldg AS src0
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY vendorbrandorderfromassociation_vendornumber,
      vendorbrandorderfromassociation_vendorbrand_code ORDER BY eventcreatedate DESC, eventsequence DESC)) = 1) AS SRC
ON CAST(SRC.vendor_brand_code AS FLOAT64) = tgt.vendor_brand_code AND LOWER(SRC.vendor_num) = LOWER(tgt.vendor_num)
WHEN MATCHED THEN UPDATE SET
 vendor_brand_name = SRC.vendor_brand_name,
 association_status = SRC.association_status,
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(CAST(trunc(cast(SRC.vendor_brand_code AS FLOAT64)) AS INTEGER), SRC.vendor_brand_name, SRC.vendor_num, SRC.association_status
 , SRC.dw_batch_id, SRC.dw_batch_date, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 );