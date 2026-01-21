--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_BRAND;'  UPDATE FOR SESSION;

MERGE INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.vendor_brand_dim AS tgt
USING (SELECT vendorbrand_code AS vendor_brand_code,
  vendorbrand_name AS vendor_brand_name,
  vendorbrand_description AS vendor_brand_desc,
  vendorbrand_brandstatus AS vendor_brand_status,
   CASE
   WHEN IF(REGEXP_CONTAINS(vendorbrand_brandinactivedate , r'^\d\d\d\d-\d\d-\d\d.*'), 1, 0) = 1
   THEN PARSE_DATE('%F', SUBSTR(vendorbrand_brandinactivedate, 1, 10))
   ELSE NULL
   END AS brand_inactive_date,
  vendorbrand_vendorhouse_code AS vendor_house_code,
  vendorbrand_vendorhouse_name AS vendor_house_name,
  vendorbrand_vendorhouse_housestatus AS vendor_house_status_code,
  vendorbrand_vendorhouse_description AS vendor_house_description,
  vendorbrand_resourceid AS brand_resource_id,
   (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_BRAND')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_BRAND')) AS dw_batch_date
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.vendor_brand_dim_ldg AS src0
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY vendorbrand_code ORDER BY eventcreatedate DESC, eventsequence DESC)) = 1) AS
SRC
ON SRC.vendor_brand_code = tgt.vendor_brand_code
WHEN MATCHED THEN UPDATE SET
 vendor_brand_name = SRC.vendor_brand_name,
 vendor_brand_desc = SRC.vendor_brand_desc,
 vendor_brand_status = SRC.vendor_brand_status,
 brand_inactive_date = CAST(SRC.brand_inactive_date AS STRING),
 vendor_house_code = SRC.vendor_house_code,
 vendor_house_name = SRC.vendor_house_name,
 vendor_house_status_code = SRC.vendor_house_status_code,
 vendor_house_description = SRC.vendor_house_description,
 brand_resource_id = SRC.brand_resource_id,
 dw_batch_id = SRC.dw_batch_id,
 dw_batch_date = SRC.dw_batch_date,
 dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(SRC.vendor_brand_code, SRC.vendor_brand_name, SRC.vendor_brand_desc, SRC.vendor_brand_status
 , CAST(SRC.brand_inactive_date AS STRING), SRC.vendor_house_code, SRC.vendor_house_name, SRC.vendor_house_status_code,
 SRC.vendor_house_description, SRC.brand_resource_id, SRC.dw_batch_id, SRC.dw_batch_date, CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 , CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));