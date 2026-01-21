--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=ldg-dim;AppSubArea=VENDOR_ORDERFROM_POSTALADDRESS_XREF;' UPDATE FOR SESSION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.vendor_orderfrom_postaladdress_xref
WHERE vendor_num IN (SELECT vendornumber
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_orderfrom_postaladdress_xref_ldg);

--COLLECT STATS ON <DBENV>_NAP_STG.VENDOR_ORDERFROM_POSTALADDRESS_XREF_LDG;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.vendor_orderfrom_postaladdress_xref (vendor_num, vendor_postal_resource_id,
 address_type_short_desc, address_type_desc, address_type_code, dw_batch_id, dw_batch_date, dw_sys_load_tmstp,
 dw_sys_updt_tmstp)
(SELECT DISTINCT vendornumber AS vendor_num,
  vendorpostaladdress_resourceid AS vendor_postal_resource_id,
  vendorpostaladdress_postaladdressaddresstype_vendoraddresstypeshortdescription AS address_type_short_desc,
  vendorpostaladdress_postaladdressaddresstype_vendoraddresstypedescription AS address_type_desc,
  vendorpostaladdress_postaladdressaddresstype_vendoraddresstypecode AS address_type_code,
   (SELECT batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_ORDERFROM_POSTALADDRESS_XREF')) AS dw_batch_id,
   (SELECT curr_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('VENDOR_ORDERFROM_POSTALADDRESS_XREF')) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.vendor_orderfrom_postaladdress_xref_ldg AS src
 QUALIFY (DENSE_RANK() OVER (PARTITION BY vendornumber ORDER BY eventcreatedate DESC, eventsequence DESC)) = 1);
--.IF ERRORCODE <> 0 THEN .QUIT 1

--COLLECT STATISTICS ON PRD_NAP_DIM.VENDOR_ORDERFROM_POSTALADDRESS_XREF;