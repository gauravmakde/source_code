
MERGE INTO {{params.bq_project_id}}.{{params.dbenv}}_nap_dim.item_supplier_size_dim AS tgt
USING (SELECT ldg.npin_num, NULLIF(ldg.rms_sku_id, '') AS rms_sku_id, ldg.supplier_size_1, ldg.supplier_size_1_desc, NULLIF(ldg.supplier_size_2, '') AS supplier_size_2, NULLIF(ldg.supplier_size_2_desc, '') AS supplier_size_2_desc, CAST(TRUNC(cast(ldg.header_version as float64)) AS INTEGER) AS header_version,
 cast(ldg.last_updated_tmstp || '+00:00' AS Timestamp)AS last_updated_tmstp,
 '+00:00' AS last_updated_tmstp_tz, 
 cast(ldg.created_tmstp || '+00:00' AS Timestamp) AS created_tmstp,
 '+00:00' AS created_tmstp_tz,
  elt.batch_id, elt.curr_batch_date
    FROM {{params.bq_project_id}}.{{params.dbenv}}_nap_stg.item_supplier_size_ldg AS ldg
        INNER JOIN {{params.bq_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control AS elt ON LOWER(elt.subject_area_nm) = LOWER('ITEM_SUPPLIER_SIZE_DATA_SERVICE')
        LEFT JOIN {{params.bq_project_id}}.{{params.dbenv}}_nap_base_vws.item_supplier_size_dim AS dim ON LOWER(ldg.npin_num) = LOWER(dim.npin_num) AND LOWER(COALESCE(ldg.rms_sku_id, '')) = LOWER(COALESCE(dim.rms_sku_id, ''))
    WHERE CAST(ldg.header_version AS FLOAT64) > dim.header_version OR dim.npin_num IS NULL
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY ldg.npin_num, ldg.rms_sku_id ORDER BY CAST(TRUNC(cast(ldg.header_version as float64)) AS INTEGER) DESC, ldg.last_updated_tmstp DESC)) = 1) AS src
ON LOWER(src.npin_num) = LOWER(tgt.npin_num) AND LOWER(COALESCE(src.rms_sku_id, '')) = LOWER(COALESCE(tgt.rms_sku_id, ''))
WHEN MATCHED THEN UPDATE SET
    supplier_size_1 = src.supplier_size_1,
    supplier_size_1_desc = src.supplier_size_1_desc,
    supplier_size_2 = src.supplier_size_2,
    supplier_size_2_desc = src.supplier_size_2_desc,
    header_version = src.header_version,
    last_updated_tmstp = src.last_updated_tmstp,
    last_updated_tmstp_tz = src.last_updated_tmstp_tz,
    created_tmstp = src.created_tmstp,
		created_tmstp_tz = src.created_tmstp_tz,
    dw_batch_id = src.batch_id,
    dw_batch_date = src.curr_batch_date,
    dw_sys_updt_tmstp = DEFAULT
WHEN NOT MATCHED THEN 
INSERT (npin_num, rms_sku_id, supplier_size_1, supplier_size_1_desc, supplier_size_2, supplier_size_2_desc, last_updated_tmstp,last_updated_tmstp_tz, created_tmstp, created_tmstp_tz, header_version, dw_batch_date, dw_batch_id)
 VALUES(src.npin_num, src.rms_sku_id, src.supplier_size_1, src.supplier_size_1_desc, src.supplier_size_2, src.supplier_size_2_desc, src.last_updated_tmstp, src.last_updated_tmstp_tz, src.created_tmstp, src.created_tmstp_tz, src.header_version, src.curr_batch_date, src.batch_id);

