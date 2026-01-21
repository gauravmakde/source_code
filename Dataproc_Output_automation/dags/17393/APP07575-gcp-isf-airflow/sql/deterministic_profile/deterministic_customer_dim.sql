BEGIN TRANSACTION;
CREATE TEMPORARY TABLE IF NOT EXISTS deterministic_customer_dim_to_update{{params.tbl_sfx}}
CLUSTER BY deterministic_profile_id
AS
SELECT deterministic_profile_id,
 customer_source,
 shopper_id,
 ocp_id,
 loyalty_id,
 billing_postal_code,
 us_dma_code,
 us_dma_desc,
 ca_dma_code,
 ca_dma_desc,
 dw_batch_date,
 dw_sys_updt_tmstp,
 dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_dim
LIMIT 0;

COMMIT TRANSACTION;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.deterministic_customer_dim AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.pending_deterministic_customer_dim AS dlt
    WHERE LOWER(dim.deterministic_profile_id) = LOWER(deterministic_profile_id) AND LOWER(change_flag) = LOWER('DEL'));


MERGE INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.deterministic_customer_dim AS tgt
USING (SELECT DISTINCT wrk.deterministic_profile_id, wrk.customer_source, dccd.classifications_partner_pool, dccd.classifications_verification_status, sol.fls_freq_purch_store_id AS fls_loyalty_store_num, sol.freq_purch_store_id AS jwn_loyalty_store_num, sol.rack_freq_purch_store_id AS rack_loyalty_store_num, cast(wrk.profile_event_tmstp as timestamp) as profile_event_tmstp, ctrl.curr_batch_date AS dw_batch_date, ctrl.batch_id AS dw_batch_id
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_wrk AS wrk
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_classification_dim AS dccd ON LOWER(wrk.deterministic_profile_id) = LOWER(dccd.deterministic_profile_id)
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_storeofloyalty_wrk AS sol ON LOWER(sol.deterministic_profile_id) = LOWER(wrk.deterministic_profile_id)
        INNER JOIN (SELECT curr_batch_date, batch_id
            FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_DIM{{params.tbl_sfx}}')) AS ctrl ON TRUE) AS src
ON LOWER(src.deterministic_profile_id) = LOWER(tgt.deterministic_profile_id) AND LOWER(src.customer_source) = LOWER(tgt.customer_source)
WHEN MATCHED THEN UPDATE SET
    classifications_partner_pool = src.classifications_partner_pool,
    classifications_verification_status = src.classifications_verification_status,
    fls_loyalty_store_num = src.fls_loyalty_store_num,
    jwn_loyalty_store_num = src.jwn_loyalty_store_num,
    rack_loyalty_store_num = src.rack_loyalty_store_num,
    profile_event_tmstp = src.profile_event_tmstp,
    dw_batch_date = src.dw_batch_date,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME),
    dw_batch_id = src.dw_batch_id
WHEN NOT MATCHED THEN INSERT (deterministic_profile_id, customer_source, shopper_id, ocp_id, loyalty_id, classifications_partner_pool, classifications_verification_status, billing_postal_code, fls_loyalty_store_num, jwn_loyalty_store_num, rack_loyalty_store_num, us_dma_code) VALUES(src.deterministic_profile_id, src.customer_source, src.classifications_partner_pool, src.classifications_verification_status, 
CAST(src.fls_loyalty_store_num AS STRING), 
CAST(src.jwn_loyalty_store_num AS STRING), 
CAST(src.rack_loyalty_store_num AS STRING), 
CAST(src.profile_event_tmstp AS STRING), 
CAST(FORMAT_DATE('%Y%m%d', src.dw_batch_date) AS INT64), 
CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', current_datetime('PST8PDT')) AS INT64), 
CAST(FORMAT_DATETIME('%Y%m%d%H%M%S', current_datetime('PST8PDT')) AS INT64), src.dw_batch_id);


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DQ_PIPELINE_AUDIT_V1('DETERMINISTIC_CUSTOMER_DIM_SRC_TO_TGT{{params.tbl_sfx}}',  'DETERMINISTIC_CUSTOMER_DIM{{params.tbl_sfx}}',  '{{params.dbenv}}_NAP_BASE_VWS',  'DETERMINISTIC_CUSTOMER_WRK{{params.tbl_sfx}}',  '{{params.dbenv}}_NAP_BASE_VWS',  'DETERMINISTIC_CUSTOMER_DIM{{params.tbl_sfx}}',  'Count_Distinct',  0,  'T-S',  'deterministic_profile_id',  'deterministic_profile_id',  NULL,  NULL,  'Y');

INSERT INTO deterministic_customer_dim_to_update{{params.tbl_sfx}}
(SELECT dim.deterministic_profile_id,
  dim.customer_source,
  program.shopper_id AS src_shopper_id,
  program.ocp_id AS src_ocp_id,
  program.loyalty_id AS src_loyalty_id,
  addr.postal_code AS src_billing_postal_code,
   CASE
   WHEN COALESCE(us_zip_dma.us_dma_code, - 1) <> - 1
   THEN us_zip_dma.us_dma_code
   WHEN COALESCE(ca_zip_dma.ca_dma_code, - 1) = - 1 AND COALESCE(ca_stor_dma.ca_dma_code, - 1) = - 1 AND COALESCE(us_stor_dma
      .us_dma_code, - 1) <> - 1
   THEN us_stor_dma.us_dma_code
   ELSE NULL
   END AS src_us_dma_code,
  dma_lkp_us.dma_desc AS src_us_dma_desc,
   CASE
   WHEN COALESCE(ca_zip_dma.ca_dma_code, - 1) <> - 1
   THEN ca_zip_dma.ca_dma_code
   WHEN COALESCE(us_zip_dma.us_dma_code, - 1) = - 1 AND COALESCE(us_stor_dma.us_dma_code, - 1) = - 1 AND COALESCE(ca_stor_dma
      .ca_dma_code, - 1) <> - 1
   THEN ca_stor_dma.ca_dma_code
   ELSE NULL
   END AS src_ca_dma_code,
   CASE
   WHEN COALESCE(ca_zip_dma.ca_dma_code, - 1) <> - 1
   THEN ca_zip_dma.ca_dma_desc
   WHEN COALESCE(us_zip_dma.us_dma_code, - 1) = - 1 AND COALESCE(us_stor_dma.us_dma_code, - 1) = - 1 AND COALESCE(ca_stor_dma
      .ca_dma_code, - 1) <> - 1
   THEN ca_stor_dma.ca_dma_desc
   ELSE NULL
   END AS src_ca_dma_desc,
  ctrl.curr_batch_date AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME) as dw_sys_updt_tmstp,
  ctrl.batch_id AS dw_batch_id
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_dim AS dim
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_program_index_wrk AS program ON LOWER(dim.deterministic_profile_id) = LOWER(program
    .unique_source_id)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_latest_address_wrk AS addr ON LOWER(dim.deterministic_profile_id) = LOWER(addr.unique_source_id
    )
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_us_zip_dma AS us_zip_dma ON LOWER(us_zip_dma.us_zip_code) = LOWER(addr.postal_code)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_ca_zip_dma AS ca_zip_dma ON LOWER(ca_zip_dma.postal_code) = LOWER(SUBSTR(addr.postal_code
      , 1, 3)) AND LOWER(addr.country_code) = LOWER('CA')
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_store_us_dma AS us_stor_dma ON dim.jwn_loyalty_store_num = us_stor_dma.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_store_ca_dma AS ca_stor_dma ON dim.jwn_loyalty_store_num = ca_stor_dma.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_dma AS dma_lkp_us ON CASE
    WHEN COALESCE(us_zip_dma.us_dma_code, - 1) <> - 1
    THEN us_zip_dma.us_dma_code
    WHEN COALESCE(ca_zip_dma.ca_dma_code, - 1) = - 1 AND COALESCE(ca_stor_dma.ca_dma_code, - 1) = - 1 AND COALESCE(us_stor_dma
       .us_dma_code, - 1) <> - 1
    THEN us_stor_dma.us_dma_code
    ELSE NULL
    END = dma_lkp_us.dma_code
  INNER JOIN (SELECT curr_batch_date,
    batch_id
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('DETERMINISTIC_CUSTOMER_DIM{{params.tbl_sfx}}')) AS ctrl ON TRUE
 WHERE LOWER(COALESCE(dim.shopper_id, '')) <> LOWER(COALESCE(program.shopper_id, ''))
  OR LOWER(COALESCE(dim.ocp_id, '')) <> LOWER(COALESCE(program.ocp_id, ''))
  OR LOWER(COALESCE(dim.loyalty_id, '')) <> LOWER(COALESCE(program.loyalty_id, ''))
  OR LOWER(COALESCE(dim.billing_postal_code, '')) <> LOWER(COALESCE(addr.postal_code, ''))
  OR LOWER(COALESCE(FORMAT('%11d', dim.us_dma_code), '')) <> LOWER(COALESCE(FORMAT('%11d', CASE
       WHEN COALESCE(us_zip_dma.us_dma_code, - 1) <> - 1
       THEN us_zip_dma.us_dma_code
       WHEN COALESCE(ca_zip_dma.ca_dma_code, - 1) = - 1 AND COALESCE(ca_stor_dma.ca_dma_code, - 1) = - 1 AND COALESCE(us_stor_dma
          .us_dma_code, - 1) <> - 1
       THEN us_stor_dma.us_dma_code
       ELSE NULL
       END), ''))
  OR LOWER(COALESCE(dim.us_dma_desc, '')) <> LOWER(COALESCE(dma_lkp_us.dma_desc, ''))
  OR LOWER(COALESCE(FORMAT('%11d', dim.ca_dma_code), '')) <> LOWER(COALESCE(FORMAT('%11d', CASE
       WHEN COALESCE(ca_zip_dma.ca_dma_code, - 1) <> - 1
       THEN ca_zip_dma.ca_dma_code
       WHEN COALESCE(us_zip_dma.us_dma_code, - 1) = - 1 AND COALESCE(us_stor_dma.us_dma_code, - 1) = - 1 AND COALESCE(ca_stor_dma
          .ca_dma_code, - 1) <> - 1
       THEN ca_stor_dma.ca_dma_code
       ELSE NULL
       END), ''))
  OR LOWER(COALESCE(dim.ca_dma_desc, '')) <> LOWER(COALESCE(CASE
      WHEN COALESCE(ca_zip_dma.ca_dma_code, - 1) <> - 1
      THEN ca_zip_dma.ca_dma_desc
      WHEN COALESCE(us_zip_dma.us_dma_code, - 1) = - 1 AND COALESCE(us_stor_dma.us_dma_code, - 1) = - 1 AND COALESCE(ca_stor_dma
         .ca_dma_code, - 1) <> - 1
      THEN ca_stor_dma.ca_dma_desc
      ELSE NULL
      END, '')));
      
BEGIN TRANSACTION;
UPDATE deterministic_customer_dim_to_update AS src SET
    shopper_id = src.shopper_id,
    ocp_id = src.ocp_id,
    loyalty_id = src.loyalty_id,
    billing_postal_code = src.billing_postal_code,
    us_dma_code = src.us_dma_code,
    us_dma_desc = src.us_dma_desc,
    ca_dma_code = src.ca_dma_code,
    ca_dma_desc = src.ca_dma_desc,
    dw_batch_date = src.dw_batch_date,
    dw_sys_updt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', current_datetime('PST8PDT')) AS DATETIME),
    dw_batch_id = src.dw_batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.deterministic_customer_dim
WHERE LOWER(deterministic_customer_dim{{params.tbl_sfx}}.deterministic_profile_id) = LOWER(src.deterministic_profile_id) AND LOWER(deterministic_customer_dim{{params.tbl_sfx}}.customer_source) = LOWER(src.customer_source);

COMMIT TRANSACTION;

