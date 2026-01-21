/* SET QUERY_BAND = '
App_ID={app_id};
DAG_ID=isf_deterministic_customer_postal_address_aux_dim_17393_customer_das_customer;
Task_Name=customer_postal_address_aux_dim;'
FOR SESSION VOLATILE;*/

--ET;

-----------------------------------------------------------------------------
-------------------- DIM.CUSTOMER_OBJ_POSTAL_ADDRESS_AUX_DIM ----------------
-----------------------------------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_postal_address_aux_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_aux_wrk{{params.tbl_sfx}} AS wrk
 WHERE LOWER(unique_source_id) = LOWER(dim.unique_source_id));


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_postal_address_aux_dim{{params.tbl_sfx}} --{tbl_sfx}

(
  unique_source_id
  , customer_source
  , address_id
  , country_code
  , postal_code
  , address_types
  , object_event_tmstp
  , object_event_tmstp_tz
  , source_audit_update_tmstp
  , source_audit_update_tmstp_tz
  , dw_batch_date
  , dw_sys_load_tmstp
  , dw_sys_updt_tmstp
  , dw_batch_id
)
SELECT customer_obj_postal_address_aux_wrk.unique_source_id,
 customer_obj_postal_address_aux_wrk.customer_source,
 customer_obj_postal_address_aux_wrk.address_id,
 customer_obj_postal_address_aux_wrk.country_code,
 customer_obj_postal_address_aux_wrk.postal_code,
 customer_obj_postal_address_aux_wrk.address_types,
 customer_obj_postal_address_aux_wrk.object_event_tmstp_utc,
 customer_obj_postal_address_aux_wrk.object_event_tmstp_tz,
 customer_obj_postal_address_aux_wrk.source_audit_update_tmstp_utc,
 customer_obj_postal_address_aux_wrk.source_audit_update_tmstp_tz,
 ctrl.curr_batch_date AS dw_batch_date,
 COALESCE(customer_obj_postal_address_aux_wrk.dw_sys_load_tmstp, current_datetime('PST8PDT')
  ) AS dw_sys_load_tmstp,
 current_datetime('PST8PDT') AS dw_sys_updt_tmstp,
 ctrl.batch_id AS dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_aux_wrk{{params.tbl_sfx}}
 INNER JOIN (SELECT curr_batch_date,
   batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ_POSTAL_ADDRESS_AUX_DIM{{params.tbl_sfx}}')) AS ctrl ON TRUE;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.pending_icon_cust_addr_ldg_dim{{params.tbl_sfx}};