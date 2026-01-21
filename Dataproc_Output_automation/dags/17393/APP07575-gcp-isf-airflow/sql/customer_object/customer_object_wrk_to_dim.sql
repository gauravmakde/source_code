DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_program_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_program_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));

--
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_program_dim{{params.tbl_sfx}}
SELECT DISTINCT customer_obj_program_wrk.unique_source_id,
 customer_obj_program_wrk.customer_source,
 customer_obj_program_wrk.program_index,
 customer_obj_program_wrk.program_index_id,
 customer_obj_program_wrk.program_index_name,
 cast(customer_obj_program_wrk.object_event_tmstp as timestamp),
 JWN_UDF.UDF_TIME_ZONE(JWN_UDF.ISO8601_TMSTP(cast(customer_obj_program_wrk.object_event_tmstp as string))) as object_event_tmstp_tz,
  ctrl.curr_batch_date AS dw_batch_date,
 COALESCE(customer_obj_program_wrk.dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)
  ) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_updt_tmstp,
 ctrl.batch_id AS dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_program_wrk{{params.tbl_sfx}}
 INNER JOIN (SELECT curr_batch_date,
   batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')) as ctrl
   ON TRUE
WHERE LOWER(customer_obj_program_wrk.event_type) <> LOWER('DELETE')
 AND LOWER(COALESCE(customer_obj_program_wrk.program_index, 'null')) <> LOWER('null');




-------------------------------------------------*/

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_email_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_email_lvl1_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));


-------------------------------------------------

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_email_dim{{params.tbl_sfx}}

SELECT customer_obj_email_lvl2_wrk.unique_source_id,
 SUBSTR(customer_obj_email_lvl2_wrk.unique_source_id, 1, STRPOS(LOWER(customer_obj_email_lvl2_wrk.unique_source_id),
    LOWER('::')) - 1) AS customer_source,
 customer_obj_email_lvl2_wrk.email_address_token_value,
 customer_obj_email_lvl2_wrk.email_address_token_authority,
 customer_obj_email_lvl2_wrk.email_address_token_strategy,
 customer_obj_email_lvl2_wrk.alt_email_ind,
 customer_obj_email_lvl2_wrk.std_email_ind,
 cast(customer_obj_email_lvl2_wrk.object_event_tmstp as timestamp),
 JWN_UDF.UDF_TIME_ZONE(JWN_UDF.ISO8601_TMSTP(cast(customer_obj_email_lvl2_wrk.object_event_tmstp as string))) as object_event_tmstp_tz,
 ctrl.curr_batch_date AS dw_batch_date,
 COALESCE(customer_obj_email_lvl2_wrk.dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)
  ) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_updt_tmstp,
 ctrl.batch_id AS dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_email_lvl2_wrk{{params.tbl_sfx}}
 INNER JOIN (SELECT curr_batch_date,
   batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')) AS ctrl ON TRUE;


-------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_telephone_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_telephone_lvl1_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));


-------------------------------------------------


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_telephone_dim{{params.tbl_sfx}}

SELECT customer_obj_telephone_lvl2_wrk.unique_source_id,
 SUBSTR(customer_obj_telephone_lvl2_wrk.unique_source_id, 1, STRPOS(LOWER(customer_obj_telephone_lvl2_wrk.unique_source_id
     ), LOWER('::')) - 1) AS customer_source,
 customer_obj_telephone_lvl2_wrk.telephone_number_token_value,
 customer_obj_telephone_lvl2_wrk.telephone_number_token_authority,
 customer_obj_telephone_lvl2_wrk.telephone_number_token_strategy,
 customer_obj_telephone_lvl2_wrk.alt_telephone_ind,
 customer_obj_telephone_lvl2_wrk.std_telephone_ind,
 cast(customer_obj_telephone_lvl2_wrk.object_event_tmstp as timestamp),
 JWN_UDF.UDF_TIME_ZONE(JWN_UDF.ISO8601_TMSTP(cast(customer_obj_telephone_lvl2_wrk.object_event_tmstp as string))) as object_event_tmstp_tz,
 ctrl.curr_batch_date AS dw_batch_date,
 COALESCE(customer_obj_telephone_lvl2_wrk.dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)
  ) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_updt_tmstp,
 ctrl.batch_id AS dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_telephone_lvl2_wrk{{params.tbl_sfx}}
 INNER JOIN (SELECT curr_batch_date,
   batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')) AS ctrl ON TRUE;


-------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_postal_address_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_postal_address_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));

-------------------------------------------------
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_postal_address_dim{{params.tbl_sfx}}
SELECT DISTINCT
    unique_source_id,
    customer_source,
    address_id,
    first_name_token_value,
    first_name_token_authority,
    first_name_token_strategy,
    middle_name_token_value,
    middle_name_token_authority,
    middle_name_token_strategy,
    last_name_token_value,
    last_name_token_authority,
    last_name_token_strategy,
    std_line_1_token_value,
    std_line_1_token_authority,
    std_line_1_token_strategy,
    std_line_2_token_value,
    std_line_2_token_authority,
    std_line_2_token_strategy,
    std_city_token_value,
    std_city_token_authority,
    std_city_token_strategy,
    state_token_value,
    state_token_authority,
    state_token_strategy,
    country_token_value,
    country_token_authority,
    country_token_strategy,
    std_postal_code_token_value,
    std_postal_code_token_authority,
    std_postal_code_token_strategy,
    address_type,
    cast(source_audit_create_tmstp as timestamp),
    JWN_UDF.UDF_TIME_ZONE(cast(cast(source_audit_create_tmstp as timestamp) as string)),
      cast(source_audit_update_tmstp as timestamp),
    JWN_UDF.UDF_TIME_ZONE(cast(cast(source_audit_update_tmstp as timestamp) as string)),
    CAST(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', cast(line_1_token_value as timestamp)) AS STRING),
    cast(line_1_token_authority as string),
    cast(line_1_token_strategy as string),
    cast(line_2_token_value as string),
    cast(line_2_token_authority as string),
    cast(line_2_token_strategy as string),
    cast(city_token_value as string),
    cast(city_token_authority as string),
    cast(city_token_strategy as string),
    cast(postal_code_token_value as string),
    cast(postal_code_token_authority as string),
    cast(postal_code_token_strategy as string),
    cast(attributed_by as string),
    cast(object_event_tmstp as timestamp),
    JWN_UDF.UDF_TIME_ZONE(cast(object_event_tmstp as string)) as object_event_tmstp_tz,
    ctrl.curr_batch_date AS dw_batch_date,
    cast(COALESCE(dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)) AS TIMESTAMP) AS dw_sys_load_tmstp,
     JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_load_tmstp,
    cast(COALESCE(dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)) AS TIMESTAMP) AS dw_sys_updt_tmstp,
     JWN_UDF.DEFAULT_TZ_PST() AS dw_sys_updt_tmstp,
    ctrl.batch_id AS dw_batch_id,
     is_verified_ind
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_OBJ_POSTAL_ADDRESS_WRK{{params.tbl_sfx}}

CROSS JOIN
    (SELECT curr_batch_date, batch_id 
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL 
     WHERE LOWER(subject_area_nm) = 'customer_obj{{params.tbl_sfx}}') AS ctrl
WHERE event_type <> 'DELETE'
    AND COALESCE(address_id, 'null') <> 'null';


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_merge_alias_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_merge_alias_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));





INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_merge_alias_dim{{params.tbl_sfx}}

SELECT DISTINCT customer_obj_merge_alias_wrk.unique_source_id,
 customer_obj_merge_alias_wrk.customer_id,
 customer_obj_merge_alias_wrk.alias_id,
 cast(customer_obj_merge_alias_wrk.object_event_tmstp as timestamp),
 customer_obj_merge_alias_wrk.object_event_tmstp_tz,
 ctrl.curr_batch_date AS dw_batch_date,
 COALESCE(customer_obj_merge_alias_wrk.dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)
  ) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_updt_tmstp,
 ctrl.batch_id AS dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_merge_alias_wrk{{params.tbl_sfx}}
 INNER JOIN (SELECT curr_batch_date,
   batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')) AS ctrl ON TRUE
WHERE LOWER(customer_obj_merge_alias_wrk.event_type) <> LOWER('DELETE')
 AND LOWER(COALESCE(customer_obj_merge_alias_wrk.alias_id, 'null')) <> LOWER('null');



-------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_payment_method_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_payment_method_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_payment_method_dim{{params.tbl_sfx}}

SELECT DISTINCT
    unique_source_id,
    customer_source,
    payment_method_id,
    payment_method_number_token_value,
    payment_method_number_token_authority,
    payment_method_number_token_strategy,
    payment_method_type,
    card_type,
    card_sub_type,
    card_holder_type,
    product_type,
    address_id,
    credit_account_id,
    credit_account_status,
    default_payment_method_ind,
    attributed_by,
    created_by,
    cast(source_audit_create_tmstp as timestamp),
    source_audit_create_tmstp_tz,
    cast(source_audit_update_tmstp as timestamp),
    source_audit_update_tmstp_tz,
    cast(object_event_tmstp as timestamp),
    object_event_tmstp_tz,
    ctrl.curr_batch_date AS dw_batch_date,
    COALESCE(dw_sys_load_tmstp,CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)) AS dw_sys_load_tmstp,
    CURRENT_DATETIME('PST8PDT') AS dw_sys_updt_tmstp,
    ctrl.batch_id AS dw_batch_id,
    email_token_value,
    email_token_authority,
    email_token_strategy,
    caid,
    card_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.CUSTOMER_OBJ_PAYMENT_METHOD_WRK{{params.tbl_sfx}}

CROSS JOIN
    (SELECT curr_batch_date, batch_id 
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL 
     WHERE LOWER(subject_area_nm) = 'customer_obj{{params.tbl_sfx}}') AS ctrl
WHERE event_type <> 'DELETE'
    AND COALESCE(payment_method_id, 'null') <> 'null';




DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_master_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_master_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));



-------------------------------------------------


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_master_dim{{params.tbl_sfx}}

SELECT DISTINCT customer_obj_master_wrk.unique_source_id,
 customer_obj_master_wrk.customer_source,
 customer_obj_master_wrk.first_name_token_value,
 customer_obj_master_wrk.first_name_token_authority,
 customer_obj_master_wrk.first_name_token_strategy,
 customer_obj_master_wrk.last_name_token_value,
 customer_obj_master_wrk.last_name_token_authority,
 customer_obj_master_wrk.last_name_token_strategy,
 customer_obj_master_wrk.middle_name_token_value,
 customer_obj_master_wrk.middle_name_token_authority,
 customer_obj_master_wrk.middle_name_token_strategy,
 customer_obj_master_wrk.alternative_name_token_value,
 customer_obj_master_wrk.alternative_name_token_authority,
 customer_obj_master_wrk.alternative_name_token_strategy,
 customer_obj_master_wrk.event_type,
 customer_obj_master_wrk.event_source_system,
 CAST(customer_obj_master_wrk.object_event_tmstp AS TIMESTAMP),
  customer_obj_master_wrk.object_event_tmstp_tz,
 ctrl.curr_batch_date AS dw_batch_date,
 COALESCE(customer_obj_master_wrk.dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)
  ) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_updt_tmstp,
 ctrl.batch_id AS dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_master_wrk{{params.tbl_sfx}}
 INNER JOIN (SELECT curr_batch_date,
   batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')) AS ctrl ON TRUE
WHERE LOWER(customer_obj_master_wrk.event_type) <> LOWER('DELETE');


-------------------------------------------------

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_credit_account_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_credit_account_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));

-------------------------------------------------

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_credit_account_dim{{params.tbl_sfx}}

SELECT DISTINCT customer_obj_credit_account_wrk.unique_source_id,
 customer_obj_credit_account_wrk.customer_source,
 customer_obj_credit_account_wrk.caid,
 cast(customer_obj_credit_account_wrk.open_date as timestamp),
  customer_obj_credit_account_wrk.open_date_tz,
 cast(customer_obj_credit_account_wrk.closed_date as timestamp),
 customer_obj_credit_account_wrk.closed_date_tz,
 customer_obj_credit_account_wrk.status,
 cast(customer_obj_credit_account_wrk.object_event_tmstp as timestamp),
  customer_obj_credit_account_wrk.object_event_tmstp_tz,
 ctrl.curr_batch_date AS dw_batch_date,
 COALESCE(customer_obj_credit_account_wrk.dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)
  ) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_updt_tmstp,
 ctrl.batch_id AS dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_credit_account_wrk{{params.tbl_sfx}}
 INNER JOIN (SELECT curr_batch_date,
   batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')) AS ctrl ON TRUE
WHERE LOWER(customer_obj_credit_account_wrk.event_type) <> LOWER('DELETE')
 AND LOWER(COALESCE(customer_obj_credit_account_wrk.caid, 'null')) <> LOWER('null');



-------------------------------------------------

-------------------------------------------------*/
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_linked_program_dim{{params.tbl_sfx}} AS dim
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_linked_program_wrk{{params.tbl_sfx}} AS dlt
    WHERE LOWER(dim.unique_source_id) = LOWER(unique_source_id));

-------------------------------------------------


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.customer_obj_linked_program_dim{{params.tbl_sfx}}

SELECT DISTINCT customer_obj_linked_program_wrk.unique_source_id,
 customer_obj_linked_program_wrk.customer_source,
 customer_obj_linked_program_wrk.link_id,
 customer_obj_linked_program_wrk.link_type,
 customer_obj_linked_program_wrk.link_type_id,
 customer_obj_linked_program_wrk.link_data_mobile_token_value,
 customer_obj_linked_program_wrk.link_data_mobile_token_authority,
 customer_obj_linked_program_wrk.link_data_mobile_token_strategy,
 cast(customer_obj_linked_program_wrk.object_event_tmstp as timestamp),
  customer_obj_linked_program_wrk.object_event_tmstp_tz,
 ctrl.curr_batch_date AS dw_batch_date,
 COALESCE(customer_obj_linked_program_wrk.dw_sys_load_tmstp, CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME)
  ) AS dw_sys_load_tmstp,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_updt_tmstp,
 ctrl.batch_id AS dw_batch_id
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.customer_obj_linked_program_wrk{{params.tbl_sfx}}
 INNER JOIN (SELECT curr_batch_date,
   batch_id
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('CUSTOMER_OBJ{{params.tbl_sfx}}')) AS ctrl ON TRUE
WHERE LOWER(customer_obj_linked_program_wrk.event_type) <> LOWER('DELETE')
 AND LOWER(COALESCE(customer_obj_linked_program_wrk.link_id, 'null')) <> LOWER('null');-------------------------------------------------


