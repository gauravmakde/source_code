
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_selling_event_tags_dim_wrk;

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_selling_event_tags_dim_wrk (selling_event_num, selling_event_association_id, tag_name,
 tag_type, src_updated_tmstp,src_updated_tmstp_tz)
(SELECT DISTINCT selling_event_num,
  selling_event_association_id,
  tag_name,
  tag_type,
  src_updated_tmstp,
  src_updated_tmstp_tz
 FROM (SELECT t3.selling_event_num,
    selling_event_association_id,
    tag_name,
    tag_type,
    cast(src_updated_tmstp as timestamp) AS src_updated_tmstp,
    src_updated_tmstp_tz
   FROM (SELECT 
        src.selling_event_num,
        src.selling_event_association_id,
        src.tag_name,
        src.tag_type,
        src.src_updated_tmstp,
        src.src_updated_tmstp_tz
     FROM (SELECT id AS selling_event_num,
        association_id AS selling_event_association_id,
        tag_name,
        tag_type,
        `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat) AS src_updated_tmstp,
        `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(`{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat)) AS src_updated_tmstp_tz
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_selling_event_tags_ldg
       WHERE tag_name IS NOT NULL
       QUALIFY (ROW_NUMBER() OVER (PARTITION BY id, association_id, tag_name ORDER BY `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat
             ) DESC)) = 1) AS SRC
      LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_selling_event_tags_dim AS tgt ON LOWER(SRC.selling_event_num) = LOWER(tgt.selling_event_num
          ) AND LOWER(SRC.tag_name) = LOWER(tgt.tag_name) AND LOWER(SRC.selling_event_association_id) = LOWER(tgt.selling_event_association_id
         )
     WHERE tgt.selling_event_num IS NULL
      OR LOWER(SRC.tag_type) <> LOWER(tgt.tag_type)
      OR tgt.tag_type IS NULL AND SRC.tag_type IS NOT NULL
      OR SRC.tag_type IS NULL AND tgt.tag_type IS NOT NULL) AS t3
   WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_selling_event_tags_dim_wrk
     WHERE selling_event_num = selling_event_num
      AND selling_event_association_id = selling_event_association_id
      AND tag_name = tag_name)) AS t7
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY selling_event_num, selling_event_association_id, tag_name)) = 1);
BEGIN
BEGIN TRANSACTION;

--.IF ERRORCODE <> 0 THEN .QUIT 3
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_selling_event_tags_dim AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_selling_event_tags_dim_wrk AS src
    WHERE LOWER(selling_event_num) = LOWER(tgt.selling_event_num) AND LOWER(tag_name) = LOWER(tgt.tag_name) AND LOWER(selling_event_association_id) = LOWER(tgt.selling_event_association_id));







--.IF ERRORCODE <> 0 THEN .QUIT 4
INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_selling_event_tags_dim (selling_event_num, selling_event_association_id, tag_name,
 tag_type, src_updated_tmstp,src_updated_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT selling_event_num,
  selling_event_association_id,
  tag_name,
  tag_type,
  src_updated_tmstp,
  src_updated_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
  
 FROM (SELECT selling_event_num,
    selling_event_association_id,
    tag_name,
    tag_type,
    src_updated_tmstp,
     src_updated_tmstp_tz,
     (SELECT batch_id
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS')) AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
  
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_selling_event_tags_dim_wrk) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_selling_event_tags_dim
   WHERE selling_event_num = t3.selling_event_num
    AND selling_event_association_id = t3.selling_event_association_id
    AND tag_name = t3.tag_name)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY selling_event_num, selling_event_association_id, tag_name)) = 1);

COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
END; 
