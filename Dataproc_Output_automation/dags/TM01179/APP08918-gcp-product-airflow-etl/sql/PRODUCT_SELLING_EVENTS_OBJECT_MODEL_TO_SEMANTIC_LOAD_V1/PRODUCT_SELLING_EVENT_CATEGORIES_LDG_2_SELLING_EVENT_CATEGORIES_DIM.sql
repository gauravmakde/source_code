


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_categories_dim_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_categories_dim_wrk (selling_event_num, categories, selling_event_status,
 src_updated_tmstp,src_updated_tmstp_tz)
(SELECT DISTINCT selling_event_num,
  categories,
  selling_event_status,
  src_updated_tmstp,
  src_updated_tmstp_tz
 FROM (SELECT selling_event_num,
    categories,
    selling_event_status,
    src_updated_tmstp,
    src_updated_tmstp_tz
   FROM (SELECT SRC.*
     FROM (SELECT id AS selling_event_num,
        category AS categories,
        status AS selling_event_status,
        CAST(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedat)AS TIMESTAMP) AS src_updated_tmstp,
        `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(`{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedat)) AS src_updated_tmstp_tz
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_selling_event_categories_ldg
       WHERE category IS NOT NULL
       QUALIFY (ROW_NUMBER() OVER (PARTITION BY id, category ORDER BY 
       `{{params.gcp_project_id}}.JWN_UDF.ISO8601_TMSTP`(lastupdatedat) DESC)) = 1) AS SRC
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_categories_dim AS tgt 
      ON LOWER(SRC.selling_event_num) = LOWER(tgt.selling_event_num
         ) AND LOWER(SRC.categories) = LOWER(tgt.categories)
     WHERE tgt.selling_event_num IS NULL
      OR LOWER(SRC.selling_event_status) <> LOWER(tgt.selling_event_status)
      OR tgt.selling_event_status IS NULL AND SRC.selling_event_status IS NOT NULL
      OR SRC.selling_event_status IS NULL AND tgt.selling_event_status IS NOT NULL) AS t3
   WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_categories_dim_wrk
     WHERE selling_event_num = selling_event_num
      AND categories = categories)) AS t7
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY selling_event_num, categories)) = 1);



BEGIN TRANSACTION;
DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_categories_dim AS tgt
WHERE EXISTS (SELECT *
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_categories_dim_wrk AS src
    WHERE LOWER(selling_event_num) = LOWER(tgt.selling_event_num) AND LOWER(categories) = LOWER(tgt.categories));



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_categories_dim 
(
  selling_event_num,
  categories,
  selling_event_status,
  src_updated_tmstp,
  src_updated_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 )
(SELECT DISTINCT selling_event_num,
  categories,
  selling_event_status,
  src_updated_tmstp,
  src_updated_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT 
    selling_event_num,
    categories,
    selling_event_status,
    src_updated_tmstp,
    src_updated_tmstp_tz,
     (SELECT batch_id FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS')) AS dw_batch_id,
     (SELECT curr_batch_date FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS')) AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_categories_dim_wrk) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_categories_dim
   WHERE selling_event_num = t3.selling_event_num
    AND categories = t3.categories)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY selling_event_num, categories)) = 1);

COMMIT TRANSACTION;

