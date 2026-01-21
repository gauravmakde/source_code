TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_xref_wrk;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_xref_wrk (selling_event_num,
    channel_country,
    channel_brand,
    selling_channel,
    selling_event_name,
    event_start_tmstp,
    event_start_tmstp_tz,
    event_end_tmstp,
    event_end_tmstp_tz,
    selling_event_status,
    event_last_update_by_id,
    event_last_update_by_id_type) 
(
SELECT
     DISTINCT selling_event_num,
     channel_country,
     channel_brand,
     selling_channel,
     selling_event_name,
     CAST(event_start_tmstp AS TIMESTAMP),
     event_start_tmstp_tz,
     CAST(event_end_tmstp AS TIMESTAMP),
     event_end_tmstp_tz,
     selling_event_status,
     event_last_update_by_id,
     event_last_update_by_id_type
     FROM (
          SELECT SRC.selling_event_num,
               SRC.channel_country,
               SRC.channel_brand,
               SRC.selling_channel,
               SRC.selling_event_name,
               SRC.event_start_tmstp,
               `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(SRC.event_start_tmstp) as event_start_tmstp_tz,
               SRC.event_end_tmstp,
               `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(SRC.event_end_tmstp) as event_end_tmstp_tz,
               SRC.selling_event_status,
               SRC.event_last_update_by_id,
               SRC.event_last_update_by_id_type
               FROM (
                    SELECT id AS selling_event_num,
                         channel_channelcountry AS channel_country,
                         channel_channelbrand AS channel_brand,
                         channel_sellingchannel AS selling_channel,
                         name AS selling_event_name,
                         `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_start) AS event_start_tmstp,
                         `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(event_end) AS event_end_tmstp,
                         status AS selling_event_status,
                         lastupdatedby_id AS event_last_update_by_id,
                         lastupdatedby_idtype AS event_last_update_by_id_type,
                         `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedat) AS src_updated_tmstp
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_selling_event_ldg
                    WHERE id IS NOT NULL
                    AND channel_channelcountry IS NOT NULL
               QUALIFY (ROW_NUMBER() OVER (PARTITION BY id, channel_channelcountry, channel_channelbrand, channel_sellingchannel
                    ORDER BY `{{params.gcp_project_id}}`.JWN_UDF.ISO8601_TMSTP(lastupdatedat) DESC)) = 1
          ) AS SRC
          LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_xref AS tgt 
          ON LOWER(SRC.selling_event_num) = LOWER(tgt.selling_event_num) 
               AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country) 
               AND LOWER(SRC.channel_brand) = LOWER(tgt.channel_brand) 
               AND LOWER(SRC.selling_channel) = LOWER(tgt.selling_channel)
          WHERE tgt.selling_event_num IS NULL
               OR LOWER(SRC.selling_event_status) <> LOWER(tgt.selling_event_status)
               OR tgt.selling_event_status IS NULL AND SRC.selling_event_status IS NOT NULL
               OR SRC.selling_event_status IS NULL AND tgt.selling_event_status IS NOT NULL
               OR LOWER(SRC.selling_event_name) <> LOWER(tgt.selling_event_name)
               OR tgt.selling_event_name IS NULL AND SRC.selling_event_name IS NOT NULL
               OR SRC.selling_event_name IS NULL AND tgt.selling_event_name IS NOT NULL
               OR CAST(SRC.event_start_tmstp AS TIMESTAMP) <> tgt.event_start_tmstp
               OR tgt.event_start_tmstp IS NULL
               OR CAST(SRC.event_end_tmstp AS TIMESTAMP) <> tgt.event_end_tmstp
               OR tgt.event_end_tmstp IS NULL
               OR LOWER(SRC.event_last_update_by_id) <> LOWER(tgt.event_last_update_by_id)
               OR tgt.event_last_update_by_id IS NULL AND SRC.event_last_update_by_id IS NOT NULL
               OR SRC.event_last_update_by_id IS NULL AND tgt.event_last_update_by_id IS NOT NULL
               OR LOWER(SRC.event_last_update_by_id_type) <> LOWER(tgt.event_last_update_by_id_type)
               OR tgt.event_last_update_by_id_type IS NULL AND SRC.event_last_update_by_id_type IS NOT NULL
               OR SRC.event_last_update_by_id_type IS NULL AND tgt.event_last_update_by_id_type IS NOT NULL
     ) AS t3
     WHERE NOT EXISTS (SELECT 1 AS `A12180`
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_xref_wrk
     WHERE selling_event_num = t3.selling_event_num
      AND channel_country = t3.channel_country
      AND channel_brand = t3.channel_brand
      AND selling_channel = t3.selling_channel)
  QUALIFY
    (ROW_NUMBER() OVER (PARTITION BY selling_event_num, channel_country, channel_brand, selling_channel)) = 1);

BEGIN TRANSACTION;
DELETE
FROM
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_xref AS tgt
WHERE
  EXISTS (
  SELECT  *
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_xref_wrk AS src
  WHERE
    LOWER(selling_event_num) = LOWER(tgt.selling_event_num)
    AND LOWER(channel_country) = LOWER(tgt.channel_country)
    AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
    AND LOWER(selling_channel) = LOWER(tgt.selling_channel) )
;

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_xref (selling_event_num,
    channel_country,
    channel_brand,
    selling_channel,
    selling_event_name,
    event_start_tmstp,
    event_start_tmstp_tz,
    event_end_tmstp,
    event_end_tmstp_tz,
    selling_event_status,
    event_last_update_by_id,
    event_last_update_by_id_type,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp) (
  SELECT
    DISTINCT selling_event_num,
    channel_country,
    channel_brand,
    selling_channel,
    selling_event_name,
    event_start_tmstp,
    event_start_tmstp_tz,
    event_end_tmstp,
    event_end_tmstp_tz,
    selling_event_status,
    event_last_update_by_id,
    event_last_update_by_id_type,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp
  FROM (
    SELECT
      selling_event_num,
      channel_country,
      channel_brand,
      selling_channel,
      selling_event_name,
      event_start_tmstp,
      event_start_tmstp_tz,
      event_end_tmstp,
      event_end_tmstp_tz,
      selling_event_status,
      event_last_update_by_id,
      event_last_update_by_id_type,
      (
        SELECT
          batch_id
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE
          LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS') 
      ) AS dw_batch_id,
      (
        SELECT
          curr_batch_date
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE
          LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS') 
      ) AS dw_batch_date,
      CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_xref_wrk
  ) AS t3
  WHERE
    NOT EXISTS (
    SELECT  1 AS `A12180`
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_xref
    WHERE
      selling_event_num = t3.selling_event_num
      AND channel_country = t3.channel_country
      AND channel_brand = t3.channel_brand
      AND selling_channel = t3.selling_channel
  )
  QUALIFY
    (ROW_NUMBER() OVER (PARTITION BY selling_event_num, channel_country, channel_brand, selling_channel)) = 1);
    
COMMIT TRANSACTION;