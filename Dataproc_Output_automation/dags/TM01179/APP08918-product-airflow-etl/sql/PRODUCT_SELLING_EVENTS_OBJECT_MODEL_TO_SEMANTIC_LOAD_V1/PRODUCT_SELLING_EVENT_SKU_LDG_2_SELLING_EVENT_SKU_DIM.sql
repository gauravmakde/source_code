TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_sku_dim_wrk;

INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_sku_dim_wrk (selling_event_num,
    sku_num,
    sku_type,
    channel_country,
    channel_brand,
    selling_channel,
    selling_event_name,
    selling_event_association_id,
    event_start_tmstp,
    event_start_tmstp_tz,
    event_end_tmstp,
    event_end_tmstp_tz,
    item_start_tmstp,
    item_start_tmstp_tz,
    item_end_tmstp,
    item_end_tmstp_tz,
    selling_event_status,
    item_status,
    event_last_update_by_id,
    event_last_update_by_id_type) (
  SELECT
    SRC.selling_event_num,
    SRC.sku_num,
    SRC.sku_type,
    SRC.channel_country,
    SRC.channel_brand,
    SRC.selling_channel,
    SRC.selling_event_name,
    SRC.selling_event_association_id,
    SRC.event_start_tmstp,
    CAST(SRC.event_start_tmstp_tz AS STRING) AS event_start_tmstp_tz,
    SRC.event_end_tmstp,
    CAST(SRC.event_end_tmstp_tz AS STRING) AS event_end_tmstp_tz,
    CAST(SRC.item_start_tmstp AS TIMESTAMP) AS item_start_tmstp,
    CAST(SRC.item_start_tmstp AS STRING) AS item_start_tmstp_tz,
    CAST(SRC.item_end_tmstp AS TIMESTAMP) AS item_end_tmstp,
    CAST(SRC.item_end_tmstp AS STRING) AS item_end_tmstp_tz,
    SRC.selling_event_status,
    SRC.item_status,
    SRC.event_last_update_by_id,
    SRC.event_last_update_by_id_type
  FROM (
    SELECT
      SKU.selling_event_num,
      SKU.sku_num,
      SKU.sku_type,
      event.channel_country,
      event.channel_brand,
      event.selling_channel,
      event.selling_event_name,
      SKU.selling_event_association_id,
      event.event_start_tmstp,
      event.event_end_tmstp,
      SKU.item_start_tmstp,
      SKU.item_end_tmstp,
      cast(event.event_start_tmstp_tz as string) AS event_start_tmstp_tz,
      CAST(event.event_end_tmstp_tz AS STRING) AS event_end_tmstp_tz,
      cast(`{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(SKU.item_start_tmstp) as timestamp) as item_start_tmstp_tz,
      cast(`{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(SKU.item_end_tmstp) as timestamp) as item_end_tmstp_tz,
      event.selling_event_status,
      SKU.item_status,
      event.event_last_update_by_id,
      event.event_last_update_by_id_type
    FROM (
      SELECT
        selling_event_num,
        sku_num,
        sku_type,
        selling_event_association_id,
        item_start_tmstp,
        item_end_tmstp,
        item_status
      FROM (
        SELECT
          id_sellingeventid AS selling_event_num,
          id_skuid AS sku_num,
          id_skuidtype AS sku_type,
          id_associationid AS selling_event_association_id,
          `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(item_start) AS item_start_tmstp,
          `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(item_end) AS item_end_tmstp,
          status AS item_status,
          `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat) AS src_updated_tmstp
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_sku_ldg
        UNION ALL
        SELECT
          selling_event_num,
          sku_num,
          sku_type,
          selling_event_association_id,
          CAST(item_start_tmstp AS STRING) AS item_start_tmstp,
          CAST(item_end_tmstp AS STRING) AS item_end_tmstp,
          item_status,
          CAST(src_updated_tmstp AS STRING) AS src_updated_tmstp
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_missing_events_wrk
      ) AS SKU_LDG
      WHERE
        sku_num IS NOT NULL
      QUALIFY
        (ROW_NUMBER() OVER (PARTITION BY selling_event_num, sku_num, sku_type, selling_event_association_id ORDER BY src_updated_tmstp DESC)) = 1
    ) AS SKU
    LEFT JOIN
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_xref AS event
    ON
      LOWER(SKU.selling_event_num) = LOWER(event.selling_event_num )
    WHERE
      event.selling_event_num IS NOT NULL
  ) AS SRC
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_sku_dim AS tgt
  ON
    LOWER(SRC.selling_event_num) = LOWER(tgt.selling_event_num )
    AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country)
    AND LOWER(SRC.channel_brand) = LOWER(tgt.channel_brand )
    AND LOWER(SRC.selling_channel) = LOWER(tgt.selling_channel)
    AND LOWER(SRC.sku_num) = LOWER(tgt.sku_num)
    AND LOWER(SRC.selling_event_association_id) = LOWER(tgt.selling_event_association_id)
  WHERE
    tgt.selling_event_num IS NULL
    OR LOWER(SRC.selling_event_status) <> LOWER(tgt.selling_event_status)
    OR tgt.selling_event_status IS NULL
    AND SRC.selling_event_status IS NOT NULL
    OR SRC.selling_event_status IS NULL
    AND tgt.selling_event_status IS NOT NULL
    OR LOWER(SRC.item_status) <> LOWER(tgt.item_status)
    OR tgt.item_status IS NULL
    AND SRC.item_status IS NOT NULL
    OR SRC.item_status IS NULL
    AND tgt.item_status IS NOT NULL
    OR LOWER(SRC.selling_event_name) <> LOWER(tgt.selling_event_name)
    OR tgt.selling_event_name IS NULL
    AND SRC.selling_event_name IS NOT NULL
    OR SRC.selling_event_name IS NULL
    AND tgt.selling_event_name IS NOT NULL
    OR SRC.event_start_tmstp <> tgt.event_start_tmstp
    OR tgt.event_start_tmstp IS NULL
    AND SRC.event_start_tmstp IS NOT NULL
    OR SRC.event_start_tmstp IS NULL
    AND tgt.event_start_tmstp IS NOT NULL
    OR SRC.event_end_tmstp <> tgt.event_end_tmstp
    OR tgt.event_end_tmstp IS NULL
    AND SRC.event_end_tmstp IS NOT NULL
    OR SRC.event_end_tmstp IS NULL
    AND tgt.event_end_tmstp IS NOT NULL
    OR CAST(SRC.item_start_tmstp AS TIMESTAMP) <> tgt.item_start_tmstp
    OR tgt.item_start_tmstp IS NULL
    OR CAST(SRC.item_end_tmstp AS TIMESTAMP) <> tgt.item_end_tmstp
    OR tgt.item_end_tmstp IS NULL
    OR LOWER(SRC.event_last_update_by_id) <> LOWER(tgt.event_last_update_by_id)
    OR tgt.event_last_update_by_id IS NULL
    AND SRC.event_last_update_by_id IS NOT NULL
    OR SRC.event_last_update_by_id IS NULL
    AND tgt.event_last_update_by_id IS NOT NULL
    OR LOWER(SRC.event_last_update_by_id_type) <> LOWER(tgt.event_last_update_by_id_type)
    OR tgt.event_last_update_by_id_type IS NULL
    AND SRC.event_last_update_by_id_type IS NOT NULL
    OR SRC.event_last_update_by_id_type IS NULL
    AND tgt.event_last_update_by_id_type IS NOT NULL);
	
DELETE
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_sku_dim AS tgt
  WHERE
    EXISTS (
    SELECT  *
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_sku_dim_wrk AS src
    WHERE
      LOWER(selling_event_num) = LOWER(tgt.selling_event_num)
      AND LOWER(channel_country) = LOWER(tgt.channel_country)
      AND LOWER(channel_brand) = LOWER(tgt.channel_brand)
      AND LOWER(selling_channel) = LOWER(tgt.selling_channel)
      AND LOWER(sku_num) = LOWER(tgt.sku_num)
      AND LOWER(selling_event_association_id) = LOWER(tgt.selling_event_association_id));


INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_sku_dim (selling_event_num,
    sku_num,
    sku_type,
    channel_country,
    channel_brand,
    selling_channel,
    selling_event_name,
    selling_event_association_id,
    event_start_tmstp,
    event_end_tmstp,
    item_start_tmstp,
    item_end_tmstp,
    event_start_tmstp_tz,
    event_end_tmstp_tz,
    item_start_tmstp_tz,
    item_end_tmstp_tz,
    selling_event_status,
    item_status,
    event_last_update_by_id,
    event_last_update_by_id_type,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp) (
  SELECT
    DISTINCT selling_event_num,
    sku_num,
    sku_type,
    channel_country,
    channel_brand,
    selling_channel,
    selling_event_name,
    selling_event_association_id,
    event_start_tmstp,
    event_end_tmstp,
    item_start_tmstp,
    item_end_tmstp,
    event_start_tmstp_tz,
    event_end_tmstp_tz,    	
    item_start_tmstp_tz,
    item_end_tmstp_tz,
    selling_event_status,
    item_status,
    event_last_update_by_id,
    event_last_update_by_id_type,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp
  FROM (
    SELECT
      selling_event_num,
      sku_num,
      sku_type,
      channel_country,
      channel_brand,
      selling_channel,
      selling_event_name,
      selling_event_association_id,
      event_start_tmstp,
      event_end_tmstp,
      item_start_tmstp,
      item_end_tmstp,
      event_start_tmstp_tz,
      event_end_tmstp_tz,
      item_start_tmstp_tz,
      item_end_tmstp_tz,
      selling_event_status,
      item_status,
      event_last_update_by_id,
      event_last_update_by_id_type,
      (
        SELECT
          batch_id
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE
          LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS')) AS dw_batch_id,
      (
        SELECT
          curr_batch_date
        FROM
          `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
        WHERE
          LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS')) AS dw_batch_date,
        CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_sku_dim_wrk
    ) AS t3
  WHERE
    NOT EXISTS (
      SELECT 1 AS `A12180`
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_sku_dim
      WHERE
        selling_event_num = t3.selling_event_num
        AND sku_num = t3.sku_num
        AND LOWER(channel_country) = LOWER(t3.channel_country)
        AND LOWER(channel_brand) = LOWER(t3.channel_brand)
        AND LOWER(selling_channel) = LOWER(t3.selling_channel)
        AND selling_event_association_id = t3.selling_event_association_id
    )
  QUALIFY
    (ROW_NUMBER() OVER (PARTITION BY selling_event_num, sku_num, channel_country, channel_brand, selling_channel, selling_event_association_id)) = 1);


TRUNCATE TABLE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_sku_wrk;


INSERT INTO  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_sku_wrk (selling_event_num,
    sku_num,
    sku_type,
    selling_event_association_id,
    item_start_tmstp,
    item_end_tmstp,
    item_status,
    src_updated_tmstp,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    item_start_tmstp_tz,
    item_end_tmstp_tz,
    src_updated_tmstp_tz) (
  SELECT
    t4.selling_event_num,
    t4.sku_num,
    t4.sku_type,
    t4.selling_event_association_id,
    t4.item_start_tmstp,
    t4.item_end_tmstp,
    t4.item_status,
    t4.src_updated_tmstp,
    (
    SELECT
      batch_id
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE
      LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS')) AS dw_batch_id,
    (
    SELECT
      curr_batch_date
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
    WHERE
      LOWER(subject_area_nm) = LOWER('NAP_PRODUCT_SELLING_EVENTS')) AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
    item_start_tmstp_tz,
    item_end_tmstp_tz,
    src_updated_tmstp_tz
  FROM (
    SELECT
      selling_event_num,
      sku_num,
      sku_type,
      selling_event_association_id,
      CAST(item_start_tmstp AS TIMESTAMP) AS item_start_tmstp,
      CAST(item_end_tmstp AS TIMESTAMP) AS item_end_tmstp,
      item_status,
      CAST(src_updated_tmstp AS TIMESTAMP) AS src_updated_tmstp,
      `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(item_start_tmstp) as item_start_tmstp_tz,
      `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(item_end_tmstp) as item_end_tmstp_tz,
      `{{params.gcp_project_id}}`.jwn_udf.udf_time_zone(src_updated_tmstp) as src_updated_tmstp_tz
    FROM (
      SELECT
        id_sellingeventid AS selling_event_num,
        id_skuid AS sku_num,
        id_skuidtype AS sku_type,
        id_associationid AS selling_event_association_id,
        `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(item_start) AS item_start_tmstp,
        `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(item_end) AS item_end_tmstp,
        status AS item_status,
        `{{params.gcp_project_id}}`.jwn_udf.iso8601_tmstp(lastupdatedat) AS src_updated_tmstp
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_event_sku_ldg
      UNION ALL
      SELECT
        selling_event_num,
        sku_num,
        sku_type,
        selling_event_association_id,
        CAST(item_start_tmstp AS STRING) AS item_start_tmstp,
        CAST(item_end_tmstp AS STRING) AS item_end_tmstp,
        item_status,
        CAST(src_updated_tmstp AS STRING) AS src_updated_tmstp
      FROM
        `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_missing_events_wrk) AS SKU_LDG
    WHERE
      sku_num IS NOT NULL
    QUALIFY
      (ROW_NUMBER() OVER (PARTITION BY selling_event_num, sku_num, sku_type, selling_event_association_id ORDER BY src_updated_tmstp DESC)) = 1) AS t4
  LEFT JOIN
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_selling_event_xref AS event
  ON
    LOWER(t4.selling_event_num) = LOWER(event.selling_event_num )
  WHERE
    event.selling_event_num IS NULL);


TRUNCATE TABLE
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_missing_events_wrk;


BEGIN
INSERT INTO
  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_missing_events_wrk (selling_event_num,
    sku_num,
    sku_type,
    selling_event_association_id,
    item_start_tmstp,
    item_end_tmstp,
    item_status,
    src_updated_tmstp,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    item_start_tmstp_tz,
    item_end_tmstp_tz,
    src_updated_tmstp_tz) (
  SELECT
    selling_event_num,
    sku_num,
    sku_type,
    selling_event_association_id,
    item_start_tmstp,
    item_end_tmstp,
    item_status,
    src_updated_tmstp,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    item_start_tmstp_tz,
    item_end_tmstp_tz,
    src_updated_tmstp_tz
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.product_selling_sku_wrk);
END
  ;
	