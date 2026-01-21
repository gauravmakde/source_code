TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_indicator_flag_vtw;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_indicator_flag_vtw ( RMS_SKU_NUM,
    STORE_TYPE_CODE,
    RP_IND_FLAG,
    EFF_BEGIN_TMSTP,
    EFF_END_TMSTP,
    EFF_BEGIN_TMSTP_TZ,
    EFF_END_TMSTP_TZ )


WITH SRC_1 AS (
  SELECT            
            DISTINCT RMS_SKU_NUM,
            STORE_TYPE_CODE,
            RP_IND_FLAG,
            RANGE( EFF_BEGIN_TMSTP, EFF_END_TMSTP) AS RP_PERIOD,
            RANGE(CAST( EFF_BEGIN_TMSTP_TZ AS TIMESTAMP), CAST(EFF_END_TMSTP_TZ AS TIMESTAMP)) AS RP_PERIOD_TZ,
          from  (
    --inner normalize
            SELECT rms_sku_num,location_num,rp_ind_flag,rp_active_flag,store_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

          (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from (
            SELECT
              XREF.RMS_SKU_NUM,
              XREF.LOCATION_NUM,
              XREF.RP_IND_FLAG,
              XREF.RP_ACTIVE_FLAG,
              XREF.EFF_BEGIN_TMSTP,
              XREF.EFF_END_TMSTP,
              STORE.STORE_TYPE_CODE,
              XREF.EFF_BEGIN_TMSTP_TZ,
              XREF.EFF_END_TMSTP_TZ
            FROM
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref XREF
            JOIN
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim STORE
            ON
              STORE.STORE_NUM = XREF.LOCATION_NUM
            WHERE
              XREF.DW_BATCH_ID > (
              SELECT
                CAST(trunc(CAST(CONFIG_VALUE AS FLOAT64)) AS BIGINT)
              FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE
                LOWER(interface_code) = LOWER('RS_LOAD_MIN_BATCH_ID'))
              AND XREF.DW_BATCH_ID <= (
              SELECT
                CAST(TRUNC(CAST(CONFIG_VALUE AS FLOAT64)) AS BIGINT)
              FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE
                LOWER(interface_code) = LOWER('RS_LOAD_MAX_BATCH_ID'))
              AND LOWER(xref.rp_ind_flag) = LOWER('Y')
              AND store.store_close_date IS NULL
              AND LOWER(store.store_country_code) <> LOWER('CA')
              AND LOWER(store.store_type_code) NOT IN (LOWER('RS'), LOWER('RR'))
            GROUP BY
              1,
              2,
              3,
              4,
              5,
              6,
              7,8,9 ) TMP 
              ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,location_num,rp_ind_flag,rp_active_flag,store_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp)),


SRC_2 AS (
  SELECT               
            DISTINCT RMS_SKU_NUM,
            STORE_TYPE_CODE,
            RP_IND_FLAG,
            RANGE( EFF_BEGIN_TMSTP, EFF_END_TMSTP) AS RP_PERIOD,
            RANGE( CAST(EFF_BEGIN_TMSTP_TZ AS TIMESTAMP), CAST(EFF_END_TMSTP_TZ AS TIMESTAMP)) AS RP_PERIOD_TZ,
          from  (
    --inner normalize
            SELECT rms_sku_num,location_num,rp_ind_flag,rp_active_flag,store_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

          (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from (
            SELECT
              XREF.RMS_SKU_NUM,
              XREF.LOCATION_NUM,
              XREF.RP_IND_FLAG,
              XREF.RP_ACTIVE_FLAG,
              XREF.EFF_BEGIN_TMSTP,
              XREF.EFF_END_TMSTP,
              STORE.STORE_TYPE_CODE,
              XREF.EFF_BEGIN_TMSTP_TZ,
              XREF.EFF_END_TMSTP_TZ
            FROM
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref XREF
            JOIN
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim STORE
            ON
              STORE.STORE_NUM = XREF.LOCATION_NUM
            WHERE
              XREF.DW_BATCH_ID > (
              SELECT
                CAST(TRUNC(CAST(CONFIG_VALUE AS FLOAT64)) AS BIGINT)
              FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE
                LOWER(interface_code) = LOWER('RS_LOAD_MIN_BATCH_ID'))
              AND XREF.DW_BATCH_ID <= (
              SELECT
                CAST(CONFIG_VALUE AS BIGINT)
              FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE
                LOWER(interface_code) = LOWER('RS_LOAD_MAX_BATCH_ID'))
                AND LOWER(XREF.rp_ind_flag) = LOWER('Y')
                AND STORE.store_close_date IS NULL
                AND LOWER(STORE.store_country_code) <> LOWER('CA')
                AND LOWER(STORE.store_type_code) NOT IN (LOWER('RS'),  LOWER('RR'))
            GROUP BY
              1,
              2,
              3,
              4,
              5,
              6,
              7,8,9 ) TMP
              ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,location_num,rp_ind_flag,rp_active_flag,store_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp))



SELECT
  RMS_SKU_NUM,
  STORE_TYPE_CODE,
  RP_IND_FLAG,
  RANGE_START (EFF_PERIOD) AS EFF_BEGIN,
  RANGE_END (EFF_PERIOD) AS EFF_BEGIN,
  CAST(EFF_PERIOD_TZ AS STRING),
  CAST(EFF_PERIOD_TZ AS STRING)
FROM 
 (
    --inner normalize
            SELECT rms_sku_num,store_type_code,rp_ind_flag,eff_period,eff_period_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,store_type_code,rp_ind_flag ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

        (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,store_type_code,rp_ind_flag ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
FROM (
  SELECT                        
    DISTINCT SRC2.RMS_SKU_NUM,
    SRC2.STORE_TYPE_CODE,
    SRC2.RP_IND_FLAG,
    RANGE( EFF_BEGIN_TMSTP, EFF_END_TMSTP ) AS EFF_PERIOD,
    RANGE(CAST( EFF_BEGIN_TMSTP_TZ AS TIMESTAMP), CAST(EFF_END_TMSTP_TZ AS TIMESTAMP)) AS EFF_PERIOD_TZ,
    EFF_END_TMSTP,
    EFF_BEGIN_TMSTP
  FROM (
    SELECT
      SRC1.RMS_SKU_NUM,
      SRC1.STORE_TYPE_CODE,
      SRC1.RP_IND_FLAG,
      SRC1.EFF_BEGIN_TMSTP,
      (COALESCE( cast(MIN(SRC1.EFF_BEGIN_TMSTP) OVER(PARTITION BY SRC1.RMS_SKU_NUM, SRC1.STORE_TYPE_CODE ORDER BY SRC1.EFF_BEGIN_TMSTP ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) as timestamp), CAST('0000-01-01 00:00:00' AS timestamp) )) AS EFF_END_TMSTP,
      SRC1.EFF_BEGIN_TMSTP_TZ,
      (COALESCE( cast(MIN(SRC1.EFF_BEGIN_TMSTP_TZ) OVER(PARTITION BY SRC1.RMS_SKU_NUM, SRC1.STORE_TYPE_CODE ORDER BY SRC1.EFF_BEGIN_TMSTP ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) as timestamp), CAST('0000-01-01 00:00:00' AS timestamp) )) AS EFF_END_TMSTP_TZ
    FROM (
      SELECT
        *
      FROM (
        SELECT
          RMS_SKU_NUM,
          STORE_TYPE_CODE,
          RP_IND_FLAG,
          RANGE_START (RP_PERIOD) AS EFF_BEGIN_TMSTP,
		      RANGE_START (RP_PERIOD_TZ) AS EFF_BEGIN_TMSTP_TZ,
          '1' AS REC_IDENTIFIER
        FROM SRC_1 AS TMP1
        UNION ALL
        SELECT
          RMS_SKU_NUM,
          STORE_TYPE_CODE,
          'N' AS RP_IND_FLAG,
          RANGE_END (RP_PERIOD) AS EFF_BEGIN_TMSTP,
		      RANGE_END (RP_PERIOD_TZ) AS EFF_BEGIN_TMSTP_TZ,
          '2' AS REC_IDENTIFIER
        FROM SRC_2 AS TMP1 ) COMBINED
      WHERE
        EFF_BEGIN_TMSTP <> CAST('0000-01-01 00:00:00' AS TIMESTAMP)
      QUALIFY
        ROW_NUMBER () OVER (PARTITION BY RMS_SKU_NUM, STORE_TYPE_CODE, EFF_BEGIN_TMSTP ORDER BY REC_IDENTIFIER ASC ) = 1 ) SRC1 ) SRC2
  WHERE
    EFF_BEGIN_TMSTP < EFF_END_TMSTP ) SRC
        )AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,store_type_code,rp_ind_flag,eff_period,eff_period_tz
ORDER BY  rms_sku_num,store_type_code,rp_ind_flag, eff_begin_tmstp)
    ;
	
	
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_active_flag_vtw;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_active_flag_vtw (rms_sku_num,
    store_type_code,
    rp_active_flag,
    eff_begin_tmstp,
    eff_end_tmstp,
    EFF_BEGIN_TMSTP_TZ,
    EFF_END_TMSTP_TZ)


WITH SRC_3 AS (
  SELECT                      
            DISTINCT RMS_SKU_NUM,
            STORE_TYPE_CODE,
            RP_ACTIVE_FLAG,
            RANGE( EFF_BEGIN_TMSTP, EFF_END_TMSTP) AS RP_PERIOD,
            RANGE( cast(EFF_BEGIN_TMSTP_TZ as timestamp), cast(EFF_END_TMSTP_TZ as timestamp)) AS RP_PERIOD_TZ
          from  (
    --inner normalize
            SELECT rms_sku_num,location_num,rp_active_flag,store_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

          (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from (
            SELECT
              XREF.RMS_SKU_NUM,
              XREF.LOCATION_NUM,
              XREF.RP_ACTIVE_FLAG,
              XREF.EFF_BEGIN_TMSTP,
              XREF.EFF_END_TMSTP,
              STORE.STORE_TYPE_CODE,
              XREF.EFF_BEGIN_TMSTP_TZ,
              XREF.EFF_END_TMSTP_TZ,
            FROM
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref XREF
            INNER JOIN
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim STORE
            ON
              STORE.STORE_NUM = XREF.LOCATION_NUM
            WHERE
              XREF.DW_BATCH_ID > (
              SELECT
                CAST(TRUNC(CAST(CONFIG_VALUE AS FLOAT64)) AS BIGINT)
              FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE
                LOWER(interface_code) = LOWER('RS_LOAD_MIN_BATCH_ID'))
                AND XREF.DW_BATCH_ID <= (SELECT
                CAST(TRUNC(CAST(CONFIG_VALUE AS FLOAT64)) AS BIGINT)
              FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE
                LOWER(interface_code) = LOWER('RS_LOAD_MAX_BATCH_ID'))
                AND LOWER(xref.rp_active_flag) = LOWER('Y')
                AND store.store_close_date IS NULL
                AND LOWER(store.store_country_code) <> LOWER('CA')
                AND LOWER(store.store_type_code) NOT IN (LOWER('RS'), LOWER('RR'))
            GROUP BY
              xref.rms_sku_num,
              xref.location_num,
              xref.rp_active_flag,
              xref.eff_begin_tmstp,
              xref.eff_end_tmstp,
              store.store_type_code,
              xref.eff_begin_tmstp_tz,
              xref.eff_end_tmstp_tz ) TMP
              ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,location_num,rp_active_flag,store_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp)
),

SRC_4 AS (
  SELECT               
            DISTINCT RMS_SKU_NUM,
            STORE_TYPE_CODE,
            RP_ACTIVE_FLAG,
            RANGE( EFF_BEGIN_TMSTP, EFF_END_TMSTP) AS RP_PERIOD,
            RANGE( CAST(EFF_BEGIN_TMSTP_TZ AS TIMESTAMP), CAST(EFF_END_TMSTP_TZ AS TIMESTAMP)) AS RP_PERIOD_TZ
          from  (
    --inner normalize
            SELECT rms_sku_num,location_num,rp_active_flag,store_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,location_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

          (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, location_num ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from (
            SELECT
              XREF.RMS_SKU_NUM,
              XREF.LOCATION_NUM,
              XREF.RP_ACTIVE_FLAG,
              XREF.EFF_BEGIN_TMSTP,
              XREF.EFF_END_TMSTP,
              STORE.STORE_TYPE_CODE,
              XREF.EFF_BEGIN_TMSTP_TZ,
              XREF.EFF_END_TMSTP_TZ
            FROM
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref AS xref
            INNER JOIN
              `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim AS store
            ON
              STORE.STORE_NUM = XREF.LOCATION_NUM
            WHERE
              XREF.DW_BATCH_ID > (
              SELECT
                CAST(TRUNC(CAST(CONFIG_VALUE AS FLOAT64)) AS BIGINT)
              FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE
                LOWER(interface_code) = LOWER('RS_LOAD_MIN_BATCH_ID'))
              AND XREF.DW_BATCH_ID <= (
              SELECT
                CAST(TRUNC(CAST(CONFIG_VALUE AS FLOAT64))AS BIGINT)
              FROM
                `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE
                LOWER(interface_code) = LOWER('RS_LOAD_MAX_BATCH_ID'))
                AND LOWER(XREF.rp_active_flag) = LOWER('Y')
                AND store.store_close_date IS NULL
                AND LOWER(store.store_country_code) <> LOWER('CA')
                AND LOWER(store.store_type_code) NOT IN (LOWER('RS'), LOWER('RR'))
            GROUP BY
              xref.rms_sku_num,
              xref.location_num,
              xref.rp_active_flag,
              xref.eff_begin_tmstp,
              xref.eff_end_tmstp,
              store.store_type_code,
              xref.eff_begin_tmstp_tz,
              xref.eff_end_tmstp_tz ) TMP
              ) AS ordered_data
) AS grouped_data
GROUP BY rms_sku_num,location_num,rp_active_flag,store_type_code,eff_begin_tmstp_tz,eff_end_tmstp_tz, range_group
ORDER BY  rms_sku_num, location_num, eff_begin_tmstp)
)


SELECT
  RMS_SKU_NUM,
  STORE_TYPE_CODE,
  RP_ACTIVE_FLAG,
  RANGE_START(eff_period) AS eff_begin,
  RANGE_END(eff_period) AS eff_begin,
  CAST(RANGE_START(EFF_PERIOD_TZ) AS STRING) AS eff_begin_tz,
  CAST(RANGE_END(EFF_PERIOD_TZ) AS STRING) AS eff_begin_tz
FROM 
 (
    --inner normalize
            SELECT rms_sku_num,store_type_code,rp_active_flag,eff_period,eff_period_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
            SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,store_type_code,rp_active_flag ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
            FROM

          (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,store_type_code,rp_active_flag ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
FROM (
  SELECT                       
    DISTINCT SRC2.RMS_SKU_NUM,
    SRC2.STORE_TYPE_CODE,
    SRC2.RP_ACTIVE_FLAG,
    RANGE( EFF_BEGIN_TMSTP, EFF_END_TMSTP ) AS EFF_PERIOD,
    RANGE( EFF_BEGIN_TMSTP_TZ, EFF_END_TMSTP_TZ ) AS EFF_PERIOD_TZ,
    EFF_BEGIN_TMSTP,
    EFF_END_TMSTP
  FROM (
    SELECT
      SRC1.RMS_SKU_NUM,
      SRC1.STORE_TYPE_CODE,
      SRC1.RP_ACTIVE_FLAG,
      SRC1.EFF_BEGIN_TMSTP,
      (COALESCE(MIN(SRC1.EFF_BEGIN_TMSTP) OVER(PARTITION BY SRC1.RMS_SKU_NUM, SRC1.STORE_TYPE_CODE ORDER BY SRC1.EFF_BEGIN_TMSTP ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),CAST('0000-01-01 00:00:00' AS TIMESTAMP)) ) AS EFF_END_TMSTP,
      SRC1.EFF_BEGIN_TMSTP_TZ,
      (COALESCE(MIN(SRC1.EFF_BEGIN_TMSTP) OVER(PARTITION BY SRC1.RMS_SKU_NUM, SRC1.STORE_TYPE_CODE ORDER BY SRC1.EFF_BEGIN_TMSTP ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),CAST('0000-01-01 00:00:00' AS TIMESTAMP)) ) AS EFF_END_TMSTP_TZ,
    FROM (
      SELECT
        *
      FROM (
        SELECT
          RMS_SKU_NUM,
          STORE_TYPE_CODE,
          RP_ACTIVE_FLAG,
          RANGE_START(RP_PERIOD) AS EFF_BEGIN_TMSTP,
          RANGE_START(RP_PERIOD_TZ) AS EFF_BEGIN_TMSTP_TZ,
          '1' AS REC_IDENTIFIER
        FROM SRC_3 AS TMP1
        UNION ALL
        SELECT
          RMS_SKU_NUM,
          STORE_TYPE_CODE,
          'N' AS RP_ACTIVE_FLAG,
          RANGE_END(RP_PERIOD) AS EFF_BEGIN_TMSTP,
          RANGE_END(RP_PERIOD_TZ) AS EFF_BEGIN_TMSTP_TZ,
          '2' AS REC_IDENTIFIER
        FROM SRC_4 AS TMP1 ) COMBINED
      WHERE
        EFF_BEGIN_TMSTP <> CAST('0000-01-01 00:00:00' AS TIMESTAMP)
      QUALIFY
        ROW_NUMBER () OVER (PARTITION BY RMS_SKU_NUM, STORE_TYPE_CODE, EFF_BEGIN_TMSTP ORDER BY REC_IDENTIFIER ASC ) = 1 ) SRC1 ) SRC2
  WHERE
    EFF_BEGIN_TMSTP < EFF_END_TMSTP ) SRC
          )AS ordered_data
  ) AS grouped_data
  GROUP BY rms_sku_num,store_type_code,rp_active_flag,eff_period,eff_period_tz
  ORDER BY  rms_sku_num,store_type_code,rp_active_flag, eff_begin_tmstp)
    
;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_flag_combined_vtw;

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_flag_combined_vtw
(
    RMS_SKU_NUM
  , STORE_TYPE_CODE
  , RP_IND_FLAG
  , RP_ACTIVE_FLAG
  , EFF_BEGIN_TMSTP
  , EFF_END_TMSTP
  , EFF_BEGIN_TMSTP_TZ
  , EFF_END_TMSTP_TZ
)
  SELECT
  RMS_SKU_NUM
  , STORE_TYPE_CODE
  , RP_IND_FLAG
  , RP_ACTIVE_FLAG
  ,RANGE_START(EFF_PERIOD) AS EFF_BEGIN
  ,RANGE_END(EFF_PERIOD) AS EFF_BEGIN
  ,CAST(RANGE_START(EFF_PERIOD_TZ) as STRING) AS EFF_BEGIN_TZ
  ,CAST(RANGE_END(EFF_PERIOD_TZ) AS STRING) AS EFF_BEGIN_TZ
  FROM 
 (
    --inner normalize
            SELECT rms_sku_num,store_type_code,rp_ind_flag,rp_active_flag,eff_period,eff_period_tz, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,store_type_code,rp_ind_flag,rp_active_flag ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM

        (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_sku_num,store_type_code,rp_ind_flag,rp_active_flag ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
FROM (
    SELECT DISTINCT                   
        SRC2.RMS_SKU_NUM
      , SRC2.STORE_TYPE_CODE
      , SRC2.RP_IND_FLAG
      , SRC2.RP_ACTIVE_FLAG
      , RANGE( EFF_BEGIN_TMSTP, EFF_END_TMSTP ) AS EFF_PERIOD
      , RANGE( CAST(EFF_BEGIN_TMSTP_TZ AS TIMESTAMP), CAST(EFF_END_TMSTP_TZ AS TIMESTAMP)) AS EFF_PERIOD_TZ
      , EFF_END_TMSTP
      , EFF_BEGIN_TMSTP
    FROM (
      SELECT
        SRC1.RMS_SKU_NUM
      , SRC1.STORE_TYPE_CODE
      , SRC1.RP_IND_FLAG
      , SRC1.RP_ACTIVE_FLAG
      , SRC1.EFF_BEGIN_TMSTP
      , (COALESCE(MIN(SRC1.EFF_BEGIN_TMSTP) OVER(PARTITION BY SRC1.RMS_SKU_NUM, SRC1.STORE_TYPE_CODE ORDER BY SRC1.EFF_BEGIN_TMSTP ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)  ,CAST('0000-01-01 00:00:00' AS TIMESTAMP))) AS EFF_END_TMSTP
      , SRC1.EFF_BEGIN_TMSTP_TZ
      , (COALESCE(cast(MIN(SRC1.EFF_BEGIN_TMSTP_TZ) OVER(PARTITION BY SRC1.RMS_SKU_NUM, SRC1.STORE_TYPE_CODE ORDER BY SRC1.EFF_BEGIN_TMSTP ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) as timestamp)  ,CAST('0000-01-01 00:00:00' AS TIMESTAMP))) AS EFF_END_TMSTP_TZ
      FROM (
        SELECT ind.rms_sku_num,
        ind.store_type_code,
        ind.rp_ind_flag,
        COALESCE(active.rp_active_flag, 'N') AS rp_active_flag,
        ind.eff_begin_tmstp,
        ind.EFF_BEGIN_TMSTP_TZ
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_indicator_flag_vtw AS ind
        LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_active_flag_vtw AS active 
        ON LOWER(ind.rms_sku_num) = LOWER(active.rms_sku_num) 
        AND LOWER(ind.store_type_code) = LOWER(active.store_type_code)
			WHERE IND.EFF_BEGIN_TMSTP BETWEEN active.EFF_BEGIN_TMSTP AND active.EFF_END_TMSTP
			UNION ALL
			SELECT active.rms_sku_num,
      active.store_type_code,
      COALESCE(ind.rp_ind_flag, 'N') AS rp_ind_flag,
      active.rp_active_flag,
      active.eff_begin_tmstp,
      active.eff_begin_tmstp_tz
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_active_flag_vtw AS active
      LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_indicator_flag_vtw AS ind 
      ON LOWER(ind.rms_sku_num) = LOWER(active.rms_sku_num) 
      AND LOWER(ind.store_type_code) = LOWER(active.store_type_code)
			WHERE  ACTIVE.EFF_BEGIN_TMSTP BETWEEN IND.EFF_BEGIN_TMSTP AND IND.EFF_END_TMSTP
      ) SRC1
    ) SRC2
    WHERE EFF_BEGIN_TMSTP < EFF_END_TMSTP
  ) SRC
        ) AS ordered_data
  ) AS grouped_data
  GROUP BY rms_sku_num,store_type_code,rp_ind_flag,rp_active_flag,eff_period,eff_period_tz
  ORDER BY  rms_sku_num,store_type_code,rp_ind_flag,rp_active_flag, eff_begin_tmstp)
  
;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_indicator_xref_vtw;

  
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_indicator_xref_vtw (rms_sku_num,
    location_num,
    rp_ind_flag,
    rp_active_flag,
    eff_begin_tmstp,
    eff_end_tmstp,
	  eff_begin_tmstp_tz,
    eff_end_tmstp_tz) (
  SELECT
    vtw.rms_sku_num,
    STORE.store_num AS location_num,
    vtw.rp_ind_flag,
    vtw.rp_active_flag,
    vtw.eff_begin_tmstp,
    vtw.eff_end_tmstp,
	  vtw.eff_begin_tmstp_tz,
    vtw.eff_end_tmstp_tz
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_flag_combined_vtw AS vtw
  INNER JOIN (
    SELECT
      store_num,
      CASE
        WHEN LOWER(store_type_code) = LOWER('RS') THEN 'FL'
        WHEN LOWER(store_type_code) = LOWER('RR') THEN 'RK'
        ELSE NULL
    END
      AS store_type_code
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.store_dim
    WHERE
      LOWER(store_type_code) IN (LOWER('RS'),
        LOWER('RR'))
      AND store_close_date IS NULL
      AND LOWER(store_country_code) <> LOWER('CA')) AS STORE
  ON
    LOWER(vtw.store_type_code) = LOWER(STORE.store_type_code )) ;


BEGIN
BEGIN TRANSACTION;
DELETE
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref AS tgt
WHERE
  EXISTS (
  SELECT
    *
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_indicator_xref_vtw AS src
  WHERE
    LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
    AND location_num = tgt.location_num);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_indicator_xref (rms_sku_num,
    location_num,
    rp_ind_flag,
    rp_active_flag,
    eff_begin_tmstp,
    eff_end_tmstp,
    dw_batch_id,
    dw_batch_date,
    dw_sys_load_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp_tz) (
  SELECT
    rms_sku_num,
    location_num,
    rp_ind_flag,
    rp_active_flag,
    eff_begin_tmstp,
    eff_end_tmstp,
    (
    SELECT
      CAST(TRUNC(CAST(config_value AS FLOAT64)) AS BIGINT) AS config_value
    FROM
      `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
    WHERE
      LOWER(interface_code) = LOWER('RS_LOAD_MAX_BATCH_ID')) AS dw_batch_id,
    CURRENT_DATE AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_load_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp_tz,
  FROM
    `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rs_rp_indicator_xref_vtw);
COMMIT TRANSACTION;
END
  ;	