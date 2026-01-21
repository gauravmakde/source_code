
-- PURGE THE PREVIOUSLY LOADED RP_INDICATOR_DTL_KEY_LDG

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_key_ldg;

-- FIND THE LIST OF SKU/LOCATION THAT NEEDS RP FLAGS RE-CALCULATED

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_key_ldg (rms_sku_num, location_num)
(SELECT *
 FROM ((SELECT rms_sku_num,
      location_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_sku_loc_timeline_dim
     WHERE dw_batch_id > (SELECT CAST(config_value AS BIGINT) AS config_value
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('RP_LOAD_MIN_BATCH_ID'))
      AND dw_batch_id <= (SELECT CAST(config_value AS BIGINT) AS config_value
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('RP_LOAD_MAX_BATCH_ID'))
     GROUP BY rms_sku_num,
      location_num
     UNION DISTINCT
     SELECT rms_sku_num,
      location_num
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim
     WHERE dw_batch_id > (SELECT CAST(config_value AS BIGINT) AS config_value
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('RP_LOAD_MIN_BATCH_ID'))
      AND dw_batch_id <= (SELECT CAST(config_value AS BIGINT) AS config_value
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
        WHERE LOWER(interface_code) = LOWER('RP_LOAD_MAX_BATCH_ID'))
     GROUP BY rms_sku_num,
      location_num)
    UNION DISTINCT
    SELECT rms_sku_num,
     location_num
    FROM (SELECT ELIG_DIM.rms_sku_num,
       STORE_DIM.store_num AS location_num,
       ELIG_DIM.channel_brand,
       ELIG_DIM.is_replenishment_eligible_ind,
        CASE
        WHEN LOWER(ELIG_DIM.channel_brand) = LOWER('NORDSTROM')
        THEN 'FL'
        WHEN LOWER(ELIG_DIM.channel_brand) = LOWER('NORDSTROM_RACK')
        THEN 'RK'
        ELSE NULL
        END AS elig_dim_store_type_code,
       ELIG_DIM.eff_begin_tmstp
      FROM (SELECT *
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim
        WHERE rms_sku_num IN (SELECT rms_sku_num
           FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim
           WHERE dw_batch_id > (SELECT CAST(config_value AS BIGINT) AS A598617007
              FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE LOWER(interface_code) = LOWER('RP_LOAD_MIN_BATCH_ID'))
            AND dw_batch_id <= (SELECT CAST(config_value AS BIGINT) AS A598617007
              FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.config_lkup
              WHERE LOWER(interface_code) = LOWER('RP_LOAD_MAX_BATCH_ID'))
           GROUP BY rms_sku_num)) AS ELIG_DIM
       INNER JOIN (SELECT CASE
          WHEN channel_num IN (110, 120)
          THEN 'FL'
          WHEN channel_num IN (210, 250)
          THEN 'RK'
          ELSE NULL
          END AS drvd_store_type_code,
         store_num,
         store_name,
         store_short_name,
         store_abbrev_name,
         store_type_code,
         store_type_desc,
         selling_store_ind,
         gross_square_footage,
         store_open_date,
         store_close_date,
         region_num,
         region_desc,
         region_medium_desc,
         region_short_desc,
         business_unit_num,
         business_unit_desc,
         business_unit_medium_desc,
         cmpy_num,
         cmpy_name,
         cmpy_medium_name,
         group_num,
         group_desc,
         group_medium_desc,
         subgroup_num,
         subgroup_desc,
         subgroup_medium_desc,
         subgroup_short_desc,
         store_address_line_1,
         store_address_city,
         store_address_state,
         store_address_state_name,
         store_postal_code,
         store_address_county,
         store_country_code,
         store_country_name,
         store_dma_code,
         store_dma_desc,
         store_location_latitude,
         store_location_longitude,
         store_time_zone,
         store_time_zone_desc,
         daylight_savings_time_ind,
         store_time_zone_offset,
         distribution_center_num,
         distribution_center_name,
         location_type_code,
         location_type_desc,
         channel_num,
         channel_desc,
         comp_status_code,
         comp_status_desc,
         dw_batch_id,
         dw_batch_date,
         dw_sys_load_tmstp,
         dw_sys_updt_tmstp,
         eligibility_types,
         receivinglocation_location_name,
         receivinglocation_location_number,
         receivinglocation_latitude,
         receivinglocation_longitude,
         receivinglocation_country_code,
         receivinglocation_country_iso_code,
         receivinglocation_country_name,
         receivinglocation_country_tel_code,
         receivinglocation_address_state,
         receivinglocation_address_state_name,
         receivinglocation_region_code,
         receivinglocation_region_name,
         receivinglocation_time_zone,
         receivinglocation_time_zone_offset,
         receivinglocation_address_city,
         receivinglocation_address_county,
         receivinglocation_address_line_1,
         receivinglocation_postal_code
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim AS store
        WHERE channel_num IN (110, 120, 210, 250)
         AND store_close_date IS NULL
         AND LOWER(store_country_code) <> LOWER('CA')
         AND LOWER(selling_store_ind) = LOWER('S')
         AND LOWER(store_type_code) NOT IN (LOWER('RS'), LOWER('RR'))) AS STORE_DIM ON LOWER(CASE
          WHEN LOWER(ELIG_DIM.channel_brand) = LOWER('NORDSTROM')
          THEN 'FL'
          WHEN LOWER(ELIG_DIM.channel_brand) = LOWER('NORDSTROM_RACK')
          THEN 'RK'
          ELSE NULL
          END) = LOWER(STORE_DIM.drvd_store_type_code)
      GROUP BY ELIG_DIM.rms_sku_num,
       location_num,
       ELIG_DIM.channel_brand,
       ELIG_DIM.is_replenishment_eligible_ind,
       elig_dim_store_type_code,
       ELIG_DIM.eff_begin_tmstp) AS ELIG_STORE
    GROUP BY rms_sku_num,
     location_num) AS DLY_KEY);

-- PURGE THE PREVIOUSLY LOADED RP_INDICATOR_DTL_LDG
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_ldg;

-- INCREMENTAL CHANGES FROM ELIGIBILITY_DIM
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_ldg (rms_sku_num, location_num, is_replenishment_eligible_ind,
 replenishment_setting_type_code, replenishment_setting_value_desc, on_sale_date, off_sale_date, eff_begin_tmstp,eff_begin_tmstp_tz,
 data_source_desc)
(SELECT ELIG_STORE.rms_sku_num,
  ELIG_STORE.location_num,
  ELIG_STORE.is_replenishment_eligible_ind,
   CASE
   WHEN setting_dim.replenishment_setting_type_code IS NOT NULL
   THEN setting_dim.replenishment_setting_type_code
   ELSE 'REPLENISHMENT_METHOD'
   END AS replenishment_setting_type_code,
   CASE
   WHEN setting_dim.replenishment_setting_value_desc IS NOT NULL
   THEN setting_dim.replenishment_setting_value_desc
   ELSE 'NO_REPLENISHMENT'
   END AS replenishment_setting_value_desc,
   CASE
   WHEN osos_dim.rms_sku_num IS NOT NULL
   THEN CAST(RANGE_START(osos_dim.eff_period) AS DATE)
   ELSE DATE '3999-12-31'
   END AS on_sale_date,
   CASE
   WHEN osos_dim.rms_sku_num IS NOT NULL
   THEN CAST(RANGE_END(osos_dim.eff_period) AS DATE)
   ELSE DATE '3999-12-30'
   END AS off_sale_date,
  ELIG_STORE.eff_begin_tmstp,
  ELIG_store.eff_begin_tmstp_tz,
  'ELIGIBILITY_DIM' AS data_source_desc
 FROM (SELECT ELIG_DIM.rms_sku_num,
    STORE_DIM.store_num AS location_num,
    ELIG_DIM.channel_brand,
    ELIG_DIM.is_replenishment_eligible_ind,
     CASE
     WHEN LOWER(ELIG_DIM.channel_brand) = LOWER('NORDSTROM')
     THEN 'FL'
     WHEN LOWER(ELIG_DIM.channel_brand) = LOWER('NORDSTROM_RACK')
     THEN 'RK'
     ELSE NULL
     END AS elig_dim_store_type_code,
    ELIG_DIM.eff_begin_tmstp,
    ELIG_DIM.eff_begin_tmstp_tz,
   FROM (SELECT *
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim
     WHERE rms_sku_num IN (SELECT rms_sku_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_key_ldg
        GROUP BY rms_sku_num)) AS ELIG_DIM
    INNER JOIN (SELECT CASE
       WHEN channel_num IN (110, 120)
       THEN 'FL'
       WHEN channel_num IN (210, 250)
       THEN 'RK'
       ELSE NULL
       END AS drvd_store_type_code,
      store_num,
      store_name,
      store_short_name,
      store_abbrev_name,
      store_type_code,
      store_type_desc,
      selling_store_ind,
      gross_square_footage,
      store_open_date,
      store_close_date,
      region_num,
      region_desc,
      region_medium_desc,
      region_short_desc,
      business_unit_num,
      business_unit_desc,
      business_unit_medium_desc,
      cmpy_num,
      cmpy_name,
      cmpy_medium_name,
      group_num,
      group_desc,
      group_medium_desc,
      subgroup_num,
      subgroup_desc,
      subgroup_medium_desc,
      subgroup_short_desc,
      store_address_line_1,
      store_address_city,
      store_address_state,
      store_address_state_name,
      store_postal_code,
      store_address_county,
      store_country_code,
      store_country_name,
      store_dma_code,
      store_dma_desc,
      store_location_latitude,
      store_location_longitude,
      store_time_zone,
      store_time_zone_desc,
      daylight_savings_time_ind,
      store_time_zone_offset,
      distribution_center_num,
      distribution_center_name,
      location_type_code,
      location_type_desc,
      channel_num,
      channel_desc,
      comp_status_code,
      comp_status_desc,
      dw_batch_id,
      dw_batch_date,
      dw_sys_load_tmstp,
      dw_sys_updt_tmstp,
      eligibility_types,
      receivinglocation_location_name,
      receivinglocation_location_number,
      receivinglocation_latitude,
      receivinglocation_longitude,
      receivinglocation_country_code,
      receivinglocation_country_iso_code,
      receivinglocation_country_name,
      receivinglocation_country_tel_code,
      receivinglocation_address_state,
      receivinglocation_address_state_name,
      receivinglocation_region_code,
      receivinglocation_region_name,
      receivinglocation_time_zone,
      receivinglocation_time_zone_offset,
      receivinglocation_address_city,
      receivinglocation_address_county,
      receivinglocation_address_line_1,
      receivinglocation_postal_code
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim AS store
     WHERE channel_num IN (110, 120, 210, 250)
      AND store_close_date IS NULL
      AND LOWER(store_country_code) <> LOWER('CA')
      AND LOWER(selling_store_ind) = LOWER('S')
      AND LOWER(store_type_code) NOT IN (LOWER('RS'), LOWER('RR'))) AS STORE_DIM ON LOWER(CASE
       WHEN LOWER(ELIG_DIM.channel_brand) = LOWER('NORDSTROM')
       THEN 'FL'
       WHEN LOWER(ELIG_DIM.channel_brand) = LOWER('NORDSTROM_RACK')
       THEN 'RK'
       ELSE NULL
       END) = LOWER(STORE_DIM.drvd_store_type_code)
   GROUP BY ELIG_DIM.rms_sku_num,
    location_num,
    ELIG_DIM.channel_brand,
    ELIG_DIM.is_replenishment_eligible_ind,
    elig_dim_store_type_code,
    ELIG_DIM.eff_begin_tmstp,
    ELIG_DIM.eff_begin_tmstp_tz) AS ELIG_STORE
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_key_ldg AS ldg_key ON LOWER(ldg_key.rms_sku_num) = LOWER(ELIG_STORE.rms_sku_num
     ) AND ELIG_STORE.location_num = ldg_key.location_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_timeline_normalize_vw AS osos_dim 
  ON LOWER(ELIG_STORE.rms_sku_num) = LOWER(osos_dim.rms_sku_num) 
  AND ELIG_STORE.location_num = osos_dim.location_num 
  AND LOWER(osos_dim.is_on_sale_ind) = LOWER('Y')
  AND RANGE_CONTAINS(OSOS_DIM.eff_period,ELIG_STORE.EFF_BEGIN_TMSTP)
  INNER JOIN (SELECT a.day_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(b.day_date AS DATETIME)) AS DATETIME) AS prev_sat_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.week_idnt > b.week_idnt
   WHERE b.day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY a.day_date ORDER BY b.week_idnt DESC, b.day_date DESC)) = 1) AS DAY_CAL ON
   DAY_CAL.day_date = CAST(ELIG_STORE.eff_begin_tmstp AS DATE)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS setting_dim 
  ON LOWER(ELIG_STORE.rms_sku_num) = LOWER(setting_dim.rms_sku_num)
  AND ELIG_STORE.location_num = setting_dim.location_num 
  AND LOWER(setting_dim.replenishment_setting_type_code) = LOWER('REPLENISHMENT_METHOD')
  AND RANGE_CONTAINS(RANGE(SETTING_DIM.EFF_BEGIN_TMSTP ,SETTING_DIM.EFF_END_TMSTP ) , CAST(DAY_CAL.PREV_SAT_DAY_DATE AS TIMESTAMP)));

-- INCREMENTAL CHANGES FROM OSOS_DIM

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_ldg (rms_sku_num, location_num, is_replenishment_eligible_ind,
 replenishment_setting_type_code, replenishment_setting_value_desc, on_sale_date, off_sale_date, eff_begin_tmstp,eff_begin_tmstp_tz,
 data_source_desc)
(SELECT OSOS_DIM.rms_sku_num,
  OSOS_DIM.location_num,
   CASE
   WHEN ELIG_DIM.is_replenishment_eligible_ind IS NOT NULL
   THEN ELIG_DIM.is_replenishment_eligible_ind
   ELSE 'N'
   END AS is_replenishment_eligible_ind,
   CASE
   WHEN setting_dim.replenishment_setting_type_code IS NOT NULL
   THEN setting_dim.replenishment_setting_type_code
   ELSE 'REPLENISHMENT_METHOD'
   END AS replenishment_setting_type_code,
   CASE
   WHEN setting_dim.replenishment_setting_value_desc IS NOT NULL
   THEN setting_dim.replenishment_setting_value_desc
   ELSE 'NO_REPLENISHMENT'
   END AS replenishment_setting_value_desc,
   CASE
   WHEN LOWER(OSOS_DIM.is_on_sale_ind) = LOWER('Y')
   THEN OSOS_DIM.on_sale_date
   ELSE DATE '3999-12-31'
   END AS on_sale_date,
   CASE
   WHEN LOWER(OSOS_DIM.is_on_sale_ind) = LOWER('Y')
   THEN OSOS_DIM.off_sale_date
   ELSE DATE '3999-12-30'
   END AS off_sale_date,
  OSOS_DIM.eff_begin_tmstp,
  OSOS_DIM.eff_begin_tmstp_tz,
  'OSOS_DIM' AS data_source_desc
 FROM (SELECT rms_sku_num,
    location_num,
    CAST(range_start(eff_period) AS DATE) AS on_sale_date,
    CAST(range_end(eff_period) AS DATE) AS off_sale_date,
    range_start(eff_period) AS eff_begin_tmstp,
    `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(range_start(eff_period)as string)) as eff_begin_tmstp_tz,
    is_on_sale_ind
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_timeline_normalize_vw
   WHERE (rms_sku_num, location_num) IN (SELECT (rms_sku_num, location_num)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_key_ldg
      GROUP BY rms_sku_num,
       location_num)) AS OSOS_DIM
  INNER JOIN (SELECT a.day_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(b.day_date AS DATETIME)) AS DATETIME) AS prev_sat_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b 
    ON a.week_idnt > b.week_idnt
   WHERE b.day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY a.day_date ORDER BY b.week_idnt DESC, b.day_date DESC)) = 1) AS DAY_CAL ON
   DAY_CAL.day_date = CAST(OSOS_DIM.eff_begin_tmstp AS DATE)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS setting_dim 
  ON LOWER(OSOS_DIM.rms_sku_num) = LOWER(setting_dim.rms_sku_num)
  AND OSOS_DIM.location_num = setting_dim.location_num 
  AND LOWER(setting_dim.replenishment_setting_type_code) = LOWER('REPLENISHMENT_METHOD')
  AND RANGE_CONTAINS(RANGE(SETTING_DIM.EFF_BEGIN_TMSTP ,SETTING_DIM.EFF_END_TMSTP) , CAST(DAY_CAL.PREV_SAT_DAY_DATE AS TIMESTAMP))
  LEFT JOIN 
  (SELECT b0.rms_sku_num,
    b0.channel_brand,
    b0.is_replenishment_eligible_ind,
    b0.eff_begin_tmstp,
    b0.eff_end_tmstp,
    b0.dw_batch_id,
    b0.dw_batch_date,
    b0.dw_sys_load_tmstp,
    a0.store_num AS location_num,
     CASE
     WHEN a0.channel_num IN (110, 120)
     THEN 'NORDSTROM'
     WHEN a0.channel_num IN (210, 250)
     THEN 'NORDSTROM_RACK'
     ELSE NULL
     END AS drvd_channel_brand
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim AS a0
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim AS b0 ON LOWER(CASE
       WHEN a0.channel_num IN (110, 120)
       THEN 'NORDSTROM'
       WHEN a0.channel_num IN (210, 250)
       THEN 'NORDSTROM_RACK'
       ELSE NULL
       END) = LOWER(b0.channel_brand)
   WHERE a0.channel_num IN (110, 120, 210, 250)
    AND a0.store_close_date IS NULL
    AND LOWER(a0.store_country_code) <> LOWER('CA')
    AND LOWER(a0.selling_store_ind) = LOWER('S')
    AND LOWER(a0.store_type_code) NOT IN (LOWER('RS'), LOWER('RR'))) AS ELIG_DIM 
    ON LOWER(OSOS_DIM.rms_sku_num) = LOWER(ELIG_DIM.rms_sku_num) 
    AND OSOS_DIM.location_num = ELIG_DIM.location_num
    AND RANGE_CONTAINS(RANGE(ELIG_DIM.EFF_BEGIN_TMSTP ,ELIG_DIM.EFF_END_TMSTP), OSOS_DIM.EFF_BEGIN_TMSTP)
 GROUP BY OSOS_DIM.rms_sku_num,
  OSOS_DIM.location_num,
  is_replenishment_eligible_ind,
  replenishment_setting_type_code,
  replenishment_setting_value_desc,
  on_sale_date,
  off_sale_date,
  OSOS_DIM.eff_begin_tmstp,
  eff_begin_tmstp_tz,
  data_source_desc);

-- INCREMENTAL CHANGES FROM SETTINGS_DIM - PREVIOUS SATURDAY EFFECTIVE FOR PREV SUNDAY

INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_ldg (rms_sku_num, location_num, is_replenishment_eligible_ind,
 replenishment_setting_type_code, replenishment_setting_value_desc, on_sale_date, off_sale_date, eff_begin_tmstp,eff_begin_tmstp_tz,
 data_source_desc)
(SELECT SETTING_DIM.rms_sku_num,
  SETTING_DIM.location_num,
   CASE
   WHEN ELIG_DIM.is_replenishment_eligible_ind IS NOT NULL
   THEN ELIG_DIM.is_replenishment_eligible_ind
   ELSE 'N'
   END AS is_replenishment_eligible_ind,
   CASE
   WHEN setting_dim2.replenishment_setting_type_code IS NOT NULL
   THEN setting_dim2.replenishment_setting_type_code
   ELSE 'REPLENISHMENT_METHOD'
   END AS replenishment_setting_type_code,
   CASE
   WHEN setting_dim2.replenishment_setting_value_desc IS NOT NULL
   THEN setting_dim2.replenishment_setting_value_desc
   ELSE 'NO_REPLENISHMENT'
   END AS replenishment_setting_value_desc,
   CASE
   WHEN osos_dim.rms_sku_num IS NOT NULL
   THEN CAST(RANGE_START(osos_dim.eff_period) AS DATE)
   ELSE DATE '3999-12-31'
   END AS on_sale_date,
   CASE
   WHEN osos_dim.rms_sku_num IS NOT NULL
   THEN CAST(RANGE_END(osos_dim.eff_period) AS DATE)
   ELSE DATE '3999-12-30'
   END AS off_sale_date,
  CAST(DAY_CAL.prev_sun_day_date AS TIMESTAMP) AS eff_begin_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(DAY_CAL.prev_sun_day_date as string)) AS eff_begin_tmstp_tz,
  'SETTINGS_DIM_PREV' AS data_source_desc
 FROM (SELECT rms_sku_num,
    location_num,
    replenishment_setting_type_code,
    replenishment_setting_value_desc,
    eff_begin_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim
   WHERE (rms_sku_num, location_num) IN (SELECT (rms_sku_num, location_num)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_key_ldg
      GROUP BY rms_sku_num,
       location_num)
    AND LOWER(replenishment_setting_type_code) = LOWER('REPLENISHMENT_METHOD')) AS SETTING_DIM
  INNER JOIN (SELECT a.day_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(b.day_date AS DATETIME)) AS DATETIME) AS prev_sat_day_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(DATE_ADD(b.day_date, INTERVAL 1 DAY) AS DATETIME)) AS DATETIME) AS
    prev_sun_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.week_idnt > b.week_idnt
   WHERE b.day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY a.day_date ORDER BY b.week_idnt DESC, b.day_date DESC)) = 1) AS DAY_CAL ON
   DAY_CAL.day_date = CAST(SETTING_DIM.eff_begin_tmstp AS DATE)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS setting_dim2 
  ON LOWER(SETTING_DIM.rms_sku_num) = LOWER(setting_dim2.rms_sku_num) 
      AND SETTING_DIM.location_num = setting_dim2.location_num 
      AND LOWER(SETTING_DIM.replenishment_setting_type_code) = LOWER(setting_dim2.replenishment_setting_type_code)
      AND RANGE_CONTAINS(RANGE(SETTING_DIM2.EFF_BEGIN_TMSTP ,SETTING_DIM2.EFF_END_TMSTP),CAST(DAY_CAL.PREV_SAT_DAY_DATE AS TIMESTAMP))

  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_timeline_normalize_vw AS osos_dim 
  ON LOWER(osos_dim.rms_sku_num) = LOWER(SETTING_DIM.rms_sku_num) 
  AND SETTING_DIM.location_num = osos_dim.location_num 
  AND LOWER(osos_dim.is_on_sale_ind) = LOWER('Y')
  AND RANGE_CONTAINS(OSOS_DIM.eff_period, CAST(DAY_CAL.PREV_SUN_DAY_DATE AS TIMESTAMP))

  LEFT JOIN
   (SELECT b0.rms_sku_num,
    b0.channel_brand,
    b0.is_replenishment_eligible_ind,
    b0.eff_begin_tmstp,
    b0.eff_end_tmstp,
    b0.dw_batch_id,
    b0.dw_batch_date,
    b0.dw_sys_load_tmstp,
    a0.store_num AS location_num,
     CASE
     WHEN a0.channel_num IN (110, 120)
     THEN 'NORDSTROM'
     WHEN a0.channel_num IN (210, 250)
     THEN 'NORDSTROM_RACK'
     ELSE NULL
     END AS drvd_channel_brand
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim AS a0
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim AS b0 ON LOWER(CASE
       WHEN a0.channel_num IN (110, 120)
       THEN 'NORDSTROM'
       WHEN a0.channel_num IN (210, 250)
       THEN 'NORDSTROM_RACK'
       ELSE NULL
       END) = LOWER(b0.channel_brand)
   WHERE a0.channel_num IN (110, 120, 210, 250)
    AND a0.store_close_date IS NULL
    AND LOWER(a0.store_country_code) <> LOWER('CA')
    AND LOWER(a0.selling_store_ind) = LOWER('S')
    AND LOWER(a0.store_type_code) NOT IN (LOWER('RS'), LOWER('RR'))) AS ELIG_DIM 
    ON LOWER(SETTING_DIM.rms_sku_num) = LOWER(ELIG_DIM.rms_sku_num) 
    AND SETTING_DIM.location_num = ELIG_DIM.location_num
    AND RANGE_CONTAINS(RANGE(ELIG_DIM.EFF_BEGIN_TMSTP ,ELIG_DIM.EFF_END_TMSTP), CAST(DAY_CAL.PREV_SUN_DAY_DATE AS TIMESTAMP))

 GROUP BY SETTING_DIM.rms_sku_num,
  SETTING_DIM.location_num,
  is_replenishment_eligible_ind,
  replenishment_setting_type_code,
  replenishment_setting_value_desc,
  on_sale_date,
  off_sale_date,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  data_source_desc);

-- INCREMENTAL CHANGES FROM SETTINGS_DIM - NEXT SUNDAY
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_ldg 
(rms_sku_num, location_num, is_replenishment_eligible_ind,
 replenishment_setting_type_code, replenishment_setting_value_desc,
  on_sale_date, off_sale_date, eff_begin_tmstp,eff_begin_tmstp_tz,
 data_source_desc)
(SELECT SETTING_DIM.rms_sku_num,
  SETTING_DIM.location_num,
   CASE
   WHEN ELIG_DIM.is_replenishment_eligible_ind IS NOT NULL
   THEN ELIG_DIM.is_replenishment_eligible_ind
   ELSE 'N'
   END AS is_replenishment_eligible_ind,
   CASE
   WHEN setting_dim2.replenishment_setting_type_code IS NOT NULL
   THEN setting_dim2.replenishment_setting_type_code
   ELSE 'REPLENISHMENT_METHOD'
   END AS replenishment_setting_type_code,
   CASE
   WHEN setting_dim2.replenishment_setting_value_desc IS NOT NULL
   THEN setting_dim2.replenishment_setting_value_desc
   ELSE 'NO_REPLENISHMENT'
   END AS replenishment_setting_value_desc,
   CASE
   WHEN osos_dim.rms_sku_num IS NOT NULL
   THEN CAST(RANGE_START(osos_dim.eff_period) AS DATE)
   ELSE DATE '3999-12-31'
   END AS on_sale_date,
   CASE
   WHEN osos_dim.rms_sku_num IS NOT NULL
   THEN CAST(RANGE_END(osos_dim.eff_period) AS DATE)
   ELSE DATE '3999-12-30'
   END AS off_sale_date,
  CAST(DAY_CAL.next_sun_day_date AS TIMESTAMP) AS eff_begin_tmstp,
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(DAY_CAL.next_sun_day_date as string)) AS eff_begin_tmstp_tz,
  'SETTINGS_DIM_NEXT' AS data_source_desc
 FROM (SELECT rms_sku_num,
    location_num,
    replenishment_setting_type_code,
    replenishment_setting_value_desc,
    eff_begin_tmstp
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim
   WHERE (rms_sku_num, location_num) IN (SELECT (rms_sku_num, location_num)
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.rp_indicator_dtl_key_ldg
      GROUP BY rms_sku_num,
       location_num)
    AND LOWER(replenishment_setting_type_code) = LOWER('REPLENISHMENT_METHOD')) AS SETTING_DIM
  INNER JOIN (SELECT DATE_SUB(a.day_date, INTERVAL 7 DAY) AS day_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(b.day_date AS DATETIME)) AS DATETIME) AS next_sat_day_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CAST(DATE_ADD(b.day_date, INTERVAL 1 DAY) AS DATETIME)) AS DATETIME) AS
    next_sun_day_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.week_idnt > b.week_idnt
   WHERE b.day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY a.day_date ORDER BY b.week_idnt DESC, b.day_date DESC)) = 1) AS DAY_CAL ON
   DAY_CAL.day_date = CAST(SETTING_DIM.eff_begin_tmstp AS DATE)

  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_setting_dtl_dim AS setting_dim2 
  ON LOWER(SETTING_DIM.rms_sku_num) = LOWER(setting_dim2.rms_sku_num)
   AND SETTING_DIM.location_num = setting_dim2.location_num 
      AND LOWER(SETTING_DIM.replenishment_setting_type_code)= LOWER(setting_dim2.replenishment_setting_type_code)
      AND RANGE_CONTAINS(RANGE(SETTING_DIM2.EFF_BEGIN_TMSTP ,SETTING_DIM2.EFF_END_TMSTP),CAST( DAY_CAL.NEXT_SAT_DAY_DATE AS TIMESTAMP))

  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.rp_sku_loc_timeline_normalize_vw AS osos_dim 
  ON LOWER(osos_dim.rms_sku_num) = LOWER(SETTING_DIM.rms_sku_num) 
      AND SETTING_DIM.location_num = osos_dim.location_num 
      AND LOWER(osos_dim.is_on_sale_ind) = LOWER('Y')
      AND RANGE_CONTAINS(OSOS_DIM.eff_period, CAST(DAY_CAL.NEXT_SUN_DAY_DATE AS TIMESTAMP))

  LEFT JOIN (SELECT b0.rms_sku_num,
    b0.channel_brand,
    b0.is_replenishment_eligible_ind,
    b0.eff_begin_tmstp,
    b0.eff_end_tmstp,
    b0.dw_batch_id,
    b0.dw_batch_date,
    b0.dw_sys_load_tmstp,
    a0.store_num AS location_num,
     CASE
     WHEN a0.channel_num IN (110, 120)
     THEN 'NORDSTROM'
     WHEN a0.channel_num IN (210, 250)
     THEN 'NORDSTROM_RACK'
     ELSE NULL
     END AS drvd_channel_brand
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.store_dim AS a0
    INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.rp_item_eligibility_dim AS b0 ON LOWER(CASE
       WHEN a0.channel_num IN (110, 120)
       THEN 'NORDSTROM'
       WHEN a0.channel_num IN (210, 250)
       THEN 'NORDSTROM_RACK'
       ELSE NULL
       END) = LOWER(b0.channel_brand)
   WHERE a0.channel_num IN (110, 120, 210, 250)
    AND a0.store_close_date IS NULL
    AND LOWER(a0.store_country_code) <> LOWER('CA')
    AND LOWER(a0.selling_store_ind) = LOWER('S')
    AND LOWER(a0.store_type_code) NOT IN (LOWER('RS'), LOWER('RR'))) AS ELIG_DIM 
    ON LOWER(SETTING_DIM.rms_sku_num) =    LOWER(ELIG_DIM.rms_sku_num) 
    AND SETTING_DIM.location_num = ELIG_DIM.location_num
    AND RANGE_CONTAINS(RANGE(ELIG_DIM.EFF_BEGIN_TMSTP ,ELIG_DIM.EFF_END_TMSTP), CAST(DAY_CAL.NEXT_SUN_DAY_DATE  AS TIMESTAMP))
 GROUP BY SETTING_DIM.rms_sku_num,
  SETTING_DIM.location_num,
  is_replenishment_eligible_ind,
  replenishment_setting_type_code,
  replenishment_setting_value_desc,
  on_sale_date,
  off_sale_date,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  data_source_desc);