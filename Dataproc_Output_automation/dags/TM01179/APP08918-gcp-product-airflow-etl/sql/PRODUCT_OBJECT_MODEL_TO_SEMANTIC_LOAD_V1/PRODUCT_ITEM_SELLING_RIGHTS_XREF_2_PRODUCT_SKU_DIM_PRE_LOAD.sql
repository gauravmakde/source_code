----main pre load query 


--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;

-- NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows


TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw;

--.IF ERRORCODE <> 0 THEN .QUIT 1

TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg;

--.IF ERRORCODE <> 0 THEN .QUIT 2

-- furture records

-- Past records

---- super future

INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg (rms_sku_num, channel_country, selling_status_code, live_date,
 selling_channel_eligibility_list, eff_begin_tmstp, eff_begin_tmstp_tz, eff_end_tmstp, eff_end_tmstp_tz, src_desc)
(
  SELECT rms_sku_num,
  channel_country,
  selling_status_code,
  live_date,
  selling_channel_eligibility_list,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  src_desc
 FROM (
  SELECT rms_sku_num,
     channel_country,
     selling_status_code,
     live_date,
     selling_channel_eligibility_list,
     eff_begin_tmstp,
     eff_begin_tmstp_tz,
     eff_end_tmstp,
     eff_end_tmstp_tz,
     'current_rec' AS src_desc
    FROM (
      SELECT *
      FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
      WHERE eff_begin_tmstp BETWEEN (SELECT extract_from_tmstp
         FROM {{params.dataplex_project_id}}.{{params.dbenv}}_nap_utl.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')) AND (SELECT extract_to_tmstp
         FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS'))
      QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp DESC)) = 1) AS
     XREF1
    WHERE selling_channel_eligibility_list IS NOT NULL

    UNION ALL

    SELECT xref1.rms_sku_num,
     xref1.channel_country,
     xref2.selling_status_code,
     xref2.live_date,
     xref2.selling_channel_eligibility_list,
     xref1.eff_begin_tmstp,
     xref1.eff_begin_tmstp_tz,
     xref2.eff_end_tmstp,
     xref2.eff_end_tmstp_tz,
     'future_effective' AS src_desc
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1
     LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref2 ON LOWER(xref1.rms_sku_num) = LOWER(xref2.rms_sku_num
        ) AND LOWER(xref1.channel_country) = LOWER(xref2.channel_country)
    WHERE xref1.eff_begin_tmstp BETWEEN (SELECT extract_from_tmstp
       FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')) AND (SELECT extract_to_tmstp
       FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS'))
     AND xref1.selling_channel_eligibility_list IS NULL
     AND xref2.eff_begin_tmstp > xref1.eff_begin_tmstp
     AND (xref1.rms_sku_num, xref1.channel_country) NOT IN (SELECT (rms_sku_num, channel_country)
       FROM (SELECT *
         FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref
         WHERE eff_begin_tmstp BETWEEN (SELECT extract_from_tmstp
            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')) AND (SELECT extract_to_tmstp
            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS'))
         QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp DESC)) = 1) AS
        XREF
       WHERE selling_channel_eligibility_list IS NOT NULL
       GROUP BY rms_sku_num,
        channel_country)
     AND xref2.selling_channel_eligibility_list IS NOT NULL
     AND xref2.rms_sku_num IS NOT NULL
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY xref2.rms_sku_num, xref2.channel_country ORDER BY xref2.eff_begin_tmstp)) =
     1
    UNION ALL
    SELECT xref10.rms_sku_num,
     xref10.channel_country,
     xref10.selling_status_code,
     xref20.live_date,
     'No Eligible Channels' AS selling_channel_eligibility_list,
     xref20.eff_end_tmstp AS eff_begin_tmstp,
     xref20.eff_end_tmstp_tz as eff_begin_tmstp_tz,
     xref10.eff_end_tmstp,
     xref10.eff_end_tmstp_tz,
     'no_eligible' AS src_desc
    FROM {{params.dataplex_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref10
     LEFT JOIN {{params.dataplex_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref20 ON LOWER(xref10.rms_sku_num) = LOWER(xref20.rms_sku_num
        ) AND LOWER(xref10.channel_country) = LOWER(xref20.channel_country)
    WHERE xref10.eff_begin_tmstp BETWEEN (SELECT extract_from_tmstp
       FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')) AND (SELECT extract_to_tmstp
       FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
       WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS'))
     AND xref10.selling_channel_eligibility_list IS NULL
     AND xref20.eff_begin_tmstp < xref10.eff_begin_tmstp
     AND (xref10.rms_sku_num, xref10.channel_country) NOT IN (SELECT (xref11.rms_sku_num, xref11.channel_country)
       FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref11
        LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref21 ON LOWER(xref11.rms_sku_num) = LOWER(xref21.rms_sku_num
           ) AND LOWER(xref11.channel_country) = LOWER(xref21.channel_country)
       WHERE xref11.eff_begin_tmstp BETWEEN (SELECT extract_from_tmstp
          FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
          WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')) AND (SELECT extract_to_tmstp
          FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
          WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS'))
        AND xref11.selling_channel_eligibility_list IS NULL
        AND xref21.eff_begin_tmstp > xref11.eff_begin_tmstp
        AND xref21.selling_channel_eligibility_list IS NOT NULL
        AND xref11.rms_sku_num IS NOT NULL
       GROUP BY xref11.rms_sku_num,
        xref11.channel_country)
     AND xref20.selling_channel_eligibility_list IS NOT NULL
     AND xref10.rms_sku_num IS NOT NULL
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY xref20.rms_sku_num, xref20.channel_country ORDER BY xref20.eff_begin_tmstp
         DESC)) = 1
    UNION ALL
    SELECT xref12.rms_sku_num,
     xref12.channel_country,
     xref12.selling_status_code,
     xref12.live_date,
     xref12.selling_channel_eligibility_list,
     cast(xref12.dw_sys_load_tmstp as timestamp) AS eff_begin_tmstp,
     `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(xref12.dw_sys_load_tmstp as string)) as eff_begin_tmstp_tz,
     xref12.eff_end_tmstp,
     xref12.eff_end_tmstp_tz,
     'default_future' AS src_desc
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref12
     INNER JOIN (SELECT rms_sku_num,
       channel_country,
       MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
        UNBOUNDED FOLLOWING) AS min_eff_begin_tmstp,
       MAX(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
        UNBOUNDED FOLLOWING) AS max_eff_begin_tmstp
      FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1
      WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
         FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
         WHERE dw_batch_date BETWEEN (SELECT CAST(extract_from_tmstp AS DATE) AS A319933093
            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')) AND (SELECT CAST(extract_to_tmstp AS DATE)
             AS A261580300
            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS'))
         GROUP BY rms_sku_num,
          channel_country)
      QUALIFY (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING)) = (MAX(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country
          RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AND (MIN(eff_begin_tmstp) OVER (PARTITION BY
            rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) > (SELECT
          extract_to_tmstp
         FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS'))) AS XREF3 ON LOWER(xref12.rms_sku_num) =
       LOWER(XREF3.rms_sku_num) AND LOWER(xref12.channel_country) = LOWER(XREF3.channel_country)) AS CONSOLIDATED);


--.IF ERRORCODE <> 0 THEN .QUIT 3


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg (rms_sku_num, channel_country, selling_status_code, live_date,
 selling_channel_eligibility_list, eff_begin_tmstp, eff_begin_tmstp_tz, eff_end_tmstp, eff_end_tmstp_tz, src_desc)
(SELECT xref1.rms_sku_num,
  xref1.channel_country,
  xref1.selling_status_code,
  xref1.live_date,
  xref1.selling_channel_eligibility_list,
  CAST(xref1.dw_sys_load_tmstp AS TIMESTAMP) AS eff_begin_tmstp,
  `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(xref1.dw_sys_load_tmstp as string)) as eff_begin_tmstp_tz,
  xref1.eff_end_tmstp,
  xref1.eff_end_tmstp_tz,
  'default_future_multi_change' AS src_desc
 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1
  INNER JOIN (SELECT rms_sku_num,
    channel_country,
    selling_channel_eligibility_list,
    MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
     UNBOUNDED FOLLOWING) AS min_eff_begin_tmstp,
    MAX(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
     UNBOUNDED FOLLOWING) AS max_eff_begin_tmstp,
    eff_begin_tmstp
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1
   WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
      FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
      WHERE dw_batch_date BETWEEN (SELECT CAST(extract_from_tmstp AS DATE) AS A319933093
         FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS')) AND (SELECT CAST(extract_to_tmstp AS DATE)
          AS A261580300
         FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_utl.elt_control
         WHERE LOWER(subject_area_nm) = LOWER('PRODUCT_ITEM_SELLING_RIGHTS'))
      GROUP BY rms_sku_num,channel_country)
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp)) = 1) AS t12 ON LOWER(xref1
      .rms_sku_num) = LOWER(t12.rms_sku_num) AND LOWER(xref1.channel_country) = LOWER(t12.channel_country) AND xref1.eff_begin_tmstp
     = t12.eff_begin_tmstp
 WHERE (xref1.rms_sku_num, xref1.channel_country) NOT IN (SELECT (rms_sku_num, channel_country)
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg
    GROUP BY rms_sku_num,
     channel_country));

-- select * from prd_nap_dim.product_item_selling_rights_xref

--.IF ERRORCODE <> 0 THEN .QUIT 4


TRUNCATE TABLE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_selling_rights_tmp_stg;


--.IF ERRORCODE <> 0 THEN .QUIT 5



INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_selling_rights_tmp_stg (rms_sku_num, channel_country, sr_eff_begin_tmstp, sr_eff_begin_tmstp_tz,
 sr_eff_end_tmstp, sr_eff_end_tmstp_tz, nps_eff_begin_tmstp, nps_eff_begin_tmstp_tz, dw_sys_load_tmstp,dw_sys_load_tmstp_tz)
(SELECT selling_rights_delta.rms_sku_num,
  selling_rights_delta.channel_country,
  selling_rights_delta.eff_begin_tmstp AS sr_eff_begin_tmstp,
  selling_rights_delta.eff_begin_tmstp_tz AS sr_eff_begin_tmstp_tz,
  selling_rights_delta.eff_end_tmstp AS sr_eff_end_tmstp,
  selling_rights_delta.eff_end_tmstp_tz AS sr_eff_end_tmstp_tz,
  CAST(NPS_SKU_DELTA.eff_begin_tmstp AS TIMESTAMP) AS nps_eff_begin_tmstp,
  `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(NPS_SKU_DELTA.eff_begin_tmstp as string)) AS nps_eff_begin_tmstp_tz,
  CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) AS   dw_sys_load_tmstp,
  `{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS TIMESTAMP) as string)) AS dw_sys_load_tmstp
 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg AS selling_rights_delta
  INNER JOIN (SELECT legacyrmsskuid AS rms_sku_num,
    marketcode AS channel_country,
    returndispositioncode,
   `{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(sourcepublishtimestamp) eff_begin_tmstp
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_ldg
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY legacyrmsskuid, marketcode ORDER BY cast(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`(sourcepublishtimestamp) as timestamp) DESC)) = 1) AS NPS_SKU_DELTA ON LOWER(selling_rights_delta.rms_sku_num) = LOWER(NPS_SKU_DELTA.rms_sku_num)
   AND LOWER(selling_rights_delta.channel_country) = LOWER(NPS_SKU_DELTA.channel_country)
 WHERE selling_rights_delta.eff_begin_tmstp > CAST(NPS_SKU_DELTA.eff_begin_tmstp AS TIMESTAMP));


--.IF ERRORCODE <> 0 THEN .QUIT 6

--COLLECT STATISTICS     column ( rms_sku_num ),     column ( channel_country ),     column ( sr_eff_begin_tmstp ),     column ( nps_eff_begin_tmstp ) ON {{params.dataplex_project_id}}.{{params.dbenv}}_nap_stg.PRODUCT_SKU_SELLING_RIGHTS_TMP_STG


--.IF ERRORCODE <> 0 THEN .QUIT 7




INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw (rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc,
 web_sku_num, brand_label_num, brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num,
 partner_relationship_type_code, web_style_num, style_desc, epm_choice_num, supp_part_num, prmy_supp_num,
 manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num,
 div_desc, cmpy_num, cmpy_desc, color_num, color_desc, nord_display_color, nrf_size_code, size_1_num, size_1_desc,
 size_2_num, size_2_desc, supp_color, supp_size, brand_name, return_disposition_code, return_disposition_desc,
 selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, sku_type_code, sku_type_desc,
 hazardous_material_class_desc, hazardous_material_class_code, fulfillment_type_code, selling_channel_eligibility_list,
 smart_sample_ind, gwp_ind, msrp_amt, msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind,
 fp_item_planning_eligible_ind, fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind,
 op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code, 
 eff_begin_tmstp,eff_begin_tmstp_tz,
  eff_end_tmstp, eff_end_tmstp_tz
)

---with src1
--first normalize 
WITH SRC_1 AS (
    SELECT 
        rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, web_sku_num, 
        brand_label_num, brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num, 
        partner_relationship_type_code, web_style_num, epm_choice_num, supp_part_num, prmy_supp_num, 
        manufacturer_num, nrf_size_code, size_1_num, size_1_desc, size_2_num, size_2_desc, supp_color, 
        supp_size, return_disposition_code, return_disposition_desc, selling_status_code, selling_status_desc, 
        live_date, drop_ship_eligible_ind, hazardous_material_class_desc, hazardous_material_class_code, 
        fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, 
        msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind, 
        fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, 
        op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code, eff_begin_tmstp, eff_end_tmstp
    FROM (
        -- First Normalize
        SELECT 
            rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, web_sku_num, 
            brand_label_num, brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num, 
            partner_relationship_type_code, web_style_num, epm_choice_num, supp_part_num, prmy_supp_num, 
            manufacturer_num, nrf_size_code, size_1_num, size_1_desc, size_2_num, size_2_desc, supp_color, 
            supp_size, return_disposition_code, return_disposition_desc, selling_status_code, selling_status_desc, 
            live_date, drop_ship_eligible_ind, hazardous_material_class_desc, hazardous_material_class_code, 
            fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, 
            msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind, 
            fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, 
            op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code, 
            MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
        FROM (
            SELECT *, 
                SUM(discontinuity_flag) OVER (
                    PARTITION BY rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, web_sku_num, 
                    brand_label_num, brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num, 
                    partner_relationship_type_code, web_style_num, epm_choice_num, supp_part_num, prmy_supp_num, 
                    manufacturer_num, nrf_size_code, size_1_num, size_1_desc, size_2_num, size_2_desc, supp_color, 
                    supp_size, return_disposition_code, return_disposition_desc, selling_status_code, selling_status_desc, 
                    live_date, drop_ship_eligible_ind, hazardous_material_class_desc, hazardous_material_class_code, 
                    fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, 
                    msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind, 
                    fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, 
                    op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code 
                    ORDER BY eff_begin_tmstp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp) OVER (
                            PARTITION BY rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, web_sku_num, 
                            brand_label_num, brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num, 
                            partner_relationship_type_code, web_style_num, epm_choice_num, supp_part_num, prmy_supp_num, 
                            manufacturer_num, nrf_size_code, size_1_num, size_1_desc, size_2_num, size_2_desc, supp_color, 
                            supp_size, return_disposition_code, return_disposition_desc, selling_status_code, selling_status_desc, 
                            live_date, drop_ship_eligible_ind, hazardous_material_class_desc, hazardous_material_class_code, 
                            fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, 
                            msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind, 
                            fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, 
                            op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code 
                            ORDER BY eff_begin_tmstp
                        ) >= DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY)
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM (
                    -- Main Query
                    SELECT DISTINCT 
                        rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, web_sku_num, brand_label_num, 
                        brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num, partner_relationship_type_code, 
                        web_style_num, epm_choice_num, supp_part_num, prmy_supp_num, manufacturer_num, nrf_size_code, size_1_num, 
                        size_1_desc, size_2_num, size_2_desc, supp_color, supp_size, return_disposition_code, return_disposition_desc, 
                        selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, hazardous_material_class_desc, 
                        hazardous_material_class_code, fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, 
                        gwp_ind, msrp_amt, msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, 
                        fp_item_planning_eligible_ind, fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, 
                        op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code, RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period,eff_begin_tmstp, eff_end_tmstp
                    FROM (
                        SELECT SRC_2.*, 
                            COALESCE(MAX(eff_begin_tmstp) OVER(
                                PARTITION BY rms_sku_num, channel_country
                                ORDER BY eff_begin_tmstp
                                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                            ), TIMESTAMP '9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
                        FROM (
                            SELECT DISTINCT 
                                sku.rms_sku_num, sku.epm_sku_num, sku.channel_country, sku.sku_short_desc, sku.sku_desc, sku.web_sku_num, 
                                sku.brand_label_num, sku.brand_label_display_name, sku.rms_style_num, sku.epm_style_num, sku.partner_relationship_num, 
                                sku.partner_relationship_type_code, sku.web_style_num, sku.epm_choice_num, sku.supp_part_num, sku.prmy_supp_num, 
                                sku.manufacturer_num, sku.nrf_size_code, sku.size_1_num, sku.size_1_desc, sku.size_2_num, sku.size_2_desc, 
                                sku.supp_color, sku.supp_size, sku.return_disposition_code, sku.return_disposition_desc, sku.drop_ship_eligible_ind, 
                                sku.hazardous_material_class_desc, sku.hazardous_material_class_code, sku.fulfillment_type_code, 
                                sku.smart_sample_ind, sku.gwp_ind, sku.msrp_amt, sku.msrp_currency_code, sku.npg_ind, sku.order_quantity_multiple, 
                                sku.fp_forecast_eligible_ind, sku.fp_item_planning_eligible_ind, sku.fp_replenishment_eligible_ind, 
                                sku.op_forecast_eligible_ind, sku.op_item_planning_eligible_ind, sku.op_replenishment_eligible_ind, 
                                sku.size_range_desc, sku.size_sequence_num, sku.size_range_code, 
                                CASE 
                                    WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED') THEN 'S1'
                                    WHEN LOWER(sku.fulfillment_type_code) IN (
                                        LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')
                                    ) THEN 'T1'
                                    WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED') THEN 'C3'
                                    ELSE NULL
                                END AS derived_selling_status_code, 
                                CASE 
                                    WHEN LOWER(CASE
                                        WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED') THEN 'S1'
                                        WHEN LOWER(sku.fulfillment_type_code) IN (
                                            LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')
                                        ) THEN 'T1'
                                        WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED') THEN 'C3'
                                        ELSE NULL
                                    END) = LOWER('S1') THEN 'Unsellable'
                                    WHEN LOWER(CASE
                                        WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED') THEN 'S1'
                                        WHEN LOWER(sku.fulfillment_type_code) IN (
                                            LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')
                                        ) THEN 'T1'
                                        WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED') THEN 'C3'
                                        ELSE NULL
                                    END) = LOWER('C3') THEN 'Sellable'
                                    WHEN LOWER(CASE
                                        WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED') THEN 'S1'
                                        WHEN LOWER(sku.fulfillment_type_code) IN (
                                            LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')
                                        ) THEN 'T1'
                                        WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED') THEN 'C3'
                                        ELSE NULL
                                    END) = LOWER('T1') THEN 'Dropship'
                                    ELSE NULL
                                END AS selling_status_desc,
                                CASE 
                                    WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED') THEN 'S1'
                                    WHEN LOWER(sku.fulfillment_type_code) IN (
                                        LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')
                                    ) THEN 'T1'
                                    WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED') THEN 'C3'
                                    ELSE NULL
                                END AS selling_status_code, xref.live_date, xref.selling_channel_eligibility_list, xref.eff_begin_tmstp
                            FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg AS xref
                            INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS sku 
                                ON LOWER(xref.rms_sku_num) = LOWER(sku.rms_sku_num) 
                                AND LOWER(xref.channel_country) = LOWER(sku.channel_country)
                            LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_selling_rights_tmp_stg AS delta_check 
                                ON LOWER(xref.rms_sku_num) = LOWER(delta_check.rms_sku_num) 
                                AND LOWER(xref.channel_country) = LOWER(delta_check.channel_country)
                                AND xref.eff_begin_tmstp = delta_check.sr_eff_begin_tmstp 
                                AND xref.eff_end_tmstp = delta_check.sr_eff_end_tmstp
                            WHERE RANGE_OVERLAPS(
                                RANGE(SKU.eff_begin_tmstp, SKU.eff_end_tmstp), 
                                RANGE(XREF.eff_begin_tmstp, XREF.eff_end_tmstp)
                            )
                            AND delta_check.rms_sku_num IS NULL
                        ) AS SRC_2
                        QUALIFY eff_begin_tmstp < COALESCE(
                            MAX(eff_begin_tmstp) OVER(
                                PARTITION BY rms_sku_num, channel_country 
                                ORDER BY eff_begin_tmstp
                                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                            ), TIMESTAMP '9999-12-31 23:59:59.999999+00:00'
                        ))  SRC_1)
                    ) AS ordered_data
                ) AS grouped_data
            GROUP BY 
                rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, web_sku_num, brand_label_num, 
                brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num, partner_relationship_type_code, 
                web_style_num, epm_choice_num, supp_part_num, prmy_supp_num, manufacturer_num, nrf_size_code, size_1_num, 
                size_1_desc, size_2_num, size_2_desc, supp_color, supp_size, return_disposition_code, return_disposition_desc, 
                selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, hazardous_material_class_desc, 
                hazardous_material_class_code, fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, 
                gwp_ind, msrp_amt, msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, 
                fp_item_planning_eligible_ind, fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, 
                op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code
            ORDER BY 
                rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, web_sku_num, brand_label_num, 
                brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num, partner_relationship_type_code, 
                web_style_num, epm_choice_num, supp_part_num, prmy_supp_num, manufacturer_num, nrf_size_code, size_1_num, 
                size_1_desc, size_2_num, size_2_desc, supp_color, supp_size, return_disposition_code, return_disposition_desc, 
                selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, hazardous_material_class_desc, 
                hazardous_material_class_code, fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, 
                gwp_ind, msrp_amt, msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, 
                fp_item_planning_eligible_ind, fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, 
                op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code, eff_begin_tmstp
    )
),


---with src3
--second level normalize

 SRC_3 as (

 SELECT rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num, sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num, div_desc,cmpy_num, cmpy_desc, color_num,color_desc, nord_display_color, nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, eff_begin_tmstp, eff_end_tmstp
    FROM (
        -- second Normalize
        SELECT rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num, sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num, div_desc,cmpy_num, cmpy_desc, color_num,color_desc, nord_display_color, nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, MIN(eff_begin_tmstp_utc) AS eff_begin_tmstp, MAX(eff_end_tmstp_utc) AS eff_end_tmstp
        FROM (
            SELECT *, 
                SUM(discontinuity_flag) OVER (
                    PARTITION BY  rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num, sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num, div_desc,cmpy_num, cmpy_desc, color_num,color_desc, nord_display_color, nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code
                    ORDER BY eff_begin_tmstp_utc 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS range_group
            FROM (
                SELECT *,
                    CASE 
                        WHEN LAG(eff_end_tmstp_utc) OVER (
                            PARTITION BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num, sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num, div_desc,cmpy_num, cmpy_desc, color_num,color_desc, nord_display_color, nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code
                            ORDER BY eff_begin_tmstp_utc
                        ) >= DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY)
                        THEN 0
                        ELSE 1
                    END AS discontinuity_flag
                FROM

  (
    
  SELECT SRC0.rms_sku_num,SRC0.epm_sku_num,SRC0.channel_country,SRC0.sku_short_desc,SRC0.sku_desc,SRC0.web_sku_num,SRC0.brand_label_num,SRC0.brand_label_display_name,SRC0.rms_style_num,SRC0.epm_style_num,SRC0.partner_relationship_num,SRC0.partner_relationship_type_code,
    SRC0.web_style_num,styl.style_desc,SRC0.epm_choice_num,SRC0.supp_part_num,SRC0.prmy_supp_num,SRC0.manufacturer_num,styl.sbclass_num, styl.sbclass_desc,styl.class_num,styl.class_desc,styl.dept_num,styl.dept_desc,dept.subdivision_num AS grp_num,dept.subdivision_short_name AS grp_desc,dept.division_num AS div_num,dept.division_short_name AS div_desc,'1000' AS cmpy_num, 'Nordstrom' AS cmpy_desc, choice.nrf_color_num AS color_num,choice.color_desc, choice.nord_display_color, SRC0.nrf_size_code,SRC0.size_1_num,SRC0.size_1_desc,SRC0.size_2_num,SRC0.size_2_desc,SRC0.supp_color,SRC0.supp_size,styl.vendor_label_name AS brand_name,SRC0.return_disposition_code,SRC0.return_disposition_desc,SRC0.selling_status_code,SRC0.selling_status_desc,SRC0.live_date,SRC0.drop_ship_eligible_ind,
    'f' AS sku_type_code,'Fashion Sku' AS sku_type_desc,SRC0.hazardous_material_class_desc,SRC0.hazardous_material_class_code,SRC0.fulfillment_type_code,SRC0.selling_channel_eligibility_list,SRC0.smart_sample_ind,SRC0.gwp_ind,SRC0.msrp_amt,SRC0.msrp_currency_code,
    SRC0.npg_ind,SRC0.order_quantity_multiple,SRC0.fp_forecast_eligible_ind,SRC0.fp_item_planning_eligible_ind,SRC0.fp_replenishment_eligible_ind,SRC0.op_forecast_eligible_ind,SRC0.op_item_planning_eligible_ind,SRC0.op_replenishment_eligible_ind,SRC0.size_range_desc,
    SRC0.size_sequence_num,SRC0.size_range_code, 
    COALESCE(SAFE.RANGE_INTERSECT(SAFE.RANGE_INTERSECT(CASE WHEN styl.epm_style_num>0 THEN ( SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(styl.eff_begin_tmstp_utc,styl.eff_end_tmstp_utc))) ELSE SRC0.eff_period END
	     ,CASE WHEN dept.dept_num>0 THEN (SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(dept.eff_begin_tmstp_utc,dept.eff_end_tmstp_utc))) ELSE SRC0.eff_period END
	     ),
		 CASE WHEN choice.epm_choice_num>0 THEN (SAFE.RANGE_INTERSECT(SRC0.eff_period , RANGE(choice.eff_begin_tmstp_utc,choice.eff_end_tmstp_utc))) ELSE SRC0.eff_period END
	     )
	          , SRC0.eff_period ) AS eff_period,
             CASE WHEN styl.epm_style_num>0 THEN styl.eff_begin_tmstp_utc
             when dept.dept_num>0 THEN dept.eff_begin_tmstp_utc
             when choice.epm_choice_num>0 THEN choice.eff_begin_tmstp_utc end as eff_begin_tmstp_utc,

             CASE WHEN styl.epm_style_num>0 THEN styl.eff_end_tmstp_utc
             when dept.dept_num>0 THEN dept.eff_end_tmstp_utc
             when choice.epm_choice_num>0 THEN choice.eff_end_tmstp_utc end as eff_end_tmstp_utc,


   FROM (
      --select from cte SRC_3
      select *, RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period  from SRC_1  ) 
                
                AS SRC0
    LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist AS styl ON SRC0.epm_style_num = styl.epm_style_num AND LOWER(SRC0
       .channel_country) = LOWER(styl.channel_country)
       AND RANGE_OVERLAPS(src0.eff_period , RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc))

    LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim_hist AS dept ON dept.dept_num = CAST(TRIM(FORMAT('%11d', styl.dept_num)) AS FLOAT64)
       AND (styl.eff_begin_tmstp > dept.eff_begin_tmstp AND styl.eff_begin_tmstp < dept.eff_end_tmstp OR styl.eff_begin_tmstp
           < dept.eff_begin_tmstp AND styl.eff_end_tmstp > dept.eff_begin_tmstp OR styl.eff_begin_tmstp = dept.eff_begin_tmstp
          AND RANGE_OVERLAPS(src0.eff_period , RANGE(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc))
          AND (styl.eff_end_tmstp = dept.eff_end_tmstp OR styl.eff_end_tmstp <> dept.eff_end_tmstp))
    LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_choice_dim_hist AS choice ON SRC0.epm_choice_num = choice.epm_choice_num AND
        LOWER(SRC0.channel_country) = LOWER(choice.channel_country) 
        AND RANGE_OVERLAPS(src0.eff_period , RANGE(choice.eff_begin_tmstp_utc, choice.eff_end_tmstp_utc))
AND (styl.eff_begin_tmstp > choice.eff_begin_tmstp
          AND styl.eff_begin_tmstp < choice.eff_end_tmstp OR styl.eff_begin_tmstp < choice.eff_begin_tmstp AND styl.eff_end_tmstp
            > choice.eff_begin_tmstp OR styl.eff_begin_tmstp = choice.eff_begin_tmstp AND (styl.eff_end_tmstp = choice.eff_end_tmstp
             OR styl.eff_end_tmstp <> choice.eff_end_tmstp)) AND (dept.eff_begin_tmstp > choice.eff_begin_tmstp AND dept
          .eff_begin_tmstp < choice.eff_end_tmstp OR dept.eff_begin_tmstp < choice.eff_begin_tmstp AND dept.eff_end_tmstp
           > choice.eff_begin_tmstp OR dept.eff_begin_tmstp = choice.eff_begin_tmstp AND (dept.eff_end_tmstp = choice.eff_end_tmstp
            OR dept.eff_end_tmstp <> choice.eff_end_tmstp))
  )
  ) AS ordered_data
                ) AS grouped_data
            GROUP BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num, sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num, div_desc,cmpy_num, cmpy_desc, color_num,color_desc, nord_display_color, nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, range_group
            ORDER BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num, sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num, div_desc,cmpy_num, cmpy_desc, color_num,color_desc, nord_display_color, nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, eff_begin_tmstp
    )
)
-----end second level normalize 


(
   SELECT
  nrml.rms_sku_num
, nrml.epm_sku_num
, nrml.channel_country
, nrml.sku_short_desc
, nrml.sku_desc
, nrml.web_sku_num
, nrml.brand_label_num
, nrml.brand_label_display_name
, nrml.rms_style_num
, nrml.epm_style_num
, nrml.partner_relationship_num
, nrml.partner_relationship_type_code
, nrml.web_style_num
, nrml.style_desc
, nrml.epm_choice_num
, nrml.supp_part_num
, nrml.prmy_supp_num
, nrml.manufacturer_num
, nrml.sbclass_num
, nrml.sbclass_desc
, nrml.class_num
, nrml.class_desc
, nrml.dept_num
, nrml.dept_desc
, nrml.grp_num
, nrml.grp_desc
, nrml.div_num
, nrml.div_desc
, nrml.cmpy_num
, nrml.cmpy_desc
, nrml.color_num
, nrml.color_desc
, nrml.nord_display_color
, nrml.nrf_size_code
, nrml.size_1_num
, nrml.size_1_desc
, nrml.size_2_num
, nrml.size_2_desc
, nrml.supp_color
, nrml.supp_size
, nrml.brand_name
, nrml.return_disposition_code
, nrml.return_disposition_desc
, nrml.selling_status_code
, nrml.selling_status_desc
, nrml.live_date
, nrml.drop_ship_eligible_ind
, nrml.sku_type_code
, nrml.sku_type_desc
, nrml.hazardous_material_class_desc
, nrml.hazardous_material_class_code
, nrml.fulfillment_type_code
, nrml.selling_channel_eligibility_list
, nrml.smart_sample_ind
, nrml.gwp_ind
, nrml.msrp_amt
, nrml.msrp_currency_code
, nrml.npg_ind
, nrml.order_quantity_multiple
, nrml.fp_forecast_eligible_ind
, nrml.fp_item_planning_eligible_ind
, nrml.fp_replenishment_eligible_ind
, nrml.op_forecast_eligible_ind
, nrml.op_item_planning_eligible_ind
, nrml.op_replenishment_eligible_ind
, nrml.size_range_desc
, nrml.size_sequence_num
, nrml.size_range_code
, eff_begin_tmstp_utc
,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(eff_begin_tmstp_utc as string)) as eff_begin_tmstp_tz
, eff_end_tmstp_utc
,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(eff_end_tmstp_utc as string)) as eff_end_tmstp_tz
FROM (
   --third normalize 

   SELECT rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name, return_disposition_code,return_disposition_desc, selling_status_code, selling_status_desc,live_date, drop_ship_eligible_ind, sku_type_code,sku_type_desc,hazardous_material_class_desc, hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt, msrp_currency_code,npg_ind, order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,MIN(RANGE_START(eff_period)) AS eff_begin_tmstp_utc, MAX(RANGE_END(eff_period)) AS eff_end_tmstp_utc
            FROM (
                SELECT *,
                SUM(discontinuity_flag) OVER (PARTITION BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name, return_disposition_code,return_disposition_desc, selling_status_code, selling_status_desc,live_date, drop_ship_eligible_ind, sku_type_code,sku_type_desc,hazardous_material_class_desc, hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt, msrp_currency_code,npg_ind, order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      (
         SELECT *, 
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size, brand_name, return_disposition_code,return_disposition_desc, selling_status_code, selling_status_desc,live_date, drop_ship_eligible_ind, sku_type_code,sku_type_desc,hazardous_material_class_desc, hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt, msrp_currency_code,npg_ind, order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code
                ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
            --,COALESCE(RANGE_INTERSECT(eff_period , RANGE(eff_begin_tmstp_utc,eff_end_tmstp_utc)),eff_period) AS eff_period,
             FROM
             ( --third level main query 
      SELECT distinct SRC.rms_sku_num,
  SRC.epm_sku_num,
  SRC.channel_country,
  SRC.sku_short_desc,
  SRC.sku_desc,
  SRC.web_sku_num,
  SRC.brand_label_num,
  SRC.brand_label_display_name,
  SRC.rms_style_num,
  SRC.epm_style_num,
  SRC.partner_relationship_num,
  SRC.partner_relationship_type_code,
  SRC.web_style_num,
  SRC.style_desc,
  SRC.epm_choice_num,
  SRC.supp_part_num,
  SRC.prmy_supp_num,
  SRC.manufacturer_num,
  SRC.sbclass_num,
  SRC.sbclass_desc,
  SRC.class_num,
  SRC.class_desc,
  SRC.dept_num,
  SRC.dept_desc,
  SRC.grp_num,
  SRC.grp_desc,
  SRC.div_num,
  SRC.div_desc,
  CAST(SRC.cmpy_num AS SMALLINT) AS cmpy_num,
  SRC.cmpy_desc,
  SRC.color_num,
  SRC.color_desc,
  SRC.nord_display_color,
  SRC.nrf_size_code,
  SRC.size_1_num,
  SRC.size_1_desc,
  SRC.size_2_num,
  SRC.size_2_desc,
  SRC.supp_color,
  SRC.supp_size,
  SRC.brand_name,
  SRC.return_disposition_code,
  SRC.return_disposition_desc,
  SRC.selling_status_code,
  SRC.selling_status_desc,
  SRC.live_date,
  SRC.drop_ship_eligible_ind,
  SRC.sku_type_code,
  SRC.sku_type_desc,
  SRC.hazardous_material_class_desc,
  SRC.hazardous_material_class_code,
  SRC.fulfillment_type_code,
  SRC.selling_channel_eligibility_list,
  SRC.smart_sample_ind,
  SRC.gwp_ind,
  SRC.msrp_amt,
  SRC.msrp_currency_code,
  SRC.npg_ind,
  SRC.order_quantity_multiple,
  SRC.fp_forecast_eligible_ind,
  SRC.fp_item_planning_eligible_ind,
  SRC.fp_replenishment_eligible_ind,
  SRC.op_forecast_eligible_ind,
  SRC.op_item_planning_eligible_ind,
  SRC.op_replenishment_eligible_ind,
  SRC.size_range_desc,
  SRC.size_sequence_num,
  SRC.size_range_code,
 COALESCE( SAFE.RANGE_INTERSECT(SRC.eff_period , range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period,
          TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc,
          TGT.eff_begin_tmstp_tz,
          TGT.eff_end_tmstp_tz
 FROM ( --second level normalize 

 --select * from SRC_3
 select *,RANGE( eff_begin_tmstp, eff_end_tmstp ) AS eff_period from SRC_3

            )AS SRC
  LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS tgt ON LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(SRC
     .channel_country) = LOWER(tgt.channel_country)
     AND RANGE_OVERLAPS(SRC.eff_period , RANGE(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc))

 WHERE tgt.rms_sku_num IS NULL
  OR SRC.epm_sku_num <> tgt.epm_sku_num
  OR tgt.epm_sku_num IS NULL AND SRC.epm_sku_num IS NOT NULL
  OR SRC.epm_sku_num IS NULL AND tgt.epm_sku_num IS NOT NULL
  OR LOWER(SRC.selling_status_code) <> LOWER(tgt.selling_status_code)
  OR tgt.selling_status_code IS NULL AND SRC.selling_status_code IS NOT NULL
  OR SRC.selling_status_code IS NULL AND tgt.selling_status_code IS NOT NULL
  OR SRC.live_date <> tgt.live_date
  OR tgt.live_date IS NULL AND SRC.live_date IS NOT NULL
  OR SRC.live_date IS NULL AND tgt.live_date IS NOT NULL
  OR LOWER(SRC.selling_channel_eligibility_list) <> LOWER(tgt.selling_channel_eligibility_list)
  OR tgt.selling_channel_eligibility_list IS NULL AND SRC.selling_channel_eligibility_list IS NOT NULL
  OR SRC.selling_channel_eligibility_list IS NULL AND tgt.selling_channel_eligibility_list IS NOT NULL
  
   )--ordered 
      AS ordered_data
) AS grouped_data)
GROUP BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,range_group
ORDER BY rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code, eff_begin_tmstp_utc
) nrml
);



--.IF ERRORCODE <> 0 THEN .QUIT 8


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
-- BT;
--.IF ERRORCODE <> 0 THEN .QUIT 9

-- SEQUENCED VALIDTIME


-- DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
-- WHERE EXISTS (SELECT *
--  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
--  WHERE epm_sku_num = tgt.epm_sku_num
--   AND LOWER(channel_country) = LOWER(tgt.channel_country));



DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (
  SELECT 1
  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
  WHERE src.epm_sku_num = tgt.epm_sku_num
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
   AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);


UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET tgt.eff_end_tmstp = src.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num = tgt.epm_sku_num
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND src.eff_begin_tmstp > tgt.eff_begin_tmstp
  AND src.eff_begin_tmstp <= tgt.eff_end_tmstp
  AND src.eff_end_tmstp >= tgt.eff_end_tmstp ;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET tgt.eff_begin_tmstp = src.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num = tgt.epm_sku_num
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND src.eff_end_tmstp >= tgt.eff_begin_tmstp
  AND src.eff_begin_tmstp < tgt.eff_begin_tmstp
  AND src.eff_end_tmstp <= tgt.eff_end_tmstp;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim(rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,pack_orderable_code,pack_orderable_desc,pack_sellable_code,pack_sellable_desc,pack_simple_code,pack_simple_desc,display_seq_1,display_seq_2,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
WITH tbl AS 
(SELECT tgt.*, 
src.eff_begin_tmstp AS src_eff_begin_tmstp, 
src.eff_end_tmstp AS src_eff_end_tmstp, 
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt ON src.epm_sku_num = tgt.epm_sku_num 
AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
AND tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), tgt_eff_begin_tmstp, src_eff_begin_tmstp FROM tbl
UNION ALL
SELECT *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), src_eff_end_tmstp, tgt_eff_end_tmstp FROM tbl;

DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt 
WHERE EXISTS (SELECT 1 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
WHERE src.epm_sku_num = tgt.epm_sku_num AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);


--.IF ERRORCODE <> 0 THEN .QUIT 10


--Remove any historical CasePack rows that have the same RMS sku but possibly different


--EPM sku key if they have overlapping history.


-- SEQUENCED VALIDTIME


-- DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
-- WHERE EXISTS (SELECT *
--  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
--  WHERE epm_sku_num <> tgt.epm_sku_num
--   AND LOWER(channel_country) = LOWER(tgt.channel_country)
--   AND LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num)
--   AND LOWER(tgt.sku_type_code) = LOWER('P'));


DELETE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (
  SELECT 1
  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
  WHERE src.epm_sku_num <> tgt.epm_sku_num
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(tgt.sku_type_code) = LOWER('P')
AND src.eff_begin_tmstp <= tgt.eff_begin_tmstp
    AND src.eff_end_tmstp >= tgt.eff_end_tmstp);


UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET tgt.eff_end_tmstp = src.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num <> tgt.epm_sku_num
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(tgt.sku_type_code) = LOWER('P')
  AND src.eff_begin_tmstp > tgt.eff_begin_tmstp
  AND src.eff_begin_tmstp <= tgt.eff_end_tmstp
  AND src.eff_end_tmstp >= tgt.eff_end_tmstp ;


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
SET tgt.eff_begin_tmstp = src.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
WHERE src.epm_sku_num <> tgt.epm_sku_num
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND LOWER(src.rms_sku_num) = LOWER(tgt.rms_sku_num)
  AND LOWER(tgt.sku_type_code) = LOWER('P')
  AND src.eff_end_tmstp >= tgt.eff_begin_tmstp
  AND src.eff_begin_tmstp < tgt.eff_begin_tmstp
  AND src.eff_end_tmstp <= tgt.eff_end_tmstp;


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim(rms_sku_num,epm_sku_num,channel_country,sku_short_desc,sku_desc,web_sku_num,brand_label_num,brand_label_display_name,rms_style_num,epm_style_num,partner_relationship_num,partner_relationship_type_code,web_style_num,style_desc,epm_choice_num,supp_part_num,prmy_supp_num,manufacturer_num,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,color_num,color_desc,nord_display_color,nrf_size_code,size_1_num,size_1_desc,size_2_num,size_2_desc,supp_color,supp_size,brand_name,return_disposition_code,return_disposition_desc,selling_status_code,selling_status_desc,live_date,drop_ship_eligible_ind,sku_type_code,sku_type_desc,pack_orderable_code,pack_orderable_desc,pack_sellable_code,pack_sellable_desc,pack_simple_code,pack_simple_desc,display_seq_1,display_seq_2,hazardous_material_class_desc,hazardous_material_class_code,fulfillment_type_code,selling_channel_eligibility_list,smart_sample_ind,gwp_ind,msrp_amt,msrp_currency_code,npg_ind,order_quantity_multiple,fp_forecast_eligible_ind,fp_item_planning_eligible_ind,fp_replenishment_eligible_ind,op_forecast_eligible_ind,op_item_planning_eligible_ind,op_replenishment_eligible_ind,size_range_desc,size_sequence_num,size_range_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
WITH tbl AS 
(SELECT tgt.*, 
src.eff_begin_tmstp AS src_eff_begin_tmstp, 
src.eff_end_tmstp AS src_eff_end_tmstp,  
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt ON src.epm_sku_num = tgt.epm_sku_num 
AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
AND tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), tgt_eff_begin_tmstp, src_eff_begin_tmstp FROM tbl
UNION ALL
SELECT *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), src_eff_end_tmstp, tgt_eff_end_tmstp FROM tbl;

DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt 
WHERE EXISTS (SELECT 1 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src 
WHERE src.epm_sku_num = tgt.epm_sku_num AND tgt.eff_begin_tmstp < src.eff_begin_tmstp 
and tgt.eff_end_tmstp > src.eff_end_tmstp);



--.IF ERRORCODE <> 0 THEN .QUIT 11

INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim (rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc,
 web_sku_num, brand_label_num, brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num,
 partner_relationship_type_code, web_style_num, style_desc, epm_choice_num, supp_part_num, prmy_supp_num,
 manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num,
 div_desc, cmpy_num, cmpy_desc, color_num, color_desc, nord_display_color, nrf_size_code, size_1_num, size_1_desc,
 size_2_num, size_2_desc, supp_color, supp_size, brand_name, return_disposition_code, return_disposition_desc,
 selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, sku_type_code, sku_type_desc,
 pack_orderable_code, pack_orderable_desc, pack_sellable_code, pack_sellable_desc, pack_simple_code, pack_simple_desc,
 display_seq_1, display_seq_2, hazardous_material_class_desc, hazardous_material_class_code, fulfillment_type_code,
 selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, msrp_currency_code, npg_ind,
 order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind, fp_replenishment_eligible_ind,
 op_forecast_eligible_ind, op_item_planning_eligible_ind, op_replenishment_eligible_ind, size_range_desc,
 size_sequence_num, size_range_code, eff_begin_tmstp, eff_begin_tmstp_tz, eff_end_tmstp, eff_end_tmstp_tz ,dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT rms_sku_num,
  epm_sku_num,
  channel_country,
  sku_short_desc,
  sku_desc,
  web_sku_num,
  brand_label_num,
  brand_label_display_name,
  rms_style_num,
  epm_style_num,
  partner_relationship_num,
  partner_relationship_type_code,
  web_style_num,
  style_desc,
  epm_choice_num,
  supp_part_num,
  prmy_supp_num,
  manufacturer_num,
  sbclass_num,
  sbclass_desc,
  class_num,
  class_desc,
  dept_num,
  dept_desc,
  grp_num,
  grp_desc,
  div_num,
  div_desc,
  cmpy_num,
  cmpy_desc,
  color_num,
  color_desc,
  nord_display_color,
  nrf_size_code,
  size_1_num,
  size_1_desc,
  size_2_num,
  size_2_desc,
  supp_color,
  supp_size,
  brand_name,
  return_disposition_code,
  return_disposition_desc,
  selling_status_code,
  selling_status_desc,
  live_date,
  drop_ship_eligible_ind,
  sku_type_code,
  sku_type_desc,
  pack_orderable_code,
  pack_orderable_desc,
  pack_sellable_code,
  pack_sellable_desc,
  pack_simple_code,
  pack_simple_desc,
  display_seq_1,
  display_seq_2,
  hazardous_material_class_desc,
  hazardous_material_class_code,
  fulfillment_type_code,
  selling_channel_eligibility_list,
  smart_sample_ind,
  gwp_ind,
  msrp_amt,
  msrp_currency_code,
  npg_ind,
  order_quantity_multiple,
  fp_forecast_eligible_ind,
  fp_item_planning_eligible_ind,
  fp_replenishment_eligible_ind,
  op_forecast_eligible_ind,
  op_item_planning_eligible_ind,
  op_replenishment_eligible_ind,
  size_range_desc,
  size_sequence_num,
  size_range_code,
  eff_begin_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT rms_sku_num,
    epm_sku_num,
    channel_country,
    sku_short_desc,
    sku_desc,
    web_sku_num,
    brand_label_num,
    brand_label_display_name,
    rms_style_num,
    epm_style_num,
    partner_relationship_num,
    partner_relationship_type_code,
    web_style_num,
    style_desc,
    epm_choice_num,
    supp_part_num,
    prmy_supp_num,
    manufacturer_num,
    sbclass_num,
    sbclass_desc,
    class_num,
    class_desc,
    dept_num,
    dept_desc,
    grp_num,
    grp_desc,
    div_num,
    div_desc,
    cmpy_num,
    cmpy_desc,
    color_num,
    color_desc,
    nord_display_color,
    nrf_size_code,
    size_1_num,
    size_1_desc,
    size_2_num,
    size_2_desc,
    supp_color,
    supp_size,
    brand_name,
    return_disposition_code,
    return_disposition_desc,
    selling_status_code,
    selling_status_desc,
    live_date,
    drop_ship_eligible_ind,
    sku_type_code,
    sku_type_desc,
    pack_orderable_code,
    pack_orderable_desc,
    pack_sellable_code,
    pack_sellable_desc,
    pack_simple_code,
    pack_simple_desc,
    display_seq_1,
    display_seq_2,
    hazardous_material_class_desc,
    hazardous_material_class_code,
    fulfillment_type_code,
    selling_channel_eligibility_list,
    smart_sample_ind,
    gwp_ind,
    msrp_amt,
    msrp_currency_code,
    npg_ind,
    order_quantity_multiple,
    fp_forecast_eligible_ind,
    fp_item_planning_eligible_ind,
    fp_replenishment_eligible_ind,
    op_forecast_eligible_ind,
    op_item_planning_eligible_ind,
    op_replenishment_eligible_ind,
    size_range_desc,
    size_sequence_num,
    size_range_code,
    eff_begin_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp,
    eff_end_tmstp_tz,
     (SELECT batch_id
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_date,
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_dim_vtw) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim
   WHERE epm_sku_num = t3.epm_sku_num
    AND channel_country = t3.channel_country)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num, channel_country)) = 1);


--.IF ERRORCODE <> 0 THEN .QUIT 12


--Check for cases where we might need to end-date certain records because the RMS9 Sku changed


--from one EPM key to another.


-- NONSEQUENCED VALIDTIME


--Ensure that we do NOT trip the temporal PK constraint with this update


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt SET
 eff_end_tmstp = SRC.new_end_tmstp FROM (SELECT rms_sku_num,
   channel_country,
   epm_sku_num,
   sku_desc,
   sku_type_code,
   eff_begin_tmstp,
   eff_end_tmstp AS old_eff_tmstp,
   MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num
    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS new_end_tmstp
  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim
  WHERE LOWER(rms_sku_num) <> LOWER('')
  QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp
       , epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS SRC
WHERE LOWER(SRC.rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(SRC.channel_country) = LOWER(tgt.channel_country) AND
    SRC.epm_sku_num = tgt.epm_sku_num AND SRC.eff_begin_tmstp = tgt.eff_begin_tmstp AND SRC.new_end_tmstp > tgt.eff_begin_tmstp;



--.IF ERRORCODE <> 0 THEN .QUIT 13


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_overlap_stg (
new_epm_sku_num, rms_sku_num, epm_sku_num, channel_country,
 sku_short_desc, sku_desc, web_sku_num, brand_label_num, brand_label_display_name, rms_style_num, epm_style_num,
 partner_relationship_num, partner_relationship_type_code, web_style_num, style_desc, epm_choice_num, supp_part_num,
 prmy_supp_num, manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num,
 grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, color_desc, nord_display_color, nrf_size_code, size_1_num
 , size_1_desc, size_2_num, size_2_desc, supp_color, supp_size, brand_name, return_disposition_code,
 return_disposition_desc, selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, sku_type_code,
 sku_type_desc, pack_orderable_code, pack_orderable_desc, pack_sellable_code, pack_sellable_desc, pack_simple_code,
 pack_simple_desc, display_seq_1, display_seq_2, hazardous_material_class_desc, hazardous_material_class_code,
 fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, msrp_currency_code,
 npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind,
 fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, op_replenishment_eligible_ind,
 size_range_desc, size_sequence_num, size_range_code, eff_begin_tmstp, eff_end_tmstp, dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp, new_epm_eff_begin, new_epm_eff_end, old_epm_eff_begin, old_epm_eff_end, process_flag
 )
(SELECT DISTINCT b.epm_sku_num AS new_epm_sku_num,
  c.rms_sku_num,
  c.epm_sku_num,
  c.channel_country,
  c.sku_short_desc,
  c.sku_desc,
  c.web_sku_num,
  c.brand_label_num,
  c.brand_label_display_name,
  c.rms_style_num,
  c.epm_style_num,
  c.partner_relationship_num,
  c.partner_relationship_type_code,
  c.web_style_num,
  c.style_desc,
  c.epm_choice_num,
  c.supp_part_num,
  c.prmy_supp_num,
  c.manufacturer_num,
  c.sbclass_num,
  c.sbclass_desc,
  c.class_num,
  c.class_desc,
  c.dept_num,
  c.dept_desc,
  c.grp_num,
  c.grp_desc,
  c.div_num,
  c.div_desc,
  c.cmpy_num,
  c.cmpy_desc,
  c.color_num,
  c.color_desc,
  c.nord_display_color,
  c.nrf_size_code,
  c.size_1_num,
  c.size_1_desc,
  c.size_2_num,
  c.size_2_desc,
  c.supp_color,
  c.supp_size,
  c.brand_name,
  c.return_disposition_code,
  c.return_disposition_desc,
  c.selling_status_code,
  c.selling_status_desc,
  c.live_date,
  c.drop_ship_eligible_ind,
  c.sku_type_code,
  c.sku_type_desc,
  c.pack_orderable_code,
  c.pack_orderable_desc,
  c.pack_sellable_code,
  c.pack_sellable_desc,
  c.pack_simple_code,
  c.pack_simple_desc,
  c.display_seq_1,
  c.display_seq_2,
  c.hazardous_material_class_desc,
  c.hazardous_material_class_code,
  c.fulfillment_type_code,
  c.selling_channel_eligibility_list,
  c.smart_sample_ind,
  c.gwp_ind,
  c.msrp_amt,
  c.msrp_currency_code,
  c.npg_ind,
  c.order_quantity_multiple,
  c.fp_forecast_eligible_ind,
  c.fp_item_planning_eligible_ind,
  c.fp_replenishment_eligible_ind,
  c.op_forecast_eligible_ind,
  c.op_item_planning_eligible_ind,
  c.op_replenishment_eligible_ind,
  c.size_range_desc,
  c.size_sequence_num,
  c.size_range_code,
  c.eff_begin_tmstp_utc,
  c.eff_end_tmstp_utc,
   (SELECT batch_id
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
  CURRENT_DATE AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME()) AS DATETIME) AS dw_sys_load_tmstp,
  MIN(b.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_begin,
  MIN(b.eff_end_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_end,
  MIN(c.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_epm_eff_begin,
  MIN(c.eff_end_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_epm_eff_end,
  'N' AS process_flag
 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS a
  INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND LOWER(a.channel_country
      ) = LOWER(b.channel_country) AND a.epm_sku_num = b.epm_sku_num
  INNER JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS c ON LOWER(b.rms_sku_num) = LOWER(c.rms_sku_num) AND LOWER(b.channel_country
      ) = LOWER(c.channel_country) AND b.epm_sku_num <> c.epm_sku_num
 WHERE 
 RANGE_OVERLAPS(RANGE(b.eff_begin_tmstp,b.eff_end_tmstp) , RANGE(c.eff_begin_tmstp,c.eff_end_tmstp)) 	  AND 
 (a.rms_sku_num, a.channel_country) IN (SELECT (rms_sku_num, channel_country)
    FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
    GROUP BY rms_sku_num,
     channel_country
    HAVING COUNT(DISTINCT epm_sku_num) > 1)
    	  AND RANGE_CONTAINS(RANGE(a.eff_begin_tmstp_utc,a.eff_end_tmstp_utc) , timestamp(current_datetime('PST8PDT')))

  qualify c.eff_begin_tmstp_utc >= MIN(b.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING));


--.IF ERRORCODE <> 0 THEN .QUIT 14


DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS target
WHERE EXISTS (SELECT *
 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg AS source
 WHERE LOWER(target.rms_sku_num) = LOWER(rms_sku_num)
  AND LOWER(target.channel_country) = LOWER(channel_country)
  AND target.epm_sku_num = old_epm_sku_num
  AND target.eff_begin_tmstp >= new_epm_eff_begin_utc
  AND LOWER(process_flag) = LOWER('N'));


--.IF ERRORCODE <> 0 THEN .QUIT 15


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_sku_dim AS target SET
 eff_begin_tmstp = SOURCE.eff_end_tmstp_prev,
 dw_batch_id = (SELECT batch_id
  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) FROM (SELECT rms_sku_num,
   epm_sku_num,
   channel_country,
   eff_begin_tmstp_utc as eff_begin_tmstp,
   eff_end_tmstp_utc as eff_end_tmstp,
   MIN(eff_end_tmstp_utc) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN
    1 PRECEDING AND 1 PRECEDING) AS eff_end_tmstp_prev
  FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
  WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
     FROM (SELECT DISTINCT rms_sku_num,
        channel_country
       FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg
       WHERE LOWER(process_flag) = LOWER('N')
        AND product_sku_dim_hist.dw_batch_id = (SELECT batch_id
          FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
          WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'))) AS t4)
  QUALIFY eff_begin_tmstp <> (MIN(eff_end_tmstp_utc) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY
       eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING))) AS SOURCE
WHERE LOWER(SOURCE.rms_sku_num) = LOWER(target.rms_sku_num) AND LOWER(SOURCE.channel_country) = LOWER(target.channel_country
      ) AND SOURCE.eff_begin_tmstp = target.eff_begin_tmstp AND SOURCE.eff_end_tmstp = target.eff_end_tmstp AND SOURCE.eff_end_tmstp_prev
   <> target.eff_begin_tmstp;


--.IF ERRORCODE <> 0 THEN .QUIT 16


UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.product_sku_overlap_stg SET
 process_flag = 'Y'
WHERE LOWER(process_flag) = LOWER('N') AND dw_batch_id = (SELECT batch_id
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'));


--.IF ERRORCODE <> 0 THEN .QUIT 17


-- ET;


--.IF ERRORCODE <> 0 THEN .QUIT 18


--COLLECT STATISTICS COLUMN(rms_sku_num), COLUMN(channel_country), COLUMN(epm_sku_num), COLUMN(epm_style_num), COLUMN(eff_end_tmstp) ON {{params.dataplex_project_id}}.{{params.dbenv}}_nap_dim.PRODUCT_SKU_DIM

--COMMIT TRANSACTION;
--EXCEPTION WHEN ERROR THEN
--ROLLBACK TRANSACTION;


