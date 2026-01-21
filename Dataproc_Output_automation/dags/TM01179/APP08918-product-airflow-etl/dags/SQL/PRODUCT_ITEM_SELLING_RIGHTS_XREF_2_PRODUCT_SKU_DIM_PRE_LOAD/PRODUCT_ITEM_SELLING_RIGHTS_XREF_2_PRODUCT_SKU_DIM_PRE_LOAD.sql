


truncate table {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW;


truncate table {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_XREF_DAILY_LOAD_LDG;


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg (rms_sku_num, channel_country, selling_status_code, live_date,
 selling_channel_eligibility_list, eff_begin_tmstp, eff_end_tmstp, src_desc,eff_begin_tmstp_tz,
     eff_end_tmstp_tz)
(SELECT *
 FROM (SELECT rms_sku_num,
     channel_country,
     selling_status_code,
     live_date,
     selling_channel_eligibility_list,
     eff_begin_tmstp,
     eff_end_tmstp,
--jwn_udf.udf_time_zone(eff_begin_tmstp) as eff_begin_tmstp_tz
	 eff_begin_tmstp_tz,
     eff_end_tmstp_tz
    , 'current_rec' AS src_desc
    FROM (SELECT *
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
      WHERE eff_begin_tmstp >= (SELECT extract_from_tmstp
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
         WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
       AND eff_begin_tmstp <= (SELECT extract_to_tmstp
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
         WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
      QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp DESC)) = 1) AS t4
    WHERE selling_channel_eligibility_list IS NOT NULL
    UNION ALL
    SELECT xref1.rms_sku_num,
     cast(xref1.channel_country as string),
     xref2.selling_status_code,
     xref2.live_date,
     xref2.selling_channel_eligibility_list,
     cast(xref1.eff_begin_tmstp as timestamp),
     xref2.eff_end_tmstp,
     'future_effective' AS src_desc,
    xref1.eff_begin_tmstp_tz as eff_begin_tmstp_tz,
    xref2.eff_end_tmstp_tz as eff_end_tmstp_tz,
      --xref2.eff_begin_tmstp_tz,
     --xref2.eff_end_tmstp_tz
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1
     LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref2 
     ON  lower(xref1.rms_sku_num) = lower(xref2.rms_sku_num) 
     AND lower(xref1.channel_country) = lower(xref2.channel_country)
    WHERE xref1.eff_begin_tmstp >= (SELECT extract_from_tmstp
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
       WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
     AND xref1.eff_begin_tmstp <= (SELECT extract_to_tmstp
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
       WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
     AND xref1.selling_channel_eligibility_list IS NULL
     AND xref2.eff_begin_tmstp > xref1.eff_begin_tmstp
     AND (xref1.rms_sku_num, xref1.channel_country) NOT IN (SELECT (rms_sku_num, channel_country)
       FROM (SELECT *
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref
         WHERE eff_begin_tmstp >= (SELECT extract_from_tmstp
            FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
            WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
          AND eff_begin_tmstp <= (SELECT extract_to_tmstp
            FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
            WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
         QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp DESC)) = 1) AS
        t17
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
     xref10.eff_end_tmstp,
     'no_eligible' AS src_desc,
    xref20.eff_end_tmstp_tz as eff_begin_tmstp_tz,
    xref10.eff_end_tmstp_tz as eff_end_tmstp_tz,


    --  xref10.eff_begin_tmstp_tz,
    --  xref10.eff_end_tmstp_tz
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref10
     LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref20 ON lower(xref10.rms_sku_num) = lower(xref20.rms_sku_num
        ) AND lower(xref10.channel_country) = lower(xref20.channel_country)
    WHERE xref10.eff_begin_tmstp >= (SELECT extract_from_tmstp
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
       WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
     AND xref10.eff_begin_tmstp <= (SELECT extract_to_tmstp
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
       WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
     AND xref10.selling_channel_eligibility_list IS NULL
     AND xref20.eff_begin_tmstp < xref10.eff_begin_tmstp
     AND (xref10.rms_sku_num, xref10.channel_country) NOT IN (SELECT (xref11.rms_sku_num, xref11.channel_country)
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref11
        LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref21 ON lower(xref11.rms_sku_num) = lower(xref21.rms_sku_num
           ) AND lower(xref11.channel_country) = lower(xref21.channel_country)
       WHERE xref11.eff_begin_tmstp >= (SELECT extract_from_tmstp
          FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
          WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
        AND xref11.eff_begin_tmstp <= (SELECT extract_to_tmstp
          FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
          WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
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
     cast(xref12.dw_sys_load_tmstp AS TIMESTAMP) AS eff_begin_tmstp,
     xref12.eff_end_tmstp,
     'default_future' AS src_desc,

    jwn_udf.udf_time_zone(cast(xref12.dw_sys_load_tmstp as string)) as eff_begin_tmstp_tz,
    xref12.eff_end_tmstp_tz as eff_end_tmstp_tz,

  
    --  ,xref12.eff_begin_tmstp_tz,
    --  xref12.eff_end_tmstp_tz
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref12
     INNER JOIN (SELECT rms_sku_num,
       channel_country,
       MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
        UNBOUNDED FOLLOWING) AS min_eff_begin_tmstp,
       MAX(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
        UNBOUNDED FOLLOWING) AS max_eff_begin_tmstp
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1
      WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
         WHERE dw_batch_date >= (SELECT CAST(extract_from_tmstp AS DATE) AS a319933093
            FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
            WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
          AND dw_batch_date <= (SELECT CAST(extract_to_tmstp AS DATE) AS a261580300
            FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
            WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
         GROUP BY rms_sku_num,
          channel_country)
      QUALIFY (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING)) = (MAX(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country
          RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AND (MIN(eff_begin_tmstp) OVER (PARTITION BY
            rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) > (SELECT
          extract_to_tmstp
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
         WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))) AS t54 
         ON lower(xref12.rms_sku_num) = lower(t54.rms_sku_num) 
         AND lower(xref12.channel_country) = lower(t54.channel_country)) AS t56);

--.IF ERRORCODE <> 0 THEN .QUIT 3

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg (rms_sku_num, channel_country, selling_status_code, live_date,
 selling_channel_eligibility_list, eff_begin_tmstp, eff_end_tmstp, src_desc,eff_begin_tmstp_tz, eff_end_tmstp_tz)
(SELECT xref1.rms_sku_num,
  xref1.channel_country,
  xref1.selling_status_code,
  xref1.live_date,
  xref1.selling_channel_eligibility_list,
  CAST(xref1.dw_sys_load_tmstp AS TIMESTAMP) AS eff_begin_tmstp,
  xref1.eff_end_tmstp,
  'default_future_multi_change' AS src_desc,

  jwn_udf.udf_time_zone(cast(xref1.dw_sys_load_tmstp as string)) as eff_begin_tmstp_tz,
  xref1.eff_end_tmstp_tz as eff_end_tmstp_tz,


  --,xref1.eff_begin_tmstp_tz, xref1.eff_end_tmstp_tz
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1
  INNER JOIN (SELECT rms_sku_num,
    channel_country,
    selling_channel_eligibility_list,
    MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
     UNBOUNDED FOLLOWING) AS min_eff_begin_tmstp,
    MAX(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country RANGE BETWEEN UNBOUNDED PRECEDING AND
     UNBOUNDED FOLLOWING) AS max_eff_begin_tmstp,
    eff_begin_tmstp
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref1
   WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
      FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_item_selling_rights_xref AS xref
      WHERE dw_batch_date >= (SELECT CAST(extract_from_tmstp AS DATE) AS a319933093
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
         WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
       AND dw_batch_date <= (SELECT CAST(extract_to_tmstp AS DATE) AS a261580300
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_utl.elt_control
         WHERE lower(subject_area_nm) = lower('PRODUCT_ITEM_SELLING_RIGHTS'))
      GROUP BY rms_sku_num,
       channel_country)
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp)) = 1) AS t12 
   ON  lower(xref1.rms_sku_num) = lower(t12.rms_sku_num) 
   AND lower(xref1.channel_country) = lower(t12.channel_country) 
   AND xref1.eff_begin_tmstp = t12.eff_begin_tmstp
 WHERE lower(xref1.channel_country) NOT IN (SELECT lower(rms_sku_num)
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg
    GROUP BY rms_sku_num,
     channel_country)
  and lower(xref1.channel_country) NOT IN (SELECT lower(channel_country)
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg
    GROUP BY rms_sku_num,
     channel_country));

--.IF ERRORCODE <> 0 THEN .QUIT 4

TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_SELLING_RIGHTS_TMP_STG;

--.IF ERRORCODE <> 0 THEN .QUIT 5

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_selling_rights_tmp_stg (rms_sku_num, channel_country, sr_eff_begin_tmstp,
 sr_eff_end_tmstp, nps_eff_begin_tmstp, dw_sys_load_tmstp,dw_sys_load_tmstp_tz,sr_eff_begin_tmstp_tz,
 sr_eff_end_tmstp_tz,
 nps_eff_begin_tmstp_tz)
(SELECT selling_rights_delta.rms_sku_num,
  selling_rights_delta.channel_country,
  selling_rights_delta.eff_begin_tmstp AS sr_eff_begin_tmstp,
  selling_rights_delta.eff_end_tmstp AS sr_eff_end_tmstp,
  CAST(t1.eff_begin_tmstp AS TIMESTAMP) AS nps_eff_begin_tmstp,
  timestamp(current_datetime('PST8PDT')) AS dw_sys_load_tmstp,
  jwn_udf.default_tz_pst() as dw_sys_load_tmstp_tz,
  selling_rights_delta.eff_begin_tmstp_tz as sr_eff_begin_tmstp_tz,
 selling_rights_delta.eff_begin_tmstp_tz as sr_eff_end_tmstp_tz,
 jwn_udf.udf_time_zone(CAST(t1.eff_begin_tmstp AS STRING)) AS nps_eff_begin_tmstp_tz
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg AS selling_rights_delta
  INNER JOIN (SELECT legacyrmsskuid AS rms_sku_num,
    marketcode AS channel_country,
    returndispositioncode,
    cast(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,'')) as timestamp) AS eff_begin_tmstp
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_ldg
   QUALIFY (ROW_NUMBER() OVER (PARTITION BY legacyrmsskuid, marketcode ORDER BY jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))
 DESC)) = 1) AS t1 
 ON  lower(selling_rights_delta.rms_sku_num) = lower(t1.rms_sku_num) 
 AND lower(selling_rights_delta.channel_country) = lower(t1.channel_country)
 WHERE selling_rights_delta.eff_begin_tmstp > CAST(t1.eff_begin_tmstp AS TIMESTAMP));

--.IF ERRORCODE <> 0 THEN .QUIT 6

-- COLLECT STATISTICS
--     column ( rms_sku_num ),
--     column ( channel_country ),
--     column ( sr_eff_begin_tmstp ),
--     column ( nps_eff_begin_tmstp )
-- ON {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_SELLING_RIGHTS_TMP_STG;

-- .IF ERRORCODE <> 0 THEN .QUIT 7

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW
( rms_sku_num
, epm_sku_num
, channel_country
, sku_short_desc
, sku_desc
, web_sku_num
, brand_label_num
, brand_label_display_name
, rms_style_num
, epm_style_num
, partner_relationship_num
, partner_relationship_type_code
, web_style_num
, style_desc
, epm_choice_num
, supp_part_num
, prmy_supp_num
, manufacturer_num
, sbclass_num
, sbclass_desc
, class_num
, class_desc
, dept_num
, dept_desc
, grp_num
, grp_desc
, div_num
, div_desc
, cmpy_num
, cmpy_desc
, color_num
, color_desc
, nord_display_color
, nrf_size_code
, size_1_num
, size_1_desc
, size_2_num
, size_2_desc
, supp_color
, supp_size
, brand_name
, return_disposition_code
, return_disposition_desc
, selling_status_code
, selling_status_desc
, live_date
, drop_ship_eligible_ind
, sku_type_code
, sku_type_desc
--, pack_orderable_code   Default to NULL
--, pack_orderable_desc   Default to NULL
--, pack_sellable_code   Default to NULL
--, pack_sellable_desc   Default to NULL
--, pack_simple_code   Default to NULL
--, pack_simple_desc   Default to NULL
--, display_seq_1   Default to NULL
--, display_seq_2   Default to NULL
, hazardous_material_class_desc
, hazardous_material_class_code
, fulfillment_type_code
, selling_channel_eligibility_list
, smart_sample_ind
, gwp_ind
, msrp_amt
, msrp_currency_code
, npg_ind
, order_quantity_multiple
, fp_forecast_eligible_ind
, fp_item_planning_eligible_ind
, fp_replenishment_eligible_ind
, op_forecast_eligible_ind
, op_item_planning_eligible_ind
, op_replenishment_eligible_ind
, size_range_desc
, size_sequence_num
, size_range_code
, eff_begin_tmstp
, eff_end_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
)
SELECT
  NRML.rms_sku_num
, NRML.epm_sku_num
, NRML.channel_country
, NRML.sku_short_desc
, NRML.sku_desc
, NRML.web_sku_num
, NRML.brand_label_num
, NRML.brand_label_display_name
, NRML.rms_style_num
, NRML.epm_style_num
, NRML.partner_relationship_num
, NRML.partner_relationship_type_code
, NRML.web_style_num
, NRML.style_desc
, NRML.epm_choice_num
, NRML.supp_part_num
, NRML.prmy_supp_num
, NRML.manufacturer_num
, NRML.sbclass_num
, NRML.sbclass_desc
, NRML.class_num
, NRML.class_desc
, NRML.dept_num
, NRML.dept_desc
, NRML.grp_num
, NRML.grp_desc
, NRML.div_num
, NRML.div_desc
, CAST(floor(cast(NRML.cmpy_num as float64)) AS int64)
, NRML.cmpy_desc
, NRML.color_num
, NRML.color_desc
, NRML.nord_display_color
, NRML.nrf_size_code
, NRML.size_1_num
, NRML.size_1_desc
, NRML.size_2_num
, NRML.size_2_desc
, NRML.supp_color
, NRML.supp_size
, NRML.brand_name
, NRML.return_disposition_code
, NRML.return_disposition_desc
, NRML.selling_status_code
, NRML.selling_status_desc
, NRML.live_date
, NRML.drop_ship_eligible_ind
, NRML.sku_type_code
, NRML.sku_type_desc
, NRML.hazardous_material_class_desc
, NRML.hazardous_material_class_code
, NRML.fulfillment_type_code
, NRML.selling_channel_eligibility_list
, NRML.smart_sample_ind
, NRML.gwp_ind
, NRML.msrp_amt
, NRML.msrp_currency_code
, NRML.npg_ind
, NRML.order_quantity_multiple
, NRML.fp_forecast_eligible_ind
, NRML.fp_item_planning_eligible_ind
, NRML.fp_replenishment_eligible_ind
, NRML.op_forecast_eligible_ind
, NRML.op_item_planning_eligible_ind
, NRML.op_replenishment_eligible_ind
, NRML.size_range_desc
, NRML.size_sequence_num
, NRML.size_range_code
, RANGE_START (NRML.eff_period) AS eff_begin
, RANGE_END(NRML.eff_period)   AS eff_end
, jwn_udf.udf_time_zone(CAST(RANGE_START(NRML.eff_period)AS string)) as eff_begin_tmstp_tz,
  jwn_udf.udf_time_zone( CAST(RANGE_END(NRML.eff_period) AS string)) as eff_end_tmstp_tz
FROM (
--NONSEQUENCED VALIDTIME
SELECT DISTINCT
  SRC.rms_sku_num
, SRC.epm_sku_num
, SRC.channel_country
, SRC.sku_short_desc
, SRC.sku_desc
, SRC.web_sku_num
, SRC.brand_label_num
, SRC.brand_label_display_name
, SRC.rms_style_num
, SRC.epm_style_num
, SRC.partner_relationship_num
, SRC.partner_relationship_type_code
, SRC.web_style_num
, SRC.style_desc
, SRC.epm_choice_num
, SRC.supp_part_num
, SRC.prmy_supp_num
, SRC.manufacturer_num
, SRC.sbclass_num
, SRC.sbclass_desc
, SRC.class_num
, SRC.class_desc
, SRC.dept_num
, SRC.dept_desc
, SRC.grp_num
, SRC.grp_desc
, SRC.div_num
, SRC.div_desc
, SRC.cmpy_num
, SRC.cmpy_desc
, SRC.color_num
, SRC.color_desc
, SRC.nord_display_color
, SRC.nrf_size_code
, SRC.size_1_num
, SRC.size_1_desc
, SRC.size_2_num
, SRC.size_2_desc
, SRC.supp_color
, SRC.supp_size
, SRC.brand_name
, SRC.return_disposition_code
, SRC.return_disposition_desc
, SRC.selling_status_code
, SRC.selling_status_desc
, SRC.live_date
, SRC.drop_ship_eligible_ind
, SRC.sku_type_code
, SRC.sku_type_desc
, SRC.hazardous_material_class_desc
, SRC.hazardous_material_class_code
, SRC.fulfillment_type_code
, SRC.selling_channel_eligibility_list
, SRC.smart_sample_ind
, SRC.gwp_ind
, SRC.msrp_amt
, SRC.msrp_currency_code
, SRC.npg_ind
, SRC.order_quantity_multiple
, SRC.fp_forecast_eligible_ind
, SRC.fp_item_planning_eligible_ind
, SRC.fp_replenishment_eligible_ind
, SRC.op_forecast_eligible_ind
, SRC.op_item_planning_eligible_ind
, SRC.op_replenishment_eligible_ind
, SRC.size_range_desc
, SRC.size_sequence_num
, SRC.size_range_code
, COALESCE( RANGE_INTERSECT(SRC.eff_period, RANGE(TIMESTAMP(TGT.eff_begin_tmstp),TIMESTAMP(TGT.eff_end_tmstp)))
          , SRC.eff_period ) AS eff_period
FROM (
	SELECT DISTINCT
	  src0.rms_sku_num
	, src0.epm_sku_num
	, src0.channel_country
	, src0.sku_short_desc
	, src0.sku_desc
	, src0.web_sku_num
	, src0.brand_label_num
	, src0.brand_label_display_name
	, src0.rms_style_num
	, src0.epm_style_num
	, src0.partner_relationship_num
    , src0.partner_relationship_type_code
	, src0.web_style_num
	, styl.style_desc
	, src0.epm_choice_num
	, src0.supp_part_num
	, src0.prmy_supp_num
	, src0.manufacturer_num
	, styl.sbclass_num
	, styl.sbclass_desc
	, styl.class_num
	, styl.class_desc
	, styl.dept_num
	, styl.dept_desc
	, dept.subdivision_num AS grp_num
	, dept.subdivision_short_name AS grp_desc
	, dept.division_num AS div_num
	, dept.division_short_name AS div_desc
	, '1000' AS cmpy_num
	, 'Nordstrom' AS cmpy_desc
	, choice.nrf_color_num AS color_num
	, choice.color_desc
	, choice.nord_display_color AS nord_display_color
	, src0.nrf_size_code
	, src0.size_1_num
	, src0.size_1_desc
	, src0.size_2_num
	, src0.size_2_desc
	, src0.supp_color
	, src0.supp_size
	, styl.vendor_label_name AS brand_name
	, src0.return_disposition_code
	, src0.return_disposition_desc
	, src0.selling_status_code
	, src0.selling_status_desc
	, src0.live_date
	, src0.drop_ship_eligible_ind
	, 'f' AS sku_type_code
	, 'Fashion Sku' AS sku_type_desc
    , src0.hazardous_material_class_desc
    , src0.hazardous_material_class_code
	, src0.fulfillment_type_code
	, src0.selling_channel_eligibility_list
	, src0.smart_sample_ind
	, src0.gwp_ind
	, src0.msrp_amt
	, src0.msrp_currency_code
	, src0.npg_ind
	, src0.order_quantity_multiple
	, src0.fp_forecast_eligible_ind
	, src0.fp_item_planning_eligible_ind
	, src0.fp_replenishment_eligible_ind
	, src0.op_forecast_eligible_ind
	, src0.op_item_planning_eligible_ind
	, src0.op_replenishment_eligible_ind
	, src0.size_range_desc
	, src0.size_sequence_num
	, src0.size_range_code

	, COALESCE(
	(
		RANGE_INTERSECT(RANGE_INTERSECT(CASE
			WHEN styl.epm_style_num > 0 THEN (
				RANGE_INTERSECT(SRC0.eff_period , RANGE(timestamp(styl.eff_begin_tmstp), timestamp(styl.eff_end_tmstp)))
			)
			ELSE SRC0.eff_period
		END , CASE
			WHEN dept.dept_num > 0 THEN (
				RANGE_INTERSECT(SRC0.eff_period , RANGE(TIMESTAMP(dept.eff_begin_tmstp), TIMESTAMP(dept.eff_end_tmstp)))
			)
			ELSE SRC0.eff_period
		END ), CASE
			WHEN choice.epm_choice_num > 0 THEN (
				RANGE_INTERSECT(SRC0.eff_period , RANGE(TIMESTAMP(choice.eff_begin_tmstp), TIMESTAMP(choice.eff_end_tmstp)))
			)
			ELSE SRC0.eff_period
		END)
	),
	SRC0.eff_period) AS eff_period
FROM (
		SELECT DISTINCT rms_sku_num, epm_sku_num	, channel_country, sku_short_desc
		, sku_desc	, web_sku_num	, brand_label_num	, brand_label_display_name
		, rms_style_num	, epm_style_num	, partner_relationship_num , partner_relationship_type_code, web_style_num, epm_choice_num
		, supp_part_num	, prmy_supp_num	, manufacturer_num	, nrf_size_code
		, size_1_num	, size_1_desc	, size_2_num	, size_2_desc
		, supp_color	, supp_size	, return_disposition_code	, return_disposition_desc
		, selling_status_code	, selling_status_desc	, live_date	, drop_ship_eligible_ind
	    , hazardous_material_class_desc    , hazardous_material_class_code
		, fulfillment_type_code	, selling_channel_eligibility_list
		, smart_sample_ind	, gwp_ind	, msrp_amt	, msrp_currency_code	, npg_ind
		, order_quantity_multiple	, fp_forecast_eligible_ind	, fp_item_planning_eligible_ind	, fp_replenishment_eligible_ind
		, op_forecast_eligible_ind	, op_item_planning_eligible_ind	, op_replenishment_eligible_ind	, size_range_desc
		, size_sequence_num	, size_range_code
		, RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
		FROM (
			SELECT t3.rms_sku_num,
 t3.epm_sku_num,
 t3.channel_country,
 t3.sku_short_desc,
 t3.sku_desc,
 t3.web_sku_num,
 t3.brand_label_num,
 t3.brand_label_display_name,
 t3.rms_style_num,
 t3.epm_style_num,
 t3.partner_relationship_num,
 t3.partner_relationship_type_code,
 t3.web_style_num,
 t3.epm_choice_num,
 t3.supp_part_num,
 t3.prmy_supp_num,
 t3.manufacturer_num,
 t3.nrf_size_code,
 t3.size_1_num,
 t3.size_1_desc,
 t3.size_2_num,
 t3.size_2_desc,
 t3.supp_color,
 t3.supp_size,
 t3.return_disposition_code,
 t3.return_disposition_desc,
 t3.drop_ship_eligible_ind,
 t3.hazardous_material_class_desc,
 t3.hazardous_material_class_code,
 t3.fulfillment_type_code,
 t3.smart_sample_ind,
 t3.gwp_ind,
 t3.msrp_amt,
 t3.msrp_currency_code,
 t3.npg_ind,
 t3.order_quantity_multiple,
 t3.fp_forecast_eligible_ind,
 t3.fp_item_planning_eligible_ind,
 t3.fp_replenishment_eligible_ind,
 t3.op_forecast_eligible_ind,
 t3.op_item_planning_eligible_ind,
 t3.op_replenishment_eligible_ind,
 t3.size_range_desc,
 t3.size_sequence_num,
 t3.size_range_code,
 t3.derived_selling_status_code,
 t3.selling_status_desc,
 t3.selling_status_code,
 t3.live_date,
 t3.selling_channel_eligibility_list,
 t3.eff_begin_tmstp,
 COALESCE(MAX(t3.eff_begin_tmstp) OVER (PARTITION BY t3.rms_sku_num, t3.channel_country ORDER BY t3.eff_begin_tmstp
   ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), CAST('0000-01-01 00:00:00' AS TIMESTAMP)) AS eff_end_tmstp
	--   COALESCE(MAX(t3.eff_begin_tmstp) OVER (PARTITION BY t3.rms_sku_num, t3.channel_country ORDER BY t3.eff_begin_tmstp
  --  ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), CAST('0000-01-01 00:00:00' AS TIMESTAMP)) AS eff_period_end
FROM (SELECT DISTINCT sku.rms_sku_num,
   sku.epm_sku_num,
   sku.channel_country,
   sku.sku_short_desc,
   sku.sku_desc,
   sku.web_sku_num,
   sku.brand_label_num,
   sku.brand_label_display_name,
   sku.rms_style_num,
   sku.epm_style_num,
   sku.partner_relationship_num,
   sku.partner_relationship_type_code,
   sku.web_style_num,
   sku.epm_choice_num,
   sku.supp_part_num,
   sku.prmy_supp_num,
   sku.manufacturer_num,
   sku.nrf_size_code,
   sku.size_1_num,
   sku.size_1_desc,
   sku.size_2_num,
   sku.size_2_desc,
   sku.supp_color,
   sku.supp_size,
   sku.return_disposition_code,
   sku.return_disposition_desc,
   sku.drop_ship_eligible_ind,
   sku.hazardous_material_class_desc,
   sku.hazardous_material_class_code,
   sku.fulfillment_type_code,
   sku.smart_sample_ind,
   sku.gwp_ind,
   sku.msrp_amt,
   sku.msrp_currency_code,
   sku.npg_ind,
   sku.order_quantity_multiple,
   sku.fp_forecast_eligible_ind,
   sku.fp_item_planning_eligible_ind,
   sku.fp_replenishment_eligible_ind,
   sku.op_forecast_eligible_ind,
   sku.op_item_planning_eligible_ind,
   sku.op_replenishment_eligible_ind,
   sku.size_range_desc,
   sku.size_sequence_num,
   sku.size_range_code,
    CASE
    WHEN lower(xref.selling_status_code) = lower('BLOCKED')
    THEN 'S1'
    WHEN lower(sku.fulfillment_type_code) IN (lower('3040'), lower('4000'), lower('4020'), lower('4040'), lower('4060'))
    THEN 'T1'
    WHEN lower(xref.selling_status_code) = lower('UNBLOCKED')
    THEN 'C3'
    ELSE NULL
    END AS derived_selling_status_code,
    CASE WHEN lower(CASE WHEN lower(xref.selling_status_code) = lower('BLOCKED') THEN 'S1' 
                    WHEN lower(sku.fulfillment_type_code) IN (lower('3040'), lower('4000'), lower('4020'), lower('4040'), lower('4060')) THEN 'T1' 
                    WHEN lower(xref.selling_status_code) = lower('UNBLOCKED') THEN 'C3'
                    ELSE NULL
              END) = lower('S1')
          THEN 'Unsellable'
          WHEN lower(CASE WHEN lower(xref.selling_status_code) = lower('BLOCKED')  THEN 'S1'
                          WHEN lower(sku.fulfillment_type_code) IN (lower('3040'), lower('4000'), lower('4020'), lower('4040'), lower('4060'))  THEN 'T1'
                          WHEN lower(xref.selling_status_code) = lower('UNBLOCKED')  THEN 'C3'
                          ELSE NULL
                    END) = lower('C3')
          THEN 'Sellable'
          WHEN LOWER(CASE WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED') THEN 'S1'
                          WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')) THEN 'T1'
                          WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED') THEN 'C3'
                          ELSE NULL
                     END) = LOWER('T1')
          THEN 'Dropship'
          ELSE NULL
    END AS selling_status_desc,
    CASE WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED') THEN 'S1' 
         WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')) THEN 'T1' 
         WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
         THEN 'C3'
         ELSE NULL
    END AS selling_status_code,
   xref.live_date,
   xref.selling_channel_eligibility_list,
   xref.eff_begin_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg AS xref
  INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS sku 
  ON  LOWER(xref.rms_sku_num) = LOWER(sku.rms_sku_num) 
  AND LOWER(xref.channel_country) = LOWER(sku.channel_country)
  LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_selling_rights_tmp_stg AS delta_check 
  ON LOWER(xref.rms_sku_num) = LOWER(delta_check.rms_sku_num) AND LOWER(xref.channel_country) = LOWER(delta_check.channel_country) AND xref.eff_begin_tmstp =
      delta_check.sr_eff_begin_tmstp AND xref.eff_end_tmstp = delta_check.sr_eff_end_tmstp
  WHERE (sku.eff_begin_tmstp > xref.eff_begin_tmstp AND sku.eff_begin_tmstp < xref.eff_end_tmstp OR xref.eff_begin_tmstp
         > sku.eff_begin_tmstp AND xref.eff_begin_tmstp < sku.eff_end_tmstp OR sku.eff_begin_tmstp = xref.eff_begin_tmstp
        AND (sku.eff_end_tmstp = xref.eff_end_tmstp OR sku.eff_end_tmstp <> xref.eff_end_tmstp))
   AND delta_check.rms_sku_num IS NULL) AS t3
QUALIFY t3.eff_begin_tmstp < COALESCE(MAX(t3.eff_begin_tmstp) OVER (PARTITION BY t3.rms_sku_num, t3.channel_country
   ORDER BY t3.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), CAST('0000-01-01 00:00:00' AS TIMESTAMP))
	) SRC_1
) SRC0
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST STYL
ON src0.epm_style_num = styl.epm_style_num
AND LOWER(src0.channel_country) = LOWER(styl.channel_country)
AND RANGE_OVERLAPS(src0.eff_period , RANGE(timestamp(styl.eff_begin_tmstp), timestamp(styl.eff_end_tmstp)))

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DEPARTMENT_DIM_HIST dept
ON dept.dept_num = styl.dept_num
AND RANGE_OVERLAPS(src0.eff_period , RANGE(TIMESTAMP(dept.eff_begin_tmstp), TIMESTAMP(dept.eff_end_tmstp)))
AND RANGE_OVERLAPS(RANGE(TIMESTAMP(styl.eff_begin_tmstp), TIMESTAMP(styl.eff_end_tmstp)) , RANGE(TIMESTAMP(dept.eff_begin_tmstp), TIMESTAMP(dept.eff_end_tmstp)))



LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_CHOICE_DIM_HIST CHOICE
ON src0.epm_choice_num = choice.epm_choice_num
AND LOWER(src0.channel_country) = LOWER(choice.channel_country)
AND RANGE_OVERLAPS(src0.eff_period , RANGE(TIMESTAMP(choice.eff_begin_tmstp), TIMESTAMP(choice.eff_end_tmstp)))
AND RANGE_OVERLAPS(RANGE(timestamp(styl.eff_begin_tmstp), timestamp(styl.eff_end_tmstp)) , RANGE(TIMESTAMP(choice.eff_begin_tmstp), TIMESTAMP(choice.eff_end_tmstp)))
AND RANGE_OVERLAPS(RANGE(TIMESTAMP(dept.eff_begin_tmstp), TIMESTAMP(dept.eff_end_tmstp)) , RANGE(TIMESTAMP(choice.eff_begin_tmstp), TIMESTAMP(choice.eff_end_tmstp)))

) SRC


LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST TGT
ON SRC.rms_sku_num = TGT.rms_sku_num
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
AND RANGE_OVERLAPS(SRC.eff_period , RANGE(TIMESTAMP(TGT.eff_begin_tmstp), TIMESTAMP(TGT.eff_end_tmstp)))

WHERE ( TGT.rms_sku_num IS NULL 
OR
   (  (SRC.epm_sku_num <> TGT.epm_sku_num 
   OR (SRC.epm_sku_num IS NOT NULL AND TGT.epm_sku_num IS NULL) 
   OR (SRC.epm_sku_num IS NULL AND TGT.epm_sku_num IS NOT NULL))
   
   OR (LOWER(SRC.selling_status_code) <> LOWER(TGT.selling_status_code) 
   OR (SRC.selling_status_code IS NOT NULL AND TGT.selling_status_code IS NULL) 
   OR (SRC.selling_status_code IS NULL AND TGT.selling_status_code IS NOT NULL))
   
   OR (SRC.live_date <> TGT.live_date 
   OR (SRC.live_date IS NOT NULL AND TGT.live_date IS NULL) 
   OR (SRC.live_date IS NULL AND TGT.live_date IS NOT NULL))
   
   OR (LOWER(SRC.selling_channel_eligibility_list) <> LOWER(TGT.selling_channel_eligibility_list) 
   OR (SRC.selling_channel_eligibility_list IS NOT NULL AND TGT.selling_channel_eligibility_list IS NULL) 
   OR (SRC.selling_channel_eligibility_list IS NULL AND TGT.selling_channel_eligibility_list IS NOT NULL))
   )  )
) NRML
; 
















-- .IF ERRORCODE <> 0 THEN .QUIT 8


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN transaction;
-- .IF ERRORCODE <> 0 THEN .QUIT 9

/* SEQUENCED VALIDTIME */


DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT *
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
 WHERE epm_sku_num = tgt.epm_sku_num
  AND lower(channel_country) = lower(tgt.channel_country));

-- .IF ERRORCODE <> 0 THEN .QUIT 10
/* SEQUENCED VALIDTIME */

--Remove any historical CasePack rows that have the same RMS sku but possibly different
--EPM sku key if they have overlapping history.
/* SEQUENCED VALIDTIME */

DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT *
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
 WHERE epm_sku_num <> tgt.epm_sku_num
  AND lower(channel_country) = lower(tgt.channel_country)
  AND lower(rms_sku_num) = lower(tgt.rms_sku_num)
  AND lower(tgt.sku_type_code) = lower('P'));
  
  
-- .IF ERRORCODE <> 0 THEN .QUIT 11


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim (rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc,
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
 size_sequence_num, size_range_code, eff_begin_tmstp, eff_end_tmstp, dw_batch_id, dw_batch_date, dw_sys_load_tmstp,eff_begin_tmstp_tz,eff_end_tmstp_tz)
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
  eff_end_tmstp,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp,
  eff_begin_tmstp_tz,
  eff_end_tmstp_tz
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
    eff_end_tmstp,
  eff_begin_tmstp_tz as eff_begin_tmstp_tz,
 eff_end_tmstp_tz as eff_end_tmstp_tz,
	-- eff_begin_tmstp_tz,
  --   eff_end_tmstp_tz,
     (SELECT batch_id
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE lower(subject_area_nm) = lower('NAP_PRODUCT')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE lower(subject_area_nm) = lower('NAP_PRODUCT')) AS dw_batch_date,
    current_datetime('PST8PDT') AS dw_sys_load_tmstp
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_vtw) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `a12180`
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim
   WHERE epm_sku_num = t3.epm_sku_num
    AND channel_country = t3.channel_country)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num, channel_country)) = 1);


-- .IF ERRORCODE <> 0 THEN .QUIT 12


--Check for cases where we might need to end-date certain records because the RMS9 Sku changed
--from one EPM key to another.

UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt 
SET eff_end_tmstp = t3.new_end_tmstp,
 eff_end_tmstp_tz = jwn_udf.udf_time_zone(cast(t3.new_end_tmstp as string)) 
FROM (SELECT rms_sku_num,
   channel_country,
   epm_sku_num,
   sku_desc,
   sku_type_code,
   eff_begin_tmstp,
   eff_end_tmstp AS old_eff_tmstp,
   eff_begin_tmstp_tz,
   eff_end_tmstp_tz,
   MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num
    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS new_end_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim
  WHERE (rms_sku_num) <> ('')
  QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp
       , epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS t3
WHERE lower(t3.rms_sku_num) = lower(tgt.rms_sku_num) 
AND lower(t3.channel_country) = lower(tgt.channel_country) 
AND t3.epm_sku_num = tgt.epm_sku_num 
AND t3.eff_begin_tmstp = tgt.eff_begin_tmstp 
AND t3.new_end_tmstp > tgt.eff_begin_tmstp;

-- .IF ERRORCODE <> 0 THEN .QUIT 13

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_OVERLAP_STG (new_epm_sku_num, rms_sku_num, epm_sku_num, channel_country,
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
 size_range_desc, size_sequence_num, size_range_code, eff_begin_tmstp, eff_end_tmstp, eff_begin_tmstp_tz,
	eff_end_tmstp_tz,dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp, new_epm_eff_begin,new_epm_eff_begin_tz, new_epm_eff_end,new_epm_eff_end_tz, old_epm_eff_begin, old_epm_eff_begin_tz,old_epm_eff_end,old_epm_eff_end_tz, process_flag)
(SELECT DISTINCT b.epm_sku_num AS new_epm_sku_num, c.rms_sku_num, c.epm_sku_num, c.channel_country, c.sku_short_desc, c.sku_desc, c.web_sku_num, c.brand_label_num, c.brand_label_display_name, c.rms_style_num, c.epm_style_num, c.partner_relationship_num, c.partner_relationship_type_code, c.web_style_num, c.style_desc, c.epm_choice_num, c.supp_part_num, c.prmy_supp_num, c.manufacturer_num, c.sbclass_num, c.sbclass_desc, c.class_num, c.class_desc, c.dept_num, c.dept_desc, c.grp_num, c.grp_desc, c.div_num, c.div_desc, c.cmpy_num, c.cmpy_desc, c.color_num, c.color_desc, c.nord_display_color, c.nrf_size_code, c.size_1_num, c.size_1_desc, c.size_2_num, c.size_2_desc, c.supp_color, c.supp_size, c.brand_name, c.return_disposition_code, c.return_disposition_desc, c.selling_status_code, c.selling_status_desc, c.live_date, c.drop_ship_eligible_ind, c.sku_type_code, c.sku_type_desc, c.pack_orderable_code, c.pack_orderable_desc, c.pack_sellable_code, c.pack_sellable_desc, c.pack_simple_code, c.pack_simple_desc, c.display_seq_1, c.display_seq_2, c.hazardous_material_class_desc, c.hazardous_material_class_code, c.fulfillment_type_code, c.selling_channel_eligibility_list, c.smart_sample_ind, c.gwp_ind, c.msrp_amt, c.msrp_currency_code, c.npg_ind, c.order_quantity_multiple, c.fp_forecast_eligible_ind, c.fp_item_planning_eligible_ind, c.fp_replenishment_eligible_ind, c.op_forecast_eligible_ind, c.op_item_planning_eligible_ind, c.op_replenishment_eligible_ind, c.size_range_desc, c.size_sequence_num, c.size_range_code, cast(c.eff_begin_tmstp as timestamp), cast(c.eff_end_tmstp as timestamp),C.eff_begin_tmstp_tz,
  C.eff_end_tmstp_tz,
 (SELECT batch_id FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control WHERE lower(subject_area_nm) = lower('NAP_PRODUCT')) AS dw_batch_id,
  current_date('PST8PDT') AS dw_batch_date,
  current_datetime('PST8PDT') AS dw_sys_load_tmstp,
  
  -- MIN(cast(b.eff_begin_tmstp as timestamp) ) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
  --  UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_begin,

   MIN(b.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_begin,

 MIN(b.eff_begin_tmstp_tz) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as new_epm_eff_begin_tz,

  MIN(b.eff_end_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_end,

MIN(b.eff_end_tmstp_tz) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as new_epm_eff_end_tz,

  MIN(c.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_epm_eff_begin,


   MIN(c.eff_begin_tmstp_tz) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as old_epm_eff_begin_tz,


     MIN(c.eff_end_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_epm_eff_end,


    MIN(c.eff_end_tmstp_tz) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as old_epm_eff_end_tz,

  'N' AS process_flag
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS a
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS b 
 ON  lower(a.rms_sku_num) = lower(b.rms_sku_num)
 AND lower(a.channel_country) = lower(b.channel_country) 
 AND a.epm_sku_num = b.epm_sku_num
 INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS c 
 ON  lower(b.rms_sku_num) = lower(c.rms_sku_num) 
 AND lower(b.channel_country) = lower(c.channel_country) 
 AND b.epm_sku_num <> c.epm_sku_num
 WHERE range_overlaps(range(B.EFF_BEGIN_TMSTP,B.EFF_END_TMSTP) ,range(C.EFF_BEGIN_TMSTP,C.EFF_END_TMSTP))
  AND lower(a.rms_sku_num) IN (SELECT lower(rms_sku_num)
                           FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
                           GROUP BY rms_sku_num, channel_country
                           HAVING COUNT(DISTINCT epm_sku_num) > 1)
  AND  lower(a.channel_country) IN (SELECT lower(channel_country)
                               FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
                               GROUP BY rms_sku_num, channel_country
                               HAVING COUNT(DISTINCT epm_sku_num) > 1)
--   AND range_contains(range(TIMESTAMP(A.EFF_BEGIN_TMSTP,A.EFF_END_TMSTP)) , CURRENT_TIMESTAMP)
-- QUALIFY c.eff_begin_tmstp >= DATETIME(new_epm_eff_begin));
AND RANGE_CONTAINS(RANGE(TIMESTAMP(A.EFF_BEGIN_TMSTP),TIMESTAMP(A.EFF_END_TMSTP)), CURRENT_TIMESTAMP)
QUALIFY C.EFF_BEGIN_TMSTP>=DATETIME(new_epm_eff_begin))
; 

-- .IF ERRORCODE <> 0 THEN .QUIT 14

DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS target
WHERE EXISTS (SELECT *
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg AS source
 WHERE lower(target.rms_sku_num) = lower(rms_sku_num)
  AND  lower(target.channel_country) = lower(channel_country)
  AND target.epm_sku_num = old_epm_sku_num
  AND cast(target.eff_begin_tmstp as timestamp) >= cast(new_epm_eff_begin as timestamp)
  AND lower(process_flag) = lower('N'));

-- .IF ERRORCODE <> 0 THEN .QUIT 15

UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS target SET
 eff_begin_tmstp = cast(t9.eff_end_tmstp_prev as timestamp),
 eff_begin_tmstp_tz = jwn_udf.udf_time_zone (cast(t9.eff_end_tmstp_prev as STRING)),
 dw_batch_id = (SELECT batch_id
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) FROM (SELECT rms_sku_num,
   epm_sku_num,
   channel_country,
   eff_begin_tmstp,
   eff_end_tmstp,
   eff_end_tmstp_prev
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
     hazardous_material_class_code,
     hazardous_material_class_desc,
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
     eff_end_tmstp,
     dw_batch_id,
     dw_batch_date,
     dw_sys_load_tmstp,
     MIN(eff_end_tmstp) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY eff_begin_tmstp
      ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS eff_end_tmstp_prev
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist) AS t
  WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
     FROM (SELECT DISTINCT rms_sku_num,
        channel_country
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg
       WHERE LOWER(process_flag) = LOWER('N')
        AND t.dw_batch_id = (SELECT batch_id
          FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
          WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'))) AS t5)
  
  AND eff_begin_tmstp <> eff_end_tmstp_prev) AS t9
WHERE lower(t9.rms_sku_num) = lower(target.rms_sku_num) AND lower(t9.channel_country) = lower(target.channel_country) AND t9.eff_begin_tmstp = DATETIME(target.eff_begin_tmstp) AND t9.eff_end_tmstp = DATETIME(target.eff_end_tmstp) AND t9.eff_end_tmstp_prev <> DATETIME(target.eff_begin_tmstp);

UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_OVERLAP_STG SET
 process_flag = 'Y'
WHERE lower(process_flag) = lower('N') 
AND dw_batch_id = (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
   WHERE lower(subject_area_nm) = lower('NAP_PRODUCT'));
  
commit transaction;
