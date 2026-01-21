TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_vtw;

--.IF ERRORCODE <> 0 THEN .QUIT 1

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
, CAST(FLOOR(CAST(NRML.cmpy_num AS FLOAT64)) AS INT64)
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
,  jwn_udf.udf_time_zone(CAST(RANGE_START(NRML.eff_period)AS string)) as eff_begin_tmstp_tz,
  jwn_udf.udf_time_zone( CAST(RANGE_END(NRML.eff_period) AS string)) as eff_end_tmstp_tz


FROM (
-- NONSEQUENCED VALIDTIME
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
, COALESCE( RANGE_INTERSECT(SRC.eff_period, RANGE((TGT.eff_begin_tmstp_utc),(TGT.eff_end_tmstp_utc)))
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
				RANGE_INTERSECT(SRC0.eff_period , RANGE((styl.eff_begin_tmstp_utc), (styl.eff_end_tmstp_utc)))
			)
			ELSE SRC0.eff_period
		END , CASE
			WHEN dept.dept_num > 0 THEN (
				RANGE_INTERSECT(SRC0.eff_period , RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))
			)
			ELSE SRC0.eff_period
		END ), CASE
			WHEN choice.epm_choice_num > 0 THEN (
				RANGE_INTERSECT(SRC0.eff_period , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))
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
			SELECT DISTINCT t3.rms_sku_num,
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
 t3.selling_status_code,
 t3.selling_status_desc,
 t3.live_date,
 t3.drop_ship_eligible_ind,
 t3.hazardous_material_class_desc,
 t3.hazardous_material_class_code,
 t3.fulfillment_type_code,
 t3.selling_channel_eligibility_list,
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
 t3.eff_begin_tmstp,
 t3.eff_end_tmstp,
 COALESCE(MAX(t3.eff_begin_tmstp) OVER (PARTITION BY t3.rms_sku_num, t3.channel_country ORDER BY t3.eff_begin_tmstp
   ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), CAST('0000-01-01 00:00:00' AS TIMESTAMP)) AS eff_period_end
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
    WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
    THEN 'S1'
    WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')
      )
    THEN 'T1'
    WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
    THEN 'C3'
    ELSE NULL
    END AS derived_selling_status_code,
    CASE
    WHEN (CASE
       WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
       THEN 'S1'
       WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
       THEN 'C3'
       ELSE NULL
       END) = ('S1')
    THEN 'Unsellable'
    WHEN (CASE
       WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
       THEN 'S1'
       WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
       THEN 'C3'
       ELSE NULL
       END) = ('C3')
    THEN 'Sellable'
    WHEN (CASE
       WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
       THEN 'S1'
       WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
       THEN 'C3'
       ELSE NULL
       END) = ('T1')
    THEN 'Dropship'
    ELSE NULL
    END AS selling_status_desc,
    CASE
    WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
    THEN 'S1'
    WHEN LOWER(sku.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060')
      )
    THEN 'T1'
    WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
    THEN 'C3'
    ELSE NULL
    END AS selling_status_code,
   xref.live_date,
   xref.selling_channel_eligibility_list,
   xref.eff_begin_tmstp,
   xref.eff_end_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_xref_daily_load_ldg AS xref
   INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS sku 
   ON LOWER(xref.rms_sku_num) = LOWER(sku.rms_sku_num) 
   AND LOWER(xref.channel_country) = LOWER(sku.channel_country)
   LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_selling_rights_tmp_stg AS delta_check 
   ON LOWER(xref.rms_sku_num) = LOWER(delta_check.rms_sku_num) 
   AND LOWER(xref.channel_country) = LOWER(delta_check.channel_country) 
   AND xref.eff_begin_tmstp =delta_check.sr_eff_begin_tmstp 
   AND xref.eff_end_tmstp = delta_check.sr_eff_end_tmstp

   WHERE RANGE_OVERLAPS(RANGE(SKU.eff_begin_tmstp, SKU.eff_end_tmstp),RANGE(XREF.eff_begin_tmstp, XREF.eff_end_tmstp))
   AND delta_check.rms_sku_num IS NOT NULL) AS t3
QUALIFY t3.eff_begin_tmstp < t3.eff_begin_tmstp
-- COALESCE(MAX(t3.eff_begin_tmstp) OVER (PARTITION BY t3.rms_sku_num, t3.channel_country
--    ORDER BY t3.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), CAST('0000-01-01 00:00:00' AS TIMESTAMP))
   ) SRC_1
) SRC0
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST STYL
ON (src0.epm_style_num) = (styl.epm_style_num)
AND LOWER(src0.channel_country) = LOWER(styl.channel_country)
AND RANGE_OVERLAPS(src0.eff_period , RANGE((styl.eff_begin_tmstp_utc), (styl.eff_end_tmstp_utc)))

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DEPARTMENT_DIM_HIST dept
ON (dept.dept_num) = (styl.dept_num)
AND RANGE_OVERLAPS(src0.eff_period , RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))
AND RANGE_OVERLAPS(RANGE((styl.eff_begin_tmstp_utc), (styl.eff_end_tmstp_utc)) , RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_CHOICE_DIM_HIST CHOICE
ON (src0.epm_choice_num) = (choice.epm_choice_num)
AND LOWER(src0.channel_country) = LOWER(choice.channel_country)
AND RANGE_OVERLAPS(src0.eff_period , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))
AND RANGE_OVERLAPS(RANGE((styl.eff_begin_tmstp_utc), (styl.eff_end_tmstp_utc)) , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))
AND RANGE_OVERLAPS(RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)) , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))

) SRC

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST TGT
ON LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num)
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
AND RANGE_OVERLAPS(SRC.eff_period , RANGE((TGT.eff_begin_tmstp_utc), (TGT.eff_end_tmstp_utc)))

WHERE ( TGT.rms_sku_num IS NULL OR
   (  (SRC.epm_sku_num <> TGT.epm_sku_num OR (SRC.epm_sku_num IS NOT NULL AND TGT.epm_sku_num IS NULL) OR (SRC.epm_sku_num IS NULL AND TGT.epm_sku_num IS NOT NULL))
   OR (SRC.selling_status_code <> TGT.selling_status_code OR (SRC.selling_status_code IS NOT NULL AND TGT.selling_status_code IS NULL) OR (SRC.selling_status_code IS NULL AND TGT.selling_status_code IS NOT NULL))
   OR (SRC.live_date <> TGT.live_date OR (SRC.live_date IS NOT NULL AND TGT.live_date IS NULL) OR (SRC.live_date IS NULL AND TGT.live_date IS NOT NULL))
   OR (SRC.selling_channel_eligibility_list <> TGT.selling_channel_eligibility_list OR (SRC.selling_channel_eligibility_list IS NOT NULL AND TGT.selling_channel_eligibility_list IS NULL) OR (SRC.selling_channel_eligibility_list IS NULL AND TGT.selling_channel_eligibility_list IS NOT NULL))
   )  )
) NRML
; 
-- .IF ERRORCODE <> 0 THEN .QUIT 2
--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
-- .IF ERRORCODE <> 0 THEN .QUIT 3
-- SEQUENCED VALIDTIME
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
    WHERE epm_sku_num = tgt.epm_sku_num AND LOWER(channel_country) = LOWER(tgt.channel_country));
-- .IF ERRORCODE <> 0 THEN .QUIT 4
--Remove any historical CasePack rows that have the same RMS sku but possibly different
--EPM sku key if they have overlapping history.
-- SEQUENCED VALIDTIME
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
    WHERE epm_sku_num <> tgt.epm_sku_num AND LOWER(channel_country) = LOWER(tgt.channel_country) AND LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(tgt.sku_type_code) = LOWER('P'));
-- .IF ERRORCODE <> 0 THEN .QUIT 5

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
 size_sequence_num, size_range_code, eff_begin_tmstp, eff_end_tmstp,eff_begin_tmstp_tz, eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
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
  eff_begin_tmstp_tz,
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
    eff_end_tmstp,
    eff_begin_tmstp_tz,
    eff_end_tmstp_tz,
     (SELECT batch_id
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_date,
    CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_vtw) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `a12180`
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim
   WHERE epm_sku_num = t3.epm_sku_num
    AND channel_country = t3.channel_country)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num, channel_country)) = 1);
-- .IF ERRORCODE <> 0 THEN .QUIT 6
--Check for cases where we might need to end-date certain records because the RMS9 Sku changed
--from one EPM key to another.
-- NONSEQUENCED VALIDTIME


UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS tgt SET
 eff_end_tmstp = t3.new_end_tmstp FROM (SELECT rms_sku_num,
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
  WHERE LOWER(rms_sku_num) <> LOWER('')
  QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp
       , epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS t3
WHERE LOWER(t3.rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(t3.channel_country) = LOWER(tgt.channel_country) AND t3.epm_sku_num
     = tgt.epm_sku_num AND t3.eff_begin_tmstp = tgt.eff_begin_tmstp AND t3.new_end_tmstp > tgt.eff_begin_tmstp;

-- .IF ERRORCODE <> 0 THEN .QUIT 7
INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_OVERLAP_STG
(
	new_epm_sku_num,
	rms_sku_num,
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
  eff_begin_tmstp_tz,
	eff_end_tmstp_tz,
	dw_batch_id,
	dw_batch_date,
	dw_sys_load_tmstp,
	new_epm_eff_begin,
	new_epm_eff_end,
	old_epm_eff_begin,
	old_epm_eff_end,
  new_epm_eff_begin_tz,
  new_epm_eff_end_tz,
  old_epm_eff_begin_tz,
  old_epm_eff_end_tz,
	process_flag
)
SELECT DISTINCT B.EPM_SKU_NUM AS new_epm_sku_num,
	C.rms_sku_num,
	C.epm_sku_num,
	C.channel_country,
	C.sku_short_desc,
	C.sku_desc,
	C.web_sku_num,
	C.brand_label_num,
	C.brand_label_display_name,
	C.rms_style_num,
	C.epm_style_num,
	C.partner_relationship_num,
    C.partner_relationship_type_code,
	C.web_style_num,
	C.style_desc,
	C.epm_choice_num,
	C.supp_part_num,
	C.prmy_supp_num,
	C.manufacturer_num,
	C.sbclass_num,
	C.sbclass_desc,
	C.class_num,
	C.class_desc,
	C.dept_num,
	C.dept_desc,
	C.grp_num,
	C.grp_desc,
	C.div_num,
	C.div_desc,
	C.cmpy_num,
	C.cmpy_desc,
	C.color_num,
	C.color_desc,
	C.nord_display_color,
	C.nrf_size_code,
	C.size_1_num,
	C.size_1_desc,
	C.size_2_num,
	C.size_2_desc,
	C.supp_color,
	C.supp_size,
	C.brand_name,
	C.return_disposition_code,
	C.return_disposition_desc,
	C.selling_status_code,
	C.selling_status_desc,
	C.live_date,
	C.drop_ship_eligible_ind,
	C.sku_type_code,
	C.sku_type_desc,
	C.pack_orderable_code,
	C.pack_orderable_desc,
	C.pack_sellable_code,
	C.pack_sellable_desc,
	C.pack_simple_code,
	C.pack_simple_desc,
	C.display_seq_1,
	C.display_seq_2,
	C.hazardous_material_class_desc,
	C.hazardous_material_class_code,
	C.fulfillment_type_code,
	C.selling_channel_eligibility_list,
	C.smart_sample_ind,
	C.gwp_ind,
	C.msrp_amt,
	C.msrp_currency_code,
	C.npg_ind,
	C.order_quantity_multiple,
	C.fp_forecast_eligible_ind,
	C.fp_item_planning_eligible_ind,
	C.fp_replenishment_eligible_ind,
	C.op_forecast_eligible_ind,
	C.op_item_planning_eligible_ind,
	C.op_replenishment_eligible_ind,
	C.size_range_desc,
	C.size_sequence_num,
	C.size_range_code,
	C.eff_begin_tmstp_utc,
	C.eff_end_tmstp_utc,
  C.eff_begin_tmstp_tz,
  C.eff_end_tmstp_tz,
	(SELECT BATCH_ID FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL WHERE Subject_Area_Nm ='NAP_PRODUCT') AS dw_batch_id,
	CURRENT_DATE('PST8PDT') AS dw_batch_date,
	CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp,
	MIN(B.EFF_BEGIN_TMSTP_utc) OVER(PARTITION BY A.EPM_SKU_NUM,A.CHANNEL_COUNTRY,A.RMS_SKU_NUM,C.EPM_SKU_NUM) AS new_epm_eff_begin,
	MIN(B.EFF_END_TMSTP_utc) OVER(PARTITION BY A.EPM_SKU_NUM,A.CHANNEL_COUNTRY,A.RMS_SKU_NUM,C.EPM_SKU_NUM) AS new_epm_eff_end,
	MIN(C.EFF_BEGIN_TMSTP_utc) OVER(PARTITION BY A.EPM_SKU_NUM,A.CHANNEL_COUNTRY,A.RMS_SKU_NUM,C.EPM_SKU_NUM) AS old_epm_eff_begin,
	MIN(C.EFF_END_TMSTP_utc) OVER(PARTITION BY A.EPM_SKU_NUM,A.CHANNEL_COUNTRY,A.RMS_SKU_NUM,C.EPM_SKU_NUM) AS old_epm_eff_end,

  jwn_udf.udf_time_zone( CAST(MIN(B.EFF_BEGIN_TMSTP_utc) OVER(PARTITION BY A.EPM_SKU_NUM,A.CHANNEL_COUNTRY,A.RMS_SKU_NUM,C.EPM_SKU_NUM) AS string)) as new_epm_eff_begin_tz,
  jwn_udf.udf_time_zone( CAST(MIN(B.EFF_END_TMSTP_utc) OVER(PARTITION BY A.EPM_SKU_NUM,A.CHANNEL_COUNTRY,A.RMS_SKU_NUM,C.EPM_SKU_NUM)  AS string)) as new_epm_eff_end_tz,
  jwn_udf.udf_time_zone( CAST(MIN(C.EFF_BEGIN_TMSTP_utc) OVER(PARTITION BY A.EPM_SKU_NUM,A.CHANNEL_COUNTRY,A.RMS_SKU_NUM,C.EPM_SKU_NUM)  AS string)) as old_epm_eff_begin_tz,
  jwn_udf.udf_time_zone( CAST(MIN(C.EFF_END_TMSTP_utc) OVER(PARTITION BY A.EPM_SKU_NUM,A.CHANNEL_COUNTRY,A.RMS_SKU_NUM,C.EPM_SKU_NUM) AS string)) as old_epm_eff_end_tz,
	'N' AS process_flag
FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST A
JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST B
  ON LOWER(A.RMS_SKU_NUM)=LOWER(B.rms_sku_num)
 AND LOWER(A.CHANNEL_COUNTRY)=LOWER(B.channel_country)
 AND A.EPM_SKU_NUM=B.epm_sku_num
JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST C
  ON LOWER(B.RMS_SKU_NUM)=LOWER(C.rms_sku_num)
 AND LOWER(B.CHANNEL_COUNTRY)=LOWER(C.channel_country)
 AND (B.EPM_SKU_NUM)<>(C.EPM_SKU_NUM)
WHERE RANGE_OVERLAPS(RANGE(B.EFF_BEGIN_TMSTP,B.EFF_END_TMSTP) , RANGE(C.EFF_BEGIN_TMSTP,C.EFF_END_TMSTP))
	  AND (a.rms_sku_num, a.channel_country) IN (SELECT (rms_sku_num, channel_country)
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
   GROUP BY rms_sku_num,
    channel_country
   HAVING COUNT(DISTINCT epm_sku_num) > 1)
	  AND RANGE_CONTAINS(RANGE((A.EFF_BEGIN_TMSTP_utc),(A.EFF_END_TMSTP_utc)), TIMESTAMP(CURRENT_DATETIME('PST8PDT')))
QUALIFY C.EFF_BEGIN_TMSTP_utc >=new_epm_eff_begin
; 
-- .IF ERRORCODE <> 0 THEN .QUIT 8
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS target
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg AS source
    WHERE LOWER(target.rms_sku_num) = LOWER(rms_sku_num) AND LOWER(target.channel_country) = LOWER(channel_country) AND target.epm_sku_num = old_epm_sku_num AND target.eff_begin_tmstp >= TIMESTAMP(new_epm_eff_begin) AND LOWER(process_flag) = LOWER('N'));
-- .IF ERRORCODE <> 0 THEN .QUIT 9

UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS target SET
 eff_begin_tmstp = t9.eff_end_tmstp_prev,
 eff_begin_tmstp_tz = eff_end_tmstp_tz,
 dw_batch_id = (SELECT batch_id
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
  WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'))
  FROM (SELECT rms_sku_num,
   epm_sku_num,
   channel_country,
   eff_begin_tmstp_utc,
   eff_end_tmstp_utc,
     MIN(eff_end_tmstp_utc) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY eff_begin_tmstp
      ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS eff_end_tmstp_prev
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS t
  WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
     FROM (SELECT DISTINCT rms_sku_num,
        channel_country
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg
       WHERE LOWER(process_flag) = LOWER('N')
        AND t.dw_batch_id = (SELECT batch_id
          FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
          WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'))) AS t5)
  QUALIFY eff_begin_tmstp_utc <> eff_end_tmstp_prev) AS t9
WHERE (t9.rms_sku_num) = (target.rms_sku_num) AND (t9.channel_country) = (target.channel_country)
   AND t9.eff_begin_tmstp_utc = target.eff_begin_tmstp AND t9.eff_end_tmstp_utc= target.eff_end_tmstp AND t9.eff_end_tmstp_prev
   <> target.eff_begin_tmstp;

-- .IF ERRORCODE <> 0 THEN .QUIT 10
UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_overlap_stg SET
    process_flag = 'Y'
WHERE LOWER(process_flag) = LOWER('N') AND dw_batch_id = (SELECT batch_id
            FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.elt_control
            WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'));
-- .IF ERRORCODE <> 0 THEN .QUIT 11
COMMIT TRANSACTION;

-- .IF ERRORCODE <> 0 THEN .QUIT 12
-- COLLECT STATISTICS COLUMN(rms_sku_num), COLUMN(channel_country), COLUMN(epm_sku_num), COLUMN(epm_style_num), COLUMN(eff_end_tmstp)
-- ON {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM;
