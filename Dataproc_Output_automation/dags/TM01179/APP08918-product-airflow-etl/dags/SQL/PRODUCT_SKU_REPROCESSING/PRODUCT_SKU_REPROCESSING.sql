--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=OneTime;AppPhase=OneTime;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;

TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_RESYNC_LDG;
--.IF ERRORCODE <> 0 THEN .QUIT 1

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_resync_ldg (epm_sku_num, rms_sku_num, channel_country)
(SELECT DISTINCT b.epm_sku_num,
  b.rms_sku_num,
  b.channel_country
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_style_dim AS a
  INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS b ON a.epm_style_num = b.epm_style_num AND LOWER(a.channel_country) = LOWER(b
     .channel_country)
 WHERE (LOWER(COALESCE(FORMAT('%11d', a.sbclass_num), '-1')) <> LOWER(COALESCE(FORMAT('%11d', b.sbclass_num), '-1')) OR
           LOWER(COALESCE(FORMAT('%11d', a.class_num), '-1')) <> LOWER(COALESCE(FORMAT('%11d', b.class_num), '-1')) OR
          LOWER(COALESCE(FORMAT('%11d', a.dept_num), '-1')) <> LOWER(COALESCE(FORMAT('%11d', b.dept_num), '-1')) OR
         LOWER(COALESCE(a.sbclass_desc, '-1')) <> LOWER(COALESCE(b.sbclass_desc, '-1')) OR LOWER(COALESCE(a.class_desc,
          '-1')) <> LOWER(COALESCE(b.class_desc, '-1')) OR LOWER(COALESCE(a.dept_desc, '-1')) <> LOWER(COALESCE(b.dept_desc
         , '-1')) OR LOWER(COALESCE(a.style_desc, '-1')) <> LOWER(COALESCE(b.style_desc, '-1')) OR LOWER(COALESCE(a.vendor_label_name
       , '-1')) <> LOWER(COALESCE(b.brand_name, '-1')))
  AND CAST(a.eff_end_tmstp AS DATE) > CURRENT_DATE('PST8PDT')
  AND CAST(b.eff_end_tmstp AS DATE) > CURRENT_DATE('PST8PDT'));

--.IF ERRORCODE <> 0 THEN .QUIT 2

--COLLECT STATS ON {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_RESYNC_LDG;

--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW;
--.IF ERRORCODE <> 0 THEN .QUIT 3


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW
( rms_sku_num
, epm_sku_num
, channel_country
, sku_short_desc
, sku_desc
, web_sku_num
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
, pack_orderable_code
, pack_orderable_desc
, pack_sellable_code
, pack_sellable_desc
, pack_simple_code
, pack_simple_desc
, display_seq_1
, display_seq_2
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
, NRML.cmpy_num
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
, NRML.pack_orderable_code
, NRML.pack_orderable_desc
, NRML.pack_sellable_code
, NRML.pack_sellable_desc
, NRML.pack_simple_code
, NRML.pack_simple_desc
, NRML.display_seq_1
, NRML.display_seq_2
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
-- ,IF(NRML.eff_period, CURRENT_TIMESTAMP(), NULL) as eff_begin
-- --, NRML.eff_period   AS eff_end
-- ,IF(NRML.eff_period, CURRENT_TIMESTAMP(), NULL) as eff_end
, CAST(RANGE_START(NRML.eff_period)AS TIMESTAMP) AS eff_begin
, CAST(RANGE_END(NRML.eff_period) AS TIMESTAMP)  AS eff_end
,jwn_udf.udf_time_zone(cast(RANGE_START(NRML.eff_period) as string)) as eff_begin_tmstp_tz
,jwn_udf.udf_time_zone(cast(RANGE_end(NRML.eff_period) as string)) as eff_end_tmstp_tz

FROM (
--NONSEQUENCED VALIDTIME
SELECT DISTINCT
  SRC.rms_sku_num
, SRC.epm_sku_num
, SRC.channel_country
, SRC.sku_short_desc
, SRC.sku_desc
, SRC.web_sku_num
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
, SRC.pack_orderable_code
, SRC.pack_orderable_desc
, SRC.pack_sellable_code
, SRC.pack_sellable_desc
, SRC.pack_simple_code
, SRC.pack_simple_desc
, SRC.display_seq_1
, SRC.display_seq_2
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
, COALESCE( RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period
FROM (
	SELECT DISTINCT
	  src0.rms_sku_num
	, src0.epm_sku_num
	, src0.channel_country
	, src0.sku_short_desc
	, src0.sku_desc
	, src0.web_sku_num
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
	, COALESCE(styl.sbclass_num, src0.sbclass_num) AS sbclass_num
	, COALESCE(styl.sbclass_desc, src0.sbclass_desc) AS sbclass_desc
	, COALESCE(styl.class_num, src0.class_num) AS class_num
	, COALESCE(styl.class_desc, src0.class_desc) AS class_desc
	, COALESCE(styl.dept_num, src0.dept_num) AS dept_num
	, COALESCE(styl.dept_desc, dept.dept_name) AS dept_desc
	, dept.subdivision_num AS grp_num
	, dept.subdivision_short_name AS grp_desc
	, dept.division_num AS div_num
	, dept.division_short_name AS div_desc
	, 1000 AS cmpy_num
	, 'Nordstrom' AS cmpy_desc
	,(CASE WHEN src0.sku_type_code='f' THEN choice.nrf_color_num ELSE src0.color_num END) AS color_num
	,(CASE WHEN src0.sku_type_code='f' THEN choice.color_desc ELSE src0.color_desc END) AS color_desc
	,(CASE WHEN src0.sku_type_code='f' THEN choice.nord_display_color ELSE src0.nord_display_color END) AS nord_display_color
	, src0.nrf_size_code AS nrf_size_code
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
	, src0.sku_type_code
	, src0.sku_type_desc
	, src0.pack_orderable_code
	, src0.pack_orderable_desc
	, src0.pack_sellable_code
	, src0.pack_sellable_desc
	, src0.pack_simple_code
	, src0.pack_simple_desc
	, src0.display_seq_1
	, src0.display_seq_2
  , src0.hazardous_material_class_desc AS hazardous_material_class_desc
  , src0.hazardous_material_class_code AS hazardous_material_class_code
  , src0.fulfillment_type_code AS fulfillment_type_code
	, src0.selling_channel_eligibility_list AS selling_channel_eligibility_list
	, src0.smart_sample_ind AS smart_sample_ind
	, src0.gwp_ind AS gwp_ind
	, src0.msrp_amt AS msrp_amt
	, src0.msrp_currency_code AS msrp_currency_code
	, src0.npg_ind AS npg_ind
	, src0.order_quantity_multiple AS order_quantity_multiple
	, src0.fp_forecast_eligible_ind AS fp_forecast_eligible_ind
	, src0.fp_item_planning_eligible_ind AS fp_item_planning_eligible_ind
	, src0.fp_replenishment_eligible_ind AS fp_replenishment_eligible_ind
	, src0.op_forecast_eligible_ind AS op_forecast_eligible_ind
	, src0.op_item_planning_eligible_ind AS op_item_planning_eligible_ind
	, src0.op_replenishment_eligible_ind AS op_replenishment_eligible_ind
	, src0.size_range_desc AS size_range_desc
	, src0.size_sequence_num AS size_sequence_num
	, src0.size_range_code AS size_range_code
	/*, COALESCE((CASE WHEN styl.epm_style_num > 0
	     THEN (RANGE_INTERSECT(RANGE(TIMESTAMP(src0.eff_begin_tmstp), TIMESTAMP(src0.eff_end_tmstp)) ,RANGE(TIMESTAMP(styl.eff_begin_tmstp),TIMESTAMP(styl.eff_end_tmstp))))
		 ELSE RANGE(src0.eff_begin_tmstp, src0.eff_end_tmstp) END
	     ,
		 CASE WHEN dept.dept_num>0
		 THEN (RANGE_INTERSECT(RANGE(src0.eff_begin_tmstp, src0.eff_end_tmstp) ,RANGE(dept.eff_begin_tmstp,dept.eff_end_tmstp)))
		 ELSE RANGE(src0.eff_begin_tmstp, src0.eff_end_tmstp) END
	     ,
		 CASE WHEN choice.epm_choice_num>0
		 THEN (RANGE_INTERSECT(RANGE(src0.eff_begin_tmstp, src0.eff_end_tmstp) , RANGE(choice.eff_begin_tmstp,choice.eff_end_tmstp)))
		 ELSE PERIOD(src0.eff_begin_tmstp, src0.eff_end_tmstp) END
	     )
	          , RANGE(src0.eff_begin_tmstp, src0.eff_end_tmstp) ) AS eff_period */
  
  	,COALESCE ((CASE WHEN styl.epm_style_num > 0
	     THEN (RANGE_INTERSECT(RANGE((src0.eff_begin_tmstp_utc), (src0.eff_end_tmstp_utc)) ,RANGE((styl.eff_begin_tmstp_utc),(styl.eff_end_tmstp_utc))))
		-- ELSE RANGE(src0.eff_begin_tmstp, src0.eff_end_tmstp) END
	     
		  WHEN dept.dept_num>0
		 THEN (RANGE_INTERSECT(RANGE((src0.eff_begin_tmstp_utc), (src0.eff_end_tmstp_utc)) ,RANGE((dept.eff_begin_tmstp_utc),(dept.eff_end_tmstp_utc))))
		-- ELSE RANGE(src0.eff_begin_tmstp, src0.eff_end_tmstp) END
	     
		  WHEN choice.epm_choice_num>0
		 THEN (RANGE_INTERSECT(RANGE((src0.eff_begin_tmstp_utc), (src0.eff_end_tmstp_utc)) , RANGE((choice.eff_begin_tmstp_utc),(choice.eff_end_tmstp_utc))))
		 --ELSE PERIOD(src0.eff_begin_tmstp, src0.eff_end_tmstp) END
	     
	          else RANGE((src0.eff_begin_tmstp_utc), (src0.eff_end_tmstp_utc)) END )) AS eff_period 
  


FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST SRC0
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST STYL
ON src0.epm_style_num = styl.epm_style_num
AND src0.channel_country = styl.channel_country
AND RANGE_OVERLAPS(RANGE(src0.eff_begin_tmstp_utc, src0.eff_end_tmstp_utc) , RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc))

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DEPARTMENT_DIM_HIST dept
ON dept.dept_num = CAST(FLOOR(CAST(TRIM(CAST(styl.dept_num AS STRING))AS FLOAT64)) AS INT64)
AND RANGE_OVERLAPS(RANGE(src0.eff_begin_tmstp_utc, src0.eff_end_tmstp_utc) ,RANGE(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc))
AND RANGE_OVERLAPS(RANGE((styl.eff_begin_tmstp_utc), (styl.eff_end_tmstp_utc)) , RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_CHOICE_DIM_HIST CHOICE
ON src0.epm_choice_num = choice.epm_choice_num
AND src0.channel_country = choice.channel_country
AND src0.sku_type_code='f'  --Only Fashion Sku's get color definition from Choice record
AND RANGE_OVERLAPS(RANGE(src0.eff_begin_tmstp_utc, src0.eff_end_tmstp_utc) , RANGE(choice.eff_begin_tmstp_utc, choice.eff_end_tmstp_utc))
AND RANGE_OVERLAPS(RANGE((styl.eff_begin_tmstp_utc), (styl.eff_end_tmstp_utc)) , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))
AND RANGE_OVERLAPS(RANGE(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc) , RANGE(choice.eff_begin_tmstp_utc, choice.eff_end_tmstp_utc))

JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_RESYNC_LDG R_SKU
ON SRC0.EPM_SKU_NUM=R_SKU.EPM_SKU_NUM
AND SRC0.RMS_SKU_NUM=R_SKU.RMS_SKU_NUM
AND SRC0.CHANNEL_COUNTRY=R_SKU.CHANNEL_COUNTRY
) SRC
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST TGT
ON SRC.rms_sku_num = TGT.rms_sku_num
AND SRC.channel_country = TGT.channel_country
AND RANGE_OVERLAPS(RANGE(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc),(SRC.eff_period)  )

WHERE TGT.rms_sku_num IS NULL
 OR (SRC.epm_sku_num <> TGT.epm_sku_num OR TGT.epm_sku_num IS NULL AND SRC.epm_sku_num IS NOT NULL)
 OR (SRC.epm_sku_num IS NULL AND TGT.epm_sku_num IS NOT NULL OR (LOWER(SRC.sku_short_desc) <> LOWER(TGT.sku_short_desc)
     OR TGT.sku_short_desc IS NULL AND SRC.sku_short_desc IS NOT NULL))
 OR (SRC.sku_short_desc IS NULL AND TGT.sku_short_desc IS NOT NULL OR (LOWER(SRC.sku_desc) <> LOWER(TGT.sku_desc) OR TGT
       .sku_desc IS NULL AND SRC.sku_desc IS NOT NULL) OR (SRC.sku_desc IS NULL AND TGT.sku_desc IS NOT NULL OR SRC.web_sku_num
        <> TGT.web_sku_num OR (TGT.web_sku_num IS NULL AND SRC.web_sku_num IS NOT NULL OR SRC.web_sku_num IS NULL AND
        TGT.web_sku_num IS NOT NULL)))
 OR (LOWER(SRC.rms_style_num) <> LOWER(TGT.rms_style_num) OR (TGT.rms_style_num IS NULL AND SRC.rms_style_num
        IS NOT NULL OR SRC.rms_style_num IS NULL AND TGT.rms_style_num IS NOT NULL) OR (SRC.epm_style_num <> TGT.epm_style_num
        OR (TGT.epm_style_num IS NULL AND SRC.epm_style_num IS NOT NULL OR SRC.epm_style_num IS NULL AND TGT.epm_style_num
         IS NOT NULL)) OR (LOWER(SRC.partner_relationship_num) <> LOWER(TGT.partner_relationship_num) OR (TGT.partner_relationship_num
         IS NULL AND SRC.partner_relationship_num IS NOT NULL OR SRC.partner_relationship_num IS NULL AND TGT.partner_relationship_num
         IS NOT NULL) OR (LOWER(SRC.partner_relationship_type_code) <> LOWER(TGT.partner_relationship_type_code) OR TGT
         .partner_relationship_type_code IS NULL AND SRC.partner_relationship_type_code IS NOT NULL OR (SRC.partner_relationship_type_code
          IS NULL AND TGT.partner_relationship_type_code IS NOT NULL OR SRC.web_style_num <> TGT.web_style_num))))
 OR (TGT.web_style_num IS NULL AND SRC.web_style_num IS NOT NULL OR (SRC.web_style_num IS NULL AND TGT.web_style_num
         IS NOT NULL OR LOWER(SRC.style_desc) <> LOWER(TGT.style_desc)) OR (TGT.style_desc IS NULL AND SRC.style_desc
        IS NOT NULL OR (SRC.style_desc IS NULL AND TGT.style_desc IS NOT NULL OR SRC.epm_choice_num <> TGT.epm_choice_num
          )) OR (TGT.epm_choice_num IS NULL AND SRC.epm_choice_num IS NOT NULL OR (SRC.epm_choice_num IS NULL AND TGT.epm_choice_num
          IS NOT NULL OR LOWER(SRC.supp_part_num) <> LOWER(TGT.supp_part_num)) OR (TGT.supp_part_num IS NULL AND SRC.supp_part_num
          IS NOT NULL OR SRC.supp_part_num IS NULL AND TGT.supp_part_num IS NOT NULL OR (LOWER(SRC.prmy_supp_num) <>
           LOWER(TGT.prmy_supp_num) OR TGT.prmy_supp_num IS NULL AND SRC.prmy_supp_num IS NOT NULL))) OR (SRC.prmy_supp_num
        IS NULL AND TGT.prmy_supp_num IS NOT NULL OR (LOWER(SRC.manufacturer_num) <> LOWER(TGT.manufacturer_num) OR TGT
          .manufacturer_num IS NULL AND SRC.manufacturer_num IS NOT NULL) OR (SRC.manufacturer_num IS NULL AND TGT.manufacturer_num
         IS NOT NULL OR (SRC.sbclass_num <> TGT.sbclass_num OR TGT.sbclass_num IS NULL AND SRC.sbclass_num IS NOT NULL)
       ) OR (SRC.sbclass_num IS NULL AND TGT.sbclass_num IS NOT NULL OR (LOWER(SRC.sbclass_desc) <> LOWER(TGT.sbclass_desc
            ) OR TGT.sbclass_desc IS NULL AND SRC.sbclass_desc IS NOT NULL) OR (SRC.sbclass_desc IS NULL AND TGT.sbclass_desc
           IS NOT NULL OR SRC.class_num <> TGT.class_num OR (TGT.class_num IS NULL AND SRC.class_num IS NOT NULL OR SRC
            .class_num IS NULL AND TGT.class_num IS NOT NULL)))))
 OR (LOWER(SRC.class_desc) <> LOWER(TGT.class_desc) OR (TGT.class_desc IS NULL AND SRC.class_desc IS NOT NULL OR SRC.class_desc
          IS NULL AND TGT.class_desc IS NOT NULL) OR (SRC.dept_num <> TGT.dept_num OR (TGT.dept_num IS NULL AND SRC.dept_num
           IS NOT NULL OR SRC.dept_num IS NULL AND TGT.dept_num IS NOT NULL)) OR (LOWER(SRC.dept_desc) <> LOWER(TGT.dept_desc
          ) OR (TGT.dept_desc IS NULL AND SRC.dept_desc IS NOT NULL OR SRC.dept_desc IS NULL AND TGT.dept_desc
           IS NOT NULL) OR (SRC.grp_num <> TGT.grp_num OR TGT.grp_num IS NULL AND SRC.grp_num IS NOT NULL OR (SRC.grp_num
            IS NULL AND TGT.grp_num IS NOT NULL OR LOWER(SRC.grp_desc) <> LOWER(TGT.grp_desc)))) OR (TGT.grp_desc
         IS NULL AND SRC.grp_desc IS NOT NULL OR (SRC.grp_desc IS NULL AND TGT.grp_desc IS NOT NULL OR SRC.div_num <>
           TGT.div_num) OR (TGT.div_num IS NULL AND SRC.div_num IS NOT NULL OR (SRC.div_num IS NULL AND TGT.div_num
            IS NOT NULL OR LOWER(SRC.div_desc) <> LOWER(TGT.div_desc))) OR (TGT.div_desc IS NULL AND SRC.div_desc
          IS NOT NULL OR (SRC.div_desc IS NULL AND TGT.div_desc IS NOT NULL OR CAST(SRC.cmpy_num AS FLOAT64) <> TGT.cmpy_num
            ) OR (TGT.cmpy_num IS NULL OR LOWER(SRC.cmpy_desc) <> LOWER(TGT.cmpy_desc) OR (TGT.cmpy_desc IS NULL OR
             LOWER(SRC.color_num) <> LOWER(TGT.color_num))))) OR (TGT.color_num IS NULL AND SRC.color_num IS NOT NULL OR
         (SRC.color_num IS NULL AND TGT.color_num IS NOT NULL OR LOWER(SRC.color_desc) <> LOWER(TGT.color_desc)) OR (TGT
          .color_desc IS NULL AND SRC.color_desc IS NOT NULL OR (SRC.color_desc IS NULL AND TGT.color_desc IS NOT NULL
           OR LOWER(SRC.nord_display_color) <> LOWER(TGT.nord_display_color))) OR (TGT.nord_display_color IS NULL AND
          SRC.nord_display_color IS NOT NULL OR (SRC.nord_display_color IS NULL AND TGT.nord_display_color IS NOT NULL
           OR LOWER(SRC.nrf_size_code) <> LOWER(TGT.nrf_size_code)) OR (TGT.nrf_size_code IS NULL AND SRC.nrf_size_code
            IS NOT NULL OR SRC.nrf_size_code IS NULL AND TGT.nrf_size_code IS NOT NULL OR (LOWER(SRC.size_1_num) <>
             LOWER(TGT.size_1_num) OR TGT.size_1_num IS NULL AND SRC.size_1_num IS NOT NULL))) OR (SRC.size_1_num
          IS NULL AND TGT.size_1_num IS NOT NULL OR (LOWER(SRC.size_1_desc) <> LOWER(TGT.size_1_desc) OR TGT.size_1_desc
            IS NULL AND SRC.size_1_desc IS NOT NULL) OR (SRC.size_1_desc IS NULL AND TGT.size_1_desc IS NOT NULL OR (LOWER(SRC
              .size_2_num) <> LOWER(TGT.size_2_num) OR TGT.size_2_num IS NULL AND SRC.size_2_num IS NOT NULL)) OR (SRC.size_2_num
           IS NULL AND TGT.size_2_num IS NOT NULL OR (LOWER(SRC.size_2_desc) <> LOWER(TGT.size_2_desc) OR TGT.size_2_desc
             IS NULL AND SRC.size_2_desc IS NOT NULL) OR (SRC.size_2_desc IS NULL AND TGT.size_2_desc IS NOT NULL OR
             LOWER(SRC.supp_color) <> LOWER(TGT.supp_color) OR (TGT.supp_color IS NULL AND SRC.supp_color IS NOT NULL OR
              SRC.supp_color IS NULL AND TGT.supp_color IS NOT NULL))))))
 OR (LOWER(SRC.supp_size) <> LOWER(TGT.supp_size) OR (TGT.supp_size IS NULL AND SRC.supp_size IS NOT NULL OR SRC.supp_size
           IS NULL AND TGT.supp_size IS NOT NULL) OR (LOWER(SRC.brand_name) <> LOWER(TGT.brand_name) OR (TGT.brand_name
            IS NULL AND SRC.brand_name IS NOT NULL OR SRC.brand_name IS NULL AND TGT.brand_name IS NOT NULL)) OR (LOWER(SRC
           .return_disposition_code) <> LOWER(TGT.return_disposition_code) OR (TGT.return_disposition_code IS NULL AND
            SRC.return_disposition_code IS NOT NULL OR SRC.return_disposition_code IS NULL AND TGT.return_disposition_code
            IS NOT NULL) OR (LOWER(SRC.selling_status_code) <> LOWER(TGT.selling_status_code) OR TGT.selling_status_code
            IS NULL AND SRC.selling_status_code IS NOT NULL OR (SRC.selling_status_code IS NULL AND TGT.selling_status_code
             IS NOT NULL OR SRC.live_date <> TGT.live_date))) OR (TGT.live_date IS NULL AND SRC.live_date IS NOT NULL OR
          (SRC.live_date IS NULL AND TGT.live_date IS NOT NULL OR LOWER(SRC.drop_ship_eligible_ind) <> LOWER(TGT.drop_ship_eligible_ind
             )) OR (TGT.drop_ship_eligible_ind IS NULL AND SRC.drop_ship_eligible_ind IS NOT NULL OR (SRC.drop_ship_eligible_ind
             IS NULL AND TGT.drop_ship_eligible_ind IS NOT NULL OR LOWER(SRC.sku_type_code) <> LOWER(TGT.sku_type_code)
           )) OR (TGT.sku_type_code IS NULL AND SRC.sku_type_code IS NOT NULL OR (SRC.sku_type_code IS NULL AND TGT.sku_type_code
             IS NOT NULL OR LOWER(SRC.sku_type_desc) <> LOWER(TGT.sku_type_desc)) OR (TGT.sku_type_desc IS NULL AND SRC
             .sku_type_desc IS NOT NULL OR SRC.sku_type_desc IS NULL AND TGT.sku_type_desc IS NOT NULL OR (LOWER(SRC.pack_orderable_code
               ) <> LOWER(TGT.pack_orderable_code) OR TGT.pack_orderable_code IS NULL AND SRC.pack_orderable_code
              IS NOT NULL)))) OR (SRC.pack_orderable_code IS NULL AND TGT.pack_orderable_code IS NOT NULL OR (LOWER(SRC
             .pack_orderable_desc) <> LOWER(TGT.pack_orderable_desc) OR TGT.pack_orderable_desc IS NULL AND SRC.pack_orderable_desc
            IS NOT NULL) OR (SRC.pack_orderable_desc IS NULL AND TGT.pack_orderable_desc IS NOT NULL OR (LOWER(SRC.pack_sellable_code
              ) <> LOWER(TGT.pack_sellable_code) OR TGT.pack_sellable_code IS NULL AND SRC.pack_sellable_code
             IS NOT NULL)) OR (SRC.pack_sellable_code IS NULL AND TGT.pack_sellable_code IS NOT NULL OR (LOWER(SRC.pack_sellable_desc
              ) <> LOWER(TGT.pack_sellable_desc) OR TGT.pack_sellable_desc IS NULL AND SRC.pack_sellable_desc
             IS NOT NULL) OR (SRC.pack_sellable_desc IS NULL AND TGT.pack_sellable_desc IS NOT NULL OR LOWER(SRC.pack_simple_code
              ) <> LOWER(TGT.pack_simple_code) OR (TGT.pack_simple_code IS NULL AND SRC.pack_simple_code IS NOT NULL OR
              SRC.pack_simple_code IS NULL AND TGT.pack_simple_code IS NOT NULL))) OR (LOWER(SRC.pack_simple_desc) <>
           LOWER(TGT.pack_simple_desc) OR (TGT.pack_simple_desc IS NULL AND SRC.pack_simple_desc IS NOT NULL OR SRC.pack_simple_desc
             IS NULL AND TGT.pack_simple_desc IS NOT NULL) OR (SRC.display_seq_1 <> TGT.display_seq_1 OR (TGT.display_seq_1
              IS NULL AND SRC.display_seq_1 IS NOT NULL OR SRC.display_seq_1 IS NULL AND TGT.display_seq_1 IS NOT NULL)
          ) OR (SRC.display_seq_2 <> TGT.display_seq_2 OR (TGT.display_seq_2 IS NULL AND SRC.display_seq_2 IS NOT NULL
             OR SRC.display_seq_2 IS NULL AND TGT.display_seq_2 IS NOT NULL) OR (LOWER(SRC.hazardous_material_class_desc
               ) <> LOWER(TGT.hazardous_material_class_desc) OR TGT.hazardous_material_class_desc IS NULL AND SRC.hazardous_material_class_desc
              IS NOT NULL OR (SRC.hazardous_material_class_desc IS NULL AND TGT.hazardous_material_class_desc
               IS NOT NULL OR LOWER(SRC.hazardous_material_class_code) <> LOWER(TGT.hazardous_material_class_code))))))
   OR (TGT.hazardous_material_class_code IS NULL AND SRC.hazardous_material_class_code IS NOT NULL OR (SRC.hazardous_material_class_code
            IS NULL AND TGT.hazardous_material_class_code IS NOT NULL OR LOWER(SRC.fulfillment_type_code) <> LOWER(TGT.fulfillment_type_code
             )) OR (TGT.fulfillment_type_code IS NULL AND SRC.fulfillment_type_code IS NOT NULL OR (SRC.fulfillment_type_code
             IS NULL AND TGT.fulfillment_type_code IS NOT NULL OR LOWER(SRC.selling_channel_eligibility_list) <> LOWER(TGT
              .selling_channel_eligibility_list))) OR (TGT.selling_channel_eligibility_list IS NULL AND SRC.selling_channel_eligibility_list
           IS NOT NULL OR (SRC.selling_channel_eligibility_list IS NULL AND TGT.selling_channel_eligibility_list
             IS NOT NULL OR LOWER(SRC.smart_sample_ind) <> LOWER(TGT.smart_sample_ind)) OR (TGT.smart_sample_ind IS NULL
             AND SRC.smart_sample_ind IS NOT NULL OR SRC.smart_sample_ind IS NULL AND TGT.smart_sample_ind IS NOT NULL
           OR (LOWER(SRC.gwp_ind) <> LOWER(TGT.gwp_ind) OR TGT.gwp_ind IS NULL AND SRC.gwp_ind IS NOT NULL))) OR (SRC.gwp_ind
           IS NULL AND TGT.gwp_ind IS NOT NULL OR (SRC.msrp_amt <> TGT.msrp_amt OR TGT.msrp_amt IS NULL AND SRC.msrp_amt
             IS NOT NULL) OR (SRC.msrp_amt IS NULL AND TGT.msrp_amt IS NOT NULL OR (LOWER(SRC.msrp_currency_code) <>
              LOWER(TGT.msrp_currency_code) OR TGT.msrp_currency_code IS NULL AND SRC.msrp_currency_code IS NOT NULL))
        OR (SRC.msrp_currency_code IS NULL AND TGT.msrp_currency_code IS NOT NULL OR (LOWER(SRC.npg_ind) <> LOWER(TGT.npg_ind
               ) OR TGT.npg_ind IS NULL AND SRC.npg_ind IS NOT NULL) OR (SRC.npg_ind IS NULL AND TGT.npg_ind IS NOT NULL
               OR SRC.order_quantity_multiple <> TGT.order_quantity_multiple OR (TGT.order_quantity_multiple IS NULL AND
               SRC.order_quantity_multiple IS NOT NULL OR SRC.order_quantity_multiple IS NULL AND TGT.order_quantity_multiple
               IS NOT NULL)))) OR (LOWER(SRC.fp_forecast_eligible_ind) <> LOWER(TGT.fp_forecast_eligible_ind) OR (TGT.fp_forecast_eligible_ind
             IS NULL AND SRC.fp_forecast_eligible_ind IS NOT NULL OR SRC.fp_forecast_eligible_ind IS NULL AND TGT.fp_forecast_eligible_ind
             IS NOT NULL) OR (LOWER(SRC.fp_item_planning_eligible_ind) <> LOWER(TGT.fp_item_planning_eligible_ind) OR (TGT
              .fp_item_planning_eligible_ind IS NULL AND SRC.fp_item_planning_eligible_ind IS NOT NULL OR SRC.fp_item_planning_eligible_ind
              IS NULL AND TGT.fp_item_planning_eligible_ind IS NOT NULL)) OR (LOWER(SRC.fp_replenishment_eligible_ind)
            <> LOWER(TGT.fp_replenishment_eligible_ind) OR (TGT.fp_replenishment_eligible_ind IS NULL AND SRC.fp_replenishment_eligible_ind
              IS NOT NULL OR SRC.fp_replenishment_eligible_ind IS NULL AND TGT.fp_replenishment_eligible_ind IS NOT NULL
              ) OR (LOWER(SRC.op_forecast_eligible_ind) <> LOWER(TGT.op_forecast_eligible_ind) OR TGT.op_forecast_eligible_ind
              IS NULL AND SRC.op_forecast_eligible_ind IS NOT NULL OR (SRC.op_forecast_eligible_ind IS NULL AND TGT.op_forecast_eligible_ind
               IS NOT NULL OR LOWER(SRC.op_item_planning_eligible_ind) <> LOWER(TGT.op_item_planning_eligible_ind)))) OR
        (TGT.op_item_planning_eligible_ind IS NULL AND SRC.op_item_planning_eligible_ind IS NOT NULL OR (SRC.op_item_planning_eligible_ind
              IS NULL AND TGT.op_item_planning_eligible_ind IS NOT NULL OR LOWER(SRC.op_replenishment_eligible_ind) <>
              LOWER(TGT.op_replenishment_eligible_ind)) OR (TGT.op_replenishment_eligible_ind IS NULL AND SRC.op_replenishment_eligible_ind
              IS NOT NULL OR SRC.op_replenishment_eligible_ind IS NULL AND TGT.op_replenishment_eligible_ind IS NOT NULL
               OR (LOWER(SRC.size_range_desc) <> LOWER(TGT.size_range_desc) OR TGT.size_range_desc IS NULL AND SRC.size_range_desc
               IS NOT NULL)) OR (SRC.size_range_desc IS NULL AND TGT.size_range_desc IS NOT NULL OR (LOWER(SRC.size_sequence_num
                ) <> LOWER(TGT.size_sequence_num) OR TGT.size_sequence_num IS NULL AND SRC.size_sequence_num IS NOT NULL
               ) OR (SRC.size_sequence_num IS NULL AND TGT.size_sequence_num IS NOT NULL OR LOWER(SRC.size_range_code)
               <> LOWER(TGT.size_range_code) OR (TGT.size_range_code IS NULL AND SRC.size_range_code IS NOT NULL OR SRC
                .size_range_code IS NULL AND TGT.size_range_code IS NOT NULL)))))))
)NRML
;
--.IF ERRORCODE <> 0 THEN .QUIT 4


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 5

--SEQUENCED VALIDTIME
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS TGT
WHERE EXISTS (SELECT *
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_sku_dim_vtw AS src
 WHERE epm_sku_num = TGT.epm_sku_num
  AND (channel_country) = (TGT.channel_country));



--.IF ERRORCODE <> 0 THEN .QUIT 6


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM
( rms_sku_num
, epm_sku_num
, channel_country
, sku_short_desc
, sku_desc
, web_sku_num
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
, pack_orderable_code
, pack_orderable_desc
, pack_sellable_code
, pack_sellable_desc
, pack_simple_code
, pack_simple_desc
, display_seq_1
, display_seq_2
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
, dw_batch_id
, dw_batch_date
, dw_sys_load_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
)
SELECT
  rms_sku_num
, epm_sku_num
, channel_country
, sku_short_desc
, sku_desc
, web_sku_num
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
, pack_orderable_code
, pack_orderable_desc
, pack_sellable_code
, pack_sellable_desc
, pack_simple_code
, pack_simple_desc
, display_seq_1
, display_seq_2
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
,(SELECT BATCH_ID FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL WHERE Subject_Area_Nm ='NAP_PRODUCT') AS dw_batch_id
,(SELECT CURR_BATCH_DATE FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL WHERE Subject_Area_Nm ='NAP_PRODUCT') AS dw_batch_date
, CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW
;
--.IF ERRORCODE <> 0 THEN .QUIT 7


--Check for cases where we might need to end-date certain records because the RMS9 Sku changed
--from one EPM key to another.
UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim AS TGT SET
 eff_end_tmstp = t3.new_end_tmstp ,
 eff_end_tmstp_tz= jwn_udf.udf_time_zone(CAST(t3.new_end_tmstp AS STRING))
 
 FROM 
 
 (SELECT rms_sku_num,
   channel_country,
   epm_sku_num,
   sku_desc,
   sku_type_code,
   eff_begin_tmstp,
   eff_end_tmstp AS old_eff_tmstp,
   MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num
    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS new_end_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_sku_dim
  WHERE (rms_sku_num) <> ('')
  QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp
       , epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS t3
WHERE (t3.rms_sku_num) = (TGT.rms_sku_num) AND (t3.channel_country) = (TGT.channel_country) AND t3.epm_sku_num
     = TGT.epm_sku_num AND t3.eff_begin_tmstp = TGT.eff_begin_tmstp AND t3.new_end_tmstp > TGT.eff_begin_tmstp;
--.IF ERRORCODE <> 0 THEN .QUIT 8


COMMIT TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 9

--COLLECT STATISTICS COLUMN(rms_sku_num), COLUMN(channel_country), COLUMN(epm_sku_num), COLUMN(epm_style_num), COLUMN(eff_end_tmstp)
--ON {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM;
