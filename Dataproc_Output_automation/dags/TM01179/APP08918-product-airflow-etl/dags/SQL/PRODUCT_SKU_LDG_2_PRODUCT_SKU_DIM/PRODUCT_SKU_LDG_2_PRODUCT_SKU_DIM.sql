
-- SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;
--  NONSEQUENCED VALIDTIME--Purge work table for staging temporal rows
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW;
-- .IF ERRORCODE <> 0 THEN .QUIT 1
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
, CAST(NRML.msrp_amt AS NUMERIC)
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
, RANGE_START(NRML.eff_period) AS eff_begin
, RANGE_END(NRML.eff_period)   AS eff_end
,  jwn_udf.udf_time_zone(CAST(RANGE_START(NRML.eff_period)AS string)) as eff_begin_tmstp_tz,
  jwn_udf.udf_time_zone( CAST(RANGE_END(NRML.eff_period) AS string)) as eff_end_tmstp_tz

FROM (
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
, COALESCE( RANGE_INTERSECT(SRC.eff_period , RANGE((TGT.eff_begin_tmstp_utc),(TGT.eff_end_tmstp_utc)))
          , SRC.eff_period ) AS eff_period
					
FROM (
	SELECT DISTINCT
	  src0.LegacyRmsSkuId AS rms_sku_num
	, src0.Id AS epm_sku_num
	, src0.MarketCode AS channel_country
	, src0.ShortDescription AS sku_short_desc
	, src0.Description AS sku_desc
	, src0.WebSkuId AS web_sku_num
	, src0.brandlabelid AS brand_label_num
	, src0.brandlabeldisplayname AS brand_label_display_name
	, src0.legacyrmsstyleids_ AS rms_style_num
	, src0.ParentEPMStyleId AS epm_style_num
	, src0.PartnerRelationshipId as partner_relationship_num
	, src0.PartnerRelationshipType as partner_relationship_type_code
	, src0.WebStyleId AS web_style_num
	, styl.style_desc
	, src0.ParentEPMChoiceId AS epm_choice_num
	, src0.vendor_part_num AS supp_part_num
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
	, src0.Sizes_EdiNrfSizeCode AS nrf_size_code
	, src0.Sizes_Size1Code AS size_1_num
	, src0.Sizes_Size1Description AS size_1_desc
	, src0.Sizes_Size2Code AS size_2_num
	, src0.Sizes_Size2Description AS size_2_desc
	, src0.vendor_color AS supp_color
	, src0.vendor_size AS supp_size
	, styl.vendor_label_name AS brand_name
	, src0.ReturnDispositionCode AS return_disposition_code
	, src0.ReturnDispositionDescription AS return_disposition_desc
	, src0.sellingstatuscode AS selling_status_code
	, src0.SellingStatusDescription AS selling_status_desc
	, src0.live_date
	, src0.drop_ship_eligible_ind
	, 'f' AS sku_type_code
	, 'Fashion Sku' AS sku_type_desc
    , src0.hazardousmaterialclassdescription AS hazardous_material_class_desc
    , src0.hazardousmaterialclasscode AS hazardous_material_class_code
	, src0.fulfillmenttypeeligibilities_fulfillmenttypecode AS fulfillment_type_code
	, src0.sellingchanneleligibilities AS selling_channel_eligibility_list
	, CASE
	  WHEN LOWER(src0.issample)=LOWER('true') THEN 'Y'
	  WHEN LOWER(src0.issample)=LOWER('false') THEN 'N'
	  ELSE 'N'
	  END AS smart_sample_ind
	, COALESCE(CASE WHEN (src0.Description LIKE '%GWP%' and styl.class_desc IN ('NO COST', 'CTS/BOL')) THEN 'Y'
	  ELSE 'N'
	  END,'N') AS gwp_ind
	, src0.msrpamtplaintxt AS msrp_amt
	, src0.msrpcurrcycd AS msrp_currency_code
	, src0.vendorupcs_vendors_isnpgvendor AS npg_ind
	, src0.vendorupcs_vendors_orderquantitymultiple AS order_quantity_multiple
	, src0.isfpforecasteligible AS fp_forecast_eligible_ind
	, src0.isfpitemplanningeligible AS fp_item_planning_eligible_ind
	, src0.isfpreplenishmenteligible AS fp_replenishment_eligible_ind
	, src0.isopforecasteligible AS op_forecast_eligible_ind
	, src0.isopitemplanningeligible AS op_item_planning_eligible_ind
	, src0.isopreplenishmenteligible AS op_replenishment_eligible_ind
	, src0.sizes_rangedescription AS size_range_desc
	, src0.sizes_seqnum AS size_sequence_num
	, src0.sizes_rangecode AS size_range_code

	, COALESCE(
	(
		RANGE_INTERSECT(RANGE_INTERSECT(CASE
			WHEN styl.epm_style_num > 0 THEN (
				RANGE_INTERSECT(SRC0.eff_period , RANGE(styl.eff_begin_tmstp_utc,styl.eff_end_tmstp_utc))
			)
			ELSE SRC0.eff_period
		END , CASE
			WHEN dept.dept_num > 0 THEN (
				RANGE_INTERSECT(SRC0.eff_period , RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))
			)
			ELSE SRC0.eff_period
		END) , CASE
			WHEN choice.epm_choice_num > 0 THEN (
				RANGE_INTERSECT(SRC0.eff_period , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))
			)
			ELSE SRC0.eff_period
		END)
	),
	SRC0.eff_period) AS eff_period
FROM (
	SELECT DISTINCT
	LegacyRmsSkuId,
	Id,
	MarketCode,
	ShortDescription,
	Description,
	WebSkuId,
	BrandLabelId,
	BrandLabelDisplayName,
	legacyrmsstyleids_,
	ParentEPMStyleId,
	PartnerRelationshipId,
	(
		CASE
			WHEN (
				(TRIM(PartnerRelationshipType) IS NULL)
				OR (LOWER(TRIM(PartnerRelationshipType)) = LOWER(''))
			) THEN 'UNKNOWN'
			ELSE PartnerRelationshipType
		END
	) AS PartnerRelationshipType,
	WebStyleId,
	ParentEPMChoiceId,
	Sizes_EdiNrfSizeCode,
	Sizes_Size1Code,
	Sizes_Size1Description,
	Sizes_Size2Code,
	Sizes_Size2Description,
	ReturnDispositionCode,
	ReturnDispositionDescription,
	sellingstatuscode,
	SellingStatusDescription,
	fulfillmenttypeeligibilities_fulfillmenttypecode,
	live_date,
	(
		CASE
			WHEN LOWER(drop_ship_eligible_ind) = LOWER('true') THEN 'Y'
			ELSE 'N'
		END
	) AS drop_ship_eligible_ind,
	prmy_supp_num,
	vendor_part_num,
	vendor_color,
	vendor_size,
	manufacturer_num,
	hazardousmaterialclassdescription,
	hazardousmaterialclasscode,
	sellingchanneleligibilities,
	issample,
	msrpamtplaintxt,
	msrpcurrcycd,
	(
		CASE
			WHEN LOWER(vendorupcs_vendors_isnpgvendor) = LOWER('true') THEN 'Y'
			ELSE 'N'
		END
	) AS vendorupcs_vendors_isnpgvendor,
	CAST(FLOOR(CAST(vendorupcs_vendors_orderquantitymultiple AS FLOAT64)) AS INTEGER) AS vendorupcs_vendors_orderquantitymultiple,
	isfpforecasteligible,
	isfpitemplanningeligible,
	isfpreplenishmenteligible,
	isopforecasteligible,
	isopitemplanningeligible,
	isopreplenishmenteligible,
	sizes_rangedescription,
	sizes_seqnum,
	sizes_rangecode,
	RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
FROM(
	SELECT t27.legacyrmsskuid,
 t27.id,
 t27.marketcode,
 t27.shortdescription,
 t27.description,
 t27.webskuid,
 t27.brandlabeldisplayname,
 t27.brandlabelid,
 t27.legacyrmsstyleids_,
 t27.parentepmstyleid,
 t27.partnerrelationshipid,
 t27.partnerrelationshiptype,
 t27.webstyleid,
 t27.parentepmchoiceid,
 t27.sizes_edinrfsizecode,
 t27.sizes_size1code,
 t27.sizes_size1description,
 t27.sizes_size2code,
 t27.sizes_size2description,
 t27.returndispositioncode,
 t27.returndispositiondescription,
 t27.sellingstatuscode,
 t27.sellingstatusdescription,
 t27.fulfillmenttypeeligibilities_fulfillmenttypecode,
 t27.live_date,
 t27.hazardousmaterialclassdescription,
 t27.hazardousmaterialclasscode,
 t27.sellingchanneleligibilities,
 t27.issample,
 t27.msrpamtplaintxt,
 t27.msrpcurrcycd,
 t27.drop_ship_eligible_ind,
 t27.prmy_supp_num,
 t27.prmy_supp_name,
 t27.vendor_part_num,
 t27.vendor_color,
 t27.vendor_size,
 t27.manufacturer_num,
 t27.vendorupcs_vendors_isnpgvendor,
 t27.vendorupcs_vendors_orderquantitymultiple,
 t27.isfpforecasteligible,
 t27.isfpitemplanningeligible,
 t27.isfpreplenishmenteligible,
 t27.isopforecasteligible,
 t27.isopitemplanningeligible,
 t27.isopreplenishmenteligible,
 t27.sizes_rangedescription,
 t27.sizes_seqnum,
 t27.sizes_rangecode,
 t27.eff_begin_tmstp,
 
 
 COALESCE(MAX(t27.eff_begin_tmstp) OVER (PARTITION BY t27.id, t27.marketcode ORDER BY t27.eff_begin_tmstp ROWS BETWEEN 1
   FOLLOWING AND 1 FOLLOWING), '9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp,
   
FROM (SELECT DISTINCT ldg.legacyrmsskuid,
   ldg.id,
   ldg.marketcode,
   ldg.shortdescription,
   ldg.description,
   ldg.webskuid,
   ldg.brandlabeldisplayname,
   ldg.brandlabelid,
   ldg.legacyrmsstyleids_,
   CAST(ldg.parentepmstyleid AS BIGINT) AS parentepmstyleid,
   ldg.partnerrelationshipid,
   ldg.partnerrelationshiptype,
   CAST(ldg.webstyleid AS BIGINT) AS webstyleid,
   CAST(ldg.parentepmchoiceid AS BIGINT) AS parentepmchoiceid,
   ldg.sizes_edinrfsizecode,
   ldg.sizes_size1code,
   ldg.sizes_size1description,
   ldg.sizes_size2code,
   ldg.sizes_size2description,
   ldg.returndispositioncode,
   ldg.returndispositiondescription,
    CASE
    WHEN t24.rms_sku_num IS NULL
    THEN 'S1'
    ELSE t24.selling_status_code
    END AS sellingstatuscode,
    CASE
    WHEN t24.rms_sku_num IS NULL
    THEN 'Unsellable'
    ELSE t24.selling_status_desc
    END AS sellingstatusdescription,
   ldg.fulfillmenttypeeligibilities_fulfillmenttypecode,
   t24.live_date,
   ldg.hazardousmaterialclassdescription,
   ldg.hazardousmaterialclasscode,
    CASE
    WHEN t24.rms_sku_num IS NULL
    THEN 'No Eligible Channels'
    ELSE t24.selling_channel_eligibility_list
    END AS sellingchanneleligibilities,
   ldg.issample,
   ldg.msrpamtplaintxt,
   ldg.msrpcurrcycd,
   /* Updating the drop_ship_eligible_ind logic as part of DASPRODUCT-2969
	,(CASE WHEN vendorupcs_vendors_isdropshipeligible='true' THEN 'Y' ELSE 'N' END) AS drop_ship_eligible_ind */
   MAX(CASE
     WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_isdropshipeligible
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS drop_ship_eligible_ind,
   MAX(CASE
     WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_number
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS prmy_supp_num,
   MAX(CASE
     WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_name
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS prmy_supp_name,
   MAX(CASE
     WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_vendorproductnumbers_code
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS vendor_part_num,
   MAX(CASE
     WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_vendorcolordescription
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS vendor_color,
   MAX(CASE
     WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_vendorsizedescription
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS vendor_size,
   MAX(CASE
     WHEN LOWER(ldg.vendor_is_manufacturer) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_number
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS manufacturer_num,
     /* Updating the NPG_IND logic as part of DASPRODUCT-2969
	, (CASE WHEN vendorupcs_vendors_isnpgvendor='true' THEN 'Y' ELSE 'N' END) AS vendorupcs_vendors_isnpgvendor */
   MAX(CASE
    /* A vendor for an item can be a Supplier and a Manufacturer, so we are considering only supplier
	         for the npg_ind calculation*/
     WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_isnpgvendor
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS vendorupcs_vendors_isnpgvendor,
    /* Updating the order_quantity_multiple logic as part of DASPRODUCT-2969
	, CAST(vendorupcs_vendors_orderquantitymultiple AS INTEGER) AS vendorupcs_vendors_orderquantitymultiple */
   MAX(CASE
     WHEN LOWER(ldg.vendor_is_supplier) = LOWER('true') AND LOWER(ldg.vendorupcs_vendors_isprimary) = LOWER('true')
     THEN ldg.vendorupcs_vendors_orderquantitymultiple
     ELSE NULL
     END) OVER (PARTITION BY ldg.id, ldg.marketcode, ldg.sourcepublishtimestamp RANGE BETWEEN UNBOUNDED PRECEDING AND
    UNBOUNDED FOLLOWING) AS vendorupcs_vendors_orderquantitymultiple,
    CASE
    WHEN LOWER(ldg.isfpforecasteligible) = LOWER('true')
    THEN 'Y'
    ELSE 'N'
    END AS isfpforecasteligible,
    CASE
    WHEN LOWER(ldg.isfpitemplanningeligible) = LOWER('true')
    THEN 'Y'
    ELSE 'N'
    END AS isfpitemplanningeligible,
    CASE
    WHEN LOWER(ldg.isfpreplenishmenteligible) = LOWER('true')
    THEN 'Y'
    ELSE 'N'
    END AS isfpreplenishmenteligible,
    CASE
    WHEN LOWER(ldg.isopforecasteligible) = LOWER('true')
    THEN 'Y'
    ELSE 'N'
    END AS isopforecasteligible,
    CASE
    WHEN LOWER(ldg.isopitemplanningeligible) = LOWER('true')
    THEN 'Y'
    ELSE 'N'
    END AS isopitemplanningeligible,
    CASE
    WHEN LOWER(ldg.isopreplenishmenteligible) = LOWER('true')
    THEN 'Y'
    ELSE 'N'
    END AS isopreplenishmenteligible,
   ldg.sizes_rangedescription,
   ldg.sizes_seqnum,
   ldg.sizes_rangecode,
   TIMESTAMP(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,'')))  AS eff_begin_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_LDG AS ldg
  	-- The right to sell attributes needs to be retrieved before the period is calculated to avoid missing data
   LEFT JOIN (
      -- SELECT CURRENT EFFECTIVE
      SELECT xref.rms_sku_num,
      xref.channel_country,
       CASE
       WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
       THEN 'S1'
       WHEN LOWER(t2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
       THEN 'C3'
       ELSE NULL
       END AS derived_selling_status_code,
       CASE
       WHEN (CASE
          WHEN LOWER(xref.selling_status_code) = LOWER('BLOCKED')
          THEN 'S1'
          WHEN LOWER(t2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
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
          WHEN LOWER(t2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
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
          WHEN LOWER(t2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
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
       WHEN LOWER(t2.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       WHEN LOWER(xref.selling_status_code) = LOWER('UNBLOCKED')
       THEN 'C3'
       ELSE NULL
       END AS selling_status_code,
      xref.live_date,
      xref.selling_channel_eligibility_list,
      xref.eff_begin_tmstp,
      xref.eff_end_tmstp
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_ITEM_SELLING_RIGHTS_XREF AS xref
      INNER JOIN (SELECT legacyrmsskuid AS rms_sku_num,
        marketcode AS channel_country,
        TIMESTAMP(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))) AS source_publish_timestamp,
        fulfillmenttypeeligibilities_fulfillmenttypecode AS fulfillment_type_code
       FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_LDG
       WHERE legacyrmsskuid IS NOT NULL
       GROUP BY marketcode,
        legacyrmsskuid,
        fulfillmenttypeeligibilities_fulfillmenttypecode,
        source_publish_timestamp) AS t2 
        ON LOWER(xref.rms_sku_num) = LOWER(t2.rms_sku_num) 
        AND LOWER(xref.channel_country) = LOWER(t2.channel_country) 
        AND xref.selling_channel_eligibility_list IS NOT NULL
        AND RANGE_CONTAINS(RANGE(XREF.eff_begin_tmstp,XREF.eff_end_tmstp) , source_publish_timestamp)
     UNION ALL
     SELECT xref0.rms_sku_num,
      xref0.channel_country,
       CASE
       WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
       THEN 'S1'
       WHEN LOWER(t9.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
       THEN 'C3'
       ELSE NULL
       END AS derived_selling_status_code,
       CASE
       WHEN (CASE
          WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
          THEN 'S1'
          WHEN LOWER(t9.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
             ))
          THEN 'T1'
          WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
          THEN 'C3'
          ELSE NULL
          END) = ('S1')
       THEN 'Unsellable'
       WHEN (CASE
          WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
          THEN 'S1'
          WHEN LOWER(t9.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
             ))
          THEN 'T1'
          WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
          THEN 'C3'
          ELSE NULL
          END) = ('C3')
       THEN 'Sellable'
       WHEN (CASE
          WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
          THEN 'S1'
          WHEN LOWER(t9.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
             ))
          THEN 'T1'
          WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
          THEN 'C3'
          ELSE NULL
          END) = ('T1')
       THEN 'Dropship'
       ELSE NULL
       END AS selling_status_desc,
       CASE
       WHEN LOWER(xref0.selling_status_code) = LOWER('BLOCKED')
       THEN 'S1'
       WHEN LOWER(t9.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       WHEN LOWER(xref0.selling_status_code) = LOWER('UNBLOCKED')
       THEN 'C3'
       ELSE NULL
       END AS selling_status_code,
      xref0.live_date,
      xref0.selling_channel_eligibility_list,
      t9.source_publish_timestamp  AS eff_begin_tmstp,
      xref0.eff_end_tmstp
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_ITEM_SELLING_RIGHTS_XREF AS xref0
      INNER JOIN (SELECT t7.rms_sku_num,
        t7.channel_country,
        t7.source_publish_timestamp,
        t7.fulfillment_type_code
       FROM (SELECT legacyrmsskuid AS rms_sku_num,
          marketcode AS channel_country,
          TIMESTAMP(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))) AS source_publish_timestamp,
          fulfillmenttypeeligibilities_fulfillmenttypecode AS fulfillment_type_code
         FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_LDG
         WHERE legacyrmsskuid IS NOT NULL
         GROUP BY marketcode,
          legacyrmsskuid,
          fulfillmenttypeeligibilities_fulfillmenttypecode,
          source_publish_timestamp) AS t7
        LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_ITEM_SELLING_RIGHTS_XREF AS xref1 ON LOWER(xref1.rms_sku_num) = LOWER(t7.rms_sku_num
            ) AND LOWER(xref1.channel_country) = LOWER(t7.channel_country) AND xref1.selling_channel_eligibility_list
         IS NOT NULL
        AND RANGE_CONTAINS(RANGE(XREF1.eff_begin_tmstp,XREF1.eff_end_tmstp) , source_publish_timestamp)
       WHERE xref1.rms_sku_num IS NULL) AS t9 ON LOWER(xref0.rms_sku_num) = LOWER(t9.rms_sku_num) AND LOWER(xref0.channel_country
            ) = LOWER(t9.channel_country) AND xref0.eff_begin_tmstp >= t9.source_publish_timestamp
           AND t9.source_publish_timestamp < xref0.eff_end_tmstp AND xref0.selling_channel_eligibility_list
       IS NOT NULL
     QUALIFY (ROW_NUMBER() OVER (PARTITION BY xref0.rms_sku_num, xref0.channel_country ORDER BY t9.source_publish_timestamp
          )) = 1
     UNION ALL
     SELECT xref2.rms_sku_num,
      xref2.channel_country,
       CASE
       WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
       THEN 'S1'
       WHEN LOWER(t20.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       ELSE 'C3'
       END AS derived_selling_status_code,
       CASE
       WHEN (CASE
          WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
          THEN 'S1'
          WHEN LOWER(t20.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
             ))
          THEN 'T1'
          ELSE 'C3'
          END) = ('S1')
       THEN 'Unsellable'
       WHEN (CASE
          WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
          THEN 'S1'
          WHEN LOWER(t20.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
             ))
          THEN 'T1'
          ELSE 'C3'
          END) = ('C3')
       THEN 'Sellable'
       WHEN (CASE
          WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
          THEN 'S1'
          WHEN LOWER(t20.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
             ))
          THEN 'T1'
          ELSE 'C3'
          END) = ('T1')
       THEN 'Dropship'
       ELSE NULL
       END AS selling_status_desc,
       CASE
       WHEN LOWER(xref_4.selling_status_code) = LOWER('BLOCKED')
       THEN 'S1'
       WHEN LOWER(t20.fulfillment_type_code) IN (LOWER('3040'), LOWER('4000'), LOWER('4020'), LOWER('4040'), LOWER('4060'
          ))
       THEN 'T1'
       ELSE 'C3'
       END AS selling_status_code,
      xref2.live_date,
      'No Eligible Channels' AS selling_channel_eligibility_list,
      xref2.eff_begin_tmstp,
      CAST('0000-01-01 00:00:00' AS TIMESTAMP) AS eff_end_tmstp
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_ITEM_SELLING_RIGHTS_XREF AS xref2
      INNER JOIN (SELECT t18.rms_sku_num,
        t18.channel_country,
        t18.source_publish_timestamp,
        t18.fulfillment_type_code
       FROM (SELECT t16.rms_sku_num,
          t16.channel_country,
          t16.source_publish_timestamp,
          t16.fulfillment_type_code
         FROM (SELECT legacyrmsskuid AS rms_sku_num,
            marketcode AS channel_country,
            TIMESTAMP(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))) AS source_publish_timestamp,
            fulfillmenttypeeligibilities_fulfillmenttypecode AS fulfillment_type_code
           FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_LDG
           WHERE legacyrmsskuid IS NOT NULL
           GROUP BY marketcode,
            legacyrmsskuid,
            fulfillmenttypeeligibilities_fulfillmenttypecode,
            source_publish_timestamp) AS t16
          LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_ITEM_SELLING_RIGHTS_XREF AS xref10 ON LOWER(xref10.rms_sku_num) = LOWER(t16.rms_sku_num
              ) AND LOWER(xref10.channel_country) = LOWER(t16.channel_country) AND xref10.selling_channel_eligibility_list
           IS NOT NULL
         AND RANGE_CONTAINS(RANGE(eff_begin_tmstp,eff_end_tmstp) , source_publish_timestamp)
         WHERE xref10.rms_sku_num IS NULL) AS t18
        LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_ITEM_SELLING_RIGHTS_XREF AS xref20 ON LOWER(xref20.rms_sku_num) = LOWER(t18.rms_sku_num
              ) AND LOWER(xref20.channel_country) = LOWER(t18.channel_country) AND xref20.eff_begin_tmstp >= t18.source_publish_timestamp 
             AND t18.source_publish_timestamp < xref20.eff_end_tmstp AND xref20
         .selling_channel_eligibility_list IS NOT NULL
       WHERE xref20.rms_sku_num IS NULL) AS t20 ON LOWER(xref2.rms_sku_num) = LOWER(t20.rms_sku_num) AND LOWER(xref2.channel_country
            ) = LOWER(t20.channel_country) AND xref2.eff_begin_tmstp <= t20.source_publish_timestamp
           AND t20.source_publish_timestamp >= xref2.eff_end_tmstp AND xref2.selling_channel_eligibility_list
       IS NOT NULL
      LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_ITEM_SELLING_RIGHTS_XREF AS xref_4 ON LOWER(t20.rms_sku_num) = LOWER(xref_4.rms_sku_num
          ) AND LOWER(t20.channel_country) = LOWER(xref_4.channel_country) AND xref_4.selling_channel_eligibility_list
       IS NULL
         AND RANGE_CONTAINS(RANGE(XREF_4.eff_begin_tmstp,XREF_4.eff_end_tmstp) , source_publish_timestamp)
     QUALIFY (ROW_NUMBER() OVER (PARTITION BY xref2.rms_sku_num, xref2.channel_country ORDER BY t20.source_publish_timestamp
          DESC)) = 1) AS t24 ON LOWER(t24.rms_sku_num) = LOWER(ldg.legacyrmsskuid) AND LOWER(t24.channel_country) =
     LOWER(ldg.marketcode)
    AND RANGE_CONTAINS(RANGE(t24.eff_begin_tmstp,t24.eff_end_tmstp) , TIMESTAMP(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))) )
     ) AS t27
QUALIFY t27.eff_begin_tmstp < COALESCE(MAX(t27.eff_begin_tmstp) OVER (PARTITION BY t27.id, t27.marketcode
    ORDER BY t27.eff_begin_tmstp ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), '9999-12-31 23:59:59.999999+00:00')
	) SRC_1
) SRC0
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST STYL
ON src0.ParentEPMStyleId = styl.epm_style_num
AND LOWER(src0.MarketCode) = LOWER(styl.channel_country)
AND RANGE_OVERLAPS(src0.eff_period , RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc))


LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DEPARTMENT_DIM_HIST dept
ON dept.dept_num = styl.dept_num
AND RANGE_OVERLAPS(src0.eff_period , RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))
AND RANGE_OVERLAPS(RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc) , RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_CHOICE_DIM_HIST CHOICE
ON src0.ParentEPMChoiceId = choice.epm_choice_num
AND LOWER(src0.MarketCode) = LOWER(choice.channel_country)
AND RANGE_OVERLAPS(src0.eff_period , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))
AND RANGE_OVERLAPS(RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc) , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))
AND RANGE_OVERLAPS(RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)) , RANGE((choice.eff_begin_tmstp_utc), (choice.eff_end_tmstp_utc)))

UNION ALL

--This second subquery scans "recently loaded" SKU records to see if the parent Style/Choice information
--has changed.  We do this because our strategy of loading data every few hours increases the probability
--that we might get the SKU record loaded BEFORE the parent Style/Choice record was loaded.
SELECT DISTINCT
  sku0.rms_sku_num
, sku0.epm_sku_num
, sku0.channel_country
, sku0.sku_short_desc
, sku0.sku_desc
, sku0.web_sku_num
, sku0.brand_label_num
, sku0.brand_label_display_name
, sku0.rms_style_num
, sku0.epm_style_num
, sku0.partner_relationship_num
, sku0.partner_relationship_type_code
, sku0.web_style_num
, styl.style_desc
, sku0.epm_choice_num
, sku0.supp_part_num
, sku0.prmy_supp_num
, sku0.manufacturer_num
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
, sku0.nrf_size_code
, sku0.size_1_num
, sku0.size_1_desc
, sku0.size_2_num
, sku0.size_2_desc
, sku0.supp_color
, sku0.supp_size
, styl.vendor_label_name AS brand_name
, sku0.return_disposition_code
, sku0.return_disposition_desc
, sku0.selling_status_code
, sku0.selling_status_desc
, sku0.live_date
, sku0.drop_ship_eligible_ind
, sku0.sku_type_code
, sku0.sku_type_desc
, sku0.hazardous_material_class_desc
, sku0.hazardous_material_class_code
, sku0.fulfillment_type_code
, sku0.selling_channel_eligibility_list
, sku0.smart_sample_ind
, sku0.gwp_ind
, sku0.msrp_amt
, sku0.msrp_currency_code
, sku0.npg_ind
, sku0.order_quantity_multiple
, sku0.fp_forecast_eligible_ind
, sku0.fp_item_planning_eligible_ind
, sku0.fp_replenishment_eligible_ind
, sku0.op_forecast_eligible_ind
, sku0.op_item_planning_eligible_ind
, sku0.op_replenishment_eligible_ind
, sku0.size_range_desc
, sku0.size_sequence_num
, sku0.size_range_code
, COALESCE ((CASE WHEN styl.epm_style_num > 0
       THEN (RANGE_INTERSECT(RANGE(SKU0.eff_begin_tmstp_utc,SKU0.eff_end_tmstp_utc) ,RANGE(styl.eff_begin_tmstp_utc,styl.eff_end_tmstp_utc)))
    -- ELSE RANGE(SKU0.eff_begin_tmstp, SKU0.eff_end_tmstp) END
       
      WHEN dept.dept_num>0
     THEN (RANGE_INTERSECT(RANGE(SKU0.eff_begin_tmstp_utc,SKU0.eff_end_tmstp_utc) ,RANGE(dept.eff_begin_tmstp_utc,dept.eff_end_tmstp_utc)))
    -- ELSE RANGE(SKU0.eff_begin_tmstp, SKU0.eff_end_tmstp) END
       
      WHEN choice.epm_choice_num>0
     THEN (RANGE_INTERSECT(RANGE(SKU0.eff_begin_tmstp_utc, SKU0.eff_end_tmstp_utc) , RANGE(choice.eff_begin_tmstp_utc,choice.eff_end_tmstp_utc)))
     --ELSE PERIOD(SKU0.eff_begin_tmstp, SKU0.eff_end_tmstp) END
       
            else RANGE(SKU0.eff_begin_tmstp_utc,SKU0.eff_end_tmstp_utc) END )) AS eff_period 

FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST SKU0
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST STYL
ON sku0.epm_style_num = styl.epm_style_num
AND LOWER(sku0.channel_country) = LOWER(styl.channel_country)
AND RANGE_OVERLAPS(RANGE(sku0.eff_begin_tmstp_utc,sku0.eff_end_tmstp_utc) , RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc))

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DEPARTMENT_DIM_HIST dept
ON dept.dept_num = styl.dept_num
AND RANGE_OVERLAPS(RANGE(sku0.eff_begin_tmstp,sku0.eff_end_tmstp) , RANGE(dept.eff_begin_tmstp, dept.eff_end_tmstp))
AND RANGE_OVERLAPS(RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc) , RANGE(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc))

LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_CHOICE_DIM_HIST CHOICE
ON sku0.epm_choice_num = choice.epm_choice_num
AND LOWER(sku0.channel_country) = LOWER(choice.channel_country)
AND RANGE_OVERLAPS(RANGE(sku0.eff_begin_tmstp,sku0.eff_end_tmstp) , RANGE(choice.eff_begin_tmstp, choice.eff_end_tmstp))
AND RANGE_OVERLAPS(RANGE(styl.eff_begin_tmstp_utc, styl.eff_end_tmstp_utc) , RANGE(choice.eff_begin_tmstp_utc,choice.eff_end_tmstp_utc))
AND RANGE_OVERLAPS(RANGE(dept.eff_begin_tmstp, dept.eff_end_tmstp) , RANGE(choice.eff_begin_tmstp, choice.eff_end_tmstp))

WHERE TIMESTAMP(sku0.dw_sys_load_tmstp) >= CAST(CURRENT_DATETIME('PST8PDT') AS TIMESTAMP) -INTERVAL '2' DAY
  AND sku0.sku_type_code = 'f'
  AND NOT EXISTS (
       SELECT 1 FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_LDG LDG
	   WHERE ldg.id = sku0.epm_sku_num AND ldg.marketcode = sku0.channel_country
      )
) SRC
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_SKU_DIM_HIST TGT
ON LOWER(SRC.rms_sku_num) = LOWER(TGT.rms_sku_num)
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
AND RANGE_OVERLAPS(SRC.eff_period , RANGE((TGT.eff_begin_tmstp_utc), (TGT.eff_end_tmstp_utc)))
WHERE ( TGT.rms_sku_num IS NULL OR
   (  (SRC.epm_sku_num <> TGT.epm_sku_num OR (SRC.epm_sku_num IS NOT NULL AND TGT.epm_sku_num IS NULL) OR (SRC.epm_sku_num IS NULL AND TGT.epm_sku_num IS NOT NULL))
   OR (SRC.sku_short_desc <> TGT.sku_short_desc OR (SRC.sku_short_desc IS NOT NULL AND TGT.sku_short_desc IS NULL) OR (SRC.sku_short_desc IS NULL AND TGT.sku_short_desc IS NOT NULL))
   OR (SRC.sku_desc <> TGT.sku_desc OR (SRC.sku_desc IS NOT NULL AND TGT.sku_desc IS NULL) OR (SRC.sku_desc IS NULL AND TGT.sku_desc IS NOT NULL))
   OR (SRC.web_sku_num <> TGT.web_sku_num OR (SRC.web_sku_num IS NOT NULL AND TGT.web_sku_num IS NULL) OR (SRC.web_sku_num IS NULL AND TGT.web_sku_num IS NOT NULL))
   OR (SRC.brand_label_num <> TGT.brand_label_num OR (SRC.brand_label_num IS NOT NULL AND TGT.brand_label_num IS NULL) OR (SRC.brand_label_num IS NULL AND TGT.brand_label_num IS NOT NULL))
   OR (SRC.brand_label_display_name <> TGT.brand_label_display_name OR (SRC.brand_label_display_name IS NOT NULL AND TGT.brand_label_display_name IS NULL) OR (SRC.brand_label_display_name IS NULL AND TGT.brand_label_display_name IS NOT NULL))
   OR (SRC.rms_style_num <> TGT.rms_style_num OR (SRC.rms_style_num IS NOT NULL AND TGT.rms_style_num IS NULL) OR (SRC.rms_style_num IS NULL AND TGT.rms_style_num IS NOT NULL))
   OR (SRC.epm_style_num <> TGT.epm_style_num OR (SRC.epm_style_num IS NOT NULL AND TGT.epm_style_num IS NULL) OR (SRC.epm_style_num IS NULL AND TGT.epm_style_num IS NOT NULL))
   OR (SRC.partner_relationship_num <> TGT.partner_relationship_num OR (SRC.partner_relationship_num IS NOT NULL AND TGT.partner_relationship_num IS NULL) OR (SRC.partner_relationship_num IS NULL AND TGT.partner_relationship_num IS NOT NULL))
   OR (SRC.partner_relationship_type_code <> TGT.partner_relationship_type_code OR (SRC.partner_relationship_type_code IS NOT NULL AND TGT.partner_relationship_type_code IS NULL) OR (SRC.partner_relationship_type_code IS NULL AND TGT.partner_relationship_type_code IS NOT NULL))
   OR (SRC.web_style_num <> TGT.web_style_num OR (SRC.web_style_num IS NOT NULL AND TGT.web_style_num IS NULL) OR (SRC.web_style_num IS NULL AND TGT.web_style_num IS NOT NULL))
   OR (SRC.style_desc <> TGT.style_desc OR (SRC.style_desc IS NOT NULL AND TGT.style_desc IS NULL) OR (SRC.style_desc IS NULL AND TGT.style_desc IS NOT NULL))
   OR (SRC.epm_choice_num <> TGT.epm_choice_num OR (SRC.epm_choice_num IS NOT NULL AND TGT.epm_choice_num IS NULL) OR (SRC.epm_choice_num IS NULL AND TGT.epm_choice_num IS NOT NULL))
   OR (SRC.supp_part_num <> TGT.supp_part_num OR (SRC.supp_part_num IS NOT NULL AND TGT.supp_part_num IS NULL) OR (SRC.supp_part_num IS NULL AND TGT.supp_part_num IS NOT NULL))
   OR (SRC.prmy_supp_num <> TGT.prmy_supp_num OR (SRC.prmy_supp_num IS NOT NULL AND TGT.prmy_supp_num IS NULL) OR (SRC.prmy_supp_num IS NULL AND TGT.prmy_supp_num IS NOT NULL))
   OR (SRC.manufacturer_num <> TGT.manufacturer_num OR (SRC.manufacturer_num IS NOT NULL AND TGT.manufacturer_num IS NULL) OR (SRC.manufacturer_num IS NULL AND TGT.manufacturer_num IS NOT NULL))
   OR (SRC.sbclass_num <> TGT.sbclass_num OR (SRC.sbclass_num IS NOT NULL AND TGT.sbclass_num IS NULL) OR (SRC.sbclass_num IS NULL AND TGT.sbclass_num IS NOT NULL))
   OR (SRC.sbclass_desc <> TGT.sbclass_desc OR (SRC.sbclass_desc IS NOT NULL AND TGT.sbclass_desc IS NULL) OR (SRC.sbclass_desc IS NULL AND TGT.sbclass_desc IS NOT NULL))
   OR (SRC.class_num <> TGT.class_num OR (SRC.class_num IS NOT NULL AND TGT.class_num IS NULL) OR (SRC.class_num IS NULL AND TGT.class_num IS NOT NULL))
   OR (SRC.class_desc <> TGT.class_desc OR (SRC.class_desc IS NOT NULL AND TGT.class_desc IS NULL) OR (SRC.class_desc IS NULL AND TGT.class_desc IS NOT NULL))
   OR (SRC.dept_num <> TGT.dept_num OR (SRC.dept_num IS NOT NULL AND TGT.dept_num IS NULL) OR (SRC.dept_num IS NULL AND TGT.dept_num IS NOT NULL))
   OR (SRC.dept_desc <> TGT.dept_desc OR (SRC.dept_desc IS NOT NULL AND TGT.dept_desc IS NULL) OR (SRC.dept_desc IS NULL AND TGT.dept_desc IS NOT NULL))
   OR (SRC.grp_num <> TGT.grp_num OR (SRC.grp_num IS NOT NULL AND TGT.grp_num IS NULL) OR (SRC.grp_num IS NULL AND TGT.grp_num IS NOT NULL))
   OR (SRC.grp_desc <> TGT.grp_desc OR (SRC.grp_desc IS NOT NULL AND TGT.grp_desc IS NULL) OR (SRC.grp_desc IS NULL AND TGT.grp_desc IS NOT NULL))
   OR (SRC.div_num <> TGT.div_num OR (SRC.div_num IS NOT NULL AND TGT.div_num IS NULL) OR (SRC.div_num IS NULL AND TGT.div_num IS NOT NULL))
   OR (SRC.div_desc <> TGT.div_desc OR (SRC.div_desc IS NOT NULL AND TGT.div_desc IS NULL) OR (SRC.div_desc IS NULL AND TGT.div_desc IS NOT NULL))
   OR (SRC.cmpy_num <> CAST(TGT.cmpy_num AS STRING) OR (SRC.cmpy_num IS NOT NULL AND TGT.cmpy_num IS NULL) OR (SRC.cmpy_num IS NULL AND TGT.cmpy_num IS NOT NULL))
   OR (SRC.cmpy_desc <> TGT.cmpy_desc OR (SRC.cmpy_desc IS NOT NULL AND TGT.cmpy_desc IS NULL) OR (SRC.cmpy_desc IS NULL AND TGT.cmpy_desc IS NOT NULL))
   OR (SRC.color_num <> TGT.color_num OR (SRC.color_num IS NOT NULL AND TGT.color_num IS NULL) OR (SRC.color_num IS NULL AND TGT.color_num IS NOT NULL))
   OR (SRC.color_desc <> TGT.color_desc OR (SRC.color_desc IS NOT NULL AND TGT.color_desc IS NULL) OR (SRC.color_desc IS NULL AND TGT.color_desc IS NOT NULL))
   OR (SRC.nord_display_color <> TGT.nord_display_color OR (SRC.nord_display_color IS NOT NULL AND TGT.nord_display_color IS NULL) OR (SRC.nord_display_color IS NULL AND TGT.nord_display_color IS NOT NULL))
   OR (SRC.nrf_size_code <> TGT.nrf_size_code OR (SRC.nrf_size_code IS NOT NULL AND TGT.nrf_size_code IS NULL) OR (SRC.nrf_size_code IS NULL AND TGT.nrf_size_code IS NOT NULL))
   OR (SRC.size_1_num <> TGT.size_1_num OR (SRC.size_1_num IS NOT NULL AND TGT.size_1_num IS NULL) OR (SRC.size_1_num IS NULL AND TGT.size_1_num IS NOT NULL))
   OR (SRC.size_1_desc <> TGT.size_1_desc OR (SRC.size_1_desc IS NOT NULL AND TGT.size_1_desc IS NULL) OR (SRC.size_1_desc IS NULL AND TGT.size_1_desc IS NOT NULL))
   OR (SRC.size_2_num <> TGT.size_2_num OR (SRC.size_2_num IS NOT NULL AND TGT.size_2_num IS NULL) OR (SRC.size_2_num IS NULL AND TGT.size_2_num IS NOT NULL))
   OR (SRC.size_2_desc <> TGT.size_2_desc OR (SRC.size_2_desc IS NOT NULL AND TGT.size_2_desc IS NULL) OR (SRC.size_2_desc IS NULL AND TGT.size_2_desc IS NOT NULL))
   OR (SRC.supp_color <> TGT.supp_color OR (SRC.supp_color IS NOT NULL AND TGT.supp_color IS NULL) OR (SRC.supp_color IS NULL AND TGT.supp_color IS NOT NULL))
   OR (SRC.supp_size <> TGT.supp_size OR (SRC.supp_size IS NOT NULL AND TGT.supp_size IS NULL) OR (SRC.supp_size IS NULL AND TGT.supp_size IS NOT NULL))
   OR (SRC.brand_name <> TGT.brand_name OR (SRC.brand_name IS NOT NULL AND TGT.brand_name IS NULL) OR (SRC.brand_name IS NULL AND TGT.brand_name IS NOT NULL))
   OR (SRC.return_disposition_code <> TGT.return_disposition_code OR (SRC.return_disposition_code IS NOT NULL AND TGT.return_disposition_code IS NULL) OR (SRC.return_disposition_code IS NULL AND TGT.return_disposition_code IS NOT NULL))
   OR (SRC.selling_status_code <> TGT.selling_status_code OR (SRC.selling_status_code IS NOT NULL AND TGT.selling_status_code IS NULL) OR (SRC.selling_status_code IS NULL AND TGT.selling_status_code IS NOT NULL))
   OR (SRC.live_date <> TGT.live_date OR (SRC.live_date IS NOT NULL AND TGT.live_date IS NULL) OR (SRC.live_date IS NULL AND TGT.live_date IS NOT NULL))
   OR (SRC.drop_ship_eligible_ind <> TGT.drop_ship_eligible_ind OR (SRC.drop_ship_eligible_ind IS NOT NULL AND TGT.drop_ship_eligible_ind IS NULL) OR (SRC.drop_ship_eligible_ind IS NULL AND TGT.drop_ship_eligible_ind IS NOT NULL))
   OR (SRC.sku_type_code <> TGT.sku_type_code OR (SRC.sku_type_code IS NOT NULL AND TGT.sku_type_code IS NULL) OR (SRC.sku_type_code IS NULL AND TGT.sku_type_code IS NOT NULL))
   OR (SRC.sku_type_desc <> TGT.sku_type_desc OR (SRC.sku_type_desc IS NOT NULL AND TGT.sku_type_desc IS NULL) OR (SRC.sku_type_desc IS NULL AND TGT.sku_type_desc IS NOT NULL))
--   OR (SRC.pack_orderable_code <> TGT.pack_orderable_code OR (SRC.pack_orderable_code IS NOT NULL AND TGT.pack_orderable_code IS NULL) OR (SRC.pack_orderable_code IS NULL AND TGT.pack_orderable_code IS NOT NULL))
--   OR (SRC.pack_orderable_desc <> TGT.pack_orderable_desc OR (SRC.pack_orderable_desc IS NOT NULL AND TGT.pack_orderable_desc IS NULL) OR (SRC.pack_orderable_desc IS NULL AND TGT.pack_orderable_desc IS NOT NULL))
--   OR (SRC.pack_sellable_code <> TGT.pack_sellable_code OR (SRC.pack_sellable_code IS NOT NULL AND TGT.pack_sellable_code IS NULL) OR (SRC.pack_sellable_code IS NULL AND TGT.pack_sellable_code IS NOT NULL))
--   OR (SRC.pack_sellable_desc <> TGT.pack_sellable_desc OR (SRC.pack_sellable_desc IS NOT NULL AND TGT.pack_sellable_desc IS NULL) OR (SRC.pack_sellable_desc IS NULL AND TGT.pack_sellable_desc IS NOT NULL))
--   OR (SRC.pack_simple_code <> TGT.pack_simple_code OR (SRC.pack_simple_code IS NOT NULL AND TGT.pack_simple_code IS NULL) OR (SRC.pack_simple_code IS NULL AND TGT.pack_simple_code IS NOT NULL))
--   OR (SRC.pack_simple_desc <> TGT.pack_simple_desc OR (SRC.pack_simple_desc IS NOT NULL AND TGT.pack_simple_desc IS NULL) OR (SRC.pack_simple_desc IS NULL AND TGT.pack_simple_desc IS NOT NULL))
   OR (SRC.hazardous_material_class_desc <> TGT.hazardous_material_class_desc OR (SRC.hazardous_material_class_desc IS NOT NULL AND TGT.hazardous_material_class_desc IS NULL) OR (SRC.hazardous_material_class_desc IS NULL AND TGT.hazardous_material_class_desc IS NOT NULL))
   OR (SRC.hazardous_material_class_code <> TGT.hazardous_material_class_code OR (SRC.hazardous_material_class_code IS NOT NULL AND TGT.hazardous_material_class_code IS NULL) OR (SRC.hazardous_material_class_code IS NULL AND TGT.hazardous_material_class_code IS NOT NULL))
   OR (SRC.fulfillment_type_code <> TGT.fulfillment_type_code OR (SRC.fulfillment_type_code IS NOT NULL AND TGT.fulfillment_type_code IS NULL) OR (SRC.fulfillment_type_code IS NULL AND TGT.fulfillment_type_code IS NOT NULL))
   OR (SRC.selling_channel_eligibility_list <> TGT.selling_channel_eligibility_list OR (SRC.selling_channel_eligibility_list IS NOT NULL AND TGT.selling_channel_eligibility_list IS NULL) OR (SRC.selling_channel_eligibility_list IS NULL AND TGT.selling_channel_eligibility_list IS NOT NULL))
   OR (SRC.smart_sample_ind <> TGT.smart_sample_ind OR (SRC.smart_sample_ind IS NOT NULL AND TGT.smart_sample_ind IS NULL) OR (SRC.smart_sample_ind IS NULL AND TGT.smart_sample_ind IS NOT NULL))
   OR (SRC.gwp_ind <> TGT.gwp_ind OR (SRC.gwp_ind IS NOT NULL AND TGT.gwp_ind IS NULL) OR (SRC.gwp_ind IS NULL AND TGT.gwp_ind IS NOT NULL))
   OR (SRC.msrp_amt <> TGT.msrp_amt OR (SRC.msrp_amt IS NOT NULL AND TGT.msrp_amt IS NULL) OR (SRC.msrp_amt IS NULL AND TGT.msrp_amt IS NOT NULL))
   OR (SRC.msrp_currency_code <> TGT.msrp_currency_code OR (SRC.msrp_currency_code IS NOT NULL AND TGT.msrp_currency_code IS NULL) OR (SRC.msrp_currency_code IS NULL AND TGT.msrp_currency_code IS NOT NULL))
   OR (SRC.npg_ind <> TGT.npg_ind OR (SRC.npg_ind IS NOT NULL AND TGT.npg_ind IS NULL) OR (SRC.npg_ind IS NULL AND TGT.npg_ind IS NOT NULL))
   OR (SRC.order_quantity_multiple <> TGT.order_quantity_multiple OR (SRC.order_quantity_multiple IS NOT NULL AND TGT.order_quantity_multiple IS NULL) OR (SRC.order_quantity_multiple IS NULL AND TGT.order_quantity_multiple IS NOT NULL))
   OR (SRC.fp_forecast_eligible_ind <> TGT.fp_forecast_eligible_ind OR (SRC.fp_forecast_eligible_ind IS NOT NULL AND TGT.fp_forecast_eligible_ind IS NULL) OR (SRC.fp_forecast_eligible_ind IS NULL AND TGT.fp_forecast_eligible_ind IS NOT NULL))
   OR (SRC.fp_item_planning_eligible_ind <> TGT.fp_item_planning_eligible_ind OR (SRC.fp_item_planning_eligible_ind IS NOT NULL AND TGT.fp_item_planning_eligible_ind IS NULL) OR (SRC.fp_item_planning_eligible_ind IS NULL AND TGT.fp_item_planning_eligible_ind IS NOT NULL))
   OR (SRC.fp_replenishment_eligible_ind <> TGT.fp_replenishment_eligible_ind OR (SRC.fp_replenishment_eligible_ind IS NOT NULL AND TGT.fp_replenishment_eligible_ind IS NULL) OR (SRC.fp_replenishment_eligible_ind IS NULL AND TGT.fp_replenishment_eligible_ind IS NOT NULL))
   OR (SRC.op_forecast_eligible_ind <> TGT.op_forecast_eligible_ind OR (SRC.op_forecast_eligible_ind IS NOT NULL AND TGT.op_forecast_eligible_ind IS NULL) OR (SRC.op_forecast_eligible_ind IS NULL AND TGT.op_forecast_eligible_ind IS NOT NULL))
   OR (SRC.op_item_planning_eligible_ind <> TGT.op_item_planning_eligible_ind OR (SRC.op_item_planning_eligible_ind IS NOT NULL AND TGT.op_item_planning_eligible_ind IS NULL) OR (SRC.op_item_planning_eligible_ind IS NULL AND TGT.op_item_planning_eligible_ind IS NOT NULL))
   OR (SRC.op_replenishment_eligible_ind <> TGT.op_replenishment_eligible_ind OR (SRC.op_replenishment_eligible_ind IS NOT NULL AND TGT.op_replenishment_eligible_ind IS NULL) OR (SRC.op_replenishment_eligible_ind IS NULL AND TGT.op_replenishment_eligible_ind IS NOT NULL))
   OR (SRC.size_range_desc <> TGT.size_range_desc OR (SRC.size_range_desc IS NOT NULL AND TGT.size_range_desc IS NULL) OR (SRC.size_range_desc IS NULL AND TGT.size_range_desc IS NOT NULL))
   OR (SRC.size_sequence_num <> TGT.size_sequence_num OR (SRC.size_sequence_num IS NOT NULL AND TGT.size_sequence_num IS NULL) OR (SRC.size_sequence_num IS NULL AND TGT.size_sequence_num IS NOT NULL))
   OR (SRC.size_range_code <> TGT.size_range_code OR (SRC.size_range_code IS NOT NULL AND TGT.size_range_code IS NULL) OR (SRC.size_range_code IS NULL AND TGT.size_range_code IS NOT NULL))
   )  )
) NRML
;
-- .IF ERRORCODE <> 0 THEN .QUIT 2
--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
-- .IF ERRORCODE <> 0 THEN .QUIT 3
-- SEQUENCED VALIDTIME
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW AS src
    WHERE epm_sku_num = tgt.epm_sku_num AND LOWER(channel_country) = LOWER(tgt.channel_country));
-- .IF ERRORCODE <> 0 THEN .QUIT 4
--Remove any historical CasePack rows that have the same RMS sku but possibly different
--EPM sku key if they have overlapping history.
-- SEQUENCED VALIDTIME
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW AS src
    WHERE epm_sku_num <> tgt.epm_sku_num AND LOWER(channel_country) = LOWER(tgt.channel_country) AND LOWER(rms_sku_num) = LOWER(tgt.rms_sku_num) AND LOWER(tgt.sku_type_code) = LOWER('P'));
-- .IF ERRORCODE <> 0 THEN .QUIT 41


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM (rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc,
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
 size_sequence_num, size_range_code, eff_begin_tmstp, eff_end_tmstp,
  eff_begin_tmstp_tz,
	eff_end_tmstp_tz, dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
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
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
     (SELECT curr_batch_date
     FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
     WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_date,
    CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_DIM_VTW) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `a12180`
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM
   WHERE epm_sku_num = t3.epm_sku_num
    AND LOWER(channel_country) = LOWER(t3.channel_country))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num, channel_country)) = 1);

-- .IF ERRORCODE <> 0 THEN .QUIT 5
--Check for cases where we might need to end-date certain records because the RMS9 Sku changed
--from one EPM key to another.
UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM AS tgt SET
 eff_end_tmstp = t3.new_end_tmstp FROM (SELECT rms_sku_num,
   channel_country,
   epm_sku_num,
   sku_desc,
   sku_type_code,
   eff_begin_tmstp,
   eff_begin_tmstp_tz,
   eff_end_tmstp AS old_eff_tmstp,
   eff_end_tmstp_tz old_eff_tmstp_tz,
   MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp, epm_sku_num
    ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS new_end_tmstp
  FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM
  WHERE LOWER(rms_sku_num) <> LOWER('')
  QUALIFY eff_end_tmstp > (MIN(eff_begin_tmstp) OVER (PARTITION BY rms_sku_num, channel_country ORDER BY eff_begin_tmstp
       , epm_sku_num ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING))) AS t3
WHERE LOWER(t3.rms_sku_num) = LOWER(tgt.rms_sku_num) 
    AND LOWER(t3.channel_country) = LOWER(tgt.channel_country) 
    AND t3.epm_sku_num = tgt.epm_sku_num 
    AND t3.eff_begin_tmstp = tgt.eff_begin_tmstp 
--Ensure that we do NOT trip the temporal PK constraint with this update
    AND t3.new_end_tmstp > tgt.eff_begin_tmstp;
-- .IF ERRORCODE <> 0 THEN .QUIT 5
--   AND RANGE_CONTAINS(RANGE(A.EFF_BEGIN_TMSTP,A.EFF_END_TMSTP) ,  timestamp(current_datetime('PST8PDT')))


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
 size_range_desc, size_sequence_num, size_range_code, eff_begin_tmstp, eff_end_tmstp,eff_begin_tmstp_tz,eff_end_tmstp_tz, dw_batch_id, dw_batch_date,
 dw_sys_load_tmstp, new_epm_eff_begin, new_epm_eff_end, old_epm_eff_begin, old_epm_eff_end, process_flag)
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
	c.eff_begin_tmstp_tz,c.eff_end_tmstp_tz,
   (SELECT batch_id
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
   WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) AS dw_batch_id,
  CURRENT_DATE('PST8PDT') AS dw_batch_date,
  CURRENT_DATETIME('PST8PDT') AS dw_sys_load_tmstp,
  MIN(b.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_begin,
  MIN(b.eff_end_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_epm_eff_end,
  MIN(c.eff_begin_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_epm_eff_begin,
  MIN(c.eff_end_tmstp_utc) OVER (PARTITION BY a.epm_sku_num, a.channel_country, a.rms_sku_num, c.epm_sku_num RANGE BETWEEN
   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS old_epm_eff_end,
  'N' AS process_flag
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS a
  INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS b 
  ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) 
  AND LOWER(a.channel_country) = LOWER(b.channel_country) 
  AND a.epm_sku_num = b.epm_sku_num
  INNER JOIN {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist AS c 
  ON LOWER(b.rms_sku_num) = LOWER(c.rms_sku_num) 
  AND LOWER(b.channel_country) = LOWER(c.channel_country) 
  AND b.epm_sku_num <> c.epm_sku_num
 WHERE 
 RANGE_OVERLAPS(RANGE(B.EFF_BEGIN_TMSTP_UTC,B.EFF_END_TMSTP_UTC), RANGE(C.EFF_BEGIN_TMSTP_UTC,C.EFF_END_TMSTP_UTC))
  AND (a.rms_sku_num, a.channel_country) IN (SELECT (rms_sku_num,
     channel_country)
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist
    GROUP BY rms_sku_num,
     channel_country
    HAVING COUNT(DISTINCT epm_sku_num) > 1)
  QUALIFY c.eff_begin_tmstp_utc >= new_epm_eff_begin);
-- .IF ERRORCODE <> 0 THEN .QUIT 7
DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM AS target
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg AS source
    WHERE LOWER(target.rms_sku_num) = LOWER(rms_sku_num) AND LOWER(target.channel_country) = LOWER(channel_country) AND target.epm_sku_num = old_epm_sku_num AND DATETIME(target.eff_begin_tmstp) >= new_epm_eff_begin AND LOWER(process_flag) = LOWER('N'));
-- .IF ERRORCODE <> 0 THEN .QUIT 8
UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_DIM.PRODUCT_SKU_DIM AS target SET
    eff_begin_tmstp = (t9.eff_end_tmstp_prev),
    dw_batch_id = (SELECT batch_id
        FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
        WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT')) FROM (SELECT rms_sku_num, epm_sku_num, channel_country, eff_begin_tmstp_UTC, eff_end_tmstp_UTC, eff_end_tmstp_prev
        FROM (SELECT rms_sku_num, epm_sku_num, channel_country, sku_short_desc, sku_desc, web_sku_num, brand_label_num, brand_label_display_name, rms_style_num, epm_style_num, partner_relationship_num, partner_relationship_type_code, web_style_num, style_desc, epm_choice_num, supp_part_num, prmy_supp_num, manufacturer_num, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, color_num, color_desc, nord_display_color, nrf_size_code, size_1_num, size_1_desc, size_2_num, size_2_desc, supp_color, supp_size, brand_name, return_disposition_code, return_disposition_desc, selling_status_code, selling_status_desc, live_date, drop_ship_eligible_ind, sku_type_code, sku_type_desc, pack_orderable_code, pack_orderable_desc, pack_sellable_code, pack_sellable_desc, pack_simple_code, pack_simple_desc, display_seq_1, display_seq_2, hazardous_material_class_code, hazardous_material_class_desc, fulfillment_type_code, selling_channel_eligibility_list, smart_sample_ind, gwp_ind, msrp_amt, msrp_currency_code, npg_ind, order_quantity_multiple, fp_forecast_eligible_ind, fp_item_planning_eligible_ind, fp_replenishment_eligible_ind, op_forecast_eligible_ind, op_item_planning_eligible_ind, op_replenishment_eligible_ind, size_range_desc, size_sequence_num, size_range_code, eff_begin_tmstp_UTC, eff_end_tmstp_UTC,eff_begin_tmstp_tz,eff_end_tmstp_tz ,dw_batch_id, dw_batch_date, dw_sys_load_tmstp, MIN(eff_end_tmstp_UTC) OVER (PARTITION BY rms_sku_num, epm_sku_num, channel_country ORDER BY eff_begin_tmstp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS eff_end_tmstp_prev
                FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_dim_hist) AS t
        WHERE (rms_sku_num, channel_country) IN (SELECT (rms_sku_num, channel_country)
                    FROM (SELECT DISTINCT rms_sku_num, channel_country
                            FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_base_vws.product_sku_overlap_stg
                            WHERE LOWER(process_flag) = LOWER('N') AND t.dw_batch_id = (SELECT batch_id
                                        FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
                                        WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'))) AS t5)
        AND eff_begin_tmstp_UTC <> eff_end_tmstp_prev) AS t9
WHERE LOWER(t9.rms_sku_num) = LOWER(target.rms_sku_num) AND LOWER(t9.channel_country) = LOWER(target.channel_country) AND t9.eff_begin_tmstp_utc = (target.eff_begin_tmstp) AND t9.eff_end_tmstp_utc = (target.eff_end_tmstp) AND t9.eff_end_tmstp_prev <> (target.eff_begin_tmstp);
-- .IF ERRORCODE <> 0 THEN .QUIT 9
UPDATE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_SKU_OVERLAP_STG SET
    process_flag = 'Y'
WHERE (process_flag) = ('N') AND dw_batch_id = (SELECT batch_id
            FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.ELT_CONTROL
            WHERE LOWER(subject_area_nm) = LOWER('NAP_PRODUCT'));
-- .IF ERRORCODE <> 0 THEN .QUIT 10
COMMIT TRANSACTION;

