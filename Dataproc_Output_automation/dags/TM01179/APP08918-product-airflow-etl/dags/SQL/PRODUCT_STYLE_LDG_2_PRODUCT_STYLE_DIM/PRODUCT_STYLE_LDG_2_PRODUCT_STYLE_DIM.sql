--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_STYLE_DIM_VTW;
--.IF ERRORCODE <> 0 THEN .QUIT 1


INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_NAP_STG.PRODUCT_STYLE_DIM_VTW
( epm_style_num
, rms_style_num_list
, channel_country
, style_desc
, style_group_num
, style_group_desc
, style_group_short_desc
, type_level_1_num
, type_level_1_desc
, type_level_2_num
, type_level_2_desc
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
, nord_label_ind
, vendor_label_code
, vendor_label_name
, special_treatment_type_code
, size_standard
, supp_part_num
, prmy_supp_num
, manufacturer_num
, product_source_code
, product_source_desc
, genders_code
, genders_desc
, age_groups_code
, age_groups_desc
, msrp_amt
, msrp_currency_code
, hanger_type_code
, hanger_type_desc
, npg_ind
, npg_sourcing_code
, eff_begin_tmstp
, eff_end_tmstp
, eff_begin_tmstp_tz
, eff_end_tmstp_tz
)
SELECT
  NRML.epm_style_num
, NRML.rms_style_num_list
, NRML.channel_country
, NRML.style_desc
, NRML.style_group_num
, NRML.style_group_desc
, NRML.style_group_short_desc
, NRML.type_level_1_num
, NRML.type_level_1_desc
, NRML.type_level_2_num
, NRML.type_level_2_desc
, cast(FLOOR(cast(NRML.sbclass_num as float64)) as int64)
, NRML.sbclass_desc
, cast(FLOOR(cast(NRML.class_num as float64)) as INt64)
, NRML.class_desc
, cast(FLOOR(cast(NRML.dept_num as float64)) as INT64)
, NRML.dept_desc
, NRML.grp_num
, NRML.grp_desc
, NRML.div_num
, NRML.div_desc
, cast(FLOOR(cast(NRML.cmpy_num as float64)) as INT64)
, NRML.cmpy_desc
, NRML.nord_label_ind
, NRML.vendor_label_code
, NRML.vendor_label_name
, NRML.special_treatment_type_code
, NRML.size_standard
, NRML.supp_part_num
, NRML.prmy_supp_num
, NRML.manufacturer_num
, NRML.product_source_code
, NRML.product_source_desc
, NRML.genders_code
, NRML.genders_desc
, NRML.age_groups_code
, NRML.age_groups_desc
, cast(NRML.msrp_amt as NUMERIC)
, NRML.msrp_currency_code
, NRML.hanger_type_code
, NRML.hanger_type_desc
, NRML.npg_ind
, NRML.npg_sourcing_code
-- , BEGIN(NRML.eff_period) AS eff_begin
-- , END(NRML.eff_period)   AS eff_end
, CAST(RANGE_START(NRML.eff_period)AS TIMESTAMP) AS eff_begin
, CAST(RANGE_END(NRML.eff_period) AS TIMESTAMP)  AS eff_end
,  jwn_udf.udf_time_zone(CAST(RANGE_START(NRML.eff_period)AS string)) as eff_begin_tmstp_tz,
  jwn_udf.udf_time_zone( CAST(RANGE_END(NRML.eff_period) AS string)) as eff_end_tmstp_tz
FROM (
-- NONSEQUENCED VALIDTIME
SELECT DISTINCT
  SRC.epm_style_num
, SRC.rms_style_num_list
, SRC.channel_country
, SRC.style_desc
, SRC.style_group_num
, SRC.style_group_desc
, SRC.style_group_short_desc
, SRC.type_level_1_num
, SRC.type_level_1_desc
, SRC.type_level_2_num
, SRC.type_level_2_desc
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
, SRC.nord_label_ind
, SRC.vendor_label_code
, SRC.vendor_label_name
, SRC.special_treatment_type_code
, SRC.size_standard
, SRC.supp_part_num
, SRC.prmy_supp_num
, SRC.manufacturer_num
, SRC.product_source_code
, SRC.product_source_desc
, SRC.genders_code
, SRC.genders_desc
, SRC.age_groups_code
, SRC.age_groups_desc
, SRC.msrp_amt
, SRC.msrp_currency_code
, SRC.hanger_type_code
, SRC.hanger_type_desc
, SRC.npg_ind
, SRC.npg_sourcing_code
-- , COALESCE( SRC.eff_period P_INTERSECT PERIOD(TGT.eff_begin_tmstp,TGT.eff_end_tmstp)
--           , SRC.eff_period ) AS eff_period
,COALESCE( RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period
					
					

FROM (
	SELECT DISTINCT
	  src0.Id AS epm_style_num
	, src0.legacyrmsstyleids_ AS rms_style_num_list
	, src0.MarketCode AS channel_country
	, src0.Description AS style_desc
	, src0.RmsStyleGroupId AS style_group_num
	, src0.style_group_desc
	, src0.style_group_short_desc
	, src0.TypeLevel1Number AS type_level_1_num
	, src0.TypeLevel1Description AS type_level_1_desc
	, src0.TypeLevel2Number AS type_level_2_num
	, src0.TypeLevel2Description AS type_level_2_desc
	, src0.SubclassNumber AS sbclass_num
	, src0.SubclassDescription AS sbclass_desc
	, src0.ClassNumber AS class_num
	, src0.ClassDescription AS class_desc
	, src0.DepartmentNumber AS dept_num
	, src0.DepartmentDescription AS dept_desc
	, dept.subdivision_num AS grp_num
	, dept.subdivision_short_name AS grp_desc
	, dept.division_num AS div_num
	, dept.division_short_name AS div_desc
	, '1000' AS cmpy_num
	, 'Nordstrom' AS cmpy_desc
	, src0.nord_label_ind
	, src0.LabelId AS vendor_label_code
	, src0.LabelName AS vendor_label_name
	, src0.SpecialTreatmentTypeCode AS special_treatment_type_code
	, src0.Sizes_StandardCode AS size_standard
	, src0.supp_part_num
	, src0.prmy_supp_num
	, src0.manufacturer_num
	, src0.SourceCode AS product_source_code
	, src0.SourceDescription AS product_source_desc
	, src0.genders_code AS genders_code
	, src0.genders_description AS genders_desc
	, src0.agegroups_code AS age_groups_code
	, src0.agegroups_description AS age_groups_desc
	, src0.msrpamtplaintxt AS msrp_amt
	, src0.msrpcurrcycd AS msrp_currency_code
	, src0.hangertypecode AS hanger_type_code
	, src0.hangertypedescription AS hanger_type_desc
	, src0.vendorupcs_vendors_isnpgvendor AS npg_ind
	, src0.sourcingcategories_code AS npg_sourcing_code
	-- , COALESCE( SRC0.eff_period P_INTERSECT PERIOD(dept.eff_begin_tmstp, dept.eff_end_tmstp)
	--           , SRC0.eff_period ) AS eff_period
  ,COALESCE( RANGE_INTERSECT (SRC0.eff_period,range(dept.eff_begin_tmstp_utc, dept.eff_end_tmstp_utc))
          , SRC0.eff_period ) AS eff_period
	FROM (
	SELECT DISTINCT Id, legacyrmsstyleids_, MarketCode, Description, RmsStyleGroupId
	, style_group_desc, style_group_short_desc
	, TypeLevel1Number, TypeLevel1Description, TypeLevel2Number, TypeLevel2Description
	, SubclassNumber, SubclassDescription, ClassNumber, ClassDescription, DepartmentNumber, DepartmentDescription
	, nord_label_ind, LabelId, LabelName, SpecialTreatmentTypeCode, Sizes_StandardCode
	, supp_part_num, prmy_supp_num, manufacturer_num, SourceCode, SourceDescription
	, genders_code, genders_description, agegroups_code, agegroups_description, msrpamtplaintxt, msrpcurrcycd
	, hangertypecode,hangertypedescription
	,(CASE WHEN LOWER(vendorupcs_vendors_isnpgvendor)=LOWER('true') THEN 'Y' ELSE 'N' END) AS vendorupcs_vendors_isnpgvendor
	,sourcingcategories_code
	, RANGE(eff_begin_tmstp, eff_end_tmstp) AS eff_period
	 
	FROM (
	SELECT SRC_2.*
	-- , COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY id,marketcode ORDER BY eff_begin_tmstp
	--            ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	--           ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
						,COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY id, marketcode ORDER BY eff_begin_tmstp ROWS BETWEEN 1
   FOLLOWING AND 1 FOLLOWING),  TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	FROM (
	SELECT DISTINCT Id, legacyrmsstyleids_, MarketCode, Description, RmsStyleGroupId
	,(CASE WHEN RmsStyleGroupId<>'' THEN Description END) AS style_group_desc
	,(CASE WHEN RmsStyleGroupId<>'' THEN ShortDescription END) AS style_group_short_desc
	, TypeLevel1Number, TypeLevel1Description, TypeLevel2Number, TypeLevel2Description
	, SubclassNumber, SubclassDescription, ClassNumber, ClassDescription, DepartmentNumber, DepartmentDescription
	,(CASE WHEN LOWER(isNordstromLabel)=LOWER('true') THEN 'Y' ELSE 'N' END) AS nord_label_ind
	, LabelId, LabelName, SpecialTreatmentTypeCode, Sizes_StandardCode

	, MAX(CASE
	      WHEN LOWER(vendor_is_supplier)=LOWER('true') AND LOWER(vendorupcs_vendors_isprimary)=LOWER('true')
	      THEN vendorupcs_vendors_number END)
	  OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS prmy_supp_num
	, MAX(CASE
	      WHEN LOWER(vendor_is_supplier)=LOWER('true') AND LOWER(vendorupcs_vendors_isprimary)=LOWER('true')
	      THEN vendorupcs_vendors_name END)
	  OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS prmy_supp_name
	, MAX(CASE
	      WHEN LOWER(vendor_is_supplier)=LOWER('true') AND LOWER(vendorupcs_vendors_isprimary)=LOWER('true')
	      THEN vendorupcs_vendors_vendorproductnumbers_code END)
	  OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS supp_part_num
	, MAX(CASE
	      WHEN LOWER(vendor_is_manufacturer)=LOWER('true') AND LOWER(vendorupcs_vendors_isprimary)=LOWER('true')
		  THEN vendorupcs_vendors_number END)
	  OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS manufacturer_num
	, SourceCode, SourceDescription
    , genders_code, genders_description, agegroups_code, agegroups_description, msrpamtplaintxt, msrpcurrcycd
	, hangertypecode,hangertypedescription
	/* Updating the NPG_IND logic
	, (CASE WHEN vendorupcs_vendors_isnpgvendor='true' THEN 'Y' ELSE 'N' END) AS vendorupcs_vendors_isnpgvendor */
	, MAX(CASE
	      WHEN LOWER(vendor_is_supplier)=LOWER('true') AND LOWER(vendorupcs_vendors_isprimary)=LOWER('true')
	      THEN vendorupcs_vendors_isnpgvendor END)
	  OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS vendorupcs_vendors_isnpgvendor
	, sourcingcategories_code
	-- ,(NORD_UDF.ISO8601_TMSTP(sourcepublishtimestamp) (NAMED eff_begin_tmstp))

   ,cast(jwn_udf.iso8601_tmstp( COLLATE(sourcepublishtimestamp,''))as timestamp) AS eff_begin_tmstp

	FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_LDG
	) SRC_2
	QUALIFY timestamp(eff_begin_tmstp) < eff_end_tmstp
	) SRC_1
	) SRC0
	LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DEPARTMENT_DIM_HIST dept
	ON LOWER(cast(dept.dept_num as STRING)) = LOWER(TRIM(src0.DepartmentNumber))
	-- AND src0.eff_period OVERLAPS PERIOD(dept.eff_begin_tmstp,dept.eff_end_tmstp)
  AND RANGE_OVERLAPS(src0.eff_period, RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))

	UNION ALL

	--This second subquery scans "recently loaded" STYLE records to see if the parent Department information
	--has changed.  We do this because our strategy of loading data every few hours increases the probability
	--that we might get the STYLE record loaded BEFORE the parent DEPARTMENT record was loaded.
	SELECT DISTINCT
	  sty0.epm_style_num
	, sty0.rms_style_num_list
	, sty0.channel_country
	, sty0.style_desc
	, sty0.style_group_num
	, sty0.style_group_desc
	, sty0.style_group_short_desc
	, sty0.type_level_1_num
	, sty0.type_level_1_desc
	, sty0.type_level_2_num
	, sty0.type_level_2_desc
	, CAST(sty0.sbclass_num AS STRING)
	, sty0.sbclass_desc
	, CAST(sty0.class_num AS STRING)
	, sty0.class_desc
	, CAST(sty0.dept_num AS STRING)
	, sty0.dept_desc
	, dept.subdivision_num AS grp_num
	, dept.subdivision_short_name AS grp_desc
	, dept.division_num AS div_num
	, dept.division_short_name AS div_desc
	, CAST(sty0.cmpy_num AS STRING)
	, sty0.cmpy_desc
	, sty0.nord_label_ind
	, sty0.vendor_label_code
	, sty0.vendor_label_name
	, sty0.special_treatment_type_code
	, sty0.size_standard
	, sty0.supp_part_num
	, sty0.prmy_supp_num
	, sty0.manufacturer_num
	, sty0.product_source_code
	, sty0.product_source_desc
	, sty0.genders_code
	, sty0.genders_desc
	, sty0.age_groups_code
	, sty0.age_groups_desc
	, sty0.msrp_amt
	, sty0.msrp_currency_code
	, sty0.hanger_type_code
	, sty0.hanger_type_desc
	, sty0.npg_ind
	, sty0.npg_sourcing_code
	-- , COALESCE( sty0."VALIDTIME" P_INTERSECT PERIOD(dept.eff_begin_tmstp, dept.eff_end_tmstp)
	--           , sty0."VALIDTIME" ) AS eff_period
  -- ,COALESCE( RANGE_INTERSECT(range(TIMESTAMP(STY0.eff_begin_tmstp), TIMESTAMP(STY0.eff_end_tmstp)),range(TIMESTAMP(dept.eff_begin_tmstp), TIMESTAMP(dept.eff_end_tmstp)))
  --         , range(TIMESTAMP(STY0.eff_begin_tmstp), TIMESTAMP(STY0.eff_end_tmstp))) AS eff_period

  ,COALESCE( RANGE_INTERSECT(range((STY0.eff_begin_tmstp_utc), (STY0.eff_end_tmstp_utc)),range((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))
          , range((STY0.eff_begin_tmstp_utc), (STY0.eff_end_tmstp_utc))) AS eff_period,
          


	FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST STY0

	LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.DEPARTMENT_DIM_HIST dept
	ON dept.dept_num = sty0.dept_num
	AND RANGE_OVERLAPS(RANGE((sty0.eff_begin_tmstp_utc),(sty0.eff_end_tmstp_utc)) , RANGE((dept.eff_begin_tmstp_utc), (dept.eff_end_tmstp_utc)))

	WHERE (sty0.dw_sys_load_tmstp) >= CURRENT_DATETIME('PST8PDT')-INTERVAL '2' DAY
  	AND NOT EXISTS (
       SELECT 1 FROM {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_LDG LDG
	   WHERE (ldg.id) = (sty0.epm_style_num) AND LOWER(ldg.marketcode) = LOWER(sty0.channel_country)
      )
) SRC
LEFT JOIN {{params.gcp_project_id}}.{{params.dbenv}}_NAP_BASE_VWS.PRODUCT_STYLE_DIM_HIST TGT
ON SRC.epm_style_num = TGT.epm_style_num
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
-- AND SRC.eff_period OVERLAPS PERIOD(TGT.eff_begin_tmstp, TGT.eff_end_tmstp)
AND RANGE_OVERLAPS(SRC.eff_period, RANGE((TGT.eff_begin_tmstp_utc),(TGT.eff_end_tmstp_utc)) )
WHERE ( TGT.channel_country IS NULL OR
   (  (LOWER(SRC.rms_style_num_list) <> LOWER(TGT.rms_style_num_list) OR (SRC.rms_style_num_list IS NOT NULL AND TGT.rms_style_num_list IS NULL) OR (SRC.rms_style_num_list IS NULL AND TGT.rms_style_num_list IS NOT NULL))
   OR (LOWER(SRC.style_desc) <> LOWER(TGT.style_desc) OR (SRC.style_desc IS NOT NULL AND TGT.style_desc IS NULL) OR (SRC.style_desc IS NULL AND TGT.style_desc IS NOT NULL))
   OR (LOWER(SRC.style_group_num) <> LOWER(TGT.style_group_num) OR (SRC.style_group_num IS NOT NULL AND TGT.style_group_num IS NULL) OR (SRC.style_group_num IS NULL AND TGT.style_group_num IS NOT NULL))
   OR (LOWER(SRC.style_group_desc) <> LOWER(TGT.style_group_desc) OR (SRC.style_group_desc IS NOT NULL AND TGT.style_group_desc IS NULL) OR (SRC.style_group_desc IS NULL AND TGT.style_group_desc IS NOT NULL))
   OR (LOWER(SRC.style_group_short_desc) <> LOWER(TGT.style_group_short_desc) OR (SRC.style_group_short_desc IS NOT NULL AND TGT.style_group_short_desc IS NULL) OR (SRC.style_group_short_desc IS NULL AND TGT.style_group_short_desc IS NOT NULL))
   OR (LOWER(SRC.type_level_1_num) <> LOWER(TGT.type_level_1_num) OR (SRC.type_level_1_num IS NOT NULL AND TGT.type_level_1_num IS NULL) OR (SRC.type_level_1_num IS NULL AND TGT.type_level_1_num IS NOT NULL))
   OR (LOWER(SRC.type_level_1_desc) <> LOWER(TGT.type_level_1_desc) OR (SRC.type_level_1_desc IS NOT NULL AND TGT.type_level_1_desc IS NULL) OR (SRC.type_level_1_desc IS NULL AND TGT.type_level_1_desc IS NOT NULL))
   OR (LOWER(SRC.type_level_2_num) <> LOWER(TGT.type_level_2_num) OR (SRC.type_level_2_num IS NOT NULL AND TGT.type_level_2_num IS NULL) OR (SRC.type_level_2_num IS NULL AND TGT.type_level_2_num IS NOT NULL))
   OR (LOWER(SRC.type_level_2_desc) <> LOWER(TGT.type_level_2_desc) OR (SRC.type_level_2_desc IS NOT NULL AND TGT.type_level_2_desc IS NULL) OR (SRC.type_level_2_desc IS NULL AND TGT.type_level_2_desc IS NOT NULL))
   OR (LOWER(SRC.sbclass_num) <> LOWER(cast(TGT.sbclass_num as string)) OR (SRC.sbclass_num IS NOT NULL AND TGT.sbclass_num IS NULL) OR (SRC.sbclass_num IS NULL AND TGT.sbclass_num IS NOT NULL))
   OR (LOWER(SRC.sbclass_desc) <> LOWER(TGT.sbclass_desc) OR (SRC.sbclass_desc IS NOT NULL AND TGT.sbclass_desc IS NULL) OR (SRC.sbclass_desc IS NULL AND TGT.sbclass_desc IS NOT NULL))
   OR (LOWER(SRC.class_num) <> LOWER(cast(TGT.sbclass_num as string)) OR (SRC.class_num IS NOT NULL AND TGT.class_num IS NULL) OR (SRC.class_num IS NULL AND TGT.class_num IS NOT NULL))
   OR (LOWER(SRC.class_desc) <> LOWER(TGT.class_desc) OR (SRC.class_desc IS NOT NULL AND TGT.class_desc IS NULL) OR (SRC.class_desc IS NULL AND TGT.class_desc IS NOT NULL))
   OR (LOWER(SRC.dept_num) <> LOWER(cast(TGT.dept_num as STRING)) OR (SRC.dept_num IS NOT NULL AND TGT.dept_num IS NULL) OR (SRC.dept_num IS NULL AND TGT.dept_num IS NOT NULL))
   OR (LOWER(SRC.dept_desc) <> LOWER(TGT.dept_desc) OR (SRC.dept_desc IS NOT NULL AND TGT.dept_desc IS NULL) OR (SRC.dept_desc IS NULL AND TGT.dept_desc IS NOT NULL))
   OR ((SRC.grp_num) <> (TGT.grp_num) OR (SRC.grp_num IS NOT NULL AND TGT.grp_num IS NULL) OR (SRC.grp_num IS NULL AND TGT.grp_num IS NOT NULL))
   OR (LOWER(SRC.grp_desc) <> LOWER(TGT.grp_desc) OR (SRC.grp_desc IS NOT NULL AND TGT.grp_desc IS NULL) OR (SRC.grp_desc IS NULL AND TGT.grp_desc IS NOT NULL))
   OR ((SRC.div_num) <> (TGT.div_num) OR (SRC.div_num IS NOT NULL AND TGT.div_num IS NULL) OR (SRC.div_num IS NULL AND TGT.div_num IS NOT NULL))
   OR (LOWER(SRC.div_desc) <> LOWER(TGT.div_desc) OR (SRC.div_desc IS NOT NULL AND TGT.div_desc IS NULL) OR (SRC.div_desc IS NULL AND TGT.div_desc IS NOT NULL))
   OR (LOWER(SRC.cmpy_num) <> LOWER(cast(TGT.cmpy_num as STRING)) OR (SRC.cmpy_num IS NOT NULL AND TGT.cmpy_num IS NULL) OR (SRC.cmpy_num IS NULL AND TGT.cmpy_num IS NOT NULL))
   OR (LOWER(SRC.cmpy_desc) <> LOWER(TGT.cmpy_desc) OR (SRC.cmpy_desc IS NOT NULL AND TGT.cmpy_desc IS NULL) OR (SRC.cmpy_desc IS NULL AND TGT.cmpy_desc IS NOT NULL))
   OR (LOWER(SRC.nord_label_ind) <> LOWER(TGT.nord_label_ind) OR (SRC.nord_label_ind IS NOT NULL AND TGT.nord_label_ind IS NULL) OR (SRC.nord_label_ind IS NULL AND TGT.nord_label_ind IS NOT NULL))
   OR (LOWER(SRC.vendor_label_code) <> LOWER(TGT.vendor_label_code) OR (SRC.vendor_label_code IS NOT NULL AND TGT.vendor_label_code IS NULL) OR (SRC.vendor_label_code IS NULL AND TGT.vendor_label_code IS NOT NULL))
   OR (LOWER(SRC.vendor_label_name) <> LOWER(TGT.vendor_label_name) OR (SRC.vendor_label_name IS NOT NULL AND TGT.vendor_label_name IS NULL) OR (SRC.vendor_label_name IS NULL AND TGT.vendor_label_name IS NOT NULL))
   OR (LOWER(SRC.special_treatment_type_code) <> LOWER(TGT.special_treatment_type_code) OR (SRC.special_treatment_type_code IS NOT NULL AND TGT.special_treatment_type_code IS NULL) OR (SRC.special_treatment_type_code IS NULL AND TGT.special_treatment_type_code IS NOT NULL))
   OR (LOWER(SRC.size_standard) <> LOWER(TGT.size_standard) OR (SRC.size_standard IS NOT NULL AND TGT.size_standard IS NULL) OR (SRC.size_standard IS NULL AND TGT.size_standard IS NOT NULL))
   OR (LOWER(SRC.supp_part_num) <> LOWER(TGT.supp_part_num) OR (SRC.supp_part_num IS NOT NULL AND TGT.supp_part_num IS NULL) OR (SRC.supp_part_num IS NULL AND TGT.supp_part_num IS NOT NULL))
   OR (LOWER(SRC.prmy_supp_num) <> LOWER(TGT.prmy_supp_num) OR (SRC.prmy_supp_num IS NOT NULL AND TGT.prmy_supp_num IS NULL) OR (SRC.prmy_supp_num IS NULL AND TGT.prmy_supp_num IS NOT NULL))
   OR (LOWER(SRC.manufacturer_num) <> LOWER(TGT.manufacturer_num) OR (SRC.manufacturer_num IS NOT NULL AND TGT.manufacturer_num IS NULL) OR (SRC.manufacturer_num IS NULL AND TGT.manufacturer_num IS NOT NULL))
   OR (LOWER(SRC.product_source_code) <> LOWER(TGT.product_source_code) OR (SRC.product_source_code IS NOT NULL AND TGT.product_source_code IS NULL) OR (SRC.product_source_code IS NULL AND TGT.product_source_code IS NOT NULL))
   OR (LOWER(SRC.product_source_desc) <> LOWER(TGT.product_source_desc) OR (SRC.product_source_desc IS NOT NULL AND TGT.product_source_desc IS NULL) OR (SRC.product_source_desc IS NULL AND TGT.product_source_desc IS NOT NULL))
   OR (LOWER(SRC.genders_code) <> LOWER(TGT.genders_code) OR (SRC.genders_code IS NOT NULL AND TGT.genders_code IS NULL) OR (SRC.genders_code IS NULL AND TGT.genders_code IS NOT NULL))
   OR (LOWER(SRC.genders_desc) <> LOWER(TGT.genders_desc) OR (SRC.genders_desc IS NOT NULL AND TGT.genders_desc IS NULL) OR (SRC.genders_desc IS NULL AND TGT.genders_desc IS NOT NULL))
   OR (LOWER(SRC.age_groups_code) <> LOWER(TGT.age_groups_code) OR (SRC.age_groups_code IS NOT NULL AND TGT.age_groups_code IS NULL) OR (SRC.age_groups_code IS NULL AND TGT.age_groups_code IS NOT NULL))
   OR (LOWER(SRC.age_groups_desc) <> LOWER(TGT.age_groups_desc) OR (SRC.age_groups_desc IS NOT NULL AND TGT.age_groups_desc IS NULL) OR (SRC.age_groups_desc IS NULL AND TGT.age_groups_desc IS NOT NULL))
   OR (SRC.msrp_amt <> TGT.msrp_amt OR (SRC.msrp_amt IS NOT NULL AND TGT.msrp_amt IS NULL) OR (SRC.msrp_amt IS NULL AND TGT.msrp_amt IS NOT NULL))
   OR (LOWER(SRC.msrp_currency_code) <> LOWER(TGT.msrp_currency_code) OR (SRC.msrp_currency_code IS NOT NULL AND TGT.msrp_currency_code IS NULL) OR (SRC.msrp_currency_code IS NULL AND TGT.msrp_currency_code IS NOT NULL))
   OR (LOWER(SRC.hanger_type_code) <> LOWER(TGT.hanger_type_code) OR (SRC.hanger_type_code IS NOT NULL AND TGT.hanger_type_code IS NULL) OR (SRC.hanger_type_code IS NULL AND TGT.hanger_type_code IS NOT NULL))
   OR (LOWER(SRC.hanger_type_desc) <> LOWER(TGT.hanger_type_desc) OR (SRC.hanger_type_desc IS NOT NULL AND TGT.hanger_type_desc IS NULL) OR (SRC.hanger_type_desc IS NULL AND TGT.hanger_type_desc IS NOT NULL))
   OR (LOWER(SRC.npg_ind) <> LOWER(TGT.npg_ind) OR (SRC.npg_ind IS NOT NULL AND TGT.npg_ind IS NULL) OR (SRC.npg_ind IS NULL AND TGT.npg_ind IS NOT NULL))
   OR (LOWER(SRC.npg_sourcing_code) <> LOWER(TGT.npg_sourcing_code) OR (SRC.npg_sourcing_code IS NOT NULL AND TGT.npg_sourcing_code IS NULL) OR (SRC.npg_sourcing_code IS NULL AND TGT.npg_sourcing_code IS NOT NULL))
   )  )
) NRML
;



--.IF ERRORCODE <> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
Begin transaction;
--.IF ERRORCODE <> 0 THEN .QUIT 3


DELETE FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_style_dim AS tgt
WHERE EXISTS (SELECT *
 FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_style_dim_vtw AS src
 WHERE epm_style_num = tgt.epm_style_num
  AND LOWER(channel_country) = LOWER(tgt.channel_country));
--.IF ERRORCODE <> 0 THEN .QUIT 4

INSERT INTO {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_style_dim (epm_style_num, rms_style_num_list, channel_country, style_desc,
 style_group_num, style_group_desc, style_group_short_desc, type_level_1_num, type_level_1_desc, type_level_2_num,
 type_level_2_desc, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num,
 div_desc, cmpy_num, cmpy_desc, nord_label_ind, vendor_label_code, vendor_label_name, special_treatment_type_code,
 size_standard, supp_part_num, prmy_supp_num, manufacturer_num, product_source_code, product_source_desc, genders_code,
 genders_desc, age_groups_code, age_groups_desc, msrp_amt, msrp_currency_code, hanger_type_code, hanger_type_desc,
 npg_ind, npg_sourcing_code, eff_begin_tmstp, eff_end_tmstp, eff_begin_tmstp_tz,
	eff_end_tmstp_tz,dw_batch_id, dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT epm_style_num,
  rms_style_num_list,
  channel_country,
  style_desc,
  style_group_num,
  style_group_desc,
  style_group_short_desc,
  type_level_1_num,
  type_level_1_desc,
  type_level_2_num,
  type_level_2_desc,
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
  nord_label_ind,
  vendor_label_code,
  vendor_label_name,
  special_treatment_type_code,
  size_standard,
  supp_part_num,
  prmy_supp_num,
  manufacturer_num,
  product_source_code,
  product_source_desc,
  genders_code,
  genders_desc,
  age_groups_code,
  age_groups_desc,
  msrp_amt,
  msrp_currency_code,
  hanger_type_code,
  hanger_type_desc,
  npg_ind,
  npg_sourcing_code,
  eff_begin_tmstp,
  eff_end_tmstp,
	eff_begin_tmstp_tz,
  eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT epm_style_num,
    rms_style_num_list,
    channel_country,
    style_desc,
    style_group_num,
    style_group_desc,
    style_group_short_desc,
    type_level_1_num,
    type_level_1_desc,
    type_level_2_num,
    type_level_2_desc,
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
    nord_label_ind,
    vendor_label_code,
    vendor_label_name,
    special_treatment_type_code,
    size_standard,
    supp_part_num,
    prmy_supp_num,
    manufacturer_num,
    product_source_code,
    product_source_desc,
    genders_code,
    genders_desc,
    age_groups_code,
    age_groups_desc,
    msrp_amt,
    msrp_currency_code,
    hanger_type_code,
    hanger_type_desc,
    npg_ind,
    npg_sourcing_code,
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
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_stg.product_style_dim_vtw) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `a12180`
   FROM {{params.gcp_project_id}}.{{params.dbenv}}_nap_dim.product_style_dim
   WHERE epm_style_num = t3.epm_style_num
    AND channel_country = t3.channel_country)
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_style_num, channel_country)) = 1);
--.IF ERRORCODE <> 0 THEN .QUIT 5

commit transaction;
--.IF ERRORCODE <> 0 THEN .QUIT 6


