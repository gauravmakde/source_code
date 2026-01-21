
--SET QUERY_BAND='AppName=NAP-Merch-Dimension;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=NAP_PRODUCT;' UPDATE FOR SESSION;


--NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
TRUNCATE TABLE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_style_dim_vtw;
--.IF ERRORCODE <> 0 THEN .QUIT 1


INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_style_dim_vtw
( epm_style_num
, rms_style_num_list
, channel_country
, style_desc
--, style_group_num   Default to NULL
--, style_group_desc   Default to NULL
--, style_group_short_desc   Default to NULL
--, type_level_1_num   Default to NULL
--, type_level_1_desc   Default to NULL
--, type_level_2_num   Default to NULL
--, type_level_2_desc   Default to NULL
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
--, vendor_label_code   Default to NULL
--, vendor_label_name   Default to NULL
--, special_treatment_type_code   Default to NULL
--, size_standard   Default to NULL
, supp_part_num
, prmy_supp_num
, manufacturer_num
, eff_begin_tmstp
, eff_end_tmstp
,eff_begin_tmstp_tz
,eff_end_tmstp_tz
)

with src_1 as
 (select rms_casepack_num, epm_casepack_num, channel_country, casepack_desc
	, subclass_num, class_num, department_num, prmy_supp_num, vendor_part_num, manufacturer_num
	, eff_begin_tmstp, eff_end_tmstp
--  range(eff_begin_tmstp,eff_end_tmstp) as eff_period 
 from  (
    --inner normalize
            SELECT rms_casepack_num, epm_casepack_num, channel_country, casepack_desc
	, subclass_num, class_num, department_num, prmy_supp_num, vendor_part_num, manufacturer_num, MIN(eff_begin_tmstp) AS eff_begin_tmstp, MAX(eff_end_tmstp) AS eff_end_tmstp
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY rms_casepack_num, epm_casepack_num, channel_country, casepack_desc
	, subclass_num, class_num, department_num, prmy_supp_num, vendor_part_num, manufacturer_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
    FROM (
        SELECT *,
            CASE 
                WHEN LAG(eff_end_tmstp) OVER (PARTITION BY rms_casepack_num, epm_casepack_num, channel_country, casepack_desc
	, subclass_num, class_num, department_num, prmy_supp_num, vendor_part_num, manufacturer_num ORDER BY eff_begin_tmstp) >= 
                DATE_SUB(eff_begin_tmstp, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag
         from (
	SELECT SRC_2.*
	, COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY epm_casepack_num,channel_country ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
			

	FROM (
	SELECT
	  COALESCE(TRIM(referencekeyfg_legacyrmscasepackid),'0') AS rms_casepack_num
	, id AS epm_casepack_num
	, marketcode AS channel_country
	, corefg_description AS casepack_desc
	, subclass_num
	, class_num
	, department_num

	, MAX(CASE
	      WHEN LOWER(vendor_is_supplier)=LOWER('true')
	       AND LOWER(supplychainfg_productvendorupcs_productvendors_isprimary)= LOWER('true')
	      THEN supplychainfg_productvendorupcs_productvendors_number
	      END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS prmy_supp_num
	, MAX(CASE
	      WHEN LOWER(vendor_is_supplier)=LOWER('true')
	       AND LOWER(supplychainfg_productvendorupcs_productvendors_isprimary)= LOWER('true')
	      THEN supplychainfg_productvendorupcs_productvendors_name
	      END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS prmy_supp_name
	, MAX(CASE
	      WHEN LOWER(vendor_is_supplier)=LOWER('true')
	       AND LOWER(supplychainfg_productvendorupcs_productvendors_isprimary)= LOWER('true')
	      THEN supplychainfg_productvendorupcs_productvendors_vendorproductnumbers_code
	      END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS vendor_part_num
	, MAX(CASE
	      WHEN LOWER(vendor_is_manufacturer)=LOWER('true')
	       AND LOWER(supplychainfg_productvendorupcs_productvendors_isprimary)= LOWER('true')
		  THEN supplychainfg_productvendorupcs_productvendors_number
		  END) OVER(PARTITION BY id, marketcode, sourcepublishtimestamp) AS manufacturer_num
	, cast(`{{params.dataplex_project_id}}.JWN_UDF.ISO8601_TMSTP`( COLLATE(sourcepublishtimestamp,''))as timestamp) AS eff_begin_tmstp
	FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.casepack_style_ldg
	) SRC_2
	QUALIFY eff_begin_tmstp < eff_end_tmstp
	)  ) AS ordered_data
) AS grouped_data
GROUP BY rms_casepack_num, epm_casepack_num, channel_country, casepack_desc
	, subclass_num, class_num, department_num, prmy_supp_num, vendor_part_num, manufacturer_num,range_group
ORDER BY rms_casepack_num, epm_casepack_num, channel_country, casepack_desc
	, subclass_num, class_num, department_num, prmy_supp_num, vendor_part_num, manufacturer_num, eff_begin_tmstp) )



SELECT
  epm_style_num
, rms_style_num_list
, channel_country
, style_desc
, cast(trunc(cast(sbclass_num as float64)) as int64)
, sbclass_desc
, cast(trunc(cast(class_num  as float64)) as int64)
, class_desc
, cast(trunc(cast(dept_num as float64)) as int64)
, dept_desc
, grp_num
, grp_desc
, div_num
, div_desc
, cmpy_num
, cmpy_desc
, nord_label_ind
, supp_part_num
, prmy_supp_num
, manufacturer_num
, CAST(min(RANGE_START(eff_period))AS TIMESTAMP) AS eff_begin
, CAST(max(RANGE_END(eff_period)) AS TIMESTAMP)  AS eff_end
,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(min(RANGE_START(eff_period)) as string)) as eff_begin_tmstp_tz
,`{{params.dataplex_project_id}}.JWN_UDF.UDF_TIME_ZONE`(cast(min(RANGE_START(eff_period)) as string)) as eff_end_tmstp_tz

       from  (
    --inner normalize
            SELECT epm_style_num, rms_style_num_list, channel_country, style_desc, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, nord_label_ind, supp_part_num, prmy_supp_num, manufacturer_num,eff_period
            FROM (
                SELECT *,SUM(discontinuity_flag) OVER (PARTITION BY epm_style_num, rms_style_num_list, channel_country, style_desc, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, nord_label_ind, supp_part_num, prmy_supp_num, manufacturer_num ORDER BY eff_begin_tmstp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_group
                FROM
      --  NONSEQUENCED VALIDTIME
      (
        
            --  NORMALIZE
         SELECT   SRC.epm_style_num, SRC.rms_style_num_list, SRC.channel_country, SRC.style_desc, SRC.sbclass_num, SRC.sbclass_desc, SRC.class_num, SRC.class_desc, SRC.dept_num, SRC.dept_desc, SRC.grp_num, SRC.grp_desc, SRC.div_num, SRC.div_desc, SRC.cmpy_num, SRC.cmpy_desc, SRC.nord_label_ind, SRC.supp_part_num, SRC.prmy_supp_num, SRC.manufacturer_num, TGT.eff_begin_tmstp,TGT.eff_end_tmstp,
            CASE 
                WHEN LAG(eff_end_tmstp_utc) OVER (PARTITION BY  SRC.epm_style_num, SRC.rms_style_num_list, SRC.channel_country, SRC.style_desc, SRC.sbclass_num, SRC.sbclass_desc, SRC.class_num, SRC.class_desc, SRC.dept_num, SRC.dept_desc, SRC.grp_num, SRC.grp_desc, SRC.div_num, SRC.div_desc, SRC.cmpy_num, SRC.cmpy_desc, SRC.nord_label_ind, SRC.supp_part_num, SRC.prmy_supp_num, SRC.manufacturer_num ORDER BY eff_begin_tmstp_utc) >= 
                DATE_SUB(eff_begin_tmstp_utc, INTERVAL 1 DAY) 
                THEN 0
                ELSE 1
            END AS discontinuity_flag,
             COALESCE( SAFE.RANGE_INTERSECT (SRC.eff_period,range(TGT.eff_begin_tmstp_utc,TGT.eff_end_tmstp_utc))
          , SRC.eff_period ) AS eff_period,
             FROM
	(SELECT
	  SRC0.epm_casepack_num AS epm_style_num
	, '["'||COALESCE(TRIM(src0.rms_casepack_num),'0')||'"]' AS rms_style_num_list
	, SRC0.channel_country
	, src0.casepack_desc AS style_desc
	, src0.subclass_num AS sbclass_num
	, hier.sbclass_desc
	, src0.class_num
	, hier.class_desc
	, src0.department_num AS dept_num
	, hier.dept_desc
	, hier.grp_num
	, hier.grp_desc
	, hier.div_num
	, hier.div_desc
	, hier.cmpy_num
	, hier.cmpy_desc
	, 'N' AS nord_label_ind
	, src0.vendor_part_num AS supp_part_num
	, src0.prmy_supp_num
	, src0.manufacturer_num
	,COALESCE( SAFE.RANGE_INTERSECT (SRC0.eff_period,hier.eff_period), SRC0.eff_period ) AS eff_period
FROM (
	SELECT 
  --normalize
   rms_casepack_num, epm_casepack_num, channel_country, casepack_desc
	, subclass_num, class_num, department_num, prmy_supp_num, vendor_part_num, manufacturer_num
	,range(eff_begin_tmstp, eff_end_tmstp) AS eff_period
	FROM  SRC_1
	) SRC0
	LEFT JOIN (SELECT subclass_num as sbclass_num, subclass_short_name as sbclass_desc, class_num, class_short_name as class_desc, dept_num, dept_name as dept_desc,
subdivision_num AS grp_num,subdivision_name AS grp_desc,division_num AS div_num,division_name AS div_desc, CAST(1000 AS SMALLINT) AS cmpy_num
, 'Nordstrom'  AS cmpy_desc,eff_begin_tmstp_utc,eff_end_tmstp_utc,RANGE(eff_begin_tmstp_utc,eff_end_tmstp_utc) as eff_period
from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.department_class_subclass_dim_hist ) hier
	ON  LOWER(src0.subclass_num) =LOWER( cast(hier.sbclass_num as string)) 
  AND LOWER(src0.class_num) =LOWER( cast(hier.class_num as string)) 
  AND LOWER(src0.department_num)= LOWER(cast(hier.dept_num as string))
	--AND RANGE_OVERLAPS(cast(src0.eff_period) as timestamp  ) , timestamp(hier.eff_period)

AND RANGE_OVERLAPS(src0.eff_period ,range(hier.eff_begin_tmstp_utc,hier.eff_end_tmstp_utc))
) SRC
LEFT JOIN `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_base_vws.product_style_dim_hist TGT
ON SRC.epm_style_num = TGT.epm_style_num
AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
AND range_overlaps(SRC.eff_period, range(TGT.eff_begin_tmstp_utc, TGT.eff_end_tmstp_utc))
WHERE ( TGT.epm_style_num IS NULL 
   OR((LOWER(SRC.rms_style_num_list) <> LOWER(TGT.rms_style_num_list) OR (SRC.rms_style_num_list IS NOT NULL AND TGT.rms_style_num_list IS NULL) OR (SRC.rms_style_num_list IS NULL AND TGT.rms_style_num_list IS NOT NULL))
   OR (LOWER(SRC.style_desc) <> LOWER(TGT.style_desc) OR (SRC.style_desc IS NOT NULL AND TGT.style_desc IS NULL) OR (SRC.style_desc IS NULL AND TGT.style_desc IS NOT NULL))
   OR (SRC.sbclass_num <> cast(TGT.sbclass_num as string) OR (SRC.sbclass_num IS NOT NULL AND TGT.sbclass_num IS NULL) OR (SRC.sbclass_num IS NULL AND TGT.sbclass_num IS NOT NULL))
   OR (LOWER(SRC.sbclass_desc) <> LOWER(TGT.sbclass_desc) OR (SRC.sbclass_desc IS NOT NULL AND TGT.sbclass_desc IS NULL) OR (SRC.sbclass_desc IS NULL AND TGT.sbclass_desc IS NOT NULL))
   OR (SRC.class_num <> cast(TGT.class_num as string) OR (SRC.class_num IS NOT NULL AND TGT.class_num IS NULL) OR (SRC.class_num IS NULL AND TGT.class_num IS NOT NULL))
   OR (LOWER(SRC.class_desc) <> LOWER(TGT.class_desc) OR (SRC.class_desc IS NOT NULL AND TGT.class_desc IS NULL) OR (SRC.class_desc IS NULL AND TGT.class_desc IS NOT NULL))
   OR (SRC.dept_num <> cast(TGT.dept_num as string) OR (SRC.dept_num IS NOT NULL AND TGT.dept_num IS NULL) OR (SRC.dept_num IS NULL AND TGT.dept_num IS NOT NULL))
   OR (LOWER(SRC.dept_desc) <> LOWER(TGT.dept_desc) OR (SRC.dept_desc IS NOT NULL AND TGT.dept_desc IS NULL) OR (SRC.dept_desc IS NULL AND TGT.dept_desc IS NOT NULL))
   OR (SRC.grp_num <> TGT.grp_num OR (SRC.grp_num IS NOT NULL AND TGT.grp_num IS NULL) OR (SRC.grp_num IS NULL AND TGT.grp_num IS NOT NULL))
   OR (LOWER(SRC.grp_desc) <> LOWER(TGT.grp_desc) OR (SRC.grp_desc IS NOT NULL AND TGT.grp_desc IS NULL) OR (SRC.grp_desc IS NULL AND TGT.grp_desc IS NOT NULL))
   OR (SRC.div_num <> TGT.div_num OR (SRC.div_num IS NOT NULL AND TGT.div_num IS NULL) OR (SRC.div_num IS NULL AND TGT.div_num IS NOT NULL))
   OR (LOWER(SRC.div_desc) <> LOWER(TGT.div_desc) OR (SRC.div_desc IS NOT NULL AND TGT.div_desc IS NULL) OR (SRC.div_desc IS NULL AND TGT.div_desc IS NOT NULL))
   OR (SRC.cmpy_num <> TGT.cmpy_num OR (SRC.cmpy_num IS NOT NULL AND TGT.cmpy_num IS NULL) OR (SRC.cmpy_num IS NULL AND TGT.cmpy_num IS NOT NULL))
   OR (LOWER(SRC.cmpy_desc) <> LOWER(TGT.cmpy_desc) OR (SRC.cmpy_desc IS NOT NULL AND TGT.cmpy_desc IS NULL) OR (SRC.cmpy_desc IS NULL AND TGT.cmpy_desc IS NOT NULL))
   OR (LOWER(SRC.nord_label_ind) <> LOWER(TGT.nord_label_ind) OR (SRC.nord_label_ind IS NOT NULL AND TGT.nord_label_ind IS NULL) OR (SRC.nord_label_ind IS NULL AND TGT.nord_label_ind IS NOT NULL))
   OR (LOWER(SRC.supp_part_num) <> LOWER(TGT.supp_part_num) OR (SRC.supp_part_num IS NOT NULL AND TGT.supp_part_num IS NULL) OR (SRC.supp_part_num IS NULL AND TGT.supp_part_num IS NOT NULL))
   OR (LOWER(SRC.prmy_supp_num) <> LOWER(TGT.prmy_supp_num) OR (SRC.prmy_supp_num IS NOT NULL AND TGT.prmy_supp_num IS NULL) OR (SRC.prmy_supp_num IS NULL AND TGT.prmy_supp_num IS NOT NULL))
   OR (LOWER(SRC.manufacturer_num) <> LOWER(TGT.manufacturer_num) OR (SRC.manufacturer_num IS NOT NULL AND TGT.manufacturer_num IS NULL) OR (SRC.manufacturer_num IS NULL AND TGT.manufacturer_num IS NOT NULL))
   )  )
  )as   ordered_data
) AS grouped_data)
GROUP BY  epm_style_num, rms_style_num_list, channel_country, style_desc, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, nord_label_ind, supp_part_num, prmy_supp_num, manufacturer_num, eff_period
ORDER BY  epm_style_num, rms_style_num_list, channel_country, style_desc, sbclass_num, sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc, nord_label_ind, supp_part_num, prmy_supp_num, manufacturer_num;

--.IF ERRORCODE <> 0 THEN .QUIT 2


--Explicit transaction on Teradata ensures that we do not corrupt target table if failure in middle of updating target table
BEGIN TRANSACTION;
--.IF ERRORCODE <> 0 THEN .QUIT 3

--SEQUENCED VALIDTIME
DELETE FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS tgt
WHERE EXISTS (SELECT 1
 FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_style_dim_vtw AS src
 WHERE src.epm_style_num = tgt.epm_style_num
  AND LOWER(src.channel_country) = LOWER(tgt.channel_country)
  AND SRC.eff_begin_tmstp <= TGT.eff_begin_tmstp
    AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp);

UPDATE  `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS TGT
SET TGT.eff_end_tmstp = SRC.eff_begin_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_style_dim_vtw AS SRC
WHERE SRC.epm_style_num = TGT.epm_style_num
  AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
  -- AND SRC.eff_begin_tmstp >= TGT.eff_begin_tmstp
  -- AND SRC.eff_begin_tmstp < TGT.eff_end_tmstp 
  -- AND SRC.eff_end_tmstp > TGT.eff_end_tmstp ;
  AND SRC.eff_begin_tmstp > TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp <= TGT.eff_end_tmstp
AND SRC.eff_end_tmstp >= TGT.eff_end_tmstp;

UPDATE `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS TGT
SET TGT.eff_begin_tmstp = SRC.eff_end_tmstp
FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_style_dim_vtw AS SRC
WHERE SRC.epm_style_num = TGT.epm_style_num
  AND LOWER(SRC.channel_country) = LOWER(TGT.channel_country)
  -- AND SRC.eff_end_tmstp > TGT.eff_begin_tmstp
  -- AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;
  AND SRC.eff_end_tmstp >= TGT.eff_begin_tmstp
    AND SRC.eff_begin_tmstp < TGT.eff_begin_tmstp
AND SRC.eff_end_tmstp <= TGT.eff_end_tmstp;

insert into `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim(epm_style_num,rms_style_num_list,channel_country,style_desc,style_group_num,style_group_desc,style_group_short_desc,type_level_1_num,type_level_1_desc,type_level_2_num,type_level_2_desc,sbclass_num,sbclass_desc,class_num,class_desc,dept_num,dept_desc,grp_num,grp_desc,div_num,div_desc,cmpy_num,cmpy_desc,nord_label_ind,vendor_label_code,vendor_label_name,special_treatment_type_code,size_standard,supp_part_num,prmy_supp_num,manufacturer_num,product_source_code,product_source_desc,genders_code,genders_desc,age_groups_code,age_groups_desc,msrp_amt,msrp_currency_code,hanger_type_code,hanger_type_desc,npg_ind,npg_sourcing_code,eff_begin_tmstp_tz,eff_end_tmstp_tz,dw_batch_id,dw_batch_date,dw_sys_load_tmstp,eff_begin_tmstp,eff_end_tmstp)
with tbl as 
(SELECT 
tgt.*,
cast(src.eff_begin_tmstp as timestamp) AS src_eff_begin_tmstp, 
cast(src.eff_end_tmstp as timestamp) AS src_eff_end_tmstp,  
tgt.eff_begin_tmstp AS tgt_eff_begin_tmstp, 
tgt.eff_end_tmstp AS tgt_eff_end_tmstp 
 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_style_dim_vtw AS SRC 
 inner join `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS TGT 
 on src.epm_style_num = tgt.epm_style_num 
 and tgt.eff_begin_tmstp < src.eff_begin_tmstp 
 and tgt.eff_end_tmstp > src.eff_end_tmstp)

SELECT *  except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp ), tgt_eff_begin_tmstp, cast(src_eff_begin_tmstp as timestamp) FROM tbl
UNION ALL
SELECT * except(eff_begin_tmstp, eff_end_tmstp, src_eff_begin_tmstp, src_eff_end_tmstp, tgt_eff_begin_tmstp,tgt_eff_end_tmstp), cast(src_eff_end_tmstp as timestamp), tgt_eff_end_tmstp FROM tbl;

delete from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim AS TGT  
where exists (select 1 from `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_style_dim_vtw AS SRC 
where src.epm_style_num = tgt.epm_style_num and tgt.eff_begin_tmstp < src.eff_begin_tmstp and tgt.eff_end_tmstp > src.eff_end_tmstp);

--IF ERRORCODE <> 0 THEN .QUIT 4

INSERT INTO `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim (epm_style_num, rms_style_num_list, channel_country, style_desc, sbclass_num,
 sbclass_desc, class_num, class_desc, dept_num, dept_desc, grp_num, grp_desc, div_num, div_desc, cmpy_num, cmpy_desc,
 nord_label_ind, supp_part_num, prmy_supp_num, manufacturer_num, eff_begin_tmstp,eff_begin_tmstp_tz, eff_end_tmstp,eff_end_tmstp_tz, dw_batch_id,
 dw_batch_date, dw_sys_load_tmstp)
(SELECT DISTINCT epm_style_num,
  rms_style_num_list,
  channel_country,
  style_desc,
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
  supp_part_num,
  prmy_supp_num,
  manufacturer_num,
  eff_begin_tmstp,eff_begin_tmstp_tz,
  eff_end_tmstp,eff_end_tmstp_tz,
  dw_batch_id,
  dw_batch_date,
  dw_sys_load_tmstp
 FROM (SELECT epm_style_num,
    rms_style_num_list,
    channel_country,
    style_desc,
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
    supp_part_num,
    prmy_supp_num,
    manufacturer_num,
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
    CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_stg.casepack_style_dim_vtw) AS t3
 WHERE NOT EXISTS (SELECT 1 AS `A12180`
   FROM `{{params.dataplex_project_id}}`.{{params.dbenv}}_nap_dim.product_style_dim
   WHERE (epm_style_num) = (t3.epm_style_num)
    AND LOWER(channel_country) = LOWER(t3.channel_country))
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_style_num, channel_country)) = 1);


COMMIT TRANSACTION;
