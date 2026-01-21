SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=org_load_2656_napstore_insights;
Task_Name=org_store_dim_load_1_fct_table;'
FOR SESSION VOLATILE;

ET;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('STORE_DIM','{db_env}_NAP_DIM','org_load_2656_napstore_insights','org_store_dimension_table',1,'LOAD_START','',current_timestamp,'ORG');
ET;

MERGE INTO {db_env}_NAP_DIM.STORE_DIM AS TGT
USING (
SELECT --DISTINCT
  currentData_storeNumber AS store_num
, currentData_name AS store_name
, currentData_shorterNames_medName AS store_short_name
, currentData_shorterNames_shortName AS store_abbrev_name
, currentData_typeCode AS store_type_code
, currentData_typeDesc AS store_type_desc
,(CASE
  WHEN currentData_typeIsSellingStore='true' THEN 'S'
  WHEN currentData_typeIsInventoryStore='true' THEN 'I'
  WHEN currentData_isStoreOperationalToPublic='true' THEN 'O'
  END) AS selling_store_ind
, currentData_locationAddlInfo_grossSquareFootage AS gross_square_footage
, CAST(SUBSTR(NULLIF(store_open_date,''),1,10) AS DATE FORMAT'YYYY-MM-DD') AS store_open_date
, CAST(SUBSTR(NULLIF(store_close_date,''),1,10) AS DATE FORMAT'YYYY-MM-DD') AS store_close_date
, currentData_adminHier_regionNumber AS region_num
, currentData_adminHier_regionDesc AS region_desc
, currentdata_adminhier_regionmeddesc AS region_medium_desc
, currentdata_adminhier_regionshortdesc AS region_short_desc
, currentData_adminHier_buNumber AS business_unit_num
, currentData_adminHier_buDesc AS business_unit_desc
, currentdata_adminhier_bumeddesc AS business_unit_medium_desc
, currentData_adminHier_entNumber AS cmpy_num
, currentdata_adminhier_entdesc AS cmpy_name
, currentData_adminHier_entMedDesc AS cmpy_medium_name
, currentData_adminHier_groupNumber AS group_num
, currentData_adminHier_groupDesc AS group_desc
, currentdata_adminhier_groupmeddesc AS group_medium_desc
, currentData_adminHier_subGroupNumber AS subgroup_num
, currentData_adminHier_subGroupDesc AS subgroup_desc
, currentData_adminHier_subGroupMedDesc AS subgroup_medium_desc
, currentdata_adminhier_subgroupshortdesc AS subgroup_short_desc
, currentData_postalAddress_stdzdLine1Text AS store_address_line_1
, currentData_postalAddress_stdzdCityName AS store_address_city
, currentData_postalAddress_origStateCode AS store_address_state
, currentData_postalAddress_origStateName AS store_address_state_name
, currentData_postalAddress_stdzdPostalCode AS store_postal_code
, currentData_postalAddress_stdzdCountyName AS store_address_county
, currentData_postalAddress_origCountryCode AS store_country_code
, currentData_postalAddress_origCountryName AS store_country_name
/* 21-May-2024 - Changes to pick DMA values if no value received from OrgMDM */
--, currentData_dma AS store_dma_code
, (CASE WHEN currentData_dma is null THEN trim(dma_zip.us_dma_code) ELSE trim(currentData_dma) END) AS store_dma_code
/* 21-May-2024 - Changes to pick DMA values if no value received from OrgMDM */
--, CAST(NULL AS VARCHAR(60) CHARACTER SET UNICODE) AS store_dma_desc
, dma.dma_desc AS store_dma_desc
, currentData_postalAddress_latitude AS store_location_latitude
, currentData_postalAddress_longitude AS store_location_longitude
, currentData_postalAddress_standardTimeZoneAbbrev AS store_time_zone
, currentData_timeZoneName AS store_time_zone_desc
,(CASE WHEN currentData_postalAddress_isDaylightSavingsTimeParticipant='true' THEN 'Y' ELSE 'N' END) AS daylight_savings_time_ind
, currentData_postalAddress_standardTimeZoneUTCOffset AS store_time_zone_offset
, currentData_dc_storeNumber AS distribution_center_num
, currentData_dc_name AS distribution_center_name
, currentData_locationTypeCode AS location_type_code
, currentData_locationTypeDesc AS location_type_desc
, currentData_storePlanningChannel_planningChannelCode AS channel_num
, currentData_storePlanningChannel_planningChannelDesc AS channel_desc
, currentData_compStatusCode AS comp_status_code
, currentData_compStatusDesc AS comp_status_desc
, currentdata_eligibilityTypes As eligibility_types
, currentdata_receivingLocation_locationName AS receivinglocation_location_name
, currentdata_receivingLocation_locationNumber AS receivinglocation_location_number
, currentData_receivingLocation_postalAddress_latitude AS receivinglocation_latitude
, currentData_receivingLocation_postalAddress_longitude AS receivinglocation_longitude
, currentData_receivingLocation_postalAddress_origCountryCode AS receivinglocation_country_code
, currentData_receivingLocation_postalAddress_origCountryIsoCode AS receivinglocation_country_iso_code
, currentData_receivingLocation_postalAddress_origCountryName AS receivinglocation_country_name
, currentData_receivingLocation_postalAddress_origCountryTelephoneCode AS receivinglocation_country_tel_code
, currentData_receivingLocation_postalAddress_origStateCode AS receivinglocation_address_state
, currentData_receivingLocation_postalAddress_origStateName AS receivinglocation_address_state_name
, currentData_receivingLocation_postalAddress_origRegionCode AS receivinglocation_region_code
, currentData_receivingLocation_postalAddress_origRegionName AS receivinglocation_region_name
, currentData_receivingLocation_postalAddress_standardTimeZoneAbbrev AS receivinglocation_time_zone
, currentData_receivingLocation_postalAddress_standardTimeZoneUTCOffset AS receivinglocation_time_zone_offset
, currentData_receivingLocation_postalAddress_stdzdCityName AS receivinglocation_address_city
, currentData_receivingLocation_postalAddress_stdzdCountyName AS receivinglocation_address_county
, currentData_receivingLocation_postalAddress_stdzdLine1Text AS receivinglocation_address_line_1
, currentData_receivingLocation_postalAddress_stdzdPostalCode AS receivinglocation_postal_code
--, (SELECT MAX(dw_batch_id)+1 FROM {db_env}_NAP_DIM.STORE_DIM) AS dw_batch_id
--,current_date as dw_batch_date
, elt.BATCH_ID AS dw_batch_id
, elt.CURR_BATCH_DATE AS dw_batch_date
FROM {db_env}_NAP_STG.STORE_DIM_LDG SRC0
/* 21-May-2024 - Changes to pick DMA values if no value received from OrgMDM */
LEFT OUTER JOIN {db_env}_NAP_DIM.ORG_US_ZIP_DMA dma_zip
ON dma_zip.us_zip_code = SUBSTR(SRC0.currentData_postalAddress_stdzdPostalCode ,1,5)
AND SRC0.currentData_postalAddress_origCountryCode = 'US'
LEFT OUTER JOIN {db_env}_NAP_DIM.ORG_DMA dma
ON dma_zip.us_dma_code = dma.dma_code
, (SELECT BATCH_ID, CURR_BATCH_DATE
FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL
WHERE Subject_Area_Nm ='ORG_STORE'
) ELT
 --Filter to most recent row image
QUALIFY ROW_NUMBER() OVER(PARTITION BY currentData_storeNumber ORDER BY currentDataUpdatedTimeStamp DESC)=1
) AS SRC
ON SRC.store_num = TGT.store_num

WHEN MATCHED THEN UPDATE SET
  store_name = SRC.store_name
, store_short_name = SRC.store_short_name
, store_abbrev_name = SRC.store_abbrev_name
, store_type_code = SRC.store_type_code
, store_type_desc = SRC.store_type_desc
, selling_store_ind = SRC.selling_store_ind
, gross_square_footage = SRC.gross_square_footage
, store_open_date = SRC.store_open_date
, store_close_date = SRC.store_close_date
, region_num = SRC.region_num
, region_desc = SRC.region_desc
, region_medium_desc = SRC.region_medium_desc
, region_short_desc = SRC.region_short_desc
, business_unit_num = SRC.business_unit_num
, business_unit_desc = SRC.business_unit_desc
, business_unit_medium_desc = SRC.business_unit_medium_desc
, cmpy_num = SRC.cmpy_num
, cmpy_name = SRC.cmpy_name
, cmpy_medium_name = SRC.cmpy_medium_name
, group_num = SRC.group_num
, group_desc = SRC.group_desc
, group_medium_desc = SRC.group_medium_desc
, subgroup_num = SRC.subgroup_num
, subgroup_desc = SRC.subgroup_desc
, subgroup_medium_desc = SRC.subgroup_medium_desc
, subgroup_short_desc = SRC.subgroup_short_desc
, store_address_line_1 = SRC.store_address_line_1
, store_address_city = SRC.store_address_city
, store_address_state = SRC.store_address_state
, store_address_state_name = SRC.store_address_state_name
, store_postal_code = SRC.store_postal_code
, store_address_county = SRC.store_address_county
, store_country_code = SRC.store_country_code
, store_country_name = SRC.store_country_name
, store_dma_code = SRC.store_dma_code
, store_dma_desc = SRC.store_dma_desc
, store_location_latitude = SRC.store_location_latitude
, store_location_longitude = SRC.store_location_longitude
, store_time_zone = SRC.store_time_zone
, store_time_zone_desc = SRC.store_time_zone_desc
, daylight_savings_time_ind = SRC.daylight_savings_time_ind
, store_time_zone_offset = SRC.store_time_zone_offset
, distribution_center_num = SRC.distribution_center_num
, distribution_center_name = SRC.distribution_center_name
, location_type_code = SRC.location_type_code
, location_type_desc = SRC.location_type_desc
, channel_num = SRC.channel_num
, channel_desc = SRC.channel_desc
, comp_status_code = SRC.comp_status_code
, comp_status_desc = SRC.comp_status_desc
, eligibility_types = SRC.eligibility_types
, receivinglocation_location_name = SRC.receivinglocation_location_name
, receivinglocation_location_number = SRC.receivinglocation_location_number
, receivinglocation_latitude = SRC.receivinglocation_latitude
, receivinglocation_longitude = SRC.receivinglocation_longitude
, receivinglocation_country_code = SRC.receivinglocation_country_code
, receivinglocation_country_iso_code = SRC.receivinglocation_country_iso_code
, receivinglocation_country_name = SRC.receivinglocation_country_name
, receivinglocation_country_tel_code = SRC.receivinglocation_country_tel_code
, receivinglocation_address_state = SRC.receivinglocation_address_state
, receivinglocation_address_state_name = SRC.receivinglocation_address_state_name
, receivinglocation_region_code = SRC.receivinglocation_region_code
, receivinglocation_region_name = SRC.receivinglocation_region_name
, receivinglocation_time_zone = SRC.receivinglocation_time_zone
, receivinglocation_time_zone_offset = SRC.receivinglocation_time_zone_offset
, receivinglocation_address_city = SRC.receivinglocation_address_city
, receivinglocation_address_county = SRC.receivinglocation_address_county
, receivinglocation_address_line_1 = SRC.receivinglocation_address_line_1
, receivinglocation_postal_code = SRC.receivinglocation_postal_code
, dw_batch_id = SRC.dw_batch_id
, dw_batch_date= SRC.dw_batch_date
, dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)

WHEN NOT MATCHED THEN INSERT (
  store_num = SRC.store_num
, store_name = SRC.store_name
, store_short_name = SRC.store_short_name
, store_abbrev_name = SRC.store_abbrev_name
, store_type_code = SRC.store_type_code
, store_type_desc = SRC.store_type_desc
, selling_store_ind = SRC.selling_store_ind
, gross_square_footage = SRC.gross_square_footage
, store_open_date = SRC.store_open_date
, store_close_date = SRC.store_close_date
, region_num = SRC.region_num
, region_desc = SRC.region_desc
, region_medium_desc = SRC.region_medium_desc
, region_short_desc = SRC.region_short_desc
, business_unit_num = SRC.business_unit_num
, business_unit_desc = SRC.business_unit_desc
, business_unit_medium_desc = SRC.business_unit_medium_desc
, cmpy_num = SRC.cmpy_num
, cmpy_name = SRC.cmpy_name
, cmpy_medium_name = SRC.cmpy_medium_name
, group_num = SRC.group_num
, group_desc = SRC.group_desc
, group_medium_desc = SRC.group_medium_desc
, subgroup_num = SRC.subgroup_num
, subgroup_desc = SRC.subgroup_desc
, subgroup_medium_desc = SRC.subgroup_medium_desc
, subgroup_short_desc = SRC.subgroup_short_desc
, store_address_line_1 = SRC.store_address_line_1
, store_address_city = SRC.store_address_city
, store_address_state = SRC.store_address_state
, store_address_state_name = SRC.store_address_state_name
, store_postal_code = SRC.store_postal_code
, store_address_county = SRC.store_address_county
, store_country_code = SRC.store_country_code
, store_country_name = SRC.store_country_name
, store_dma_code = SRC.store_dma_code
, store_dma_desc = SRC.store_dma_desc
, store_location_latitude = SRC.store_location_latitude
, store_location_longitude = SRC.store_location_longitude
, store_time_zone = SRC.store_time_zone
, store_time_zone_desc = SRC.store_time_zone_desc
, daylight_savings_time_ind = SRC.daylight_savings_time_ind
, store_time_zone_offset = SRC.store_time_zone_offset
, distribution_center_num = SRC.distribution_center_num
, distribution_center_name = SRC.distribution_center_name
, location_type_code = SRC.location_type_code
, location_type_desc = SRC.location_type_desc
, channel_num = SRC.channel_num
, channel_desc = SRC.channel_desc
, comp_status_code = SRC.comp_status_code
, comp_status_desc = SRC.comp_status_desc
, eligibility_types = SRC.eligibility_types
, receivinglocation_location_name = SRC.receivinglocation_location_name
, receivinglocation_location_number = SRC.receivinglocation_location_number
, receivinglocation_latitude = SRC.receivinglocation_latitude
, receivinglocation_longitude = SRC.receivinglocation_longitude
, receivinglocation_country_code = SRC.receivinglocation_country_code
, receivinglocation_country_iso_code = SRC.receivinglocation_country_iso_code
, receivinglocation_country_name = SRC.receivinglocation_country_name
, receivinglocation_country_tel_code = SRC.receivinglocation_country_tel_code
, receivinglocation_address_state = SRC.receivinglocation_address_state
, receivinglocation_address_state_name = SRC.receivinglocation_address_state_name
, receivinglocation_region_code = SRC.receivinglocation_region_code
, receivinglocation_region_name = SRC.receivinglocation_region_name
, receivinglocation_time_zone = SRC.receivinglocation_time_zone
, receivinglocation_time_zone_offset = SRC.receivinglocation_time_zone_offset
, receivinglocation_address_city = SRC.receivinglocation_address_city
, receivinglocation_address_county = SRC.receivinglocation_address_county
, receivinglocation_address_line_1 = SRC.receivinglocation_address_line_1
, receivinglocation_postal_code = SRC.receivinglocation_postal_code
, dw_batch_id = SRC.dw_batch_id
, dw_batch_date= SRC.dw_batch_date
, dw_sys_load_tmstp = CURRENT_TIMESTAMP(0)
, dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
);
ET;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('STORE_DIM','{db_env}_NAP_DIM','org_load_2656_napstore_insights','org_store_dimension_table',2,'LOAD_END','',current_timestamp,'ORG');
ET;

COLLECT STATISTICS COLUMN(store_num) ON {db_env}_NAP_DIM.STORE_DIM;
ET;

-- Loading peer groups
EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('STORE_PEER_GROUP_DIM','{db_env}_NAP_DIM','org_load_2656_napstore_insights','org_store_peer_group_dimension_table',3,'LOAD_START','',current_timestamp,'ORG');
ET;

NONSEQUENCED VALIDTIME --PURGE WORK TABLE FOR STAGING TEMPORAL ROWS
DELETE FROM {db_env}_NAP_STG.STORE_PEER_GROUP_DIM_VTW;


INSERT INTO {db_env}_NAP_STG.STORE_PEER_GROUP_DIM_VTW
( STORE_NUM
, PEER_GROUP_NUM
, PEER_GROUP_DESC
, PEER_GROUP_SHORT_DESC
, PEER_GROUP_TYPE_CODE
, PEER_GROUP_TYPE_DESC
, EFF_BEGIN_TMSTP
, EFF_END_TMSTP
)
SELECT
  NRML.STORE_NUM
, NRML.PEER_GROUP_NUM
, NRML.PEER_GROUP_DESC
, NRML.PEER_GROUP_SHORT_DESC
, NRML.PEER_GROUP_TYPE_CODE
, NRML.PEER_GROUP_TYPE_DESC
, BEGIN(NRML.EFF_PERIOD) AS EFF_BEGIN
, END(NRML.EFF_PERIOD)   AS EFF_END
FROM (
NONSEQUENCED VALIDTIME
SELECT NORMALIZE
  SRC.STORE_NUM
, SRC.PEER_GROUP_NUM
, SRC.PEER_GROUP_DESC
, SRC.PEER_GROUP_SHORT_DESC
, SRC.PEER_GROUP_TYPE_CODE
, SRC.PEER_GROUP_TYPE_DESC
, COALESCE( SRC.EFF_PERIOD P_INTERSECT PERIOD(TGT.EFF_BEGIN_TMSTP,TGT.EFF_END_TMSTP)
          , SRC.EFF_PERIOD ) AS EFF_PERIOD
FROM (
	SELECT NORMALIZE
    STORE_NUM,PEER_GROUP_NUM,PEER_GROUP_DESC,
    PEER_GROUP_SHORT_DESC,PEER_GROUP_TYPE_CODE,PEER_GROUP_TYPE_DESC
    , PERIOD(EFF_BEGIN_TMSTP, EFF_END_TMSTP) AS EFF_PERIOD
	FROM (
	SELECT SRC_2.*
	, COALESCE(MAX(EFF_BEGIN_TMSTP) OVER(PARTITION BY STORE_NUM,PEER_GROUP_NUM
               ORDER BY EFF_BEGIN_TMSTP
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS EFF_END_TMSTP
	FROM (
	SELECT
 		CURRENTDATA_STORENUMBER AS STORE_NUM
 		, CURRENTDATA_PEERGROUPS_PEERGROUPNUMBER AS PEER_GROUP_NUM
 		, CURRENTDATA_PEERGROUPS_PEERGROUPDESC AS PEER_GROUP_DESC
 		, CURRENTDATA_PEERGROUPS_PEERGROUPSHORTDESC AS PEER_GROUP_SHORT_DESC
 		, CURRENTDATA_PEERGROUPS_PEERGROUPTYPECODE AS PEER_GROUP_TYPE_CODE
 		, CURRENTDATA_PEERGROUPS_PEERGROUPTYPEDESC AS PEER_GROUP_TYPE_DESC
 		, CURRENTDATAUPDATEDTIMESTAMP
 		,(NORD_UDF.ISO8601_TMSTP(CURRENTDATAUPDATEDTIMESTAMP) (NAMED EFF_BEGIN_TMSTP))
 		FROM {db_env}_NAP_BASE_VWS.STORE_PEER_GROUP_DIM_LDG
	) SRC_2
	QUALIFY EFF_BEGIN_TMSTP < EFF_END_TMSTP
	) SRC_1
) SRC
LEFT JOIN {db_env}_NAP_BASE_VWS.STORE_PEER_GROUP_DIM_HIST TGT
ON SRC.STORE_NUM = TGT.STORE_NUM AND SRC.PEER_GROUP_NUM = TGT.PEER_GROUP_NUM
AND SRC.EFF_PERIOD OVERLAPS PERIOD(TGT.EFF_BEGIN_TMSTP, TGT.EFF_END_TMSTP)
WHERE ( TGT.STORE_NUM IS NULL OR TGT.PEER_GROUP_NUM IS NULL OR
   (  (SRC.PEER_GROUP_DESC <> TGT.PEER_GROUP_DESC OR (SRC.PEER_GROUP_DESC IS NOT NULL AND TGT.PEER_GROUP_DESC IS NULL) OR (SRC.PEER_GROUP_DESC IS NULL AND TGT.PEER_GROUP_DESC IS NOT NULL))
   OR (SRC.PEER_GROUP_SHORT_DESC <> TGT.PEER_GROUP_SHORT_DESC OR (SRC.PEER_GROUP_SHORT_DESC IS NOT NULL AND TGT.PEER_GROUP_SHORT_DESC IS NULL) OR (SRC.PEER_GROUP_SHORT_DESC IS NULL AND TGT.PEER_GROUP_SHORT_DESC IS NOT NULL))
   OR (SRC.PEER_GROUP_TYPE_CODE <> TGT.PEER_GROUP_TYPE_CODE OR (SRC.PEER_GROUP_TYPE_CODE IS NOT NULL AND TGT.PEER_GROUP_TYPE_CODE IS NULL) OR (SRC.PEER_GROUP_TYPE_CODE IS NULL AND TGT.PEER_GROUP_TYPE_CODE IS NOT NULL))
   OR (SRC.PEER_GROUP_TYPE_DESC <> TGT.PEER_GROUP_TYPE_DESC OR (SRC.PEER_GROUP_TYPE_DESC IS NOT NULL AND TGT.PEER_GROUP_TYPE_DESC IS NULL) OR (SRC.PEER_GROUP_TYPE_DESC IS NULL AND TGT.PEER_GROUP_TYPE_DESC IS NOT NULL))
  )  )
) NRML
;
ET;

SEQUENCED VALIDTIME
DELETE FROM {db_env}_NAP_DIM.STORE_PEER_GROUP_DIM AS TGT,
           {db_env}_NAP_STG.STORE_PEER_GROUP_DIM_VTW AS SRC
WHERE SRC.STORE_NUM = TGT.STORE_NUM AND SRC.PEER_GROUP_NUM = TGT.PEER_GROUP_NUM
;
ET;

INSERT INTO {db_env}_NAP_DIM.STORE_PEER_GROUP_DIM
( STORE_NUM
, PEER_GROUP_NUM
, PEER_GROUP_DESC
, PEER_GROUP_SHORT_DESC
, PEER_GROUP_TYPE_CODE
, PEER_GROUP_TYPE_DESC
, EFF_BEGIN_TMSTP
, EFF_END_TMSTP
, DW_BATCH_ID
, DW_BATCH_DATE
, DW_SYS_LOAD_TMSTP
)
SELECT
  STORE_NUM
, PEER_GROUP_NUM
, PEER_GROUP_DESC
, PEER_GROUP_SHORT_DESC
, PEER_GROUP_TYPE_CODE
, PEER_GROUP_TYPE_DESC
, EFF_BEGIN_TMSTP
, EFF_END_TMSTP
--,(SELECT MAX(dw_batch_id)+1 FROM {db_env}_NAP_DIM.STORE_PEER_GROUP_DIM) AS dw_batch_id
--,current_date as dw_batch_date
,(SELECT BATCH_ID FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='ORG_STORE') AS DW_BATCH_ID
,(SELECT CURR_BATCH_DATE FROM {db_env}_NAP_BASE_VWS.ELT_CONTROL WHERE SUBJECT_AREA_NM ='ORG_STORE') AS DW_BATCH_DATE
, CURRENT_TIMESTAMP(0) AS DW_SYS_LOAD_TMSTP
FROM {db_env}_NAP_STG.STORE_PEER_GROUP_DIM_VTW
;

-- CLOSE THE COMBINATION WHICH IS NOT ACTIVE IN LANDING
UPDATE DIM
FROM   {db_env}_NAP_DIM.STORE_PEER_GROUP_DIM AS DIM ,
       (
        SELECT   CURRENTDATA_STORENUMBER  AS STORE_NUM ,
             MAX((NORD_UDF.ISO8601_TMSTP(CURRENTDATAUPDATEDTIMESTAMP))) AS EFF_BEGIN_TMSTP
        FROM     {db_env}_NAP_BASE_VWS.STORE_PEER_GROUP_DIM_LDG
        GROUP BY 1 )LDGT
SET    EFF_END_TMSTP = LDGT.EFF_BEGIN_TMSTP
WHERE  DIM.STORE_NUM = LDGT.STORE_NUM
AND    LDGT.EFF_BEGIN_TMSTP BETWEEN BEGIN (DIM.EFFECTIVE_PERIOD ) AND
       END(DIM.EFFECTIVE_PERIOD)
AND ( DIM.STORE_NUM,DIM.PEER_GROUP_NUM) NOT IN
    (
     SELECT STORE_NUM,
         PEER_GROUP_NUM
     FROM   (
         SELECT   CURRENTDATA_STORENUMBER      AS STORE_NUM ,
              CURRENTDATA_PEERGROUPS_PEERGROUPNUMBER   AS PEER_GROUP_NUM ,
              RANK() OVER(PARTITION BY STORE_NUM
              ORDER BY (NORD_UDF.ISO8601_TMSTP(CURRENTDATAUPDATEDTIMESTAMP)) DESC )    RNK
         FROM     {db_env}_NAP_BASE_VWS.STORE_PEER_GROUP_DIM_LDG )LLD
     WHERE  RNK =1 )
     ;
ET;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('STORE_PEER_GROUP_DIM','{db_env}_NAP_DIM','org_load_2656_napstore_insights','org_store_peer_group_dimension_table',4,'LOAD_END','',current_timestamp,'ORG');
ET;

COLLECT STATISTICS COLUMN(STORE_NUM) ON {db_env}_NAP_DIM.STORE_PEER_GROUP_DIM;
ET;

SET QUERY_BAND = NONE FOR SESSION;
ET;