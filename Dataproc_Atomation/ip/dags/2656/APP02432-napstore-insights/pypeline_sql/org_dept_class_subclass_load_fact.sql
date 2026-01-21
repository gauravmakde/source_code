SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=org_load_2656_napstore_insights;
Task_Name=org_dept_dim_load_1_fct_table;'
FOR SESSION VOLATILE;

ET;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('DEPARTMENT_DIM','{db_env}_NAP_DIM','org_load_2656_napstore_insights','org_department_dimension_table',1,'LOAD_START','',current_timestamp,'ORG');
ET;

NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
DELETE FROM {db_env}_NAP_STG.DEPARTMENT_DIM_VTW;
ET;

INSERT INTO {db_env}_NAP_STG.DEPARTMENT_DIM_VTW
( dept_num
, dept_name
, dept_short_name
, dept_type_code
, dept_type_desc
, dept_subtype_code
, dept_subtype_desc
, merch_dept_ind
, division_num
, division_name
, division_short_name
, division_type_code
, division_type_desc
, subdivision_num
, subdivision_name
, subdivision_short_name
, active_store_ind
, eff_begin_tmstp
, eff_end_tmstp
)
SELECT
  NRML.dept_num
, NRML.dept_name
, NRML.dept_short_name
, NRML.dept_type_code
, NRML.dept_type_desc
, NRML.dept_subtype_code
, NRML.dept_subtype_desc
, NRML.merch_dept_ind
, NRML.division_num
, NRML.division_name
, NRML.division_short_name
, NRML.division_type_code
, NRML.division_type_desc
, NRML.subdivision_num
, NRML.subdivision_name
, NRML.subdivision_short_name
, NRML.active_store_ind
, BEGIN(NRML.eff_period) AS eff_begin
, END(NRML.eff_period)   AS eff_end
FROM (
NONSEQUENCED VALIDTIME
SELECT NORMALIZE
  SRC.dept_num
, SRC.dept_name
, SRC.dept_short_name
, SRC.dept_type_code
, SRC.dept_type_desc
, SRC.dept_subtype_code
, SRC.dept_subtype_desc
, SRC.merch_dept_ind
, SRC.division_num
, SRC.division_name
, SRC.division_short_name
, SRC.division_type_code
, SRC.division_type_desc
, SRC.subdivision_num
, SRC.subdivision_name
, SRC.subdivision_short_name
, SRC.active_store_ind
, COALESCE( SRC.eff_period P_INTERSECT PERIOD(TGT.eff_begin_tmstp,TGT.eff_end_tmstp)
          , SRC.eff_period ) AS eff_period
FROM (
	SELECT NORMALIZE dept_num, dept_name, dept_short_name, dept_type_code, dept_type_desc
	, dept_subtype_code, dept_subtype_desc, merch_dept_ind, division_num, division_name, division_short_name
	, division_type_code, division_type_desc, subdivision_num, subdivision_name, subdivision_short_name, active_store_ind
	, PERIOD(eff_begin_tmstp, eff_end_tmstp) AS eff_period
	FROM (
	SELECT SRC_2.*
	, COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY dept_num ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	FROM (
	SELECT DISTINCT
	  currentData_deptNumber AS dept_num
	, currentData_desc AS dept_name
	, currentData_medDesc AS dept_short_name
	, currentData_deptType_typeCode AS dept_type_code
	, currentData_deptType_typeDesc AS dept_type_desc
	, currentData_deptType_subTypeCode AS dept_subtype_code
	, currentData_deptType_subTypeDesc AS dept_subtype_desc
	, (CASE WHEN currentData_deptType_merchDept='true' THEN 'Y' ELSE 'N' END) AS merch_dept_ind
	, currentData_hierarchy_divisionNumber AS division_num
	, currentData_hierarchy_divisionDesc AS division_name
	, currentData_hierarchy_divisionMedDesc AS division_short_name
	, currentData_hierarchy_divType_code AS division_type_code
	, currentData_hierarchy_divType_desc AS division_type_desc
	, currentData_hierarchy_subDivisionNumber AS subdivision_num
	, currentData_hierarchy_subDivisionDesc AS subdivision_name
	, currentData_hierarchy_subDivisionMedDesc AS subdivision_short_name
	, COALESCE (currentData_status, 'U') AS active_store_ind
	,(NORD_UDF.ISO8601_TMSTP(currentDataUpdatedTimeStamp) (NAMED eff_begin_tmstp))
	FROM {db_env}_NAP_BASE_VWS.DEPARTMENT_DIM_LDG
	) SRC_2
	QUALIFY eff_begin_tmstp < eff_end_tmstp
	) SRC_1
) SRC
LEFT JOIN {db_env}_NAP_BASE_VWS.DEPARTMENT_DIM_HIST TGT
ON SRC.dept_num = TGT.dept_num
AND SRC.eff_period OVERLAPS PERIOD(TGT.eff_begin_tmstp, TGT.eff_end_tmstp)
WHERE ( TGT.dept_num IS NULL OR
   (  (SRC.dept_name <> TGT.dept_name OR (SRC.dept_name IS NOT NULL AND TGT.dept_name IS NULL) OR (SRC.dept_name IS NULL AND TGT.dept_name IS NOT NULL))
   OR (SRC.dept_short_name <> TGT.dept_short_name OR (SRC.dept_short_name IS NOT NULL AND TGT.dept_short_name IS NULL) OR (SRC.dept_short_name IS NULL AND TGT.dept_short_name IS NOT NULL))
   OR (SRC.dept_type_code <> TGT.dept_type_code OR (SRC.dept_type_code IS NOT NULL AND TGT.dept_type_code IS NULL) OR (SRC.dept_type_code IS NULL AND TGT.dept_type_code IS NOT NULL))
   OR (SRC.dept_type_desc <> TGT.dept_type_desc OR (SRC.dept_type_desc IS NOT NULL AND TGT.dept_type_desc IS NULL) OR (SRC.dept_type_desc IS NULL AND TGT.dept_type_desc IS NOT NULL))
   OR (SRC.dept_subtype_code <> TGT.dept_subtype_code OR (SRC.dept_subtype_code IS NOT NULL AND TGT.dept_subtype_code IS NULL) OR (SRC.dept_subtype_code IS NULL AND TGT.dept_subtype_code IS NOT NULL))
   OR (SRC.dept_subtype_desc <> TGT.dept_subtype_desc OR (SRC.dept_subtype_desc IS NOT NULL AND TGT.dept_subtype_desc IS NULL) OR (SRC.dept_subtype_desc IS NULL AND TGT.dept_subtype_desc IS NOT NULL))
   OR (SRC.merch_dept_ind <> TGT.merch_dept_ind OR (SRC.merch_dept_ind IS NOT NULL AND TGT.merch_dept_ind IS NULL) OR (SRC.merch_dept_ind IS NULL AND TGT.merch_dept_ind IS NOT NULL))
   OR (SRC.division_num <> TGT.division_num OR (SRC.division_num IS NOT NULL AND TGT.division_num IS NULL) OR (SRC.division_num IS NULL AND TGT.division_num IS NOT NULL))
   OR (SRC.division_name <> TGT.division_name OR (SRC.division_name IS NOT NULL AND TGT.division_name IS NULL) OR (SRC.division_name IS NULL AND TGT.division_name IS NOT NULL))
   OR (SRC.division_short_name <> TGT.division_short_name OR (SRC.division_short_name IS NOT NULL AND TGT.division_short_name IS NULL) OR (SRC.division_short_name IS NULL AND TGT.division_short_name IS NOT NULL))
   OR (SRC.division_type_code <> TGT.division_type_code OR (SRC.division_type_code IS NOT NULL AND TGT.division_type_code IS NULL) OR (SRC.division_type_code IS NULL AND TGT.division_type_code IS NOT NULL))
   OR (SRC.division_type_desc <> TGT.division_type_desc OR (SRC.division_type_desc IS NOT NULL AND TGT.division_type_desc IS NULL) OR (SRC.division_type_desc IS NULL AND TGT.division_type_desc IS NOT NULL))
   OR (SRC.subdivision_num <> TGT.subdivision_num OR (SRC.subdivision_num IS NOT NULL AND TGT.subdivision_num IS NULL) OR (SRC.subdivision_num IS NULL AND TGT.subdivision_num IS NOT NULL))
   OR (SRC.subdivision_name <> TGT.subdivision_name OR (SRC.subdivision_name IS NOT NULL AND TGT.subdivision_name IS NULL) OR (SRC.subdivision_name IS NULL AND TGT.subdivision_name IS NOT NULL))
   OR (SRC.subdivision_short_name <> TGT.subdivision_short_name OR (SRC.subdivision_short_name IS NOT NULL AND TGT.subdivision_short_name IS NULL) OR (SRC.subdivision_short_name IS NULL AND TGT.subdivision_short_name IS NOT NULL))
   OR (SRC.active_store_ind <> TGT.active_store_ind OR (SRC.active_store_ind IS NOT NULL AND TGT.active_store_ind IS NULL) OR (SRC.active_store_ind IS NULL AND TGT.active_store_ind IS NOT NULL))
   )  )
) NRML;
ET;

SEQUENCED VALIDTIME
DELETE FROM {db_env}_NAP_DIM.DEPARTMENT_DIM AS TGT, {db_env}_NAP_STG.DEPARTMENT_DIM_VTW AS SRC
WHERE SRC.dept_num = TGT.dept_num;
ET;

INSERT INTO {db_env}_NAP_DIM.DEPARTMENT_DIM
( dept_num
, dept_name
, dept_short_name
, dept_type_code
, dept_type_desc
, dept_subtype_code
, dept_subtype_desc
, merch_dept_ind
, division_num
, division_name
, division_short_name
, division_type_code
, division_type_desc
, subdivision_num
, subdivision_name
, subdivision_short_name
, active_store_ind
, eff_begin_tmstp
, eff_end_tmstp
, dw_batch_id
, dw_batch_date
, dw_sys_load_tmstp
)
SELECT
  dept_num
, dept_name
, dept_short_name
, dept_type_code
, dept_type_desc
, dept_subtype_code
, dept_subtype_desc
, merch_dept_ind
, division_num
, division_name
, division_short_name
, division_type_code
, division_type_desc
, subdivision_num
, subdivision_name
, subdivision_short_name
, active_store_ind
, eff_begin_tmstp
, eff_end_tmstp
,(SELECT MAX(dw_batch_id)+1 FROM {db_env}_NAP_DIM.DEPARTMENT_DIM) AS dw_batch_id
,current_date as dw_batch_date
,CURRENT_TIMESTAMP(0) AS dw_sys_load_tmstp
FROM {db_env}_NAP_STG.DEPARTMENT_DIM_VTW
;
ET;


EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('DEPARTMENT_DIM','{db_env}_NAP_DIM','org_load_2656_napstore_insights','org_department_dimension_table',2,'LOAD_END','',current_timestamp,'ORG');
ET;

-- Loading subclasses
EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('DEPARTMENT_CLASS_SUBCLASS_DIM','{db_env}_NAP_DIM','org_load_2656_napstore_insights','org_class_dimension_table',3,'LOAD_START','',current_timestamp,'ORG');
ET;

NONSEQUENCED VALIDTIME --Purge work table for staging temporal rows
DELETE FROM {db_env}_NAP_STG.DEPARTMENT_CLASS_SUBCLASS_DIM_VTW;

INSERT INTO {db_env}_NAP_STG.DEPARTMENT_CLASS_SUBCLASS_DIM_VTW
( dept_num
, dept_name
, dept_short_name
, dept_type_code
, dept_type_desc
, dept_subtype_code
, dept_subtype_desc
, merch_dept_ind
, division_num
, division_name
, division_short_name
, division_type_code
, division_type_desc
, subdivision_num
, subdivision_name
, subdivision_short_name
, active_store_ind
, eff_begin_tmstp
, eff_end_tmstp
, class_num
, class_name
, class_short_name
, class_tax_code
, class_tax_desc
, subclass_num
, subclass_name
, subclass_short_name
)
SELECT
 NRML.dept_num
, NRML.dept_name
, NRML.dept_short_name
, NRML.dept_type_code
, NRML.dept_type_desc
, NRML.dept_subtype_code
, NRML.dept_subtype_desc
, NRML.merch_dept_ind
, NRML.division_num
, NRML.division_name
, NRML.division_short_name
, NRML.division_type_code
, NRML.division_type_desc
, NRML.subdivision_num
, NRML.subdivision_name
, NRML.subdivision_short_name
, NRML.active_store_ind
, BEGIN(NRML.eff_period) AS eff_begin
, END(NRML.eff_period)   AS eff_end
, NRML.class_num
, NRML.class_name
, NRML.class_short_name
, NRML.class_tax_code
, NRML.class_tax_desc
, NRML.subclass_num
, NRML.subclass_name
, NRML.subclass_short_name
FROM (
NONSEQUENCED VALIDTIME
SELECT NORMALIZE
  SRC.dept_num
, SRC.dept_name
, SRC.dept_short_name
, SRC.dept_type_code
, SRC.dept_type_desc
, SRC.dept_subtype_code
, SRC.dept_subtype_desc
, SRC.merch_dept_ind
, SRC.division_num
, SRC.division_name
, SRC.division_short_name
, SRC.division_type_code
, SRC.division_type_desc
, SRC.subdivision_num
, SRC.subdivision_name
, SRC.subdivision_short_name
, SRC.active_store_ind
, COALESCE( SRC.eff_period P_INTERSECT PERIOD(TGT.eff_begin_tmstp,TGT.eff_end_tmstp)
          , SRC.eff_period ) AS eff_period
, SRC.class_num
, SRC.class_name
, SRC.class_short_name
, SRC.class_tax_code
, SRC.class_tax_desc
, SRC.subclass_num
, SRC.subclass_name
, SRC.subclass_short_name
FROM (
	SELECT NORMALIZE dept_num, dept_name, dept_short_name, dept_type_code, dept_type_desc
	, dept_subtype_code, dept_subtype_desc, merch_dept_ind, division_num, division_name, division_short_name
	, division_type_code, division_type_desc, subdivision_num, subdivision_name, subdivision_short_name, active_store_ind
	, PERIOD(eff_begin_tmstp, eff_end_tmstp) AS eff_period, class_num, class_name, class_short_name, class_tax_code, class_tax_desc
    , subclass_num, subclass_name, subclass_short_name
	FROM (
	SELECT SRC_2.*
	, COALESCE(MAX(eff_begin_tmstp) OVER(PARTITION BY dept_num,class_num,subclass_num  ORDER BY eff_begin_tmstp
	           ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
	          ,TIMESTAMP'9999-12-31 23:59:59.999999+00:00') AS eff_end_tmstp
	FROM (
	SELECT DISTINCT
	  CAST(currentData_deptNumber AS INTEGER) AS dept_num
	, currentData_desc AS dept_name
	, currentData_medDesc AS dept_short_name
	, currentData_deptType_typeCode AS dept_type_code
	, currentData_deptType_typeDesc AS dept_type_desc
	, currentData_deptType_subTypeCode AS dept_subtype_code
	, currentData_deptType_subTypeDesc AS dept_subtype_desc
	, (CASE WHEN currentData_deptType_merchDept='true' THEN 'Y' ELSE 'N' END) AS merch_dept_ind
	, currentData_hierarchy_divisionNumber AS division_num
	, currentData_hierarchy_divisionDesc AS division_name
	, currentData_hierarchy_divisionMedDesc AS division_short_name
	, currentData_hierarchy_divType_code AS division_type_code
	, currentData_hierarchy_divType_desc AS division_type_desc
	, currentData_hierarchy_subDivisionNumber AS subdivision_num
	, currentData_hierarchy_subDivisionDesc AS subdivision_name
	, currentData_hierarchy_subDivisionMedDesc AS subdivision_short_name
	, COALESCE (currentData_status, 'U') AS active_store_ind
	,(NORD_UDF.ISO8601_TMSTP(currentDataUpdatedTimeStamp) at 'GMT' (NAMED eff_begin_tmstp))
    , COALESCE(CAST(currentdata_classes_classNumber AS INTEGER),-1) AS class_num
    , currentdata_classes_desc AS class_name
    , currentdata_classes_medDesc AS class_short_name
    , currentdata_classes_taxCode AS class_tax_code
    , currentdata_classes_taxDesc AS class_tax_desc
    , COALESCE(CAST(currentdata_classes_subclasses_subclassNumber AS INTEGER),-1) AS subclass_num
    , currentdata_classes_subclasses_desc AS subclass_name
    , currentdata_classes_subclasses_medDesc AS subclass_short_name

	FROM {db_env}_NAP_BASE_VWS.DEPARTMENT_CLASS_SUBCLASS_DIM_LDG
	) SRC_2
	QUALIFY eff_begin_tmstp < eff_end_tmstp
	) SRC_1
) SRC
LEFT JOIN {db_env}_NAP_BASE_VWS.DEPARTMENT_CLASS_SUBCLASS_DIM_HIST TGT
ON SRC.dept_num = TGT.dept_num and SRC.class_num = TGT.class_num and SRC.subclass_num = TGT.subclass_num
AND SRC.eff_period OVERLAPS PERIOD(TGT.eff_begin_tmstp, TGT.eff_end_tmstp)
-----insert only if there is a change detected in any of these values and SRC.class_num = TGT.class_num and SRC.subclass_num = TGT.subclass_num
WHERE ( TGT.dept_num IS NULL OR TGT.class_num IS NULL OR TGT.subclass_num IS NULL OR
   (  (SRC.dept_name <> TGT.dept_name OR (SRC.dept_name IS NOT NULL AND TGT.dept_name IS NULL) OR (SRC.dept_name IS NULL AND TGT.dept_name IS NOT NULL))
   OR (SRC.dept_short_name <> TGT.dept_short_name OR (SRC.dept_short_name IS NOT NULL AND TGT.dept_short_name IS NULL) OR (SRC.dept_short_name IS NULL AND TGT.dept_short_name IS NOT NULL))
   OR (SRC.dept_type_code <> TGT.dept_type_code OR (SRC.dept_type_code IS NOT NULL AND TGT.dept_type_code IS NULL) OR (SRC.dept_type_code IS NULL AND TGT.dept_type_code IS NOT NULL))
   OR (SRC.dept_type_desc <> TGT.dept_type_desc OR (SRC.dept_type_desc IS NOT NULL AND TGT.dept_type_desc IS NULL) OR (SRC.dept_type_desc IS NULL AND TGT.dept_type_desc IS NOT NULL))
   OR (SRC.dept_subtype_code <> TGT.dept_subtype_code OR (SRC.dept_subtype_code IS NOT NULL AND TGT.dept_subtype_code IS NULL) OR (SRC.dept_subtype_code IS NULL AND TGT.dept_subtype_code IS NOT NULL))
   OR (SRC.dept_subtype_desc <> TGT.dept_subtype_desc OR (SRC.dept_subtype_desc IS NOT NULL AND TGT.dept_subtype_desc IS NULL) OR (SRC.dept_subtype_desc IS NULL AND TGT.dept_subtype_desc IS NOT NULL))
   OR (SRC.merch_dept_ind <> TGT.merch_dept_ind OR (SRC.merch_dept_ind IS NOT NULL AND TGT.merch_dept_ind IS NULL) OR (SRC.merch_dept_ind IS NULL AND TGT.merch_dept_ind IS NOT NULL))
   OR (SRC.division_num <> TGT.division_num OR (SRC.division_num IS NOT NULL AND TGT.division_num IS NULL) OR (SRC.division_num IS NULL AND TGT.division_num IS NOT NULL))
   OR (SRC.division_name <> TGT.division_name OR (SRC.division_name IS NOT NULL AND TGT.division_name IS NULL) OR (SRC.division_name IS NULL AND TGT.division_name IS NOT NULL))
   OR (SRC.division_short_name <> TGT.division_short_name OR (SRC.division_short_name IS NOT NULL AND TGT.division_short_name IS NULL) OR (SRC.division_short_name IS NULL AND TGT.division_short_name IS NOT NULL))
   OR (SRC.division_type_code <> TGT.division_type_code OR (SRC.division_type_code IS NOT NULL AND TGT.division_type_code IS NULL) OR (SRC.division_type_code IS NULL AND TGT.division_type_code IS NOT NULL))
   OR (SRC.division_type_desc <> TGT.division_type_desc OR (SRC.division_type_desc IS NOT NULL AND TGT.division_type_desc IS NULL) OR (SRC.division_type_desc IS NULL AND TGT.division_type_desc IS NOT NULL))
   OR (SRC.subdivision_num <> TGT.subdivision_num OR (SRC.subdivision_num IS NOT NULL AND TGT.subdivision_num IS NULL) OR (SRC.subdivision_num IS NULL AND TGT.subdivision_num IS NOT NULL))
   OR (SRC.subdivision_name <> TGT.subdivision_name OR (SRC.subdivision_name IS NOT NULL AND TGT.subdivision_name IS NULL) OR (SRC.subdivision_name IS NULL AND TGT.subdivision_name IS NOT NULL))
   OR (SRC.subdivision_short_name <> TGT.subdivision_short_name OR (SRC.subdivision_short_name IS NOT NULL AND TGT.subdivision_short_name IS NULL) OR (SRC.subdivision_short_name IS NULL AND TGT.subdivision_short_name IS NOT NULL))
   OR (SRC.active_store_ind <> TGT.active_store_ind OR (SRC.active_store_ind IS NOT NULL AND TGT.active_store_ind IS NULL) OR (SRC.active_store_ind IS NULL AND TGT.active_store_ind IS NOT NULL))
   ---OR (SRC.class_num <> TGT.class_num OR (SRC.class_num IS NOT NULL AND TGT.class_num IS NULL) OR (SRC.class_num IS NULL AND TGT.class_num IS NOT NULL))
   OR (SRC.class_name <> TGT.class_name OR (SRC.class_name IS NOT NULL AND TGT.class_name IS NULL) OR (SRC.class_name IS NULL AND TGT.class_name IS NOT NULL))
   OR (SRC.class_short_name <> TGT.class_short_name OR (SRC.class_short_name IS NOT NULL AND TGT.class_short_name IS NULL) OR (SRC.class_short_name IS NULL AND TGT.class_short_name IS NOT NULL))
   OR (SRC.class_tax_code <> TGT.class_tax_code OR (SRC.class_tax_code IS NOT NULL AND TGT.class_tax_code IS NULL) OR (SRC.class_tax_code IS NULL AND TGT.class_tax_code IS NOT NULL))
   OR (SRC.class_tax_desc <> TGT.class_tax_desc OR (SRC.class_tax_desc IS NOT NULL AND TGT.class_tax_desc IS NULL) OR (SRC.class_tax_desc IS NULL AND TGT.class_tax_desc IS NOT NULL))
   --OR (SRC.subclass_num <> TGT.subclass_num OR (SRC.subclass_num IS NOT NULL AND TGT.subclass_num IS NULL) OR (SRC.subclass_num IS NULL AND TGT.subclass_num IS NOT NULL))
   OR (SRC.subclass_name <> TGT.subclass_name OR (SRC.subclass_name IS NOT NULL AND TGT.subclass_name IS NULL) OR (SRC.subclass_name IS NULL AND TGT.subclass_name IS NOT NULL))
   OR (SRC.subclass_short_name <> TGT.subclass_short_name OR (SRC.subclass_short_name IS NOT NULL AND TGT.subclass_short_name IS NULL) OR (SRC.subclass_short_name IS NULL AND TGT.subclass_short_name IS NOT NULL))
   )  )
 ) NRML
;

--We deviate from the standard ETL design pattern because the source team does not give us explicit indicators
--for deleted rows. Instead, the only guarantee is that if any class/subclass rows under a particular department
--are updated then all class/subclass rows for that department are transmitted. Hence, we have to deduce which
--rows have been deleted by checking the data in the LDG table against the target table to see if and class/subclass
--keys under that department are no longer listed with the incoming data.  The below UPDATE statement will end-date
--these "implicitly deleted" records. Note first that this assumes that we don't have out-of-order batches to process
--and we assume that we always need to check just the "current" records in the target table for end-dating.
--Note second that we check the LDG table and not the VTW table as part of this delete because the INSERT above
--into the VTW table automatically filters out cases where the incoming is identical to what is already in the
--target table.
NONSEQUENCED VALIDTIME
UPDATE TGT
FROM {db_env}_NAP_DIM.DEPARTMENT_CLASS_SUBCLASS_DIM TGT, (

SELECT T0.dept_num, T0.class_num, T0.subclass_num, D0.eff_tmstp
FROM {db_env}_NAP_DIM.DEPARTMENT_CLASS_SUBCLASS_DIM T0
JOIN (
SELECT dept_num, MAX(eff_begin_tmstp) AS eff_tmstp
FROM {db_env}_NAP_STG.DEPARTMENT_CLASS_SUBCLASS_DIM_VTW
WHERE eff_end_tmstp > CURRENT_DATE+1
GROUP BY 1
) D0 ON D0.dept_num = T0.dept_num AND T0.eff_end_tmstp > CURRENT_DATE+1

LEFT JOIN (
SELECT DISTINCT CAST(currentData_deptNumber AS INTEGER) AS dept_num
, COALESCE(CAST(currentdata_classes_classNumber AS INTEGER),-1) AS class_num
, COALESCE(CAST(currentdata_classes_subclasses_subclassNumber AS INTEGER),-1) AS subclass_num
FROM {db_env}_NAP_BASE_VWS.DEPARTMENT_CLASS_SUBCLASS_DIM_LDG
) T1
ON T0.dept_num = T1.dept_num AND T0.class_num = T1.class_num AND T0.subclass_num = T1.subclass_num

WHERE t1.dept_num IS NULL  --row exists as current in TGT but not as current in VTW

) SRC
SET eff_end_tmstp = SRC.eff_tmstp
WHERE TGT.dept_num = SRC.dept_num
  AND TGT.class_num = SRC.class_num
  AND TGT.subclass_num = SRC.subclass_num
  AND TGT.eff_end_tmstp > CURRENT_DATE+1
;

--Now, we can follow that standard design pattern for adjusting the other history rows
SEQUENCED VALIDTIME
DELETE FROM {db_env}_NAP_DIM.DEPARTMENT_CLASS_SUBCLASS_DIM AS TGT, {db_env}_NAP_STG.DEPARTMENT_CLASS_SUBCLASS_DIM_VTW AS SRC
WHERE SRC.dept_num = TGT.dept_num and SRC.class_num = TGT.class_num and SRC.subclass_num = TGT.subclass_num
;

INSERT INTO {db_env}_NAP_DIM.DEPARTMENT_CLASS_SUBCLASS_DIM
( dept_num
, dept_name
, dept_short_name
, dept_type_code
, dept_type_desc
, dept_subtype_code
, dept_subtype_desc
, merch_dept_ind
, division_num
, division_name
, division_short_name
, division_type_code
, division_type_desc
, subdivision_num
, subdivision_name
, subdivision_short_name
, active_store_ind
, eff_begin_tmstp
, eff_end_tmstp
, dw_batch_id
, dw_batch_date
, dw_sys_load_tmstp
, class_num
, class_name
, class_short_name
, class_tax_code
, class_tax_desc
, subclass_num
, subclass_name
, subclass_short_name
)
SELECT
  dept_num
, dept_name
, dept_short_name
, dept_type_code
, dept_type_desc
, dept_subtype_code
, dept_subtype_desc
, merch_dept_ind
, division_num
, division_name
, division_short_name
, division_type_code
, division_type_desc
, subdivision_num
, subdivision_name
, subdivision_short_name
, active_store_ind
, eff_begin_tmstp
, eff_end_tmstp
,(SELECT MAX(dw_batch_id)+1 FROM {db_env}_NAP_DIM.DEPARTMENT_CLASS_SUBCLASS_DIM) AS dw_batch_id
,current_date as dw_batch_date
, CURRENT_TIMESTAMP(0) AS dw_sys_load_tmstp
, class_num
, class_name
, class_short_name
, class_tax_code
, class_tax_desc
, subclass_num
, subclass_name
, subclass_short_name
FROM {db_env}_NAP_STG.DEPARTMENT_CLASS_SUBCLASS_DIM_VTW
;

ET;

EXEC {db_env}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD
('DEPARTMENT_CLASS_SUBCLASS_DIM','{db_env}_NAP_DIM','org_load_2656_napstore_insights','org_class_dimension_table',4,'LOAD_END','',current_timestamp,'ORG');
ET;

SET QUERY_BAND = NONE FOR SESSION;
ET;
