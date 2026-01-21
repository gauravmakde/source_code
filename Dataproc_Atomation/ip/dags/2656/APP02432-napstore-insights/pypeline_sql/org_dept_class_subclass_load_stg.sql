SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=org_load_2656_napstore_insights;
Task_Name=org_dept_dim_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view org_dept_dim_input AS
select *
from kafka_org_dept_dim_input;

-- Writing Kafka to Semantic Layer:
insert
overwrite table org_dept_dim_stg_table
select      
       currentData.deptNumber as currentdata_deptnumber,
       currentData.deptType.subTypeCode as currentdata_depttype_subtypecode,
       currentData.deptType.subTypeDesc as currentdata_depttype_subtypedesc,
       currentData.deptType.typeCode as currentdata_depttype_typecode,
       currentData.deptType.typeDesc as currentdata_depttype_typedesc,
       cast(currentData.deptType.merchDept as string) as currentdata_depttype_merchdept,
       currentData.desc as currentdata_desc,
       currentData.medDesc as currentdata_meddesc,
       currentData.hierarchy.divType.code as currentdata_hierarchy_divtype_code,
       currentData.hierarchy.divType.desc as currentdata_hierarchy_divtype_desc,
       currentData.hierarchy.divisionDesc as currentdata_hierarchy_divisiondesc,
       currentData.hierarchy.divisionMedDesc as currentdata_hierarchy_divisionmeddesc,
       currentData.hierarchy.divisionNumber as currentdata_hierarchy_divisionnumber,
       currentData.hierarchy.subDivisionDesc as currentdata_hierarchy_subdivisiondesc,
       currentData.hierarchy.subDivisionMedDesc as currentdata_hierarchy_subdivisionmeddesc,
       currentData.hierarchy.subDivisionNumber as currentdata_hierarchy_subdivisionnumber,
       currentData.status as currentdata_status,
       currentDataUpdatedTimeStamp
from org_dept_dim_input;

create
temporary view org_dept_class_dim_input AS
select 
       currentData.deptNumber as currentdata_deptnumber,
       currentData.deptType.subTypeCode as currentdata_depttype_subtypecode,
       currentData.deptType.subTypeDesc as currentdata_depttype_subtypedesc,
       currentData.deptType.typeCode as currentdata_depttype_typecode,
       currentData.deptType.typeDesc as currentdata_depttype_typedesc,
       cast(currentData.deptType.merchDept as string) as currentdata_depttype_merchdept,
       currentData.desc as currentdata_desc,
       currentData.medDesc as currentdata_meddesc,
       currentData.hierarchy.divType.code as currentdata_hierarchy_divtype_code,
       currentData.hierarchy.divType.desc as currentdata_hierarchy_divtype_desc,
       currentData.hierarchy.divisionDesc as currentdata_hierarchy_divisiondesc,
       currentData.hierarchy.divisionMedDesc as currentdata_hierarchy_divisionmeddesc,
       currentData.hierarchy.divisionNumber as currentdata_hierarchy_divisionnumber,
       currentData.hierarchy.subDivisionDesc as currentdata_hierarchy_subdivisiondesc,
       currentData.hierarchy.subDivisionMedDesc as currentdata_hierarchy_subdivisionmeddesc,
       currentData.hierarchy.subDivisionNumber as currentdata_hierarchy_subdivisionnumber,
       currentData.status as currentdata_status,
       currentDataUpdatedTimeStamp,
       explode_outer(currentData.classes) as class
from org_dept_dim_input;

create
temporary view org_dept_class_subclass_dim_input AS
select
       currentdata_deptnumber,
       currentdata_depttype_subtypecode,
       currentdata_depttype_subtypedesc,
       currentdata_depttype_typecode,
       currentdata_depttype_typedesc,
       currentdata_depttype_merchdept,
       currentdata_desc,
       currentdata_meddesc,
       currentdata_hierarchy_divtype_code,
       currentdata_hierarchy_divtype_desc,
       currentdata_hierarchy_divisiondesc,
       currentdata_hierarchy_divisionmeddesc,
       currentdata_hierarchy_divisionnumber,
       currentdata_hierarchy_subdivisiondesc,
       currentdata_hierarchy_subdivisionmeddesc,
       currentdata_hierarchy_subdivisionnumber,
       currentdata_status,
       currentDataUpdatedTimeStamp,
       class.classNumber as currentdata_classes_classNumber,
       class.desc as currentdata_classes_desc,
       class.medDesc as currentdata_classes_medDesc,
       class.taxCode as currentdata_classes_taxCode,
       class.taxDesc as currentdata_classes_taxDesc,
       explode_outer(class.subclasses) as subclass
from org_dept_class_dim_input;

insert
overwrite table org_dept_class_subclass_dim_stg_table
select
       currentdata_deptnumber,
       currentdata_depttype_subtypecode,
       currentdata_depttype_subtypedesc,
       currentdata_depttype_typecode,
       currentdata_depttype_typedesc,
       currentdata_depttype_merchdept,
       currentdata_desc,
       currentdata_meddesc,
       currentdata_hierarchy_divtype_code,
       currentdata_hierarchy_divtype_desc,
       currentdata_hierarchy_divisiondesc,
       currentdata_hierarchy_divisionmeddesc,
       currentdata_hierarchy_divisionnumber,
       currentdata_hierarchy_subdivisiondesc,
       currentdata_hierarchy_subdivisionmeddesc,
       currentdata_hierarchy_subdivisionnumber,
       currentdata_status,
       currentDataUpdatedTimeStamp,
       currentdata_classes_classNumber,
       currentdata_classes_desc,
       currentdata_classes_medDesc,
       currentdata_classes_taxCode,
       currentdata_classes_taxDesc,
       subclass.subclassNumber as currentdata_classes_subclasses_subclassNumber,
       subclass.desc as currentdata_classes_subclasses_desc,
       subclass.medDesc as currentdata_classes_subclasses_medDesc
from org_dept_class_subclass_dim_input;
