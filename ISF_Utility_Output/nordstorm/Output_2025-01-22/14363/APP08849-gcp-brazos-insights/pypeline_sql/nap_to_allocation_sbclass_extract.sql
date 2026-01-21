SET QUERY_BAND = '
App_ID=APP08849;
DAG_ID=nap_to_allocation_product_location_extracts;
Task_Name=sbclass_extract;'
FOR SESSION VOLATILE;

--- Read the product data 
create temporary view product_dim_vw as 
select * from default.department_class_subclass_dim;

-- generating the extract Data
create temporary view sbclass_extract as 
select 1 AS OPERATION_CODE, 
       dept_num,
       class_num,
       subclass_num,
       regexp_replace(subclass_name,"[\"]",'') as subclass_name
from  product_dim_vw
where subclass_num <> -1
group by dept_num,
         class_num,
         subclass_num,
         subclass_name;

--- Writing Data to s3 on a single partition
insert overwrite table sbclass_tab
select /*+ COALESCE(1) */ * 
  from sbclass_extract;

insert overwrite table sbclass_audit 
select concat(date_format(from_utc_timestamp(current_timestamp,'PST'),'yyyyMMddHHmmss'),' ',format_number(count(1),'000000000000')) 
  from sbclass_extract;
