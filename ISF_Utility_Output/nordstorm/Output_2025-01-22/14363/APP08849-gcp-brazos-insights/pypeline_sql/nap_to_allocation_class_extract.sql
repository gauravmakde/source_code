SET QUERY_BAND = '
App_ID=APP08849;
DAG_ID=nap_to_allocation_product_location_extracts;
Task_Name=class_extract;'
FOR SESSION VOLATILE;

--- Read the product data 
create temporary view product_dim_vw as 
select * from default.department_class_subclass_dim;

-- generating the extract Data
create temporary view by_class_extract as 
select 1 AS OPERATION_CODE, 
       dept_num,
       class_num,
       class_name
from  product_dim_vw
group by dept_num,
         class_num,
         class_name;

--- Writing Data to s3 on a single partition
insert overwrite table class_tab
select /*+ COALESCE(1) */ * 
  from  by_class_extract;

insert overwrite table class_audit 
select concat(date_format(from_utc_timestamp(current_timestamp,'PST'),'yyyyMMddHHmmss'),' ',format_number(count(1),'000000000000')) 
  from by_class_extract;
