SET QUERY_BAND = '
App_ID=APP08849;
DAG_ID=nap_to_allocation_product_location_calender_extracts;
Task_Name=calendar_extracts;'
FOR SESSION VOLATILE;

-- generating the extract Data
create temporary view calendar_extracts as 
select '1',
       fiscal_year_num,
       fiscal_week_num
  from OBJECT_MODEL.DAY_CAL_454_DIM 
 where day_date=date_format((current_date()+1), 'MM/dd/yyyy');

--- Writing Data to s3 on a single partition
insert overwrite table calendar_tab
select * from  calendar_extracts; 

insert overwrite table calendar_audit select concat(date_format(from_utc_timestamp(current_timestamp,'PST'),'yyyyMMddHHmmss'),' ',format_number(count(1),'000000000000')) from calendar_extracts;
