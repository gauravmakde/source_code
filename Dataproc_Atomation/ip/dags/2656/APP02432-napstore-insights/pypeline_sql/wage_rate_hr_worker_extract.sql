SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wage_rate_to_s3_extract_2656_napstore_insights;
Task_Name=wage_rate_extract_load_1_hr_worker;'
FOR SESSION VOLATILE;

--Reading Data from  Source Teradata
create temporary view  temp_hr_worker_v1_dim_read AS
select worker_number, payroll_store, payroll_department from hr_worker_v1_dim
where worker_status = 'ACTIVE' and pay_rate_type = 'HOURLY';


--Writing S3 Data to S3
insert into table hr_worker_v1_dim_s3_output   partition (year, month, day )
select *,year( current_date()) as year,month( current_date()) as month,day( current_date()) as day
from temp_hr_worker_v1_dim_read;
