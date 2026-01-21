SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wage_rate_to_s3_extract_2656_napstore_insights;
Task_Name=wage_rate_extract_load_0_employee_pay_rate;'
FOR SESSION VOLATILE;

--Reading Data from  Source Teradata
create temporary view  temp_employee_pay_rate_dim_read AS
select employee_id, base_pay_value from  employee_pay_rate_dim ;


--Writing S3 Data to S3
insert into table employee_pay_rate_s3_output   partition (year, month, day )
select *, year( current_date()) as year,month( current_date()) as month,day( current_date()) as day
from temp_employee_pay_rate_dim_read;
