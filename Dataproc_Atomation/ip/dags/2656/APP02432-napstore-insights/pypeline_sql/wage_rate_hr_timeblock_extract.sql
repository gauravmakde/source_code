SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=wage_rate_to_s3_extract_2656_napstore_insights;
Task_Name=wage_rate_extract_load_2_hr_timeblock;'
FOR SESSION VOLATILE;

--Reading Data from  Source Teradata
create temporary view  temp_hr_timeblock_fact_read AS
select reference_id, worker_number, total_hours, override_codes_store, override_codes_department
from hr_timeblock_fact;

--Writing S3 Data to S3
insert into table hr_timeblock_fact_s3_output   partition (year, month, day )
select *, year( current_date()) as year, month( current_date()) as month, day( current_date()) as day
from temp_hr_timeblock_fact_read;