CREATE OR REPLACE TEMPORARY VIEW csv_test USING CSV 
OPTIONS(path "s3://{s3_bucket_root_var}/test2.csv",
  sep  ",",
  inferSchema  "true",
  header "true");


--- CSV Data Example
-- a,date_field,year_field
-- 1,2022-08-01,2022
-- 2,2022-08-02,2022
-- 3,2022-08-03,2022
-- 4,2022-08-04,2022

create or replace temp view csv_slim as 
select * from csv_test where date_field between {start_date} and {end_date} and year_field = {year};

--- In Airflow this would resolve to:
-- create or replace temp view csv_slim as 
-- select * from csv_test where date_field between current_date()-1 and current_date() and year_field = 2022;