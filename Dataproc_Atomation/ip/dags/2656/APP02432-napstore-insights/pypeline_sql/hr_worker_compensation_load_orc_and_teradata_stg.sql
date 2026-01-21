SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_worker_compensation_load_2656_napstore_insights;
Task_Name=hr_worker_compensation_load_orc_td_stg;'
FOR SESSION VOLATILE;

--Reading Data from  Source Kafka Topic Name=linear_worker_compensation_analytical_avro
create temporary view temp_object_model AS select * from kafka_linear_worker_compensation_analytical_avro;

-- Reading Data from out of order bucket:
CREATE TEMPORARY VIEW temp_object_model_out_of_order USING AVRO
OPTIONS (path "s3://{hr_worker_compensation_bucket_name}/outOfOrder/EmployeePayRateUpdated/");

-- Writing Kafka Data and Data from out of order S3 bucket to S3 ORC Path
INSERT INTO TABLE hr_worker_compensation_orc_output partition(year, month, day)
SELECT  lastUpdated,
        transactionId,
        employee.id,
        effectiveDate,
        basePay.value,
        compensationCurrencyCode,
        payRateType,
        updateType,
        current_date() AS process_date, year(current_date()) AS year,month(current_date()) AS month,day(current_date()) AS day
FROM temp_object_model
UNION ALL
SELECT  eventTime AS lastUpdated,
        transactionId, 
        employee.id, 
        effectiveDate, 
        basePay.value,
        compensationCurrencyCode,
        payRateType,
        updateType,
        current_date() AS process_date, year(current_date()) AS year,month(current_date()) AS month,day(current_date()) AS day
FROM temp_object_model_out_of_order WHERE year = year(current_date() - 1) AND month = month(current_date() - 1) AND day = day(current_date() - 1);

-- Writing Kafka Data to Semantic Layer 
insert overwrite table hr_worker_compensation_ldg
select  lastUpdated as last_updated,
        transactionId as transaction_id,
        employee.id as employee_id,
        effectiveDate as effective_date,
        basePay.value as base_pay_value,
        compensationCurrencyCode as compensation_currency_code,
        payRateType as pay_rate_type,
        updateType as update_type
from temp_object_model
UNION ALL
SELECT  eventTime AS last_updated,
        transactionId AS transaction_id, 
        employee.id AS employee_id, 
        effectiveDate AS effective_date, 
        basePay.value AS base_pay_value,
        compensationCurrencyCode as compensation_currency_code,
        payRateType AS pay_rate_type, 
        updateType AS update_type
FROM temp_object_model_out_of_order WHERE year = year(current_date() - 1) AND month = month(current_date() - 1) AND day = day(current_date() - 1);