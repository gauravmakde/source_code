--Reading Data from  Source Kafka Topic Name=cpp_object_model_avro
create temporary view  temp_cpp_object_model AS select * from kafka_cpp_object_model_avro ;


--Extracting Header from Kafka Table
-- create temporary view header_data_only as select element_at(headers,'EventType') as EventType,
-- element_at(headers,'AppId') as AppId,
-- element_at(headers,'SourceEventType') as SourceEventType,
-- element_at(headers,'LastUpdatedTime') as LastUpdatedTime
-- from temp_cpp_object_model limit 100;


--Extracting Selected Column Data from Kafka Table
-- create temporary view kafka_temp1 as select preference.id as id_col,preference.type as type_col,
-- preference.value.authority as authority ,'USA' as country,
-- element_at(headers,'CreationTime') as CreationTime
-- from temp_cpp_object_model limit 1000;


--https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-aux-cache-cache-table.html
--Running Spark SQL (Any Valid SPARK 3.0 SQL will work)
CACHE TABLE cache_tbl OPTIONS ('storageLevel' 'MEMORY_AND_DISK')  TABLE  temp_cpp_object_model;

/*Writing S3 Data to new S3 Path*/ 
insert into table s3_cpp_object_tbl_parquet_write   partition (year, month, day )
select *,year( current_date()) as year,month( current_date()) as month,day( current_date()) as day
from  temp_cpp_object_model;

-- NOTE: This example is initially set to write to an S3 bucket owned by nap-1hop team. Steps to update
-- to use your own S3 bucket:
--   1. Update variable __S3_NAP_WRITE_TEST__NONPROD path value to your s3 bucket.
--   2. Update that buckets policy using these instructions: https://nap.prod.dots.vip.nordstrom.com/1-hop/insights-framework/docs/02%20Getting%20Started/01%20Prerequisites/#add_or_update_policies_to_s3
--   3. If Airflow job is not picking up new path from the variable, force this sql file to deploy by making an update.  Then force the deployment of the DAG by updating the spark_s3_example.json file.
