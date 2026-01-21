--Source
--Reading Data from S3 bucket
create temporary view temp_clienteling_started AS select * from kafka_clienteling_started_avro;

--Transform Logic
--Extracting Required Column (Cache in previous)
-- create employee commission exploded
-- note: no "value" field in nonprod data from proton sink, remove after testing
create temporary view s3_clienteling_started_extract_columns AS
 select
  cast(cast(temp_clienteling_started.eventtime as bigint) as string) as eventtime,
  temp_clienteling_started.customer.id as customer_id,
  temp_clienteling_started.customer.idType as customer_id_type,
  temp_clienteling_started.employee.id as employee_id,
  temp_clienteling_started.employee.idType as employee_id_type
from temp_clienteling_started;

--Sink
---Writing Data to Teradata using SQL API CODE
insert overwrite table write_teradata_clienteling_started_tbl select distinct *  from s3_clienteling_started_extract_columns;