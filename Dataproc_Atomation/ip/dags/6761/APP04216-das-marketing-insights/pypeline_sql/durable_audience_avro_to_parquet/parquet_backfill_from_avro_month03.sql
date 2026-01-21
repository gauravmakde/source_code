-- create path to s3 avro:
create temporary view avro_src using AVRO 
OPTIONS(
  path 's3://tf-nap-prod-mkt-objectmodel-landing/event_consumer/customer-audience-customer-segmentation-analytical-avro/year=2023/month=03/'
);


-- sink to s3 parquet:
insert into table prq_tbl
partition(year, month, day, hour)
select
  headers,
  value,
  date_format(value.lasttriggeringeventtime, 'yyyy') AS year,
  date_format(value.lasttriggeringeventtime, 'MM') AS month,
  date_format(value.lasttriggeringeventtime, 'dd') AS day,
  date_format(value.lasttriggeringeventtime, 'HH') AS hour
from avro_src;
