--Reading Data from Source Kafka Topic
create temporary view temp_kafka_customer_credit_account_analytical_avro
as
select *
from kafka_customer_credit_account_analytical_avro;

--Writing data to new S3 Path
insert into table credit_account_analytical_object partition (year, month, day)
select
    *,
    date_format(openedtime, 'yyyy') as year,
    date_format(openedtime, 'MM') as month,
    date_format(openedtime, 'dd') as day
from temp_kafka_customer_credit_account_analytical_avro;
