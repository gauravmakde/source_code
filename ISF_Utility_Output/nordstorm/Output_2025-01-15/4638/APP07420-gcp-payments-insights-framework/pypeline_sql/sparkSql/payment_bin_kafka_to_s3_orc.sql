--Reading Data from Source Kafka Topic
create temporary view temp_payments_bin_analytical_object as
select *
from kafka_customer_payment_bin_analytical_avro;

--Writing data to new S3 Path
insert into table payments_result_bin_analytical_object partition (
    year, month, day
)
select
    *,
    date_format(transactiondate, 'yyyy') as year,
    date_format(transactiondate, 'MM') as month,
    date_format(transactiondate, 'dd') as day
from temp_payments_bin_analytical_object;
