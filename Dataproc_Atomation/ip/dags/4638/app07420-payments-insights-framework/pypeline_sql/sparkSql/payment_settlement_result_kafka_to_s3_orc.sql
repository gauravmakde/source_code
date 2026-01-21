--Reading Data from Source Kafka Topic
create temporary view temp_payment_settlement_result_analytical_object as select *
from kafka_payment_settlement_result_analytical_avro;

--Writing data to new S3 Path
insert into table payments_settlement_analytical_object partition (
    year, month, day
)
select
    *,
    date_format(transactiontimestamp.timestamp, 'yyyy') as year,
    date_format(transactiontimestamp.timestamp, 'MM') as month,
    date_format(transactiontimestamp.timestamp, 'dd') as day
from temp_payment_settlement_result_analytical_object;
