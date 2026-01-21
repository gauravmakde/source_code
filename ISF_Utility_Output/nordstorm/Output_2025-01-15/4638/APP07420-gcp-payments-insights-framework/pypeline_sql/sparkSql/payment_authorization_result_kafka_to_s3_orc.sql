--Reading Data from Source Kafka Topic
CREATE
TEMPORARY VIEW temp_object_model AS
SELECT *
FROM kafka_customer_payment_authorization_result_analytical_avro;

--Writing data to new S3 Path
INSERT INTO TABLE payments_authorization_analytical_object PARTITION (
    year, month, day
)
SELECT
    *,
    date_format(transactiontimestamp.timestamp, 'yyyy') AS year,
    date_format(transactiontimestamp.timestamp, 'MM') AS month,
    date_format(transactiontimestamp.timestamp, 'dd') AS day
FROM temp_object_model;
