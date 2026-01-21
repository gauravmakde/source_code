-- Read Kafka into temp table
CREATE TEMPORARY VIEW temp_object_model AS
SELECT *
FROM kafka_payment_result_bin_v2_analytical_avro;

-- Transform in temp view
CREATE TEMPORARY VIEW temp_kafka_payment_result_bin_v2_analytical_avro AS
SELECT
    sourceid AS source_id,
    bin AS bin,
    eventsource AS event_source,
    createdtime AS created_time
FROM temp_object_model;

-- Sink to Teradata
INSERT INTO TABLE payment_result_bin_v2_ldg
SELECT
    source_id,
    bin,
    event_source,
    CAST(created_time AS STRING)
FROM temp_kafka_payment_result_bin_v2_analytical_avro;
