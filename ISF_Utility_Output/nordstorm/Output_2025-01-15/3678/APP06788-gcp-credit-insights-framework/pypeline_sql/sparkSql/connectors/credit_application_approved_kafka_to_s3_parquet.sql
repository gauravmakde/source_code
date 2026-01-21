-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app06788;
DAG_ID=credit_connectors_s3_parquet;
Task_Name=kafka_to_s3_parquet_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

--Reading Data from Source Kafka Topic
CREATE TEMPORARY VIEW temp_object_model AS
SELECT * FROM kafka_credit_application_approved;

CREATE TEMPORARY VIEW credit_application_approved_temp AS
SELECT
    temp_object_model.headers AS headers,
    struct(*) AS value,
    date_format(cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP), 'yyyy') AS year,
    date_format(cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP), 'MM') AS month,
    date_format(cast(cast(element_at(headers, 'EventTime') AS BIGINT) / 1e3 AS TIMESTAMP), 'dd') AS day
FROM temp_object_model;

--Writing to s3 parquet bucket
INSERT INTO TABLE credit_application_approved_parquet PARTITION (
    year, month, day
)
SELECT * FROM credit_application_approved_temp;
