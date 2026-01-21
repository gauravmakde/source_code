-- mandatory Query Band part
SET QUERY_BAND = '
App_ID=app07420;
Task_Name=s3_avro_to_teradata_stg_job;'
-- noqa: disable=all
FOR SESSION VOLATILE;
-- noqa: enable=all

CREATE EXTERNAL TABLE IF NOT EXISTS {reset_schema_name}.payment_online_matched_avro
PARTITIONED BY (
    year int,
    month int,
    day int
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
's3://{env_s3_reset_bucket_name}/to_process/reset-online-matched-avro-data-files/'
TBLPROPERTIES (
    'avro.schema.url'
    = 's3://{env_s3_reset_bucket_name}/avro-schema/reset-online-matched-avro-data-files-avro/reset-online-matched-avro-data.avsc',
    'avro.compress' = 'SNAPPY'
);

CREATE EXTERNAL TABLE IF NOT EXISTS {reset_schema_name}.payment_reset_store_matched_avro
PARTITIONED BY (
    year int,
    month int,
    day int
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
's3://{env_s3_reset_bucket_name}/to_process/reset-store-matched-avro-data-files/'
TBLPROPERTIES (
    'avro.schema.url'
    = 's3://{env_s3_reset_bucket_name}/avro-schema/reset-store-matched-avro-data-files-avro/reset-store-matched-avro-data.avsc',
    'avro.compress' = 'SNAPPY'
);

MSCK REPAIR TABLE {reset_schema_name}.payment_online_matched_avro;
MSCK REPAIR TABLE {reset_schema_name}.payment_reset_store_matched_avro;

CREATE TEMPORARY VIEW temp_reset_matched AS
SELECT
    to_timestamp(processedtimestamp / 1e6) AS processed_timestamp,
    eventid AS event_id,
    settlementresultreceivedid AS settlement_result_received_id,
    originalglobaltransactionid AS original_global_transaction_id,
    globaltransactionid AS global_transaction_id,
    ertm_tenderitemsequence AS ertm_tender_item_sequence,
    settlementtransactionmatchcriteria AS settlement_transaction_match_criteria,
    tenderrefid AS tender_ref_id
FROM {reset_schema_name}.payment_online_matched_avro
UNION
SELECT
    to_timestamp(processedtimestamp / 1e6) AS processed_timestamp,
    eventid AS event_id,
    settlementresultreceivedid AS settlement_result_received_id,
    originalglobaltransactionid AS original_global_transaction_id,
    globaltransactionid AS global_transaction_id,
    ertm_tenderitemsequence AS ertm_tender_item_sequence,
    settlementtransactionmatchcriteria AS settlement_transaction_match_criteria,
    null AS tender_ref_id
FROM {reset_schema_name}.payment_reset_store_matched_avro;

--Drop redundant records
CREATE TEMPORARY VIEW temp_reset_matched_filtered AS
SELECT * FROM temp_reset_matched WHERE
    event_id IN (
        SELECT event_id FROM temp_reset_matched
        GROUP BY event_id HAVING count(*) > 1
    ) AND tender_ref_id IS NOT NULL
UNION ALL
SELECT * FROM temp_reset_matched WHERE event_id IN (
    SELECT event_id FROM temp_reset_matched
    GROUP BY event_id HAVING count(*) = 1
);

--Sink
INSERT INTO TABLE payment_settlement_transaction_match_ldg
SELECT * FROM temp_reset_matched_filtered;

DROP TABLE {reset_schema_name}.payment_online_matched_avro;
DROP TABLE {reset_schema_name}.payment_reset_store_matched_avro;
