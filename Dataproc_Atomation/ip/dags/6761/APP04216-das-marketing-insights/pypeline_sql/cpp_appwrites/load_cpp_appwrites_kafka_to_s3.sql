-- Read kafka source data with exploded application writes
CREATE TEMPORARY VIEW exploded_kafka_source AS
SELECT
    preference.id AS enterprise_id,
    explode_outer(map_values(preference.applicationWrites)) applicationWrites,
    decode(headers.SystemTime,'UTF-8') AS SystemTime
FROM kafka_source;

-- prepare final table
CREATE TEMPORARY VIEW prepared_final_result_table AS
SELECT
    enterprise_id,
    applicationWrites.customerDataSourceDetails.customerDataSource AS customerDataSource,
    applicationWrites.customerDataSourceDetails.customerDataSubSource AS customerDataSubSource,
    applicationWrites.sourceUpdateDate AS sourceUpdateDate,
    applicationWrites.databaseUpdateDate AS databaseUpdateDate,
    SystemTime
FROM exploded_kafka_source;

-- write result table
INSERT OVERWRITE cpp_appwrites_data_extract
SELECT * FROM prepared_final_result_table;

-- Audit table
INSERT OVERWRITE cpp_appwrites_final_count
SELECT COUNT(*) FROM prepared_final_result_table;
