CREATE TEMPORARY VIEW merged_data AS SELECT * FROM s3_merged_data;

INSERT OVERWRITE parquet SELECT * FROM merged_data;
