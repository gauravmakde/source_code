create table if not exists {hive_schema}.test_dag_example
(
    sellingChannel STRING,
    year int,
    month int,
    day int
)
using PARQUET
location 's3://{s3_bucket_root_var}/Isf_test/<lan_id>_dummy_sql_example/'
partitioned by (year, month, day);

select count(*) from {hive_schema}.test_dag_example;
