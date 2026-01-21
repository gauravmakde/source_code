--Reading Data from  Source S3
create temporary view  temp_s3_cpp_object_tbl_parquet_read AS select * from s3_cpp_object_tbl_parquet_read ;

--map s3 schema to teradata schema
create temporary view s3_temp_view as
select id as id_col,
type as type_col,
authority as authority,
country as country
from temp_s3_cpp_object_tbl_parquet_read limit 1000;

--Write to teradata table teradata_cpp_object_tbl [connection string created in CICD]
insert overwrite table teradata_cpp_object_tbl select distinct * from s3_temp_view ;
