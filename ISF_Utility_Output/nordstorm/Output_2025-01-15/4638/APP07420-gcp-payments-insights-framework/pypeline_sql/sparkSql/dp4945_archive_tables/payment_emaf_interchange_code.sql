--Reading Data from landing S3 bucket
create temporary table temp_emaf_interchange_code_csv using csv
options (
    path "s3://{dp495_s3_bucket}/csv/emaf_interchange_code/", header "true"
);

--Writing data to new S3 Path
insert overwrite table emaf_interchange_code
select distinct
    cast(interchange_code as int) as interchange_code,
    interchange_code_desc,
    brand,
    current_timestamp() as last_updated_time
from temp_emaf_interchange_code_csv;
