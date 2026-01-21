--Reading Data from landing S3 bucket
create temporary table temp_emaf_transaction_type_code_csv using csv
options (
    path "s3://{dp495_s3_bucket}/csv/emaf_transaction_type_code/", header "true"
);

--Writing data to new S3 Path
insert overwrite table emaf_transaction_type_code
select distinct
    transaction_type_code,
    transaction_type_desc,
    current_timestamp() as last_updated_time
from temp_emaf_transaction_type_code_csv;
