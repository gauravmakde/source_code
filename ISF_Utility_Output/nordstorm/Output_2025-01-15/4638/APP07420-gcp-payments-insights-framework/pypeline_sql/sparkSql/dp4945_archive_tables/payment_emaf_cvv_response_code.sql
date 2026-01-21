--Reading Data from landing S3 bucket
create temporary table temp_emaf_cvv_response_code_csv using csv
options (
    path "s3://{dp495_s3_bucket}/csv/emaf_cvv_response_code/", header "true"
);

--Writing data to new S3 Path
insert overwrite table emaf_cvv_response_code
select distinct
    cvv_response_code_ind,
    cvv_response_code_desc,
    current_timestamp() as last_updated_time
from temp_emaf_cvv_response_code_csv;
