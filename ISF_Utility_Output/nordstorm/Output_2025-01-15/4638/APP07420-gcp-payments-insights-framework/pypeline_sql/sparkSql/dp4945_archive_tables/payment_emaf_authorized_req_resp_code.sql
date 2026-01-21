--Reading Data from landing S3 bucket
create temporary table temp_emaf_authorized_req_resp_code_csv using csv
options (
    path "s3://{dp495_s3_bucket}/csv/emaf_authorized_req_resp_code/",
    header "true"
);

--Writing data to new S3 Path
insert overwrite table emaf_authorized_req_resp_code
select distinct
    authorized_req_response_code,
    authorized_req_response_desc,
    current_timestamp() as last_updated_time
from temp_emaf_authorized_req_resp_code_csv;
