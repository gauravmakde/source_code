--Reading Data from landing S3 bucket
create temporary table temp_emaf_authorization_source_csv using csv
options (
    path "s3://{dp495_s3_bucket}/csv/emaf_authorization_source/", header "true"
);

--Writing data to new S3 Path
insert overwrite table emaf_authorization_source
select distinct
    authorization_source,
    authorization_source_desc,
    current_timestamp() as last_updated_time
from temp_emaf_authorization_source_csv;
