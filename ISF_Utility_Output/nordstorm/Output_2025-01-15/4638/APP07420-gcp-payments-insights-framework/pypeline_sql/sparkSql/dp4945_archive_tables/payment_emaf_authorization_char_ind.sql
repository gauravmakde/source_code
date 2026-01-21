--Reading Data from landing S3 bucket
create temporary table temp_emaf_authorization_char_ind_csv using csv
options (
    path "s3://{dp495_s3_bucket}/csv/emaf_authorization_char_ind/",
    header "true"
);

--Writing data to new S3 Path
insert overwrite table emaf_authorization_char_ind
select distinct
    authorization_char_ind,
    authorization_char_desc,
    current_timestamp() as last_updated_time
from temp_emaf_authorization_char_ind_csv;
