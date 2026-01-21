--Reading Data from landing S3 bucket
create temporary table temp_emaf_original_interchange_ind_csv using csv
options (
    path "s3://{dp495_s3_bucket}/csv/emaf_original_interchange_ind/",
    header "true"
);

--Writing data to new S3 Path
insert overwrite table emaf_original_interchange_ind
select distinct
    original_interchange_ind,
    original_interchange_desc,
    current_timestamp() as last_updated_time
from temp_emaf_original_interchange_ind_csv;
