--Reading Data from landing S3 bucket
create temporary table temp_emaf_pos_entry_mode_csv using csv
options (path "s3://{dp495_s3_bucket}/csv/emaf_pos_entry_mode/", header "true");

--Writing data to new S3 Path
insert overwrite table emaf_pos_entry_mode
select distinct
    pos_entry_mode,
    pos_entry_mode_desc,
    current_timestamp() as last_updated_time
from temp_emaf_pos_entry_mode_csv;
