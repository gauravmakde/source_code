create or replace temporary view data_view 
        (
    event_id            string
    ,event_name         string
    ,featured_flag      integer
    ,private_sale_flag  integer
    ,unique_brands      integer
    ,branded_flag       string
    ,brand_name         string
    ,event_start        date
    ,event_end          date
    ,dw_sys_load_tmstp          timestamp
        )
USING csv 
OPTIONS (path "s3://{s3_bucket_root_var}/tpt_export/flash_event_output_td_to_s3.csv",
        sep ",",
        header "false",
        dateFormat "yy/M/d",
        escape ""); 


-- DROP TABLE IF EXISTS {hive_schema}.flash_event_output; 
create table if not exists {hive_schema}.flash_event_output
(
    event_id            string
    ,event_name         string
    ,featured_flag      integer
    ,private_sale_flag  integer
    ,unique_brands      integer
    ,branded_flag       string
    ,brand_name         string
    ,event_start        date
    ,event_end          date
    ,dw_sys_load_tmstp          timestamp
        )
USING PARQUET
location 's3://{s3_bucket_root_var}/flash_event_output'
;


--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
-- msck repair table {hive_schema}.flash_event_output;


insert overwrite table {hive_schema}.flash_event_output
select
    event_id 
    ,event_name        
    ,featured_flag 
    ,private_sale_flag 
    ,unique_brands      
    ,branded_flag       
    ,brand_name  
    ,event_start        
    ,event_end     
    ,dw_sys_load_tmstp
from data_view;


