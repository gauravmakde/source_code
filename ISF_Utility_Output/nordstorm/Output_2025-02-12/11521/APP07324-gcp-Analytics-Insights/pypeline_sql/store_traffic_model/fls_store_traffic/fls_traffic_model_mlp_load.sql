SET QUERY_BAND = 'App_ID=APP08150;
     DAG_ID=fls_store_traffic_11521_ACE_ENG;
     Task_Name=fls_traffic_model_mlp_load;'
     FOR SESSION VOLATILE;

-- Reading data from S3
create or replace temporary view temp_fls_traffic_model_mlp_csv (
    store_number integer
    , day_date date
    , thanksgiving integer
    , christmas integer
    , easter integer
    , estimated_traffic decimal(12,0)
    , estimate_tmstp integer
    , model_version string
)
USING csv OPTIONS (
    path "s3://store-traffic/napbi_store_traffic/fls_model_v2_output.csv",
    sep ",",
    header "true"
);

create or replace temporary view temp_fls_traffic_model_mlp_csv_2 as
select store_number
     , day_date
     , thanksgiving
     , christmas
     , easter
     , estimated_traffic
     , estimate_tmstp
     , model_version
from temp_fls_traffic_model_mlp_csv
where day_date between {start_date} and {end_date};

-- Writing output to teradata landing table.
-- This should match the "sql_table_reference" indicated on the .json file.
insert into table fls_traffic_model_mlp_ldg_output
select *
from temp_fls_traffic_model_mlp_csv_2;