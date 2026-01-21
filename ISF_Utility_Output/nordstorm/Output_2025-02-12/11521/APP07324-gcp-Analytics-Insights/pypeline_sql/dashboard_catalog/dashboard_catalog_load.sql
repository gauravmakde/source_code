SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=dashboard_catalog_11521_ACE_ENG;
     Task_Name=dashboard_catalog_load;'
     FOR SESSION VOLATILE;


-- Reading data from S3
create or replace temporary view dashboard_catalog_csv 
        (
        is_featured	string
        , has_rms	string
        , analyst	string
        , asset_type	string
        , business_area	string
        , subject_area	string
        , dashboard_name string
        , description	string
        , data_source	string
        , database string
        , update_frequency string
        , dashboard_url	string
        )
USING csv 
OPTIONS (path "s3://analytics-insights-triggers/as_dashboard_catalog.csv",
        sep ",",
        header "true"); 

-- Writing output to teradata landing table.  
-- This should match the "sql_table_reference" indicated on the .json file.
insert overwrite table dashboard_catalog_ldg_output 
select * from dashboard_catalog_csv
;
