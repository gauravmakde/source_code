SET QUERY_BAND = 'App_ID=app07324;
     DAG_ID=test_nsk_migration_11521_ACE_ENG;
     Task_Name=check_spark_td;'
     FOR SESSION VOLATILE;

create or replace temporary view ae_test_tpt_td_csv 
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

-- Writing output to CSV
-- This should match the "sql_table_reference" indicated on the .json file.
insert overwrite table ae_test_tpt_td_ldg_output 
select * from ae_test_tpt_td_csv
;
