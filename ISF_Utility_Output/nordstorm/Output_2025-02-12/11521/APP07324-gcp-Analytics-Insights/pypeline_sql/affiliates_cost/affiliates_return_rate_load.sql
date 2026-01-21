
-- Reading data from S3
create or replace temporary view temp_affiliates_rr_csv
        (
        banner                  string
        , country               string
        , start_day_date        date
        , end_day_date          date
        , return_rate           decimal(18,4)
        )
USING csv
OPTIONS (path "s3://analytics-insights-triggers/affiliates/Affiliate_return_rate.csv",
        sep ",",
        header "true",
        dateFormat "M/d/yyyy");

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert overwrite table affiliates_return_rate_ldg_output
select * from temp_affiliates_rr_csv
;

