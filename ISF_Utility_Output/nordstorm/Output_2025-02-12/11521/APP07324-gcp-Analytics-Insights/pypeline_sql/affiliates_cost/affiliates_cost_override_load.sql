
-- Reading data from S3
create or replace temporary view temp_affiliates_cost_override_csv
        (
        banner                  string
        , country               string
        , day_date              date
        , lf_cost               decimal(18,2)
        , total_cost            decimal(18,2)
        )
USING csv
OPTIONS (path "s3://analytics-insights-triggers/affiliates/affiliate_cost_override.csv",
        sep ",",
        header "true",
        dateFormat "M/d/yyyy");

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert overwrite table affiliates_cost_override_ldg_output
select * from temp_affiliates_cost_override_csv
;

