
-- Reading data from S3
create or replace temporary view temp_affiliates_campaign_csv
        (
        banner                  string
        , country               string
        , encrypted_id          string
        , publisher             string
        , start_day_date        date
        , end_day_date          date
        , mf_total_spend        decimal(18,2)
        , lf_commission         decimal(18,2)
        , lf_paid_placement     decimal(18,2)
        )
USING csv
OPTIONS (path "s3://analytics-insights-triggers/affiliates/Affiliate_MF_Reporting.csv",
        sep ",",
        header "true",
        dateFormat "M/d/yyyy");

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert overwrite table affiliates_campaign_cost_ldg_output
select * from temp_affiliates_campaign_csv
;
