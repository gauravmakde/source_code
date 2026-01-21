SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=pay_period_data_onetime_load_2656_napstore_insights;
Task_Name=s3_csv_pay_period_data_load_load_0_pay_period_stg_tables;'
FOR SESSION VOLATILE;

CREATE TEMPORARY VIEW pay_period_input
AS select * from pay_period_s3_input;


insert overwrite table pay_period_ldg
select
CURRENT_TIMESTAMP as last_updated,
cast(pay_period_input.START_DATE as DATE) as pay_period_start_date,
cast(pay_period_input.END_DATE as DATE) as pay_period_end_date,
cast(pay_period_input.LY_START_DATE as DATE) as pay_period_ly_start_date,
cast(pay_period_input.LY_END_DATE as DATE) as pay_period_ly_end_date,
pay_period_input.PACE_YEAR as pcstr_year_num,
pay_period_input.PACE_PERIOD_NUM as pcstr_period_num
from pay_period_input;