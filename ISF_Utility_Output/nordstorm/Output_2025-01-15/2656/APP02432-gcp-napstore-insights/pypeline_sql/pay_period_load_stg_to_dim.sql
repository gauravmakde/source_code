SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=pay_period_data_onetime_load_2656_napstore_insights;
Task_Name=s3_csv_pay_period_data_load_load_td_stg_to_dims;'
FOR SESSION VOLATILE;


ET;

DELETE FROM {db_env}_NAP_DIM.PAY_PERIOD_DIM;

ET;

INSERT INTO {db_env}_NAP_DIM.PAY_PERIOD_DIM (
    last_updated,
    pay_period_start_date,
    pay_period_end_date,
    pay_period_ly_start_date,
    pay_period_ly_end_date,
    pcstr_period_num,
    pcstr_year_num
    )
 SELECT last_updated,
        pay_period_start_date,
        pay_period_end_date,
        pay_period_ly_start_date,
        pay_period_ly_end_date,
        pcstr_period_num,
        pcstr_year_num
  FROM {db_env}_NAP_STG.PAY_PERIOD_LDG;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
