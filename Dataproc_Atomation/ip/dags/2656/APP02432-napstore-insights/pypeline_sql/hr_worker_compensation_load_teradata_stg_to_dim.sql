SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=hr_worker_compensation_load_2656_napstore_insights;
Task_Name=hr_worker_compensation_load_td_stg_to_dims;'
FOR SESSION VOLATILE;

ET;

-- Create temporary table
CREATE VOLATILE MULTISET TABLE EMPLOYEE_PAY_RATE_TEMP AS (
    SELECT DISTINCT * FROM {db_env}_NAP_HR_WAGE_RATE_STG.EMPLOYEE_PAY_RATE_LDG qualify row_number() over (partition by transaction_id, update_type ORDER BY LAST_UPDATED DESC) = 1
) WITH DATA PRIMARY INDEX(transaction_id) ON COMMIT PRESERVE ROWS;
ET;

-- Merge and update if employee_id and last_updated combination exists.
MERGE INTO {db_env}_NAP_HR_WAGE_RATE_BASE_VWS.EMPLOYEE_PAY_RATE_DIM tgt
USING  EMPLOYEE_PAY_RATE_TEMP src
	ON (src.transaction_id = tgt.transaction_id and 
        src.update_type = tgt.update_type)
WHEN MATCHED THEN
UPDATE
SET
 last_updated = src.last_updated,
 employee_id = src.employee_id,
 effective_date = src.effective_date,
 base_pay_value = src.base_pay_value,
 compensation_currency_code = src.compensation_currency_code,
 pay_rate_type =  src.pay_rate_type,
 dw_sys_updt_tmstp = CURRENT_TIMESTAMP(0)
WHEN NOT MATCHED THEN
INSERT (
    last_updated,
    transaction_id,
    employee_id,
    effective_date,
    base_pay_value,
    compensation_currency_code,
    pay_rate_type,
    update_type,
    dw_batch_date,
    dw_sys_load_tmstp,
    dw_sys_updt_tmstp
)
VALUES (
    src.last_updated,
    src.transaction_id,
    src.employee_id,
    src.effective_date,
    src.base_pay_value,
    src.compensation_currency_code,
    src.pay_rate_type,
    src.update_type,
    CURRENT_DATE,
    CURRENT_TIMESTAMP(0),
    CURRENT_TIMESTAMP(0)
);
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
