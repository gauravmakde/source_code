SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=ddl_empdisc_monthly_summary_11521_ACE_ENG;
     Task_Name=ddl_empdisc_monthly_summary;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_TRUST_EMP.empdisc_monthly_summary
Team/Owner: Data Science And Analytics - Digital and Fraud
Date Created/Modified: 4/2/2024

Note:
-- Purpose of the table: Employee discount summary by year, month, channel, and merchant
-- Update Cadence: Daily
*/
/*
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_TRUST_EMP', 'empdisc_monthly_summary', OUT_RETURN_MSG);
*/
create multiset table T2DL_DAS_TRUST_EMP.empdisc_monthly_summary
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    year_num integer NOT NULL
    ,month_num integer NOT NULL
    ,month_short_desc char(3) NOT NULL character set unicode not casespecific
    ,data_source_code varchar(10) NOT NULL character set unicode not casespecific
    ,merch_nonmerch_ind varchar(12) NOT NULL CHARACTER SET UNICODE NOT CASESPECIFIC
    ,ring_channel varchar(20) NOT NULL character set unicode not casespecific
    --total discount
    ,total_discount_amt decimal(12, 2)
    ,total_emp_count BIGINT
    ,total_tran_count BIGINT
    --invalid employee id
    ,invalid_emp_id_amt decimal(12, 2)
    ,invalid_emp_id_emp_count BIGINT
    ,invalid_emp_id_tran_count BIGINT
    --valid discount used by active or retire employee throughout a year
    ,valid_discount_pct_amt decimal(12, 2)
    ,valid_discount_pct_emp_count BIGINT
    ,valid_discount_pct_tran_count BIGINT
    --invalid discount used by active or retire employees, who use higher discount pct that what they are eligible for and outside special event period
    ,misuse_discount_pct_amt decimal(12, 2)
    ,misuse_discount_pct_emp_count BIGINT
    ,misuse_discount_pct_tran_count BIGINT
    --invalid discount used by terminated or ineligible employee, who are not eligible for getting discount
    ,ineligible_discount_amt decimal(12, 2)
    ,ineligible_discount_emp_count BIGINT
    ,ineligible_discount_tran_count BIGINT
    --unverified cases: discount_status is unknown
    ,unverified_discount_gt33_amt decimal(12, 2)
    ,unverified_discount_gt33_emp_count BIGINT
    ,unverified_discount_gt33_tran_count BIGINT
    --compute ineligible discount amount
    ,compute_ineligible_discount_amt decimal(12, 2)
    --load timestamp
    ,dw_sys_load_tmstp  timestamp(6) default current_timestamp(6) not null
    )
primary index(year_num, month_num, month_short_desc, data_source_code, merch_nonmerch_ind, ring_channel)
PARTITION BY RANGE_N(year_num BETWEEN 2021 AND 2034 EACH 1 ,
 NO RANGE);
;
COLLECT STATISTICS COLUMN (year_num),
                   COLUMN (month_num),
                   COLUMN (data_source_code),
                   COLUMN (merch_nonmerch_ind),
                   COLUMN (ring_channel)
on T2DL_DAS_TRUST_EMP.empdisc_monthly_summary;

-- Table Comment (STANDARD)
COMMENT ON T2DL_DAS_TRUST_EMP.empdisc_monthly_summary IS 'Employee discount summary by year, month, channel, and merchant';

SET QUERY_BAND = NONE FOR SESSION;
