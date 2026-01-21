SET QUERY_BAND = 'App_ID=APP02602;
     DAG_ID=empdisc_fiscal_month_summary_11521_ACE_ENG;
     Task_Name=empdisc_fiscal_month_summary;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_TRUST_EMP.empdisc_fiscal_month_summary{t2_test}
Team/Owner: Rujira Achawanantakun (rujira.achawanantakun@nordstrom.com)
Date Created/Modified: 4/19/2024

Note:
-- Purpose of the table: aggregate discount by fiscal month
-- Update Cadence: Daily
*/
-- fiscal calendar: first day of the month given a date
create multiset volatile table fiscal_first_day as (
    select
    -- First day of the month
    min(cal1.day_date) first_day
    -- two digits year
    ,cal1.year_num year_num
    -- two digits month
    ,CAST(SUBSTRING(cal1.month_num, LENGTH(cal1.month_num) - 1, 2) AS INT) month_num
    -- six digits year and month, year_num
    ,cal1.month_num year_month_num
    from prd_nap_usr_vws.day_cal cal1
    right join (select month_num
                from prd_nap_usr_vws.day_cal
                where day_date = {execute_date}
          ) cal2
          on cal1.month_num = cal2.month_num
    group by 2,3,4
) with data on commit preserve rows
;

-- create a table to store queried data
create multiset volatile table data as (
  SELECT
    cal.month_num year_month_num --YYYYMM
    ,cal.year_num  year_num
    ,SUBSTRING(CAST(cal.month_num AS VARCHAR(6)), LENGTH(CAST(cal.month_num AS VARCHAR(6))) -1) month_num
    ,e.data_source_code
    ,e.line_item_merch_nonmerch_ind as merch_nonmerch_ind
    ,e.ring_channel
    --total discount
    ,sum(e.employee_discount_usd_amt) total_discount_amt
    ,count(distinct e.emp_number) total_emp_count
    ,count(distinct e.global_tran_id) total_tran_count
    --invalid employee id
    ,sum(CASE WHEN e.e_worker_number IS NULL THEN e.employee_discount_usd_amt ELSE 0 END) invalid_emp_id_amt
    ,count(distinct CASE WHEN e.e_worker_number IS NULL THEN e.emp_number ELSE NULL END) invalid_emp_id_emp_count
    ,count(distinct CASE WHEN e.e_worker_number IS NULL THEN e.global_tran_id ELSE NULL END) invalid_emp_id_tran_count
    --valid discount used by active or retire employee throughout a year
    ,sum(CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'ACTIVE' or e.e_discount_status = 'RETIREE')
        AND (e.use_disc_pct <= e.e_discount_percent
            OR e.scaled_event_name IS NOT NULL --item is on sale
            OR e.prod_event_num IS NOT NULL) --special event
        THEN e.employee_discount_usd_amt ELSE 0 END) valid_discount_pct_amt
    ,count(distinct CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'ACTIVE' or e.e_discount_status = 'RETIREE')
        AND (e.use_disc_pct <= e.e_discount_percent
            OR  e.scaled_event_name IS NOT NULL --item is on sale
            OR e.prod_event_num IS NOT NULL) --special event
        THEN e.emp_number ELSE NULL END) valid_discount_pct_emp_count
    ,count(distinct CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'ACTIVE' or e.e_discount_status = 'RETIREE')
        AND (e.use_disc_pct <= e.e_discount_percent
            OR  e.scaled_event_name IS NOT NULL --item is on sale
            OR e.prod_event_num IS NOT NULL) --special event
        THEN e.global_tran_id ELSE NULL END) valid_discount_pct_tran_count
    --invalid discount used by active or retire employees, who use higher discount pct that what they are eligible for and outside special event period
    ,sum(CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'ACTIVE' or e.e_discount_status = 'RETIREE')
        AND e.use_disc_pct > e.e_discount_percent
        AND e.scaled_event_name IS NULL
        AND e.prod_event_num IS NULL
        THEN e.employee_discount_usd_amt ELSE 0 END) misuse_discount_pct_amt
    ,count(distinct CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'ACTIVE' or e.e_discount_status = 'RETIREE')
        AND e.use_disc_pct > e.e_discount_percent
        AND e.scaled_event_name IS NULL --item is not on sale
        AND e.prod_event_num IS NULL --not buy during any special event
        THEN e.emp_number ELSE NULL END) misuse_discount_pct_emp_count
    ,count(distinct CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'ACTIVE' or e.e_discount_status = 'RETIREE')
        AND e.use_disc_pct > e.e_discount_percent
        AND e.scaled_event_name IS NULL --item is not on sale
        AND e.prod_event_num IS NULL --not buy during any special event
        THEN e.global_tran_id ELSE NULL END) misuse_discount_pct_tran_count
    --invalid discount used by terminated or ineligible employee, who are not eligible for getting discount
    ,sum(CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'TERMINATED' or e.e_discount_status = 'INELIGIBLE')
        THEN e.employee_discount_usd_amt ELSE 0 END) ineligible_discount_amt
    ,count(distinct CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'TERMINATED' or e.e_discount_status = 'INELIGIBLE')
        THEN e.emp_number ELSE NULL END) ineligible_discount_emp_count
    ,count(distinct CASE WHEN e.e_worker_number IS NOT NULL
        AND (e.e_discount_status = 'TERMINATED' or e.e_discount_status = 'INELIGIBLE')
        THEN e.global_tran_id ELSE NULL END) ineligible_discount_tran_count
    --unverified cases: discount_status is unknown
    ,sum(CASE WHEN e.e_worker_number IS NOT NULL
        AND e.e_discount_status IS NULL
        AND e.use_disc_pct > 33
        AND e.scaled_event_name IS NULL --item is not on sale
        AND e.prod_event_num IS NULL --not buy during any special event
        THEN e.employee_discount_usd_amt ELSE 0 END) unverified_discount_gt33_amt
    ,count(distinct CASE WHEN e.e_worker_number IS NOT NULL
        AND e.e_discount_status IS NULL
        AND e.use_disc_pct > 33
        AND e.scaled_event_name IS NULL --item is not on sale
        AND e.prod_event_num IS NULL --not buy during any special event
        THEN e.emp_number ELSE NULL END) unverified_discount_gt33_emp_count
    ,count(distinct CASE WHEN e.e_worker_number IS NOT NULL
        AND e.e_discount_status IS NULL
        AND e.use_disc_pct > 33
        AND e.scaled_event_name IS NULL --item is not on sale
        AND e.prod_event_num IS NULL --not buy during any special event
        THEN e.global_tran_id ELSE NULL END) unverified_discount_gt33_tran_count
    --compute ineligible discount amount
    ,(total_discount_amt - valid_discount_pct_amt) compute_ineligible_discount_amt
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
  FROM T2DL_DAS_TRUST_EMP.empdisc_pct_item_trans{t2_test} e
    left join prd_nap_usr_vws.day_cal cal
        on e.business_day_date = cal.day_date
  WHERE e.business_day_date BETWEEN (SELECT first_day from fiscal_first_day) AND  {execute_date}
    and cal.day_date BETWEEN (SELECT first_day from fiscal_first_day) AND  {execute_date}
  GROUP BY 1,2,3,4,5,6
) with data primary index(year_month_num, year_num, month_num, data_source_code, merch_nonmerch_ind, ring_channel) on commit preserve rows;
;

--delete the latest record in the table before inserting the update data into the table
DELETE FROM T2DL_DAS_TRUST_EMP.empdisc_fiscal_month_summary{t2_test}
WHERE year_month_num = (SELECT year_month_num from fiscal_first_day)
;

-- insert the data into the table
INSERT INTO T2DL_DAS_TRUST_EMP.empdisc_fiscal_month_summary{t2_test}
SELECT *
FROM data
;

COLLECT STATISTICS  COLUMN (year_month_num),
                    COLUMN (year_num),
                    COLUMN (month_num),
                    COLUMN (data_source_code),
                    COLUMN (merch_nonmerch_ind),
                    COLUMN (ring_channel)
on T2DL_DAS_TRUST_EMP.empdisc_fiscal_month_summary{t2_test};

/*
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
