select * from {deg_t2_schema}.customer_cohort
where execution_qtr_end_dt>= {end_date}
and execution_qtr_start_dt <={end_date};         