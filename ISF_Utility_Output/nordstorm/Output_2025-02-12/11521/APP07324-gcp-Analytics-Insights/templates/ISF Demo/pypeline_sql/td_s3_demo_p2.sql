-- write output table
create or replace temporary view temp_td_output as
select 
acp_id,
channel,
shipped_sales,
shipped_qty,
return_amt,
return_qty,
s3_year as year,
s3_month as month,
s3_day as day
from test_return_isf;
--write output to S3
insert into table s3_td_spark_write partition (year, month, day) select * from temp_td_output;
