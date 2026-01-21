---Reading S3 Data from S3 path
CREATE TEMPORARY VIEW online_order USING orc OPTIONS (path "s3://tf-nap-prod-acp-presentation/event/customer-activity-ordersubmitted-v1/"); 
---create output table
create temporary view online_order_view AS
select *
from
(
select
        CAST(invoice.orderNumber as string) as order_number,
        CAST(customer.id as string) as user_id,
        CAST(source.channelcountry as string) as country,
        CAST(source.channel as string) as channel,
        CAST(source.platform as string) as platform
 from online_order o
where o.headers['Sretest'] is null
  and o.headers['sretest'] is null
  and o.headers['Nord-Load'] is null
  and o.headers['nord-load'] is null
  and o.headers['nord-test'] is null
  and o.headers['Nord-Test'] is null
  and to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') = current_date
) 
where user_id is not null
limit 100;
---Writing output to new S3 Path
insert into table demo_orc_write partition (year, month, day)
select *, 
       year(current_date()) as year,
       month(current_date()) as month,
       day(current_date()) as day
  from online_order_view;
