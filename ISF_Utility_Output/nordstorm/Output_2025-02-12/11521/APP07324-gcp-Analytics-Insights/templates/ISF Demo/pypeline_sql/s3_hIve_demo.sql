---Reading S3 Data from S3 path
CREATE TEMPORARY VIEW online_order USING orc OPTIONS (path "s3://tf-nap-prod-acp-presentation/event/customer-activity-ordersubmitted-v1/"); 

---create output view
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

-- create hive table
create table if not exists {hive_schema}.online_orders
(
    order_number string,
    user_id string,
    country string,
    channel string,
    platform string,
    year int,
    month int,
    day int
)
using PARQUET
location 's3://{s3_bucket_root_var}/online_orders'
partitioned by (year, month, day);

-- insert overwrite for dynamic partitioning
-- this ensures that any data for the YMD partition gets overwritten with new data

INSERT OVERWRITE TABLE {hive_schema}.online_orders partition (year, month, day)
select * from online_order_view;
