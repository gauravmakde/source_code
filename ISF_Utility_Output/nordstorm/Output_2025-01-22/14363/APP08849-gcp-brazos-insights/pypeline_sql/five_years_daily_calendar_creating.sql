SET QUERY_BAND = '
App_ID=APP08849;
DAG_ID=nap_to_allocation_454_calendar_extracts_daily;
Task_Name=extract_454_calendar;'
FOR SESSION VOLATILE;


create temporary view five_years_daily_calendar as
select
    date_format(to_date(c.day_date, 'MM/dd/yyyy'), 'yyyy-MM-dd') as day_date,
    c.week_idnt as week_idnt
from day_cal_454_dim c
where
    c.fiscal_year_num
        between
            (year(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')) - 2)
        and
            (year(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')) + 2)
order by c.day_date;


insert overwrite table daily_calendar
select * from five_years_daily_calendar ;

drop table if exists {hive_schema}.by_allocation_daily_calendar;

create table if not exists {hive_schema}.by_allocation_daily_calendar
(
    day_date date,
    week_idnt integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location 's3://{s3_bucket_by}/daily-calendar/daily_calendar/';
