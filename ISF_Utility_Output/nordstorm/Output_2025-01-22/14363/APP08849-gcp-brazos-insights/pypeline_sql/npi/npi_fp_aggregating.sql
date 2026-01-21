
drop table if exists {hive_schema}.by_allocation_npi_partitioned_fp;


create table if not exists {hive_schema}.by_allocation_npi_partitioned_fp
(
    npi_month string,
    dept_num integer,
    class_num integer,
    sbclass_num integer,
    return_forecast_unit double,
    prediction_unit double,
    prediction_date string,
    week integer,
    business_unit_num integer
)
PARTITIONED BY (location_num integer)
STORED AS PARQUET
location 's3://{s3_bucket_by}/raw/npi_by_allocation/business_unit=fp/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');


MSCK REPAIR TABLE {hive_schema}.by_allocation_npi_partitioned_fp;


--- Writing Data to AWS S3
insert overwrite table npi_by_allocation_fp_result PARTITION (locations)
select
    b.location_num as location,
    b.npi_month,
    b.dept_num as dept,
    b.class_num as cls,
    b.sbclass_num as subclass,
    max(case b.week when 1 then b.return_forecast_unit else 0 end) as returns_week1,
    max(case b.week when 2 then b.return_forecast_unit else 0 end) as returns_week2,
    max(case b.week when 3 then b.return_forecast_unit else 0 end) as returns_week3,
    max(case b.week when 4 then b.return_forecast_unit else 0 end) as returns_week4,
    max(case b.week when 5 then b.return_forecast_unit else 0 end) as returns_week5,
    max(case b.week when 1 then b.prediction_unit else 0 end) as npi_week1,
    max(case b.week when 2 then b.prediction_unit else 0 end) as npi_week2,
    max(case b.week when 3 then b.prediction_unit else 0 end) as npi_week3,
    max(case b.week when 4 then b.prediction_unit else 0 end) as npi_week4,
    max(case b.week when 5 then b.prediction_unit else 0 end) as npi_week5,
    case
        when b.location_num between 1 and 100 then 100
        when b.location_num between 101 and 200 then 200
        when b.location_num between 201 and 300 then 300
        when b.location_num between 301 and 400 then 400
        when b.location_num between 401 and 500 then 500
        when b.location_num between 501 and 600 then 600
        when b.location_num between 601 and 700 then 700
        when b.location_num between 701 and 800 then 800
        when b.location_num between 801 and 900 then 900
        when b.location_num > 901 then 901
    end as locations
from {hive_schema}.by_allocation_npi_partitioned_fp b
group by b.location_num, b.npi_month, b.dept_num, b.class_num, b.sbclass_num;
