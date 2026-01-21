SET QUERY_BAND = '
App_ID=APP08849;
DAG_ID=npi_to_by_allocation;
Task_Name=process_npi_data_split;'
FOR SESSION VOLATILE;

set `mapreduce.input.fileinputformat.input.dir.recursive` = true;


drop table if exists {hive_schema}.by_allocation_two_years_calendar;


create table if not exists {hive_schema}.by_allocation_two_years_calendar
(
    npi_month string,
    day_date date,
    week integer
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location 's3://{s3_bucket_by}/calendar/calendar/';


drop table if exists {hive_schema}.by_allocation_npi_projection_version;


create table if not exists {hive_schema}.by_allocation_npi_projection_version
 (
     npi_projection_version_id integer,
     prediction_name string,
     version_active boolean,
     created_by string,
     create_date date,
     update_date date
 )
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 location 's3://{s3_bucket_by}/npi-versions/';


--- Read the store data
create temporary view store_dim_vw as
select * from store_dim;


--- Reading today NPI data
create temporary view by_allocation_npi_processed as
select
    cast(n.npi_projection_version_dim_id as int) as npi_projection_version_dim_id,
    case s.business_unit_num
        when '1000' then 'fp'
        when '2000' then 'op'
        when '5000' then 'op'
        else 'other' end as business_unit,
    n.location_num,
    c.npi_month,
    cast(n.dept_num as int) as dept_num,
    cast(n.class_num as int) as class_num,
    cast(n.sbclass_num as int) as sbclass_num,
    cast(n.return_forecast_unit as double) as return_forecast_unit,
    cast(n.prediction_unit as double) as prediction_unit,
    n.prediction_date,
    c.week,
    s.business_unit_num
from npi_source_today n
    join {hive_schema}.by_allocation_two_years_calendar c on cast(n.prediction_date as date) = c.day_date
    join store_dim_vw s on n.location_num = s.store_num;


--- Writing Partitioned Data to AWS S3
insert overwrite table npi_by_allocation PARTITION (business_unit, location_num)
select b.* from by_allocation_npi_processed b
join {hive_schema}.by_allocation_npi_projection_version v on b.npi_projection_version_dim_id = v.npi_projection_version_id
where v.version_active = true;
