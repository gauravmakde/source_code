SET
    QUERY_BAND = '
App_ID=APP07780;
DAG_ID=anniversary_report_extract;
Task_Name=SPARK_READ_S3_WRITE_POSTGRES_ITEM_REPORT;' FOR SESSION VOLATILE;

--- Read the Item data which is in csv format from S3 into hive DB as a temporary view.
create temporary view temp_s3_item_report_data as
SELECT supplier                                                   as supplier_name,
       split(trim(department), ',')[0]                            as department_number,
       split(trim(department), ',')[1]                            as department_name,
       to_date(from_unixtime(unix_timestamp(day_dt, 'yy/MM/dd'))) as date_aligned,
       COALESCE(vpn, '')                                          as vpn,
       COALESCE(style_group_idnt, '')                             as style_group_num,
       COALESCE(style_desc, '')                                   as style_name,
       COALESCE(supp_color, '')                                   as supplier_color,
       COALESCE(rp_ind, '')                                       as rp,
       cast(sum(demand_dollars)     as DECIMAL(38, 2))            as demand,
       cast(sum(demand_units)       as NUMERIC)                   as demand_u,
       cast(sum(eoh_dollars)        as DECIMAL(38, 2))            as eoh,
       cast(sum(eoh_units)          as NUMERIC)                   as eoh_u,
       cast(sum(nonsellable_units)  as NUMERIC)                   as nonsellable_u,
       cast(sum(sales_dollars)      as DECIMAL(38, 2))            as sales,
       cast(sum(sales_units)        as NUMERIC)                   as sales_u,
       cast(sum(eoh_cost)           as DECIMAL(38, 2))            as eoh_c,
       cast(sum(sales_cost)         as DECIMAL(38, 2))            as sales_c,
       cast(sum(sales_pm)           as DECIMAL(38, 2))            as sales_pm
from s3_item_report
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
order by date_aligned;

insert overwrite table item_report
SELECT *
FROM temp_s3_item_report_data
WHERE temp_s3_item_report_data.supplier_name IS NOT NULL
  AND temp_s3_item_report_data.date_aligned IS NOT NULL;

