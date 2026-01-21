SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=sales_person_video_pdp_view_11521_ACE_ENG;
     Task_Name=pdp_view_load;'
     FOR SESSION VOLATILE;

-- Stakeholders: Elianna Wang, Niharika Srivastava
-- Edit Date: 2023-02-09

-- Define new Hive table for output
create table if not exists {hive_schema}.spv_pdp_view_table
(
	shopper_id                     string,
	web_style_num                  string,
	view_page_country              string,
	view_page_channel              string,
	view_page_platform             string,
	view_page_year                 integer,
	view_page_month                integer,
	view_page_day                  integer,
	view_page_date                 date,
	view_pdp_count                 integer,
	min_view_pdp_time              timestamp,
	max_view_pdp_time              timestamp
)
using orc
location 's3://{s3_bucket_root_var}/spv/pdp_view/'
partitioned by (view_page_date);

-- History of PDP with SPV
create or replace temp view product_with_spv as (
    select
	    digitalcontent.id as web_style_num,
		year as view_page_year,
		month as view_page_month,
	    day as view_page_day
	from acp_event_view.customer_activity_impressed
	lateral view explode(elements) as element
	lateral view explode(context.digitalcontents) as digitalcontent
	where element.id = 'ProductDetail/SalesPersonVideo'
	and digitalcontent.idtype = 'STYLE'
	and digitalcontent.id is not null
	and digitalcontent.id <> ''
	and date(from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) >= date '2022-02-01'
	and to_date(concat(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')),'yyyy-MM-dd') between {start_date} and {end_date}
    group by 1, 2, 3, 4
);


-- Customers view PDP with SPV records on daily level

create or replace temp view pdp_with_spv_view as (
    select
        shopper_id,
		cai.web_style_num,
		view_page_country,
		view_page_channel,
		view_page_platform,
		cai.view_page_year,
		cai.view_page_month,
		cai.view_page_day,
		count(distinct from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) as view_pdp_count,
		min(from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) as min_view_pdp_time,
		max(from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) as max_view_pdp_time,
		to_date(concat(cai.view_page_year, '-', LPAD(cai.view_page_month, 2, '0'), '-', LPAD(cai.view_page_day, 2, '0')),'yyyy-MM-dd') as view_page_date
	from 
        (
        select
            customer.id as shopper_id,
            digitalcontent.id as web_style_num,
        	source.channelcountry as view_page_country,
        	source.channel as view_page_channel,
        	source.platform as view_page_platform,
        	year as view_page_year,
        	month as view_page_month,
        	day as view_page_day,
        	headers
        from acp_event_view.customer_activity_impressed
        lateral view explode(context.digitalcontents) as digitalcontent
        where customer.idtype = 'SHOPPER_ID'
        and digitalcontent.idtype = 'STYLE'
	    and digitalcontent.id <> ''
	    and to_date(concat(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')),'yyyy-MM-dd') between {start_date} and {end_date}
        ) as cai
	right join product_with_spv hpws
	on cai.web_style_num = hpws.web_style_num
	and cai.view_page_year = hpws.view_page_year
	and cai.view_page_month = hpws.view_page_month
	and cai.view_page_day = hpws.view_page_day
	where trim(shopper_id) <> ''
	and to_date(concat(cai.view_page_year, '-', LPAD(cai.view_page_month, 2, '0'), '-', LPAD(cai.view_page_day, 2, '0')),'yyyy-MM-dd') between {start_date} and {end_date}
    and arrays_overlap(map_keys(headers), array('Nord-Load','nord-load','nord-test','Nord-Test', 'Sretest', 'sretest')) = False
    and (
        array_contains(map_keys(headers), 'identified-bot') = False
        or
        (array_contains(map_keys(headers), 'identified-bot') = True  and decode(headers['identified-bot'], 'utf-8') = False)
        )
	group by 1, 2, 3, 4, 5, 6, 7, 8, 12
);

-- Writing output to Hive table
insert overwrite table {hive_schema}.spv_pdp_view_table partition (view_page_date)
select /*+ REPARTITION(100) */ * 
from pdp_with_spv_view
;

-- Sync partitions on hive table
MSCK REPAIR TABLE {hive_schema}.spv_pdp_view_table;

-- Write output for to teradata landing table
insert into table pdp_view_landing_table
select 
	shopper_id,
	web_style_num,
	view_page_country,
	view_page_channel,
	view_page_platform,
	view_page_year,
	view_page_month,
	view_page_day,
	view_page_date,
	view_pdp_count,
	min_view_pdp_time,
	max_view_pdp_time
from {hive_schema}.spv_pdp_view_table
where view_page_date between {start_date} and {end_date}
; 
