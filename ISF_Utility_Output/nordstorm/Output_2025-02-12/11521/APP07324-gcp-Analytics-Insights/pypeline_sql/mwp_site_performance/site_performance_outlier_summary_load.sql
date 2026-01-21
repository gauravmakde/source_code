SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=site_performance_outlier_summary_11521_ACE_ENG;
     Task_Name=site_performance_outlier_summary_load;'
     FOR SESSION VOLATILE;

-- Reading data from upstream Hive table
create or replace temp view site_perf_outlier_summary as
select  
	to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') as eventtime,
	navigationtype,
	useragent,
	experience,
	bucketingid,
	case when pagetype = 'HOME' then 'HOME'         
		when pagetype in ('BRAND_RESULTS', 'CATEGORY_RESULTS', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS', 'SEARCH_RESULTS') then 'SBN'
		when pagetype = 'PRODUCT_DETAIL' then 'PDP'
		when pagetype in ('SHOPPING_BAG', 'CHECKOUT', 'ORDER_CONFIRMATION','REVIEW_ORDER') then 'CHECKOUT'
		else pagetype end as pagetype,
	case when lower(channel.channelbrand) like 'nordstrom' and channel.channelcountry = 'US' then 'Nordstrom' 
		when lower(channel.channelbrand) like 'nordstrom' and channel.channelcountry = 'CA' then 'Nordstrom_Canada'
		when lower(channel.channelbrand) like '%nordstrom_rack%' then 'Rack'
		else 'UNKNOWN' end as brand_country,
	channel.channelcountry,  
	case when pageinteractive = 0 then null else pageinteractive end as pageinteractive
	from acp_event_view.customer_page_load_measured
	where eventtime between {start_date} and {end_date}
	and isinterstitialnavigation = false 				
	and isidentifiedbot <> true 
; 

-- Navigation Type x Page Type x Experience x Brand Country
create or replace temp view outlier_1 as
select distinct 
	navigationtype, 
	pagetype,
	experience,     
	brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
group by 1,2,3,4,6
;

-- 'HARD+SOFT' Navigation
create or replace temp view outlier_2 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	experience,
	brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where navigationtype in ('HARD', 'SOFT')
group by 1,2,3,4,6
;

-- 'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience
create or replace temp view outlier_3 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
group by 1,2,3,4,6
;

-- 'DESKTOP_WEB+MOBILE_WEB' experience
create or replace temp view outlier_4 as
select distinct      
	navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,   
	brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
group by 1,2,3,4,6
;

-- 'HARD+SOFT' Navigation, 'All' Pagetype
create or replace temp view outlier_5 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	experience,
	brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where navigationtype in ('HARD', 'SOFT')
group by 1,2,3,4,6
;

-- 'HARD+SOFT' Navigation, 'All' Pagetype, 'All' Brand Country 
create or replace temp view outlier_6 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	experience,
	'All' brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where navigationtype in ('HARD', 'SOFT')
group by 1,2,3,4,6
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype
create or replace temp view outlier_7 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
group by 1,2,3,4,6
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype, 'All' brand_country
create or replace temp view outlier_8 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	'All' as brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
group by 1,2,3,4,6
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' brand_country
create or replace temp view outlier_9 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	'All' as brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
group by 1,2,3,4,6
;

--'HARD+SOFT' Navigation, 'All' brand_country
create or replace temp view outlier_10 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	experience,
	'All' as brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where navigationtype in ('HARD', 'SOFT')
group by 1,2,3,4,6
;

-- Navigation Type, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype, 'All' brand_country
create or replace temp view outlier_11 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	'All' as brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
group by 1,2,3,4,6
;

-- Navigation Type x Experience
create or replace temp view outlier_12 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	experience,     
	'All' as brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
group by 1,2,3,4,6
;

-- Navigation Type x brand_country
create or replace temp view outlier_13 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
group by 1,2,3,4,6
;

-- Navigation Type x brand_country x experience 
create or replace temp view outlier_14 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	experience,     
	brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
group by 1,2,3,4,6
;

-- Navigation Type x pagetype 
create or replace temp view outlier_15 as
select distinct 
	navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	'All' as brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
group by 1,2,3,4,6
;

-- Navigation Type x pagetype x experience 
create or replace temp view outlier_16 as
select distinct 
	navigationtype, 
	pagetype,
	experience,     
	'All' as brand_country,
	avg(pageinteractive) as avg_page_interactive,
	eventtime
from site_perf_outlier_summary
group by 1,2,3,4,6
;

create or replace temp view outlier_final as
select * from outlier_1
union all
select * from outlier_2
union all
select * from outlier_3
union all
select * from outlier_4
union all
select * from outlier_5
union all
select * from outlier_6
union all
select * from outlier_7
union all
select * from outlier_8
union all
select * from outlier_9
union all
select * from outlier_10
union all
select * from outlier_11
union all
select * from outlier_12
union all
select * from outlier_13
union all
select * from outlier_14
union all
select * from outlier_15
union all  
select * from outlier_16
;  

-- Writing output to teradata landing table from hive table to reduce memory load
-- This should match the "sql_table_reference" indicated on the .json file.
insert into table outlier_summary_final_output
select * from outlier_final
;
