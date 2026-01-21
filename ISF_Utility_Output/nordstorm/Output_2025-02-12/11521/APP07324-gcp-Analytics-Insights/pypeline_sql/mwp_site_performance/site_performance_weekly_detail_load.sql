SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=site_performance_weekly_detail_11521_ACE_ENG;
     Task_Name=site_performance_weekly_detail_load;'
     FOR SESSION VOLATILE;

-- Reading data from upstream Hive table
create or replace temp view WBR_date_table as
select   
	distinct week_idnt, week_start_day_date , week_end_day_date
	from object_model.day_cal_454_dim 
	where from_unixtime(unix_timestamp(day_date, 'MM/dd/yyyy')) 
	between date(TO_DATE(CAST(UNIX_TIMESTAMP({start_date}, 'MM/dd/yyyy') AS TIMESTAMP)))
	and date(TO_DATE(CAST(UNIX_TIMESTAMP({end_date}, 'MM/dd/yyyy') AS TIMESTAMP)))
;

create or replace temp view site_perf_WBR_daily_detail as
select 
    b.week_idnt,  
	date_trunc('day', eventtime) as eventtime,
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
	from acp_event_view.customer_page_load_measured a
	join WBR_date_table b  
	on to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') 
	between from_unixtime(unix_timestamp(week_start_day_date, 'MM/dd/yyy')) and from_unixtime(unix_timestamp(week_end_day_date, 'MM/dd/yyy'))
    where isinterstitialnavigation = false 				
	and isidentifiedbot <> true  
;

-- Four different filters available(Navigation Type x Page Type x Experience x Brand Country) is two values each
-- Hence, a total number of 16(2 to the power 4) permutations are possible for the above given four filtersbe created to cover all possibilities  
-- Below given is the code of calculating and storing the percentile value of pageinteractive in a month for one such permutation

create or replace temp view weekly_1 as
select distinct 
	navigationtype, 
	pagetype,
	experience,     
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, navigationtype, pagetype, experience, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, navigationtype, pagetype, experience, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, navigationtype, pagetype, experience, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, navigationtype, pagetype, experience, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, navigationtype, pagetype, experience, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, navigationtype, pagetype, experience, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, navigationtype, pagetype, experience, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, navigationtype, pagetype, experience, brand_country) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
;

-- 'HARD+SOFT' Navigation
create or replace temp view weekly_2 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	experience,
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, pagetype, experience, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, pagetype, experience, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, pagetype, experience, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, pagetype, experience, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, pagetype, experience, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, pagetype, experience, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, pagetype, experience, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, pagetype, experience, brand_country) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where navigationtype in ('HARD', 'SOFT')
;

-- 'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience
create or replace temp view weekly_3 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, pagetype, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, pagetype, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, pagetype, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, pagetype, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, pagetype, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, pagetype, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, pagetype, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, pagetype, brand_country) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
;

-- 'DESKTOP_WEB+MOBILE_WEB' experience
create or replace temp view weekly_4 as
select distinct      
	navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,   
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, navigationtype, pagetype, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, navigationtype, pagetype, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, navigationtype, pagetype, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, navigationtype, pagetype, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, navigationtype, pagetype, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, navigationtype, pagetype, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, navigationtype, pagetype, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, navigationtype, pagetype, brand_country) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
;

-- 'HARD+SOFT' Navigation, 'All' Pagetype
create or replace temp view weekly_5 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	experience,
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, experience, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, experience, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, experience, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, experience, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, experience, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, experience, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, experience, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, experience, brand_country) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where navigationtype in ('HARD', 'SOFT')
;

-- 'HARD+SOFT' Navigation, 'All' Pagetype, 'All' Brand Country 
create or replace temp view weekly_6 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	experience,
	'All' brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, experience) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where navigationtype in ('HARD', 'SOFT')
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype
create or replace temp view weekly_7 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, brand_country) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype, 'All' brand_country
create or replace temp view weekly_8 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' brand_country
create or replace temp view weekly_9 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, pagetype) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, pagetype) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, pagetype) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, pagetype) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, pagetype) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, pagetype) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, pagetype) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, pagetype) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
;

--'HARD+SOFT' Navigation, 'All' brand_country
create or replace temp view weekly_10 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	experience,
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, pagetype, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, pagetype, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, pagetype, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, pagetype, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, pagetype, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, pagetype, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, pagetype, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, pagetype, experience) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where navigationtype in ('HARD', 'SOFT')
;

-- Navigation Type, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype, 'All' brand_country
create or replace temp view weekly_11 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, navigationtype) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, navigationtype) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, navigationtype) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, navigationtype) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, navigationtype) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, navigationtype) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, navigationtype) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, navigationtype) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
;

-- Navigation Type x Experience
create or replace temp view weekly_12 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	experience,     
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, navigationtype, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, navigationtype, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, navigationtype, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, navigationtype, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, navigationtype, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, navigationtype, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, navigationtype, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, navigationtype, experience) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
;

-- Navigation Type x brand_country
create or replace temp view weekly_13 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, navigationtype, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, navigationtype, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, navigationtype, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, navigationtype, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, navigationtype, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, navigationtype, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, navigationtype, brand_country)as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, navigationtype, brand_country) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
;

-- Navigation Type x brand_country x experience 
create or replace temp view weekly_14 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	experience,     
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, navigationtype, brand_country, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, navigationtype, brand_country, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, navigationtype, brand_country, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, navigationtype, brand_country, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, navigationtype, brand_country, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, navigationtype, brand_country, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, navigationtype, brand_country, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, navigationtype, brand_country, experience) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
;

-- Navigation Type x pagetype 
create or replace temp view weekly_15 as
select distinct 
	navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, navigationtype, pagetype) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, navigationtype, pagetype) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, navigationtype, pagetype) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, navigationtype, pagetype) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, navigationtype, pagetype) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, navigationtype, pagetype) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, navigationtype, pagetype) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, navigationtype, pagetype) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
;

-- Navigation Type x pagetype x experience 
create or replace temp view weekly_16 as
select distinct 
	navigationtype, 
	pagetype,
	experience,     
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by week_idnt, navigationtype, pagetype, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by week_idnt, navigationtype, pagetype, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by week_idnt, navigationtype, pagetype, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by week_idnt, navigationtype, pagetype, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by week_idnt, navigationtype, pagetype, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by week_idnt, navigationtype, pagetype, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by week_idnt, navigationtype, pagetype, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by week_idnt, navigationtype, pagetype, experience) as page_views,
	week_idnt
from site_perf_WBR_daily_detail
;


create or replace temp view weekly_final as
select * from weekly_1
union all
select * from weekly_2
union all
select * from weekly_3
union all
select * from weekly_4
union all
select * from weekly_5
union all
select * from weekly_6
union all
select * from weekly_7
union all
select * from weekly_8
union all
select * from weekly_9
union all
select * from weekly_10
union all
select * from weekly_11
union all
select * from weekly_12
union all
select * from weekly_13
union all
select * from weekly_14
union all
select * from weekly_15
union all
select * from weekly_16
;

-- Writing output to teradata landing table from hive table to reduce memory load
-- This should match the "sql_table_reference" indicated on the .json file.
insert into table weekly_detail_output
select * from weekly_final
;

