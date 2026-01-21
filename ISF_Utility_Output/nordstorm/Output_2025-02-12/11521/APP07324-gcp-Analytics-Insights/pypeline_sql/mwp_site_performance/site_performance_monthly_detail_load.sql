SET QUERY_BAND = 'App_ID=APP08715;
     DAG_ID=site_performance_monthly_detail_11521_ACE_ENG;
     Task_Name=site_performance_monthly_detail_load;'
     FOR SESSION VOLATILE;

-- Reading data from upstream Hive table
create or replace temp view MBR_date_table_1 as
select   
	distinct month_idnt, month_start_day_date, month_end_day_date
	from object_model.day_cal_454_dim 
	where from_unixtime(unix_timestamp(day_date, 'MM/dd/yyyy')) 
	between date(TO_DATE(CAST(UNIX_TIMESTAMP({start_date}, 'MM/dd/yyyy') AS TIMESTAMP)))
	and date(TO_DATE(CAST(UNIX_TIMESTAMP({end_date}, 'MM/dd/yyyy') AS TIMESTAMP)))
;

create or replace temp view MBR_date_table_2 as
select * from MBR_date_table_1 
where date(TO_DATE(CAST(UNIX_TIMESTAMP(month_end_day_date, 'MM/dd/yyyy') AS TIMESTAMP))) <= current_date()
;

create or replace temp view site_perf_MBR_daily_detail as
select 
    b.month_idnt,
	date_trunc('day', eventtime) as eventtime,
	navigationtype,  
	useragent,
	experience,
	bucketingid,
	case when pagetype = 'HOME' then 'HOME'         
		when pagetype in ('BRAND_RESULTS', 'CATEGORY_RESULTS', 'FLASH_EVENTS', 'FLASH_EVENT_RESULTS', 'SEARCH_RESULTS')then 'SBN'
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
	join MBR_date_table_2 b
	on to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') 
	between from_unixtime(unix_timestamp(month_start_day_date, 'MM/dd/yyy')) 
	and from_unixtime(unix_timestamp(month_end_day_date, 'MM/dd/yyy'))
    where isinterstitialnavigation = false 				
	and isidentifiedbot <> true
;

-- Four different filters available(Navigation Type x Page Type x Experience x Brand Country) is two values each
-- Hence, a total number of 16(2 to the power 4) permutations are possible for the above given four filtersbe created to cover all possibilities  
-- Below given is the code of calculating and storing the percentile value of pageinteractive in a month for one such permutation

create or replace temp view monthly_1 as
select distinct 
	navigationtype, 
	pagetype,
	experience,     
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, navigationtype, pagetype, experience, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, navigationtype, pagetype, experience, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, navigationtype, pagetype, experience, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, navigationtype, pagetype, experience, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, navigationtype, pagetype, experience, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, navigationtype, pagetype, experience, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, navigationtype, pagetype, experience, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, navigationtype, pagetype, experience, brand_country) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
;

-- 'HARD+SOFT' Navigation
create or replace temp view monthly_2 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	experience,
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, pagetype, experience, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, pagetype, experience, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, pagetype, experience, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, pagetype, experience, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, pagetype, experience, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, pagetype, experience, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, pagetype, experience, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, pagetype, experience, brand_country) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where navigationtype in ('HARD', 'SOFT')
;

-- 'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience
create or replace temp view monthly_3 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, pagetype, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, pagetype, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, pagetype, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, pagetype, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, pagetype, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, pagetype, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, pagetype, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, pagetype, brand_country) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
;

-- 'DESKTOP_WEB+MOBILE_WEB' experience
create or replace temp view monthly_4 as
select distinct      
	navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,   
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, navigationtype, pagetype, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, navigationtype, pagetype, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, navigationtype, pagetype, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, navigationtype, pagetype, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, navigationtype, pagetype, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, navigationtype, pagetype, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, navigationtype, pagetype, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, navigationtype, pagetype, brand_country) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
;

-- 'HARD+SOFT' Navigation, 'All' Pagetype
create or replace temp view monthly_5 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	experience,
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, experience, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, experience, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, experience, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, experience, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, experience, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, experience, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, experience, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, experience, brand_country) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where navigationtype in ('HARD', 'SOFT')
;

-- 'HARD+SOFT' Navigation, 'All' Pagetype, 'All' Brand Country 
create or replace temp view monthly_6 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	experience,
	'All' brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, experience) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where navigationtype in ('HARD', 'SOFT')
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype
create or replace temp view monthly_7 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, brand_country) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, brand_country) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype, 'All' brand_country
create or replace temp view monthly_8 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
;

--'HARD+SOFT' Navigation, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' brand_country
create or replace temp view monthly_9 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, pagetype) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, pagetype) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, pagetype) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, pagetype) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, pagetype) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, pagetype) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, pagetype) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, pagetype) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
and navigationtype in ('HARD', 'SOFT')
;

--'HARD+SOFT' Navigation, 'All' brand_country
create or replace temp view monthly_10 as
select distinct 
	'HARD+SOFT' as navigationtype, 
	pagetype,
	experience,
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, pagetype, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, pagetype, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, pagetype, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, pagetype, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, pagetype, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, pagetype, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, pagetype, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, pagetype, experience) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where navigationtype in ('HARD', 'SOFT')
;

-- Navigation Type, 'DESKTOP_WEB+MOBILE_WEB' experience, 'All' pagetype, 'All' brand_country
create or replace temp view monthly_11 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, navigationtype) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, navigationtype) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, navigationtype) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, navigationtype) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, navigationtype) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, navigationtype) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, navigationtype) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, navigationtype) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
;

-- Navigation Type x Experience
create or replace temp view monthly_12 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	experience,     
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, navigationtype, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, navigationtype, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, navigationtype, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, navigationtype, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, navigationtype, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, navigationtype, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, navigationtype, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, navigationtype, experience) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
;

-- Navigation Type x brand_country
create or replace temp view monthly_13 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, navigationtype, brand_country) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, navigationtype, brand_country) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, navigationtype, brand_country) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, navigationtype, brand_country) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, navigationtype, brand_country) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, navigationtype, brand_country) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, navigationtype, brand_country)as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, navigationtype, brand_country) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
;

-- Navigation Type x brand_country x experience 
create or replace temp view monthly_14 as
select distinct 
	navigationtype, 
	'All' as pagetype,
	experience,     
	brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, navigationtype, brand_country, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, navigationtype, brand_country, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, navigationtype, brand_country, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, navigationtype, brand_country, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, navigationtype, brand_country, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, navigationtype, brand_country, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, navigationtype, brand_country, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, navigationtype, brand_country, experience) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
;

-- Navigation Type x pagetype 
create or replace temp view monthly_15 as
select distinct 
	navigationtype, 
	pagetype,
	'DESKTOP_WEB+MOBILE_WEB' as experience,     
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, navigationtype, pagetype) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, navigationtype, pagetype) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, navigationtype, pagetype) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, navigationtype, pagetype) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, navigationtype, pagetype) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, navigationtype, pagetype) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, navigationtype, pagetype) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, navigationtype, pagetype) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
where experience in ('DESKTOP_WEB', 'MOBILE_WEB')
;

-- Navigation Type x pagetype x experience 
create or replace temp view monthly_16 as
select distinct 
	navigationtype, 
	pagetype,
	experience,     
	'All' as brand_country,
	approx_percentile(pageinteractive, 0.25) over (partition by month_idnt, navigationtype, pagetype, experience) as p25_pageinteractive,
	approx_percentile(pageinteractive, 0.5) over (partition by month_idnt, navigationtype, pagetype, experience) as p50_pageinteractive,
	approx_percentile(pageinteractive, 0.75) over (partition by month_idnt, navigationtype, pagetype, experience) as p75_pageinteractive,
	approx_percentile(pageinteractive, 0.9) over (partition by month_idnt, navigationtype, pagetype, experience) as p90_pageinteractive,
	approx_percentile(pageinteractive, 0.95) over (partition by month_idnt, navigationtype, pagetype, experience) as p95_pageinteractive,
	approx_percentile(pageinteractive, 0.99) over (partition by month_idnt, navigationtype, pagetype, experience) as p99_pageinteractive,
	sum(case when navigationtype = 'SOFT' then 1 else 0 end) over (partition by month_idnt, navigationtype, pagetype, experience) as Soft_Navs,
	sum(case when navigationtype in ('HARD', 'SOFT') then 1 else 0 end) over (partition by month_idnt, navigationtype, pagetype, experience) as page_views,
	month_idnt
from site_perf_MBR_daily_detail
;


create or replace temp view monthly_final as
select * from monthly_1
union all
select * from monthly_2
union all
select * from monthly_3
union all
select * from monthly_4
union all
select * from monthly_5
union all
select * from monthly_6
union all
select * from monthly_7
union all
select * from monthly_8
union all
select * from monthly_9
union all
select * from monthly_10
union all
select * from monthly_11
union all
select * from monthly_12
union all
select * from monthly_13
union all
select * from monthly_14
union all
select * from monthly_15
union all
select * from monthly_16
;
  
-- Writing output to teradata landing table from hive table to reduce memory load
-- This should match the "sql_table_reference" indicated on the .json file.

insert into table monthly_detail_output
select * from monthly_final
;
