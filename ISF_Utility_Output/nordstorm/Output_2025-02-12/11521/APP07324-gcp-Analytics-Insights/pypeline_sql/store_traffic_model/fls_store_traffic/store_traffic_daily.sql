SET QUERY_BAND = 'App_ID=APP08150; 
     DAG_ID=store_traffic_daily_11521_ACE_ENG;
     Task_Name=store_traffic_daily;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: t2dl_das_fls_traffic_model.store_traffic_daily
Team/Owner: tech_ffp_analytics/Selina Song/Matthew Bond

Notes: 
-- This view combines traffic for all the different store types - Nordstrom Stores, Nordstrom Rack Stores,
-- Nordstrom Locals & Last Chance Stores, in a single view. 
-- triggered by rn_camera_and_trips_daily

*/

--pull all retail_next and trips data 
CREATE MULTISET VOLATILE TABLE cam_traffic_trips AS (
select
rn_trips.day_date
, store_num as store_number
, channel
, store_type_code
, purchase_trips
, camera_traffic as traffic
from t2dl_das_fls_traffic_model.rn_camera_and_trips_daily as rn_trips
inner join prd_nap_usr_vws.day_cal as dc on rn_trips.day_date = dc.day_date
where month_num >= 202101
)
WITH DATA PRIMARY INDEX(store_number, day_date) ON COMMIT PRESERVE ROWS;


--load traffic from retail next cameras (~90 stores) and purchase trips for all stores
CREATE MULTISET VOLATILE TABLE rn_cam AS (
select
wct.day_date,
wct.store_number as store_num,
wct.channel,
wct.store_type_code,
'retailnext_cam' as traffic_source,
wct.purchase_trips,
wct.traffic
from cam_traffic_trips as wct
inner join prd_nap_base_vws.day_cal as dc on wct.day_date = dc.day_date
where 1=1
and wct.store_number in (select distinct store_num from t2dl_das_fls_traffic_model.rn_camera_and_trips_daily where camera_traffic is not null and store_num not in (4,5,358))
and month_num >= 202101
    UNION ALL
select
wct.day_date,
wct.store_number as store_num,
wct.channel,
wct.store_type_code,
cast(null as varchar(30)) as traffic_source,
wct.purchase_trips,
null as traffic
from cam_traffic_trips as wct
inner join prd_nap_base_vws.day_cal as dc on wct.day_date = dc.day_date
where 1=1
and wct.store_number not in (select distinct store_num from t2dl_das_fls_traffic_model.rn_camera_and_trips_daily where camera_traffic is not null and store_num not in (4,5,358))
and month_num >= 202101
)
WITH DATA PRIMARY INDEX(store_num, day_date) ON COMMIT PRESERVE ROWS;


--placer stores (317) 
CREATE MULTISET VOLATILE TABLE placer AS (
select
p.day_date,
p.store_num,
sd.business_unit_desc as channel,
case when p.store_num in (674,560,363,347,432,14,267,17,544,475,254,533,264,624,739,747,482,359,539,374) then 2 else 0 end as rack_walkthrough_flag,
cast('placer_store_raw' as varchar(30)) as traffic_source,
p.traffic
from t2dl_das_fls_traffic_model.store_traffic_daily_placer as p
inner join PRD_NAP_base_vws.STORE_DIM as sd on p.store_num = sd.store_num
inner join prd_nap_base_vws.day_cal as dc on p.day_date = dc.day_date
LEFT JOIN t2dl_das_real_estate.re_store_attributes as sa ON p.store_num = sa.store_number
where 1=1
and sd.business_unit_desc in ('Rack', 'FULL LINE') -- NL and CC also under them
and sd.store_type_code in ('RK', 'FL')
and month_num >= 202101
and (p.store_num <> 787 or p.day_date >= '2023-05-24') --when data starts at new 787 store location
    UNION ALL
select
p.day_date,
p.store_num,
sd.business_unit_desc as channel,
case when p.store_num in (674,560,363,347,432,14,267,17,544,475,254,533,264,624,739,747,482,359,539,374) then 2 else 0 end as rack_walkthrough_flag,
'placer_store_raw_closed' as traffic_source,
p.traffic
from t2dl_das_fls_traffic_model.store_traffic_daily_placer_closed as p
inner join PRD_NAP_base_vws.STORE_DIM as sd on p.store_num = sd.store_num
inner join prd_nap_base_vws.day_cal as dc on p.day_date = dc.day_date
LEFT JOIN t2dl_das_real_estate.re_store_attributes as sa ON p.store_num = sa.store_number
where 1=1
and ((p.store_num = 787 and p.day_date < '2023-05-24') --when data starts at new 787 store location
    or (p.day_date >= '2019-01-01')) --add all other closed stores
)
WITH DATA PRIMARY INDEX(store_num, day_date) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE rn_placer AS (
select
wct.day_date,
wct.store_number as store_num,
wct.channel,
case when wct.store_number in (674,560,363,347,432,14,267,17,544,475,254,533,264,624,739,747,482,359,539,374,267,264) then 2 else 0 end as rack_walkthrough_flag,
csd.traffic_source,
wct.purchase_trips,
wct.traffic as rn_traffic,
p.traffic as placer_traffic
from cam_traffic_trips as wct
inner join prd_nap_base_vws.day_cal as dc on wct.day_date = dc.day_date
inner join placer as p on p.store_num = wct.store_number and p.day_date = wct.day_date
left join T2DL_DAS_FLS_Traffic_Model.camera_store_details as csd on csd.store_number = wct.store_number and wct.day_date BETWEEN COALESCE(csd.start_date, date'1900-01-01') AND COALESCE(csd.end_date, date'2099-12-31')
left join t2dl_das_real_estate.re_store_attributes as sa ON wct.store_number = sa.store_number
where 1=1
and coalesce(csd.traffic_source, 'rn') <> 'Internal'
and month_num >= 202201
and wct.traffic is not null
and month_454_num not in (10,11) --remove nov/dec because placer is often much higher than camera (which is probably bc of camera weaknesses) but it scales the rest of the year too low
and wct.day_date not between '2022-08-29' and '2022-09-07' --remove p0 when camera and wifi data wasn't reliable
)
WITH DATA PRIMARY INDEX(store_num, day_date) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE channel_scaler AS (
select
channel,
rack_walkthrough_flag,
case when rack_walkthrough_flag = 0 and channel = 'RACK' then sd.subgroup_desc else 'all' end as region,
count(distinct rn_placer.store_num) as stores,
avg(rn_traffic) as rn_mean,
avg(placer_traffic) as placer_mean,
(placer_mean/(rn_mean*1.0000000)+2)/3 as placer_scaling_ratio,
--reduce scaler impact by 67% to prevent extremely high or low conversion due to channel scaler
placer_mean/placer_scaling_ratio as scaled_placer
from rn_placer
inner join prd_nap_base_vws.store_dim as sd on sd.store_num = rn_placer.store_num
where rn_placer.store_num <> 57 --exclude asos store which isn't really representative of fls or rack
--and coalesce(traffic_source, 'rn') = 'RetailNext' --<> 'Internal' --use only retail next cameras (for now)
and rn_placer.store_num <> 400 --exclude a store with really high scaling ratio (>2) so it doesn't affect channel scaler
and sd.store_open_date < current_date - 360 --remove NSOs since their placer data needs time to stabilize
group by 1,2,3
)
WITH DATA PRIMARY INDEX(channel) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE store_scaler AS (
select
store_num,
channel,
traffic_source,
avg(rn_traffic) as rn_mean,
avg(placer_traffic) as placer_mean,
rn_mean/(placer_mean*1.0000000) as rn_scaling_ratio,
rn_mean/rn_scaling_ratio as scaled_rn,
placer_mean/(rn_mean*1.0000000) as placer_scaling_ratio,
placer_mean/placer_scaling_ratio as scaled_placer
from rn_placer
group by 1,2,3
QUALIFY Row_Number() OVER (PARTITION BY store_num ORDER BY traffic_source DESC) = 1 --remove a few duplicate rows where csd has date gaps
)
WITH DATA PRIMARY INDEX(store_num) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE placer_store_scaled AS (
select
p.day_date,
p.store_num,
p.channel,
case when coalesce(s.traffic_source, 'rn') = 'internal' then 'placer_store_scaled_to_lp'
    when p.traffic_source = 'placer_store_raw_closed' then 'placer_closed_store_scaled_to_retailnext'
    else 'placer_store_scaled_to_retailnext' end as traffic_source,
p.traffic/s.placer_scaling_ratio as traffic
, p.traffic as traffic_old
from placer as p
inner join store_scaler as s on p.store_num = s.store_num
)
WITH DATA PRIMARY INDEX(day_date, store_num) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE placer_channel_scaled AS (
select
p.day_date,
p.store_num,
p.channel,
case when p.traffic_source = 'placer_store_raw_closed' then 'placer_closed_store_scaled_to_retailnext'
    else 'placer_channel_scaled_to_retailnext' end as traffic_source,
p.traffic/s.placer_scaling_ratio as traffic,
p.rack_walkthrough_flag,
s.placer_scaling_ratio,
p.traffic as traffic_old
from placer as p
inner join prd_nap_base_vws.store_dim as sd on sd.store_num = p.store_num
left join channel_scaler as s on p.channel = s.channel and p.rack_walkthrough_flag = s.rack_walkthrough_flag and s.region = (case when p.rack_walkthrough_flag = 0 and p.channel = 'RACK' then sd.subgroup_desc else 'all' end)
where p.store_num not in (select distinct store_num from store_scaler where coalesce(traffic_source, 'rn') <> 'internal') --remove stores with individual store data
and p.store_num not in (select distinct store_num from t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference) --remove stores with VI data (higher quality)
)
WITH DATA PRIMARY INDEX(day_date, store_num) ON COMMIT PRESERVE ROWS;


--stores with vertical interference (residential/commercial space above/below the store that pollutes the placer data) need a special placer pipeline
--the placer pipeline filters based on opening hours and removes residents/employees and people that stayed over 150min
--the placer pipeline pulls weekly data (daily is unavailable/incomplete)- for some stores it pulls 2-3 weeks (bc low panel size, typically for multi-floor stores that have a small footprint)
--table below scales 1/2/3 week chunks of data to daily scale using channel-level ratios of how traffic is split across weekdays
--table below also removes known store closures and adjusts the scaler to evenly distribute the traffic from the closed day(s) across the other weekdays
CREATE MULTISET VOLATILE TABLE vertical_interference AS (
with weeks as (
select distinct
week_idnt
,week_start_day_date
,day_abrv
,day_date
from PRD_NAP_USR_VWS.DAY_CAL_454_DIM as dc
where dc.week_idnt between 202201 and (select week_idnt from PRD_NAP_USR_VWS.DAY_CAL_454_DIM where day_date = current_date)-1
)
, weeks_lead as (
select
week_idnt
,LEAD(week_idnt, 7) OVER(ORDER BY day_date) as week_idnt_lead_1
,LEAD(week_idnt, 14) OVER(ORDER BY day_date) as week_idnt_lead_2
,week_start_day_date
,day_abrv
,day_date
from weeks
)
, daily as (
select
store_nums.store_num
,dc.week_idnt
,dc.week_idnt_lead_1
,dc.week_idnt_lead_2
,dc.day_date
,dc.day_abrv
,case when vi.store_num = 539 then --this store is closed on sunday so it needs a special scaling ratio
        case when dc.day_abrv = 'SUN' then 0
            when dc.day_abrv in ('MON','TUE','WED') then 0.15
            when dc.day_abrv = 'THU' then 0.14
            when dc.day_abrv = 'FRI' then 0.16
            when dc.day_abrv = 'SAT' then 0.25
            else null end
when sd.business_unit_desc = 'FULL LINE' then
        case when dc.day_abrv = 'SUN' then 0.16
            when dc.day_abrv in ('MON','TUE','WED','THU') then 0.11
            when dc.day_abrv = 'FRI' then 0.16
            when dc.day_abrv = 'SAT' then 0.24
            else null end
when sd.business_unit_desc = 'RACK' then
        case when dc.day_abrv = 'SUN' then 0.15
            when dc.day_abrv in ('MON','TUE') then 0.11
            when dc.day_abrv in ('WED','THU') then 0.12
            when dc.day_abrv = 'FRI' then 0.16
            when dc.day_abrv = 'SAT' then 0.22
            else null end
else null end as ratio
,closed_flag
,case when closed_flag is null then 1 else 0 end as open_days
,traffic
,panel_visits
,visitors
,panel_visitors
from weeks_lead as dc
cross join (select distinct store_num from t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference) as store_nums
left join t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference as vi on vi.fw_start_date = dc.week_start_day_date and vi.store_num = store_nums.store_num
left join PRD_NAP_USR_VWS.store_dim as sd on vi.store_num = sd.store_num
left join t2dl_das_fls_traffic_model.store_closures as sc on dc.day_date = sc.day_date and vi.store_num = sc.store_num
    UNION ALL
select
store_nums.store_num
,dc.week_idnt
,dc.week_idnt_lead_1
,dc.week_idnt_lead_2
,dc.day_date
,dc.day_abrv
,case when dc.day_abrv = 'SUN' then 0.15
            when dc.day_abrv in ('MON','TUE') then 0.11
            when dc.day_abrv in ('WED','THU') then 0.12
            when dc.day_abrv = 'FRI' then 0.16
            when dc.day_abrv = 'SAT' then 0.22
            else null end as ratio
,closed_flag
,case when closed_flag is null then 1 else 0 end as open_days
,traffic
,panel_visits
,visitors
,panel_visitors
from weeks_lead as dc
cross join (select distinct store_num from t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference where store_num in (551,716,111,112)) as store_nums
left join t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference as vi on vi.store_num = store_nums.store_num
left join t2dl_das_fls_traffic_model.store_closures as sc on dc.day_date = sc.day_date and vi.store_num = sc.store_num
and dc.week_start_day_date > (select max(fw_start_date) from t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference where store_num in (551,716,111,112))
    UNION ALL
select
store_nums.store_num
,dc.week_idnt
,dc.week_idnt_lead_1
,dc.week_idnt_lead_2
,dc.day_date
,dc.day_abrv
,case when dc.day_abrv = 'SUN' then 0.15
            when dc.day_abrv in ('MON','TUE') then 0.11
            when dc.day_abrv in ('WED','THU') then 0.12
            when dc.day_abrv = 'FRI' then 0.16
            when dc.day_abrv = 'SAT' then 0.22
            else null end as ratio
,closed_flag
,case when closed_flag is null then 1 else 0 end as open_days
,traffic
,panel_visits
,visitors
,panel_visitors
from weeks_lead as dc
cross join (select distinct store_num from t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference where store_num in (675,547,274,509,671)) as store_nums
left join t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference as vi on vi.store_num = store_nums.store_num
left join t2dl_das_fls_traffic_model.store_closures as sc on dc.day_date = sc.day_date and vi.store_num = sc.store_num
and dc.week_start_day_date > (select max(fw_start_date) from t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference where store_num in (675,547,274,509,671))
)
, closed as (
select
store_num
,week_idnt
,week_idnt_lead_1
,week_idnt_lead_2
,sum(case when open_days = 0 then d.ratio else 0 end) as ratio
,sum(open_days) as open_days
from daily as d
group by 1,2,3,4
)
, lead1_weekly as (
select
daily.store_num
,daily.week_idnt
,daily.week_idnt_lead_2
,avg(daily.traffic) as traffic
,avg(daily.panel_visits) as panel_visits
,avg(daily.visitors) as visitors
,avg(daily.panel_visitors) as panel_visitors
from daily
left join closed on daily.store_num = closed.store_num and daily.week_idnt between closed.week_idnt and closed.week_idnt_lead_1
group by 1,2,3
where daily.store_num in (675,547,274,509,671)
)
, daily_lead1 as (
select
vi.store_num
,dc.week_idnt
,dc.day_date
,case when dc.day_abrv = 'SUN' then 0.15
        when dc.day_abrv in ('MON','TUE') then 0.11
        when dc.day_abrv in ('WED','THU') then 0.12
        when dc.day_abrv = 'FRI' then 0.16
        when dc.day_abrv = 'SAT' then 0.22
        else null end as ratio
,closed_flag
,case when closed_flag is null then 1 else 0 end as open_days
,traffic
,panel_visits
,visitors
,panel_visitors
from weeks_lead as dc
cross join (select distinct store_num from t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference where store_num in (675,547,274,509,671)) as store_nums
left join lead1_weekly as vi on vi.week_idnt = dc.week_idnt and vi.store_num = store_nums.store_num
left join t2dl_das_fls_traffic_model.store_closures as sc on dc.day_date = sc.day_date and vi.store_num = sc.store_num
where vi.store_num in (675,547,274,509,671)
)
, closed_lead1 as (
select
store_num
,week_idnt
,sum(case when open_days = 0 then ratio else 0 end) as ratio
,sum(open_days) as open_days
from daily_lead1
group by 1,2
)
, lead2_weekly as (
select
daily.store_num
,daily.week_idnt
,daily.week_idnt_lead_2
,avg(daily.traffic) as traffic
,count(daily.traffic) as traffic_test
,avg(daily.panel_visits) as panel_visits
,avg(daily.visitors) as visitors
,avg(daily.panel_visitors) as panel_visitors
from daily
left join closed on daily.store_num = closed.store_num and daily.week_idnt between closed.week_idnt and closed.week_idnt_lead_2
group by 1,2,3
where daily.store_num in (551,716,111,112)
)
, daily_lead2 as (
select
vi.store_num
,dc.week_idnt
,dc.day_date
,case when dc.day_abrv = 'SUN' then 0.15
        when dc.day_abrv in ('MON','TUE') then 0.11
        when dc.day_abrv in ('WED','THU') then 0.12
        when dc.day_abrv = 'FRI' then 0.16
        when dc.day_abrv = 'SAT' then 0.22
        else null end as ratio
,closed_flag
,case when closed_flag is null then 1 else 0 end as open_days
,traffic
,panel_visits
,visitors
,panel_visitors
from weeks_lead as dc
cross join (select distinct store_num from t2dl_das_fls_traffic_model.store_traffic_daily_placer_vertical_interference where store_num in (551,716,111,112)) as store_nums
left join lead2_weekly as vi on vi.week_idnt = dc.week_idnt and vi.store_num = store_nums.store_num
left join t2dl_das_fls_traffic_model.store_closures as sc on dc.day_date = sc.day_date and vi.store_num = sc.store_num
where vi.store_num in (551,716,111,112)
)
, closed_lead2 as (
select
store_num
,week_idnt
,sum(case when open_days = 0 then ratio else 0 end) as ratio
,sum(open_days) as open_days
from daily_lead2
group by 1,2
)
select
    dl.store_num
        , day_date
        , dl.week_idnt
        , case when closed_flag is null then dl.ratio + (cl.ratio/cl.open_days) else 0 end as ratio_scaled
        , cast ((traffic * ratio_scaled) as int) as traffic
        , cast ((panel_visits * ratio_scaled) as int) as panel_visits
        , cast ((visitors * ratio_scaled) as int) as visitors
        , cast ((panel_visitors * ratio_scaled) as int) as panel_visitors
from daily_lead2 as dl
left join closed_lead2 as cl on cl.week_idnt = dl.week_idnt and cl.store_num = dl.store_num
UNION ALL
select
    dl.store_num
        , day_date
        , dl.week_idnt
        , case when closed_flag is null then dl.ratio + (cl.ratio/cl.open_days) else 0 end as ratio_scaled
        , cast ((traffic * ratio_scaled) as int) as traffic
        , cast ((panel_visits * ratio_scaled) as int) as panel_visits
        , cast ((visitors * ratio_scaled) as int) as visitors
        , cast ((panel_visitors * ratio_scaled) as int) as panel_visitors
from daily_lead1 as dl
left join closed_lead1 as cl on cl.week_idnt = dl.week_idnt and cl.store_num = dl.store_num
    UNION ALL
select
dl.store_num
,day_date
,dl.week_idnt
,case when closed_flag is null then dl.ratio + (cl.ratio/cl.open_days) else 0 end as ratio_scaled
,cast((traffic * ratio_scaled) as int) as traffic
,cast((panel_visits * ratio_scaled) as int) as panel_visits
,cast((visitors * ratio_scaled) as int) as visitors
,cast((panel_visitors * ratio_scaled) as int) as panel_visitors
from daily as dl
left join closed as cl on cl.week_idnt = dl.week_idnt and cl.store_num = dl.store_num
where dl.store_num not in (551,716,111,112,675,547,274,509,671)
)
WITH DATA PRIMARY INDEX(day_date, store_num) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE rn_placer_vi AS (
select
wct.day_date,
wct.store_number as store_num,
wct.channel,
sd.store_type_code,
csd.traffic_source,
wct.purchase_trips,
wct.traffic as rn_traffic,
vi.traffic as placer_traffic
from cam_traffic_trips as wct
inner join prd_nap_base_vws.day_cal as dc on wct.day_date = dc.day_date
inner join prd_nap_usr_vws.store_dim as sd on sd.store_num = wct.store_number
inner join vertical_interference as vi on vi.store_num = wct.store_number and vi.day_date = wct.day_date
left join T2DL_DAS_FLS_Traffic_Model.camera_store_details as csd on csd.store_number = wct.store_number and wct.day_date BETWEEN COALESCE(csd.start_date, date'1900-01-01') AND COALESCE(csd.end_date, date'2099-12-31')
where 1=1
and coalesce(csd.traffic_source, 'rn') <> 'Internal'
and month_num >= 202201
and wct.store_number not in (select distinct store_num from t2dl_das_fls_traffic_model.store_traffic_daily_placer) --remove stores with store level data
and month_454_num not in (10,11) --remove nov/dec because placer is often much higher than camera (which is probably bc of camera weaknesses) but it scales the rest of the year too low
and wct.day_date not between '2022-08-29' and '2022-09-07' --remove p0 when camera and wifi data wasn't reliable
and wct.traffic is not null
and wct.traffic > 0
)
WITH DATA PRIMARY INDEX(store_num, day_date) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE scaler_vi AS (
select
store_num,
channel,
store_type_code,
traffic_source,
avg(rn_traffic) as rn_mean,
avg(placer_traffic) as placer_mean,
--rn_mean/(placer_mean*1.0000000) as rn_scaling_ratio,
--rn_mean/rn_scaling_ratio as scaled_rn,
placer_mean/(rn_mean*1.0000000) as placer_scaling_ratio,
placer_mean/placer_scaling_ratio as scaled_placer
from rn_placer_vi
group by 1,2,3,4
)
WITH DATA PRIMARY INDEX(store_num) ON COMMIT PRESERVE ROWS;
-- most scalers are 0.3-0.6 which makes sense because we are filtering down so much and may have poor reception
-- Exceptions are store 16 (underground but there is a lot of traffic above ground), 674 (probably interference from stores above ours)


CREATE MULTISET VOLATILE TABLE placer_scaled_vi AS (
select
p.day_date,
p.store_num,
sd.business_unit_desc as channel,
'placer_vertical_interference_scaled_to_retailnext' as traffic_source,
p.traffic/s.placer_scaling_ratio as traffic
--, p.traffic as traffic_old
from vertical_interference as p
inner join PRD_NAP_base_vws.STORE_DIM as sd on p.store_num = sd.store_num
inner join scaler_vi as s on p.store_num = s.store_num
)
WITH DATA PRIMARY INDEX(day_date, store_num) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE placer_vi_custom AS (
select
p.day_date,
p.store_num,
case
    when p.store_num = 1 then 321--
    when p.store_num = 47 then 785--
    when p.store_num = 111 then 670--
    when p.store_num = 112 then 265
    when p.store_num = 210 then 425--
    when p.store_num = 238 then 239--
    when p.store_num = 245 then 268--
    when p.store_num = 280 then 265--
    when p.store_num = 289 then 513
    when p.store_num = 350 then 476
    when p.store_num = 351 then 160
    when p.store_num = 357 then 272--
    when p.store_num = 359 then 393
    when p.store_num = 363 then 17
    when p.store_num = 432 then 14--
    when p.store_num = 510 then 717
    when p.store_num = 519 then 649
    when p.store_num = 529 then 736--
    when p.store_num = 531 then 235--
    when p.store_num = 544 then 648
    when p.store_num = 547 then 534--
    when p.store_num = 551 then 513--
    when p.store_num = 560 then 791 --rack walkthrough store, scaling with non-walkthrough bc no comparable walkthroughs
    when p.store_num = 642 then 164
    when p.store_num = 671 then 649
    when p.store_num = 675 then 393
    when p.store_num = 706 then 621
    when p.store_num = 716 then 798
    when p.store_num = 739 then 264
    when p.store_num = 745 then 545
    when p.store_num = 774 then 763
    when p.store_num = 788 then 253
    else null end as rn_store_num,
sd.business_unit_desc as channel,
p.traffic
from vertical_interference as p
inner join PRD_NAP_base_vws.STORE_DIM as sd on p.store_num = sd.store_num
inner join prd_nap_base_vws.day_cal as dc on p.day_date = dc.day_date
where 1=1
and p.store_num not in (select distinct store_num from t2dl_das_fls_traffic_model.store_traffic_daily_placer) --remove complexes that have store level data
and sd.business_unit_desc in ('Rack', 'FULL LINE') -- NL and CC also under them
and month_num >= 202201
)
WITH DATA PRIMARY INDEX(store_num, day_date) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE scaler_vi_custom AS (
select
p.store_num,
p.rn_store_num,
p.channel,
avg(p.traffic) as placer_vi_mean,
coalesce(avg(pss.traffic),avg(pcs.traffic)) as placer_mean,
avg(p.traffic)/(coalesce(avg(pss.traffic),avg(pcs.traffic))*1.0000000) as placer_scaling_ratio
from placer_vi_custom as p
left join placer_store_scaled as pss on pss.store_num = p.rn_store_num and pss.day_date = p.day_date
left join placer_channel_scaled as pcs on pcs.store_num = p.rn_store_num and pcs.day_date = p.day_date
where p.store_num not in (select distinct store_num from scaler_vi) --remove stores scaled to camera data
group by 1,2,3
)
WITH DATA PRIMARY INDEX(store_num) ON COMMIT PRESERVE ROWS;
--scalers are 0.4-5x which makes sense since some custom pois will be getting extra non-nordstrom traffic and some will have reception issues (undercount)


CREATE MULTISET VOLATILE TABLE placer_scaled_vi_custom AS (
select
p.day_date,
p.store_num,
p.channel,
'placer_vertical_interference_scaled_to_est_retailnext' as traffic_source,
p.traffic/s.placer_scaling_ratio as traffic
from placer_vi_custom as p
inner join scaler_vi_custom as s on p.store_num = s.store_num
)
WITH DATA PRIMARY INDEX(day_date, store_num) ON COMMIT PRESERVE ROWS;


CREATE MULTISET VOLATILE TABLE combo AS (
select
day_date,
store_num,
channel,
cast(traffic_source as varchar(60)) as traffic_source,
purchase_trips,
traffic
from rn_cam
    UNION ALL
select
day_date,
store_num,
channel,
traffic_source,
null as purchase_trips,
traffic
from placer_store_scaled
    UNION ALL
select
day_date,
store_num,
channel,
traffic_source,
null as purchase_trips,
traffic
from placer_channel_scaled
    UNION ALL
select
day_date,
store_num,
channel,
traffic_source,
null as purchase_trips,
traffic
from placer_scaled_vi
    UNION ALL
select
day_date,
store_num,
channel,
traffic_source,
null as purchase_trips,
traffic
from placer_scaled_vi_custom
)
WITH DATA PRIMARY INDEX(day_date, store_num) ON COMMIT PRESERVE ROWS;


-- force Placer traffic to be NULL for store closures (holiday, sunday, random, etc)
CREATE MULTISET VOLATILE TABLE combo_final AS (
select
combo.day_date,
combo.store_num,
channel,
traffic_source,
purchase_trips,
case when sc.closed_flag is not null then 0 else traffic end as traffic
from combo
left join t2dl_das_fls_traffic_model.store_closures as sc on combo.day_date = sc.day_date and combo.store_num = sc.store_num
where combo.day_date >= '2021-01-31'
)
WITH DATA PRIMARY INDEX(day_date, store_num) ON COMMIT PRESERVE ROWS;


DELETE FROM {fls_traffic_model_t2_schema}.store_traffic_daily;

--------------------------------------------------------------------
/* 
final table creation
*/
INSERT INTO {fls_traffic_model_t2_schema}.store_traffic_daily
SELECT 
day_date,
store_num,
channel,
traffic_source,
purchase_trips,
traffic, 
CURRENT_TIMESTAMP as dw_sys_load_tmstp
from combo_final
;

COLLECT STATISTICS COLUMN(day_date), COLUMN(store_num) ON {fls_traffic_model_t2_schema}.store_traffic_daily;


SET QUERY_BAND = NONE FOR SESSION;