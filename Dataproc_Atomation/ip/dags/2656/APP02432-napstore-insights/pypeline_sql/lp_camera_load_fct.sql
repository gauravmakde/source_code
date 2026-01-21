SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=lp_camera_load_2656_napstore_insights;
Task_Name=lpcamera_events_to_csv_load_1_fct;'
FOR SESSION VOLATILE;

ET;

merge into {db_env}_NAP_FCT.STORE_TRAFFIC_FACT as T
using
(
select
a.store_num
,measurement_location_id
,measurement_location_type
,measurement_location_name
,sum(case when traffic_type = 'TrafficIn' then coalesce(traffic_count,0) else 0 end) as traffic_in
,sum(case when traffic_type = 'TrafficOut' then coalesce(traffic_count,0) else 0 end) as traffic_Out
,source_type
,start_time as start_time_utc
,end_time as end_time_utc
,cast(substr(start_time,1,19) || '+00:00' as timestamp(0)) at coalesce(b.time_zone,'GMT') as store_local_start_time
,cast(substr(end_time,1,19) || '+00:00' as timestamp(0))  at coalesce(b.time_zone,'GMT') as store_local_end_time
--,cast(substr(start_time,1,10) AS date) as traffic_date
,cast(substr(cast(store_local_start_time as varchar(50)),1,10) AS date) as traffic_date
--,cast( cast(substr(start_time,1,19) || '+00:00' as timestamp(0)) at coalesce(b.time_zone,'GMT') AS date) as traffic_date
,min(validity) as validity
,coalesce(c.country_code,'USA') as country_code
,current_date-1 as dw_batch_date
,current_timestamp as dw_sys_load_tmstp
,current_timestamp as dw_sys_updt_tmstp
from
(
Select a.*
,row_number() over (partition by a.store_num,a.measurement_location_id,start_time,traffic_type,source_type order by validity asc,measurement_time desc) as row_num
from
{db_env}_NAP_STG.STORE_TRAFFIC_STG a
where source_type ='Internal'
/* where cast(substr(start_time,1,10) as date)  >= date - 35 */
qualify row_num = 1
)A
left join
(
	SELECT	store_num,
		case when business_unit_num in (5500,9000,9500,6500) then 'CANADA' else 'USA' end as country_code
FROM	{db_env}_NAP_BASE_VWS.STORE_DIM
group by store_num,
		business_unit_num
)C
on a.store_num = c.store_num
Left join
(
	SELECT STORE_NUM 
	,case when trim(store_time_zone_desc) in ('America/New_York') then 'america eastern'
	when trim(store_time_zone_desc) in ('America/Los_Angeles') then 'america pacific'
	when trim(store_time_zone_desc) in ('America/Phoenix') then 'america mountain'
	when trim(store_time_zone_desc) in ('America/Chicago') then 'america central'
	when trim(store_time_zone_desc) in ('America/Denver') then 'america mountain'
	when trim(store_time_zone_desc) in ('Pacific/Honolulu') then 'america aleutian'
	when trim(store_time_zone_desc) in ('America/Anchorage') then 'america alaska'
	when trim(store_time_zone_desc) in ('America/Puerto_Rico') then 'america atlantic'
	when trim(store_time_zone_desc) in ('America/Detroit') then 'america eastern'
	when trim(store_time_zone_desc) in ('America/Edmonton') then 'america mountain'
	else 'GMT' end as time_zone
	FROM {db_env}_NAP_BASE_VWS.STORE_DIM
)B
on a.store_num = b.store_num
where trim(a.store_Num) not in ('CEC','706t')
group by
a.store_num
,b.time_zone
,measurement_location_id
,measurement_location_type
,measurement_location_name
,source_type
,start_time
,end_time
,traffic_date
,coalesce(c.country_code,'USA')
)as S
on s.store_num = t.store_num
and s.measurement_location_id = t.measurement_location_id
and s.start_time_utc = t.start_time_utc
and s.traffic_date = t.traffic_date
and s.source_type= t.source_type
/*and t.traffic_date >= date -35 */
when matched then update
set
	measurement_location_name = s.measurement_location_name  ,
	traffic_in    = s.traffic_in          ,
	traffic_out   = s.traffic_out          ,
	source_type   = s.source_type         ,
	measurement_status   = s.validity  ,
	country_code = s.country_code,
	dw_sys_updt_tmstp = current_timestamp

when not matched then insert
(
	store_num   ,
	measurement_location_id  ,
	measurement_location_type   ,
	measurement_location_name   ,
	traffic_in              ,
	traffic_out             ,
	source_type            ,
	start_time_utc             ,
	end_time_utc              ,
	start_time             ,
	end_time              ,
	traffic_date          ,
	measurement_status     ,
	country_code,
	dw_batch_date         ,
	dw_sys_load_tmstp     ,
	dw_sys_updt_tmstp
)
values
(
	s.store_num   ,
	s.measurement_location_id  ,
	s.measurement_location_type   ,
	s.measurement_location_name   ,
	s.traffic_in              ,
	s.traffic_out             ,
	s.source_type            ,
	s.start_time_utc             ,
	s.end_time_utc              ,
	s.store_local_start_time    ,
	s.store_local_end_time    ,
	s.traffic_date          ,
	s.validity     ,
	s.country_code,
	s.dw_batch_date         ,
	s.dw_sys_load_tmstp     ,
	s.dw_sys_updt_tmstp
);
ET;

collect stats on {db_env}_NAP_FCT.STORE_TRAFFIC_FACT;
ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
