SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=retailnext_load_2656_napstore_insights;
Task_Name=retailnext_load_fct;'
FOR SESSION VOLATILE;

ET;

update {db_env}_NAP_BASE_VWS.store_traffic_stg
set end_time = '2024-03-10 03:59:00'
where start_time = '2024-03-10 03:45:00'
and store_num = '701';

ET;

update {db_env}_NAP_BASE_VWS.store_traffic_stg
set end_time = '2024-03-10 04:00:00'
where start_time = '2024-03-10 03:45:00'
and store_num <> '701';

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
,cast(substr(start_time,1,19) || '+00:00' as timestamp(0)) at coalesce(b.time_zone,'GMT') as start_time
,cast(substr(end_time,1,19) || '+00:00' as timestamp(0))  at coalesce(b.time_zone,'GMT') as end_time
--,cast(substr(start_time,1,10) AS date) as traffic_date
,cast( cast(substr(start_time,1,19) || '+00:00' as timestamp(0)) at coalesce(b.time_zone,'GMT') AS date) as traffic_date
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
{db_env}_NAP_BASE_VWS.store_traffic_stg a
where source_type ='RetailNext'
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
where trim(a.store_Num) not in ('CEC','706t','3lab')
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
	s.start_time             ,
	s.end_time              ,
	s.traffic_date          ,
	s.validity     ,
	s.country_code,
	s.dw_batch_date         ,
	s.dw_sys_load_tmstp     ,
	s.dw_sys_updt_tmstp
);

collect stats on {db_env}_NAP_FCT.STORE_TRAFFIC_FACT;


ET;


DELETE FROM {db_env}_NAP_FCT.STORE_TRAFFIC_SALES_REGISTER_FACT WHERE tran_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT');

INSERT INTO {db_env}_NAP_FCT.STORE_TRAFFIC_SALES_REGISTER_FACT
SELECT
	header.ringing_store_num,
	coalesce(register_map.measurement_location_id , '00000000-0000-0000-0000-000000000000' ) measurement_location_id,
	coalesce(register_map.measurement_location_type, 'other') measurement_location_type,
	coalesce(register_map.measurement_location_name, 'Unknown location') measurement_location_name ,
	header.register_num,
	NORD_UDF.getWindowStart_15min(header.tran_time) as start_time,
	start_time + interval '15' minute as end_time,
 	header.tran_date,
 	header.tran_type_code,
	COUNT( DISTINCT header.global_tran_id) AS transaction_count,
	SUM(CASE WHEN detail.line_item_merch_nonmerch_ind = 'MERCH' THEN 1 ELSE 0 END) AS merch_line_count,
	SUM(CASE WHEN detail.line_item_merch_nonmerch_ind = 'NMERCH' THEN 1 ELSE 0 END) AS non_merch_line_count,
	COUNT(detail.line_item_seq_num) line_item_count,
	SUM(detail.line_net_amt) AS sum_line_item_amount,
	detail.line_item_net_amt_currency_code currency_code,
	'USA' country_code ,
	current_date dw_batch_date,
	current_timestamp(0) as dw_sys_load_tmstp,
	current_timestamp(0) as dw_sys_updt_tmstp
FROM
	{db_env}_NAP_BASE_VWS.RETAIL_TRAN_DETAIL_FACT detail
INNER JOIN {db_env}_NAP_BASE_VWS.RETAIL_TRAN_HDR_FACT header ON
	header.global_tran_id = detail.global_tran_id
	AND header.business_day_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT')
	AND detail.business_day_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT')
	AND header.ringing_store_num IN (SELECT DISTINCT store_num FROM {db_env}_NAP_DIM.STORE_TRAFFIC_LOCATION_REGISTER)
LEFT JOIN {db_env}_NAP_BASE_VWS.STORE_TRAFFIC_LOCATION_REGISTER register_map ON
	header.ringing_store_num = register_map.store_num
	AND header.register_num = register_map.register_num
	AND header.business_day_date BETWEEN register_map.effective_start_date AND register_map.effective_end_date
GROUP BY
	header.ringing_store_num,
	register_map.measurement_location_id,
	register_map.measurement_location_name,
	register_map.measurement_location_type,
	header.register_num,
	start_time,
	header.tran_date,
	header.tran_type_code,
	detail.line_item_net_amt_currency_code;



ET;


DELETE FROM {db_env}_NAP_FCT.STORE_TRAFFIC_TENDER_FACT WHERE tran_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT');

INSERT INTO {db_env}_NAP_FCT.STORE_TRAFFIC_TENDER_FACT
SELECT
	header.ringing_store_num,
	CASE WHEN register_map.measurement_location_id IS NULL THEN '00000000-0000-0000-0000-000000000000' ELSE register_map.measurement_location_id END,
	CASE WHEN register_map.measurement_location_type IS NULL THEN 'other' ELSE register_map.measurement_location_type END,
	CASE WHEN register_map.measurement_location_name IS NULL THEN 'Unknown location' ELSE register_map.measurement_location_name END,
	header.register_num,
	NORD_UDF.getWindowStart_15min(header.tran_time) as start_time,
	start_time + interval '15' minute as end_time,
 	header.tran_date,
	tender.tender_type_code,
	count(1) as tender_count,
	sum(tender_item_amt) as tender_amt,
	tender_item_amt_currency_code as currency_code,
	'USA' as country_code,
	current_date dw_batch_date,
	current_timestamp(0) as dw_sys_load_tmstp ,
	current_timestamp(0) as dw_sys_updt_tmstp
FROM
	{db_env}_NAP_FCT.RETAIL_TRAN_TENDER_FACT tender
INNER JOIN {db_env}_NAP_FCT.RETAIL_TRAN_HDR_FACT header ON
	header.global_tran_id = tender.global_tran_id
	AND header.business_day_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT')
	AND tender.business_day_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT')
	AND header.ringing_store_num IN (210, 384)
LEFT JOIN {db_env}_NAP_DIM.STORE_TRAFFIC_LOCATION_REGISTER register_map ON
	header.ringing_store_num = register_map.store_num
	AND header.register_num = register_map.register_num
	AND header.business_day_date BETWEEN register_map.effective_start_date AND register_map.effective_end_date
GROUP BY
	header.ringing_store_num,
	register_map.measurement_location_id,
	register_map.measurement_location_name,
	register_map.measurement_location_type,
	header.register_num,
	start_time,
	header.tran_date,
	tender.tender_type_code,
	tender_item_amt_currency_code ;



ET;

DELETE FROM {db_env}_NAP_FCT.STORE_TRAFFIC_SALES_FACT WHERE traffic_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT');

INSERT INTO {db_env}_NAP_FCT.STORE_TRAFFIC_SALES_FACT
SELECT
	traffic.store_num,
	traffic.measurement_location_id,
	traffic.measurement_location_type,
	traffic.measurement_location_name,
	traffic.start_time,
	traffic.end_time,
	traffic.traffic_date,
	transactions.tran_type_code,
	ZEROIFNULL(transactions.transaction_count),
	ZEROIFNULL(transactions.line_item_count),
	ZEROIFNULL(transactions.sum_line_item_amount),
	transactions.currency_code,
	ZEROIFNULL(traffic.traffic_in),
	ZEROIFNULL(traffic.traffic_out),
	traffic.country_code,
	current_date dw_batch_date,
	current_timestamp(0) as dw_sys_load_tmstp,
	current_timestamp(0) as dw_sys_updt_tmstp
FROM (SELECT
		header.ringing_store_num,
		coalesce(register_map.measurement_location_id , '00000000-0000-0000-0000-000000000000' ) measurement_location_id,
		coalesce(register_map.measurement_location_type, 'other') measurement_location_type,
		coalesce(register_map.measurement_location_name, 'Unknown location') measurement_location_name ,
		NORD_UDF.getWindowStart_15min(header.tran_time) as start_time,
		start_time + interval '15' minute as end_time,
	 	header.tran_type_code,
		COUNT( DISTINCT header.global_tran_id) AS transaction_count,
		COUNT(detail.line_item_seq_num) line_item_count,
		SUM(CASE WHEN detail.line_item_merch_nonmerch_ind = 'MERCH' THEN 1 ELSE 0 END) AS merch_line_count,
		SUM(CASE WHEN detail.line_item_merch_nonmerch_ind = 'NMERCH' THEN 1 ELSE 0 END) AS non_merch_line_count,
		SUM(detail.line_net_amt) AS sum_line_item_amount,
		detail.line_item_net_amt_currency_code currency_code
	FROM
		{db_env}_NAP_BASE_VWS.RETAIL_TRAN_DETAIL_FACT detail
	INNER JOIN {db_env}_NAP_BASE_VWS.RETAIL_TRAN_HDR_FACT header ON
		header.global_tran_id = detail.global_tran_id
		AND header.business_day_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT')
		AND detail.business_day_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT')
		AND header.ringing_store_num IN (SELECT DISTINCT store_num FROM {db_env}_NAP_DIM.STORE_TRAFFIC_LOCATION_REGISTER)
	LEFT JOIN {db_env}_NAP_BASE_VWS.STORE_TRAFFIC_LOCATION_REGISTER register_map ON
		header.ringing_store_num = register_map.store_num
		AND header.register_num = register_map.register_num
		AND header.business_day_date BETWEEN register_map.effective_start_date AND register_map.effective_end_date
	GROUP BY
		header.ringing_store_num,
		register_map.measurement_location_id,
		register_map.measurement_location_name,
		register_map.measurement_location_type,
		start_time,
		header.tran_type_code,
		detail.line_item_net_amt_currency_code ) transactions
FULL OUTER JOIN (
	SELECT
		register_map.store_num,
		traffic.measurement_location_id,
		traffic.measurement_location_type,
		traffic.measurement_location_name,
		traffic.start_time,
		traffic.end_time,
		traffic.traffic_date,
		traffic.traffic_in,
		traffic.traffic_out,
		traffic.country_code
	FROM
		{db_env}_NAP_BASE_VWS.STORE_TRAFFIC_FACT traffic
		INNER JOIN (
			SELECT
				register_map.store_num,
				register_map.measurement_location_id,
				register_map.measurement_location_name
			FROM
				{db_env}_NAP_BASE_VWS.STORE_TRAFFIC_LOCATION_REGISTER register_map
			GROUP BY
				register_map.measurement_location_id,
				register_map.store_num,
				register_map.measurement_location_name ) register_map ON
		register_map.store_num = traffic.store_num
		AND register_map.measurement_location_id = traffic.measurement_location_id
	WHERE traffic.traffic_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT')
) traffic ON
	traffic.store_num = transactions.ringing_store_num
	AND traffic.measurement_location_id = transactions.measurement_location_id
	AND traffic.start_time = transactions.start_time
WHERE traffic.traffic_date BETWEEN (SELECT CAST (CURR_BATCH_DATE AS DATE) - INTERVAL '37' DAY FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT') AND (SELECT CURR_BATCH_DATE FROM {db_env}_NAP_UTL.ELT_CONTROL WHERE SUBJECT_AREA_NM = 'RETAIL_NEXT');


ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;
