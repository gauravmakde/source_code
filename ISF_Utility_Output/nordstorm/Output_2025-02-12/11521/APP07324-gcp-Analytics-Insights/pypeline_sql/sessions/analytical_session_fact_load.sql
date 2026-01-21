-- Description: analytical session fact table is used to create a list of session ids, whether they had an arrived event or
--              didn't. If they had an arrived event, get the URL information so that downstream jobs can parse url for marketing attribution
--              uses logic from existing job that sources from teradata instead of the object layer 
--              https://gitlab.nordstrom.com/nordace/napbi/-/blob/master/sql/teradata/analytical_session_fact.sql
-- Analyst team: Operational Insights
-- Modified SQL: 2024-10-24 Kerri Mill

create table if not exists {hive_schema}.analytical_session_fact
(
    activity_date_pacific   date
    , session_id            string
    , channelcountry        string
    , channel               string
    , experience            string
    , referrer              string
    , requestedDestination  string
    , actualDestination     string
    , session_type          string
    , first_arrived_event   decimal(38,0)
    , last_arrived_event    decimal(38,0)
	, dw_sys_load_timestamp timestamp
	, active_session_flag   INTEGER
	, deterministic_bot_flag  INTEGER
	, sus_bot_flag            INTEGER
	, bot_demand_flag         INTEGER
)
using ORC
location 's3://{s3_bucket_root_var}/analytical_session_fact/'
partitioned by (activity_date_pacific);


-- for each session, get fields for whether the event is arrived or search/browse, then rank accoring to order of events happening
create or replace temporary view all_events as
(
SELECT
	activity_date,
	session_id,
	event_name,
	event_id,
	event_time_utc,
	channelcountry,
	channel,
	experience,
	requesteddestination,
    actualdestination,
	referrer,
	CASE WHEN event_name = 'com.nordstrom.customer.Arrived' THEN 1 ELSE 0 END AS arrived_events,
	CASE WHEN event_name IN ('com.nordstrom.customer.event.BrowseSearchResultsGenerated','com.nordstrom.customer.event. KeywordSearchResultsGenerated')
		THEN 1 ELSE 0
				END AS sbn_events,
	ROW_NUMBER() OVER (PARTITION BY session_id, event_name ORDER BY event_time_utc ASC, case when requesteddestination is not null and requesteddestination <> '' then 1 
		when referrer is not null then 2 else 3 end ASC) AS first_arrived_event,
    ROW_NUMBER() OVER (PARTITION BY session_id, event_name ORDER BY event_time_utc DESC, case when requesteddestination is not null and requesteddestination <> '' then 1 
		when referrer is not null then 2 else 3 end ASC) AS last_arrived_event

FROM acp_vector.customer_digital_session_evt_fact

WHERE activity_date BETWEEN {start_date} and {end_date}
);

-- aggregate count of events for each session id
create or replace temporary view all_events_agg as
(
SELECT
	activity_date,
	session_id,
	channelcountry,
	channel,
	experience,
	sum(cast(trim(arrived_events) as int)) AS arrived_events,
	sum(cast(trim(sbn_events) as int)) AS sbn_events,
	count(DISTINCT event_id)  AS all_events

FROM all_events

WHERE 1=1

GROUP BY
		activity_date,
		session_id,
		channelcountry,
		channel,
		experience
);

-- only select the first and last arrived events
create or replace temporary view arrived_events as
(
SELECT
	activity_date,
	session_id,
	channelcountry,
	channel,
	experience,
	referrer,
	requesteddestination,
    actualdestination,
    'ARRIVED' AS session_type,
    cast(first_arrived_event as decimal(38, 0)) AS first_arrived_event,
    cast(last_arrived_event as decimal(38, 0)) AS last_arrived_event

FROM all_events

WHERE 1=1
	AND event_name = 'com.nordstrom.customer.Arrived'
	AND (first_arrived_event = 1 OR last_arrived_event = 1)
);

-- get a list of sessions that have no arrived events
create or replace temporary view non_arrived_sessions as
(
SELECT
	activity_date,
	session_id,
	channelcountry,
	channel,
	experience,
	'NULL' AS referrer,
	'NULL' AS requesteddestination,
    'NULL' AS actualdestination,
	'NON_ARRIVED' AS  session_type,
    0 AS first_arrived_event,
    0 AS last_arrived_event

FROM all_events_agg

WHERE 1=1
	AND arrived_events = 0
	AND sbn_events <> all_events
);

-- union together list of sessions with and without arrived events
create or replace temporary view final_session_table as
(
SELECT
	session_id,
	channelcountry,
	channel,
	experience,
	referrer,
	requesteddestination,
    actualdestination,
	CAST(session_type AS VARCHAR(25)) AS session_type,
    first_arrived_event,
    last_arrived_event,
    activity_date as activity_date_pacific

FROM arrived_events

UNION ALL

SELECT
	session_id,
	channelcountry,
	channel,
	experience,
	referrer,
	requesteddestination,
    actualdestination,
	session_type,
	cast(first_arrived_event as decimal(38, 0)) AS first_arrived_event,
    cast(last_arrived_event as decimal(38, 0)) AS last_arrived_event,
    activity_date as activity_date_pacific

FROM non_arrived_sessions
);

create or replace temporary view final_session_table_with_tstamp as  
select 
		a.session_id
        , a.channelcountry
        , a.channel
		, a.experience
        , a.referrer
        , a.requesteddestination
        , a.actualdestination
        , a.session_type
        , a.first_arrived_event
        , a.last_arrived_event
		, CURRENT_TIMESTAMP as dw_sys_load_timestamp
		, case when c.session_id is null then 0 else 1 end as active_session_flag
        , COALESCE(b.deterministic_bot_flag, 0) as deterministic_bot_flag
        , COALESCE(b.sus_bot_flag, 0) as sus_bot_flag
        , COALESCE(b.demand_flag, 0) as bot_demand_flag
        , a.activity_date_pacific
from final_session_table a
left join (select activity_date_partition, session_id from acp_event_intermediate.session_fact_attributes_parquet where activity_date_partition BETWEEN {start_date} and {end_date}) c
on a.session_id = c.session_id
and a.activity_date_pacific = c.activity_date_partition
left join (select * from acp_event_intermediate.customer_digital_session_bot_fact_parquet where activity_date_partition BETWEEN {start_date} and {end_date}) b
on a.activity_date_pacific = b.activity_date_partition
and a.session_id = b.session_id;


-- replace records from the days that the job is running
insert overwrite table {hive_schema}.analytical_session_fact partition (activity_date_pacific)
select /*+ REPARTITION(100) */ * from final_session_table_with_tstamp;

MSCK REPAIR TABLE {hive_schema}.analytical_session_fact;




