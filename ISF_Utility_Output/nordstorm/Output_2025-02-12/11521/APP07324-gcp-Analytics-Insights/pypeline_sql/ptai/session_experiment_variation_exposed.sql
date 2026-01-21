create table if not exists {hive_schema}.session_experiment_variation_exposed (
	cust_id string,
	bucketingid string,
	acp_id string,
	session_id string,
	channel string,
	channelcountry string,
	experience string,
	projectname string,
	variationname string,
	min_exposure_time_utc timestamp,
	min_exposure_time_pst timestamp,
	activity_date_pst date,
	experimentname string
)
using PARQUET
location "s3://{s3_bucket_root_var}/session_experiment_variation_exposed"
partitioned by (activity_date_pst,experimentname);

MSCK REPAIR table {hive_schema}.session_experiment_variation_exposed;

-- Pull basic experiment data from AVRO table daily
create or replace temporary view base_data as 
select 
	shopperid,
	bucketingid,
	case 
		when channel = 'FULL_LINE' then 'NORDSTROM'
		when channel = 'RACK' then 'NORDSTROM_RACK'
		else channel
	end as channel,
	channelcountry,
	case
		when platform = 'MOW' then 'MOBILE_WEB'
		when platform = 'WEB' then 'DESKTOP_WEB'
		when platform = 'IOS' then 'IOS_APP'
		when platform = 'ANDROID' then 'ANDROID_APP'
	end as platform,
	projectname,
	lower(experimentname) as experimentname,
	lower(variationname) as variationname,
	event_time_utc,
	from_utc_timestamp(event_time_utc, 'US/Pacific') event_time_pst
from
	(
	select
	    value.customer.id as shopperid,
	    value.bucketingid,
	    value.source.channel,
	    value.source.channelcountry,
	    value.source.platform,
	    value.projectname,
	    value.experimentname,
	    value.variationname,
		cast(from_unixtime(cast(cast(headers ['SystemTime'] as string) as BIGINT) * 0.001) as timestamp) as event_time_utc
	from acp_event.customer_experiment_variation_exposed_raw_parquet
	where
	    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} -1 and {end_date} +1
	and length(value.bucketingid) = 36
	and value.bucketingid <> 'forced'
	and value.bucketingid <> 'leapfrogAll'
	and value.experimentname not like '%_leapfrog_holdout'
	and value.projectname in ('FULL_LINE_MOBILE', 'FULL_LINE_BACKEND_SERVICE', 'IOS_APP', 'FULL_LINE_DESKTOP', 'ANDROID_APP', 'DESKTOP', 'JWN')
        and cast(headers['Nord-Load'] as string) is null
        and cast(headers['nord-load'] as string) is null
        and cast(headers['Nord-Test'] as string) is null
        and cast(headers['nord-test'] as string) is null
        and cast(headers['Sretest'] as string) is null
        and coalesce(cast(headers['identified-bot'] as string),'XXX') <> 'True'
	        )
;

-- Lots of bad experimentnames that have just 1 shopper, remove them to clean the output up
create or replace temporary view bad_experimentnames as 
select experimentname
from base_data
group by 1 
having count(distinct shopperid) = 1
;

-- Create a sessions table and fix issue with session_user_lookup timestamp
-- Create a buffer timestamp because sometimes experiment occurs before or after session start/end time 
create or replace temporary view sessions as 
select
	a.channel,
	a.experience,
	a.session_id,
	a.cust_id,
	a.acp_id,
	a.activity_date_partition,
    b.session_starttime_pst,
	b.session_endtime_pst,
	b.session_starttime_pst - interval '5' minute as session_starttime_pst_buffer,
	b.session_endtime_pst + interval '5' minute as session_endtime_pst_buffer
from
	(
	select 
	    distinct 
		session_channel as channel,
		session_experience as experience,
		session_id,
		shopper_id as cust_id,
		acp_id,
		activity_date_partition
	from acp_event_intermediate.session_user_lookup_parquet
	where activity_date_partition between {start_date} and {end_date}
	group by 1,2,3,4,5,6
	) a 
	inner join
	(
	select 
		session_id,
		channel,
		experience,
		min(from_utc_timestamp(session_starttime_utc, 'US/Pacific')) as session_starttime_pst,
		max(from_utc_timestamp(session_endtime_utc, 'US/Pacific')) as session_endtime_pst
	from acp_vector.customer_session_fact
	where activity_date_partition between {start_date} and {end_date}
	group by 1,2,3
	) b
on a.session_id = b.session_id
and a.channel = b.channel
and a.experience = b.experience
;

-- First try to join with no buffer, this gets about 98% of the data 
create or replace temporary view no_buffer as 
select *
from (
	select  
		base.shopperid,
		base.bucketingid,
		sess.acp_id,
		base.channel,
		base.channelcountry,
		base.platform,
		base.projectname,
		base.experimentname,
		base.variationname,
		base.event_time_utc,
		base.event_time_pst,
		CAST(base.event_time_pst as date) as activity_date_pst,
		min(session_id) as min_session_id,
		max(session_id) as max_session_id
	from base_data base 
	left join sessions sess
	on base.shopperid = sess.cust_id
	and base.channel = sess.channel
	and base.platform = sess.experience
	and base.event_time_pst > sess.session_starttime_pst
	and base.event_time_pst < sess.session_endtime_pst
	and CAST(base.event_time_pst as date) = sess.activity_date_partition
	where base.experimentname not in 
							(
							select *
							from bad_experimentnames
							)
	group by 1,2,3,4,5,6,7,8,9,10,11,12
) 
where activity_date_pst between {start_date} and {end_date}
;

-- Try again with the buffer, this gets about 1.8% of the data 
create or replace temporary view buffer as 
select 
	a.shopperid,
	a.bucketingid,
	sess.acp_id,
	a.channel,
	a.channelcountry,
	a.platform,
	a.projectname,
	a.experimentname,
	a.variationname,
	a.event_time_utc,
	a.event_time_pst,
	a.activity_date_pst,
	min(sess.session_id) as min_session_id,
	max(sess.session_id) as max_session_id
from no_buffer a
left join sessions sess
	on a.shopperid = sess.cust_id
	and a.channel = sess.channel
	and a.platform = sess.experience
	and a.event_time_pst > sess.session_starttime_pst_buffer
	and a.event_time_pst < sess.session_endtime_pst_buffer
	and CAST(a.event_time_pst as date) = sess.activity_date_partition
where a.min_session_id is null 
group by 1,2,3,4,5,6,7,8,9,10,11,12
;

-- Create final view and use coalesce to pull max session to fix the final 0.02%
-- That final 0.02% is somewhat of a guess because we don't know how to handle outliers in this scenario
create or replace temporary view experiment_sessions as 
select 
	shopperid as cust_id,
	bucketingid,
	acp_id,
	coalesce(max_session_id, null) as session_id,
	channel,
	channelcountry,
	platform as experience,
	projectname,
	variationname,
	min(event_time_utc) as min_exposure_time_utc,
	min(event_time_pst) as min_exposure_time_pst,
	activity_date_pst,
	experimentname
from 
	(
	select * from no_buffer where min_session_id is not null
	union all 
	select * from buffer
	)
group by 1,2,3,4,5,6,7,8,9,12,13
;

insert
    OVERWRITE TABLE {hive_schema}.session_experiment_variation_exposed PARTITION (activity_date_pst,experimentname)
select
	/*+ REPARTITION(100) */
    *
from
    experiment_sessions
where cust_id not RLIKE '[^A-Za-z0-9-_]'
and bucketingid not RLIKE '[^A-Za-z0-9-_]'
and experimentname not RLIKE '[^A-Za-z0-9-_]'
and variationname not RLIKE '[^A-Za-z0-9-_]'
and length(cust_id) <= 36
and length(bucketingid) <= 36
;

create table if not exists {hive_schema}.session_experiment_variation_orders (
	session_id string,
	channel string, 
	channelcountry string,
	experience string, 
	projectname string, 
	experimentname string,
	variationname string, 
	min_exposure_time_utc timestamp, 
	min_exposure_time_pst timestamp, 
	ordernumber string,
	orderlineid string,
	orderlinenumber string,
	order_rmssku_id string,
	order_style_id string,
	order_event_time_pst timestamp,
    activity_date_pst date
)
using PARQUET
location "s3://{s3_bucket_root_var}/session_experiment_variation_orders"
partitioned by (activity_date_pst);

MSCK REPAIR table {hive_schema}.session_experiment_variation_orders;

-- Pull orders information and join with sessions to get sessionID
create or replace temporary view customer_orders as 
select 
	ord.cust_id,
	sess.session_id,
	ord.event_id,
	ord.ordernumber,
	ord.orderlineid,
	ord.orderlinenumber,
	ord.order_rmssku_id,
	ord.order_style_id,
	ord.channel,
	ord.channelcountry,
	ord.experience,
	min_event_time_utc,
	from_utc_timestamp(min_event_time_utc, 'US/Pacific') order_event_time_pst,
	cast(from_utc_timestamp(min_event_time_utc, 'US/Pacific')as date) as activity_date_pst
from (
	select 
		customer.id as cust_id,
		cast(headers ['Id'] AS string) as event_id,
	    invoice.ordernumber as ordernumber,
	    i.orderLineId as orderlineid,
	    i.orderLineNumber as orderlinenumber,
	    i.product.id AS order_rmssku_id,
	    i.productStyle.id AS order_style_id,
		case 
			when source.channel = 'FULL_LINE' then 'NORDSTROM'
			when source.channel = 'RACK' then 'NORDSTROM_RACK'
		end as channel,
		source.channelcountry,
		case
			when source.platform = 'MOW' then 'MOBILE_WEB'
			when source.platform = 'WEB' then 'DESKTOP_WEB'
			when source.platform = 'IOS' then 'IOS_APP'
			when source.platform = 'ANDROID' then 'ANDROID_APP'
		end as experience,
		min(cast(from_unixtime(cast(cast(headers ['SystemTime'] as string) as BIGINT) * 0.001) as timestamp)) as min_event_time_utc
	from acp_event_view.customer_activity_order_submitted LATERAL VIEW OUTER explode(items) as i
	where
    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} -1 and {end_date} +1
	and source.channelcountry ='US'
	group by 1,2,3,4,5,6,7,8,9,10
	) ord
inner join acp_vector.customer_digital_session_evt_fact sess
on ord.event_id = sess.event_id
where sess.activity_date_partition between {start_date} and {end_date}
and sess.event_name = 'com.nordstrom.customer.OrderSubmitted'
;

-- Pull min and max exposure timestamps for all experiments that a session was in 
create or replace temporary view sessions_exp as 
select 
	session_id,
	channel,
	channelcountry,
	experience,
	projectname,
	variationname,
	min(min_exposure_time_utc) as min_exposure_time_utc,
	min(min_exposure_time_pst) as min_exposure_time_pst,
	activity_date_pst,
	experimentname
from ace_etl.session_experiment_variation_exposed
where activity_date_pst between {start_date} and {end_date}
group by 1,2,3,4,5,6,9,10
;

-- Join orders to experiments where the order occurred after the minimum exposure timestamp
create or replace temporary view experiment_orders as 
select
	a.session_id,
	a.channel, 
	a.channelcountry,
	a.experience, 
	a.projectname,
	a.experimentname,
	a.variationname, 
	a.min_exposure_time_utc, 
	a.min_exposure_time_pst, 
	b.ordernumber,
	b.orderlineid,
	b.orderlinenumber,
	b.order_rmssku_id,
	b.order_style_id,
	b.order_event_time_pst,
	a.activity_date_pst
from	
(
select * 
from sessions_exp
where activity_date_pst between {start_date} and {end_date}
) a 
inner join
(
select * 
from customer_orders
) b	
on a.channel = b.channel
and a.experience = b.experience
and a.session_id = b.session_id
and a.activity_date_pst = b.activity_date_pst
and a.min_exposure_time_pst <= b.order_event_time_pst + interval '1' second
;

insert
    OVERWRITE TABLE {hive_schema}.session_experiment_variation_orders PARTITION (activity_date_pst)
select
	/*+ REPARTITION(100) */
    *
from
    experiment_orders;