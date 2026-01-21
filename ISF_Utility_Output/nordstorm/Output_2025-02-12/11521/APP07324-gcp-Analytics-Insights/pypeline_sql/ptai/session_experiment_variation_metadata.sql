create table if not exists {hive_schema}.session_experiment_variation_metadata (
    channel string,
	channelcountry string,
	experience string,
	projectname string,
	variationname string,
    first_activity_date_pst date,
    last_activity_date_pst date,
	total_unique_cust_id bigint, 
	total_unique_bucketingid bigint,
	total_unique_acp_id bigint,
	total_unique_sessions bigint,
	experimentname string
)
using PARQUET
location "s3://{s3_bucket_root_var}/session_experiment_variation_metadata"
partitioned by (experimentname);

MSCK REPAIR table {hive_schema}.session_experiment_variation_metadata;

-- Pull all running experiments 
create or replace temporary view running_experiments as 
select 
    distinct experimentname
from ace_etl.session_experiment_variation_exposed 
where activity_date_pst between {start_date} and {end_date}
;

-- Find the date that the PdM turned the experiment off or moved full population to 1 arm
create or replace temporary view stop_date as 
select 
    channel,
	channelcountry,
	experience,
	projectname,
	experimentname,
	min(last_activity_date_pst) as stop_date
from 
    (
    select
        channel,
	    channelcountry,
	    experience,
	    projectname,
	    experimentname,
	    variationname,
        min(activity_date_pst) as first_activity_date_pst,
        max(activity_date_pst) as last_activity_date_pst
    from ace_etl.session_experiment_variation_exposed
    where experimentname in (select * from running_experiments)
    group by 1,2,3,4,5,6
    )
group by 1,2,3,4,5
;

-- Pull metrics based on when the experiment was turned off 
create or replace temporary view metadata as 
select
    a.channel,
	a.channelcountry,
	a.experience,
	a.projectname,
	a.variationname,
    min(a.activity_date_pst) as first_activity_date_pst,
    max(b.stop_date) as last_activity_date_pst,
	count(distinct a.cust_id) as total_unique_cust_id, 
	count(distinct a.bucketingid) as total_unique_bucketingid,
	count(distinct a.acp_id) as total_unique_acp_id,
	count(distinct a.session_id) as total_unique_sessions,
	a.experimentname
from ace_etl.session_experiment_variation_exposed a
inner join stop_date b
on a.channel = b.channel
and a.channelcountry = b.channelcountry
and a.experience = b.experience
and a.projectname = b.projectname
and a.experimentname = b.experimentname
where a.experimentname in (select * from running_experiments)
and a.activity_date_pst <= b.stop_date
group by 1,2,3,4,5,12
;

insert
    OVERWRITE TABLE {hive_schema}.session_experiment_variation_metadata PARTITION (experimentname)
select
    *
from
    metadata;