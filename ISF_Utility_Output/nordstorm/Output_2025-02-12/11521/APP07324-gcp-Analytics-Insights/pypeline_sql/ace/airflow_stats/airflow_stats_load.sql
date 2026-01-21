create or replace temporary view dag_run
        (
        id integer, 
        dag_id string,
        execution_date timestamp,
        state string,
        run_id string,
        external_trigger boolean,
        conf string,
        end_date timestamp,
        start_date timestamp
        )
USING csv
OPTIONS (path "s3://{s3_bucket_root_var}/dsae_airflow_stats/dr_result.csv",
        sep ",",
        header "true");

-- remove conf - doesn't include anything

create or replace temporary view dag
        (
        dag_id string,
        is_paused boolean,
        is_subdag boolean,
        is_active boolean,
        last_scheduler_run timestamp,
        last_pickled timestamp,
        last_expired timestamp,
        scheduler_lock boolean,
        pickle_id integer,
        fileloc string,
        owners string,
        description string,
        default_view string,
        schedule_interval string
        )
USING csv
OPTIONS (path "s3://{s3_bucket_root_var}/dsae_airflow_stats/dag_result.csv",
        sep ",",
        header "true");

create or replace temporary view task_instances
        (
        task_id string,
        dag_id string,
        execution_date timestamp,
        start_date timestamp,
        end_date timestamp,
        duration float,
        state string,
        try_number integer,
        hostname string,
        unixname string,
        job_id integer,
        pool string,
        queue string,
        priority_weight integer,
        operator string,
        queued_dttm timestamp,
        pid integer, 
        max_tries string,
        executor_config string
        )
USING csv
OPTIONS (path "s3://{s3_bucket_root_var}/dsae_airflow_stats/ti_result.csv",
        sep ",",
        header "true");

-- remove executor_config

create or replace temporary view sla_miss
        (
task_id	string,
dag_id	string,
execution_date timestamp,	
email_sent boolean,
`timestamp` timestamp,
description string,
notification_sent boolean
        )
USING csv
OPTIONS (path "s3://{s3_bucket_root_var}/dsae_airflow_stats/sla_result.csv",
        sep ",",
        header "true");

-- TODO: Teradata


create table if not exists {hive_schema}.dag_run
(       id integer, 
        dag_id string,
        state string,
        run_id string,
        external_trigger boolean,
        start_date timestamp,
        end_date timestamp,
        execution_date date
)
USING PARQUET
location 's3://{s3_bucket_root_var}/dsae_airflow_stats_hive/dag_run/'
partitioned by (execution_date);

insert overwrite table {hive_schema}.dag_run partition (execution_date)
select /*+ REPARTITION(10) */
    id,
    dag_id,
    state,
    run_id,
    external_trigger,
    start_date,
    end_date,
    cast(execution_date as date) as execution_date
from dag_run;


create table if not exists {hive_schema}.dag
(       
        dag_id string,        
        is_paused boolean,
        is_subdag boolean,
        is_active boolean,
        last_scheduler_run timestamp,
        last_pickled timestamp,
        last_expired timestamp,
        scheduler_lock boolean,
        pickle_id integer,
        fileloc string,
        owners string,
        description string,
        default_view string,
        schedule_interval string
)
USING PARQUET
location 's3://{s3_bucket_root_var}/dsae_airflow_stats_hive/dag/';

insert overwrite table {hive_schema}.dag
select /*+ REPARTITION(10) */
        dag_id ,        
        is_paused ,
        is_subdag ,
        is_active ,
        last_scheduler_run ,
        last_pickled ,
        last_expired ,
        scheduler_lock ,
        pickle_id ,
        fileloc ,
        owners ,
        description ,
        default_view ,
        schedule_interval
from dag;

create table if not exists {hive_schema}.task_instances
(       
        task_id string,
        dag_id string ,
        start_date timestamp,
        end_date timestamp,
        duration float,
        state string,
        try_number integer,
        hostname string,
        unixname string,
        job_id integer,
        pool string,
        queue string,
        priority_weight integer,
        operator string,
        queued_dttm timestamp,
        pid integer, 
        max_tries string,
        execution_date date
)
USING PARQUET
location 's3://{s3_bucket_root_var}/dsae_airflow_stats_hive/task_instances/'
partitioned by (execution_date);

insert overwrite table {hive_schema}.task_instances partition (execution_date)
select /*+ REPARTITION(10) */
        task_id ,
        dag_id  ,
        start_date ,
        end_date ,
        duration ,
        state ,
        try_number ,
        hostname ,
        unixname ,
        job_id ,
        pool ,
        queue ,
        priority_weight ,
        operator ,
        queued_dttm ,
        pid , 
        max_tries ,
        cast(execution_date as date) as execution_date
from task_instances;

create table if not exists {hive_schema}.sla_miss
(       
    task_id	string,
    dag_id	string,
    email_sent boolean,
    sla_miss_timestamp timestamp,
    description string,
    notification_sent boolean,
    execution_date date
)
USING PARQUET
location 's3://{s3_bucket_root_var}/dsae_airflow_stats_hive/sla_miss/'
partitioned by (execution_date);

insert overwrite table {hive_schema}.sla_miss partition (execution_date)
select /*+ REPARTITION(10) */
    task_id	,
    dag_id	,
    email_sent ,
    `timestamp` as sla_miss_timestamp ,
    description ,
    notification_sent ,
    cast(execution_date as date) as execution_date
from sla_miss;