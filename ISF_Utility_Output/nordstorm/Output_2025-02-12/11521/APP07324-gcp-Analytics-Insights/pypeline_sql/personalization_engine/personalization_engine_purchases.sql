

create table if not exists {hive_schema}.perso_engine_purchases_v2(
	event_time_pst timestamp,
    cust_id string,
    session_id string,
    event_id string,
    actiontype string,
    RMSSKU string,
    Styleid string,
    interaction_date date
)
using PARQUET
location 's3://{perso_s3_bucket_root_var}/perso_engine_purchases_v2'
partitioned by (interaction_date); 

create or replace temporary view output as
 
select distinct event_time_pst,cust_id,session_id,event_id,'ONLINE_PURCHASE' as actiontype,
        order_rmssku_id as RMSSKU, order_style_id as Styleid, activity_date_partition as interaction_date
        from acp_event_intermediate.session_evt_expanded_attributes_parquet
        where activity_date_partition between {start_date} and {end_date}
        and event_name = 'com.nordstrom.customer.OrderSubmitted'
        and channel = 'NORDSTROM'
        and order_style_id is not null;

insert OVERWRITE TABLE {hive_schema}.perso_engine_purchases_v2 PARTITION (interaction_date)
select * from output;

MSCK REPAIR TABLE {hive_schema}.perso_engine_purchases_v2;

