-- pipeline job for daily customer interactions (add to bag and wishlists)  
-- This data powers the personalization engine project 
-- code owner Kunal Sonalkar
-- APPID: app-09439

create table if not exists ace_etl.perso_engine_atbwl_v2(
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
location 's3://mlp-data-bucket/perso_engine_atbwl_v2'
partitioned by (interaction_date); 

create or replace temporary view output as
 
select
a.event_time_pst,
a.cust_id,
a.session_id,
a.event_id,
a.actiontype,
a.RMSSKU,
b.Styleid,
a.activity_date_partition as interaction_date
        from 
        (
        select distinct event_time_pst,cust_id,session_id,event_id,case when lower(element_id)  like '%addtobag%' then 'ATB' else 'ATWL' end as actiontype,
        digitalcontents_id as RMSSKU, activity_date_partition
        from acp_event_intermediate.session_evt_expanded_attributes_parquet
        where activity_date_partition between '2024-03-01' and '2024-10-17'
        and ( lower(element_id)  like '%addtobag%' or lower(element_id)  like '%addtowishlist%')
        and event_name = 'com.nordstrom.event.customer.Engaged'
        and digitalcontents_type ='RMSSKU'
        and channel = 'NORDSTROM'
        ) a
        left join 
        (
        select distinct event_id,
        digitalcontents_id as Styleid
        from acp_event_intermediate.session_evt_expanded_attributes_parquet
        where activity_date_partition between '2024-03-01' and '2024-10-17'
        and ( lower(element_id)  like '%addtobag%' or lower(element_id)  like '%addtowishlist%')
        and event_name = 'com.nordstrom.event.customer.Engaged'
        and digitalcontents_type = 'STYLE'
        and channel = 'NORDSTROM'
        ) b 
        on a.event_id = b.event_id
        where b.Styleid is not NULL;

insert OVERWRITE TABLE ace_etl.perso_engine_atbwl_v2 PARTITION (interaction_date)
select * from output;

