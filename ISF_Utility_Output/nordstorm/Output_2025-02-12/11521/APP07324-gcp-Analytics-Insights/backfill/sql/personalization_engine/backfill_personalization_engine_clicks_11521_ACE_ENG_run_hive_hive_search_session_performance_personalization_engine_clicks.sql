-- pipeline job for daily customer interactions (clicks/product detail page views)  
-- This data powers the personalization engine project 
-- code owner Kunal Sonalkar
-- APPID: app-09439

create table if not exists ace_etl.perso_engine_clicks_v5(
	event_time_pst timestamp,
    cust_id string,
    session_id string,
    event_id string,
    actiontype string,
    Styleid string,
    interaction_date date
)
using PARQUET
location 's3://mlp-data-bucket/perso_engine_clicks_v5'
partitioned by (interaction_date); 

create or replace temporary view output as
 
select distinct 
        event_time_pst,
        cust_id,
        session_id,
        event_id, 
        'click' as actiontype,
        productstyle_id as Styleid,
        activity_date_partition as interaction_date
        from acp_event_intermediate.session_evt_expanded_attributes_parquet
        where 
        activity_date_partition between '2024-03-01' and '2024-10-16'
        and (event_name = 'com.nordstrom.event.customer.ProductSummarySelected')
        and productstyle_type ='WEB'
        and productstyle_id is not null
        and cust_id is not null
        and cust_id <> ''
        and channel = 'NORDSTROM'
        order by 
        cust_id,
 		event_time_pst,
 		session_id
;

insert OVERWRITE TABLE ace_etl.perso_engine_clicks_v5 PARTITION (interaction_date)
select * from output;
