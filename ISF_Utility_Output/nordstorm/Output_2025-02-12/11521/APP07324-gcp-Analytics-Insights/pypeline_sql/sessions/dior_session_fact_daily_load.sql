SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=dior_sessions_11521_ACE_ENG;
     Task_Name=dior_session_fact_daily_load;'
     FOR SESSION VOLATILE;


-- Definine new Hive table for output  
create table if not exists {hive_schema}.dior_session_fact_daily 
(
      activity_date_pacific date
    , channelcountry VARCHAR(8)
    , channel VARCHAR(32)
    , experience VARCHAR(32)
    , mrkt_type VARCHAR(50)
    , finance_rollup VARCHAR(50)
    , finance_detail VARCHAR(50)
    , recognized_flag INTEGER
    , guest_flag INTEGER
    , authenticated_flag INTEGER
    , shopper_ids INTEGER
    , acp_ids INTEGER
    , session_duration_seconds BIGINT
    , bounce_flag INTEGER
    , sessions INTEGER
    , product_views INTEGER
    , cart_adds INTEGER
    , web_orders INTEGER
    , web_ordered_units INTEGER
    , web_demand_usd DECIMAL(15, 2)
    , web_demand DECIMAL(15, 2)
    , web_demand_currency_code CHAR(3)
    , oms_orders INTEGER
    , oms_ordered_units DECIMAL(15, 2)
    , oms_demand_usd DECIMAL(15, 2)
    , oms_demand DECIMAL(15, 2)
    , oms_demand_currency_code CHAR(3)
    , bopus_orders INTEGER
    , bopus_ordered_units INTEGER
    , bopus_demand_usd DECIMAL(15, 2)
    , bopus_demand DECIMAL(15, 2)
    , bopus_demand_currency_code CHAR(3)
    , product_view_session INTEGER
    , cart_add_session INTEGER
    , web_order_session INTEGER
    , oms_order_session INTEGER
    , bopus_order_session INTEGER
    , visited_homepage_session INTEGER
    , visited_checkout_session INTEGER 
    , searched_session INTEGER
    , browsed_session INTEGER
    , first_page_type VARCHAR(200)
    , last_page_type VARCHAR(200)
    , dw_batch_date DATE NOT NULL
    , dw_sys_load_tmstp TIMESTAMP NOT NULL
    , active_session_flag  INTEGER
    , deterministic_bot_flag  INTEGER
    , sus_bot_flag  INTEGER
    , bot_demand_flag  INTEGER
)
using ORC
location 's3://{s3_bucket_root_var}/dior_session_fact_daily/'
partitioned by (activity_date_pacific);



-- Reading data from upstream Hive table and aggregating across dimensions
create or replace temporary view dior_session_daily as
(
select active_session_flag
    , channelcountry
    , channel
    , experience
    , mrkt_type
    , finance_rollup
    , finance_detail
    , recognized_flag
    , guest_flag
    , authenticated_flag
    , count(distinct shopper_id) as shopper_ids
    , count(distinct acp_id) as acp_ids
    , sum(session_duration_seconds) as session_duration_seconds
    , bounce_flag
    , count(distinct session_id) as sessions
    , sum(product_views) as product_views
    , sum(cart_adds) as cart_adds
    , sum(web_orders) as web_orders
    , sum(web_ordered_units) as web_ordered_units
    , sum(web_demand_usd) as web_demand_usd 
    , sum(web_demand) as web_demand 
    , web_demand_currency_code 
    , sum(oms_orders) as oms_orders 
    , sum(oms_ordered_units) as oms_ordered_units
    , sum(oms_demand_usd) as oms_demand_usd
    , sum(oms_demand) as oms_demand
    , oms_demand_currency_code
    , sum(bopus_orders) as bopus_orders
    , sum(bopus_ordered_units) as bopus_ordered_units
    , sum(bopus_demand_usd) as bopus_demand_usd
    , sum(bopus_demand) as bopus_demand
    , bopus_demand_currency_code
    , product_view_session
    , cart_add_session
    , web_order_session
    , oms_order_session
    , bopus_order_session
    , visited_homepage_session
    , visited_checkout_session
    , searched_session
    , browsed_session
    , first_page_type
    , last_page_type
    , CURRENT_DATE() as dw_batch_date
    , CURRENT_TIMESTAMP() as dw_sys_load_tmstp
    , deterministic_bot_flag
    , sus_bot_flag
    , bot_demand_flag
    , activity_date_pacific
from {hive_schema}.dior_session_fact
where activity_date_pacific BETWEEN {start_date} and {end_date}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 22, 27, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49
);

-- Writing output to new Hive table
insert overwrite table {hive_schema}.dior_session_fact_daily partition (activity_date_pacific)
select /*+ REPARTITION(100) */
    channelcountry               
    , channel                       
    , experience                   
    , mrkt_type                     
    , finance_rollup                
    , finance_detail                
    , recognized_flag            
    , guest_flag                   
    , authenticated_flag            
    , shopper_ids                   
    , acp_ids                    
    , session_duration_seconds     
    , bounce_flag
    , sessions                    
    , product_views                
    , cart_adds                    
    , web_orders                    
    , web_ordered_units             
    , web_demand_usd               
    , web_demand                    
    , web_demand_currency_code      
    , oms_orders                   
    , oms_ordered_units            
    , oms_demand_usd               
    , oms_demand                   
    , oms_demand_currency_code      
    , bopus_orders                 
    , bopus_ordered_units          
    , bopus_demand_usd             
    , bopus_demand                  
    , bopus_demand_currency_code    
    , product_view_session          
    , cart_add_session              
    , web_order_session             
    , oms_order_session             
    , bopus_order_session           
    , visited_homepage_session      
    , visited_checkout_session     
    , searched_session              
    , browsed_session             
    , first_page_type              
    , last_page_type                
    , dw_batch_date                 
    , dw_sys_load_tmstp
    , active_session_flag	
    , deterministic_bot_flag
    , sus_bot_flag
    , bot_demand_flag
    , activity_date_pacific        
from dior_session_daily
;

--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
msck repair table {hive_schema}.dior_session_fact_daily;

-- Writing output to teradata landing table.  
insert overwrite table dior_session_fact_daily_ldg_output
select 
	activity_date_pacific        
    , sessions                     
    , channelcountry                
    , channel                       
    , experience                   
    , mrkt_type                     
    , finance_rollup                
    , finance_detail                
    , recognized_flag               
    , guest_flag                   
    , authenticated_flag            
    , shopper_ids                 
    , acp_ids                      
    , session_duration_seconds     
    , bounce_flag                   
    , product_views                 
    , cart_adds                    
    , web_orders                    
    , web_ordered_units             
    , web_demand_usd                
    , web_demand                   
    , web_demand_currency_code      
    , oms_orders                    
    , oms_ordered_units             
    , oms_demand_usd                
    , oms_demand                    
    , oms_demand_currency_code      
    , bopus_orders                  
    , bopus_ordered_units          
    , bopus_demand_usd              
    , bopus_demand                 
    , bopus_demand_currency_code    
    , product_view_session          
    , cart_add_session             
    , web_order_session             
    , oms_order_session             
    , bopus_order_session           
    , visited_homepage_session      
    , visited_checkout_session      
    , searched_session              
    , browsed_session               
    , first_page_type               
    , last_page_type               
    , dw_batch_date                
    , dw_sys_load_tmstp
    , active_session_flag
    , deterministic_bot_flag
    , sus_bot_flag
    , bot_demand_flag	
from {hive_schema}.dior_session_fact_daily where activity_date_pacific BETWEEN {start_date} and {end_date}
;




