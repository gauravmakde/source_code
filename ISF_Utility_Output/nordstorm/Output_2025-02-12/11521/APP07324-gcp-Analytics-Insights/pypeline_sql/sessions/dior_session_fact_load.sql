SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=dior_sessions_11521_ACE_ENG;
     Task_Name=dior_session_fact_load;'
     FOR SESSION VOLATILE;

-- Definine new Hive table for output      
create table if not exists {hive_schema}.dior_session_fact 
(
     activity_date_pacific date
    , session_id VARCHAR(100)
    , channelcountry VARCHAR(8)
    , channel VARCHAR(32)
    , experience VARCHAR(32)
    , mrkt_type VARCHAR(50)
    , finance_rollup VARCHAR(50)
    , finance_detail VARCHAR(50)
    , recognized_flag INTEGER
    , guest_flag INTEGER
    , authenticated_flag INTEGER
    , shopper_id VARCHAR(200)
    , acp_id VARCHAR(200)
    , session_duration_seconds BIGINT
    , bounce_flag INTEGER
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
    , first_page_instance_id VARCHAR(200)
    , last_page_instance_id VARCHAR(200)
    , first_page_type VARCHAR(16)
    , last_page_type VARCHAR(16)
    , dw_batch_date DATE NOT NULL
    , dw_sys_load_tmstp TIMESTAMP NOT NULL
    , active_session_flag INTEGER
    , deterministic_bot_flag  INTEGER
    , sus_bot_flag  INTEGER
    , bot_demand_flag  INTEGER
)
using ORC
location 's3://{s3_bucket_root_var}/dior_session_fact/'
partitioned by (activity_date_pacific);


-- Reading data from upstream Hive table. This step parses out referrer and utm channel for use in identifying finance defined marketing channel
create or replace temporary view utm_param as
(
SELECT DISTINCT
    activity_date_pacific,
    session_id,
	active_session_flag,
    deterministic_bot_flag,
    sus_bot_flag,
    bot_demand_flag,
    channelcountry,
    channel,
    experience,
    nullif(referrer,'') as referrer,
    nullif(url_destination,'') as url_destination,
    nullif(utm_channel,'') as utm_channel,
    nullif(session_type,'') as session_type
 FROM
 (   SELECT
        asf.activity_date_pacific,
        asf.session_id,
        asf.active_session_flag,
        asf.deterministic_bot_flag,
        asf.sus_bot_flag,
        asf.bot_demand_flag,
        asf.channelcountry,
        asf.channel,
        asf.experience,
        CASE
            WHEN LOWER(asf.referrer) LIKE '%google%' THEN 'google'
            WHEN LOWER(asf.referrer) LIKE '%bing%' THEN 'bing'
            WHEN LOWER(asf.referrer) LIKE '%yahoo%' THEN 'yahoo'
            WHEN LOWER(asf.referrer) LIKE '%facebook%'  THEN 'facebook'
            WHEN LOWER(asf.referrer) LIKE '%instagram%'  THEN 'instagram'
            WHEN LOWER(asf.referrer) LIKE '%pinterest%' THEN 'pinterest'
            WHEN LOWER(asf.referrer) LIKE '%twitter.com%' THEN 'twitter'
            ELSE asf.referrer
                END AS referrer,
        COALESCE(asf.requesteddestination,asf.actualdestination) AS url_destination,
        REPLACE(REGEXP_EXTRACT(COALESCE(asf.requesteddestination, asf.actualdestination), 'utm_channel=[^&]*',0), 'utm_channel=') AS utm_channel
        , asf.session_type

    FROM {hive_schema}.analytical_session_fact asf

    WHERE 1=1
        AND asf.first_arrived_event in (0,1)
        AND asf.activity_date_pacific BETWEEN {start_date} and {end_date}

) asf
);

-- uses logic to identify which marketing channel drove someone to their session
create or replace temporary view mkt_cat as
(
    SELECT
    utm_param.activity_date_pacific,
    utm_param.session_id,
	utm_param.active_session_flag,
    utm_param.deterministic_bot_flag,
    utm_param.sus_bot_flag,
    utm_param.bot_demand_flag,
    utm_param.channelcountry,
    utm_param.channel,
    utm_param.experience,

    CASE 
        WHEN utm_param.referrer = 'App-PushNotification' OR utm_param.utm_channel LIKE '%apppush%' THEN 'UNPAID'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.referrer IS NULL AND utm_param.utm_channel IS NULL THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer NOT IN ('google','bing','yahoo','facebook','instagram','pinterest',
                                                                'App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('google','bing','yahoo') THEN 'UNPAID'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('facebook','instagram','pinterest','twitter') THEN 'UNPAID'
        WHEN mch.marketing_type IS NULL THEN 'UNATTR/TIEOUT'
            ELSE mch.marketing_type
                END AS mrkt_type,

    CASE
        WHEN utm_param.referrer = 'App-PushNotification' OR utm_param.utm_channel LIKE '%apppush%' THEN 'EMAIL'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.referrer IS NULL AND utm_param.utm_channel IS NULL THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer NOT IN ('google','bing','yahoo','facebook','instagram','pinterest',
                                                                'App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('google','bing','yahoo') THEN 'SEO'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('facebook','instagram','pinterest','twitter') THEN 'UNPAID_OTHER'
        WHEN mch.finance_rollup IS NULL THEN 'UNATTR/TIEOUT'
            ELSE mch.finance_rollup
                END AS finance_rollup,

    CASE
        WHEN (utm_param.referrer = 'App-PushNotification' AND utm_param.utm_channel IS NULL OR utm_param.utm_channel = 'apppush_ret_p') 
        	OR utm_param.utm_channel = 'apppush_plan_ret_p"' THEN 'APP_PUSH_PLANNED'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.referrer IS NULL AND utm_param.utm_channel IS NULL THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer NOT IN ('google','bing','yahoo','facebook','instagram','pinterest',
                                                                'App-None','App-Launch','App-PushNotification','App-OpenURL','App-Foreground') THEN 'BASE'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('google','bing','yahoo') THEN 'SEO_SEARCH'
        WHEN utm_param.utm_channel IS NULL AND utm_param.referrer IN ('facebook','instagram','pinterest','twitter') THEN 'SOCIAL_ORGANIC'
        WHEN mch.finance_detail IS NULL then 'UNATTRIBUTED'
        ELSE mch.finance_detail
            END AS finance_detail


FROM utm_param

    LEFT JOIN ace_etl.utm_channel_lookup ucl
        ON lower(ucl.utm_mkt_chnl) = lower(utm_param.utm_channel)

    LEFT JOIN ace_etl.marketing_channel_hierarchy mch
        ON lower(mch.join_channel) = lower(ucl.bi_channel)
);

-- checks if a session authenticated
create or replace temporary view shopper_auth as
(
select
 s.activity_date
 , s.session_id
 , s.channelcountry
 , s.channel
 , s.experience
 , s.cust_id
 , min(s.rownum) as rownum
 , max(s.auth_flag) as auth_flag
from
(
select a.*
  , row_number() over (partition by activity_date, session_id order by event_time_utc desc) as rownum
  , max(case when event_name = 'com.nordstrom.customer.Authenticated' then 1 else 0 end) over (partition by activity_date, session_id) as auth_flag
from acp_vector.customer_session_evt_fact a
where activity_date BETWEEN {start_date} and {end_date}
)
 s
 group by 1, 2, 3, 4, 5, 6
 );

-- checks if a shopper id has an account
create or replace temporary view customer_tokenized_shopper_id
as
(
select o.entitlements.programs.web as shopper_id
from object_model.customer_tokenized o
where o.entitlements.programs.web is not null
group by 1
);

-- using logic from above tables to parse if the session was recognized, guest, or authenticated at any point during the session
create or replace temporary view session_auth_flags_shopperid as
(
select  grc.activity_date
  , grc.session_id
  , grc.channelcountry
  , grc.channel
  , grc.experience
  , grc.recognized_flag
  , grc.guest_flag
  , grc.auth_flag
  , s.shopper_id
  
from
(
select activity_date
  , session_id
  , channelcountry
  , channel
  , experience
  , max(case when o.shopper_id is not null then 1 else 0 end) as recognized_flag
  , max(case when o.shopper_id is null then 1 else 0 end) as guest_flag
  , max(a.auth_flag) as auth_flag
from shopper_auth a
left join customer_tokenized_shopper_id o on a.cust_id = o.shopper_id
where 1=1
group by 1, 2, 3, 4, 5
) grc
left join 
(
select activity_date
  , session_id
  , channelcountry
  , channel
  , experience
  , a.cust_id as shopper_id
from shopper_auth a
where a.rownum = 1
) s
on grc.activity_date = s.activity_date
and grc.session_id = s.session_id
and grc.channelcountry = s.channelcountry
and grc.channel = s.channel
and grc.experience = s.experience
);

-- bring in all metrics related to session
create or replace temporary view dior_session_detail 
    as
(
select 
    m.session_id,
    m.channelcountry,
    m.channel,
    m.experience,
    m.mrkt_type,
    m.finance_rollup,
    m.finance_detail,
    s.recognized_flag,
    s.guest_flag,
    s.auth_flag as authenticated_flag,
    s.shopper_id,
    x.acp_id
  , bigint(cast(sf.session_endtime_utc as timestamp)) - bigint(cast(sf.session_starttime_utc as timestamp)) as session_duration_seconds
  , case when bounce_ind = 'Y' then 1 else 0 end as bounce_flag
  , sf.product_views
  , sf.cart_adds
  , sf.web_orders
  , sf.web_ordered_units
  , sf.web_demand_usd
  , sf.web_demand
  , sf.web_demand_currency_code
  , sf.oms_orders
  , sf.oms_ordered_units
  , sf.oms_demand_usd
  , sf.oms_demand
  , sf.oms_demand_currency_code
  , sf.bopus_orders
  , sf.bopus_ordered_units
  , sf.bopus_demand_usd
  , sf.bopus_demand
  , sf.bopus_demand_currency_code
  , case when sf.product_views > 0 then 1 else 0 end as product_view_session
  , case when sf.cart_adds > 0 then 1 else 0 end as cart_add_session
  , case when sf.web_orders > 0 then 1 else 0 end as web_order_session
  , case when sf.oms_orders > 0 then 1 else 0 end as oms_order_session
  , case when sf.bopus_orders > 0 then 1 else 0 end as bopus_order_session
    , NULL as visited_homepage_session
    , NULL as visited_checkout_session
    , NULL as searched_session
    , NULL as browsed_session
    , NULL as first_page_instance_id
    , NULL as last_page_instance_id
    , NULL as first_page_type
    , NULL as last_page_type
    , CURRENT_DATE() as dw_batch_date
    , CURRENT_TIMESTAMp() as dw_sys_load_tmstp
	, m.active_session_flag
    , m.deterministic_bot_flag
    , m.sus_bot_flag
    , m.bot_demand_flag
    , m.activity_date_pacific
from mkt_cat m
left join session_auth_flags_shopperid s on m.activity_date_pacific = s.activity_date
    and s.session_id = m.session_id
    and s.channelcountry = m.channelcountry
    and s.channel = m.channel
    and s.experience = m.experience
left join acp_vector.customer_session_fact sf on m.activity_date_pacific = sf.activity_date
    and m.session_id = sf.session_id
    and m.channelcountry = sf.channelcountry
    and m.channel = sf.channel
    and m.experience = sf.experience
left join acp_vector.customer_session_xref x on x.session_id = s.session_id
    and x.activity_date = s.activity_date
    and s.shopper_id = x.shopper_id
);

-- bring in all the order & demand data that went into unknown session ID 
   -- For certain dates UNKNOWN session ID data is excluded because it's duplicating the data.

create or replace temporary view unknown_demand_data
  as
(
select 
    session_id,
    channelcountry, 
    channel, 
    experience, 
    NULL as mrkt_type, 
    NULL as finance_rollup, 
    NULL as finance_detail,
    NULL as recognized_flag,
    NULL as guest_flag,
    NULL as authenticated_flag,
    NULL as shopper_id,
    NULL as acp_id
  , NULL as session_duration_seconds
  , case when bounce_ind = 'Y' then 1 else 0 end as bounce_flag
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
  , NULL as product_view_session
  , NULL as cart_add_session
  , NULL as web_order_session
  , NULL as oms_order_session
  , NULL as bopus_order_session
    , NULL as visited_homepage_session
    , NULL as visited_checkout_session
    , NULL as searched_session
    , NULL as browsed_session
    , NULL as first_page_instance_id
    , NULL as last_page_instance_id
    , NULL as first_page_type
    , NULL as last_page_type
    , CURRENT_DATE() as dw_batch_date
    , CURRENT_TIMESTAMp() as dw_sys_load_tmstp 
	, NULL as active_session_flag
    , 0 as deterministic_bot_flag
    , 0 as sus_bot_flag
    , 0 as bot_demand_flag
    , activity_date as activity_date_pacific
from acp_vector.customer_session_fact 
where activity_date between {start_date} and {end_date} and activity_date not in (select distinct activity_date_pacific
from ace_etl.analytical_session_fact
where activity_date_pacific between date'2022-01-31' and date'2022-03-23' or activity_date_pacific IN (date'2022-05-15',
date'2022-05-16', date'2022-05-17', date'2022-06-21', date'2022-08-28', date'2022-08-29', date'2022-08-30', date'2022-08-31', 
date'2022-09-01', date'2022-09-02', date'2022-11-06', date'2022-11-07', date'2023-04-28', date'2023-05-02'))
and session_id ='UNKNOWN'
);


-- Writing output to new Hive table
insert overwrite table {hive_schema}.dior_session_fact partition (activity_date_pacific)
select /*+ REPARTITION(100) */
      session_id  	  
    , channelcountry               
    , channel                       
    , experience                    
    , mrkt_type                     
    , finance_rollup                
    , finance_detail                
    , recognized_flag              
    , guest_flag                    
    , authenticated_flag            
    , shopper_id                    
    , acp_id                        
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
    , first_page_instance_id        
    , last_page_instance_id         
    , first_page_type              
    , last_page_type              
    , dw_batch_date              
    , dw_sys_load_tmstp
    , active_session_flag 
    , deterministic_bot_flag
    , sus_bot_flag
    , bot_demand_flag           
    , activity_date_pacific         
from dior_session_detail
;

-- Writing UNKNOWN Session ID output to new Hive table
insert into table {hive_schema}.dior_session_fact partition (activity_date_pacific)
select /*+ REPARTITION(100) */
      session_id	  
    , channelcountry               
    , channel                       
    , experience                    
    , mrkt_type                     
    , finance_rollup                
    , finance_detail                
    , recognized_flag              
    , guest_flag                    
    , authenticated_flag            
    , shopper_id                    
    , acp_id                        
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
    , first_page_instance_id        
    , last_page_instance_id         
    , first_page_type              
    , last_page_type              
    , dw_batch_date              
    , dw_sys_load_tmstp
    , active_session_flag
    , deterministic_bot_flag
    , sus_bot_flag
    , bot_demand_flag 	
    , activity_date_pacific    
from unknown_demand_data
;


msck repair table {hive_schema}.dior_session_fact;

-- Writing output to teradata landing table.  
insert overwrite table dior_session_fact_ldg_output
select 
      activity_date_pacific          
    , session_id	
    , channelcountry                
    , channel                       
    , experience                    
    , mrkt_type                    
    , finance_rollup                
    , finance_detail                
    , recognized_flag               
    , guest_flag                    
    , authenticated_flag            
    , shopper_id                   
    , acp_id                       
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
    , first_page_instance_id        
    , last_page_instance_id         
    , first_page_type               
    , last_page_type                
    , dw_batch_date               
    , dw_sys_load_tmstp  
    , active_session_flag
    , deterministic_bot_flag
    , sus_bot_flag
    , bot_demand_flag 	
from {hive_schema}.dior_session_fact where activity_date_pacific BETWEEN {start_date} and {end_date}
;





