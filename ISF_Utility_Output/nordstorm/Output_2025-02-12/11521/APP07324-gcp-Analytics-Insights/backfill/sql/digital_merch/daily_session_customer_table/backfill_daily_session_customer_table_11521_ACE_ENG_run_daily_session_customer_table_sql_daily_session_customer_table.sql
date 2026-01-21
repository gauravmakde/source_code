-- Table Name: daily_session_customer_table
-- Team/Owner: CIA - Digital Customer Experience Analytics
-- Date Created/Modified: created on 08/12/2024, last modified on 09/19/2024
-- What is the update cadence/lookback window: refresh daily


-- DROP TABLE IF EXISTS ace_etl.daily_session_customer_table;
create table if not exists ace_etl.daily_session_customer_table (
	channel                         string
    ,experience                     string
    ,channelcountry                 string
    ,session_id                     string
    ,fiscal_day_num                 string
    ,week_idnt                      string
    ,month_idnt                     string
    ,quarter_idnt                   string
    ,fiscal_year_num                string
    ,landing_page                   string
    ,exit_page                      string
    ,mrkt_type                      string
    ,finance_rollup                 string
    ,finance_Detail                 string
    ,bounce_ind                     integer
    ,new_acp_id                     string
    ,loyalty_status                 string
    ,cardmember_flag                string
    ,engagement_cohort              string
    ,dma_desc                       string
    ,state_name                     string
    ,predicted_ct_segment           string
    ,age_group                      string
    ,lifestage                      string
    ,session_duration_seconds       numeric(38,3)
    ,total_pages_visited            integer
    ,total_unique_pages_visited     integer
    ,hp_sessions_flag               integer
    ,hp_engaged_sessions_flag       integer
    ,hp_views                       integer
    ,pdp_sessions_flag              integer
    ,pdp_engaged_sessions_flag      integer
    ,pdp_views                      integer
    ,unique_pdp_views               integer
    ,browse_sessions_flag           integer
    ,browse_engaged_sessions_flag   integer
    ,browse_views                   integer
    ,unique_browses                 integer
    ,search_sessions_flag           integer
    ,search_engaged_sessions_flag   integer
    ,search_views                   integer
    ,unique_searches                integer
    ,sbn_sessions_flag              integer
    ,sbn_engaged_sessions_flag      integer
    ,sbn_views                      integer
    ,wishlist_sessions_flag         integer
    ,wishlist_engaged_sessions_flag integer
    ,wishlist_views                 integer
    ,checkout_sessions_flag         integer
    ,checkout_engaged_sessions_flag integer
    ,checkout_views                 integer
    ,shopb_sessions_flag            integer
    ,shopb_engaged_sessions_flag    integer
    ,shopb_views                    integer
    ,atb_sessions_flag              integer
    ,atb_views                      integer
    ,atb_events                     integer
    ,atb_styles_count               integer
    ,atb_demand_usd                 numeric(20,2) 
    ,ordered_sessions_flag          integer
    ,sales_qty                      integer
    ,web_orders                     integer
    ,web_order_styles_count         integer
    ,web_demand_usd                 numeric(20,2)    
    ,web_order_discount_avg         numeric(20,2)    
    ,web_order_free_qty             integer
    ,marketplace_sessions_flag      integer
    ,under100_sessions_flag         integer
    ,num_prod_click                 integer
    ,num_mp_click                   integer
    ,num_nmp_click                  integer
    ,num_under100_click             integer
    ,unique_styles_clicks           integer
    ,unique_styles_mp_clicks        integer
    ,unique_styles_nmp_clicks       integer
    ,unique_styles_under100_clicks  integer
    ,num_full_pdv                   integer
    ,num_mp_full_pdv                integer
    ,num_nmp_full_pdv               integer
    ,num_under100_full_pdv          integer
    ,unique_styles_full_pdv         integer
    ,unique_styles_mp_full_pdv      integer
    ,unique_styles_nmp_full_pdv     integer
    ,unique_styles_under100_full_pdv integer
    ,num_prod_imp                   integer
    ,num_mp_imp                     integer
    ,num_nmp_imp                    integer
    ,num_under100_imp               integer
    ,unique_styles_imp              integer
    ,unique_styles_mp_imp           integer
    ,unique_styles_nmp_imp          integer
    ,unique_styles_under100_imp     integer
    ,num_mp_ATB                     integer
    ,num_nmp_ATB                    integer
    ,num_under100_ATB               integer
    ,unique_styles_ATB              integer
    ,unique_styles_mp_ATB           integer
    ,unique_styles_nmp_ATB          integer
    ,unique_styles_under100_ATB     integer
    ,ttl_mp_atb_demand              numeric(20,2)    
    ,ttl_nmp_atb_demand             numeric(20,2)    
    ,ttl_under100_atb_demand        numeric(20,2)    
    ,ttl_mp_order_qty               integer
    ,ttl_nmp_order_qty              integer
    ,ttl_under100_order_qty         integer
    ,unique_styles_ordered          integer
    ,unique_styles_mp_ordered       integer
    ,unique_styles_nmp_ordered      integer
    ,unique_styles_under100_ordered integer
    ,ttl_mp_ord_demand              numeric(20,2)    
    ,ttl_nmp_ord_demand             numeric(20,2)    
    ,ttl_under100_ord_demand        numeric(20,2)    
    ,dw_sys_load_tmstp              timestamp NOT NULL
    ,day_date                       date
) using PARQUET location 's3://ace-etl/daily_session_customer_table' partitioned by (day_date);
;
 
-- sync partitions on hive table
MSCK REPAIR TABLE ace_etl.daily_session_customer_table;

-- grab session level marketplace and under $100 engagement data
create or replace temporary view marketplace_under100_tbl as
    select PDFI.channel,
          PDFI.experience,
          PDFI.session_id,
          PDFI.activity_date_partition,

          sum(case when product_action = 'Click on Product' then 1 else 0 end)                                              as num_click,
          sum(case when product_action = 'Click on Product' and partner_relationship_type <> 'UNKNOWN-0' then 1 else 0 end) as num_mp_click,
          sum(case when product_action = 'Click on Product' and partner_relationship_type =  'UNKNOWN-0' then 1 else 0 end) as num_nmp_click,
          sum(case when product_action = 'Click on Product' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then 1 else 0 end)                 as num_under100_click,

          count(distinct case when product_action = 'Click on Product' then pdfi.product_style_id else null end)                                              as unique_styles_clicks,
          count(distinct case when product_action = 'Click on Product' and partner_relationship_type <> 'UNKNOWN-0' then pdfi.product_style_id else null end) as unique_styles_mp_clicks,
          count(distinct case when product_action = 'Click on Product' and partner_relationship_type =  'UNKNOWN-0' then pdfi.product_style_id else null end) as unique_styles_nmp_clicks,
          count(distinct case when product_action = 'Click on Product' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then pdfi.product_style_id else null end)                 as unique_styles_under100_clicks,

          sum(case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' then 1 else 0 end)                                                as num_full_pdv,
          sum(case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' and partner_relationship_type <> 'UNKNOWN-0' then 1 else 0 end)   as num_mp_full_pdv,
          sum(case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' and partner_relationship_type =  'UNKNOWN-0' then 1 else 0 end)   as num_nmp_full_pdv,
          sum(case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then 1 else 0 end)                   as num_under100_full_pdv,

          count(distinct case when product_action = 'View Product Detail' then pdfi.product_style_id else null end)                                                                                as unique_styles_full_pdv,
          count(distinct case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' and partner_relationship_type <> 'UNKNOWN-0' then pdfi.product_style_id else null end)   as unique_styles_mp_full_pdv,
          count(distinct case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' and partner_relationship_type =  'UNKNOWN-0' then pdfi.product_style_id else null end)   as unique_styles_nmp_full_pdv,
          count(distinct case when product_action = 'View Product Detail' and action_feature = 'FULL PDP' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then pdfi.product_style_id else null end)                   as unique_styles_under100_full_pdv,

          sum(case when product_action = 'See Product' then 1 else 0 end)                                              as num_imp,
          sum(case when product_action = 'See Product' and partner_relationship_type <> 'UNKNOWN-0' then 1 else 0 end) as num_mp_imp,
          sum(case when product_action = 'See Product' and partner_relationship_type =  'UNKNOWN-0' then 1 else 0 end) as num_nmp_imp,
          sum(case when product_action = 'See Product' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then 1 else 0 end)                 as num_under100_imp,
          
          count(distinct case when product_action = 'See Product' then pdfi.product_style_id else null end)                                              as unique_styles_imp,
          count(distinct case when product_action = 'See Product' and partner_relationship_type <> 'UNKNOWN-0' then pdfi.product_style_id else null end) as unique_styles_mp_imp,
          count(distinct case when product_action = 'See Product' and partner_relationship_type =  'UNKNOWN-0' then pdfi.product_style_id else null end) as unique_styles_nmp_imp,
          count(distinct case when product_action = 'See Product' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then pdfi.product_style_id else null end)                 as unique_styles_under100_imp,

          sum(case when product_action = 'Add To Bag' then 1 else 0 end)                                              as num_ATB,
          sum(case when product_action = 'Add To Bag' and partner_relationship_type <> 'UNKNOWN-0' then 1 else 0 end) as num_mp_ATB,
          sum(case when product_action = 'Add To Bag' and partner_relationship_type =  'UNKNOWN-0' then 1 else 0 end) as num_nmp_ATB,
          sum(case when product_action = 'Add To Bag' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then 1 else 0 end)                 as num_under100_ATB,

          count(distinct case when product_action = 'Add To Bag' then pdfi.product_style_id else null end)                                              as unique_styles_ATB,
          count(distinct case when product_action = 'Add To Bag' and partner_relationship_type <> 'UNKNOWN-0' then pdfi.product_style_id else null end) as unique_styles_mp_ATB,
          count(distinct case when product_action = 'Add To Bag' and partner_relationship_type =  'UNKNOWN-0' then pdfi.product_style_id else null end) as unique_styles_nmp_ATB,
          count(distinct case when product_action = 'Add To Bag' and  coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then pdfi.product_style_id else null end)                as unique_styles_under100_ATB,

          sum(case when product_action = 'Add To Bag' then event_current_price else 0 end) as ttl_atb_demand,
          sum(case when product_action = 'Add To Bag' and partner_relationship_type <> 'UNKNOWN-0' then event_current_price else 0 end) as ttl_mp_atb_demand,
          sum(case when product_action = 'Add To Bag' and partner_relationship_type =  'UNKNOWN-0' then event_current_price else 0 end) as ttl_nmp_atb_demand,
          sum(case when product_action = 'Add To Bag' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then event_current_price else 0 end)                 as ttl_under100_atb_demand,

          count(distinct orderlineid)                                                                       as ttl_order_qty,
          count(distinct case when partner_relationship_type <> 'UNKNOWN-0' then orderlineid else null end) as ttl_mp_order_qty,
          count(distinct case when partner_relationship_type =  'UNKNOWN-0' then orderlineid else null end) as ttl_nmp_order_qty,
          count(distinct case when coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then orderlineid else null end)                 as ttl_under100_order_qty,

          sum(case when product_action = 'Order Item' then event_current_price else 0 end)                                              as ttl_ord_demand,
          sum(case when product_action = 'Order Item' and partner_relationship_type <> 'UNKNOWN-0' then event_current_price else 0 end) as ttl_mp_ord_demand,
          sum(case when product_action = 'Order Item' and partner_relationship_type =  'UNKNOWN-0' then event_current_price else 0 end) as ttl_nmp_ord_demand,
          sum(case when product_action = 'Order Item' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then event_current_price else 0 end)                 as ttl_under100_ord_demand,

          count(distinct case when product_action = 'Order Item' then pdfi.product_style_id else null end) as unique_styles_ordered,
          count(distinct case when product_action = 'Order Item' and partner_relationship_type <> 'UNKNOWN-0' then pdfi.product_style_id else null end) as unique_styles_mp_ordered,
          count(distinct case when product_action = 'Order Item' and partner_relationship_type =  'UNKNOWN-0' then pdfi.product_style_id else null end) as unique_styles_nmp_ordered,
          count(distinct case when product_action = 'Order Item' and coalesce(avg_selling_price,(prod_max_price+ prod_min_price)/2) <= 100 then pdfi.product_style_id else null end) as unique_styles_under100_ordered


    from ace_etl.session_product_discovery_funnel_intermediate PDFI
    left join ace_etl.digital_merch_table dmt
        on pdfi.product_style_id = dmt.web_style_num
            and cast(PDFI.activity_date_partition as date) = cast(dmt.day_date as date)
            and pdfi.channel = dmt.selling_channel
    where 1 = 1
        and channelcountry = 'US'
        and cast(PDFI.activity_date_partition as date) between date '2023-04-23' and date '2023-04-29'
    group by 1,2,3,4
;



insert OVERWRITE TABLE ace_etl.daily_session_customer_table partition (day_date)
with session_acp_mapping as (
        select sub.*
        from
        (select xref.*,
                coalesce(case when xref.acp_id = '' then NULL else xref.acp_id end, last_value(acp_id) IGNORE NULLS OVER (PARTITION BY shopper_id ORDER BY activity_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as acp_id_v2,
                row_number() over (partition by session_id order by acp_id desc) as rnk
         from acp_vector.customer_session_xref xref
         where 1 = 1
           and cast(xref.activity_date as date) between date '2023-04-23' and date '2023-04-29'
        )sub
        where rnk = 1
    )
select
u.channel
,u.experience
,u.channelcountry
,u.session_id
,fiscal_day_num
,week_idnt
,month_idnt
,quarter_idnt
,fiscal_year_num
,substring(entry_context_pagetype, 1, 100)  landing_page
,substring(exit_context_pagetype, 1, 100) exit_page
,first_arrival_mrkt_type AS mrkt_type
,first_arrival_finance_rollup AS finance_rollup
,first_arrival_finance_detail AS finance_Detail
,bounced AS bounce_ind
,coalesce(cat.acp_id, sam.acp_id) as new_acp_id
,loyalty_status
,cardmember_flag
,engagement_cohort
,dma_desc
,state_name
,predicted_ct_segment
,age_group
,lifestage
,sum(unix_timestamp(session_max_time_pst)- unix_timestamp(session_min_time_pst)) as session_duration_seconds
,sum(page_ct) AS total_pages_visited
,sum(pageinstance_ct) AS total_unique_pages_visited
,max(CASE WHEN home_pageinstance_ct >0 THEN 1 else 0 end) hp_sessions_flag
,max(CASE WHEN home_pageinstance_ct >0 AND click_occurred=1 THEN 1 else 0 end) hp_engaged_sessions_flag
,sum(home_pageinstance_ct) AS hp_views
,max(CASE WHEN  pdp_pageinstance_ct>0 THEN 1 else 0 end) pdp_sessions_flag
,max(CASE WHEN  pdp_pageinstance_ct>0 AND click_occurred=1 THEN 1 else 0 end) pdp_engaged_sessions_flag
,sum(pdp_pageinstance_ct) AS pdp_views
,sum(unique_pdv_product_paths) as unique_pdp_views
,max(CASE WHEN  browse_pageinstance_ct>0 THEN 1 else 0 end) browse_sessions_flag
,max(CASE WHEN  browse_pageinstance_ct>0 AND click_occurred=1 THEN 1 else 0 end) browse_engaged_sessions_flag
,sum(browse_pageinstance_ct) AS browse_views
,sum(unique_browse_paths) as unique_browses
,max(CASE WHEN  search_pageinstance_ct>0 THEN 1 else 0 end) search_sessions_flag
,max(CASE WHEN  search_pageinstance_ct>0 AND click_occurred=1  THEN 1 else 0 end) search_engaged_sessions_flag
,sum(search_pageinstance_ct) AS search_views
,sum(unique_search_paths) as unique_searches
,max(CASE WHEN  search_pageinstance_ct>0 AND browse_pageinstance_ct>0 THEN 1 else 0 end) sbn_sessions_flag
,max(CASE WHEN  search_pageinstance_ct>0 AND browse_pageinstance_ct>0  AND click_occurred=1  THEN 1 else 0 end) sbn_engaged_sessions_flag
,sum(search_pageinstance_ct+browse_pageinstance_ct) sbn_views
,max(CASE WHEN  wishlist_pageinstance_ct>0  THEN 1 else 0 end) wishlist_sessions_flag
,max(CASE WHEN  wishlist_pageinstance_ct>0 AND click_occurred=1 THEN 1 else 0 end) wishlist_engaged_sessions_flag
,sum(wishlist_pageinstance_ct) AS wishlist_views
,max(CASE WHEN  checkout_pageinstance_ct>0 THEN 1 else 0 end) checkout_sessions_flag
,max( CASE WHEN  checkout_pageinstance_ct>0 AND click_occurred=1 THEN 1 else 0 end) checkout_engaged_sessions_flag
,sum(checkout_pageinstance_ct) AS checkout_views
,max( CASE WHEN  shopping_bag_pageinstance_ct>0 THEN 1 else 0 end) shopb_sessions_flag
,max( CASE WHEN  shopping_bag_pageinstance_ct>0 AND click_occurred=1 THEN 1 else 0 end) shopb_engaged_sessions_flag
,sum(shopping_bag_pageinstance_ct) AS shopb_views
,max( CASE WHEN  atb_onpage_ct>0 THEN 1 else 0 end) atb_sessions_flag
,sum(atb_onpage_ct) AS atb_views
,sum(atb_event_ct) AS atb_events
,sum(atb_style_ct) as atb_styles_count
,sum(atb_current_price) as atb_demand_usd
,max( CASE WHEN  order_onpage_ct>0 THEN 1 else 0 end) ordered_sessions_flag
,sum(order_item_qty) sales_qty
,sum(order_event_ct) web_orders
,sum(order_style_ct) as web_order_styles_count
,sum(order_current_price) web_demand_usd
,avg(order_discount) as web_order_discount_avg
,sum(order_free_item_qty) as web_order_free_qty
,max(case when num_mp_full_pdv>0 then 1 else 0 end) as marketplace_sessions_flag
,max(case when num_under100_full_pdv>0 then 1 else 0 end) as under100_sessions_flag
,sum(num_click) as num_prod_click
,sum(num_mp_click) as num_mp_click
,sum(num_nmp_click) as num_nmp_click
,sum(num_under100_click) as num_under100_click
,sum(unique_styles_clicks) as unique_styles_clicks
,sum(unique_styles_mp_clicks) as unique_styles_mp_clicks
,sum(unique_styles_nmp_clicks) as unique_styles_nmp_clicks
,sum(unique_styles_under100_clicks) as unique_styles_under100_clicks
,sum(num_full_pdv) as num_full_pdv
,sum(num_mp_full_pdv) as num_mp_full_pdv
,sum(num_nmp_full_pdv) as num_nmp_full_pdv
,sum(num_under100_full_pdv) as num_under100_full_pdv
,sum(unique_styles_full_pdv) as unique_styles_full_pdv
,sum(unique_styles_mp_full_pdv) as unique_styles_mp_full_pdv
,sum(unique_styles_nmp_full_pdv) as unique_styles_nmp_full_pdv
,sum(unique_styles_under100_full_pdv) as unique_styles_under100_full_pdv
,sum(num_imp) as num_prod_imp
,sum(num_mp_imp) as num_mp_imp
,sum(num_nmp_imp) as num_nmp_imp
,sum(num_under100_imp) as num_under100_imp
,sum(unique_styles_imp) as unique_styles_imp
,sum(unique_styles_mp_imp) as unique_styles_mp_imp
,sum(unique_styles_nmp_imp) as unique_styles_nmp_imp
,sum(unique_styles_under100_imp) as unique_styles_under100_imp
,sum(num_mp_ATB) as num_mp_ATB
,sum(num_nmp_ATB) as num_nmp_ATB
,sum(num_under100_ATB) as num_under100_ATB
,sum(unique_styles_ATB) as unique_styles_ATB
,sum(unique_styles_mp_ATB) as unique_styles_mp_ATB
,sum(unique_styles_nmp_ATB) as unique_styles_nmp_ATB
,sum(unique_styles_under100_ATB) as unique_styles_under100_ATB
,sum(ttl_mp_atb_demand) as ttl_mp_atb_demand
,sum(ttl_nmp_atb_demand) as ttl_nmp_atb_demand
,sum(ttl_under100_atb_demand) as ttl_under100_atb_demand
,sum(ttl_mp_order_qty) as ttl_mp_order_qty
,sum(ttl_nmp_order_qty) as ttl_nmp_order_qty
,sum(ttl_under100_order_qty) as ttl_under100_order_qty
,sum(unique_styles_ordered) as unique_styles_ordered
,sum(unique_styles_mp_ordered) as unique_styles_mp_ordered
,sum(unique_styles_nmp_ordered) as unique_styles_nmp_ordered
,sum(unique_styles_under100_ordered) as unique_styles_under100_ordered
,sum(ttl_mp_ord_demand) as ttl_mp_ord_demand
,sum(ttl_nmp_ord_demand) as ttl_nmp_ord_demand
,sum(ttl_under100_ord_demand) as ttl_under100_ord_demand
,CURRENT_TIMESTAMP() as dw_sys_load_tmstp
,u.activity_date_partition as day_date

from acp_event_intermediate.session_fact_attributes_parquet u
inner join OBJECT_MODEL.DAY_CAL_454_DIM dc on cast(u.activity_date_partition as date) = date_format(to_date(dc.day_date, 'MM/dd/yyyy'),'yyyy-MM-dd')
left join ace_etl.customer_attributes_table cat on u.session_id = cat.session_id and cast(u.activity_date_partition as date) = cast(cat.activity_date_pacific as date)
left join session_acp_mapping sam on u.session_id = sam.session_id and cast(u.activity_date_partition as date) = cast(sam.activity_date as date)
left join marketplace_under100_tbl mut on mut.session_id = u.session_id and cast(mut.activity_date_partition as date) = cast(u.activity_date_partition as date)
where u.channelcountry = 'US'
  and cast(u.activity_date_partition as date)  between date '2023-04-23' and date '2023-04-29'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,113,114
;


