-- Table Name:digital_product_review_table
-- Team/Owner: CIA - Digital Customer Experience Analytics
-- Date Created/Modified: created on 06/18/2024, last modified on 06/21/2024
-- What is the update cadence/lookback window: refresh weekly


-- Definine new Hive table for output
create table if not exists ace_etl.digital_product_review_table
(
    channel                 string
    , month_idnt            string
    , quarter_idnt          string
    , fiscal_halfyear_num   string
    , fiscal_year_num       string
    , brand_name            string
    , class                 string
    , department            string
    , groups                string
    , division              string
    , quantrix_category     string
    , parent_group          string
    , product_review_idnt   integer
    , product_count         integer
    , num_sessions_weekly          integer
    , num_pv_sessions_weekly       integer
    , num_sessions_merch           integer
    , num_pv_sessions_merch        integer
    , num_imp               integer
    , num_click             integer
    , num_full_pdv          integer
    , num_atb               integer
    , ttl_atb               integer
    , ttl_atb_demand        numeric(20,4)
    , ttl_order_count       integer
    , ttl_ord_demand        numeric(20,4)
    , num_ord_sessions_weekly      integer
    , num_ord_sessions_merch       integer
    , ttl_review_count      numeric(20,4)
    , avg_review_count      numeric(20,4)
    , avg_review_rating     numeric(20,4)
    , return_qty            integer
    , return_amt            numeric(20,4)
    , avg_days_to_return    numeric(20,4)
    , week_idnt             string
)
using PARQUET
location 's3://ace-etl/digital_product_review_table'
partitioned by (week_idnt);

-- sync partitions on hive table
MSCK REPAIR TABLE ace_etl.digital_product_review_table;

create or replace temporary view week_lkp as 
    select min(week_idnt) as start_week,
            max(week_idnt) as end_week
    from OBJECT_MODEL.DAY_CAL_454_DIM dt
    where date_format(to_date(dt.day_date, 'MM/dd/yyyy'),'yyyy-MM-dd') between date '2023-03-05' and date '2023-04-01'
;



create or replace temporary view weekly_style_return_base_rd as
    select order_style_id,
            channel,
            week_idnt,
            sum(return_quantity) as return_qty,
            sum(return_order_line_current_units) as return_amt,
            avg(days_to_return) as avg_days_to_return
    from ace_etl.session_sales_returns_transactions r
    left join OBJECT_MODEL.DAY_CAL_454_DIM dt
        on date_format(to_date(dt.day_date, 'MM/dd/yyyy'),'yyyy-MM-dd') = cast(r.return_date as date)
    where dt.week_idnt between (select start_week from week_lkp) and (select end_week from week_lkp)
    group by 1,2,3
;



create or replace temporary view weekly_session_by_merch as 
    select PDFI.channel,
            dt.week_idnt,
            dmt.brand_name,
            concat(cast(dmt.class_num as varchar(6)), ',', dmt.class_desc) as class,
            concat(cast(dmt.dept_num as varchar(6)), ',', dmt.dept_desc) as department,
            concat(cast(dmt.grp_num as varchar(6)), ',', dmt.grp_desc) as groups,
            concat(cast(dmt.div_num as varchar(6)), ',', dmt.div_desc) as division,
            dmt.quantrix_category,
            dmt.parent_group,
            case
            when PDFI.prod_avg_reviewcount is null or PDFI.prod_avg_reviewcount = 0 then 0
            else 1 end                                                                   as product_review_idnt,
            count(distinct session_id) as session_count,
            count(distinct case when product_action = 'View Product Detail'then session_id else null end) as pv_session_count,
            count(distinct case when product_action = 'Order Item' then session_id else null end) as ord_session_count
    from ace_etl.session_product_discovery_funnel_intermediate PDFI
    inner join OBJECT_MODEL.DAY_CAL_454_DIM dt
        on date_format(to_date(dt.day_date, 'MM/dd/yyyy'),'yyyy-MM-dd') = cast(PDFI.activity_date_partition as date)
    left join ace_etl.digital_merch_table dmt
        on pdfi.product_style_id = dmt.web_style_num
            and cast(PDFI.activity_date_partition as date) = cast(dmt.day_date as date)
            and pdfi.channel = dmt.selling_channel
     where dt.week_idnt between (select start_week from week_lkp) and (select end_week from week_lkp)
    group by 1,2,3,4,5,6,7,8,9,10
;




create or replace temporary view weekly_session as 
    select PDFI.channel,
            dt.week_idnt,
            case
            when PDFI.prod_avg_reviewcount is null or PDFI.prod_avg_reviewcount = 0 then 0
            else 1 end                                                                   as product_review_idnt,
            count(distinct session_id) as session_count,
            count(distinct case when product_action = 'View Product Detail'then session_id else null end) as pv_session_count,
            count(distinct case when product_action = 'Order Item' then session_id else null end) as ord_session_count
    from ace_etl.session_product_discovery_funnel_intermediate PDFI
    inner join OBJECT_MODEL.DAY_CAL_454_DIM dt
        on date_format(to_date(dt.day_date, 'MM/dd/yyyy'),'yyyy-MM-dd') = cast(PDFI.activity_date_partition as date)
    where dt.week_idnt between (select start_week from week_lkp) and (select end_week from week_lkp)
    group by 1,2,3
;




create or replace temporary view weekly_style_review_base as 
    select
        PDFI.product_style_id,
        PDFI.channel,
        dt.week_idnt,
        dt.month_idnt,
        dt.quarter_idnt,
        dt.fiscal_halfyear_num,
        dt.fiscal_year_num,
        dmt.brand_name,
        concat(cast(dmt.class_num as varchar(6)), ',', dmt.class_desc) as class,
        concat(cast(dmt.dept_num as varchar(6)), ',', dmt.dept_desc) as department,
        concat(cast(dmt.grp_num as varchar(6)), ',', dmt.grp_desc) as groups,
        concat(cast(dmt.div_num as varchar(6)), ',', dmt.div_desc) as division,
        dmt.quantrix_category,
        dmt.parent_group,
        case
            when PDFI.prod_avg_reviewcount is null or PDFI.prod_avg_reviewcount = 0 then 0
            else 1 end                                                                   as product_review_idnt,
        sum(case when product_action = 'See Product' then 1 else 0 end)                  as num_imp,
        sum(case when product_action = 'Click on Product' then 1 else 0 end)             as num_click,
        sum(case
                when product_action = 'View Product Detail' and action_feature = 'FULL PDP'
                    then 1
                else 0 end)                                                              as num_full_pdv,
        sum(case when product_action = 'Add To Bag' then 1 else 0 end)                   as num_ATB,
        count(distinct case
                            when product_action = 'Add To Bag' then event_id
                            else null end)                                                   ttl_ATB,
        sum(case when product_action = 'Add To Bag' then event_regular_price else 0 end) as ttl_atb_value,
        sum(case when product_action = 'Add To Bag' then event_current_price else 0 end) as ttl_atb_demand,
        count(distinct orderlineid)                                                         ttl_order_qty,
        sum(case when product_action = 'Order Item' then event_regular_price else 0 end) as ttl_ord_value,
        sum(case when product_action = 'Order Item' then event_current_price else 0 end) as ttl_ord_demand,
        max(PDFI.prod_avg_reviewcount)                                                   as Num_reviews,
        avg(PDFI.prod_avg_rating)                                                        as avg_rating
    from ace_etl.session_product_discovery_funnel_intermediate PDFI
    inner join OBJECT_MODEL.DAY_CAL_454_DIM dt
            on date_format(to_date(dt.day_date, 'MM/dd/yyyy'),'yyyy-MM-dd') =
                cast(PDFI.activity_date_partition as date)
    left join ace_etl.digital_merch_table dmt
            on pdfi.product_style_id = dmt.web_style_num
                and cast(PDFI.activity_date_partition as date) = cast(dmt.day_date as date)
                and pdfi.channel = dmt.selling_channel
    where 1 = 1
        and dt.week_idnt between (select start_week from week_lkp) and (select end_week from week_lkp)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
;




create or replace temporary view digi_prod_rev_tbl as 
    select base.channel,
        base.month_idnt,
        base.quarter_idnt,
        base.fiscal_halfyear_num,
        base.fiscal_year_num,
        base.brand_name,
        base.class,
        base.department,
        base.groups,
        base.division,
        base.quantrix_category,
        base.parent_group,
        base.product_review_idnt,
        base.week_idnt,
        count(distinct product_style_id) as product_count,
        avg(wsw.session_count) as num_sessions_weekly,
        avg(wsw.pv_session_count) as num_pv_sessions_weekly,
        avg(wsm.session_count) as num_sessions_merch,
        avg(wsm.pv_session_count) as num_pv_sessions_merch,
        sum(num_imp) as num_imp,
        sum(num_click) as num_click,
        sum(num_full_pdv) as num_full_pdv,
        sum(num_ATB) as num_ATB,
        sum(ttl_ATB) as ttl_ATB,
        sum(ttl_atb_demand) as ttl_atb_demand,
        sum(ttl_order_qty) as ttl_order_count,
        sum(ttl_ord_demand) as ttl_ord_demand,
        avg(wsw.ord_session_count) as num_ord_sessions_weekly,
        avg(wsm.ord_session_count) as num_ord_sessions_merch,
        sum(Num_reviews) as ttl_review_count,
        avg(Num_reviews)*1.0000 as avg_review_count,
        avg(avg_rating) as avg_review_rating,
        sum(dsrb.return_qty) as return_qty,
        sum(dsrb.return_amt) as return_amt,
        avg(dsrb.avg_days_to_return)*1.0000 as avg_days_to_return
    from weekly_style_review_base base
    left join weekly_style_return_base_rd dsrb
        on base.product_style_id = dsrb.order_style_id
            and dsrb.week_idnt = base.week_idnt
            and base.channel = dsrb.channel
    left join weekly_session_by_merch wsm
        on wsm.week_idnt = base.week_idnt
            and wsm.channel = base.channel
            and wsm.brand_name = base.brand_name
            and wsm.division = base.division
            and wsm.class = base.class
            and wsm.department = base.department
            and wsm.groups = base.groups
            and wsm.quantrix_category = base.quantrix_category
            and wsm.parent_group = base.parent_group
            and wsm.product_review_idnt = base.product_review_idnt
    left join weekly_session wsw
        on wsw.week_idnt = base.week_idnt
            and wsw.channel = base.channel
            and wsw.product_review_idnt = base.product_review_idnt
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
;



--- now the INSERT into our hive table
--- the Hive table will have the data queryable from presto and hive cli
-- insert into table ace_etl.digital_product_review_table partition (week_idnt)
insert overwrite table ace_etl.digital_product_review_table partition (week_idnt)
select channel                 
    , month_idnt            
    , quarter_idnt          
    , fiscal_halfyear_num   
    , fiscal_year_num       
    , brand_name            
    , class                 
    , department            
    , groups                
    , division              
    , quantrix_category     
    , parent_group          
    , product_review_idnt   
    , product_count         
    , num_sessions_weekly          
    , num_pv_sessions_weekly       
    , num_sessions_merch         
    , num_pv_sessions_merch        
    , num_imp               
    , num_click             
    , num_full_pdv          
    , num_atb               
    , ttl_atb               
    , ttl_atb_demand        
    , ttl_order_count       
    , ttl_ord_demand       
    , num_ord_sessions_weekly      
    , num_ord_sessions_merch       
    , ttl_review_count     
    , avg_review_count     
    , avg_review_rating    
    , return_qty           
    , return_amt          
    , avg_days_to_return   
    , week_idnt        
from digi_prod_rev_tbl
;
