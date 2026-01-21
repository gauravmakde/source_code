SET QUERY_BAND = 'App_ID=APP08750;
     DAG_ID=wishlist_sessions_11521_ACE_ENG;
     Task_Name=wishlist_sessions_ldg;'
     FOR SESSION VOLATILE;

--output to landing table

create or replace temp view engaged
as
select * from acp_event_view.customer_activity_engaged
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
;

create or replace temp view pscv
as
select * from acp_event_view.customer_activity_product_summary_collection_viewed
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
;

create or replace temp view added_to_bag
as
select * from acp_event_view.customer_activity_added_to_bag
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
;

create or replace temp view order_submitted
as
select * from acp_event_view.customer_activity_order_submitted
where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}
;

create or replace temp view session_evt_fact
as
select * from acp_vector.customer_digital_session_evt_fact
where activity_date_partition between {start_date} and {end_date}
;


CREATE OR REPLACE TEMPORARY VIEW sessions_temp AS
select
    activity_date,
    channelcountry,
    channel,
    experience,
    feature,
    event_name,
    event_id,
    event_time_utc,
    session_id
    from session_evt_fact
    where event_name in ('com.nordstrom.customer.ProductDetailViewed', 'com.nordstrom.event.customer.Engaged'
        , 'com.nordstrom.event.customer.ProductSummaryCollectionViewed', 
        'com.nordstrom.customer.AddedToBag', 'com.nordstrom.customer.OrderSubmitted')
    and experience in ('ANDROID_APP', 'DESKTOP_WEB', 'IOS_APP', 'MOBILE_WEB')
    and channel in ('NORDSTROM', 'NORDSTROM_RACK')
    group by 1,2,3,4,5,6,7,8,9;



CREATE OR REPLACE TEMPORARY VIEW product_detail_viewed AS
select
    activity_date,
    channelcountry,
    channel,
    experience,
    count(distinct session_id) as session_count
    from sessions_temp
    where event_name = 'com.nordstrom.customer.ProductDetailViewed'
    group by 1,2,3,4;


CREATE OR REPLACE TEMPORARY VIEW add_to_wishlist AS
select
    a.activity_date, 
    a.channelcountry,
    a.channel,
    a.experience,
    count(distinct a.session_id) as session_count
    from sessions_temp a
    inner join (
        select
        explode_outer(context.digitalcontents) as dc_array,
        dc_array.idType,
        element.id as element_id,
        cast(headers.Id as string) as event_id
        from engaged
        ) b
    on a.event_id = b.event_id
    where a.event_name = 'com.nordstrom.event.customer.Engaged'
    and lower(b.element_id) in ('productdetail/addtowishlist', 'minipdp/addtowishlist', 'productresults/productgallery/product/skupicker/addtowishlist')
    and b.idType = 'RMSSKU'
    group by 1,2,3,4;


CREATE OR REPLACE TEMPORARY VIEW wishlist_pscv AS
select
    a.activity_date,
    a.channelcountry,
    a.channel,
    a.experience,
    count(distinct a.session_id) as session_count
    from sessions_temp a
    inner join (
        select
        explode_outer(productsummarycollection) as psc_array,
        psc_array.element.id as element_id,
        cast(headers.Id as string) as event_id
        from pscv
        where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') between {start_date} and {end_date}) b 
    on a.event_id = b.event_id
    where a.event_name = 'com.nordstrom.event.customer.ProductSummaryCollectionViewed'
    and b.element_id in ('Wishlist/WishlistItem','Wishlist/Highlights/HighlightItem')
    group by 1,2,3,4;


CREATE OR REPLACE TEMPORARY VIEW atb_detail AS
select
    a.activity_date,
    a.channelcountry,
    a.channel,
    a.experience,
    a.session_id,
    b.product.id as rms_sku,
    min(a.event_time_utc) as event_time_utc
    from sessions_temp a
    inner join added_to_bag b 
    on a.event_id = cast(b.headers.Id as string)
    where a.event_name = 'com.nordstrom.customer.AddedToBag'
    and a.feature = 'WISH_LIST'
    group by 1,2,3,4,5,6;


CREATE OR REPLACE TEMPORARY VIEW os_detail AS
select 
    activity_date,
    channelcountry,
    channel,
    experience,
    session_id,
    event_time_utc,
    rms_sku,
    sum(current_price + coalesce(cast(ed.units + ed.nanos * power(10,-9) as decimal(8, 2)),0)) as demand
    from (
    select
        a.activity_date,
        a.channelcountry,
        a.channel,
        a.experience,
        a.session_id,
        b.rms_sku,
        b.orderLineId,
        b.current_price,
        explode_outer(b.employee_discount) as ed,
        min(a.event_time_utc) as event_time_utc 
        from sessions_temp a
        inner join 
            (select 
            cast(headers.Id as string) as event_id,
            explode_outer(items) as items_array,
            items_array.orderLineId as orderLineId,
            items_array.product.id as rms_sku,
            filter(items_array.promotions, x -> length(x.Employee.Id)>0).discount as employee_discount,
            cast(items_array.price.current.units + items_array.price.current.nanos * power(10,-9) as decimal(8, 2)) as current_price
            from order_submitted
            ) b
            on a.event_id = b.event_id
        where a.event_name = 'com.nordstrom.customer.OrderSubmitted'
        group by 1,2,3,4,5,6,7,8,9)    
    group by 1,2,3,4,5,6,7;


CREATE OR REPLACE TEMPORARY VIEW atb_os AS
select
    a.activity_date,
    a.channelcountry,
    a.channel,
    a.experience,
    count(distinct a.session_id) as atb_sessions,
    count(distinct b.session_id) as ord_sessions,
    sum(b.demand) as demand
    from atb_detail a
    left outer join os_detail b
    on a.session_id = b.session_id
    and a.rms_sku = b.rms_sku
    and a.event_time_utc <= b.event_time_utc
    group by 1,2,3,4;
    

CREATE OR REPLACE TEMPORARY VIEW sessions_left AS
select
    activity_date, 
    channelcountry,
    channel,
    experience
    from sessions_temp
    group by 1,2,3,4;

--output to s3
insert overwrite table wishlist_sessions partition (partition_date)
select
    sess.activity_date,
    sess.activity_date as partition_date,
    sess.channelcountry,
    sess.channel,
    sess.experience,
    cast(sum(nvl(pdv.session_count,0)) as double) as pdv_sessions,
    cast(sum(nvl(atw.session_count,0)) as double) as atwl_sessions,
    cast(sum(nvl(pscv.session_count,0)) as double) as pscv_sessions,
    cast(sum(nvl(ao.atb_sessions,0)) as double) as atb_sessions,
    cast(sum(nvl(ao.ord_sessions,0)) as double) as ord_sessions,
    cast(sum(nvl(ao.demand,0)) as double) as demand
    from sessions_left sess
    left join product_detail_viewed pdv
    on sess.activity_date = pdv.activity_date
    and sess.channelcountry = pdv.channelcountry
    and sess.channel = pdv.channel
    and sess.experience = pdv.experience
    left join add_to_wishlist atw
    on sess.activity_date = atw.activity_date
    and sess.channelcountry = atw.channelcountry
    and sess.channel = atw.channel
    and sess.experience = atw.experience
    left join wishlist_pscv pscv
    on sess.activity_date = pscv.activity_date
    and sess.channelcountry = pscv.channelcountry
    and sess.channel = pscv.channel
    and sess.experience = pscv.experience   
    left join atb_os ao
    on sess.activity_date = ao.activity_date
    and sess.channelcountry = ao.channelcountry
    and sess.channel = ao.channel
    and sess.experience = ao.experience  
    group by 1,2,3,4,5;

-- Writing output to teradata landing table.
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table wishlist_sessions_output
select
    sess.activity_date,
    sess.channelcountry,
    sess.channel,
    sess.experience,
    cast(sum(nvl(pdv.session_count,0)) as double) as pdv_sessions,
    cast(sum(nvl(atw.session_count,0)) as double) as atwl_sessions,
    cast(sum(nvl(pscv.session_count,0)) as double) as pscv_sessions,
    cast(sum(nvl(ao.atb_sessions,0)) as double) as atb_sessions,
    cast(sum(nvl(ao.ord_sessions,0)) as double) as ord_sessions,
    cast(sum(nvl(ao.demand,0)) as double) as demand
    from sessions_left sess
    left join product_detail_viewed pdv
    on sess.activity_date = pdv.activity_date
    and sess.channelcountry = pdv.channelcountry
    and sess.channel = pdv.channel
    and sess.experience = pdv.experience
    left join add_to_wishlist atw
    on sess.activity_date = atw.activity_date
    and sess.channelcountry = atw.channelcountry
    and sess.channel = atw.channel
    and sess.experience = atw.experience
    left join wishlist_pscv pscv
    on sess.activity_date = pscv.activity_date
    and sess.channelcountry = pscv.channelcountry
    and sess.channel = pscv.channel
    and sess.experience = pscv.experience   
    left join atb_os ao
    on sess.activity_date = ao.activity_date
    and sess.channelcountry = ao.channelcountry
    and sess.channel = ao.channel
    and sess.experience = ao.experience  
    group by 1,2,3,4
;

