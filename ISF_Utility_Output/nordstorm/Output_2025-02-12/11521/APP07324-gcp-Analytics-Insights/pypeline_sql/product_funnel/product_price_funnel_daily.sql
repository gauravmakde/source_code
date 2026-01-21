SET QUERY_BAND = 'App_ID=APP08227;
     DAG_ID=product_price_funnel_daily_11521_ACE_ENG;
     Task_Name=product_price_funnel_daily;'
     FOR SESSION VOLATILE;

-- Product Price Funnel Daily
  -- Creates layer on top of base PFD that includes additional non-hierarchy merch dimension
  -- Includes price amount and type, RP indicators, and in-stock rates
--  T2DL_DAS_PRODUCT_FUNNEL.product_price_funnel_daily


/* Join Modern & Legacy Data */
create multiset volatile table pfd_base AS (

  select
      event_date_pacific
      , channelcountry
      , channel
      , platform
      , site_source
      , style_id
      , sku_id
      , averagerating
      , review_count
      , order_quantity
      , order_demand
      , order_sessions
      , add_to_bag_quantity
      , add_to_bag_sessions
      , product_views
      , product_view_sessions
  from t2dl_das_product_funnel.product_funnel_daily
  where event_date_pacific between cast({start_date}  as date) and cast({end_date} as date) and site_source = 'MODERN'

  union all

  select
      event_date_pacific
      , channelcountry
      , channel
      , platform
      , site_source
      , web_style_id as style_id
      , rms_sku_num as sku_id
      , averagerating
      , review_count
      , order_units as order_quantity
      , demand as order_demand
      , order_sessions
      , cart_adds as add_to_bag_quantity
      , add_to_bag_sessions
      , product_views
      , product_view_sessions
  from t2dl_das_product_funnel.product_funnel_daily_history
  where event_date_pacific between cast({start_date}  as date) and cast({end_date} as date)

) with data primary index (sku_id, style_id, event_date_pacific, channel, channelcountry) on commit preserve rows
;

collect statistics primary index (sku_id, style_id, event_date_pacific, channel, channelcountry)
  , column(sku_id)
  , column(style_id)
  , column(event_date_pacific)
  , column(channel)
  , column(channelcountry)
on pfd_base;


/* Distribute Remaining Unknown Skus with Live on Site where can */
create multiset volatile table pfd_los as (

    select
      b.event_date_pacific
      , b.channelcountry
      , b.channel
      , b.platform
      , b.site_source
      , b.style_id
      , coalesce(l.sku_id, 'unknown') as sku_id
      , b.averagerating
      , b.review_count
      , b.order_quantity
      , b.order_demand
      , b.order_sessions
      , b.add_to_bag_quantity
      , b.add_to_bag_sessions
      , product_views/(coalesce(sku_cnt, 1)) as product_views
      , product_view_sessions
    from pfd_base b
    left join (
        select
            distinct los.day_date
            , los.channel_country
            , case when channel_brand = 'NORDSTROM' then 'FULL_LINE'
                when channel_brand = 'NORDSTROM_RACK' then 'RACK'
                end as channel_brand
            , case when day_date <= cast('2022-05-01'as date) then cast(coalesce(e.web_style_id, s.web_style_num) as bigint)
                else cast(coalesce(s.web_style_num, e.web_style_id) as bigint) end as web_style
            , sku_id
            , count(sku_id) over(partition by los.day_date, los.channel_country, los.channel_brand, web_style) as sku_cnt
        from T2DL_DAS_SITE_MERCH.live_on_site_daily los
        left join prd_nap_usr_vws.product_sku_dim_vw s
          on los.sku_id = s.rms_sku_num and los.channel_country = s.channel_country
        left join T2DL_DAS_ETE_INSTRUMENTATION.LIVE_ON_SITE_PCDB_US_OPR e
          on los.sku_id = e.rms_sku_id
        where day_date between cast({start_date}  as date) and cast({end_date} as date)
    ) l
    on b.event_date_pacific = l.day_date
      and b.channelcountry = l.channel_country
      and b.channel = l.channel_brand
      and b.style_id = l.web_style
    where b.sku_id = 'unknown'

    union all

    select
        event_date_pacific
        , channelcountry
        , channel
        , platform
        , site_source
        , style_id
        , sku_id
        , averagerating
        , review_count
        , order_quantity
        , order_demand
        , order_sessions
        , add_to_bag_quantity
        , add_to_bag_sessions
        , product_views
        , product_view_sessions
    from pfd_base where sku_id <> 'unknown'

) with data primary index (sku_id, style_id, event_date_pacific, channel, channelcountry) on commit preserve rows;

collect statistics primary index (sku_id, style_id, event_date_pacific, channel, channelcountry)
  , column(sku_id)
  , column(style_id)
  , column(event_date_pacific)
  , column(channel)
  , column(channelcountry)
on pfd_los;


-- RP Flag
create multiset volatile table rp_lkp
  as (
      select
          rms_sku_num
          , case when business_unit_desc  in ('N.COM', 'N.CA') then 'FULL_LINE' else 'RACK' end as channel
          , case when business_unit_desc  in ('N.COM', 'OFFPRICE ONLINE') then 'US' else 'CA' end as channelcountry
          , day_date
          , 'Y' as aip_rp_fl
      from PRD_NAP_USR_VWS.MERCH_RP_SKU_LOC_DIM_HIST rp
      inner join (
                select distinct day_date
                from prd_nap_usr_vws.day_cal
                where day_date between cast({start_date}  as date) and cast({end_date} as date)
            ) d
      on d.day_date between BEGIN(rp_period) and END(rp_period)
      inner join prd_nap_usr_vws.store_dim st
         on rp.location_num = st.store_num
      where business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE', 'N.CA')
      group by 1,2,3,4,5
) with data primary index (rms_sku_num, day_date, channel, channelcountry) on commit preserve rows;

collect statistics primary index (rms_sku_num, day_date, channel, channelcountry)
  , column(rms_sku_num)
  , column(day_date)
  , column(channel)
  , column(channelcountry)
on rp_lkp;


-- Prep Price Data to Join in
create multiset volatile table prc as (

    /* Current Table: Includes Price Changes */
    select
        distinct c.day_date
        , case when nap.channel_brand = 'NORDSTROM' then 'FULL_LINE' else 'RACK' end as channel_brand
        , channel_country as channelcountry
        , nap.rms_sku_num
        , regular_price_amt
        , selling_retail_price_amt as current_price_amt
        , case when selling_retail_price_type_code  = 'PROMOTION' and ownership_retail_price_type_code = 'CLEARANCE' then 'C'
              when selling_retail_price_type_code  = 'REGULAR' then 'R'
              when selling_retail_price_type_code  = 'PROMOTION' then 'P'
              when selling_retail_price_type_code  = 'CLEARANCE' then 'C'
              end as current_price_type
    from prd_nap_usr_vws.PRODUCT_PRICE_TIMELINE_DIM nap
    inner join (
        select day_date
        from prd_nap_usr_vws.day_cal
        where day_date between cast({start_date} as date) and cast({end_date}  as date)
          and day_date >= '2020-10-01'
    ) c
      on day_date between cast(eff_begin_tmstp as date) and cast(eff_end_tmstp as date)
    where selling_channel = 'ONLINE'
    qualify row_number() over(partition by day_date, rms_sku_num, channelcountry, channel_brand order by eff_begin_tmstp desc) = 1

) with data primary index (rms_sku_num, day_date, channel_brand, channelcountry) on commit preserve rows;

collect statistics primary index (rms_sku_num, day_date, channel_brand, channelcountry)
  , column(rms_sku_num)
  , column(day_date)
  , column(channel_brand)
  , column(channelcountry)
on prc;


-- Instocks Join
create multiset volatile table twist as (
    select
        day_date
        , case when banner = 'NORD' then 'FULL_LINE' else 'RACK' end as channel
        , country
        , web_style_num
        , sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_views
        , sum(allocated_traffic) as total_views
        , case when total_views = 0 then null else instock_views/total_views end as pct_instock
    from {twist_t2_schema}.twist_daily
    where business_unit_desc IN ('N.COM', 'OFFPRICE ONLINE', 'N.CA')
        and product_views > 0
        and day_date between cast({start_date} as date) and cast({end_date} as date)
    group by 1,2,3,4
) with data primary index (web_style_num, day_date, channel, country) on commit preserve rows ;

collect statistics primary index (web_style_num, day_date, channel, country)
  , column(web_style_num)
  , column(day_date)
  , column(channel)
  , column(country)
on twist;


-- Flash Events
create multiset volatile table flash as (
     select sku_num
          , 'RACK' as channel
          , channel_country
          , day_date
          , selling_event_name as event_name
      from (
        select distinct day_date
        from prd_nap_usr_vws.day_cal
        where day_date between cast({start_date} as date) and cast({end_date}  as date)
      ) dt
      inner join prd_nap_usr_vws.PRODUCT_SELLING_EVENT_SKU_DIM e
          on dt.day_date BETWEEN cast(e.item_start_tmstp as date) and cast(e.item_end_tmstp as date)
      inner join PRD_NAP_USR_VWS.PRODUCT_SELLING_EVENT_TAGS_DIM t
          on e.selling_event_num = t.selling_event_num
          and t.tag_name = 'FLASH'
      qualify row_number() over(partition by sku_num, channel, channel_country, day_date order by selling_event_name desc) = 1
) with data primary index (sku_num, day_date, channel, channel_country) on commit preserve rows ;

collect statistics primary index (sku_num, day_date, channel, channel_country)
  , column(sku_num)
  , column(day_date)
  , column(channel)
  , column(channel_country)
on flash;


-- Delete & Insert Records to Final Table
delete {product_funnel_t2_schema}.product_price_funnel_daily where event_date_pacific between {start_date} and {end_date} ;

insert into {product_funnel_t2_schema}.product_price_funnel_daily
    select
      p.event_date_pacific
      , p.channelcountry
      , p.channel
      , p.platform
      , p.site_source
      , p.style_id as web_style_num
      , p.sku_id as rms_sku_num
      , case when p.sku_id = 'unknown' then null
            when r.rms_sku_num is not null then 1
            else 0 end as rp_ind
      , p.averagerating
      , p.review_count
      , p.order_quantity
      , p.order_demand
      , p.order_sessions
      , p.add_to_bag_quantity
      , p.add_to_bag_sessions
      , p.product_views
      , p.product_view_sessions
      , case when p.sku_id = 'unknown' then null
            when c.current_price_type is null then 'R'
            else c.current_price_type end as current_price_type
      , c.current_price_amt
      , c.regular_price_amt
      , t.pct_instock
      , case when f.sku_num is not null then 'FLASH' else 'PERSISTENT' end as event_type
      , upper(coalesce(f.event_name, 'NONE')) as event_name
      , CURRENT_TIMESTAMP as dw_sys_load_tmstp
    from pfd_los p
    left join rp_lkp r
    on p.event_date_pacific = r.day_date
      and p.channelcountry = r.channelcountry
      and p.channel = r.channel
      and p.sku_id = r.rms_sku_num
    left join prc c
    on p.event_date_pacific = c.day_date
      and p.channelcountry = c.channelcountry
      and p.channel = c.channel_brand
      and p.sku_id = c.rms_sku_num
    left join T2DL_DAS_ETE_INSTRUMENTATION.LIVE_ON_SITE_PCDB_US_OPR e
    on p.sku_id = e.rms_sku_id
    left join twist t
    on p.event_date_pacific = t.day_date
      and p.channel = t.channel
      and p.channelcountry = t.country
      and cast(coalesce(p.style_id,e.web_style_id) as bigint) = cast(t.web_style_num as bigint)
    left join flash f
    on p.event_date_pacific = f.day_date
      and p.channel = f.channel
      and p.channelcountry = f.channel_country
      and p.sku_id = f.sku_num
;

COLLECT STATS ON {product_funnel_t2_schema}.product_price_funnel_daily  COLUMN( event_date_pacific ,rms_sku_num);
COLLECT STATS ON {product_funnel_t2_schema}.product_price_funnel_daily  COLUMN( event_date_pacific );
COLLECT STATS ON {product_funnel_t2_schema}.product_price_funnel_daily  COLUMN( rms_sku_num);
COLLECT STATS ON {product_funnel_t2_schema}.product_price_funnel_daily  COLUMN( channelcountry);

SET QUERY_BAND = NONE FOR SESSION;
