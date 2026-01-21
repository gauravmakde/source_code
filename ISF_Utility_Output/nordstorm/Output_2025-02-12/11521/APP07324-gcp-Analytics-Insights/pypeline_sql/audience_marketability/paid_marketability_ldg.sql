-- Stakeholders: Sophie Wang, Amanda Van Orsdale, Sushmitha Palleti, Nicole Miao
-- Creation/Modification Date: 05/05/2023
--Definine new S3 for output
create table if not exists {hive_schema}.paid_marketability
(
  acp_id                 string
, loyaltyid              string
, iscardmember           integer
, nordylevel             string
, channelcountry         string
, channel                string
, platform               string
, utm_source             string
, finance_detail         string
, converted              string
, order_channelcountry   string
, order_channel          string
, order_platform         string
, order_date_pacific     date
, demand                 decimal(38,2)
, event_date_pacific     date
)
using PARQUET
location 's3://{s3_bucket_root_var}/paid_marketability/'
partitioned by (event_date_pacific);

--eligible_touches: creates a view of all touches over the eligible time period.
--This will filter out any arrivals that lack marketing information as we only want paid Marketing touches.
--This also de-duplicates instances where we see consecutive marketing arrivals from the same campaign in quick succession.
--To avoid over-crediting these campaigns, we limit touches from a single user/campaign to one every 30 mins.
--De-dupe arrivals are those that came to the site from the same source within 30 mins of each other
--Ex: someone clicks on the same paid link twice to navigate to a specific product
--Only dedupe where we have specific utm info. ex: someone could search multiple different items and arrive on site several times. Each different search should result in separate SHOPPING arrival.
--We pull headers into a field called header_keys and can check against this to see if any of the test keys are present.

create or replace temporary view eligible_marketing_touches as
(
  select
        event_id
        , event_timestamp_utc
        , cust_id_type
        , cust_id
        , channelcountry
        , channel
        , platform
        , utm_channel
        , utm_source
        , event_date_pacific
    from
    (
        select
            event_id
            , event_timestamp_utc
            , cust_id_type
            , cust_id
            , channelcountry
            , channel
            , platform
            , utm_channel
            , utm_source
            , case when event_timestamp_utc - interval '30' minute < lag(event_timestamp_utc) over (
                partition by cust_id, channelcountry, channel, platform, unique_campaign_key
                order by  event_timestamp_utc asc, event_id asc) and dedup_potenial = 1 then 0 else 1
            end eligible_touch
            , event_date_pacific
        from
        (
            select
                  event_id
                , event_timestamp_utc
                , cust_id_type
                , cust_id
                , channelcountry
                , channel
                , platform
                , utm_channel
                , utm_campaign
                , utm_source
                , sp_campaign
                , case
                    when utm_channel is null and referrer in ('google','bing','yahoo') then 'SEO_SEARCH'
                    else 'paid'
                end as mktg_type
                , case
                    when utm_channel is not null and (utm_campaign is not null or sp_campaign is not null) then 1
                    else 0
                end as dedup_potenial
                , utm_channel||'::'||utm_campaign||'::'||sp_campaign as unique_campaign_key
                , event_date_pacific
            from(
                select
                    event_id
                    , event_timestamp_utc
                    , cust_id_type
                    , cust_id
                    , channelcountry
                    , channel
                    , platform
                    , coalesce(requested_destination, actual_destination) as url_destination
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'utm_channel') as utm_channel
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'utm_campaign') as utm_campaign
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'sp_campaign') as sp_campaign
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'utm_source') as utm_source
                    , case
                        when lower(referrer) like '%google%' then 'google'
                        when lower(referrer) like '%bing%' then 'bing'
                        when lower(referrer) like '%yahoo%' then 'yahoo'
                      end as referrer
                    , event_date_pacific
                 from acp_event_intermediate.funnel_events_arrived
                where event_date_pacific between {start_date} and {end_date}
                and coalesce(lower(identified_bot),'false') = 'false'
                and channel in ('FULL_LINE','RACK')
                and not arrays_overlap(header_keys,array('Nord-Load','nord-load','nord-test','Nord-Test'))
                and (coalesce(requested_destination, actual_destination) like '%utm_channel%'
                or referrer is not null)
            ) as arrive_base
            where utm_channel is not null or referrer is not null
        ) as arrived
    )a
    where eligible_touch = 1
);

--icon_map: set a baseline shopper/member id mapping to icon id where available.
--This will be the default icon id used in all mappings to acp so we can ensure as many shopper ids
--are mapped to the same acp id as possible. This logic also dedupes instances of multiple shopper-icon
--pairings, picking the most recent based on sourceupdatedate.
--substring_index = split_part, 1 gets you first, -1 gets you last
--create a row for each item in the programsIndex array, where
--shopper id and member id live if present.
--shopper id: (substring_index(pi,'::',1)='WEB'
--member id:  or substring_index(pi,'::',1)='NRHL')
create or replace temporary view cust_icon_map as
(
    select
          customerid as iconid
        , customerid_type
        , cust_id
        , loyaltyid
    from
    (
        select
             uniquesourceid
            ,customerid
            , customerid_type
            , substring_index(pi,'::',-1) as cust_id
            , loyaltyid
            , row_number() over (partition by pi order by sourceupdatedate desc, customerid asc) as row_num
        from
        (
            select
                customerid
                ,uniquesourceid
                ,substring_index(uniquesourceid,'::',1) customerid_type
                ,entitlements.programs.mtlylty as loyaltyid
                ,explode_outer(entitlements.programsIndex) as pi
                ,sourceupdatedate
            from object_model.customer_tokenized
        )
        where
        (substring_index(pi,'::',1)='WEB'
        or substring_index(pi,'::',1)='NRHL')
        and customerid_type='icon'
    ) where row_num = 1
);

--acp_view: return mappings of acp to icon, member id (legacy rack) and transactions.
--Legacy rack acp mappings were brought in from legacy systems prior to account merging.
--Customers who have mapped their Rack account to their icon account will have that mapping
--present in icon_map, and this will catch those who don't. source ids (icon, ertm, member) should
--only have one acp id, but there is currently a bug where there could be several. This logic dedupes
--on arbitrary criteria to account for this bug as we wait for a fix.
create or replace temporary view cust_acp_view as
(
    select
        acpprofileid as acp_id
        , customerid
        , customerid_type
    from(
        select
            cpa.acpprofileid
            , cpa.uniquesourceid
            , substring_index(cpa.uniquesourceid,'::',-1) as customerid
            , substring_index(cpa.uniquesourceid,'::',1) as customerid_type
            , row_number() over (partition by cpa.uniquesourceid order by acpprofileid asc) row_num
        from
        acp_vector.customer_profile_association cpa
        where (cpa.businessdate is null
               or cpa.businessdate between {start_date} and {end_date})
        and substring_index(cpa.uniquesourceid,'::',1) in ('COM','icon','ertm')
    ) where row_num = 1
);

--used to dedupe instance where there are multiple authentications to a single guest shopper map the authentication to all arrivals prior
--We will see a product join here when there are multiple auths, but the row_num handles deduping based on earliest authentication
create or replace temporary view remap_arrived_cust_ids as
(
    select
          arrive.event_id
        , arrive.event_timestamp_utc
        , arrive.cust_id
        , icon.iconid
        , acp.acp_id
        , icon.loyaltyid
        , arrive.channelcountry
        , arrive.channel
        , arrive.platform
        , arrive.utm_channel
        , arrive.utm_source
        , arrive.event_date_pacific
    from
    (
        select
              t.event_id
            , t.event_timestamp_utc
            , coalesce(a.cust_id_type, t.cust_id_type) as cust_id_type
            , coalesce(a.cust_id, t.cust_id) as cust_id
            , t.channelcountry
            , t.channel
            , t.platform
            , t.utm_channel
            , t.utm_source
            , t.event_date_pacific
            , row_number() over (partition by t.event_id order by a.event_timestamp_utc asc, a.cust_id asc, t.cust_id asc, a.iconid asc) row_num
        from eligible_marketing_touches t
        left join acp_event_intermediate.funnel_events_authenticated a
        on t.cust_id = a.cust_id_before
        and t.platform = a.platform
        and t.event_timestamp_utc <= a.event_timestamp_utc
        and a.event_date_pacific between {start_date} and {end_date}
    ) arrive
    left join cust_icon_map icon
    on arrive.cust_id = icon.cust_id
    left join cust_acp_view acp
    on coalesce(icon.iconid, arrive.cust_id) = acp.customerid
    where row_num = 1
);


--final_touches_paid: join utm_channel_lookup and marketing_channel_hierarchy to get mapped finance_detail on utm_channel
--only filtering for customer level paid marketing touches
create  or replace temporary view cust_touches_paid as
(
    select
          o.cust_id
        , o.iconid
        , o.acp_id
        , o.loyaltyid
        , o.channelcountry
        , o.channel
        , case when o.platform in ('IOS','ANDROID') then 'APP' else 'WEB' end as platform
        , o.event_date_pacific
        , o.utm_channel
        , o.utm_source
        , mch.finance_detail
    from remap_arrived_cust_ids o
    LEFT JOIN ace_etl.utm_channel_lookup ucl
    ON ucl.utm_mkt_chnl = o.utm_channel
    LEFT JOIN ace_etl.marketing_channel_hierarchy mch
    ON mch.join_channel = ucl.bi_channel
    where mch.finance_detail in ('AFFILIATES', 'SHOPPING', 'PAID_SEARCH_BRANDED', 'PAID_SEARCH_UNBRANDED','DISPLAY', 'SOCIAL_PAID', 'VIDEO')
);

--joining cardmember data and nordylevel data on loyaltid
create or replace temporary view market_paid_touches_all as
(
SELECT
          o.cust_id
        , o.iconid
        , o.acp_id
        , o.loyaltyid
        , l.iscardmember
        , l.nordylevel
        , o.channelcountry
        , o.channel
        , o.platform
        , o.event_date_pacific
        , o.utm_source
        , o.finance_detail
FROM cust_touches_paid o
left join (
SELECT
value.customerid,
value.loyaltyid,
case when value.cardholderstatus.iscardholder = true then 1 else 0 end as iscardmember,
value.nordylevel.nordylevel as nordylevel
FROM object_model.loyalty_member_avro
)l
on o.loyaltyid = l.loyaltyid
);

--cust_order_details: pull all order with an arrived touch from mta_touches, which has both store and online sales joining arrived touches at customer level
--filtering for customer who only had an arrived touches (arrived_channel is not null) and had a marketing touch
--filtering for demand > 5 to exclude gifts or sampl orders which would have demand between 0 to 1
create or replace temporary view cust_order_details as
(
    select
      shopper_id
    , icon_id
    , acp_id
    , 'Y' as converted
    , order_channelcountry
    , order_channel
    , case when order_platform in ('IOS','ANDROID') then 'APP'
           when order_platform = 'STORE' then 'STORE' else 'WEB' end as order_platform
    , order_date_pacific
    , sum(demand) as demand
    FROM ace_etl.mta_touches
    where order_date_pacific between {start_date} and {end_date}
    and marketing_touch  = 'Y'
    and arrived_channel is not null
    and demand > 5
    group by 1,2,3,4,5,6,7,8

);

--cust_marketability_paid: final mapping of arrival touches to conversions.
create or replace temporary view cust_marketability_paid as
(
    SELECT
          a.cust_id
        , a.iconid
        , a.acp_id
        , a.loyaltyid
        , a.iscardmember
        , a.nordylevel
        , a.channelcountry
        , a.channel
        , a.platform
        , a.event_date_pacific
        , a.utm_source
        , a.finance_detail
        , o.converted
        , o.order_channelcountry
        , o.order_channel
        , o.order_platform
        , o.order_date_pacific
        , o.demand
    FROM market_paid_touches_all a
    left join cust_order_details o
    on coalesce(o.acp_id,o.shopper_id)= coalesce(a.acp_id,a.cust_id)
    and order_date_pacific >= event_date_pacific
);


-- Writing output to new Hive table
insert overwrite table {hive_schema}.paid_marketability partition (event_date_pacific)
 SELECT

         acp_id
        , loyaltyid
        , iscardmember
        , nordylevel
        , channelcountry
        , channel
        , platform
        , utm_source
        , finance_detail
        , converted
        , order_channelcountry
        , order_channel
        , order_platform
        , order_date_pacific
        , sum(demand) as demand
        , event_date_pacific
    FROM cust_marketability_paid
    where event_date_pacific between {start_date} and {end_date}
    group by acp_id
   , loyaltyid
   , iscardmember
   , nordylevel
   , channelcountry
   , channel
   , platform
   , utm_source
   , finance_detail
   , converted
   , order_channelcountry
   , order_channel
   , order_platform
   , order_date_pacific
   , event_date_pacific

;

-- sync partitions on hive table
MSCK REPAIR TABLE {hive_schema}.paid_marketability;

-- Writing output to S3 CSV
-- This sould match the "sql_table_reference" indicated on the .json file.
insert overwrite table paid_marketability_ldg_output
 SELECT

          acp_id
        , loyaltyid
        , iscardmember
        , nordylevel
        , channelcountry
        , channel
        , platform
        , utm_source
        , finance_detail
        , converted
        , order_channelcountry
        , order_channel
        , order_platform
        , order_date_pacific
        , round(sum(demand),2) as demand
        , event_date_pacific
    FROM cust_marketability_paid
    where event_date_pacific between {start_date} and {end_date}
    group by acp_id
   , loyaltyid
   , iscardmember
   , nordylevel
   , channelcountry
   , channel
   , platform
   , utm_source
   , finance_detail
   , converted
   , order_channelcountry
   , order_channel
   , order_platform
   , order_date_pacific
   , event_date_pacific
;
