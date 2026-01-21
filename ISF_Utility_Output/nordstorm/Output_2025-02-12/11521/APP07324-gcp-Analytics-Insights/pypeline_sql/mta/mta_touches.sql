--Description: this code is the first step in the NAP MTA process, which connects orders
--to any associated arrivals that have marketing values associated. This is later used to
--attribute each marketing channel a user visited based on the value of their order and
--the frequency of that channel's use. To do so this sql must match arrivals to orders using
--a customer's shopper id, icon id or member id for Legacy Rack. While post-rocket rack will
--use shopper id, this logic also maps member id to support legacy data until we are outside a
--window where it is no longer needed.

-- Description: mta_touches
-- Analyst team: Analytics Engineering
-- Modified SQL: 2023-06-15

create table if not exists {hive_schema}.mta_touches (
    order_number string
    , order_channelcountry string
    , order_channel string
    , order_platform string
    , acp_id string
    , shopper_id string
    , icon_id string
    , order_timestamp_utc timestamp
    , style_id string
    , sku_id string
    , is_marketplace string
    , partner_relationship_id string
    , partner_relationship_type string
    , units long
    , demand double
    , arrived_event_id string
    , arrived_timestamp_utc timestamp
    , arrived_date_pacific date
    , arrived_channelcountry string
    , arrived_channel string
    , arrived_platform string
    , marketing_touch string
    , mktg_type string
    , utm_channel string
    , siteid string
    , utm_campaign string
    , sp_campaign string
    , utm_term string
    , utm_content string
    , utm_source string
    , sp_source string
    , gclid string
    , sys_load_timestamp timestamp
    , order_date_pacific date
)
using ORC
location 's3://{s3_bucket_root_var}/mta/touches/'
partitioned by (order_date_pacific);

msck repair table {hive_schema}.mta_touches;

create or replace temporary view org_dept_orc USING orc OPTIONS (path "s3://nap-org-app-prod/as-is/org/dept/");

--eligible_touches: creates a view of all touches that could be applied to an order over the eligible
--time period. This will filter out any arrivals that lack marketing information as applying MTA to
--those touches was out of the initial scope. Instead a base coefficient is applied to each order
--as a part of the subsequent attribution phase of MTA. This also de-duplicates instances where we see
--consecutive marketing arrivals from the same campaign in quick succession. To avoid over-crediting these
--campaigns, we limit touches from a single user/campaign to one every 30 mins.
--
--de-dupe arrivals that came to the site from the same source within 30 mins of each other
--ex: someone clicks on the same email link twice to navigate to a specific product
--only dedupe where we have specific utm info. ex: someone could search multiple different items and
--arrive on site several times. Each different search should result in separate SEO credit.
--requested url is primary but not available in legacy Rack
--facebook, pinterest and instagram referrals removed for v1 as they are attributed to base anyway
--MTA looks for touches in last 30 days for order attribution
--test events have headers indicating this. We pull headers into a field called header_keys and
--can check against this to see if any of the test keys are present.
create or replace temporary view eligible_touches as
(
    select
        event_id
        , event_timestamp_utc
        , cust_id_type
        , cust_id
        , channelcountry
        , channel
        , platform
        , mktg_type
        , utm_channel
        , siteid
        , utm_campaign
        , sp_campaign
        , utm_term
        , utm_content
        , utm_source
        , sp_source
        , gclid
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
            , mktg_type
            , utm_channel
            , siteid
            , utm_campaign
            , sp_campaign
            , utm_term
            , utm_content
            , utm_source
            , sp_source
            , gclid
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
                , siteid
                , utm_campaign
                , sp_campaign
                , utm_term
                , utm_content
                , utm_source
                , sp_source
                , gclid
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
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'siteid') as siteid
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'utm_campaign') as utm_campaign
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'sp_campaign') as sp_campaign
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'utm_term') as utm_term
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'utm_content') as utm_content
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'utm_source') as utm_source
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'sp_source') as sp_source
                    , parse_url(coalesce(requested_destination, actual_destination), 'QUERY', 'gclid') as gclid
                    , case
                        when lower(referrer) like '%google%' then 'google'
                        when lower(referrer) like '%bing%' then 'bing'
                        when lower(referrer) like '%yahoo%' then 'yahoo'
                    end as referrer
                    , event_date_pacific
                from acp_event_intermediate.funnel_events_arrived
                where event_date_pacific between date_sub({start_date},30) and date({end_date})
                and coalesce(lower(identified_bot),'false') = 'false'
                and channel in ('FULL_LINE','RACK')
                and not arrays_overlap(header_keys,array('Nord-Load','nord-load','nord-test','Nord-Test'))
                and (coalesce(requested_destination, actual_destination) like '%utm_channel%'
                or referrer is not null)
            ) as arrive_base
            where utm_channel is not null or referrer is not null
        ) as arrived
    ) where eligible_touch = 1
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
create or replace temporary view icon_map as
(
    select
        customerid
        , customerid_type
        , cust_id
    from
    (
        select
            uniquesourceid
            ,customerid
            , customerid_type
            , substring_index(pi,'::',-1) as cust_id
            , row_number() over (partition by pi order by sourceupdatedate desc, customerid asc) as row_num
        from
        (
            select
                customerid
                ,uniquesourceid
                ,substring_index(uniquesourceid,'::',1) customerid_type
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
create or replace temporary view acp_view as
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

--used to dedupe instance where there are multiple authentications to a single guest shopper
--map the authentication to all arrivals prior
--we will see a product join here when there are multiple auths, but the row_num handles deduping
--based on earliest authentication
create or replace temporary view remap_arrived_cust_ids as
(
    select
        arrive.event_id
        , arrive.event_timestamp_utc
        , acp.acp_id
        , arrive.cust_id_type
        , arrive.cust_id
        , arrive.iconid
        , arrive.channelcountry
        , arrive.channel
        , arrive.platform
        , arrive.mktg_type
        , arrive.utm_channel
        , arrive.siteid
        , arrive.utm_campaign
        , arrive.sp_campaign
        , arrive.utm_term
        , arrive.utm_content
        , arrive.utm_source
        , arrive.sp_source
        , arrive.gclid
        , arrive.event_date_pacific
    from
    (
        select
            t.event_id
            , t.event_timestamp_utc
            , coalesce(a.cust_id_type, t.cust_id_type) as cust_id_type
            , coalesce(a.cust_id, t.cust_id) as cust_id
            , a.iconid
            , t.channelcountry
            , t.channel
            , t.platform
            , t.mktg_type
            , t.utm_channel
            , t.siteid
            , t.utm_campaign
            , t.sp_campaign
            , t.utm_term
            , t.utm_content
            , t.utm_source
            , t.sp_source
            , t.gclid
            , t.event_date_pacific
            , row_number() over (partition by t.event_id order by a.event_timestamp_utc asc, a.cust_id asc, t.cust_id asc, a.iconid asc) row_num
        from eligible_touches t
        left join acp_event_intermediate.funnel_events_authenticated a
        on t.cust_id = a.cust_id_before
        and t.platform = a.platform
        and t.event_timestamp_utc <= a.event_timestamp_utc
        and a.event_date_pacific between date_sub({start_date},30) and date({end_date})
    ) arrive
    left join icon_map icon
    on arrive.cust_id = icon.cust_id
    left join acp_view acp
    on coalesce(icon.customerid, arrive.cust_id) = acp.customerid
    where row_num = 1
);

--nonmerch_depts: used to filter nonmerch transactions out of store transactions

create or replace temporary view nonmerch_depts as (
  select distinct
    cast(deptnumber as smallint) as dept_num
  from org_dept_orc
  where to_date(concat(year, '-', LPAD(month,2,'0'), '-', LPAD(day,2,'0')),'yyyy-MM-dd') = current_date()-2
    and (deptnumber in ('18', '102', '482', '497', '501', '584', '585', '753', '902')
    or hierarchy.divisionnumber in ('70', '100', '800', '900', '96'))
);

-- ertm: must be filtered by busineess_date to prevent performance issues on full scan
create or replace temp view ertm
as select * from object_model.enterprise_retail_transaction
where   businessdate between {start_date} and {end_date}
;

--valid_transactions: There are some inconsistencies in how data shows up in ertm s3 v. ertm teradata.
--Voided and reversed transactions don't actually flow to Teradata and are thus not part of the
--enterprise definition. The logic below attempts to mirror the logic for defining a valid transaction
--so it can be joined in the final pull.
--filter out voids
--filter out reversals
--ensuring we filter out restaurant sales
create or replace temporary view valid_transactions as (
    select
        businessdate,
        transactionidentifier.transactionid as transactionid,
        salescollectheader.transactionversionnumber as tranvernum,
        min(salescollectheader.globaltransactionid) as globaltranid
    from ertm
    left anti join (
        select distinct
            businessdate,
            voidafteritem.voidtransactionidentifier as transactionid
    from ertm
    where salescollectheader.transactiontypecode = 'VOID'
    ) voids
    on ertm.businessdate = voids.businessdate
    and ertm.transactionidentifier.transactionid = voids.transactionid
    left anti join (
        select distinct
            businessdate,
            transactionidentifier.transactionid as transactionid,
            salescollectheader.transactionversionnumber as tranvernum
        from ertm
        where salescollectheader.transactiontypecode in ('SALE', 'RETN', 'EXCH')
        and coalesce(salescollectheader.transactionreversalcode, 'N') = 'Y'
    ) reversals
    on ertm.businessdate = reversals.businessdate
    and ertm.transactionidentifier.transactionid = reversals.transactionid
    and ertm.salescollectheader.transactionversionnumber = reversals.tranvernum
    where salescollectheader.transactiontypecode in ('SALE', 'RETN', 'EXCH')
    and coalesce(salescollectheader.transactionreversalcode, 'N') = 'N'
    and coalesce(salescollectheader.datasourcecode, '') != 'RPOS'
    and merchandiseitems is not null
    and cardinality(merchandiseitems) > 0
    group by 1,2,3
);

--store_transactions: formats the transaction data so that it can be unioned to orders. Also
--filters out online orders, while making sure to both filter out bopus (included in demand) and
--keep store initiated online orders (not part of demand). Again we default to icon mapping to acp
--to try to keep consistent with touches and orders, but fall back to the transaction acp if an
--icon id is not present on the order.
--convert to utc for consistency with orders and touches
--logic used to bucket bopus into web channels
--this will filter down customer ids to only show icon ids without filtering transactions missing it
--remove returns
--if we have an icon id, use that to get acp. Otherwise use tran id
--filters to only show FULL LINE, RACK, FL CANADA and RACK CANADA, respectively
create or replace temporary view store_transactions as
(
    select
        globaltranid as order_number
        , left(storeplanningchannel.marketcode,2) as channelcountry
        , st.adminhier.budesc as channel
        , 'STORE' as platform
        , acp.acp_id
        , NULL as shopper_id
        , icon_id as icon_id
        , NULL as style_id
        , sku_id
        , NULL as is_marketplace
        , NULL as partner_relationship_id
        , NULL as partner_relationship_type
        , to_utc_timestamp(transactiontimestamp,'US/Pacific') as event_timestamp_utc
        , ertm_final.businessdate as event_date_pacific
        , count(*) as units
        , sum(demand) as demand
    from
    (
        select
            globaltranid
            , businessdate
            , transactiontimestamp
            , icon_id.customerid as icon_id
            , case when lower(datasourcecode) = 'com'
                and lower(ordertypecode) like 'custinit%'
                and lower(fulfillmenttypecode) = 'storepickup' then retail_store
                else intent_store
            end as storenumber
            , sku_id
            , amount + empdisc as demand
        from
        (
            select
                businessdate
                ,salescollectheader.globaltransactionid as globaltranid
                ,salescollectheader.datasourcecode as datasourcecode
                ,cast(to_timestamp(salescollectheader.transactiondate,
                    'yyyy-MM-dd\'T\'HH:mm:ss.SSSXX') as date) as trandate
                ,to_timestamp(salescollectheader.transactiontimestamp,
                    'yyyy-MM-dd\'T\'HH:mm:ssXXX') as transactiontimestamp
                ,filter(customerids.customerids, x -> x.customeridtype = 'ENT_CUST')[0] as icon_id
                ,cast(salescollectheader.retailstoreid as smallint) as retail_store
                , miscitem.ndirordernumber as ordernumber
                ,explode(merchandiseitems) as merchitems
                ,cast(merchitems.lineitemnumber as smallint) as lineitemnum
                ,cast(merchitems.intentstorenumber as smallint) as intent_store
                ,merchitems.fulfillmenttypecode as fulfillmenttypecode
                ,merchitems.ordertypecode as ordertypecode
                ,merchitems.itemid as sku_id
                ,cast(merchitems.posdepartmentid as smallint) as pos_dept_num
                ,merchitems.amount.currencycode as currencycode
                ,merchitems.amount.amount as amount
                ,coalesce(merchitems.employeediscountamount.amount, 0) as empdisc
            from ertm
        ) ertm_dtl
        left anti join nonmerch_depts
        on ertm_dtl.pos_dept_num = nonmerch_depts.dept_num
        left semi join valid_transactions
        on ertm_dtl.globaltranid = valid_transactions.globaltranid
        and ertm_dtl.businessdate = valid_transactions.businessdate
        where ertm_dtl.amount > 0
    ) ertm_final
    left join acp_view acp
    on coalesce(icon_id,ertm_final.globaltranid) = acp.customerid
    left join org.store st
    on ertm_final.storenumber = cast(st.storenumber as smallint)
    where st.adminhier.bunumber in ('1000','2000','9000','9500')
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
);

--order_details: pull online orders. Used instead of the transaction so that we can include
--orders that have not yet shipped (demand). Filtered to remove fraud, bots, tests. Joined on acp
--in following priority: icon/member id from icon_map (for consistency with touches) > icon id from order >
--shopper/member id from order. Last is mainly used to capture rack orders where icon is not present.
create or replace temporary view order_details as
(
    select
        o.order_number
        , o.channelcountry
        , case when o.channel = 'FULL_LINE' and o.channelcountry = 'CA' then 'N.CA'
            when o.channel = 'FULL_LINE' and o.channelcountry = 'US' then 'N.COM'
            when o.channel = 'RACK' then 'RACK.COM'
            else 'UNKNOWN'
        end as channel
        , o.platform
        , acp.acp_id
        , o.shopper_id
        , case when icon.customerid_type = 'icon' then icon.customerid
            else o.icon_id
        end as icon_id
        , o.style_id
        , o.sku_id
        , o.is_marketplace
        , o.partner_relationship_id
        , o.partner_relationship_type
        , o.event_timestamp_utc
        , o.event_date_pacific
        , count(*) as units
        , sum(o.current_price + o.employee_discount) demand
    from acp_event_intermediate.funnel_events_order_submitted o
    left join
    (
        select
            orderLineId
        from acp_event_intermediate.funnel_events_order_cancelled
        where event_date_pacific between date({start_date}) and date({end_date})
        and cancel_reason like '%FRAUD%'
    ) c
    on o.orderLineId = c.orderLineId
    left join icon_map icon
    on o.shopper_id = icon.cust_id
    left join acp_view acp
    on coalesce(icon.customerid, o.icon_id, o.shopper_id) = acp.customerid
    where o.event_date_pacific between date({start_date}) and date({end_date})
    and o.platform <> 'POS'
    and coalesce(lower(o.identified_bot),'false') = 'false'
    and not arrays_overlap(o.header_keys,array('Nord-Load','nord-load','nord-test','Nord-Test'))
    and c.orderLineId is null
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
);

--order_to_touch: final mapping of arrival events to orders/transactions. Looks for any arrive
--touches that occurred 30 days prior to the order and creates a row for that order item/touch combo.
--Coefficients will later be applied to each item to attribute a percentage of demand to that touch,
--but that doesn't happen in this job. We also add a "Base" row for each order/item so base propensity
--can be applied to that "touch".
--add a row for base by which to apply attr of base to order
create or replace temporary view order_to_touch as
(
    select
        o.order_number
        , o.channelcountry as order_channelcountry
        , o.channel as order_channel
        , o.platform as order_platform
        , coalesce(o.acp_id, a.acp_id) as acp_id
        , o.shopper_id as shopper_id
        , coalesce(o.icon_id, a.iconid) as icon_id
        , o.event_timestamp_utc as order_timestamp_utc
        , o.style_id
        , o.sku_id
        , o.is_marketplace
        , o.partner_relationship_id
        , o.partner_relationship_type
        , o.units
        , o.demand
        , o.event_date_pacific order_date_pacific
        , a.event_id as arrived_event_id
        , a.event_timestamp_utc as arrived_timestamp_utc
        , a.channelcountry as arrived_channelcountry
        , a.channel as arrived_channel
        , a.platform as arrived_platform
        , a.mktg_type
        , a.utm_channel
        , a.siteid
        , a.utm_campaign
        , a.sp_campaign
        , a.utm_term
        , a.utm_content
        , a.utm_source
        , a.sp_source
        , a.gclid
        , a.event_date_pacific arrived_date_pacific
        , 'Y' as marketing_touch
    from
    (
        select * from order_details o
        union all
        select * from store_transactions st
    ) o
    inner join remap_arrived_cust_ids a
    on coalesce(o.acp_id,o.shopper_id)=coalesce(a.acp_id,a.cust_id)
    and a.event_timestamp_utc
        between o.event_timestamp_utc - interval '30' day and o.event_timestamp_utc
    union all
    select
        o.order_number
        , o.channelcountry as order_channelcountry
        , o.channel as order_channel
        , o.platform as order_platform
        , o.acp_id as acp_id
        , o.shopper_id as shopper_id
        , o.icon_id as icon_id
        , o.event_timestamp_utc as order_timestamp_utc
        , o.style_id
        , o.sku_id
        , o.is_marketplace
        , o.partner_relationship_id
        , o.partner_relationship_type
        , o.units
        , o.demand
        , o.event_date_pacific order_date_pacific
        , null as arrived_event_id
        , null as arrived_timestamp_utc
        , null as arrived_channelcountry
        , null as arrived_channel
        , null as arrived_platform
        , 'BASE' as mktg_type
        , 'BASE' utm_channel
        , null as siteid
        , null as utm_campaign
        , null as sp_campaign
        , null as utm_term
        , null as utm_content
        , null as utm_source
        , null as sp_source
        , null as gclid
        , null as arrived_date_pacific
        , 'N' as marketing_touch
    from
    (
        select * from order_details o
        union all
        select * from store_transactions st
    ) o
);

--Final select. Joins in marketing hierarchy as that is used to score and attribute marketing touches.
--As a result changes in the hierarchy are not reflected in historical data unless it is backfilled.
create or replace temp view mta_touches_output
as
select
    o.order_number
    , o.order_channelcountry
    , o.order_channel
    , o.order_platform
    , o.acp_id
    , o.shopper_id
    , o.icon_id
    , o.order_timestamp_utc
    , o.style_id
    , o.sku_id
    , o.is_marketplace
    , o.partner_relationship_id
    , o.partner_relationship_type
    , o.units
    , cast(o.demand as double) demand
    , o.arrived_event_id
    , o.arrived_timestamp_utc
    , o.arrived_date_pacific
    , o.arrived_channelcountry
    , o.arrived_channel
    , o.arrived_platform
    , o.marketing_touch
    , o.mktg_type
    , o.utm_channel
    , o.siteid
    , o.utm_campaign
    , o.sp_campaign
    , o.utm_term
    , o.utm_content
    , o.utm_source
    , o.sp_source
    , o.gclid
    , current_timestamp() as sys_load_timestamp
    , o.order_date_pacific
from order_to_touch o
;

-- Writing to hive table
insert overwrite table {hive_schema}.mta_touches partition (order_date_pacific)
select /*+ REPARTITION(100) */ * from mta_touches_output
; 

