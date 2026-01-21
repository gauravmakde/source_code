
/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=08805;
     DAG_ID=marketability_last12month_11521_ACE_ENG;
     Task_Name=marketability_last12month;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_MKTG_AUDIENCE.CUSTOMER_MARKETABILITY_LTM
Team/Owner: Customer Analytics/Nicole Miao
Date Created/Modified: Dec 6, 2023


Note:
-- What is the the purpose of the table: The purpose of this table is to create an one-stop view, containing marketable/reachable information for four marketing channels (Paid, Email, DM, and Site), as well as customer attributes on the ACP_ID level.
-- What is the update cadence/lookback window: Monthly update, 12 months look back window
*/






/* Step 1:
Update AARE Table
*/

---- 1. Engagement
create volatile multiset table date_lookup as (
  select
    min(day_date) as ty_start_dt,
    max(day_date) as ty_end_dt,
    ty_start_dt - 1461 as pre_start_dt,
    min(day_date) -1 as pre_end_dt
  from prd_nap_usr_vws.day_cal
  where day_date between current_date() - interval '1' year and current_date()-1
) with data on commit preserve rows;
select * from date_lookup;

-- customer shopped in pre-period
create volatile multiset table pre_purchase as (
  select
    distinct
    -- change bopus intent_store_num to ringing_store_num 808 or 867
    case
      when line_item_order_type = 'CustInitWebOrder'
        and line_item_fulfillment_type = 'StorePickUp'
        and line_item_net_amt_currency_code = 'USD'
        then 808
      when line_item_order_type = 'CustInitWebOrder'
        and line_item_fulfillment_type = 'StorePickUp'
        and line_item_net_amt_currency_code = 'CAD'
        then 867
      else intent_store_num end as store_num,
    dtl.acp_id,
    coalesce(dtl.order_date,dtl.tran_date) as sale_date
  from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
  where coalesce(dtl.order_date,dtl.tran_date) >= (select pre_start_dt from date_lookup)
    and coalesce(dtl.order_date,dtl.tran_date) <= (select pre_end_dt from date_lookup)
    and not dtl.acp_id is null
   and dtl.line_net_usd_amt > 0
   and dtl.tran_type_code in ('SALE','EXCH')
) with data primary index(acp_id) on commit preserve rows;


-- append channel
create volatile multiset table pre_purchase_channel as (
  select
    case
        when str.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
        when str.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
        when str.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
        when str.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
      else null end as channel,
    pre_purchase.acp_id,
    max(sale_date) as latest_sale_date
  from pre_purchase
  inner join prd_nap_usr_vws.store_dim as str
    on pre_purchase.store_num = str.store_num
    and str.business_unit_desc in (
         'FULL LINE',
         'FULL LINE CANADA',
         'N.CA',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK',
         'RACK CANADA',
         'TRUNK CLUB')
  group by 1,2
) with data primary index(acp_id) on commit preserve rows;


-- customer shopped in the reporting period
create volatile multiset table ty_purchase as (
  select
    distinct
    -- change bopus intent_store_num to ringing_store_num 808 or 867
    case
      when line_item_order_type = 'CustInitWebOrder'
        and line_item_fulfillment_type = 'StorePickUp'
        and line_item_net_amt_currency_code = 'USD'
        then 808
      when line_item_order_type = 'CustInitWebOrder'
        and line_item_fulfillment_type = 'StorePickUp'
        and line_item_net_amt_currency_code = 'CAD'
        then 867
      else intent_store_num end as store_num,
    dtl.acp_id,
    coalesce(dtl.order_date,dtl.tran_date) as sale_date
  from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
  where coalesce(dtl.order_date,dtl.tran_date) >= (select ty_start_dt from date_lookup)
    and coalesce(dtl.order_date,dtl.tran_date) <= (select ty_end_dt from date_lookup)
    and not dtl.acp_id is null
   and dtl.line_net_usd_amt > 0
   and dtl.tran_type_code in ('SALE','EXCH')
) with data primary index(acp_id) on commit preserve rows;


-- append channel
create volatile multiset table ty_purchase_channel as (
  select
    case
        when str.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
        when str.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
        when str.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
        when str.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
      else null end as channel,
    ty_purchase.acp_id,
    min(sale_date) as first_sale_date
  from ty_purchase
  inner join prd_nap_usr_vws.store_dim as str
    on ty_purchase.store_num = str.store_num
    and str.business_unit_desc in (
         'FULL LINE',
         'FULL LINE CANADA',
         'N.CA',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK',
         'RACK CANADA',
         'TRUNK CLUB')
  group by 1,2
) with data primary index(acp_id) on commit preserve rows;


-- engaged - 1) existing customers who shopped a new channel this year
create volatile multiset table existing_cust as (
  select
    ty.acp_id,
    ty.channel,
    -- if the acp_id is recognized as new customers, use new customer rule
    case when acq.acp_id is not null then 1 else 0 end as new_customer_flg,
    -- if this channel is used in the past 4 years
    case when pre.latest_sale_date is not null
        and ty.first_sale_date - pre.latest_sale_date <1461
      then 1 else 0 end as old_channel_flg
  from ty_purchase_channel as ty
  -- look for new customers
  left join (
    select acp_id
    from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT
    where aare_status_date between (select ty_start_dt from date_lookup)
    and (select ty_end_dt from date_lookup)) as acq
    on ty.acp_id = acq.acp_id
  left join pre_purchase_channel as pre
    on ty.acp_id = pre.acp_id
    and ty.channel = pre.channel
) with data primary index(acp_id) on commit preserve rows;


-- engaged - 2) new customers who shopped another channel than their acquired channel
create volatile multiset table new_cust as (
select
  ty.acp_id,
  ty.channel
from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT as acq
inner join ty_purchase_channel as ty
  on acq.acp_id = ty.acp_id
  -- shopped another channel than acquired channel
  and acq.aare_chnl_code <> ty.channel
  -- if acquired through return, and shopped only one channel, they donnot count
inner join (
  select acp_id
  from ty_purchase_channel
  group by acp_id
  having count(distinct channel) > 1 ) as multi
  on acq.acp_id = multi.acp_id
where aare_status_date between (select ty_start_dt from date_lookup)
and (select ty_end_dt from date_lookup)
) with data primary index(acp_id) on commit preserve rows;

-- save engaged customer and their original channel
create multiset volatile table ltm_engaged_customer as (
  select
    'LTM' as report_period,
    acp_id,
    channel as engaged_to_channel
  from existing_cust
  where old_channel_flg = 0 and new_customer_flg = 0
  union all
  select
    'LTM' as report_period,
    acp_id,
    channel as engaged_to_channel
  from new_cust
) with data primary index (acp_id);

collect statistics on ltm_engaged_customer column (acp_id);




------- Buyer Flow
drop table date_lookup;
create volatile multiset table date_lookup as (
  select
    min(last_year_day_date_realigned) as ly_start_dt,
    min(day_date) -1 as ly_end_dt,
    min(day_date) as ty_start_dt,
    max(day_date) as ty_end_dt
  from prd_nap_usr_vws.day_cal
  where day_date between current_date() - interval '1' year and current_date()-1
) with data  on commit preserve rows;


-- TY customer and transactional behavior
create volatile multiset table ty_positive as (
  select
    case
        when str.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
        when str.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
        when str.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
        when str.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
      else null end as channel,
    case when channel in ('FLS','NCOM') then 'FP' else 'OP' end as brand,
    acp_id,
    sum(gross_amt) as gross_spend,
    -- remove giftcard purchase in net spend
    sum(non_gc_amt) as non_gc_spend,
    count(distinct trip_id) as trips,
    sum(items) as items
  from (
      select
        dtl.acp_id,
        dtl.line_net_usd_amt as gross_amt,
        case when dtl.nonmerch_fee_code = '6666'
          then 0 else dtl.line_net_usd_amt end as non_gc_amt,
          -- change bopus intent_store_num to ringing_store_num 808 or 867
          case
            when line_item_order_type = 'CustInitWebOrder'
              and line_item_fulfillment_type = 'StorePickUp'
              and line_item_net_amt_currency_code = 'USD'
              then 808
            when line_item_order_type = 'CustInitWebOrder'
              and line_item_fulfillment_type = 'StorePickUp'
              and line_item_net_amt_currency_code = 'CAD'
              then 867
            else intent_store_num end as store_num,
        -- use order date is there is one
        coalesce(dtl.order_date,dtl.tran_date) as sale_date,
        -- define trip based on store_num + date
        cast(dtl.acp_id||store_num||sale_date as varchar(150)) as trip_id,
        dtl.line_item_quantity as items
      from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
      where coalesce(dtl.order_date,dtl.tran_date) >= (select ty_start_dt from date_lookup)
        and coalesce(dtl.order_date,dtl.tran_date) <= (select ty_end_dt from date_lookup)
        and not dtl.acp_id is null
        and dtl.line_net_usd_amt > 0
--        and dtl.tran_type_code in ('SALE','EXCH')
    ) as tran
    inner join prd_nap_usr_vws.store_dim as str
        on tran.store_num = str.store_num
      and str.business_unit_desc in (
         'FULL LINE',
         'FULL LINE CANADA',
         'N.CA',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK',
         'RACK CANADA',
         'TRUNK CLUB')
  group by 1,2,3
) with data primary index(acp_id) on commit preserve rows;

create volatile multiset table ty_negative as (
  select
    case
        when str.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
        when str.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
        when str.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
        when str.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
      else null end as channel,
    case when channel in ('FLS','NCOM') then 'FP' else 'OP' end as brand,
    --case when channel in ('OFFPRICE ONLINE','NCOM', 'N.CA') then 'ditital' else 'store' end as digital,
    acp_id,
    sum(line_net_usd_amt) as return_spend
  from (
      select
        dtl.acp_id,
        dtl.line_net_usd_amt,
        -- change bopus intent_store_num to ringing_store_num 808 or 867
        case
          when line_item_order_type = 'CustInitWebOrder'
            and line_item_fulfillment_type = 'StorePickUp'
            and line_item_net_amt_currency_code = 'USD'
            then 808
          when line_item_order_type = 'CustInitWebOrder'
            and line_item_fulfillment_type = 'StorePickUp'
            and line_item_net_amt_currency_code = 'CAD'
            then 867
          else intent_store_num end as store_num
      from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
      where dtl.tran_date >= (select ty_start_dt from date_lookup)
        and dtl.tran_date <= (select ty_end_dt from date_lookup)
        and not dtl.acp_id is null
        and dtl.line_net_usd_amt <= 0
--        and dtl.tran_type_code in ('RETN','EXCH')
    ) as tran
    inner join prd_nap_usr_vws.store_dim as str
        on tran.store_num = str.store_num
      and str.business_unit_desc in (
         'FULL LINE',
         'FULL LINE CANADA',
         'N.CA',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK',
         'RACK CANADA',
         'TRUNK CLUB')
  group by 1,2,3
) with data primary index(acp_id) on commit preserve rows;

-- combine ty
create volatile multiset table ty as (
  select
    coalesce(a.acp_id, b.acp_id) as acp_id,
    coalesce(a.channel, b.channel) as channel,
    coalesce(a.brand, b.brand) as brand,
    coalesce(a.gross_spend, 0) as gross_spend,
    coalesce(a.non_gc_spend, 0) + coalesce(b.return_spend, 0) as net_spend,
    coalesce(a.trips, 0) as trips,
    coalesce(a.items, 0) as items
  from ty_positive as a
  full join ty_negative as b
    on a.acp_id = b.acp_id
    and a.channel = b.channel
    and a.brand = b.brand
) with data primary index(acp_id) on commit preserve rows;


-- LY purchase or return customer, to determine active
create volatile multiset table ly_positive as (
  select distinct acp_id
  from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
  inner join prd_nap_usr_vws.store_dim as str
    on dtl.intent_store_num = str.store_num
  where coalesce(dtl.order_date,dtl.tran_date) >= (select ly_start_dt from date_lookup)
    and coalesce(dtl.order_date,dtl.tran_date) <= (select ly_end_dt from date_lookup)
    and dtl.line_net_usd_amt > 0
    and not dtl.acp_id is null
    and str.business_unit_desc in (
         'FULL LINE',
         'FULL LINE CANADA',
         'N.CA',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK',
         'RACK CANADA',
         'TRUNK CLUB')
) with data primary index(acp_id) on commit preserve rows;

-- negative items
create volatile multiset table ly_negative as (
  select distinct acp_id
  from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
  inner join prd_nap_usr_vws.store_dim as str
    on dtl.intent_store_num = str.store_num
  where dtl.tran_date >= (select ly_start_dt from date_lookup)
    and dtl.tran_date <= (select ly_end_dt from date_lookup)
    and dtl.line_net_usd_amt <= 0
    and not dtl.acp_id is null
    and str.business_unit_desc in (
         'FULL LINE',
         'FULL LINE CANADA',
         'N.CA',
         'N.COM',
         'OFFPRICE ONLINE',
         'RACK',
         'RACK CANADA',
         'TRUNK CLUB')
) with data primary index(acp_id) on commit preserve rows;

-- combine ly
create volatile multiset table ly as (
  select acp_id from ly_positive
  union
  select acp_id from ly_negative
) with data primary index(acp_id) on commit preserve rows;

-- acquired customers
create volatile multiset table new_ty as (
select
  acp_id,
  aare_chnl_code as acquired_channel
--from dl_cma_cmbr.customer_acquisition
from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT
where aare_status_date between (select ty_start_dt from date_lookup)
and (select ty_end_dt from date_lookup)
) with data primary index(acp_id) on commit preserve rows;

-- activated customers
create volatile multiset table activated_ty as (
select
  acp_id,
  activated_channel
from dl_cma_cmbr.customer_activation
where activated_date between (select ty_start_dt from date_lookup)
and (select ty_end_dt from date_lookup)
) with data primary index(acp_id) on commit preserve rows;

-- customer level summary
create multiset volatile table temp_customer_summary as (
  select
    ty.brand,
    ty.channel,
    ty.acp_id,
    ty.gross_spend,
    ty.net_spend,
    ty.trips,
    ty.items,
    case when acq.acp_id is not null then 1 else 0 end as new_flg,
    case when ly.acp_id is not null then 1 else 0 end as retained_flg,
    case when eng.acp_id is not null then 1 else 0 end as eng_flg,
    case when act.acp_id is not null then 1 else 0 end as act_flg
    from ty
    left join ly
      on ty.acp_id = ly.acp_id
    left join ltm_engaged_customer eng
      on ty.acp_id = eng.acp_id
      and ty.channel = eng.engaged_to_channel
    left join new_ty acq
      on ty.acp_id = acq.acp_id
    left join activated_ty act
      on ty.acp_id = act.acp_id
      and ty.channel = act.activated_channel
 ) with data
 primary index (acp_id)
 on commit preserve rows;
collect statistics on temp_customer_summary column (acp_id);

-- drop table purchase_channel;
create multiset volatile table purchase_channel as (
    with cte as (
        select acp_id, count(distinct channel) as channel_cnt
            from temp_customer_summary
            group by 1
    )
    select acp_id,
            TRIM(TRAILING ',' FROM (XMLAGG(TRIM(channel)|| ',' ORDER BY channel) (VARCHAR(10000)))) as channel_ind
--             array_agg(channel order by channel asc,NEW store_array()) as channel_ind
    from temp_customer_summary
    where acp_id in (select distinct acp_id from cte where channel_cnt = 4)
    group by 1
    union all
    select acp_id,
            TRIM(TRAILING ',' FROM (XMLAGG(TRIM(channel)|| ',' ORDER BY channel) (VARCHAR(10000)))) as channel_ind
    from temp_customer_summary
    where acp_id in (select distinct acp_id from cte where channel_cnt = 3)
    group by 1
    union all
    select acp_id,
            TRIM(TRAILING ',' FROM (XMLAGG(TRIM(channel)|| ',' ORDER BY channel) (VARCHAR(10000)))) as channel_ind
    from temp_customer_summary
    where acp_id in (select distinct acp_id from cte where channel_cnt = 2)
    group by 1
    union all
    select acp_id,
            TRIM(TRAILING ',' FROM (XMLAGG(TRIM(channel)|| ',' ORDER BY channel) (VARCHAR(10000)))) as channel_ind
    from temp_customer_summary
    where acp_id in (select distinct acp_id from cte where channel_cnt = 1)
    group by 1
) with data primary index (acp_id, channel_ind) on commit preserve rows;

create multiset volatile table acp_aare as (
select a.*,
       b.channel_ind,
      case when new_flg = 1 and retained_flg = 0 and eng_flg = 0 and act_flg = 0 then 'Acquired'
            when new_flg = 1 and retained_flg = 0 and eng_flg = 0 and act_flg = 1 then 'Acquired'
            when new_flg = 1 and retained_flg = 0 and eng_flg = 1 and act_flg = 1 then 'Acquired'
            when new_flg = 0 and retained_flg = 1 and eng_flg = 0 and act_flg = 0 then 'Retained'
            when new_flg = 0 and retained_flg = 1 and eng_flg = 1 and act_flg = 0 then 'Retained'
            when new_flg = 0 and retained_flg = 1 and eng_flg = 0 and act_flg = 1 then 'Retained'
            when new_flg = 0 and retained_flg = 1 and eng_flg = 1 and act_flg = 1 then 'Retained'
            else 'Reactivted'
            end as aare
from temp_customer_summary a
left join purchase_channel b on a.acp_id = b.acp_id
    ) with data primary index (acp_id) on commit preserve rows;




/* Step 2:
Email Marketability
*/
-- create multiset volatile table email_marketable as (
-- select a.acp_id,
--        b.icon_id,
--        ocp.ocp_id,
--        preference,
--        ocp.event_timestamp
-- from (select distinct acp_id, preference from {mktg_audience_t2_schema}.mktg_email_customer_audience) a
-- left join (select acp_id, cust_id as icon_id
--  	  			from prd_nap_usr_vws.acp_analytical_cust_xref
--  	  			where cust_source = 'icon')b
--  	  		on a.acp_id = b.acp_id
-- left join (select program_index_id as ocp_id, object_event_tmstp as event_timestamp
--  	  			from prd_nap_usr_vws.customer_obj_program_dim
--  	  			where customer_source = 'icon'
--  	  			and program_index_name = 'OCP'
-- 				)ocp
--  	  		on b.icon_id = ocp.ocp_id
-- ) with data on commit preserve rows
-- ;
create multiset volatile table email_distinct as (
select acp_id,
        max(case when fp_ind = 'Y' then 1 else 0 end) as fp_email_marketable_idnt,
        max(case when op_ind = 'Y' then 1 else 0 end) as op_email_marketable_idnt
  from t2dl_das_mktg_operations.email_marketability_vw
  where marketability_ind = 'Y'
      and acp_id is not null
  group by 1
    ) with data primary index (acp_id) on commit preserve rows;




/* Step 3:
Site Marketability
*/
--- Site ACP IDs
-- drop table site_marketable;
create multiset volatile table site_marketable as (
select xref.session_shopper_id as shopper_id,
       xref.session_icon_id as icon_id,
       xref.session_acp_id as acp_id,
       f.channel,
       f.experience,
       f.channelcountry,
       count(distinct f.session_id) as session_cnt,
       sum(product_views) as product_views,
       sum(cart_adds) as cart_adds,
       sum(web_orders) as web_orders,
       sum(web_ordered_units) as order_units,
       sum(web_demand_usd) as demand
from (select session_id,
             max(shopper_id) as session_shopper_id,
             max(ent_cust_id) as session_icon_id,
             max(acp_id) as session_acp_id
      from PRD_NAP_USR_VWS.CUSTOMER_SESSION_XREF
      where ent_cust_id like 'icon::%'
      group by 1
      ) xref
join PRD_NAP_USR_VWS.CUSTOMER_SESSION_FACT f on xref.session_id = f.session_id
where 1 = 1
    and activity_date_pacific between current_date() - interval '1' year and current_date()-1
--     and bounce_ind in ('U', 'N')
    and f.experience in ('DESKTOP_WEB', 'MOBILE_WEB')
group by 1,2,3,4,5,6
    ) with data primary index (shopper_id, icon_id, acp_id, channel, experience,channelcountry) on commit preserve rows
;

-- drop table  sm_banner_by_shopper;
create multiset volatile table sm_banner_by_shopper as (
select icon_id,
       acp_id,
        TRIM(TRAILING ',' FROM (XMLAGG(TRIM(site_ind)|| ',' ORDER BY site_ind) (VARCHAR(10000)))) as site_ind
from(
    select distinct icon_id,
            acp_id,
            case when experience in ('DESKTOP_WEB') and channel = 'NORDSTROM' then 'n.com site'
                when experience in ('DESKTOP_WEB') and channel = 'NORDSTROM_RACK' then 'r.com site'
                when experience in ('MOBILE_WEB') and channel = 'NORDSTROM' then 'n.com mow'
                when experience in ('MOBILE_WEB') and channel = 'NORDSTROM_RACK' then 'r.com mow'
                when experience in ('IOS_APP', 'ANDROID_APP') and channel = 'NORDSTROM' then 'n.com app'
                when experience in ('IOS_APP', 'ANDROID_APP') and channel = 'NORDSTROM_RACK' then 'r.com app'
            end as site_ind
    from site_marketable
    ) sub
group by 1,2
) with data primary index (icon_id, acp_id) on commit preserve rows
;

-- drop table  sm_channel_by_shopper;
 create multiset volatile table sm_channel_by_shopper as (
  select icon_id,
         acp_id,
          TRIM(TRAILING ',' FROM (XMLAGG(TRIM(channel)|| ',' ORDER BY channel) (VARCHAR(10000)))) as site_channel_ind
  from(
      select distinct icon_id,
              acp_id,
                channel
      from site_marketable
      ) sub
  group by 1,2
) with data primary index (icon_id, acp_id) on commit preserve rows;


create multiset volatile table hqyy_site_marketability_0 as (
select sm.*,
       sbbs.site_ind,
        scbs.site_channel_ind
from site_marketable sm
left join sm_banner_by_shopper sbbs on sm.icon_id = sbbs.icon_id--(sm.icon_id = sbbs.icon_id or sm.acp_id = sbbs.acp_id )
left join sm_channel_by_shopper scbs on sm.icon_id = scbs.icon_id
) with data
primary index (shopper_id, icon_id, acp_id, channel, experience,channelcountry)
on commit preserve rows;


create volatile multiset table acp_fy_cohort_19_thru_22 as (
      select
    acp_id,
    case when engagement_cohort in ('Acquire & Activate','Acquired Mid-Qtr') then 'Acquire & Activate'
        else engagement_cohort end as engagement_cohort
    from t2dl_das_aec.audience_engagement_cohorts
  where execution_qtr in (select max(execution_qtr) from t2dl_das_aec.audience_engagement_cohorts)
) with data primary index(acp_id,engagement_cohort) on commit preserve rows;

create multiset volatile table hqyy_site_marketability as (
with aare_dat as (
    select acp_id,
           channel_ind,
           aare,
           sum(coalesce(gross_spend,0)) as gross_spend,
           sum(trips) as total_trips
    from acp_aare
    group by 1,2,3
)
    select distinct *
    from
(select sm.*,
       squad.engagement_cohort,
       a.channel_ind as historic_shopping_channel,
       a.aare,

       --- customer attributes
       cal.loyalty_level,
       cal.is_loyalty_member,
       cal.is_cardmember,
       cal.member_enroll_country_code

from hqyy_site_marketability_0 sm
left join  acp_fy_cohort_19_thru_22 squad on sm.acp_id = squad.acp_id
left join aare_dat a on a.acp_id = sm.acp_id
left join T2DL_das_cal.customer_attributes_loyalty cal on sm.acp_id = cal.acp_id
    )  sub
    ) with data
    primary index (icon_id, acp_id)
    on commit preserve rows
;

create multiset volatile table sm_distinct as (
select distinct acp_id,
                site_ind,
                site_channel_ind,
                session_cnt,
                product_views,
                cart_adds,
                web_orders,
                order_units,
                demand
from  hqyy_site_marketability
    ) with data primary index (acp_id) on commit preserve rows;



/*Step 3.5:
  Pull Last 1,2,4 year cohort
*/


create multiset volatile table cust_l1y as (
    select distinct acp_id
    from t2dl_das_cal.customer_attributes_transactions
    where net_spend_1year_jwn >0
) with data primary index (acp_id) on commit preserve rows;


create multiset volatile table cust_l2y as (
    select distinct acp_id
    from t2dl_das_cal.customer_attributes_transactions
    where net_spend_2year_jwn >0
) with data primary index (acp_id) on commit preserve rows;

create multiset volatile table cust_l4y as (
    select distinct acp_id
    from t2dl_das_cal.customer_attributes_transactions
    where net_spend_4year_jwn >0
) with data primary index (acp_id) on commit preserve rows;


create multiset volatile table ltm_shopping_history as (
select acp_id,
oreplace(
        trim(
            trim(
            trim(coalesce(active_1year_ncom, '') ||' '||
                coalesce(active_1year_nstores,'')) || ' '||
                coalesce(active_1year_rcom,'')) ||' '||
                coalesce(active_1year_rstores, '')
            ), ' ', ','
        ) as historic_shopping_channel,
       sum(trips_1year_jwn) as ltm_total_trips,
       sum(gross_spend_1year_jwn) as ltm_gross_spend
from (
select acp_id,
       case when active_1year_nstores = 1 then 'FLS' end as active_1year_nstores,
       case when active_1year_ncom = 1 then 'NCOM' end as active_1year_ncom,
       case when active_1year_rstores = 1 then 'RACK' end as active_1year_rstores,
       case when active_1year_rcom = 1 then 'NRHL' end as active_1year_rcom,
       net_spend_1year_jwn,
       trips_1year_jwn,
       gross_spend_1year_jwn
from t2dl_das_cal.customer_attributes_transactions
where net_spend_1year_jwn >0
) sub group by 1,2
    ) with data primary index (acp_id) on commit preserve rows;



/* Step 4:
Combine full table
*/
create multiset volatile table icon_list as (
    select distinct acp_id from hqyy_site_marketability
    union
    select distinct acp_id from {mktg_audience_t2_schema}.mktg_dm_customer_audience
    union
    select distinct acp_id from email_distinct
    union
    select distinct acp_id from T2DL_DAS_MKTG_AUDIENCE.paid_marketability
    union
    select distinct acp_id from cust_l1y
    union
    select distinct acp_id from cust_l2y
    union
    select distinct acp_id from cust_l4y
) with data primary index (acp_id) on commit preserve rows;

create multiset volatile table multi_media_base as (
    select main.acp_id,
           ---- Last 1 Year
            case when l1y.acp_id is not null then 1 else 0 end as l1y_purchase_idnt,

           ---- Last 2 Year
            case when l2y.acp_id is not null then 1 else 0 end as l2y_purchase_idnt,

           ---- Last 4 Year
            case when l4y.acp_id is not null then 1 else 0 end as l4y_purchase_idnt,


           ---- DM
           case when dm.acp_id is not null then 1 else 0 end as directmail_idnt,

           ---- Site
           case when sm.acp_id is not null then site_channel_ind else null end as site_visited_channel,
           case when sm.acp_id is not null then site_ind else null end as site_visited_experience,
           case when sm.acp_id is not null then total_sessions else 0 end as site_visited_sessions,
           case when sm.acp_id is not null then total_pv else 0 end as site_visited_pageviews,
           case when sm.acp_id is not null then total_atb else 0 end as site_visited_atb,
           case when sm.acp_id is not null then total_orders else 0 end as site_converted_orders,
           case when sm.acp_id is not null then total_demand else 0 end as site_converted_demand,


            ---- Paid
            case when paid.acp_id is not null then 1 else 0 end as paid_idnt,
            0 as paid_marketable_idnt,

            ---- Email
            case when email.acp_id is not null then 1 else 0 end as email_idnt,
            -- case when email.acp_id is not null then preference else null end as email_preference_idnt,
            case when email.acp_id is not null then op_email_marketable_idnt else 0 end as op_email_marketable_idnt,
            case when email.acp_id is not null then fp_email_marketable_idnt else 0 end as fp_email_marketable_idnt,
            case when er.acp_id is not null then 1 else 0 end as email_reached_idnt,
            case when ep.acp_id is not null then 1 else 0 end as email_purchased_idnt
    from icon_list main
    left join cust_l1y l1y on l1y.acp_id = main.acp_id
    left join cust_l2y l2y on l2y.acp_id = main.acp_id
    left join cust_l4y l4y on l4y.acp_id = main.acp_id
    left join {mktg_audience_t2_schema}.mktg_dm_customer_audience dm on main.acp_id = dm.acp_id--on dm.icon_id = main.icon_id
    left join
        (select acp_id,
                site_ind,
                site_channel_ind,
                sum(session_cnt) as total_sessions,
                sum(product_views) as total_pv,
                sum(cart_adds) as total_atb,
                sum(web_orders) as total_orders,
                sum(order_units) as total_units,
                sum(demand) as total_demand
         from sm_distinct
         group by 1,2,3) sm on sm.acp_id = main.acp_id
    left join email_distinct email on email.acp_id = main.acp_id
    left join (select distinct acp_id from T2DL_DAS_MKTG_AUDIENCE.paid_marketability) paid on paid.acp_id = main.acp_id --paid.iconid = main.icon_id-- or
    left join (select distinct acp_id from T2DL_DAS_MKTG_AUDIENCE.email_marketability where utm_medium = 'email') er on er.acp_id = main.acp_id
    left join (select distinct acp_id from T2DL_DAS_MKTG_AUDIENCE.email_marketability  where utm_medium = 'email' and converted = 'Y') ep on ep.acp_id = main.acp_id
) with data primary index (acp_id) on commit preserve rows
;


create multiset volatile table hqyy_marketability as (
           with aare_dat as (
            select acp_id,
                   channel_ind,
                   aare,
                   sum(coalesce(gross_spend,0)) as gross_spend,
                   sum(trips) as total_trips
            from acp_aare
            group by 1,2,3
        )
        select base.*,

               --- customer loyalty
--                 case when obj.unique_source_id is not null then 1 else 0 end as is_loyalty_member,
                cal.is_loyalty_member as is_loyalty_member,
                cal.is_cardmember,
                case when cal.is_cardmember = 1
                        then 'CARDMEMBER'
                    when cal.loyalty_level = 'ICON'
                        then 'CARDMEMBER'
                    when cal.is_loyalty_member = 1
                        then 'MEMBER'
                    else 'NON-LOYALTY' end as loyalty_status,
                cal.member_enroll_country_code,
                cal.loyalty_level,

                --- anniversary
                an.anniversary_flag,

                --- customer squad
                squad.engagement_cohort,

                --- customer aare
                ltm.historic_shopping_channel as historic_shopping_channel,
                a.aare,
                ltm.ltm_gross_spend as ltm_gross_spend,
                ltm.ltm_total_trips as ltm_total_trips,

                --- LiveRamp reserved space
                cast(0 as decimal(18,0)) as lr_custom_column1,  -- for aec
                cast(0 as decimal(18,0)) as lr_custom_column2,  -- for loyalty
                cast(0 as decimal(18,0)) as lr_custom_column3   -- for cardmember

        from multi_media_base base
        left join T2DL_das_cal.customer_attributes_loyalty cal on base.acp_id = cal.acp_id
        left join acp_fy_cohort_19_thru_22 squad on base.acp_id = squad.acp_id
        left join aare_dat a on a.acp_id = base.acp_id
        left join ltm_shopping_history ltm on base.acp_id = ltm.acp_id
        left join (select distinct acp_id,
                           case when SHOPPED_LY_ANNIVERSARY = 1 or SHOPPED_TY_ANNIVERSARY = 1 or SHOPPED_LLY_ANNIVERSARY = 1 then 'Anniversary Shopper' else 'Non Anniversary Shopper' end as anniversary_flag
                    from T2DL_DAS_CAL.CUSTOMER_ATTRIBUTES_TRANSACTIONS) an on an.acp_id = base.acp_id
    ) with data
    primary index (acp_id)
    on commit preserve rows;


--- Insert custom additional data from LiveRamp to marketability table
insert into hqyy_marketability
    select a.acp_id,
           l1y_purchase_idnt,
           l2y_purchase_idnt,
           l4y_purchase_idnt,
           a.directmail_idnt,
           a.site_visited_channel,
           site_visited_experience,
           site_visited_sessions,
           site_visited_pageviews,
           site_visited_atb,
           site_converted_orders,
           site_converted_demand,
           paid_idnt,
           paid_marketable_idnt,
           email_idnt,
           op_email_marketable_idnt,            --- Added Oct 6, 2023
           fp_email_marketable_idnt,            --- Added Oct 6, 2023
           email_reached_idnt,
           email_purchased_idnt,
           is_loyalty_member,
           is_cardmember,
           loyalty_status,
           member_enroll_country_code,
           loyalty_level,
           'Non Anniversary Shopper' as anniversary_flag,
           engagement_cohort,
           historic_shopping_channel,
           aare,
           ltm_gross_spend,
           ltm_total_trips,
           cast(lr_custom_column1 as decimal(18,0)) lr_custom_column1,
           cast(lr_custom_column2 as  decimal(18,0)) lr_custom_column2,
           cast(lr_custom_column3 as  decimal(18,0)) lr_custom_column3
    from {mktg_audience_t2_schema}.liveramp_table_last12month a;
collect statistics on hqyy_marketability column (acp_id);



--- finalized the marketability table
create multiset volatile table hqyy_marketability_final as(
      select sub.*,

            --- custom column
            CURRENT_TIMESTAMP as dw_sys_load_tmstp
     from (select distinct * from hqyy_marketability) sub
) with data
    primary index (acp_id)
    on commit preserve rows;
collect statistics on hqyy_marketability_final column (acp_id);


/*
--------------------------------------------
Drop Table and Create a new table
--------------------------------------------
*/
delete from {mktg_audience_t2_schema}.CUSTOMER_MARKETABILITY_LTM
;


insert into {mktg_audience_t2_schema}.CUSTOMER_MARKETABILITY_LTM
  select * from hqyy_marketability_final
;

collect statistics on {mktg_audience_t2_schema}.CUSTOMER_MARKETABILITY_LTM column (acp_id);


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;

