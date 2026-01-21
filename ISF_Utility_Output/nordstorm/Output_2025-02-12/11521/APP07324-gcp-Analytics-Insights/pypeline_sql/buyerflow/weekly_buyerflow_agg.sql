/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP09058;
     DAG_ID=weekly_buyerflow_agg_11521_ACE_ENG;
     Task_Name=weekly_buyerflow_agg;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: {bfl_t2_schema}.WEEKLY_BUYERFLOW_AGG
Team/Owner: Engagement Analytics - Amanda Wolfman + Elisa Olvera
Date Created/Modified: January 19,2023

Note:
-- What is the the purpose of the table - run buyerflow counts weekly and insert into the agg table
-- What is the update cadence/lookback window - it will run the past week + 2 weeks ago

*/



/*
Temp table notes here if applicable
*/

--ENGAGEMENT CODE - LAST WEEK
-- reporting time period
create volatile multiset table date_lookup as (
  select
    week_idnt report_period,
    min(day_date) as ty_start_dt,
    max(day_date) as ty_end_dt,
    ty_start_dt - 1461 as pre_start_dt,
    min(day_date) -1 as pre_end_dt
  from prd_nap_usr_vws.day_cal_454_dim
  where week_idnt = (select max(week_idnt) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date)
group by 1
) with data primary index(report_period) on commit preserve rows;


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
    and dtl.business_day_date between (select pre_start_dt from date_lookup) and  (select pre_end_dt+14 from date_lookup)
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
    and dtl.business_day_date between (select ty_start_dt from date_lookup) and  (select ty_end_dt+14 from date_lookup)
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
--drop table week_num_engaged_customer;

create multiset volatile table week_num_engaged_customer as (
  select
    (select report_period from date_lookup) report_period,
    acp_id,
    channel as engaged_to_channel
  from existing_cust
  where old_channel_flg = 0 and new_customer_flg = 0
  union all
  select
    (select report_period from date_lookup),
    acp_id,
    channel as engaged_to_channel
  from new_cust
) with data primary index (acp_id) on commit preserve rows;

collect statistics on week_num_engaged_customer column (acp_id);

-----END OF ENGAGEMENT CODE


drop table date_lookup;
CREATE multiset volatile TABLE date_lookup AS (
select
  week_idnt report_period,
   min(day_date) ty_start_dt    
   ,max(day_date) ty_end_dt
   ,min(day_date_last_year_realigned) as ly_start_dt
   ,min(day_date) -1 as ly_end_dt
from prd_nap_usr_vws.day_cal_454_dim
where week_idnt = (select max(week_idnt) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date)
group by 1
) with data primary index(ty_start_dt) on commit preserve rows;



CREATE multiset volatile TABLE ty AS (
select dtl.acp_id
    ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
        when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
        when st.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
        when st.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
        else null end as channel
    ,case when channel in ('FLS','NCOM') then 'FP' else 'OP' end as brand
    ,sum(case when dtl.line_net_usd_amt>0 then dtl.line_net_usd_amt
        else 0 end) gross_spend
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt<0 then dtl.line_net_usd_amt
        else 0 end) return_amount
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' then dtl.line_net_usd_amt
        else 0 end) net_spend
    ,count(distinct case when dtl.line_net_usd_amt>0
        then st.store_num||coalesce(dtl.order_date,dtl.tran_date)
        else null end) trips
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt >  0 then dtl.line_item_quantity 
        else 0 end)  AS items
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt <  0 then (-1)*dtl.line_item_quantity
        else 0 end)  AS return_items
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt <  0 then (-1)*dtl.line_item_quantity
        when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt >  0 then dtl.line_item_quantity 
        else 0 end)  AS net_items
from prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW as dtl
  join prd_nap_usr_vws.store_dim as st
    on (case when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp' 
        and dtl.line_item_net_amt_currency_code = 'CAD' then 867
        when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp' 
        and dtl.line_item_net_amt_currency_code = 'USD' then 808
        else dtl.intent_store_num
         end) = st.store_num
  where case when coalesce(dtl.line_net_usd_amt,0)>=0 then coalesce(dtl.order_date,dtl.tran_date)
        else dtl.tran_date end between (select ty_start_dt from date_lookup) and  (select ty_end_dt from date_lookup)
    and dtl.business_day_date between (select ty_start_dt from date_lookup) and  (select ty_end_dt+14 from date_lookup)
    and dtl.acp_id is not null
    and st.business_unit_desc 
        in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE')
group by 1,2,3
)with data unique primary index(acp_id,channel) on commit preserve rows;


CREATE set volatile TABLE ly AS (
select dtl.acp_id
   from prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW as dtl
  join prd_nap_usr_vws.store_dim as st
    on dtl.intent_store_num = st.store_num
where case when coalesce(dtl.line_net_usd_amt,0)>=0 then coalesce(dtl.order_date,dtl.tran_date)
    else dtl.tran_date end between (select ly_start_dt from date_lookup) and  (select ly_end_dt from date_lookup)
    and dtl.business_day_date between (select ly_start_dt from date_lookup) and  (select ly_end_dt+30 from date_lookup)
    and dtl.acp_id is not null
    and st.business_unit_desc 
        in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE')
)with data primary index(acp_id) on commit preserve rows;



-- acquired customers
create volatile multiset table new_ty as (
select
  acp_id,
  aare_chnl_code as acquired_channel
from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT
where aare_status_date between (select ty_start_dt from date_lookup)
and (select ty_end_dt from date_lookup)
) with data primary index(acp_id) on commit preserve rows;

-- activated customers
create volatile multiset table activated_ty as (
select
  acp_id,
  activated_chnl_code activated_channel
from PRD_NAP_USR_VWS.CUSTOMER_ACTIVATED_FACT
where activated_date between (select ty_start_dt from date_lookup)
and (select ty_end_dt from date_lookup)
) with data primary index(acp_id) on commit preserve rows;


-- customer level summary
create multiset volatile table customer_summary as (
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
    left join week_num_engaged_customer eng
      on ty.acp_id = eng.acp_id
      and ty.channel = eng.engaged_to_channel
    left join new_ty acq
      on ty.acp_id = acq.acp_id
    left join activated_ty act
      on ty.acp_id = act.acp_id
      and ty.channel = act.activated_channel
 ) with data primary index (acp_id) on commit preserve rows;



delete from {bfl_t2_schema}.weekly_buyerflow_agg where report_period = (select report_period from date_lookup);


-- channel level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'all' report_type,
    channel,
    '' predicted_segment,
    '' country,
    '' loyalty_status,
    '' nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary
  group by channel;


-- brand level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'all' report_type,
    brand,
    '' predicted_segment,
    '' country,
    '' loyalty_status,
    '' nordy_level,
        count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
    from customer_summary
  group by brand;


-- JWN level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'all' report_type,
    'JWN',
    '' predicted_segment,
    '' country,
    '' loyalty_status,
    '' nordy_level,
        count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary;


-- Loyalty status, based on time period
 create volatile multiset table lyl_status as (
   select
   	acp_id,
    case when cardmember_fl = 1 then 'cardmember'
    when member_fl = 1 then 'member'
    else 'non-member' end as loyalty_status,
 	rewards_level
 	from (
 	select
    DISTINCT ac.acp_id,
     -- member status
     case
       when cardmember_enroll_date <= (select ty_end_dt from date_lookup)
         and (cardmember_close_date >= (select ty_end_dt from date_lookup)
         or cardmember_close_date is null)
       then 1 else 0 end as cardmember_fl,
     case
       when cardmember_fl = 0
         and member_enroll_date <= (select ty_end_dt from date_lookup)
         and (member_close_date >= (select ty_end_dt from date_lookup)
         or member_close_date is null)
       then 1 else 0 end as member_fl,
     -- nordy_level
     -- i think we should include max here for the weird records
	max( case
            when rwd.rewards_level in ('MEMBER') then 1
            when rwd.rewards_level in ('INSIDER','L1') then 2
            when rwd.rewards_level in ('INFLUENCER','L2') then 3
            when rwd.rewards_level in ('AMBASSADOR','L3') then 4
            when rwd.rewards_level in ('ICON','L4') then 5
            else 0 end  ) AS rewards_level
   from prd_nap_usr_vws.analytical_customer as ac
   inner join prd_nap_usr_vws.loyalty_member_dim_vw as lmd
     on ac.acp_loyalty_id = lmd.loyalty_id
   left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw as rwd
   -- date piece in join for nordy
    on ac.acp_loyalty_id = rwd.loyalty_id and  rwd.start_day_date <= (select ty_end_dt from date_lookup)
          	and rwd.end_day_date > (select ty_end_dt from date_lookup)
	group by 1,2,3
    ) as lyl
) with data primary index(acp_id) on commit preserve rows;


-- customer level summary
create volatile multiset table customer_summary_seg as (
  select
    ty.brand,
    ty.channel,
    ty.acp_id,
    case when x.predicted_ct_segment is null then 'unknown' else x.predicted_ct_segment end as predicted_segment,
    ac.country,
 	  case when lyl.loyalty_status is null then 'non-member' else lyl.loyalty_status end as loyalty_status,
  	case when lyl.rewards_level=1 then '1-member'
  		 when lyl.rewards_level=2 then '2-insider'
  		 when lyl.rewards_level=3 then '3-influencer'
  		 when lyl.rewards_level=4 then '4-ambassador'
   		 when lyl.rewards_level=5 then '5-icon'
       when lyl.rewards_level=0 then (case when lyl.loyalty_status='cardmember' then '1-member'
                                           when lyl.loyalty_status='member' then '1-member'
                                           when lyl.loyalty_status='non-member' then '0-nonmember' end)
   		 else '0-nonmember' end as nordy_level,
    ty.gross_spend,
    ty.net_spend,
    ty.trips,
    ty.items,
    ty.new_flg,
    ty.retained_flg,
    ty.eng_flg,
    ty.act_flg
    from customer_summary ty
     --
     left join
     	T2DL_DAS_CUSTOMBER_MODEL_ATTRIBUTE_PRODUCTIONALIZATION.customer_prediction_core_target_segment x
       on ty.acp_id = x.acp_id
--       and x.last_scored_date = date'2021-10-04'
       left join lyl_status lyl
           on ty.acp_id = lyl.acp_id
        left join (
          select
          acp_id,
          -- country
          case when us_dma_code = -1 then null else us_dma_desc end as us_dma,
          case
            when us_dma is not null then 'US'
            when ca_dma_code is not null then 'CA'
          else 'unknown' end as country
          	from prd_nap_usr_vws.analytical_customer ) as ac
         	 on ty.acp_id = ac.acp_id
     ) with data primary index (acp_id) on commit preserve rows;

collect statistics on customer_summary_seg column (acp_id);



-- channel level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'segment' report_type,
    channel,
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary_seg
  group by channel, predicted_segment, country, loyalty_status, nordy_level;


-- brand level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'segment' report_type,
    brand,
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary_seg
  group by brand, predicted_segment, country, loyalty_status, nordy_level;


-- JWN level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'segment' report_type,
    'JWN',
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary_seg
  group by predicted_segment, country, loyalty_status, nordy_level;
 

---CODE FOR TWO WEEKS AGO (CODE ABOVE IS LAST WEEK)

--ENGAGEMENT CODE - TWO WEEKS AGO
-- reporting time period

drop table date_lookup;
create volatile multiset table date_lookup as (
  select
    week_idnt report_period,
    min(day_date) as ty_start_dt,
    max(day_date) as ty_end_dt,
    ty_start_dt - 1461 as pre_start_dt,
    min(day_date) -1 as pre_end_dt
  from prd_nap_usr_vws.day_cal_454_dim
  where week_idnt = (select max(week_idnt) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date-7)
group by 1
) with data primary index(report_period) on commit preserve rows;


-- customer shopped in pre-period
drop table pre_purchase;
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
    and dtl.business_day_date between (select pre_start_dt from date_lookup) and  (select pre_end_dt+14 from date_lookup)
    and not dtl.acp_id is null
   and dtl.line_net_usd_amt > 0
   and dtl.tran_type_code in ('SALE','EXCH')
) with data primary index(acp_id) on commit preserve rows;


-- append channel
drop table pre_purchase_channel;
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
drop table ty_purchase;
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
    and dtl.business_day_date between (select ty_start_dt from date_lookup) and  (select ty_end_dt+14 from date_lookup)
    and not dtl.acp_id is null
   and dtl.line_net_usd_amt > 0
   and dtl.tran_type_code in ('SALE','EXCH')
) with data primary index(acp_id) on commit preserve rows;
 

-- append channel
drop table ty_purchase_channel;
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
drop table existing_cust;
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
drop table new_cust;
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
drop table week_num_engaged_customer; 
create multiset volatile table week_num_engaged_customer as (
  select
    (select report_period from date_lookup) report_period,
    acp_id,
    channel as engaged_to_channel
  from existing_cust
  where old_channel_flg = 0 and new_customer_flg = 0
  union all
  select
    (select report_period from date_lookup),
    acp_id,
    channel as engaged_to_channel
  from new_cust
) with data primary index (acp_id) on commit preserve rows;

collect statistics on week_num_engaged_customer column (acp_id);


-----END OF ENGAGEMENT CODE


drop table date_lookup;
CREATE multiset volatile TABLE date_lookup AS (
select
  week_idnt report_period,
   min(day_date) ty_start_dt
   ,max(day_date) ty_end_dt
   ,min(day_date_last_year_realigned) as ly_start_dt
   ,min(day_date) -1 as ly_end_dt
from prd_nap_usr_vws.day_cal_454_dim
where week_idnt = (select max(week_idnt) from prd_nap_usr_vws.day_cal_454_dim where week_end_day_date <= current_date-7)
group by 1
) with data primary index(ty_start_dt) on commit preserve rows;



drop table ty;
CREATE multiset volatile TABLE ty AS (
select dtl.acp_id
    -- ,cal.month_num fiscal_month --OPTIONAL: to group by a fiscal time-period
    ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
          when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
          when st.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
          when st.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
          else null end as channel
    ,case when channel in ('FLS','NCOM') then 'FP' else 'OP' end as brand
    ,sum(case when dtl.line_net_usd_amt>0 then dtl.line_net_usd_amt
              else 0 end) gross_spend
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt<0 then dtl.line_net_usd_amt
              else 0 end) return_amount
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' then dtl.line_net_usd_amt
              else 0 end) net_spend
    ,count(distinct case when dtl.line_net_usd_amt>0
                              then st.store_num||coalesce(dtl.order_date,dtl.tran_date)
                         else null end) trips
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt >  0 then dtl.line_item_quantity 
              else 0 end)  AS items
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt <  0 then (-1)*dtl.line_item_quantity
              else 0 end)  AS return_items
    ,sum(case when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt <  0 then (-1)*dtl.line_item_quantity
              when coalesce(dtl.nonmerch_fee_code,'-999') <> '6666' and dtl.line_net_usd_amt >  0 then dtl.line_item_quantity 
              else 0 end)  AS net_items
    ,max(case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') and trim(nts.aare_chnl_code) = 'FLS' then 1
              when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') and trim(nts.aare_chnl_code) = 'NCOM' then 1
              when st.business_unit_desc in ('RACK', 'RACK CANADA') and trim(nts.aare_chnl_code) = 'RACK' then 1
              when st.business_unit_desc in ('OFFPRICE ONLINE') and trim(nts.aare_chnl_code) = 'NRHL' then 1
              else 0 end) as acq_channel
from prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW as dtl
  join prd_nap_usr_vws.store_dim as st
    on (case when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp' 
                                 and dtl.line_item_net_amt_currency_code = 'CAD' then 867
             when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp' 
                                 and dtl.line_item_net_amt_currency_code = 'USD' then 808
             else dtl.intent_store_num
         end) = st.store_num
  left join prd_nap_usr_vws.CUSTOMER_NTN_STATUS_FACT nts on dtl.global_tran_id = nts.aare_global_tran_id
  where case when coalesce(dtl.line_net_usd_amt,0)>=0 then coalesce(dtl.order_date,dtl.tran_date)
            else dtl.tran_date end between (select ty_start_dt from date_lookup) and  (select ty_end_dt from date_lookup)
    and dtl.business_day_date between (select ty_start_dt from date_lookup) and  (select ty_end_dt+14 from date_lookup)
    and dtl.acp_id is not null
    and st.business_unit_desc 
        in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE')
group by 1,2,3
)with data unique primary index(acp_id,channel) on commit preserve rows;



drop table ly;
CREATE set volatile TABLE ly AS (
select dtl.acp_id
   from prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW as dtl
  join prd_nap_usr_vws.store_dim as st
    on dtl.intent_store_num = st.store_num
where case when coalesce(dtl.line_net_usd_amt,0)>=0 then coalesce(dtl.order_date,dtl.tran_date)
            else dtl.tran_date end between (select ly_start_dt from date_lookup) and  (select ly_end_dt from date_lookup)
    and dtl.business_day_date between (select ly_start_dt from date_lookup) and  (select ly_end_dt+30 from date_lookup)
    and dtl.acp_id is not null
    and st.business_unit_desc 
        in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE')
)with data primary index(acp_id) on commit preserve rows;


-- acquired customers
drop table new_ty;
create volatile multiset table new_ty as (
select
  acp_id,
  aare_chnl_code as acquired_channel
from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT
where aare_status_date between (select ty_start_dt from date_lookup)
and (select ty_end_dt from date_lookup)
) with data primary index(acp_id) on commit preserve rows;

-- activated customers
drop table activated_ty;
create volatile multiset table activated_ty as (
select
  acp_id,
  activated_chnl_code activated_channel
from PRD_NAP_USR_VWS.CUSTOMER_ACTIVATED_FACT
where activated_date between (select ty_start_dt from date_lookup)
and (select ty_end_dt from date_lookup)
) with data primary index(acp_id) on commit preserve rows;



-- customer level summary
drop table customer_summary;
create multiset volatile table customer_summary as (
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
    left join week_num_engaged_customer eng
      on ty.acp_id = eng.acp_id
      and ty.channel = eng.engaged_to_channel
    left join new_ty acq
      on ty.acp_id = acq.acp_id
    left join activated_ty act
      on ty.acp_id = act.acp_id
      and ty.channel = act.activated_channel
 ) with data primary index (acp_id) on commit preserve rows;


delete from {bfl_t2_schema}.weekly_buyerflow_agg
where report_period = (select report_period from date_lookup);


-- channel level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'all' report_type,
    channel,
    '' predicted_segment,
    '' country,
    '' loyalty_status,
    '' nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary
  group by channel;


-- brand level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'all' report_type,
    brand,
    '' predicted_segment,
    '' country,
    '' loyalty_status,
    '' nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary
  group by brand;


-- JWN level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'all' report_type,
    'JWN',
    '' predicted_segment,
    '' country,
    '' loyalty_status,
    '' nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary;



-- Loyalty status, based on time period
drop table lyl_status;
 create volatile multiset table lyl_status as (
   select
   	acp_id,
    case when cardmember_fl = 1 then 'cardmember'
    when member_fl = 1 then 'member'
    else 'non-member' end as loyalty_status,
 	rewards_level
 	from (
 	select
    DISTINCT ac.acp_id,
     -- member status
     case
       when cardmember_enroll_date <= (select ty_end_dt from date_lookup)
         and (cardmember_close_date >= (select ty_end_dt from date_lookup)
         or cardmember_close_date is null)
       then 1 else 0 end as cardmember_fl,
     case
       when cardmember_fl = 0
         and member_enroll_date <= (select ty_end_dt from date_lookup)
         and (member_close_date >= (select ty_end_dt from date_lookup)
         or member_close_date is null)
       then 1 else 0 end as member_fl,
     -- nordy_level
     -- i think we should include max here for the weird records
	max( case
            when rwd.rewards_level in ('MEMBER') then 1
            when rwd.rewards_level in ('INSIDER','L1') then 2
            when rwd.rewards_level in ('INFLUENCER','L2') then 3
            when rwd.rewards_level in ('AMBASSADOR','L3') then 4
            when rwd.rewards_level in ('ICON','L4') then 5
            else 0 end  ) AS rewards_level
   from prd_nap_usr_vws.analytical_customer as ac
   inner join prd_nap_usr_vws.loyalty_member_dim_vw as lmd
     on ac.acp_loyalty_id = lmd.loyalty_id
   left join prd_nap_usr_vws.loyalty_level_lifecycle_fact_vw as rwd
   -- date piece in join for nordy
    on ac.acp_loyalty_id = rwd.loyalty_id and  rwd.start_day_date <= (select ty_end_dt from date_lookup)
          	and rwd.end_day_date > (select ty_end_dt from date_lookup)
	group by 1,2,3
    ) as lyl
) with data primary index(acp_id) on commit preserve rows;

 


-- customer level summary
drop table customer_summary_seg;
create volatile multiset table customer_summary_seg as (
  select
    ty.brand,
    ty.channel,
    ty.acp_id,
    case when x.predicted_ct_segment is null then 'unknown' else x.predicted_ct_segment end as predicted_segment,
    ac.country,
 	  case when lyl.loyalty_status is null then 'non-member' else lyl.loyalty_status end as loyalty_status,
  	case when lyl.rewards_level=1 then '1-member'
  		 when lyl.rewards_level=2 then '2-insider'
  		 when lyl.rewards_level=3 then '3-influencer'
  		 when lyl.rewards_level=4 then '4-ambassador'
   		 when lyl.rewards_level=5 then '5-icon'
       when lyl.rewards_level=0 then (case when lyl.loyalty_status='cardmember' then '1-member'
                                           when lyl.loyalty_status='member' then '1-member'
                                           when lyl.loyalty_status='non-member' then '0-nonmember' end)
   		 else '0-nonmember' end as nordy_level,
    ty.gross_spend,
    ty.net_spend,
    ty.trips,
    ty.items,
    ty.new_flg,
    ty.retained_flg,
    ty.eng_flg,
    ty.act_flg
    from customer_summary ty
     --
     left join
     	T2DL_DAS_CUSTOMBER_MODEL_ATTRIBUTE_PRODUCTIONALIZATION.customer_prediction_core_target_segment x
       on ty.acp_id = x.acp_id
--       and x.last_scored_date = date'2021-10-04'
       left join lyl_status lyl
           on ty.acp_id = lyl.acp_id
        left join (
          select
          acp_id,
          -- country
          case when us_dma_code = -1 then null else us_dma_desc end as us_dma,
          case
            when us_dma is not null then 'US'
            when ca_dma_code is not null then 'CA'
          else 'unknown' end as country
          	from prd_nap_usr_vws.analytical_customer ) as ac
         	 on ty.acp_id = ac.acp_id
     ) with data primary index (acp_id) on commit preserve rows;

collect statistics on customer_summary_seg column (acp_id);



-- channel level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'segment' report_type,
    channel,
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary_seg
  group by channel, predicted_segment, country, loyalty_status, nordy_level;


-- brand level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'segment' report_type,
    brand,
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary_seg
  group by brand, predicted_segment, country, loyalty_status, nordy_level;


-- JWN level
insert into {bfl_t2_schema}.weekly_buyerflow_agg
  select
    (select report_period from date_lookup),
    'segment' report_type,
    'JWN',
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customer,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained_customer,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as total_retained_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as net_retained_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as new_customer,
    sum(case when new_flg = 1 then gross_spend else 0 end) as total_new_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as net_new_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customer - new_customer - retained_customer  as react_customer,
    total_gross_spend - total_retained_spend - total_new_spend as total_react_spend,
    total_net_spend - net_retained_spend - net_new_spend as net_react_spend,
    total_trips - retained_trips - new_trips as react_trips,
    total_items - retained_items - new_items as react_items,
    sum(act_flg) as activated_customer,
    sum(case when act_flg = 1 then gross_spend else 0 end) as total_activated_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as net_activated_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as eng_customer,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as total_eng_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as net_eng_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as eng_trips,
    sum(case when eng_flg = 1 then items else 0 end) as eng_items,
    current_timestamp update_tmstp
  from customer_summary_seg
  group by predicted_segment, country, loyalty_status, nordy_level;
 
 
 grant select on {bfl_t2_schema}.weekly_buyerflow_agg to public;
 

COLLECT STATISTICS  COLUMN (report_period, box)
on {bfl_t2_schema}.weekly_buyerflow_agg;


/* 
SQL script must end with statement to turn off QUERY_BAND
*/
SET QUERY_BAND = NONE FOR SESSION;
