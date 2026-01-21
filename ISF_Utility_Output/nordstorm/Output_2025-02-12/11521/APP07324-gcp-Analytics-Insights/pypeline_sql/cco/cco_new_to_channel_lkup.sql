SET QUERY_BAND = 'App_ID=APP08240;
     DAG_ID=cco_cust_chan_yr_attributes_11521_ACE_ENG;
     Task_Name=cco_new_to_channel_lkup;'
     FOR SESSION VOLATILE;

/* March 2023 migrated to isf */ 

create volatile multiset table customer_tran as  (   
  select
    dtl.acp_id,
    dtl.marketing_profile_type_ind,
    -- use order_date for orders
    case
      when tran_type_code in ('SALE','EXCH') and dtl.line_net_usd_amt >= 0
        then coalesce(dtl.order_date, dtl.tran_date)
      else dtl.tran_date end as sale_date,
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
    dtl.tran_time,
    dtl.tran_num,
    dtl.global_tran_id,
    dtl.line_item_seq_num,
    dtl.line_item_merch_nonmerch_ind,
    case when dtl.line_net_usd_amt > 0 then 1 else 0 end as sale_ind,
    dtl.sku_num
  from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
  where  dtl.acp_id is not null
   -- start date is current_date for test; '1999-01-01' for prod (i.e., full table)
  and dtl.business_day_date between date'{new_start_date}' and date'{new_end_date1}' --HARDCODED!  
      --(dates needs to go back to start of customer data in NAP 
      -- ... otherwise everyone gets mistakenly identified as "new" in our first reporting year)
 ) with data primary index(acp_id) on commit preserve rows;


insert into customer_tran
  select
    dtl.acp_id,
    dtl.marketing_profile_type_ind,
    -- use order_date for orders
    case
      when tran_type_code in ('SALE','EXCH') and dtl.line_net_usd_amt >= 0
        then coalesce(dtl.order_date, dtl.tran_date)
      else dtl.tran_date end as sale_date,
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
    dtl.tran_time,
    dtl.tran_num,
    dtl.global_tran_id,
    dtl.line_item_seq_num,
    dtl.line_item_merch_nonmerch_ind,
    case when dtl.line_net_usd_amt > 0 then 1 else 0 end as sale_ind,
    dtl.sku_num
  from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
  where  dtl.acp_id is not null
   -- start date is current_date for test; '1999-01-01' for prod (i.e., full table)
  and dtl.business_day_date between date'{new_end_date1}'+1  and date'{new_end_date2}' --query remaining dates separately to avoid CPU timeout
;

create volatile multiset table customer_tran_channel as (
  select
    tran.*,
    case
        when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FL'
        when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'FC'
        when st.business_unit_desc in ('RACK', 'RACK CANADA') then 'RK'
        when st.business_unit_desc in ('OFFPRICE ONLINE') then 'HL'
    else null end as channel
  from customer_tran as tran
  inner join prd_nap_usr_vws.store_dim as st
    on tran.store_num = st.store_num
    and st.business_unit_desc in (
     'FULL LINE',
     'FULL LINE CANADA',
     'N.CA',
     'N.COM',
     'OFFPRICE ONLINE',
     'RACK',
     'RACK CANADA',
     'TRUNK CLUB')
 ) with data primary index(acp_id) on commit preserve rows;

create volatile multiset table customer_tran_brand as (
  select
    tran.*,
    case
        when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB') then 'FP'
        when st.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then 'OP'
    else null end as channel
  from customer_tran as tran
  inner join prd_nap_usr_vws.store_dim as st
    on tran.store_num = st.store_num
    and st.business_unit_desc in (
     'FULL LINE',
     'FULL LINE CANADA',
     'N.CA',
     'N.COM',
     'OFFPRICE ONLINE',
     'RACK',
     'RACK CANADA',
     'TRUNK CLUB')
 ) with data primary index(acp_id) on commit preserve rows;

COLLECT STATISTICS COLUMN (ACP_ID ,CHANNEL) ON customer_tran_channel;
COLLECT STATISTICS COLUMN (ACP_ID ,SALE_DATE ,TRAN_TIME ,TRAN_NUM ,GLOBAL_TRAN_ID ,CHANNEL) ON customer_tran_channel;

create volatile multiset table customer_chan_rank as (
  select
    acp_id,
    channel,
    sale_date,
    tran_time,
    global_tran_id,
    lag(sale_date) over(partition by acp_id, channel order by sale_date, tran_time, tran_num) as lag_dt,
    case
      when lag_dt is null then 1
      when lag_dt is not null and sale_date - lag_dt >= 1461 then 1 -- 4years
      else 0
    end as acq_id
  from (
    select distinct
      acp_id,
      channel,
      sale_date,
      tran_time,
      tran_num,
      global_tran_id
    from customer_tran_channel) as a
 ) with data primary index(acp_id) on commit preserve rows;

create volatile multiset table customer_brand_rank as (
  select
    acp_id,
    channel,
    sale_date,
    tran_time,
    global_tran_id,
    lag(sale_date) over(partition by acp_id, channel order by sale_date, tran_time, tran_num) as lag_dt,
    case
      when lag_dt is null then 1
      when lag_dt is not null and sale_date - lag_dt >= 1461 then 1 -- 4years
      else 0
    end as acq_id
  from (
    select distinct
      acp_id,
      channel,
      sale_date,
      tran_time,
      tran_num,
      global_tran_id
    from customer_tran_brand) as a
 ) with data primary index(acp_id) on commit preserve rows;

DELETE FROM {cco_t2_schema}.cco_new_to_channel_lkup ALL;

INSERT INTO {cco_t2_schema}.cco_new_to_channel_lkup
select distinct acp_id
  ,channel
  ,max(sale_date) as ntx_date
from customer_chan_rank
where acq_id=1
group by 1,2
union all
select distinct acp_id
  ,channel
  ,max(sale_date) as ntx_date
from customer_brand_rank
where acq_id=1
group by 1,2
;

COLLECT STATISTICS COLUMN(acp_id) ON {cco_t2_schema}.cco_new_to_channel_lkup;


SET QUERY_BAND = NONE FOR SESSION;