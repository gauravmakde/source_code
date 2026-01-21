SET QUERY_BAND = 'App_ID=APP09044;
     DAG_ID=kpi_buyerflow_11521_ACE_ENG;
     Task_Name=kpi_buyerflow;'
     FOR SESSION VOLATILE;
/*

T2/Table Name: T2DL_DAS_MOA_KPI.kpi_buyerflow
Owner: Analytics Engineering
Modified:03/11/2023 

-- To automate the KPI Scorecard 

*/

CREATE volatile multiset TABLE date_lookup
AS
(SELECT b.ty_start_dt AS ty_start_dt,
       c.ty_end_dt AS ty_end_dt,
       trim(CASE
         WHEN b.report = {report_period} THEN CAST(CONCAT (fiscal_year_num,'_',month_abrv) AS VARCHAR(18))
         ELSE {report_period}
       END) AS report_period,
       ty_start_dt - 1461 AS pre_start_dt,
       ty_end_dt - 1 AS pre_end_dt
FROM prd_nap_usr_vws.DAY_CAL_454_DIM a
  LEFT JOIN (SELECT month_start_day_date AS ty_start_dt,
                      '1234' AS report
             FROM prd_nap_usr_vws.DAY_CAL_454_DIM
             WHERE day_date ={start_date}) b ON 1 = 1
  LEFT JOIN  (SELECT month_end_day_date AS ty_end_dt,
                       '1234' AS report
             FROM prd_nap_usr_vws.DAY_CAL_454_DIM
             WHERE day_date = {end_date}) c ON 1 = 1          
WHERE day_date = (select month_start_day_date from prd_nap_usr_vws.DAY_CAL_454_DIM where day_date= {start_date})
GROUP BY 1,
         2,
         3,
         4,
         5) WITH data PRIMARY INDEX (report_period) ON COMMIT preserve rows;


create volatile multiset table pre_purchase as (
  select
    distinct
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


create volatile multiset table ty_purchase as (
  select
    distinct
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


create volatile multiset table existing_cust as (
  select
    ty.acp_id,
    ty.channel,
    case when acq.acp_id is not null then 1 else 0 end as new_customer_flg,
    case when pre.latest_sale_date is not null
        and ty.first_sale_date - pre.latest_sale_date <1461
      then 1 else 0 end as old_channel_flg
  from ty_purchase_channel as ty
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



create volatile multiset table new_cust as (
select
  ty.acp_id,
  ty.channel
from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT as acq
inner join ty_purchase_channel as ty
  on acq.acp_id = ty.acp_id
  and acq.aare_chnl_code <> ty.channel
inner join (
  select acp_id
  from ty_purchase_channel
  group by acp_id
  having count(distinct channel) > 1 ) as multi
  on acq.acp_id = multi.acp_id
where aare_status_date between (select ty_start_dt from date_lookup)
and (select ty_end_dt from date_lookup)
) with data primary index(acp_id) on commit preserve rows;


create multiset volatile table engaged_customer as (
  select
    (select report_period from date_lookup) as report_period,
    acp_id,
    channel as engaged_to_channel
  from existing_cust
  where old_channel_flg = 0 and new_customer_flg = 0
  union all
  select
   (select report_period from date_lookup) as report_period,
    acp_id,
    channel as engaged_to_channel
  from new_cust
) with data primary index (acp_id) on commit preserve rows;


create volatile multiset table date_lookup_ly as (
  select
    trim(CASE 
		WHEN b.report= {report_period} THEN CAST(CONCAT(fiscal_year_num,'_',month_abrv) as VARCHAR(18))
		ELSE {report_period}
	END)
	as report_period,
    min(a.day_date_last_year_realigned) as ly_start_dt,
    a.month_start_day_date -1 as ly_end_dt,
    a.month_start_day_date as ty_start_dt,
    a.month_end_day_date as ty_end_dt
  from prd_nap_usr_vws.DAY_CAL_454_DIM a
  LEFT JOIN 
	(
    SELECT month_start_day_date as ty_start_dt, 
	'1234' as report
	from prd_nap_usr_vws.DAY_CAL_454_DIM 
	where day_date = {start_date}
  )b
  ON a.month_start_day_date=b.ty_start_dt
	where day_date = {end_date}
	group by 1,3,4,5
) with data primary index(report_period) on commit preserve rows;



create volatile multiset table ty_positive as (
  select
    case
        when str.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'FLS'
        when str.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'NCOM'
        when str.business_unit_desc in ('RACK', 'RACK CANADA') then 'RACK'
        when str.business_unit_desc in ('OFFPRICE ONLINE') then 'NRHL'
      else null end as channel,
    case when channel in ('FLS','NCOM') then 'FP' else 'OP' end as brand,
    case when channel in ('FLS','RACK') then 'STORE' else 'DIGITAL' end as digital_store,
    acp_id,
    sum(gross_amt) as gross_spend,
    sum(non_gc_amt) as non_gc_spend,
    count(distinct trip_id) as trips,
    sum(items) as items
  from (
      select
        dtl.acp_id,
        dtl.line_net_usd_amt as gross_amt,
        case when dtl.nonmerch_fee_code = '6666'
          then 0 else dtl.line_net_usd_amt end as non_gc_amt,
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
        coalesce(dtl.order_date,dtl.tran_date) as sale_date,
        cast(dtl.acp_id||store_num||sale_date as varchar(150)) as trip_id,
        dtl.line_item_quantity as items
      from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
      where coalesce(dtl.order_date,dtl.tran_date) >= (select ty_start_dt from date_lookup_ly)
        and coalesce(dtl.order_date,dtl.tran_date) <= (select ty_end_dt from date_lookup_ly)
        and not dtl.acp_id is null
        and dtl.line_net_usd_amt > 0
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
  group by 1,2,3,4
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
    case when channel in ('FLS','RACK') then 'STORE' else 'DIGITAL' end as digital_store,
    acp_id,
    sum(line_net_usd_amt) as return_spend
  from (
      select
        dtl.acp_id,
        dtl.line_net_usd_amt,
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
      where dtl.tran_date >= (select ty_start_dt from date_lookup_ly)
        and dtl.tran_date <= (select ty_end_dt from date_lookup_ly)
        and not dtl.acp_id is null
        and dtl.line_net_usd_amt <= 0
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
  group by 1,2,3,4
) with data primary index(acp_id) on commit preserve rows;


create volatile multiset table ty as (
  select
    coalesce(a.acp_id, b.acp_id) as acp_id,
    coalesce(a.channel, b.channel) as channel,
    coalesce(a.brand, b.brand) as brand,
    coalesce(a.digital_store, b.digital_store) as digital_store,
    coalesce(a.gross_spend, 0) as gross_spend,
    coalesce(a.non_gc_spend, 0) + coalesce(b.return_spend, 0) as net_spend,
    coalesce(a.trips, 0) as trips,
    coalesce(a.items, 0) as items
  from ty_positive as a
  full join ty_negative as b
    on a.acp_id = b.acp_id
    and a.channel = b.channel
    and a.brand = b.brand
    and a.digital_store = b.digital_store
) with data primary index(acp_id) on commit preserve rows;



create volatile multiset table ly_positive as (
  select distinct acp_id
  from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
  inner join prd_nap_usr_vws.store_dim as str
    on dtl.intent_store_num = str.store_num
  where coalesce(dtl.order_date,dtl.tran_date) >= (select ly_start_dt from date_lookup_ly)
    and coalesce(dtl.order_date,dtl.tran_date) <= (select ly_end_dt from date_lookup_ly)
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


create volatile multiset table ly_negative as (
  select distinct acp_id
  from prd_nap_usr_vws.retail_tran_detail_fact_vw as dtl
  inner join prd_nap_usr_vws.store_dim as str
    on dtl.intent_store_num = str.store_num
  where dtl.tran_date >= (select ly_start_dt from date_lookup_ly)
    and dtl.tran_date <= (select ly_end_dt from date_lookup_ly)
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


create volatile multiset table ly as (
  select acp_id from ly_positive
  union
  select acp_id from ly_negative
) with data primary index(acp_id) on commit preserve rows;


create volatile multiset table new_ty as (
select
  acp_id,
  aare_chnl_code as acquired_channel
from PRD_NAP_USR_VWS.CUSTOMER_NTN_STATUS_FACT
where aare_status_date between (select ty_start_dt from date_lookup_ly)
and (select ty_end_dt from date_lookup_ly)
) with data primary index(acp_id) on commit preserve rows;


create volatile multiset table activated_ty as (
select
  acp_id,
  activated_channel
from dl_cma_cmbr.customer_activation
where activated_date between (select ty_start_dt from date_lookup_ly)
and (select ty_end_dt from date_lookup_ly)
) with data primary index(acp_id) on commit preserve rows;


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
     case
       when cardmember_enroll_date <= (select ty_end_dt from date_lookup_ly)
         and (cardmember_close_date >= (select ty_end_dt from date_lookup_ly)
         or cardmember_close_date is null)
       then 1 else 0 end as cardmember_fl,
     case
       when cardmember_fl = 0
         and member_enroll_date <= (select ty_end_dt from date_lookup_ly)
         and (member_close_date >= (select ty_end_dt from date_lookup_ly)
         or member_close_date is null)
       then 1 else 0 end as member_fl,
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
    on ac.acp_loyalty_id = rwd.loyalty_id and  rwd.start_day_date <= (select ty_end_dt from date_lookup_ly)
          	and rwd.end_day_date > (select ty_end_dt from date_lookup_ly)
	group by 1,2,3
    ) as lyl
) with data primary index(acp_id) on commit preserve rows;




create volatile multiset table customer_summary as (
  select
    ty.brand,
    ty.channel,
    ty.digital_store,
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
    case when acq.acp_id is not null then 1 else 0 end as new_flg,
    case when ly.acp_id is not null then 1 else 0 end as retained_flg,
    case when eng.acp_id is not null then 1 else 0 end as eng_flg,
    case when act.acp_id is not null then 1 else 0 end as act_flg
    from ty
    left join ly
      on ty.acp_id = ly.acp_id
    left join engaged_customer eng
      on ty.acp_id = eng.acp_id
      and ty.channel = eng.engaged_to_channel
    left join new_ty acq
      on ty.acp_id = acq.acp_id
    left join activated_ty act
      on ty.acp_id = act.acp_id
      and ty.channel = act.activated_channel 
     left join
     	T2DL_DAS_CUSTOMBER_MODEL_ATTRIBUTE_PRODUCTIONALIZATION.customer_prediction_core_target_segment x     
       on ty.acp_id = x.acp_id
       left join lyl_status lyl
           on ty.acp_id = lyl.acp_id
        left join (
          select
          acp_id,
          case when us_dma_code = -1 then null else us_dma_desc end as us_dma,
          case
            when us_dma is not null then 'US'
            when ca_dma_code is not null then 'CA'
          else 'unknown' end as country
          	from prd_nap_usr_vws.analytical_customer ) as ac
         	 on ty.acp_id = ac.acp_id
     ) with data primary index (acp_id) on commit preserve rows;

collect statistics on customer_summary column (acp_id);




create volatile multiset table temporary_buyerflowplus_table as (
  select
    (select report_period from date_lookup_ly) as report_period,
    channel,
    predicted_segment as predicted_segment,
    country as country,
    loyalty_status as loyalty_status,
    nordy_level as nordy_level,
    count(distinct acp_id) as total_customers,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as retained_gross_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as retained_net_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as acquired,
    sum(case when new_flg = 1 then gross_spend else 0 end) as new_gross_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as new_net_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customers - acquired - retained  as reactivated,
    total_gross_spend - retained_gross_spend - new_gross_spend as reactivated_gross_spend,
    total_net_spend - retained_net_spend - new_net_spend as reactivated_net_spend,
    total_trips - retained_trips - new_trips as reactivated_trips,
    total_items - retained_items - new_items as reactivated_items,
    sum(act_flg) as activated,
    sum(case when act_flg = 1 then gross_spend else 0 end) as activated_gross_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as activated_net_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    sum(eng_flg) as engaged,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as engaged_gross_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as engaged_net_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as engaged_trips,
    sum(case when eng_flg = 1 then items else 0 end) as engaged_items,
    current_timestamp as current_timesta
  from customer_summary
  group by report_period, channel, predicted_segment, country, loyalty_status, nordy_level)
 with data primary index (report_period, channel, predicted_segment, country) on commit preserve rows;



insert into temporary_buyerflowplus_table
  select
   (select report_period from date_lookup_ly) as report_period,
    brand,
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customers,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as retained_gross_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as retained_net_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as acquired,
    sum(case when new_flg = 1 then gross_spend else 0 end) as new_gross_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as new_net_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customers - acquired - retained  as reactivated,
    total_gross_spend - retained_gross_spend - new_gross_spend as reactivated_gross_spend,
    total_net_spend - retained_net_spend - new_net_spend as reactivated_net_spend,
    total_trips - retained_trips - new_trips as reactivated_trips,
    total_items - retained_items - new_items as reactivated_items,
    sum(act_flg) as activated,
    sum(case when act_flg = 1 then gross_spend else 0 end) as activated_gross_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as activated_net_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    count(distinct case when eng_flg = 1 then acp_id else null end) as engaged,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as engaged_gross_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as engaged_net_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as engaged_trips,
    sum(case when eng_flg = 1 then items else 0 end) as engaged_items,
    current_timestamp
  from customer_summary
  group by brand, predicted_segment, country, loyalty_status, nordy_level;


insert into temporary_buyerflowplus_table
  select
   (select report_period from date_lookup_ly) as report_period,
    digital_store,
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customers,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as retained_gross_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as retained_net_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as acquired,
    sum(case when new_flg = 1 then gross_spend else 0 end) as new_gross_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as new_net_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customers - acquired - retained  as reactivated,
    total_gross_spend - retained_gross_spend - new_gross_spend as reactivated_gross_spend,
    total_net_spend - retained_net_spend - new_net_spend as reactivated_net_spend,
    total_trips - retained_trips - new_trips as reactivated_trips,
    total_items - retained_items - new_items as reactivated_items,
    sum(act_flg) as activated,
    sum(case when act_flg = 1 then gross_spend else 0 end) as activated_gross_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as activated_net_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    count(distinct case when eng_flg = 1 then acp_id else null end) as engaged,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as engaged_gross_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as engaged_net_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as engaged_trips,
    sum(case when eng_flg = 1 then items else 0 end) as engaged_items,
    current_timestamp
  from customer_summary
  group by digital_store, predicted_segment, country, loyalty_status, nordy_level;



insert into temporary_buyerflowplus_table
  select
    (select report_period from date_lookup_ly) as report_period,
    'JWN',
    predicted_segment,
    country,
    loyalty_status,
    nordy_level,
    count(distinct acp_id) as total_customers,
    sum(gross_spend) as total_gross_spend,
    sum(net_spend) as total_net_spend,
    sum(trips) as total_trips,
    sum(items) as total_items,
    count(distinct case when retained_flg = 1 then acp_id else null end) as retained,
    sum(case when retained_flg = 1 then gross_spend else 0 end) as retained_gross_spend,
    sum(case when retained_flg = 1 then net_spend else 0 end) as retained_net_spend,
    sum(case when retained_flg = 1 then trips else 0 end) retained_trips,
    sum(case when retained_flg = 1 then items else 0 end) retained_items,
    count(distinct case when new_flg= 1 then acp_id else null end) as acquired,
    sum(case when new_flg = 1 then gross_spend else 0 end) as new_gross_spend,
    sum(case when new_flg = 1 then net_spend else 0 end) as new_net_spend,
    sum(case when new_flg = 1 then trips else 0 end) as new_trips,
    sum(case when new_flg = 1 then items else 0 end) as new_items,
    total_customers - acquired - retained  as reactivated,
    total_gross_spend - retained_gross_spend - new_gross_spend as reactivated_gross_spend,
    total_net_spend - retained_net_spend - new_net_spend as reactivated_net_spend,
    total_trips - retained_trips - new_trips as reactivated_trips,
    total_items - retained_items - new_items as reactivated_items,
    sum(act_flg) as activated,
    sum(case when act_flg = 1 then gross_spend else 0 end) as activated_gross_spend,
    sum(case when act_flg = 1 then net_spend else 0 end) as activated_net_spend,
    sum(case when act_flg = 1 then trips else 0 end) as activated_trips,
    sum(case when act_flg = 1 then items else 0 end) as activated_items,
    count(distinct case when eng_flg = 1 then acp_id else null end) as engaged,
    sum(case when eng_flg = 1 then gross_spend else 0 end) as engaged_gross_spend,
    sum(case when eng_flg = 1 then net_spend else 0 end) as engaged_net_spend,
    sum(case when eng_flg = 1 then trips else 0 end) as engaged_trips,
    sum(case when eng_flg = 1 then items else 0 end) as engaged_items,
    current_timestamp
  from customer_summary
  group by predicted_segment, country, loyalty_status, nordy_level;
 
Create volatile multiset table final_table as
 (SELECT 
 trim(report_period) as report_period,
 CAST(SUBSTRING(report_period FROM 1 FOR POSITION('_' IN report_period) - 1) AS INTEGER) AS report_year,
 SUBSTRING(report_period FROM POSITION('_' IN report_period) + 1) AS report_month,
 channel as channel,
 country,
 SUM(total_customers) as total_customers,
 SUM(total_trips)as total_trips,
 SUM(retained) as retained,
 SUM(retained_trips) as retained_trips,
 SUM(acquired) as acquired,
 SUM(new_trips) as new_trips,
 SUM(reactivated) as reactivated,
 SUM(reactivated_trips) as reactivated_trips,
 SUM(activated)as activated,
 SUM(activated_trips) as activated_trips,
 SUM(engaged) as engaged,
 SUM(engaged_trips) as engaged_trips,
 SUM(total_net_spend) as total_net_spend,
 SUM(retained_net_spend) as retained_net_spend,
 SUM(new_net_spend) as new_net_spend,
 SUM(reactivated_net_spend) as reactivated_net_spend,
 current_timestamp as dw_sys_load_tmstp
FROM temporary_buyerflowplus_table
GROUP BY 1,2,3,4,5
)
with data primary index (report_period,report_year, channel, country) on commit preserve rows;

DELETE FROM {kpi_scorecard_t2_schema}.kpi_buyerflow
WHERE report_period = (select report_period from date_lookup) ;

INSERT INTO {kpi_scorecard_t2_schema}.kpi_buyerflow
Select 
a.report_period,
a.report_year,
a.report_month,
a.channel,
a.country,
SUM(a.total_customers) as total_customers,
 SUM(b.total_customers) as LY_total_customers,
 SUM(a.total_trips)as total_trips,
 SUM(b.total_trips)as LY_total_trips,
 SUM(a.retained) as retained,
 SUM(b.retained) as LY_retained,
 SUM(a.retained_trips) as retained_trips,
 SUM(b.retained_trips) as LY_retained_trips,
 SUM(a.acquired) as acquired,
 SUM(b.acquired) as LY_acquired,
 SUM(a.new_trips) as new_trips,
 SUM(b.new_trips) as LY_new_trips,
 SUM(a.reactivated) as reactivated,
 SUM(b.reactivated) as LY_reactivated,
 SUM(a.reactivated_trips) as reactivated_trips,
 SUM(b.reactivated_trips) as LY_reactivated_trips,
 SUM(a.activated)as activated,
 SUM(b.activated)as LY_activated,
 SUM(a.activated_trips) as activated_trips,
 SUM(b.activated_trips) as LY_activated_trips,
 SUM(a.engaged) as engaged,
 SUM(b.engaged) as LY_engaged,
 SUM(a.engaged_trips) as engaged_trips,
 SUM(b.engaged_trips) as LY_engaged_trips,
 SUM(a.total_net_spend) as total_net_spend,
 SUM(b.total_net_spend) as LY_total_net_spend,
 SUM(a.retained_net_spend) as retained_net_spend,
 SUM(b.retained_net_spend)as LY_retained_net_spend,
 SUM(a.new_net_spend) as new_net_spend,
 SUM(b.new_net_spend) as LY_new_net_spend,
 SUM(a.reactivated_net_spend) as reactivated_net_spend,
 SUM(b.reactivated_net_spend) as LY_reactivated_net_spend,
 current_timestamp as dw_sys_load_tmstp
FROM final_table as a 
LEFT JOIN {kpi_scorecard_t2_schema}.kpi_buyerflow as b
ON b.report_year = a.report_year - 1
and trim(b.report_month)=trim(a.report_month)
and b.channel=a.channel
and a.country=b.country
GROUP BY 1,2,3,4,5;

SET QUERY_BAND = NONE FOR SESSION;
