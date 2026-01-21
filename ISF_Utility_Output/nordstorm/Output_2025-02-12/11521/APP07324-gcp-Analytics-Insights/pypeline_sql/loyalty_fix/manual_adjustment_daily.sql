SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=manual_adjustment_daily_11521_ACE_ENG;
     Task_Name=manual_adjustment_daily;'
     FOR SESSION VOLATILE;

-- Loyalty Points Mismatch
-- OLD AS OF 10-11-24 - Analytics Contact: Andrew Porter or Katie Hains
-- Last updated 10-11-24
-- Runtime: about 3 minutes
-- Records: about 3K per day

----------- All AIMIA returns -----------
-- Identify all transactions where points were taken away
create volatile multiset table retn as (
  select distinct
    loyalty_transaction_key as global_tran_id,
    loyalty_id,
    business_day_date,
    intent_store_num,
    total_points,
    tran_num,
    -- original transaction details
    original_global_tran_id,
    original_ringing_store_num,
    original_business_date,
    original_tran_num,
    case when original_global_tran_id is not null then 1 else 0 end as orig_tran_fl --AIMIA provides original_global_tran_id
  from prd_nap_usr_vws.loyalty_transaction_fact_vw
  where
    business_day_date between {start_date} and {end_date} --allow an extra, extra day for data to flow in
    and net_outside_spend_usd_amt = 0 -- not concerned about outside spend
    and total_net_spend_amt < 0 --only looking at returns or negative balance exchanges
    and total_points < 0
    and intent_store_num not in ('742','780') -- stores are closed, outside spend is mapping to it
) with data primary index (global_tran_id) on commit preserve rows;

collect statistics on retn index (global_tran_id);

-----------Identifying Cash Transactions-----------
--Using cash_fl to identify ONLINE anomolies later
create volatile multiset table tndr as (
  select distinct
    global_tran_id,
    business_day_date,
    max(case when tender_type_code in ('CASH','CHECK','CA') then 1 else 0 end) as cash_fl
  from prd_nap_usr_vws.retail_tran_tender_fact
  where
    business_day_date >= CURRENT_DATE -730
    and tran_type_code in ('SALE','RETN','EXCH') -- not interested in VOIDs or any other types
  group by
    global_tran_id,
    business_day_date
) with data primary index (global_tran_id) on commit preserve rows;

collect statistics on tndr index (global_tran_id);

-----------ERTM data-----------
-- Starting to identify the population that maps to ERTM
create volatile multiset table ertm as (
  select
    retn.global_tran_id,
    retn.loyalty_id,
    retn.business_day_date,
    retn.total_points,
    retn.intent_store_num,
    -- original purchase details
    retn.original_global_tran_id,
    retn.original_ringing_store_num,
    retn.original_business_date,
    retn.original_tran_num,
    -- flags
    case when tran.loyalty_transaction_key is not null then 1 else 0 end as base_returns_fl, -- return mapped correctly to a sale in AIMIA
    retn.orig_tran_fl,
    case when hdr.global_tran_id is not null then 1 else 0 end as ertm_hdr_fl, --AIMIA's global_tran_id is found in ERTM
    case when t2.global_tran_id is not null then 1 else 0 end as orig_ertm_fl, --AIMIA's original_global_tran_id is found in ERTM
    coalesce(t2.cash_fl, 0) as cash_fl
  from retn
  left join prd_nap_usr_vws.loyalty_transaction_fact_vw as tran
    on retn.original_global_tran_id = tran.loyalty_transaction_key
  left join prd_nap_usr_vws.retail_tran_hdr_fact as hdr
    on retn.global_tran_id = hdr.global_tran_id
  left join tndr as t2
    on retn.original_global_tran_id = t2.global_tran_id
  where base_returns_fl = 0 --no issue when = 1
) with data primary index (global_tran_id) on commit preserve rows;

collect statistics on ertm index (global_tran_id);

-----------return maps to ERTM-----------
-- Looking for Sale transaction using ERTM mapping instead of AIMIA mapping
-- Step 1: Create a volatile table for rdtl with necessary filters
create volatile multiset table rdtl as (
  select global_tran_id, original_transaction_identifier, business_day_date
  from prd_nap_usr_vws.retail_tran_detail_fact_vw
  where business_day_date >= CURRENT_DATE - 90
) with data primary index (global_tran_id) on commit preserve rows;

collect statistics on rdtl index (global_tran_id);

-- Step 2: Create a volatile table for sdtl with necessary filters
create volatile multiset table sdtl as (
  select global_tran_id, transaction_identifier, business_day_date
  from prd_nap_usr_vws.retail_tran_detail_fact_vw
  where business_day_date >= CURRENT_DATE - 730
) with data primary index (global_tran_id) on commit preserve rows;

collect statistics on sdtl index (global_tran_id);

-- Step 3: Create a volatile table for siss
create volatile multiset table siss as (
  select loyalty_transaction_key
  from prd_nap_usr_vws.loyalty_transaction_fact_vw
) with data primary index (loyalty_transaction_key) on commit preserve rows;

collect statistics on siss index (loyalty_transaction_key);

-- Step 4: Create the ertm_map table using the pre-filtered volatile tables
create volatile multiset table ertm_map as (
  select distinct
    ertm.global_tran_id,
    ertm.original_global_tran_id,
    max(case when rdtl.global_tran_id is not null then 1 else 0 end) as ertm_dtl_fl, --return found in ertm
    max(case when sdtl.global_tran_id is not null then 1 else 0 end) as ertm_dtl_sale_fl, --original sale found within ertm mapping
    max(case when siss.loyalty_transaction_key is not null then 1 else 0 end) as ertm_map_fl --original sale now found in AIMIA
  from ertm
  left join rdtl
    on ertm.global_tran_id = rdtl.global_tran_id
  left join sdtl
    on rdtl.original_transaction_identifier = sdtl.transaction_identifier
  left join siss
    on sdtl.global_tran_id = siss.loyalty_transaction_key
  where ertm.ertm_hdr_fl = 1 --only interested in segments that can use ERTM to track
  group by ertm.global_tran_id, ertm.original_global_tran_id
) with data primary index (global_tran_id) on commit preserve rows;

collect statistics on ertm_map index (global_tran_id);

-----------NRHL mapping with "trans_id"-----------
--Identifying NRHL edge case mappings directly in AIMIA
create volatile multiset table nrhl_map as (
  select distinct
    ertm.global_tran_id,
    ertm.original_global_tran_id,
    case
      when ertm.original_ringing_store_num = 828
        and sale.loyalty_id is not null then 1
      else 0
    end as nrhl_map_fl
  from ertm
  left join prd_nap_usr_vws.loyalty_transaction_fact_vw as sale
    on ertm.original_ringing_store_num = sale.intent_store_num
    and ertm.original_tran_num = sale.tran_num
    and ertm.original_business_date = sale.tran_date
) with data primary index (global_tran_id) on commit preserve rows;

collect statistics on nrhl_map index (global_tran_id);

-----------Final rollup table-----------
create volatile multiset table issue as (
  select
    global_tran_id,
    original_global_tran_id,
    loyalty_id,
    business_day_date,
    total_points,
    case
      when orig_tran_fl = 0 and ertm_hdr_fl = 0 and nrhl_map_fl = 0 then 1 --no mapping found from AIMIA to any other system (bad cdw data)
      when ertm_dtl_fl = 1 and ertm_dtl_sale_fl = 0 and nrhl_map_fl = 0 then 1 --lose the mapping from retn to sale in ertm
      when ertm_dtl_fl = 0 and ertm_hdr_fl = 1 and nrhl_map_fl = 0 then 1 --lose the mapping from ertm header to ertm detail
      when intent_store_num in (828, 808, 141, 867) and cash_fl = 1 and nrhl_map_fl = 0 then 1 --online and cash usually means call center
      else 0
    end as cant_research_fl,
    case
      when ertm_map_fl = 0 and nrhl_map_fl = 0 and cant_research_fl = 0 then 1
      else 0
    end as true_problem_fl
  from (
    select distinct
      ertm.global_tran_id,
      ertm.original_global_tran_id,
      ertm.loyalty_id,
      ertm.business_day_date,
      ertm.total_points,
      ertm.intent_store_num,
      -- flags
      ertm.orig_tran_fl,
      ertm.ertm_hdr_fl,
      ertm.cash_fl,
      -- mapping flags
      max(coalesce(em.ertm_dtl_fl, 0)) as ertm_dtl_fl,
      max(coalesce(em.ertm_dtl_sale_fl, 0)) as ertm_dtl_sale_fl,
      max(coalesce(em.ertm_map_fl, 0)) as ertm_map_fl,
      max(coalesce(nm.nrhl_map_fl, 0)) as nrhl_map_fl
    from ertm
    left join ertm_map as em
      on ertm.global_tran_id = em.global_tran_id
    left join nrhl_map as nm
      on ertm.global_tran_id = nm.global_tran_id
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
  ) as a
) with data primary index (global_tran_id) on commit preserve rows;

collect statistics on issue index (global_tran_id);

----------- Customer data -----------
--Pulling in Customer's Enrollment data to get Country of Enrollment
create volatile multiset table cust as (
  select
    loyalty_id,
    case when member_enroll_region_desc in ('FULL LINE CANADA', 'RACK CANADA', 'N.CA') then 'CA' else 'US' end as enroll_country
  from prd_nap_usr_vws.loyalty_member_dim_vw
) with data primary index (loyalty_id) on commit preserve rows;

collect statistics on cust index (loyalty_id);

DELETE FROM {loyalty_fix_t2_schema}.manual_adjustment_daily where business_day_date between {start_date} and {end_date};

--------- CUSTOMER COHORT ROLL-UP -------------
INSERT INTO {loyalty_fix_t2_schema}.manual_adjustment_daily
select distinct
  a.loyalty_id,
  a.business_day_date,
  sum(a.total_points) as sum_points,
  cust.enroll_country,
  CURRENT_TIMESTAMP as dw_sys_load_tmstp
from issue as a
left join cust
  on a.loyalty_id = cust.loyalty_id
where
  a.true_problem_fl = 1
group by
  a.loyalty_id,
  a.business_day_date,
  cust.enroll_country;

COLLECT STATISTICS COLUMN(loyalty_id) ON {loyalty_fix_t2_schema}.manual_adjustment_daily;

SET QUERY_BAND = NONE FOR SESSION;