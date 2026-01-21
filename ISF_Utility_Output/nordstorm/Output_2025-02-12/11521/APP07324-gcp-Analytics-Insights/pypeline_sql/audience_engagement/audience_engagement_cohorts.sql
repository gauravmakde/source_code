SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=aec_audience_engagement_cohorts_11521_ACE_ENG;
     Task_Name=audience_engagement_cohorts;'
     FOR SESSION VOLATILE;

/************************************************************************************/
/************************************************************************************
 * 
 * Build a table of customers & their "Engagement Cohort" for the past several rolling-4-quarter periods
 * 
 * We look at all customers shopping a given 12-mo (4-qtr) period, and segment them into 1 of the 
 * following 4 cohorts:
    'Acquire & Activate': anyone Acquired that year who made <= 5 non-restaurant trips that year
    'Lightly-Engaged': anyone not in Cohort #1 who made <= 5 non-restaurant trips that year
    'Highly-Engaged': anyone not in Cohorts #1 or 2 who made 27+ non-restaurant trips that year
    'Moderately-Engaged': anyone not in Cohorts #1, 2, or 3 who made 6-26 trips that year
 * 
 * ADDITIONALLY, we include customers who did NOT shop the 12-mo period, but were acquired the quarter 
 *   JUST AFTER the period ended.  We call this cohort 'Acquired Mid-Qtr'
 * 
 * Steps involved:
 * 0. Create a lookup table of "Realigned" fiscal dates (going back 8 years) 
 * 1. Create date lookup table
 * 2. Create lookup table of all Restaurant tran-line items
 * 3. Create lookup table of all Acquired Customers
 * 4. Create a customer/quarter-level table (including Acquisition flag & # of non-restaurant trips)
 * 5. Create lookup tables containing start & end dates by quarter & rolling-4-qtr year
 * 6. Aggregate #4 to a customer/rolling-4-quarter level
 * 7. Create final table and insert 
 *      a) all customers shopping the 4-qtr period (table #5)
 *      b) any customers Acquired the quarter JUST AFTER the 4-qtr period ended
 * 
 ************************************************************************************/
/************************************************************************************/


/***************************************************************************************/
/********* Step 0: create realigned fiscal calendar (going back far enough to include 4+ years of Cohorts) *********/
/***************************************************************************************/

/********* Step 0-a: Going back 8 years from today, find all years with a "53rd week" *********/
--drop table week_53_yrs;
create MULTISET volatile table week_53_yrs as (
select year_num
  ,rank() over (order by year_num desc) recency_rank
from
  (
  select distinct year_num
  from PRD_NAP_USR_VWS.DAY_CAL
  where week_of_fyr = 53
    and day_date between current_date-(365*8) and current_date
  ) x 
) with data primary index(year_num) on commit preserve rows;

/********* Step 0-b: Count the # years with a "53rd week" *********/
--drop table week_53_yr_count;
create MULTISET volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

/********* Step 0-c: Create an empty realigned calendar *********/
--drop table realigned_calendar;
create MULTISET volatile table realigned_calendar ,NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO 
(
  day_date date 
  ,day_num integer 
  ,day_desc varchar(30)
  ,week_num integer
  ,week_desc varchar(30)
  ,month_num integer
  ,month_short_desc varchar(30)
  ,quarter_num integer
  ,halfyear_num integer
  ,year_num integer
  ,month_454_num integer
  ,year_454_num integer
) primary index (day_date) on commit preserve rows;

/********* Step 0-d: insert data into realigned calendar 
 *  (any weeks before the latest 53rd week should get started 7 days late,
 *   any weeks before the 2nd-latest 53rd week--if there was one--should get started 14 days late)
 * *********/
insert into realigned_calendar
select case when year_num > (select year_num from week_53_yrs where recency_rank=1)
              then day_date
            when (select year_count from week_53_yr_count) = 1
                and year_num <= (select year_num from week_53_yrs where recency_rank=1)
              then day_date + 7
            when (select year_count from week_53_yr_count) = 2
                and year_num >  (select year_num from week_53_yrs where recency_rank=2)
              then day_date + 7
            when (select year_count from week_53_yr_count) = 2
                and year_num <=  (select year_num from week_53_yrs where recency_rank=2)
              then day_date+14
            else null end day_date
  ,day_num
  ,day_desc
  ,week_num
  ,week_desc
  ,month_num
  ,month_short_desc
  ,quarter_num
  ,halfyear_num
  ,year_num
  ,month_454_num
  ,year_num year_454_num
from PRD_NAP_USR_VWS.DAY_CAL
where day_date between current_date-(365*8) and current_date
  and week_of_fyr <> 53
;



/***************************************************************************************/
/********* Step 1: create a date lookup table *********/
/***************************************************************************************/

/********* Step 1-a: Find the current fiscal quarter & week *********/
create multiset volatile table current_date_qtr as (
select  max(week_num) as current_week
       ,max(quarter_num) as current_qtr
from realigned_calendar
where day_date = {end_date}
)with data primary index(current_qtr) on commit preserve rows;


/********* Step 1-b: Set the start & end dates (also, the most recently-ended fiscal quarter) 
 *  NOTE: data should span the last 16 most recently-completed fiscal quarters
 *        AND include all of the current quarter through the most recently-completed fiscal week
 * *********/
--drop table date_lookup;
create multiset volatile table date_lookup as (
select min(day_date) start_dt
    ,max(day_date) end_dt
    ,min(quarter_num) start_qtr
    ,max(case when quarter_num < (select current_qtr from current_date_qtr) then quarter_num else null end) last_full_qtr
    ,max(case when year_num=2021 then day_date else null end) p1_end_dt
    ,min(case when year_num=2022 then day_date else null end) p2_start_dt
from realigned_calendar
--where quarter_num >= (select current_qtr from current_date_qtr) - 40
where quarter_num >= 20191
  and week_num < (select current_week from current_date_qtr)
)with data primary index(start_dt) on commit preserve rows;



/***************************************************************************************/
/********* Step 2: create lookup table of all Restaurant tran-line items *********/
/***************************************************************************************/

/********* Step 2-a: create lookup table of all Restaurant depts *********/
--drop table restaurant_service_lookup;
CREATE multiset volatile TABLE restaurant_dept_lookup AS (
select distinct dept_num
from prd_nap_usr_vws.DEPARTMENT_DIM  
where division_num=70
) with data primary index(dept_num) on commit preserve rows;

COLLECT STATISTICS COLUMN (DEPT_NUM) ON restaurant_dept_lookup;

/********* Step 2-b: create lookup table of all Restaurant transaction line-items (2019-2021) *********/
--drop table restaurant_tran_lines;
CREATE multiset volatile TABLE restaurant_tran_lines_p1 AS (
select distinct dtl.global_tran_id
  ,dtl.line_item_seq_num
FROM prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW dtl
  join prd_nap_usr_vws.store_dim st on
         (case
          when dtl.line_item_order_type = 'CustInitWebOrder'
            and dtl.line_item_fulfillment_type = 'StorePickUp'
            and dtl.line_item_net_amt_currency_code = 'USD'
            then 808
          when dtl.line_item_order_type = 'CustInitWebOrder'
            and dtl.line_item_fulfillment_type = 'StorePickUp'
            and dtl.line_item_net_amt_currency_code = 'CAD'
            then 867
          else dtl.intent_store_num end) = st.store_num
  join restaurant_dept_lookup r on coalesce(dtl.merch_dept_num,-1*dtl.intent_store_num)=r.dept_num
WHERE
     dtl.business_day_date between (select start_dt from date_lookup) and  (select p1_end_dt from date_lookup)
     and dtl.acp_id is not null
     and dtl.line_net_usd_amt >0
     and st.business_unit_desc in ('FULL LINE','FULL LINE CANADA')
)with data unique primary index(global_tran_id,line_item_seq_num) on commit preserve rows;

/********* Step 2-c: create lookup table of all Restaurant transaction line-items (2022-present) *********/
CREATE multiset volatile TABLE restaurant_tran_lines_p2 AS (
select distinct dtl.global_tran_id
  ,dtl.line_item_seq_num
FROM prd_nap_usr_vws.RETAIL_TRAN_DETAIL_FACT_VW dtl
  join prd_nap_usr_vws.store_dim st on
         (case
          when dtl.line_item_order_type = 'CustInitWebOrder'
            and dtl.line_item_fulfillment_type = 'StorePickUp'
            and dtl.line_item_net_amt_currency_code = 'USD'
            then 808
          when dtl.line_item_order_type = 'CustInitWebOrder'
            and dtl.line_item_fulfillment_type = 'StorePickUp'
            and dtl.line_item_net_amt_currency_code = 'CAD'
            then 867
          else dtl.intent_store_num end) = st.store_num
  join restaurant_dept_lookup r on coalesce(dtl.merch_dept_num,-1*dtl.intent_store_num)=r.dept_num
WHERE
     dtl.business_day_date between (select p2_start_dt from date_lookup) and  (select end_dt+14 from date_lookup)
     and dtl.acp_id is not null
     and dtl.line_net_usd_amt >0
     and st.business_unit_desc in ('FULL LINE','FULL LINE CANADA')
)with data unique primary index(global_tran_id,line_item_seq_num) on commit preserve rows;


/***************************************************************************************/
/********* Step 3: create lookup table of all Acquired Customers (shopping TY for 1st time in 4 years) *********/
/***************************************************************************************/
create multiset volatile table acquired_customers as (
select distinct a.acp_id
    ,a.aare_global_tran_id global_tran_id
from prd_nap_usr_vws.customer_ntn_status_fact  a
where a.aare_status_date between (select start_dt from date_lookup) and (select end_dt from date_lookup)
) with data primary index (global_tran_id) on commit preserve rows;


/***************************************************************************************/
/********* Step 4: Create a customer/quarter-level table 
 *  (include the components needed to build Engagement Cohorts: non-restaurant trip counts & acquired flag)
 *  
 *  NOTE: in some cases, customers who look "Acquired" in prd_nap_usr_vws.customer_ntn_status_fact
 *          actually have prior shopping history, so we'll code their acquired_ind = 0 when this happens
 * *********/
/***************************************************************************************/


/********* Step 4-a: Extract only the necessary data from SARF (2019-2021) *********/
create multiset volatile table SARF_EXTRACT_p1 as
(
    select distinct dtl.acp_id
        ,dtl.global_tran_id
        ,dtl.line_item_seq_num
        ,coalesce(dtl.shipped_usd_sales,0) gross_sales
        ,coalesce(dtl.order_date,dtl.tran_date) date_shopped
        ,case when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp' 
                  and (dtl.business_unit_desc like '%CANADA' or dtl.business_unit_desc like '%.CA') then 867
               when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp' then 808
               else dtl.intent_store_num end store_num
        ,case when coalesce(dtl.employee_discount_usd_amt,0) <> 0 then 1 else 0 end employee_ind
    from t2dl_das_sales_returns.sales_and_returns_fact as dtl
    where coalesce(dtl.order_date,dtl.tran_date) >= (select start_dt from date_lookup) 
        and dtl.business_day_date between (select start_dt from date_lookup) and  (select p1_end_dt from date_lookup)
        and dtl.acp_id is not null
        and dtl.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE')
        and dtl.shipped_usd_sales >= 0                           --
        and coalesce(dtl.return_usd_amt,0)>=0
)with data primary index(date_shopped, global_tran_id, line_item_seq_num) 
partition by range_n(date_shopped between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;


/********* Step 4-b: Extract only the necessary data from SARF (2022-present) *********/
create multiset volatile table SARF_EXTRACT_p2 as
(
    select distinct dtl.acp_id
        ,dtl.global_tran_id
        ,dtl.line_item_seq_num
        ,coalesce(dtl.shipped_usd_sales,0) gross_sales
        ,coalesce(dtl.order_date,dtl.tran_date) date_shopped
        ,case when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp' 
                  and (dtl.business_unit_desc like '%CANADA' or dtl.business_unit_desc like '%.CA') then 867
               when dtl.line_item_order_type = 'CustInitWebOrder' and dtl.line_item_fulfillment_type = 'StorePickUp' then 808
               else dtl.intent_store_num end store_num
        ,case when coalesce(dtl.employee_discount_usd_amt,0) <> 0 then 1 else 0 end employee_ind
    from t2dl_das_sales_returns.sales_and_returns_fact as dtl
    where coalesce(dtl.order_date,dtl.tran_date)  <=(select end_dt from date_lookup)
        and dtl.business_day_date between (select p2_start_dt from date_lookup) and  (select end_dt+14 from date_lookup)
        and dtl.acp_id is not null
        and dtl.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK','RACK CANADA','OFFPRICE ONLINE')
        and dtl.shipped_usd_sales >= 0                           --
        and coalesce(dtl.return_usd_amt,0)>=0
)with data primary index(date_shopped, global_tran_id, line_item_seq_num) 
--partition by range_n(date_shopped between date '2017-01-01' and date '2025-12-31' each interval '1' day,
--unknown)
on commit preserve rows;



/********* Step 4-c: Create empty Customer/Quarter-level table *********/
--drop table cust_qtr_seg_elements_prep;
create MULTISET volatile table cust_qtr_seg_elements_prep ,NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO 
(    
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,quarter_num INTEGER NOT NULL
    ,nonrestaurant_trips DECIMAL(22,2) 
    ,acquired_ind  INTEGER COMPRESS
    ,employee_ind  INTEGER COMPRESS
) primary index (acp_id, quarter_num) on commit preserve rows
;

/********* Step 4-d: Aggregate to customer/quarter-level (2019-2021) *********/
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID) ON     acquired_customers;
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM)     ON SARF_EXTRACT_p1;
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM)     ON restaurant_tran_lines_p1;
COLLECT STATISTICS COLUMN (STORE_NUM) ON SARF_EXTRACT_p1;
COLLECT STATISTICS COLUMN (DAY_DATE) ON     realigned_calendar;
COLLECT STATISTICS COLUMN (DATE_SHOPPED ,STORE_NUM) ON     SARF_EXTRACT_p1;

insert into cust_qtr_seg_elements_prep
select dtl.acp_id
    ,cal.quarter_num
    ,count(distinct case when dtl.gross_sales>0 and r.line_item_seq_num is null then dtl.store_num||dtl.date_shopped 
                         else null end) nonrestaurant_trips
    ,max(case when ntn.global_tran_id is not null then 1 else 0 end) acquired_ind
    ,max(dtl.employee_ind) employee_ind
from SARF_EXTRACT_p1 dtl
    join prd_nap_usr_vws.store_dim as st on dtl.store_num = st.store_num
    join realigned_calendar as cal on dtl.date_shopped = cal.day_date 
    left join restaurant_tran_lines_p1 r on dtl.global_tran_id=r.global_tran_id and dtl.line_item_seq_num=r.line_item_seq_num
    left join acquired_customers ntn on dtl.global_tran_id=ntn.global_tran_id   
group by 1,2
;

/********* Step 4-e: Aggregate to customer/quarter-level (2022-preset) *********/
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM)     ON SARF_EXTRACT_p2;
COLLECT STATISTICS COLUMN (GLOBAL_TRAN_ID ,LINE_ITEM_SEQ_NUM)     ON restaurant_tran_lines_p2;
COLLECT STATISTICS COLUMN (STORE_NUM) ON SARF_EXTRACT_p2;
COLLECT STATISTICS COLUMN (DATE_SHOPPED ,STORE_NUM) ON     SARF_EXTRACT_p2;

insert into cust_qtr_seg_elements_prep
select dtl.acp_id
    ,cal.quarter_num
    ,count(distinct case when dtl.gross_sales>0 and r.line_item_seq_num is null then dtl.store_num||dtl.date_shopped 
                         else null end) nonrestaurant_trips
    ,max(case when ntn.global_tran_id is not null then 1 else 0 end) acquired_ind
    ,max(dtl.employee_ind) employee_ind
from SARF_EXTRACT_p2 dtl
    join prd_nap_usr_vws.store_dim as st on dtl.store_num = st.store_num
    join realigned_calendar as cal on dtl.date_shopped = cal.day_date 
    left join restaurant_tran_lines_p2 r on dtl.global_tran_id=r.global_tran_id and dtl.line_item_seq_num=r.line_item_seq_num
    left join acquired_customers ntn on dtl.global_tran_id=ntn.global_tran_id   
group by 1,2
;

/********* Step 4-f: remove duplicates *********/
COLLECT STATISTICS COLUMN (ACP_ID ,QUARTER_NUM) ON cust_qtr_seg_elements_prep;

-- drop table cust_qtr_seg_elements;
CREATE multiset volatile TABLE cust_qtr_seg_elements AS ( 
select acp_id
    ,quarter_num
    ,max(nonrestaurant_trips) nonrestaurant_trips
    ,max(acquired_ind) acquired_ind
    ,max(employee_ind) employee_ind
from cust_qtr_seg_elements_prep
where quarter_num >= (select start_qtr from date_lookup) 
group by 1,2
) with data primary index(acp_id,quarter_num) on commit preserve rows;


/********* Step 4-f: For each quarter a customer shopped, find the PRIOR quarter they shopped *********/
COLLECT STATISTICS COLUMN (ACP_ID) ON cust_qtr_seg_elements;

CREATE multiset volatile TABLE cust_qtr_time_since AS ( 
select acp_id
    ,quarter_num
    ,nonrestaurant_trips
    ,acquired_ind
    ,employee_ind
    ,lag(quarter_num) over (partition by acp_id order by quarter_num) as prior_qtr
    ,quarter_num - lag(quarter_num) over (partition by acp_id order by quarter_num) time_passed
from cust_qtr_seg_elements
) with data primary index(acp_id,quarter_num) on commit preserve rows;


/********* Step 4-g: If an Acquired customer has actually shopped within the past 4 years, do NOT call them Acquired *********/
--drop table cust_qtr_corrected;
CREATE multiset volatile TABLE cust_qtr_corrected AS ( 
select acp_id
    ,quarter_num
    ,nonrestaurant_trips
    ,acquired_ind acquired_ind_orig
    ,employee_ind
    ,prior_qtr
    ,time_passed
    ,case when coalesce(time_passed,99) < 40 then 0 else acquired_ind end acquired_ind_fixed
from cust_qtr_time_since
) with data primary index(acp_id,quarter_num) on commit preserve rows;

/***************************************************************************************/
/********* Step 5: Create lookup tables for:
 * a) quarter start & end dates
 * b) rolling-4-qtr period start & end quarters
 * c) rolling-4-qtr period start & end dates                                   *********/
/***************************************************************************************/

/********* Step 5-a: create lookup table of every quarter's start & end dates *********/
create multiset volatile table quarter_start_end_dates as (
select quarter_num
    ,min(day_date) start_dt
    ,max(day_date) end_dt
from realigned_calendar
--where quarter_num >= (select current_qtr from current_date_qtr) - 40
where quarter_num >= 20191
  and week_num < (select current_week from current_date_qtr)
group by 1
)with data primary index(quarter_num) on commit preserve rows;


/********* Step 5-b: create lookup table of every 4-qtr period's start & end quarter *********/
create multiset volatile table rolling_4_qtr_periods as (
select distinct r4_start_qtr
    ,r4_end_qtr
from
   (
    select distinct case when (quarter_num mod 10) = 4 then (quarter_num - 3) else (quarter_num - 9) end r4_start_qtr
        ,quarter_num as r4_end_qtr
    from realigned_calendar
    where quarter_num between (select start_qtr from date_lookup) and (select last_full_qtr from date_lookup)
    ) x 
where r4_start_qtr >= (select start_qtr from date_lookup)
)with data primary index(r4_end_qtr) on commit preserve rows;


/********* Step 5-c: create lookup table of every 4-qtr period's start & end dates *********/
create multiset volatile table period_start_end_dates as (
select distinct r4_start_qtr
    ,r4_end_qtr
    ,st.start_dt r4_start_dt
    ,en.end_dt r4_end_dt
from rolling_4_qtr_periods x 
    left join quarter_start_end_dates st on x.r4_start_qtr=st.quarter_num
    left join quarter_start_end_dates en on x.r4_end_qtr=en.quarter_num
)with data primary index(r4_end_qtr) on commit preserve rows;


/********* Step 5-d: cartesian join of all possible quarters within 1 year of each other *********/
--drop table all_quarter_combos;
create multiset volatile table all_quarter_combos as (
select distinct a.quarter_num qtr_A
    ,b.quarter_num qtr_B
from 
    (
    select quarter_num
    from realigned_calendar
    where quarter_num between (select start_qtr from date_lookup) and (select last_full_qtr from date_lookup)
    ) a
cross join
    (
    select quarter_num
    from realigned_calendar
    where quarter_num between (select start_qtr from date_lookup) and (select last_full_qtr from date_lookup)
    ) b
where (b.quarter_num - a.quarter_num) < 10
  and a.quarter_num<=b.quarter_num
)with data primary index(qtr_A,qtr_B) on commit preserve rows;


/********* Step 5-e: map every quarter to a 4-qtr period ending Q1, Q2, Q3, & Q4 *********/
create multiset volatile table quarter_to_rolling4_mapping as (
select qtr_A quarter_num
    ,min(case when  (qtr_B mod 10) = 1 then qtr_B else null end) YE_Q1
    ,min(case when  (qtr_B mod 10) = 2 then qtr_B else null end) YE_Q2
    ,min(case when  (qtr_B mod 10) = 3 then qtr_B else null end) YE_Q3
    ,min(case when  (qtr_B mod 10) = 4 then qtr_B else null end) YE_Q4
from all_quarter_combos
where qtr_b >= (select min(r4_end_qtr) from rolling_4_qtr_periods)
group by 1
)with data primary index(quarter_num) on commit preserve rows;




/***************************************************************************************/
/********* Step 6: Aggregate customer/qtr data to acustomer/rolling-4-qtr level *********/
/***************************************************************************************/

/********* Step 6-a: create empty customer/rolling-4-qtr-period table *********/
-- drop table cust_yr_seg_elements;
create MULTISET volatile table cust_yr_seg_elements ,NO FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO 
(    
    acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,quarter_num INTEGER NOT NULL
    ,acquired_ind  INTEGER COMPRESS
    ,employee_ind  INTEGER COMPRESS
    ,nonrestaurant_trips DECIMAL(22,2) 
) primary index (acp_id, quarter_num) on commit preserve rows
;

/********* Step 6-b: insert all the years ending in Q1 *********/

COLLECT STATISTICS COLUMN (QUARTER_NUM) ON     cust_qtr_corrected;
COLLECT STATISTICS COLUMN (YE_Q1) ON     quarter_to_rolling4_mapping;
COLLECT STATISTICS COLUMN (YE_Q2) ON     quarter_to_rolling4_mapping;
COLLECT STATISTICS COLUMN (YE_Q3) ON     quarter_to_rolling4_mapping;
COLLECT STATISTICS COLUMN (YE_Q4) ON     quarter_to_rolling4_mapping;
COLLECT STATISTICS COLUMN (QUARTER_NUM) ON     quarter_to_rolling4_mapping;


insert into cust_yr_seg_elements
select a.acp_id
    ,b.YE_Q1 quarter_num
    ,max(a.acquired_ind_fixed) acquired_ind
    ,max(a.employee_ind) employee_ind
    ,sum(a.nonrestaurant_trips) nonrestaurant_trips
from cust_qtr_corrected as a
  join quarter_to_rolling4_mapping b on a.quarter_num =b.quarter_num
where b.YE_Q1 is not null
group by 1,2
;

/********* Step 6-c: insert all the years ending in Q2 *********/
insert into cust_yr_seg_elements
select a.acp_id
    ,b.YE_Q2 quarter_num
    ,max(a.acquired_ind_fixed) acquired_ind
    ,max(a.employee_ind) employee_ind
    ,sum(a.nonrestaurant_trips) nonrestaurant_trips
from cust_qtr_corrected as a
  join quarter_to_rolling4_mapping b on a.quarter_num =b.quarter_num
where b.YE_Q2 is not null
group by 1,2
;

/********* Step 6-d: insert all the years ending in Q3 *********/
insert into cust_yr_seg_elements
select a.acp_id
    ,b.YE_Q3 quarter_num
    ,max(a.acquired_ind_fixed) acquired_ind
    ,max(a.employee_ind) employee_ind
    ,sum(a.nonrestaurant_trips) nonrestaurant_trips
from cust_qtr_corrected as a
  join quarter_to_rolling4_mapping b on a.quarter_num =b.quarter_num
where b.YE_Q3 is not null
group by 1,2
;

/********* Step 6-e: insert all the years ending in Q4 *********/
insert into cust_yr_seg_elements
select a.acp_id
    ,b.YE_Q4 quarter_num
    ,max(a.acquired_ind_fixed) acquired_ind
    ,max(a.employee_ind) employee_ind
    ,sum(a.nonrestaurant_trips) nonrestaurant_trips
from cust_qtr_corrected as a
  join quarter_to_rolling4_mapping b on a.quarter_num =b.quarter_num
where b.YE_Q4 is not null
group by 1,2
;


/***************************************************************************************/
/********* Step 7: Create final table (Engagement Cohort by customer/rolling-4-qtr period *********/
/***************************************************************************************/

/********* Step 7-a: clear up the old table *********/
delete from {audience_engagement_t2_schema}.audience_engagement_cohorts where 1=1;

/********* Step 7-b: insert Engagement Cohorts built using on past-4-qtr activity *********/

COLLECT STATISTICS COLUMN (QUARTER_NUM) ON      cust_yr_seg_elements;
COLLECT STATISTICS COLUMN (R4_END_QTR) ON       period_start_end_dates;
COLLECT STATISTICS COLUMN (QUARTER_NUM) ON      quarter_start_end_dates;
COLLECT STATISTICS COLUMN (EMPLOYEE_IND) ON     cust_yr_seg_elements;

insert into {audience_engagement_t2_schema}.audience_engagement_cohorts
select distinct a.acp_id
    ,a.defining_year_ending_qtr
    ,b.r4_start_dt defining_year_start_dt
    ,b.r4_end_dt defining_year_end_dt
    ,a.execution_qtr
    ,c.start_dt execution_qtr_start_dt
    ,c.end_dt execution_qtr_end_dt
    ,a.acquired_ind
    ,a.nonrestaurant_trips 
    ,a.engagement_cohort
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
    (  
    select distinct acp_id
        ,quarter_num defining_year_ending_qtr
        ,case when (quarter_num mod 10) = 4 then (quarter_num + 7) else (quarter_num + 1) end execution_qtr
        ,acquired_ind
        ,nonrestaurant_trips 
        ,case when acquired_ind = 1 and coalesce(nonrestaurant_trips,0) <= 5 then 'Acquire & Activate'
              when coalesce(nonrestaurant_trips,0) <= 5  then 'Lightly-Engaged'
              when coalesce(nonrestaurant_trips,0) <= 13 then 'Moderately-Engaged'
              when coalesce(nonrestaurant_trips,0) >= 14 then 'Highly-Engaged'
              else null end engagement_cohort
    from cust_yr_seg_elements
    where employee_ind = 0 
    ) a
    left join period_start_end_dates b on a.defining_year_ending_qtr=b.r4_end_qtr
    left join quarter_start_end_dates c on a.execution_qtr=c.quarter_num
;

/********* Step 7-c: insert "Acquired Mid-Qtr" customers, assigned to quarter BEFORE they were Acquired *********/

COLLECT STATISTICS COLUMN (ACQUIRED_IND_FIXED) ON     cust_qtr_corrected;
COLLECT STATISTICS COLUMN (EMPLOYEE_IND) ON     cust_qtr_corrected;

insert into {audience_engagement_t2_schema}.audience_engagement_cohorts
select distinct a.acp_id
    ,a.defining_year_ending_qtr
    ,b.r4_start_dt defining_year_start_dt
    ,b.r4_end_dt defining_year_end_dt
    ,a.execution_qtr
    ,c.start_dt execution_qtr_start_dt
    ,c.end_dt execution_qtr_end_dt
    ,a.acquired_ind
    ,a.nonrestaurant_trips 
    ,a.engagement_cohort
    ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
    (
    select distinct acp_id
        ,case when (quarter_num mod 10) = 1 then (quarter_num - 7) else (quarter_num - 1) end defining_year_ending_qtr
        ,quarter_num execution_qtr
        ,null acquired_ind
        ,null nonrestaurant_trips
        ,'Acquired Mid-Qtr' engagement_cohort
    from cust_qtr_corrected
    where acquired_ind_fixed=1
        and employee_ind=0
        and quarter_num > (select min(r4_end_qtr) from rolling_4_qtr_periods)
    ) a 
    left join period_start_end_dates b on a.defining_year_ending_qtr=b.r4_end_qtr
    left join quarter_start_end_dates c on a.execution_qtr=c.quarter_num
;


COLLECT STATISTICS 
COLUMN (PARTITION),
COLUMN(acp_id),
COLUMN(execution_qtr) ON {audience_engagement_t2_schema}.audience_engagement_cohorts;

SET QUERY_BAND = NONE FOR SESSION;