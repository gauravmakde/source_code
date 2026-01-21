SET QUERY_BAND = 'App_ID=APP08159;
     DAG_ID=cust_service_11521_ACE_ENG;
     Task_Name=cust_service_6mo;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {service_eng_t2_schema}.cust_service_6mo
Team：Customer Analytics - Styling & Strategy
Date Created: Mar. 12th 2023 

Note:
-- Update Cadence: Monthly (after end of each fiscal month)

*/


/***************************************************************************************/
/***************************************************************************************/
/********* Step 1: create realigned fiscal calendar                            *********/
/***************************************************************************************/
/***************************************************************************************/

/***************************************************************************************/
/********* Step 1-a: Going back to start of 2009, find all years with a "53rd week" *********/
/***************************************************************************************/
--drop table week_53_yrs;
create MULTISET volatile table week_53_yrs as (
select year_num
  ,rank() over (order by year_num desc) recency_rank
from
  (
  select distinct year_num
  from PRD_NAP_USR_VWS.DAY_CAL
  where week_of_fyr = 53
    and day_date between date'2009-01-01' and current_date+365 
  ) x 
) with data primary index(year_num) on commit preserve rows;

/***************************************************************************************/
/********* Step 1-b: Count the # years with a "53rd week" *********/
/***************************************************************************************/
--drop table week_53_yr_count;
create MULTISET volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

/***************************************************************************************/
/********* Step 1-c: Create an empty table *********/
/***************************************************************************************/
--drop table realigned_fiscal_calendar;
create MULTISET volatile table realigned_fiscal_calendar ,NO FALLBACK ,
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

/***************************************************************************************/
/********* Step 1-d: insert data into realigned calendar 
 *  (any weeks before the latest 53rd week should get started 7 days late,
 *   any weeks before the 2nd-latest 53rd week year get started 14 days late,
 *   any weeks before the 3rd-latest 53rd week year get started 21 days late,
 *   any weeks before the 4th-latest 53rd week year get started 28 days late,
 * *********/
 /***************************************************************************************/
insert into realigned_fiscal_calendar
select
  case when year_num > (select year_num from week_53_yrs where recency_rank=1) then day_date
        when (select year_count from week_53_yr_count) = 1 and year_num <= (select year_num from week_53_yrs where recency_rank=1) then day_date + 7
        when (select year_count from week_53_yr_count) = 2 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
        when (select year_count from week_53_yr_count) = 2 and year_num <= (select year_num from week_53_yrs where recency_rank=2) then day_date + 14
        when (select year_count from week_53_yr_count) = 3 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
        when (select year_count from week_53_yr_count) = 3 and year_num >  (select year_num from week_53_yrs where recency_rank=3) then day_date + 14
        when (select year_count from week_53_yr_count) = 3 and year_num <= (select year_num from week_53_yrs where recency_rank=3) then day_date + 21
        when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=2) then day_date + 7
        when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=3) then day_date + 14
        when (select year_count from week_53_yr_count) = 4 and year_num >  (select year_num from week_53_yrs where recency_rank=4) then day_date + 21
        when (select year_count from week_53_yr_count) = 4 and year_num <= (select year_num from week_53_yrs where recency_rank=4) then day_date + 28
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
where day_date between date'2009-01-01' and current_date+365 
  and week_of_fyr <> 53
;

COLLECT STATISTICS COLUMN (MONTH_NUM) ON     realigned_fiscal_calendar;
COLLECT STATISTICS COLUMN (DAY_DATE) ON     realigned_fiscal_calendar;

--DROP TABLE get_months;
CREATE multiset volatile TABLE get_months AS (
select 
  distinct earliest_minus_3
  ,earliest_act_mo  

  ,last_complete_mo-200 part1_end_mo
  ,case when mod(last_complete_mo,100)=12 then last_complete_mo-111 else last_complete_mo-199 end part2_start_mo
  ,last_complete_mo-100 part2_end_mo
  ,case when mod(last_complete_mo,100)=12 then last_complete_mo-11 else last_complete_mo-99 end part3_start_mo
  ,last_complete_mo

from
  ( select distinct current_date todays_date
       ,month_num todays_month
       ,case when mod(month_num,100)=1 then month_num-89 else month_num-1 end last_complete_mo
       ,case when mod(month_num,100)=12 then month_num-211 else month_num-299 end earliest_act_mo
       ,case when mod(month_num,100) in (1,2) then month_num-390 else month_num-302 end earliest_minus_3
    from realigned_fiscal_calendar a
    where day_date=(current_date /*- 25*/)
    --where day_date='2020-02-25'
  ) x
) with data primary index(last_complete_mo) on commit preserve rows;

-- Create lookup table of start & end month for all 6-mo periods
--drop table fiscal_months;
create multiset volatile table fiscal_months as (
select 
    distinct 
    case when mod(cast(month_num as integer),100) in (1,2,3,4,5) then month_num - 93
         else month_num - 5 end start_6mo
    ,month_num year_ending
from realigned_fiscal_calendar
where month_num between
  (SELECT part2_start_mo FROM get_months) AND (SELECT last_complete_mo FROM get_months)
) with data primary index (year_ending) on commit preserve rows;

/************************************************************************/
/*F-4) Create a permanent customer /6-mo/ service level table           */
/*************************************************************************/
-- Aggregate data to a customer/6-mo/service-level
--drop table acp_id_service_6mo_temp;
create multiset volatile table acp_id_service_6mo_temp as (
select a.acp_id
  ,b.year_ending six_mo_ending
  ,a.service_name
  ,max(a.customer_qualifier) customer_qualifier
  ,sum(a.gross_usd_amt_whole) gross_usd_amt_whole
  ,sum(a.net_usd_amt_whole) net_usd_amt_whole
  ,sum(a.gross_usd_amt_split) gross_usd_amt_split
  ,sum(a.net_usd_amt_split) net_usd_amt_split
  ,max(a.private_style) as private_style
from {service_eng_t2_schema}.cust_service_month a 
  join fiscal_months b on a.month_num between b.start_6mo and b.year_ending 
group by 1,2,3
) with data unique primary index (acp_id,six_mo_ending,service_name) on commit preserve rows;

/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------
*/
delete from {service_eng_t2_schema}.cust_service_6mo;

insert into {service_eng_t2_schema}.cust_service_6mo
select distinct a.acp_id
  ,a.six_mo_ending
  ,a.service_name
  ,a.customer_qualifier
  ,a.gross_usd_amt_whole
  ,a.net_usd_amt_whole
  ,a.gross_usd_amt_split
  ,a.net_usd_amt_split
  ,a.private_style
  ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from  acp_id_service_6mo_temp a 
  join 
     (select distinct acp_id
      ,six_mo_ending
      from acp_id_service_6mo_temp
      where service_name='Z_) Nordstrom') b on a.acp_id=b.acp_id and a.six_mo_ending=b.six_mo_ending
;

COLLECT STATISTICS COLUMN(acp_id),
                   COLUMN(six_mo_ending),
                   COLUMN(service_name)
ON {service_eng_t2_schema}.cust_service_6mo;

SET QUERY_BAND = NONE FOR SESSION;