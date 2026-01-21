/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=usl_rolling_52wk_calendar_11521_ACE_ENG;
     Task_Name=usl_rolling_52wk_calendar;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.rolling_52wk_calendar
Team/Owner: Customer Analytics - Grant Coffey, Niharika Srivastava
Date Created/Modified: Mar 22 2024*/





/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Create a Fiscal Calendar (spanning 2009 - end of current fiscal year)
 * where "Years" = consecutive rolling 52-week periods
 * 
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/

/***************************************************************************************/
/********* Step 1: Set date range: start of FY 2009 thru end of current year *********/
/***************************************************************************************/

create MULTISET volatile table cal_date_range as (
select date'2009-02-01' range_min_dt
  ,max(day_date) range_max_dt
from PRD_NAP_USR_VWS.DAY_CAL a 
where a.year_num= 
      (select distinct year_num
       from PRD_NAP_USR_VWS.DAY_CAL
       where day_date=current_date)
group by 1
) with data primary index(range_min_dt) on commit preserve rows;

/***************************************************************************************/
/********* Step 2: Going back to start of 2009, find all years with a "53rd week" *********/
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
    and day_date between (select range_min_dt from cal_date_range) and (select range_max_dt from cal_date_range)
  ) x 
) with data primary index(year_num) on commit preserve rows;

/***************************************************************************************/
/********* Step 3: Count the # years with a "53rd week" *********/
/***************************************************************************************/

--drop table week_53_yr_count;
create MULTISET volatile table week_53_yr_count as (
select count(distinct year_num) year_count
from week_53_yrs x 
) with data primary index(year_count) on commit preserve rows;

/***************************************************************************************/
/********* Step 4: insert data into realigned calendar 
 *  (any weeks before the latest 53rd week should start & end 7 days late,
 *   any weeks before the 2nd-latest 53rd week year should start & end 14 days late,
 *   any weeks before the 3rd-latest 53rd week year should start & end 21 days late,
 *   any weeks before the 4th-latest 53rd week year should start & end 28 days late,
 * *********/
 /***************************************************************************************/

delete from {usl_t2_schema}.usl_rolling_52wk_calendar where 1=1;

insert into {usl_t2_schema}.usl_rolling_52wk_calendar
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
  ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from PRD_NAP_USR_VWS.DAY_CAL
where day_date between (select range_min_dt from cal_date_range) and (select range_max_dt from cal_date_range)
  and week_of_fyr <> 53
;

COLLECT STATISTICS COLUMN (MONTH_NUM) ON {usl_t2_schema}.usl_rolling_52wk_calendar;
COLLECT STATISTICS COLUMN (QUARTER_NUM) ON {usl_t2_schema}.usl_rolling_52wk_calendar;
COLLECT STATISTICS COLUMN (YEAR_NUM) ON {usl_t2_schema}.usl_rolling_52wk_calendar;
COLLECT STATISTICS COLUMN (DAY_DATE) ON {usl_t2_schema}.usl_rolling_52wk_calendar;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;