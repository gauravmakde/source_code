SET QUERY_BAND = 'App_ID=APP08750;
     DAG_ID=wishlist_sessions_11521_ACE_ENG;
     Task_Name=wishlist_sessions;'
     FOR SESSION VOLATILE;

/*

T2/Table Name: T2DL_DAS_WISHLIST_DIGITAL_EXP.wishlist_sessions_daily
Team/Owner: Sachin Goyal
Date Created: 01/10/2023
Date Modified: 02/03/2023

Note:
-- What is the the purpose of the table 
-- What is the update cadence/lookback window

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from    {wishlist_t2_schema}.wishlist_sessions_daily
where   activity_date between {start_date} and {end_date}
;

insert into {wishlist_t2_schema}.wishlist_sessions_daily
select
    activity_date,
    channelcountry,
    channel,
    experience,
    pdv_sessions,
    atwl_sessions,
    pscv_sessions,
    atb_sessions,
    ord_sessions,
    demand,
    CURRENT_TIMESTAMP as dw_sys_load_tmstp
from (
    select distinct *
    from {wishlist_t2_schema}.wishlist_sessions_daily_ldg
    ) a
;

collect statistics column (channelcountry, channel, experience)
on {wishlist_t2_schema}.wishlist_sessions_daily
;

drop table {wishlist_t2_schema}.wishlist_sessions_daily_ldg 
;

SET QUERY_BAND = NONE FOR SESSION;