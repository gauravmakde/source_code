SET QUERY_BAND = 'App_ID=APP08844;
     DAG_ID=account_sessions_fact_11521_ACE_ENG;
     Task_Name=account_session_status;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_BIE_DEV.account_session_status
Team/Owner: Rasagnya Avala
Date Created/Modified: 2023-01-18

Note:
-- What is the purpose of the table - To create an intermediate table to be used to consolidate both account experiences and sisu
-- What is the update cadence/lookback window - This needs to run daily and there's no date range on this table as we want to pull all records
*/

/*
First, create the volatile table customer_enrollment_dates, which contains icon ID and open and close dates of loyalty and Nordy card enrollment
to compare with the dates of their sessions to determine their account status at the time.

There is no date range on this table as we want to pull all records.
*/
--drop table customer_enrollment_dates;
create multiset volatile table customer_enrollment_dates as
(
    select
        accounts.unique_source_id
        -- Loyalty open and close enrollment dates
        ,max(loyalty.member_enroll_date) as member_enroll_date
        ,max(loyalty.member_close_date) as member_close_date
        -- Cardmember open and close enrollment dates
        ,max(loyalty.cardmember_enroll_date) as cardmember_enroll_date
        ,max(loyalty.cardmember_close_date) as cardmember_close_date
        
    from PRD_NAP_USR_VWS.CUSTOMER_OBJ_PROGRAM_DIM as accounts
        left join PRD_NAP_USR_VWS.LOYALTY_MEMBER_DIM_VW as loyalty
            on accounts.program_index_id = loyalty.loyalty_id
            
    where accounts.customer_source = 'icon'
        and accounts.program_index_name in ('WEB', 'MTLYLTY')
        
    group by 1
)
with data
primary index(unique_source_id)
on commit preserve rows
;

/*
Second, create the volatile table session_cust_ids, which contains the date, session ID, and icon IDs of each session.

The date range of the sessions of interest is added here.
*/
--drop table session_cust_ids;
create multiset volatile table session_cust_ids as
(
    select distinct
        activity_date_pacific
        ,session_id
        ,ent_cust_id
        
    from PRD_NAP_USR_VWS.CUSTOMER_SESSION_XREF
    
    where activity_date_pacific between {start_date} and {end_date}
)
with data
primary index(activity_date_pacific, session_id)
partition by range_n(activity_date_pacific between {start_date} and {end_date} each interval '1' day, unknown)
on commit preserve rows
;

/*
Third, create the volatile table session_account_statuses, which joins the two tables above and contains the date, session ID, and account status of each session.
Account status is mapped using the following key:
- 3: Cardholder
- 2: Loyalty member
- 1: Account holder
- 0: Guest

The account status of each session is determined by taking the highest account status of any customer ID within the session.
The highest account status is defined as that most engrained in the Nordstrom loyalty program,
with the hierarchy being Cardholder > Loyalty members > Account holders > Guests.

The date range of the sessions of interest is pulled from the table session_cust_ids above.
*/
--drop table session_account_statuses;
create multiset volatile table session_account_statuses as
(
    select
        activity_date_pacific
        ,session_id
        ,max(account_status) as account_status
        
    from
    (
        select
            sessions.activity_date_pacific
            ,sessions.session_id
            ,sessions.ent_cust_id
            ,case when enrollment.cardmember_enroll_date <= sessions.activity_date_pacific
                    and (enrollment.cardmember_close_date >= sessions.activity_date_pacific
                        or enrollment.cardmember_close_date is null) then 3 -- Cardholder
                when enrollment.member_enroll_date <= sessions.activity_date_pacific
                    and (enrollment.member_close_date >= sessions.activity_date_pacific
                        or enrollment.member_close_date is null) then 2 -- Loyalty member
                when enrollment.unique_source_id is not null then 1 -- Account holder
                else 0 -- Guest
                    end as account_status
                    
        from session_cust_ids as sessions
            left join customer_enrollment_dates as enrollment
                on sessions.ent_cust_id = enrollment.unique_source_id
    ) as x
    
    group by 1,2
)
with data
primary index(activity_date_pacific, session_id)
partition by range_n(activity_date_pacific between {start_date} and {end_date} each interval '1' day, unknown)
on commit preserve rows
; 

/*
Finally, delete records from and insert records into T2DL_DAS_BIE_DEV.account_session_status.

The table pulls channel values of 'NORDSTROM' and 'NORDSTROM_RACK' to limit the data to full line and rack
and experience values of 'DESKTOP_WEB', 'MOBILE_WEB', 'IOS_APP', and 'ANDROID_APP' to limit the data to online activity only.

The date range of the sessions of interest is pulled from the table sessions above and loaded to T2DL_DAS_BIE_DEV.account_session_status.
*/
delete from {sessions_t2_schema}.account_session_status
where activity_date_pacific between {start_date} and {end_date}
;

insert into {sessions_t2_schema}.account_session_status
select  sessions.*
        ,events.channelcountry
        ,events.channel
        ,events.experience
    from session_account_statuses as sessions
        inner join PRD_NAP_USR_VWS.CUSTOMER_SESSION_FACT as events
            on sessions.activity_date_pacific = events.activity_date_pacific
            	and sessions.session_id = events.session_id
            	and events.activity_date_pacific between {start_date} and {end_date}
                and events.channel in ('NORDSTROM', 'NORDSTROM_RACK')
                and events.experience in ('DESKTOP_WEB', 'MOBILE_WEB', 'IOS_APP', 'ANDROID_APP')

    group by 1,2,3,4,5,6
;

collect statistics column (activity_date_pacific)
                   ,column (channelcountry)
                   ,column (channel)
                   ,column (experience)
on {sessions_t2_schema}.account_session_status;
;
SET QUERY_BAND = NONE FOR SESSION;