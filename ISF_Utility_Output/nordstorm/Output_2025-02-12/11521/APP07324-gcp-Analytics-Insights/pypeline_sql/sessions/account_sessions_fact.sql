SET QUERY_BAND = 'App_ID=APP08844;
     DAG_ID=account_sessions_fact_11521_ACE_ENG;
     Task_Name=account_sessions_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_SESSIONS.account_sessions_fact
Team/Owner: Rasagnya Avala
Date Created/Modified: 2023-01-18

Note:
-- What is the purpose of the table - To consolidate both account_status and session_events tables
-- What is the update cadence/lookback window - This needs to run daily and there's no date range on this table as we want to pull all records
*/

/*
First, create the volatile table customer_enrollment_dates, which contains icon ID and open and close dates of loyalty and Nordy card enrollment
to compare with the dates of their sessions to determine their account status at the time.

There is no date range on this table as we want to pull all records.
*/
--drop table customer_enrollment_dates;

delete from {sessions_t2_schema}.account_sessions_fact
where activity_date_pacific between {start_date} and {end_date} 
;

insert into {sessions_t2_schema}.account_sessions_fact
select  
events.activity_date as activity_date_pacific
,events.channelcountry
,events.channel
,events.experience
,case when sessions.account_status = 3 then 'Cardholders'
        when sessions.account_status = 2 then 'Loyalty members'
        when sessions.account_status = 1 then 'Account holders'
        else 'Guests' end as account_status
,case when sessions.account_status = 0 then 'Guests'
        when not events.auth_flag = 1 then 'Recognized'
        else 'Authenticated' end as auth_status
,pagetype
,count(*) as sessions
        -- Account navigation
    ,sum(events.is_account_global) as account_global_sessions
    ,sum(events.is_account_acct) as account_acct_sessions
    ,sum(events.is_purchases_global) as purchases_global_sessions
    ,sum(events.is_purchases_acct) as purchases_acct_sessions
    ,sum(events.is_wishlist_global) as wishlist_global_sessions
    ,sum(events.is_wishlist_acct) as wishlist_acct_sessions
    ,sum(events.is_wishlist_landingpage) as wishlist_lp_sessions
    ,sum(events.is_rewards_global) as rewards_global_sessions
    ,sum(events.is_rewards_acct) as rewards_acct_sessions
    ,sum(events.is_rewards_landingpage) as rewards_lp_sessions
    ,sum(events.is_looks_global) as looks_global_sessions
    ,sum(events.is_looks_acct) as looks_acct_sessions
    ,sum(events.is_ppi_global) as ppi_global_sessions
    ,sum(events.is_ppi_acct) as ppi_acct_sessions
    ,sum(events.is_ppi_landingpage) as ppi_lp_sessions
    ,sum(events.is_addresses_global) as addresses_global_sessions
    ,sum(events.is_addresses_acct) as addresses_acct_sessions
    ,sum(events.is_address_submit_landingpage) as submit_address_lp_sessions
    ,sum(events.is_address_setasprimary_landingpage) as setprimary_address_lp_sessions
    ,sum(events.is_shippingaddress_landingpage) as shipping_address_lp_sessions
    ,sum(events.is_shippingaddress_add_landingpage) as add_shipping_address_lp_sessions
    ,sum(events.is_customerservice_landingpage) as customercare_lp_sessions
    ,sum(events.is_cs_call_chat_landingpage) as customercare_call_chat_lp_sessions
    ,sum(events.is_cs_priceadjustment_landingpage) as customercare_priceadjustment_lp_sessions
    ,sum(events.is_cs_start_return_landingpage) as customercare_start_return_lp_sessions
    ,sum(events.is_cs_track_order_landingpage) as customercare_track_order_lp_sessions
    ,sum(events.is_paymethods_global) as paymethods_global_sessions
    ,sum(events.is_paymethods_acct) as paymethods_acct_sessions
    ,sum(events.is_paymethods_landingpage) as paymethods_lp_sessions
    ,sum(events.is_paymethods_add_landingpage) as add_paymethods_lp_sessions
    ,sum(events.is_paymethods_submit_landingpage) as submit_paymethods_lp_sessions
    ,sum(events.is_paymethods_setprimary_landingpage) as setprimary_paymethods_lp_sessions
    ,sum(events.is_profile_landingpage) as profile_lp_sessions
    ,sum(events.is_settings_global) as settings_global_sessions
    ,sum(events.is_settings_acct) as settings_acct_sessions
    ,sum(events.is_settings_landingpage) as settings_lp_sessions
    ,sum(events.is_notifications_acct) as notifications_acct_sessions
    ,sum(events.is_setstore_global) as setstore_global_sessions
    ,sum(events.is_setstore_acct) as setstore_acct_sessions
    ,sum(events.is_setstore_landingpage) as setstore_lp_sessions
    ,sum(events.is_paycard_global) as paycard_global_sessions
    ,sum(events.is_paycard_acct) as paycard_acct_sessions
    ,sum(events.is_paycard_landingpage) as paycard_lp_sessions
    ,sum(events.is_signout_global) as signout_global_sessions
    ,sum(events.is_signout_acct) as signout_acct_sessions
    ,sum(events.is_signout_landingpage) as signout_lp_sessions
    ,sum(events.is_carecontact_global) as carecontact_global_sessions
    ,sum(events.is_carecontact_acct) as carecontact_acct_sessions
    ,sum(events.is_purchases_landingpage) as purchases_lp_sessions
        -- Purchase history
    ,sum(events.is_purchase_history_landingpage) as purchase_history_lp_sessions
    ,sum(events.is_purchases_return_intent_landingpage) as purchase_history_return_intent_lp_sessions
    ,sum(events.is_purchase_history_shipment_tracking_landingpage) as purchase_history_shipment_tracking_lp_sessions
    ,sum(events.is_carecontact_purchdetails) as carecontact_purchdetails_sessions
    ,sum(events.is_carecontact_shiptracking) as carecontact_shiptracking_sessions
    ,sum(events.is_carecontact_backorderconf) as carecontact_backorderconf_sessions
    ,sum(events.is_timeperiod_dropdown) as timeperiod_dropdown_sessions
    ,sum(events.is_buyagain) as buyagain_sessions
    ,sum(events.is_writereview) as writereview_sessions
    ,sum(events.is_seelooks) as seelooks_sessions
    ,sum(events.is_return_purchdetails_intent) as return_purchdetails_intent_sessions
    ,sum(events.is_return_purchdetails_submit) as return_purchdetails_submit_sessions
    ,sum(events.is_return_blank_intent) as return_blank_intent_sessions
    ,sum(events.is_return_blank_submit) as return_blank_submit_sessions
    ,sum(events.is_priceadj_blank_init) as priceadj_blank_init_sessions
    ,sum(events.is_priceadj_intent) as priceadj_intent_sessions
    ,sum(events.is_priceadj_submit) as priceadj_submit_sessions
    ,sum(events.is_cancel_intent) as cancel_intent_sessions
    ,sum(events.is_cancel_submit) as cancel_submit_sessions
    ,sum(events.is_cancel_intent_landingpage) as cancel_intent_lp_sessions
    ,sum(events.is_orderlookup) as orderlookup_sessions
    ,sum(events.is_auth_attempt) as auth_attempt_sessions
    ,sum(events.is_checkout) as checkout_sessions
    ,sum(events.is_add_to_bag) as addtobag_Sessions
    ,sum(events.is_order) as order_sessions
    ,sum(events.web_orders) as web_orders
    ,sum(events.web_demand_usd) as web_demand_usd
    ,CURRENT_TIMESTAMP(6) as dw_sys_load_tmstp
    from {sessions_t2_schema}.account_session_status as sessions
        inner join (select distinct * from {sessions_t2_schema}.account_session_events_ldg) as events
            on sessions.activity_date_pacific = events.activity_date
            	and sessions.session_id = events.session_id
                and sessions.channel = events.channel
                and sessions.channelcountry = events.channelcountry 
                and sessions.experience = events.experience
            	and events.activity_date between {start_date} and {end_date}
                and sessions.activity_date_pacific between {start_date} and {end_date}
                and events.channel in ('NORDSTROM', 'NORDSTROM_RACK')
                and events.experience in ('DESKTOP_WEB', 'MOBILE_WEB', 'IOS_APP', 'ANDROID_APP')

    group by 1,2,3,4,5,6,7
;

collect statistics column (activity_date_pacific)
                   ,column (channelcountry)
                   ,column (channel)
                   ,column (experience)
                   ,column (pagetype)
                   ,column (account_status)
on {sessions_t2_schema}.account_sessions_fact; 
;

-- drop staging table
drop table {sessions_t2_schema}.account_session_events_ldg;

SET QUERY_BAND = NONE FOR SESSION;