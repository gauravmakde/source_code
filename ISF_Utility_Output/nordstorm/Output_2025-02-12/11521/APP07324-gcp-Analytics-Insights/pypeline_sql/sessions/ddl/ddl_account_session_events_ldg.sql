SET QUERY_BAND = 'App_ID=APP08844;
     DAG_ID=account_sessions_fact_11521_ACE_ENG;
     Task_Name=ddl_account_session_events_ldg;'
     FOR SESSION VOLATILE;

/*
Account Experiences dashboard insertion file
This file inserts data into the production table {sessions_t2_schema}.account_exp_sessions,
which contains traffic, account navigation, purchase history, order management, and care contact engagement
broken out by date, country, channel, experience, and customer account status.
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{sessions_t2_schema}', 'account_session_events_ldg', OUT_RETURN_MSG);

create multiset table {sessions_t2_schema}.account_session_events_ldg
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    -- Dimensions
    channelcountry varchar(5)
    ,channel varchar(20)
    ,experience varchar(20) 
    -- Measures
    ,session_id varchar(100)
    ,pagetype varchar(50)
        -- Account navigation
    ,is_account_global bigint 
    ,is_account_acct bigint
    ,is_purchases_global bigint
    ,is_purchases_acct bigint
    ,is_wishlist_global bigint
    ,is_wishlist_landingpage bigint
    ,is_wishlist_acct bigint
    ,is_rewards_global bigint
    ,is_rewards_acct bigint
    ,is_rewards_landingpage bigint
    ,is_looks_global bigint
    ,is_looks_acct bigint
    ,is_ppi_global bigint
    ,is_ppi_acct bigint
    ,is_ppi_landingpage bigint
    ,is_addresses_global bigint
    ,is_addresses_acct bigint
    ,is_address_submit_landingpage bigint
    ,is_address_setasprimary_landingpage bigint
    ,is_shippingaddress_landingpage bigint
    ,is_shippingaddress_add_landingpage bigint
    ,is_customerservice_landingpage bigint
    ,is_cs_call_chat_landingpage bigint
    ,is_cs_priceadjustment_landingpage bigint
    ,is_cs_start_return_landingpage bigint
    ,is_cs_track_order_landingpage bigint
    ,is_paymethods_global bigint
    ,is_paymethods_acct bigint
    ,is_paymethods_landingpage bigint
    ,is_paymethods_add_landingpage bigint
    ,is_paymethods_submit_landingpage bigint
    ,is_paymethods_setprimary_landingpage bigint
    ,is_profile_landingpage bigint
    ,is_settings_global bigint
    ,is_settings_acct bigint
    ,is_settings_landingpage bigint
    ,is_notifications_acct bigint
    ,is_setstore_global bigint
    ,is_setstore_acct bigint
    ,is_setstore_landingpage bigint
    ,is_paycard_global bigint
    ,is_paycard_acct bigint
    ,is_paycard_landingpage bigint
    ,is_signout_global bigint
    ,is_signout_acct bigint
    ,is_signout_landingpage bigint
    ,is_carecontact_global bigint
    ,is_carecontact_acct bigint
    ,is_purchases_landingpage bigint
        -- Purchase history
    ,is_purchase_history_landingpage bigint
    ,is_purchases_return_intent_landingpage bigint
    ,is_purchase_history_shipment_tracking_landingpage bigint
    ,is_carecontact_purchdetails bigint
    ,is_carecontact_shiptracking bigint
    ,is_carecontact_backorderconf bigint
    ,is_timeperiod_dropdown bigint
    ,is_buyagain bigint
    ,is_writereview bigint
    ,is_seelooks bigint
    ,is_return_purchdetails_intent bigint
    ,is_return_purchdetails_submit bigint
    ,is_return_blank_intent bigint
    ,is_return_blank_submit bigint
    ,is_priceadj_blank_init bigint
    ,is_priceadj_intent bigint
    ,is_priceadj_submit bigint
    ,is_cancel_intent bigint
    ,is_cancel_submit bigint
    ,is_cancel_intent_landingpage bigint
    ,is_orderlookup bigint
    ,auth_flag bigint
    ,is_auth_attempt bigint
    ,is_checkout bigint
    ,is_add_to_bag bigint
    ,is_order bigint
    ,web_orders bigint
    ,web_demand_usd decimal(32,6)
    ,activity_date Date
)
primary index(session_id, activity_date)
partition by range_n(activity_date between '2022-01-01' and '2025-01-31' each interval '1' day, unknown)
;

-- Comments on columns
    -- Table
comment on {sessions_t2_schema}.account_session_events_ldg                                is 'A session-level table containing engagement by date,channel,platform and country.';
    -- Dimensions
comment on {sessions_t2_schema}.account_session_events_ldg.channelcountry                        is 'The country where the sessions occurred.';
comment on {sessions_t2_schema}.account_session_events_ldg.channel                               is 'The channel where the sessions occurred.';
comment on {sessions_t2_schema}.account_session_events_ldg.experience                            is 'The experience where the sessions occurred.';
    -- Measures
comment on {sessions_t2_schema}.account_session_events_ldg.session_id                              is 'The number of sessions.';
comment on {sessions_t2_schema}.account_session_events_ldg.pagetype                                is 'PageType - Global Nav, Left Nav , Account Landing Page.';
        -- Account navigation
comment on {sessions_t2_schema}.account_session_events_ldg.is_account_global                     is 'The number of sessions where the user clicked the global account nav account button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_account_acct                  is 'The number of sessions where the user clicked the account left nav account button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_purchases_global              is 'The number of sessions where the user clicked the global account nav purchases button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_purchases_acct                is 'The number of sessions where the user clicked the account left nav purchases button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_wishlist_global               is 'The number of sessions where the user clicked the global account nav wish list button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_wishlist_acct                 is 'The number of sessions where the user clicked the account left nav wish list button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_wishlist_landingpage          is 'The number of sessions where the user clicked the account landing page wish list button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_rewards_global                is 'The number of sessions where the user clicked the global account nav rewards button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_rewards_acct                  is 'The number of sessions where the user clicked the account left nav rewards button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_rewards_landingpage           is 'The number of sessions where the user clicked the account landing page rewards button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_looks_global                  is 'The number of sessions where the user clicked the global account nav looks button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_looks_acct                    is 'The number of sessions where the user clicked the account left nav looks button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_ppi_global                    is 'The number of sessions where the user clicked the global account nav password & personal info button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_ppi_acct                      is 'The number of sessions where the user clicked the account left nav password & personal info button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_ppi_landingpage               is 'The number of sessions where the user clicked the account landing page password & personal info button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_addresses_global              is 'The number of sessions where the user clicked the global account nav shipping addresses button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_addresses_acct                is 'The number of sessions where the user clicked the account left nav shipping addresses button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_address_submit_landingpage    is 'The number of sessions where the user clicked the account lading page address book save button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_address_setasprimary_landingpage           is 'The number of sessions where the user clicked the account lading page address book shipping addresses set primary address button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_shippingaddress_landingpage   is 'The number of sessions where the user clicked the account landing page shipping addresses button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_shippingaddress_add_landingpage            is 'The number of sessions where the user clicked the account landing page shipping addresses add address button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_customerservice_landingpage   is 'The number of sessions where the user clicked the account landing page customer service buttons.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_cs_call_chat_landingpage      is 'The number of sessions where the user clicked the account landing page customer service buttons - call/chat.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_cs_priceadjustment_landingpage   is 'The number of sessions where the user clicked the account landing page customer service buttons - price adjustment.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_cs_start_return_landingpage   is 'The number of sessions where the user clicked the account landing page customer service buttons - start return.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_cs_track_order_landingpage    is 'The number of sessions where the user clicked the account landing page customer service buttons - track order.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paymethods_global             is 'The number of sessions where the user clicked the global account nav payment methods button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paymethods_acct               is 'The number of sessions where the user clicked the account left nav payment methods button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paymethods_landingpage        is 'The number of sessions where the user clicked the account landing page payment methods button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paymethods_add_landingpage        is 'The number of sessions where the user clicked the account landing page payment methods button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paymethods_submit_landingpage        is 'The number of sessions where the user clicked the account landing page payment methods button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paymethods_setprimary_landingpage        is 'The number of sessions where the user clicked the account landing page payment methods button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_profile_landingpage           is 'The number of sessions where the user clicked the account landing page profile info button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_settings_global               is 'The number of sessions where the user clicked the global account nav email & mail preferences button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_settings_acct                 is 'The number of sessions where the user clicked the account left nav email & mail preferences button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_settings_landingpage          is 'The number of sessions where the user clicked the account landing page email & mail preferences button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_notifications_acct            is 'The number of sessions where the user clicked the account notifications button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_setstore_global               is 'The number of sessions where the user clicked the global account nav set store button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_setstore_acct                 is 'The number of sessions where the user clicked the account left nav set store button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_setstore_landingpage          is 'The number of sessions where the user clicked the account landing page set store button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paycard_global                is 'The number of sessions where the user clicked the global account nav pay & manage Nordstrom card button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paycard_acct                  is 'The number of sessions where the user clicked the account left nav pay & manage Nordstrom card button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_paycard_landingpage           is 'The number of sessions where the user clicked the account landing page pay & manage Nordstrom card button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_signout_global                is 'The number of sessions where the user clicked the global account nav sign out button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_signout_acct                  is 'The number of sessions where the user clicked the account left nav sign out button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_signout_landingpage           is 'The number of sessions where the user clicked the account landing page sign out button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_carecontact_global            is 'The number of sessions where the user clicked the global account nav care contact button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_carecontact_acct              is 'The number of sessions where the user clicked the account left nav care contact button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_purchases_landingpage         is 'The number of sessions where the user engaged with the purchase history flow.';
        -- Purchase history
comment on {sessions_t2_schema}.account_session_events_ldg.is_purchase_history_landingpage                           is 'The number of sessions where the user engaged with the purchase history landing page.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_purchases_return_intent_landingpage                    is 'The number of sessions where the user engaged with the purchase history landing page start return button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_purchase_history_shipment_tracking_landingpage         is 'The number of sessions where the user engaged with the purchase history landing page track order button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_carecontact_purchdetails      is 'The number of sessions where the user clicked the purchase details care contact button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_carecontact_shiptracking      is 'The number of sessions where the user clicked the shipment tracking care contact button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_carecontact_backorderconf     is 'The number of sessions where the user clicked the backorder confirmation care contact button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_timeperiod_dropdown           is 'The number of sessions where the user clicked the purchase history time period dropdown.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_buyagain                      is 'The number of sessions where the user clicked the buy it again button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_writereview                   is 'The number of sessions where the user clicked the write a review button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_seelooks                      is 'The number of sessions where the user clicked the see looks inspired by your purchase button.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_return_purchdetails_intent    is 'The number of sessions where the user engaged with the purchase details return flow.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_return_purchdetails_submit    is 'The number of sessions where the user submitted a purchase details return.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_return_blank_intent           is 'The number of sessions where the user engaged with the blank return flow.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_return_blank_submit           is 'The number of sessions where the user submitted a blank return.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_priceadj_blank_init           is 'The number of sessions where the user initiated a blank price adjustment.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_priceadj_intent               is 'The number of sessions where the user engaged with the price adjustment flow.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_priceadj_submit               is 'The number of sessions where the user submitted a price adjustment.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_cancel_intent                 is 'The number of sessions where the user engaged with the item cancellation flow.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_cancel_submit                 is 'The number of sessions where the user submitted an item cancellation.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_cancel_intent_landingpage     is 'The number of sessions where the user engaged with the item cancellation flow on landing page.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_orderlookup                   is 'The number of sessions where the user completed a single order lookup.';
comment on {sessions_t2_schema}.account_session_events_ldg.auth_flag                        is 'The number of sessions where the user attempted to authenticate.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_auth_attempt                  is 'The number of sessions where the user attempted to authenticate.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_add_to_bag                    is 'The number of sessions where the user added an item to their shopping bag.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_checkout                      is 'The number of sessions where the user entered the checkout process.';
comment on {sessions_t2_schema}.account_session_events_ldg.is_order                         is 'The number of sessions where the user placed an order.';
comment on {sessions_t2_schema}.account_session_events_ldg.web_orders                    is 'The number of orders placed during the sessions.';
comment on {sessions_t2_schema}.account_session_events_ldg.web_demand_usd                is 'The cost of orders placed during the sessions.';
comment on {sessions_t2_schema}.account_session_events_ldg.activity_date                          is 'The date when the sessions occurred, converted to Pacific time.';

SET QUERY_BAND = NONE FOR SESSION;