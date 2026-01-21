SET QUERY_BAND = 'App_ID=APP08844;
     DAG_ID=ddl_account_sessions_fact_11521_ACE_ENG;
     Task_Name=ddl_account_sessions_fact;'
     FOR SESSION VOLATILE;

/*
Account Experiences dashboard insertion file
This file inserts data into the production table {sessions_t2_schema}.account_sessions_fact,
which contains traffic, account navigation, purchase history, order management, and care contact engagement
broken out by date, country, channel, experience, and customer account status.
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{sessions_t2_schema}', 'account_sessions_fact', OUT_RETURN_MSG);

create multiset table {sessions_t2_schema}.account_sessions_fact
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio   
    (
    -- Dimensions
    activity_date_pacific Date
    ,channelcountry varchar(5)
    ,channel varchar(20)
    ,experience varchar(20) 
    ,account_status varchar(20)
    ,auth_status varchar(20)
    ,pagetype varchar(50)
    -- Measures
    ,sessions bigint
        -- Account navigation
    ,account_global_sessions bigint
    ,account_acct_sessions bigint
    ,purchases_global_sessions bigint
    ,purchases_acct_sessions bigint
    ,wishlist_global_sessions bigint
    ,wishlist_acct_sessions bigint
    ,wishlist_lp_sessions bigint
    ,rewards_global_sessions bigint
    ,rewards_acct_sessions bigint
    ,rewards_lp_sessions bigint
    ,looks_global_sessions bigint
    ,looks_acct_sessions bigint
    ,ppi_global_sessions bigint
    ,ppi_acct_sessions bigint
    ,ppi_lp_sessions bigint
    ,addresses_global_sessions bigint
    ,addresses_acct_sessions bigint
    ,submit_address_lp_sessions bigint
    ,setprimary_address_lp_sessions bigint
    ,shipping_address_lp_sessions bigint
    ,add_shipping_address_lp_sessions bigint
    ,customercare_lp_sessions bigint
    ,customercare_call_chat_lp_sessions bigint
    ,customercare_priceadjustment_lp_sessions bigint
    ,customercare_start_return_lp_sessions bigint
    ,customercare_track_order_lp_sessions bigint
    ,paymethods_global_sessions bigint
    ,paymethods_acct_sessions bigint
    ,paymethods_lp_sessions bigint
    ,add_paymethods_lp_sessions bigint
    ,submit_paymethods_lp_sessions bigint
    ,setprimary_paymethods_lp_sessions bigint
    ,profile_lp_sessions bigint
    ,settings_global_sessions bigint
    ,settings_acct_sessions bigint
    ,settings_lp_sessions bigint
    ,notifications_acct_sessions bigint
    ,setstore_global_sessions bigint
    ,setstore_acct_sessions bigint
    ,setstore_lp_sessions bigint
    ,paycard_global_sessions bigint
    ,paycard_acct_sessions bigint
    ,paycard_lp_sessions bigint
    ,signout_global_sessions bigint
    ,signout_acct_sessions bigint
    ,signout_lp_sessions bigint
    ,carecontact_global_sessions bigint
    ,carecontact_acct_sessions bigint
    ,purchases_lp_sessions bigint
        -- Purchase history
    ,purchase_history_lp_sessions bigint
    ,purchase_history_return_intent_lp_sessions bigint
    ,purchase_history_shipment_tracking_lp_sessions bigint
    ,carecontact_purchdetails_sessions bigint
    ,carecontact_shiptracking_sessions bigint
    ,carecontact_backorderconf_sessions bigint
    ,timeperiod_dropdown_sessions bigint
    ,buyagain_sessions bigint
    ,writereview_sessions bigint
    ,seelooks_sessions bigint
    ,return_purchdetails_intent_sessions bigint
    ,return_purchdetails_submit_sessions bigint
    ,return_blank_intent_sessions bigint
    ,return_blank_submit_sessions bigint
    ,priceadj_blank_init_sessions bigint
    ,priceadj_intent_sessions bigint
    ,priceadj_submit_sessions bigint
    ,cancel_intent_sessions bigint
    ,cancel_submit_sessions bigint
    ,cancel_intent_lp_sessions bigint
    ,orderlookup_sessions bigint
    ,auth_attempt_sessions bigint
    ,checkout_sessions bigint
    ,addtobag_Sessions bigint
    ,order_sessions bigint
    ,web_orders bigint
    ,web_demand_usd decimal(32,6)  
    ,dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL 
)
primary index(activity_date_pacific, channelcountry, channel, experience, pagetype, account_status)
partition by range_n(activity_date_pacific between '2022-01-01' and '2025-01-31' each interval '1' day, unknown)
;

-- Comments on columns
    -- Table
comment on {sessions_t2_schema}.account_sessions_fact                                is 'A session-level table containing engagement by date,channel,platform and country.';
    -- Dimensions
comment on {sessions_t2_schema}.account_sessions_fact.activity_date_pacific                 is 'The date when the sessions occurred, converted to Pacific time.';
comment on {sessions_t2_schema}.account_sessions_fact.channelcountry                        is 'The country where the sessions occurred.';
comment on {sessions_t2_schema}.account_sessions_fact.channel                               is 'The channel where the sessions occurred.';
comment on {sessions_t2_schema}.account_sessions_fact.experience                            is 'The experience where the sessions occurred.';
comment on {sessions_t2_schema}.account_sessions_fact.account_status                        is 'The customer account status ("Guest", "Account holders", "Loyalty members", or "Cardholders").';
comment on {sessions_t2_schema}.account_sessions_fact.auth_status                           is 'The customer authentication status ("Guest", "Recognized", or "Authenticated").';
comment on {sessions_t2_schema}.account_sessions_fact.pagetype                              is 'The page type ("Global Nav", "Left Nav", or "Landing Page").';
    -- Measures
comment on {sessions_t2_schema}.account_sessions_fact.sessions                              is 'The number of sessions.';
        -- Account navigation
comment on {sessions_t2_schema}.account_sessions_fact.account_global_sessions               is 'The number of sessions where the user clicked the global account nav account button.';
comment on {sessions_t2_schema}.account_sessions_fact.account_acct_sessions                 is 'The number of sessions where the user clicked the account left nav account button.';
comment on {sessions_t2_schema}.account_sessions_fact.purchases_global_sessions             is 'The number of sessions where the user clicked the global account nav purchases button.';
comment on {sessions_t2_schema}.account_sessions_fact.purchases_acct_sessions               is 'The number of sessions where the user clicked the account left nav purchases button.';
comment on {sessions_t2_schema}.account_sessions_fact.wishlist_global_sessions              is 'The number of sessions where the user clicked the global account nav wish list button.';
comment on {sessions_t2_schema}.account_sessions_fact.wishlist_acct_sessions                is 'The number of sessions where the user clicked the account left nav wish list button.';
comment on {sessions_t2_schema}.account_sessions_fact.wishlist_lp_sessions                  is 'The number of sessions where the user clicked the account landing page wish list button.';
comment on {sessions_t2_schema}.account_sessions_fact.rewards_global_sessions               is 'The number of sessions where the user clicked the global account nav rewards button.';
comment on {sessions_t2_schema}.account_sessions_fact.rewards_acct_sessions                 is 'The number of sessions where the user clicked the account left nav rewards button.';
comment on {sessions_t2_schema}.account_sessions_fact.rewards_lp_sessions                   is 'The number of sessions where the user clicked the account landing page rewards button.';
comment on {sessions_t2_schema}.account_sessions_fact.looks_global_sessions                 is 'The number of sessions where the user clicked the global account nav looks button.';
comment on {sessions_t2_schema}.account_sessions_fact.looks_acct_sessions                   is 'The number of sessions where the user clicked the account left nav looks button.';
comment on {sessions_t2_schema}.account_sessions_fact.ppi_global_sessions                   is 'The number of sessions where the user clicked the global account nav password & personal info button.';
comment on {sessions_t2_schema}.account_sessions_fact.ppi_acct_sessions                     is 'The number of sessions where the user clicked the account left nav password & personal info button.';
comment on {sessions_t2_schema}.account_sessions_fact.ppi_lp_sessions                       is 'The number of sessions where the user clicked the account landing page password & personal info button.';
comment on {sessions_t2_schema}.account_sessions_fact.addresses_global_sessions             is 'The number of sessions where the user clicked the global account nav shipping addresses button.';
comment on {sessions_t2_schema}.account_sessions_fact.addresses_acct_sessions               is 'The number of sessions where the user clicked the account left nav shipping addresses button.';
comment on {sessions_t2_schema}.account_sessions_fact.submit_address_lp_sessions                   is 'The number of sessions where the user clicked the account landing page shipping addresses button.';
comment on {sessions_t2_schema}.account_sessions_fact.setprimary_address_lp_sessions                   is 'The number of sessions where the user clicked the account landing page shipping addresses button.';
comment on {sessions_t2_schema}.account_sessions_fact.shipping_address_lp_sessions          is 'The number of sessions where the user clicked the account landing page address book shipping addresses button.';
comment on {sessions_t2_schema}.account_sessions_fact.add_shipping_address_lp_sessions          is 'The number of sessions where the user clicked the account landing page address book shipping addresses button.';
comment on {sessions_t2_schema}.account_sessions_fact.customercare_lp_sessions              is 'The number of sessions where the user clicked the account landing page customer service flow.';
comment on {sessions_t2_schema}.account_sessions_fact.customercare_call_chat_lp_sessions              is 'The number of sessions where the user clicked the account landing page customer service flow.';
comment on {sessions_t2_schema}.account_sessions_fact.customercare_priceadjustment_lp_sessions              is 'The number of sessions where the user clicked the account landing page customer service flow.';
comment on {sessions_t2_schema}.account_sessions_fact.customercare_start_return_lp_sessions              is 'The number of sessions where the user clicked the account landing page customer service flow.';
comment on {sessions_t2_schema}.account_sessions_fact.customercare_track_order_lp_sessions              is 'The number of sessions where the user clicked the account landing page customer service flow.';
comment on {sessions_t2_schema}.account_sessions_fact.paymethods_global_sessions            is 'The number of sessions where the user clicked the global account nav payment methods button.';
comment on {sessions_t2_schema}.account_sessions_fact.paymethods_acct_sessions              is 'The number of sessions where the user clicked the account left nav payment methods button.';
comment on {sessions_t2_schema}.account_sessions_fact.paymethods_lp_sessions                is 'The number of sessions where the user clicked the account landing page payment methods button.';
comment on {sessions_t2_schema}.account_sessions_fact.add_paymethods_lp_sessions                is 'The number of sessions where the user clicked the account landing page payment methods button.';
comment on {sessions_t2_schema}.account_sessions_fact.submit_paymethods_lp_sessions                is 'The number of sessions where the user clicked the account landing page payment methods button.';
comment on {sessions_t2_schema}.account_sessions_fact.setprimary_paymethods_lp_sessions                is 'The number of sessions where the user clicked the account landing page payment methods button.';
comment on {sessions_t2_schema}.account_sessions_fact.profile_lp_sessions                   is 'The number of sessions where the user clicked the account landing page profile info button.';
comment on {sessions_t2_schema}.account_sessions_fact.settings_global_sessions              is 'The number of sessions where the user clicked the global account nav email & mail preferences button.';
comment on {sessions_t2_schema}.account_sessions_fact.settings_acct_sessions                is 'The number of sessions where the user clicked the account left nav email & mail preferences button.';
comment on {sessions_t2_schema}.account_sessions_fact.settings_lp_sessions                  is 'The number of sessions where the user clicked the account landing page email & mail preferences button.';
comment on {sessions_t2_schema}.account_sessions_fact.notifications_acct_sessions           is 'The number of sessions where the user clicked the account notifications button.';
comment on {sessions_t2_schema}.account_sessions_fact.setstore_global_sessions              is 'The number of sessions where the user clicked the global account nav set store button.';
comment on {sessions_t2_schema}.account_sessions_fact.setstore_acct_sessions                is 'The number of sessions where the user clicked the account left nav set store button.';
comment on {sessions_t2_schema}.account_sessions_fact.setstore_lp_sessions                  is 'The number of sessions where the user clicked the account landing page set store button.';
comment on {sessions_t2_schema}.account_sessions_fact.paycard_global_sessions               is 'The number of sessions where the user clicked the global account nav pay & manage Nordstrom card button.';
comment on {sessions_t2_schema}.account_sessions_fact.paycard_acct_sessions                 is 'The number of sessions where the user clicked the account left nav pay & manage Nordstrom card button.';
comment on {sessions_t2_schema}.account_sessions_fact.paycard_lp_sessions                   is 'The number of sessions where the user clicked the account landing page pay & manage Nordstrom card button.';
comment on {sessions_t2_schema}.account_sessions_fact.signout_global_sessions               is 'The number of sessions where the user clicked the global account nav sign out button.';
comment on {sessions_t2_schema}.account_sessions_fact.signout_acct_sessions                 is 'The number of sessions where the user clicked the account left nav sign out button.';
comment on {sessions_t2_schema}.account_sessions_fact.signout_lp_sessions                   is 'The number of sessions where the user clicked the account left nav sign out button.';
comment on {sessions_t2_schema}.account_sessions_fact.carecontact_global_sessions           is 'The number of sessions where the user clicked the global account nav care contact button.';
comment on {sessions_t2_schema}.account_sessions_fact.carecontact_acct_sessions             is 'The number of sessions where the user clicked the account left nav care contact button.';
comment on {sessions_t2_schema}.account_sessions_fact.purchases_lp_sessions                 is 'The number of sessions where the user clicked the purchase details care contact button.';
        -- Purchase history
comment on {sessions_t2_schema}.account_sessions_fact.purchase_history_lp_sessions     is 'The number of sessions where the user clicked the purchase details care contact button.';
comment on {sessions_t2_schema}.account_sessions_fact.purchase_history_return_intent_lp_sessions     is 'The number of sessions where the user clicked the purchase details care contact button.';
comment on {sessions_t2_schema}.account_sessions_fact.purchase_history_shipment_tracking_lp_sessions     is 'The number of sessions where the user clicked the purchase details care contact button.';
comment on {sessions_t2_schema}.account_sessions_fact.carecontact_purchdetails_sessions     is 'The number of sessions where the user clicked the purchase details care contact button.';
comment on {sessions_t2_schema}.account_sessions_fact.carecontact_shiptracking_sessions     is 'The number of sessions where the user clicked the shipment tracking care contact button.';
comment on {sessions_t2_schema}.account_sessions_fact.carecontact_backorderconf_sessions    is 'The number of sessions where the user clicked the backorder confirmation care contact button.';
comment on {sessions_t2_schema}.account_sessions_fact.timeperiod_dropdown_sessions          is 'The number of sessions where the user clicked the purchase history time period dropdown.';
comment on {sessions_t2_schema}.account_sessions_fact.buyagain_sessions                     is 'The number of sessions where the user clicked the buy it again button.';
comment on {sessions_t2_schema}.account_sessions_fact.writereview_sessions                  is 'The number of sessions where the user clicked the write a review button.';
comment on {sessions_t2_schema}.account_sessions_fact.seelooks_sessions                     is 'The number of sessions where the user clicked the see looks inspired by your purchase button.';
comment on {sessions_t2_schema}.account_sessions_fact.return_purchdetails_intent_sessions   is 'The number of sessions where the user engaged with the purchase details return flow.';
comment on {sessions_t2_schema}.account_sessions_fact.return_purchdetails_submit_sessions   is 'The number of sessions where the user submitted a purchase details return.';
comment on {sessions_t2_schema}.account_sessions_fact.return_blank_intent_sessions          is 'The number of sessions where the user engaged with the blank return flow.';
comment on {sessions_t2_schema}.account_sessions_fact.return_blank_submit_sessions          is 'The number of sessions where the user submitted a blank return.';
comment on {sessions_t2_schema}.account_sessions_fact.priceadj_blank_init_sessions          is 'The number of sessions where the user initiated a blank price adjustment.';
comment on {sessions_t2_schema}.account_sessions_fact.priceadj_intent_sessions              is 'The number of sessions where the user engaged with the price adjustment flow.';
comment on {sessions_t2_schema}.account_sessions_fact.priceadj_submit_sessions              is 'The number of sessions where the user submitted a price adjustment.';
comment on {sessions_t2_schema}.account_sessions_fact.cancel_intent_sessions                is 'The number of sessions where the user engaged with the item cancellation flow.';
comment on {sessions_t2_schema}.account_sessions_fact.cancel_submit_sessions                is 'The number of sessions where the user submitted an item cancellation.';
comment on {sessions_t2_schema}.account_sessions_fact.cancel_intent_lp_sessions             is 'The number of sessions where the user engaged with the item cancellation flow on landing page.';
comment on {sessions_t2_schema}.account_sessions_fact.orderlookup_sessions                  is 'The number of sessions where the user completed a single order lookup.';
comment on {sessions_t2_schema}.account_sessions_fact.auth_attempt_sessions         is 'The number of sessions where the user attempted to authenticate.';
comment on {sessions_t2_schema}.account_sessions_fact.checkout_sessions             is 'The number of sessions where the user entered the checkout process.';
comment on {sessions_t2_schema}.account_sessions_fact.addtobag_sessions             is 'The number of sessions where the user added an item to their shopping bag.';
comment on {sessions_t2_schema}.account_sessions_fact.order_sessions                is 'The number of sessions where the user placed an order.';
comment on {sessions_t2_schema}.account_sessions_fact.web_orders                    is 'The number of orders placed during the sessions.';
comment on {sessions_t2_schema}.account_sessions_fact.web_demand_usd                is 'The cost of orders placed during the sessions.';
COMMENT ON {sessions_t2_schema}.account_sessions_fact.dw_sys_load_tmstp                     is 'datalab load timestamp';

SET QUERY_BAND = NONE FOR SESSION;