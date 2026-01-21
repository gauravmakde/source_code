SET QUERY_BAND = 'App_ID=APP08844;
     DAG_ID=account_sessions_fact_11521_ACE_ENG;
     Task_Name=account_session_events_ldg;'
     FOR SESSION VOLATILE;

create table if not exists {hive_schema}.account_session_events
(
    channelcountry string
    ,channel string
    ,experience string 
    -- Measures
    ,session_id string
    ,pagetype string
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
    ,activity_date date
 )
using ORC
location 's3://{s3_bucket_root_var}/account_session_events/'
partitioned by (activity_date);

create or replace temporary view account_session_events as
select events.channelcountry
        ,events.channel
        ,events.experience
        ,events.session_id
        ,case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Global/AccountNav/Dropdown'
        ,'Global/AccountNav/YourAccount') and events.element_type = 'HYPERLINK' then 'Global Nav'
        when events.event_name = 'com.nordstrom.event.customer.Engaged' 
        and events.element_id in ('Global/AccountNav/Purchases'
        ,'Global/AccountNav/WishList'
        ,'Global/AccountNav/Wishlist'
        ,'Global/AccountNav/NordyClub'
        ,'Global/AccountNav/Looks'
        ,'Global/AccountNav/PasswordPersonalInfo'
        ,'Global/AccountNav/ShippingAddresses'
        ,'Global/AccountNav/PaymentMethods'
        ,'Global/AccountNav/Settings'
        ,'Global/AccountNav/Communications'
        ,'Global/AccountNav/SetYourStore'
        ,'Global/AccountNav/PayManageNordstromCard'
        ,'Global/AccountNav/SignOut'
        ,'Global/AccountNav/HereToHelp'
        ,'Global/AccountNav/ContactUs') then 'Global Nav'
        when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/YourAccount'
        ,'Account/LeftNav/Purchases'
        ,'Account/Purchases'
        ,'Account/LeftNav/WishList'
        ,'Account/LeftNav/NordyClub'
        ,'Account/Rewards'
        ,'Account/LeftNav/Looks'
        ,'Account/LeftNav/PasswordPersonalInfo'
        ,'Account/LeftNav/ShippingAddresses'
        ,'Account/AddressBook'
        ,'Account/LeftNav/PaymentMethods'
        ,'Account/PaymentMethods'
        ,'Account/LeftNav/Communications'
        ,'Account/InfoAndSettings'
        ,'Account/LeftNav/SetStore'
        ,'Account/LeftNav/PayManageNordstromCard'
        ,'Account/PayBillOnline'
        ,'Account/LeftNav/SignOut'
        ,'Account/LeftNav/ContactUs'
        ,'Account/ContactCustomerCare') then 'Left Nav' 
        when events.context_pagetype = 'ACCOUNT_LANDING' and events.element_id in ('Account/AddressBook/AddNewAddress/Cancel'
        ,'Account/AddressBook/AddNewAddress/Save'
        ,'Account/AddressBook/AddNewAddress/SetAsPrimary'
        ,'Account/Communications'
        ,'Account/CustomerService'
        ,'Account/CustomerService/Call'
        ,'Account/CustomerService/Chat'
        ,'Account/CustomerService/FAQs'
        ,'Account/CustomerService/PriceAdjustment'
        ,'Account/CustomerService/StartReturn'
        ,'Account/CustomerService/TrackOrder'
        ,'Account/GiveUsFeedback'
        ,'Account/OrderDetails/Order/CancelItems'
        ,'Account/PasswordPersonalInfo'
        ,'Account/PayManageNordstromCard'
        ,'Account/PaymentMethods'
        ,'Account/PaymentMethods/AddNewCard/Cancel'
        ,'Account/PaymentMethods/AddNewCard/NewBillingAddress/Save'
        ,'Account/PaymentMethods/AddNewCard/Save'
        ,'Account/PaymentMethods/AddNewCard/SetAsPrimaryPayment'
        ,'Account/PaymentMethods/AddPaymentMethod'
        ,'Account/PaymentMethods/NewBillingAddress/SetAsPrimaryAddress'
        ,'Account/PaymentMethods/SetPrimaryPaymentMethod'
        ,'Account/Profile/Brands'
        ,'Account/Profile/Sizes'
        ,'Account/Profile/Style'
        ,'Account/Purchases'
        ,'Account/Purchases/OrderDetails'
        ,'Account/Purchases/OrderDetails/PriceAdjustment'
        ,'Account/Purchases/OrderDetails/StartReturn'
        ,'Account/Purchases/OrderItemImage'
        ,'Account/Purchases/OrderNumber'
        ,'Account/Purchases/ShipmentTracking'
        ,'Account/Purchases/StartShopping'
        ,'Account/Rewards'
        ,'Account/Rewards/AlterationsBenefit'
        ,'Account/Rewards/BonusPoints'
        ,'Account/Rewards/BonusPointsApply'
        ,'Account/Rewards/BookAlteration'
        ,'Account/Rewards/Connect'
        ,'Account/Rewards/Join'
        ,'Account/Rewards/Learn'
        ,'Account/Rewards/Notes'
        ,'Account/SetStore'
        ,'Account/SetStore/SetYourStore'
        ,'Account/SetStore/ViewDetails'
        ,'Account/ShippingAddresses'
        ,'Account/ShippingAddresses/AddAddress'
        ,'Account/ShippingAddresses/SetPrimaryAddress'
        ,'Account/SignOut'
        ,'Account/Wishlist'
        ,'Account/Wishlist/CreateWishlist'
        ,'Account/Wishlist/List') then 'Account Landing Page' else 'Account' end as pagetype
            -- Account
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Global/AccountNav/Dropdown', 'Global/AccountNav/YourAccount') and events.element_type = 'HYPERLINK' then 1 else 0 end) as is_account_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/LeftNav/YourAccount' then 1 else 0 end) as is_account_acct
                --or (events.element_id = 'Global/TabBar' and events.element_value = 'Account')
            -- Purchases
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/Purchases' then 1 else 0 end) as is_purchases_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/Purchases', 'Account/Purchases') then 1 else 0 end) as is_purchases_acct
            -- Wish list
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and lower(events.element_id) = lower('Global/AccountNav/WishList') then 1 else 0 end) as is_wishlist_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/LeftNav/WishList' then 1 else 0 end) as is_wishlist_acct
                --or (events.element_id = 'Global/TabBar' and events.element_value = 'Wish List')
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Wishlist','Account/Wishlist/CreateWishlist','Account/Wishlist/List') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_wishlist_landingpage
        -- Nordy Club rewards
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/NordyClub' then 1 else 0 end) as is_rewards_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/NordyClub', 'Account/Rewards') then 1 else 0 end) as is_rewards_acct
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Rewards','Account/Rewards/AlterationsBenefit','Account/Rewards/BonusPoints','Account/Rewards/BonusPointsApply','Account/Rewards/BookAlteration','Account/Rewards/Connect','Account/Rewards/Join','Account/Rewards/Learn','Account/Rewards/Notes') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_rewards_landingpage       
        -- Looks
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/Looks' then 1 else 0 end) as is_looks_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/LeftNav/Looks' then 1 else 0 end) as is_looks_acct
                --or (events.element_id = 'Global/TabBar' and events.element_value = 'Looks')
            -- Password & personal info (web only)
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/PasswordPersonalInfo' then 1 else 0 end) as is_ppi_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/LeftNav/PasswordPersonalInfo' then 1 else 0 end) as is_ppi_acct
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/PasswordPersonalInfo' and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_ppi_landingpage
        -- Shipping addresses
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/ShippingAddresses' then 1 else 0 end) as is_addresses_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/ShippingAddresses', 'Account/AddressBook') then 1 else 0 end) as is_addresses_acct
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/AddressBook/AddNewAddress/Save'
        ,'Account/AddressBook/AddNewAddress/SetAsPrimary') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_address_submit_landingpage
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/AddressBook/AddNewAddress/SetAsPrimary','Account/ShippingAddresses/SetPrimaryAddress','Account/PaymentMethods/NewBillingAddress/SetAsPrimaryAddress') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_address_setasprimary_landingpage
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/ShippingAddresses') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_shippingaddress_landingpage
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/ShippingAddresses/AddAddress') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_shippingaddress_add_landingpage
        -- Customer Service
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/CustomerService','Account/CustomerService/FAQs') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_customerservice_landingpage
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/CustomerService/Call'
        ,'Account/CustomerService/Chat') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_cs_call_chat_landingpage
         ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/CustomerService/PriceAdjustment') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_cs_priceadjustment_landingpage
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/CustomerService/StartReturn') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_cs_start_return_landingpage
         ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/CustomerService/TrackOrder') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_cs_track_order_landingpage
        -- Payment methods
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/PaymentMethods' then 1 else 0 end) as is_paymethods_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/PaymentMethods', 'Account/PaymentMethods') then 1 else 0 end) as is_paymethods_acct
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/PaymentMethods') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_paymethods_landingpage
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/PaymentMethods/AddPaymentMethod') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_paymethods_add_landingpage  
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/PaymentMethods/AddNewCard/Save') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_paymethods_submit_landingpage
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/PaymentMethods/AddNewCard/SetAsPrimaryPayment'
        ,'Account/PaymentMethods/SetPrimaryPaymentMethod') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_paymethods_setprimary_landingpage  
        -- Profile
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Profile/Brands','Account/Profile/Sizes','Account/Profile/Style') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_profile_landingpage
        -- Settings (Email & mail preferences)
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Global/AccountNav/Settings', 'Global/AccountNav/Communications') then 1 else 0 end) as is_settings_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/Communications', 'Account/InfoAndSettings') then 1 else 0 end) as is_settings_acct
         ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Communications') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_settings_landingpage       
        -- Notification settings (app only)
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/NotificationSettings' then 1 else 0 end) as is_notifications_acct
            -- Set your store (web only)
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/SetYourStore' then 1 else 0 end) as is_setstore_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/LeftNav/SetStore' then 1 else 0 end) as is_setstore_acct
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/SetStore','Account/SetStore/SetYourStore','Account/SetStore/ViewDetails') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_setstore_landingpage        
        -- Pay & manage Nordstrom card
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/PayManageNordstromCard' then 1 else 0 end) as is_paycard_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/PayManageNordstromCard', 'Account/PayBillOnline') then 1 else 0 end) as is_paycard_acct
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/PayManageNordstromCard') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_paycard_landingpage
        -- Sign out
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Global/AccountNav/SignOut' then 1 else 0 end) as is_signout_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/SignOut') then 1 else 0 end) as is_signout_acct
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/SignOut') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_signout_landingpage
            -- Care contact
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Global/AccountNav/HereToHelp', 'Global/AccountNav/ContactUs') then 1 else 0 end) as is_carecontact_global
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/LeftNav/ContactUs', 'Account/ContactCustomerCare') then 1 else 0 end) as is_carecontact_acct
        -- Purchase history
            -- Purchase Sessions
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Purchases') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_purchases_landingpage
         ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Purchases/OrderDetails','Account/Purchases/OrderDetails/PriceAdjustment','Account/Purchases/OrderItemImage','Account/Purchases/OrderNumber','Account/Purchases/StartShopping') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_purchase_history_landingpage
 		,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Purchases/OrderDetails/StartReturn') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_purchases_return_intent_landingpage
 		 ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Purchases/ShipmentTracking') and events.context_pagetype = 'ACCOUNT_LANDING'  then 1 else 0 end) as is_purchase_history_shipment_tracking_landingpage
            -- Care contact
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Purchases/OrderDetails/ChatWithUs', 'Account/Purchases/OrderDetails/CarePhoneNumber') then 1 else 0 end) as is_carecontact_purchdetails
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('ShipmentTracking/ChatWithUs', 'ShipmentTracking/CarePhoneNumber') then 1 else 0 end) as is_carecontact_shiptracking
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/BackOrderItem/Error/ChatWithUs', 'Account/OrderLineCancelConfirmation/CarePhoneNumber') then 1 else 0 end) as is_carecontact_backorderconf
            -- Time period dropdown
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/Purchases/TimePeriod' then 1 else 0 end) as is_timeperiod_dropdown
            -- Buy it again
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/Purchases/OrderDetails/BuyItAgain' then 1 else 0 end) as is_buyagain
            -- Write a review
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Purchases/WriteAReview', 'Account/Purchases/OrderDetails/WriteAReview') then 1 else 0 end) as is_writereview
            -- Looks inspired by purchase
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/Purchases/OrderDetails/SeeLooks' then 1 else 0 end) as is_seelooks
            -- Return
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/Purchases/StartAReturn', 'Account/Purchases/OrderDetails/StartAReturn','Account/Purchases/OrderDetails/StartReturn', 'Account/Purchases/OrderDetails/ReturnThisItem') then 1 else 0 end) as is_return_purchdetails_intent
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged'  and events.element_id = 'Account/Returns/SubmitReturn' and not events.context_pagetype = 'ACCOUNT_BLANK_RETURN' then 1 else 0 end) as is_return_purchdetails_submit
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'BlankReturn/StartReturn' then 1 else 0 end) as is_return_blank_intent
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/Returns/SubmitReturn' and events.context_pagetype = 'ACCOUNT_BLANK_RETURN' then 1 else 0 end) as is_return_blank_submit
            -- Price adjustment
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/PriceAdjustment/FindOrder' then 1 else 0 end) as is_priceadj_blank_init
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/Purchases/OrderDetails/PriceAdjustment' then 1 else 0 end) as is_priceadj_intent
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/Purchases/OrderDetails/PriceAdjustment/SubmitRequest' then 1 else 0 end) as is_priceadj_submit
            -- Cancellation
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/OrderDetails/Order/CancelItems', 'Account/OrderDetails/Item/CancelItem') and events.context_pagetype <> 'ACCOUNT_LANDING' then 1 else 0 end) as is_cancel_intent
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'Account/OrderLineCancel/SubmitCancel' then 1 else 0 end) as is_cancel_submit
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Account/OrderDetails/Order/CancelItems') and events.context_pagetype = 'ACCOUNT_LANDING' then 1 else 0 end) as is_cancel_intent_landingpage 
        -- Order lookup
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id = 'SingleOrderLookup/OrderStatus' then 1 else 0 end) as is_orderlookup
        ,max(case when events.event_name = 'com.nordstrom.customer.Authenticated' then 1 else 0 end) as auth_flag
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Access/SignIn', 'Access/CreateAccount') then 1 else 0 end) as is_auth_attempt
        ,max(case when events.event_name = 'com.nordstrom.event.customer.Engaged' and events.element_id in ('Global/MiniBag/Checkout'
        , 'MiniPDP/AddToBagCheckoutLink'
        , 'PostAddToBagModal/CheckoutLink'
        , 'ProductDetail/AddToBagCheckoutLink'
        , 'ShoppingBag/Checkout'
        , 'ShoppingBag/GuestCheckoutButton'
        , 'ShoppingBag/SignInAndCheckoutButton'
        , 'ShoppingBag/ShoppingBag/SignIn'
        , 'ShoppingBag/SavedForLater/SignIn'
        , 'ShoppingBag/PayPal', 'Checkout/SignIn/PayPal'
        , 'Checkout/SignIn/GuestCheckout') then 1
        when events.event_name = 'com.nordstrom.event.customer.Engaged' and context_pagetype in ('CHECKOUT', 'REVIEW_ORDER') and
        events.element_id not in ('Access/SignIn', 'Access/CreateAccount')
         then 1 
        when events.event_name = 'com.nordstrom.event.customer.Impressed'
        and context_pagetype in ('CHECKOUT', 'REVIEW_ORDER') then 1 else 0 end) as is_checkout
        ,max(case when events.event_name = 'com.nordstrom.customer.AddedToBag' then 1 else 0 end) as is_add_to_bag
        ,max(case when orders.web_orders > 0 and orders.session_id <> 'UNKNOWN' then 1 else 0 end) as is_order
        ,max(orders.web_orders) as web_orders
        ,max(orders.web_demand_usd) as web_demand_usd
        ,events.activity_date
    from acp_vector.CUSTOMER_SESSION_EVT_FACT as events
    left join acp_vector.CUSTOMER_SESSION_FACT as orders
            on orders.activity_date = events.activity_date
                and orders.session_id = events.session_id
                where events.activity_date between {start_date} and {end_date}
                and events.channel in ('NORDSTROM', 'NORDSTROM_RACK')
                and events.experience in ('DESKTOP_WEB', 'MOBILE_WEB', 'IOS_APP', 'ANDROID_APP')
                group by 1,2,3,4,5,83;

insert overwrite table {hive_schema}.account_session_events partition (activity_date)
select /*+ REPARTITION(10) */ * from account_session_events
;

-- sync partitions 
MSCK REPAIR TABLE {hive_schema}.account_session_events; 

-- Writing output to teradata landing table.  
-- This sould match the "sql_table_reference" indicated on the .json file.
insert into table account_session_events_ldg_output 
select * from {hive_schema}.account_session_events WHERE activity_date BETWEEN {start_date} and {end_date}
;
