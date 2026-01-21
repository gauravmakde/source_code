SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=sales_person_video_customer_journey_orderline_11521_ACE_ENG;
     Task_Name=customer_journey_orderline;'
     FOR SESSION VOLATILE;

/*

Table: spv.customer_journey_orderline
Owner: Elianna Wang
Edit Date: 2022-11-14

SQL moves data from the landing table to the final T2 table for the lookback period.
The landing table is dropped when all steps are complete.

*/

delete 
from    T2DL_DAS_SPV.customer_journey_orderline
where   click_spv_date between '2024-08-13' and '2024-08-14'
;

insert into T2DL_DAS_SPV.customer_journey_orderline
select  
    shopper_id,
    web_style_num,
    click_spv_country,
    click_spv_channel,
    click_spv_platform,
    click_spv_year,
    click_spv_month,
    click_spv_day,
    click_spv_date,
    click_spv_count,
    --click_spv_time_start,
    --click_spv_time_end,
    cast(left(to_char(click_spv_time_start),19) as timestamp(0)) as click_spv_time_start,
    cast(left(to_char(click_spv_time_end),19) as timestamp(0)) as click_spv_time_end,
    add_or_remove_bag_country,
    add_or_remove_bag_channel,
    add_or_remove_bag_platform,
    add_or_remove_bag_year,
    add_or_remove_bag_month,
    add_or_remove_bag_day,
    add_or_remove,
    --add_or_remove_time,
    cast(left(to_char(add_or_remove_time),19) as timestamp(0)) as add_or_remove_time,
    ordernumber,
    checkout_country,
    checkout_channel,
    checkout_platform,
    checkout_year,
    checkout_month,
    checkout_day,
    --checkout_time,
    cast(left(to_char(checkout_time),19) as timestamp(0)) as checkout_time,
    item_price,
    orderlineid,
    order_amount,
	CURRENT_TIMESTAMP as dw_sys_load_tmstp
from T2DL_DAS_SPV.customer_journey_orderline_landing
where click_spv_date between '2024-08-13' and '2024-08-14'
;

collect statistics column (shopper_id)
		, column (web_style_num)
		, column (click_spv_country)
		, column (click_spv_channel)
		, column (click_spv_platform)
		, column (click_spv_date)
		, column (ordernumber)
		, column (orderlineid)
on T2DL_DAS_SPV.customer_journey_orderline
;


SET QUERY_BAND = NONE FOR SESSION;

