
-- Stakeholders: Elianna Wang, Niharika Srivastava
-- Edit Date: 2024-07-29

-- History of PDP with SPV
create or replace temp view product_with_spv as (
    select
		digitalcontent.id as web_style_num,
		year as view_page_year,
		month as view_page_month,
		day as view_page_day
	from acp_event_view.customer_activity_impressed
	lateral view explode(elements) as element
	lateral view explode(context.digitalcontents) as digitalcontent
	where element.id = 'ProductDetail/SalesPersonVideo'
	and digitalcontent.idtype = 'STYLE'
	and digitalcontent.id is not null
	and digitalcontent.id <> ''
	and to_date(concat(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')),'yyyy-MM-dd') between '2024-08-13' and '2024-08-14'
    group by 1, 2, 3, 4
);

-- Customers click SPV records on daily level
create or replace temp view spv_click as (
    select
        shopper_id,
        web_style_num,
        click_spv_country,
        click_spv_channel,
        click_spv_platform,
        click_spv_year,
        click_spv_month,
        click_spv_day,
        to_date(concat(a.click_spv_year, '-', LPAD(a.click_spv_month, 2, '0'), '-', LPAD(a.click_spv_day, 2, '0')),'yyyy-MM-dd') as click_spv_date,
        click_spv_count,
        min_click_spv_time,
        max_click_spv_time,
        min_click_spv_time as click_spv_time_start,
        date_add(max_click_spv_time, 2) as click_spv_time_end
    from (
        select
			customer.id as shopper_id,
			digitalcontent.id as web_style_num,
			source.channelcountry as click_spv_country,
			source.channel as click_spv_channel,
			source.platform as click_spv_platform,
			year as click_spv_year,
			month as click_spv_month,
			day as click_spv_day,
			count(distinct from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) as click_spv_count,
			min(from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) as min_click_spv_time,
			max(from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) as max_click_spv_time
		from acp_event_view.customer_activity_engaged
		lateral view explode(context.digitalcontents) as digitalcontent
		where element.id = 'ProductDetail/SalesPersonVideo'
		and element.value = 'PLAY'
		and digitalcontent.idtype = 'STYLE'
		and customer.idtype = 'SHOPPER_ID'
		and digitalcontent.id <> ''
    	and to_date(concat(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')),'yyyy-MM-dd') between '2024-08-13' and '2024-08-14'
        and arrays_overlap(map_keys(headers), array('Nord-Load','nord-load','nord-test','Nord-Test', 'Sretest', 'sretest')) = False
        and (
            array_contains(map_keys(headers), 'identified-bot') = False
            or
            (array_contains(map_keys(headers), 'identified-bot') = True  and decode(headers['identified-bot'], 'utf-8') = False)
            )
        group by 1, 2, 3, 4, 5, 6, 7, 8
        ) as a
    where trim(shopper_id) <> ''
    and shopper_id is not null
);

-- Add to bag action during the days
create or replace temp view spv_add_to_bag as (
	select
		b.shopper_id,
		b.web_style_num,
		b.add_to_bag_country,
		b.add_to_bag_channel,
		b.add_to_bag_platform,
		b.add_to_bag_year,
		b.add_to_bag_month,
		b.add_to_bag_day,
		b.add_to_bag_time,
		a.add_to_bag_count
	from
		(
        select
			customer.id as shopper_id,
			productstyle.id as web_style_num,
			source.channelcountry as add_to_bag_country,
			source.channel as add_to_bag_channel,
			source.platform as add_to_bag_platform,
			year as add_to_bag_year,
			month as add_to_bag_month,
			day as add_to_bag_day,
			count(distinct from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) as add_to_bag_count
		from acp_event_view.customer_activity_added_to_bag caatb
		right join product_with_spv hpws
		on caatb.productstyle.id = hpws.web_style_num
		and caatb.year = hpws.view_page_year
		and caatb.month = hpws.view_page_month
		and caatb.day = hpws.view_page_day
		where caatb.customer.idtype = 'SHOPPER_ID'
		and caatb.productstyle.idtype = 'WEB'
		and customer.id is not null
		and trim(customer.id) <> ''
        and to_date(concat(caatb.year, '-', LPAD(caatb.month, 2, '0'), '-', LPAD(caatb.day, 2, '0')),'yyyy-MM-dd') between '2024-08-13' and '2024-08-14'
        and arrays_overlap(map_keys(headers), array('Nord-Load','nord-load','nord-test','Nord-Test', 'Sretest', 'sretest')) = False
        and (
            array_contains(map_keys(headers), 'identified-bot') = False
            or
            (array_contains(map_keys(headers), 'identified-bot') = True  and decode(headers['identified-bot'], 'utf-8') = False)
            )
		group by 1, 2, 3, 4, 5, 6, 7, 8
		having count(distinct from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) <= 30
		) as a
	left join
		(select
			customer.id as shopper_id,
			productstyle.id as web_style_num,
			source.channelcountry as add_to_bag_country,
			source.channel as add_to_bag_channel,
			source.platform as add_to_bag_platform,
			year as add_to_bag_year,
			month as add_to_bag_month,
			day as add_to_bag_day,
			from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific') as add_to_bag_time
		from acp_event_view.customer_activity_added_to_bag caatb
		right join product_with_spv hpws
		on caatb.productstyle.id = hpws.web_style_num
		and caatb.year = hpws.view_page_year
		and caatb.month = hpws.view_page_month
		and caatb.day = hpws.view_page_day
		where caatb.customer.idtype = 'SHOPPER_ID'
		and productstyle.idtype = 'WEB'
		and customer.id is not null
		and trim(customer.id) <> ''
        and to_date(concat(caatb.year, '-', LPAD(caatb.month, 2, '0'), '-', LPAD(caatb.day, 2, '0')),'yyyy-MM-dd') between '2024-08-13' and '2024-08-14'
        and arrays_overlap(map_keys(headers), array('Nord-Load','nord-load','nord-test','Nord-Test', 'Sretest', 'sretest')) = False
        and (
            array_contains(map_keys(headers), 'identified-bot') = False
            or
            (array_contains(map_keys(headers), 'identified-bot') = True  and decode(headers['identified-bot'], 'utf-8') = False)
            )
        ) as b
	on a.shopper_id = b.shopper_id
	and a.web_style_num = b.web_style_num
	and a.add_to_bag_country = b.add_to_bag_country
	and a.add_to_bag_channel = b.add_to_bag_channel
	and a.add_to_bag_platform = b.add_to_bag_platform
	and a.add_to_bag_year = b.add_to_bag_year
	and a.add_to_bag_month = b.add_to_bag_month
	and a.add_to_bag_day = b.add_to_bag_day
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
);

-- Remove from bag action during the days
create or replace temp view spv_remove_from_bag as (
	select
		b.shopper_id,
		b.web_style_num,
		b.remove_from_bag_country,
		b.remove_from_bag_channel,
		b.remove_from_bag_platform,
		b.remove_from_bag_year,
		b.remove_from_bag_month,
		b.remove_from_bag_day,
		b.remove_from_bag_time,
		a.remove_from_bag_count
	from (
        select
			customer.id as shopper_id,
			productstyle.id as web_style_num,
			source.channelcountry as remove_from_bag_country,
			source.channel as remove_from_bag_channel,
			source.platform as remove_from_bag_platform,
			year as remove_from_bag_year,
			month as remove_from_bag_month,
			day as remove_from_bag_day,
			count(distinct from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) as remove_from_bag_count
		from acp_event_view.customer_activity_removed_from_bag carfb
		right join product_with_spv hpws
		on carfb.productstyle.id = hpws.web_style_num
		and carfb.year = hpws.view_page_year
		and carfb.month = hpws.view_page_month
		and carfb.day = hpws.view_page_day
		where carfb.customer.idtype = 'SHOPPER_ID'
		and carfb.productstyle.idtype = 'WEB'
		and customer.id is not null
		and trim(customer.id) <> ''
        and to_date(concat(carfb.year, '-', LPAD(carfb.month, 2, '0'), '-', LPAD(carfb.day, 2, '0')),'yyyy-MM-dd') between '2024-08-13' and '2024-08-14'
        and arrays_overlap(map_keys(headers), array('Nord-Load','nord-load','nord-test','Nord-Test', 'Sretest', 'sretest')) = False
        and (
            array_contains(map_keys(headers), 'identified-bot') = False
            or
            (array_contains(map_keys(headers), 'identified-bot') = True  and decode(headers['identified-bot'], 'utf-8') = False)
            )
		group by 1, 2, 3, 4, 5, 6, 7, 8
		having count(distinct from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific')) <= 30
		) as a
		left join
		(select
			customer.id as shopper_id,
			productstyle.id as web_style_num,
			source.channelcountry as remove_from_bag_country,
			source.channel as remove_from_bag_channel,
			source.platform as remove_from_bag_platform,
			year as remove_from_bag_year,
			month as remove_from_bag_month,
			day as remove_from_bag_day,
			from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific') as remove_from_bag_time
		from acp_event_view.customer_activity_removed_from_bag carfb
		right join product_with_spv hpws
		on carfb.productstyle.id = hpws.web_style_num
		and carfb.year = hpws.view_page_year
		and carfb.month = hpws.view_page_month
		and carfb.day = hpws.view_page_day
		where carfb.customer.idtype = 'SHOPPER_ID'
		and productstyle.idtype = 'WEB'
		and customer.id is not null
		and trim(customer.id) <> ''
	    and to_date(concat(carfb.year, '-', LPAD(carfb.month, 2, '0'), '-', LPAD(carfb.day, 2, '0')),'yyyy-MM-dd') between '2024-08-13' and '2024-08-14'
        and arrays_overlap(map_keys(headers), array('Nord-Load','nord-load','nord-test','Nord-Test', 'Sretest', 'sretest')) = False
        and (
            array_contains(map_keys(headers), 'identified-bot') = False
            or
            (array_contains(map_keys(headers), 'identified-bot') = True  and decode(headers['identified-bot'], 'utf-8') = False)
            )
        ) as b
	on a.shopper_id = b.shopper_id
	and a.web_style_num = b.web_style_num
	and a.remove_from_bag_country = b.remove_from_bag_country
	and a.remove_from_bag_channel = b.remove_from_bag_channel
	and a.remove_from_bag_platform = b.remove_from_bag_platform
	and a.remove_from_bag_year = b.remove_from_bag_year
	and a.remove_from_bag_month = b.remove_from_bag_month
	and a.remove_from_bag_day = b.remove_from_bag_day
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
);

-- Join Click SPV and Add to Bag
create or replace temp view spv_add_or_remove_from_bag_temp1 as (
    select
		shopper_id,
		web_style_num,
		click_spv_country,
		click_spv_channel,
		click_spv_platform,
		click_spv_time_start,
		click_spv_time_end,
		case when add_or_remove_time is null then null
		else add_or_remove end as add_or_remove,
		add_or_remove_time
	from
	    (
        select
			shopper_id,
			web_style_num,
			click_spv_country,
			click_spv_channel,
			click_spv_platform,
			click_spv_time_start,
			click_spv_time_end,
			add_or_remove,
			add_or_remove_time,
			rank() over (partition by shopper_id, web_style_num, click_spv_time_start order by add_or_remove_time) as ranking
		from
            (
            select
                shopper_id,
                web_style_num,
                click_spv_country,
                click_spv_channel,
                click_spv_platform,
                click_spv_time_start,
                click_spv_time_end,
                add_or_remove_time,
                cast('Add' as varchar(15)) as add_or_remove
            from
                (
                select
					hsc.shopper_id,
					hsc.web_style_num,
					hsc.click_spv_country,
					hsc.click_spv_channel,
					hsc.click_spv_platform,
					hsc.click_spv_time_start,
					hsc.click_spv_time_end,
					hsatb.add_to_bag_time as add_or_remove_time,
					rank() over (partition by hsc.shopper_id, hsc.web_style_num, hsc.click_spv_year, hsc.click_spv_month, hsc.click_spv_day order by add_to_bag_time) as ranking
                from spv_click hsc
                left join spv_add_to_bag hsatb
                on hsc.shopper_id = hsatb.shopper_id
                and hsc.web_style_num = hsatb.web_style_num
                and hsatb.add_to_bag_time between hsc.click_spv_time_start and hsc.click_spv_time_end
                ) as a
            where ranking = 1
            union all
            select
                shopper_id,
                web_style_num,
                click_spv_country,
                click_spv_channel,
                click_spv_platform,
                click_spv_time_start,
                click_spv_time_end,
                add_or_remove_time,
                cast('Remove' as varchar(15)) as add_or_remove
            from
                (
                select
					hsc.shopper_id,
					hsc.web_style_num,
					hsc.click_spv_country,
					hsc.click_spv_channel,
					hsc.click_spv_platform,
					hsc.click_spv_time_start,
					hsc.click_spv_time_end,
					hsrfb.remove_from_bag_time as add_or_remove_time,
					rank() over (partition by hsc.shopper_id, hsc.web_style_num, hsc.click_spv_year, hsc.click_spv_month, hsc.click_spv_day order by remove_from_bag_time) as ranking
    			from spv_click hsc
    			left join spv_remove_from_bag hsrfb
    			on hsc.shopper_id = hsrfb.shopper_id
    			and hsc.web_style_num = hsrfb.web_style_num
    			and hsrfb.remove_from_bag_time between hsc.click_spv_time_start and hsc.click_spv_time_end
    			) as b
    		where ranking = 1
    		 ) as c
        ) as d
    where ranking = 1
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
);

-- Only add or remove bag would be kept for records
create or replace temp view spv_add_or_remove_from_bag_temp2 as (
    select
		shopper_id,
		web_style_num,
		click_spv_country,
		click_spv_channel,
		click_spv_platform,
		click_spv_time_start,
		click_spv_time_end,
		add_or_remove_time,
		add_or_remove
	from
		(select
			shopper_id,
			web_style_num,
			click_spv_country,
			click_spv_channel,
			click_spv_platform,
			click_spv_time_start,
			click_spv_time_end,
			add_or_remove_time,
			add_or_remove,
			rank() over (partition by shopper_id, web_style_num, add_or_remove_time order by click_spv_time_start desc) as ranking
		from spv_add_or_remove_from_bag_temp1
		) as a
	where ranking = 1
);

-- Join processed add or remove from bag event with spv click move
create or replace temp view spv_add_or_remove_from_bag_temp3 as (
    select
		hsc.shopper_id,
		hsc.web_style_num,
		hsc.click_spv_country,
		hsc.click_spv_channel,
		hsc.click_spv_platform,
		click_spv_year,
		click_spv_month,
		click_spv_day,
		click_spv_date,
		click_spv_count,
		min_click_spv_time,
		max_click_spv_time,
		hsc.click_spv_time_start,
		hsc.click_spv_time_end,
		add_or_remove,
		add_or_remove_time
	from spv_click hsc
	left join spv_add_or_remove_from_bag_temp2 hsarfbt2
	on hsc.shopper_id = hsarfbt2.shopper_id
	and hsc.web_style_num = hsarfbt2.web_style_num
	and hsc.click_spv_country = hsarfbt2.click_spv_country
	and hsc.click_spv_channel = hsarfbt2.click_spv_channel
	and hsc.click_spv_platform = hsarfbt2.click_spv_platform
	and hsc.click_spv_time_start = hsarfbt2.click_spv_time_start
);


-- All add to bag remove from bag details are added by add or remove action
create or replace temp view spv_add_or_remove_from_bag as (
	select
		add_temp.shopper_id,
		add_temp.web_style_num,
		add_temp.click_spv_country,
		add_temp.click_spv_channel,
		add_temp.click_spv_platform,
		add_temp.click_spv_year,
		add_temp.click_spv_month,
		add_temp.click_spv_day,
		add_temp.click_spv_date,
		add_temp.click_spv_count,
		add_temp.click_spv_time_start,
		add_temp.click_spv_time_end,
		hsatb.add_to_bag_country as add_or_remove_bag_country,
		hsatb.add_to_bag_channel as add_or_remove_bag_channel,
		hsatb.add_to_bag_platform as add_or_remove_bag_platform,
		hsatb.add_to_bag_year as add_or_remove_bag_year,
		hsatb.add_to_bag_month as add_or_remove_bag_month,
		hsatb.add_to_bag_day as add_or_remove_bag_day,
		add_temp.add_or_remove,
		add_temp.add_or_remove_time
	from
		(
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
			min_click_spv_time,
			max_click_spv_time,
			click_spv_time_start,
			click_spv_time_end,
			add_or_remove,
			add_or_remove_time
		from spv_add_or_remove_from_bag_temp3 hsarfbt3
		where add_or_remove = 'Add'
		) as add_temp
	left join spv_add_to_bag hsatb
	on add_temp.shopper_id = hsatb.shopper_id
	and add_temp.web_style_num = hsatb.web_style_num
	and add_temp.add_or_remove_time = hsatb.add_to_bag_time
	union all
	select
		remove_temp.shopper_id,
		remove_temp.web_style_num,
		remove_temp.click_spv_country,
		remove_temp.click_spv_channel,
		remove_temp.click_spv_platform,
		remove_temp.click_spv_year,
		remove_temp.click_spv_month,
		remove_temp.click_spv_day,
		remove_temp.click_spv_date,
		remove_temp.click_spv_count,
		remove_temp.click_spv_time_start,
		remove_temp.click_spv_time_end,
		hsrfb.remove_from_bag_country as add_or_remove_bag_country,
		hsrfb.remove_from_bag_channel as add_or_remove_bag_channel,
		hsrfb.remove_from_bag_platform as add_or_remove_bag_platform,
		hsrfb.remove_from_bag_year as add_or_remove_bag_year,
		hsrfb.remove_from_bag_month as add_or_remove_bag_month,
		hsrfb.remove_from_bag_day as add_or_remove_bag_day,
		remove_temp.add_or_remove,
		remove_temp.add_or_remove_time
	from
		(
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
			min_click_spv_time,
			max_click_spv_time,
			click_spv_time_start,
			click_spv_time_end,
			add_or_remove,
			add_or_remove_time
		from spv_add_or_remove_from_bag_temp3 hsarfbt3
		where add_or_remove = 'Remove'
		) as remove_temp
	left join spv_remove_from_bag hsrfb
	on remove_temp.shopper_id = hsrfb.shopper_id
	and remove_temp.web_style_num = hsrfb.web_style_num
	and remove_temp.add_or_remove_time = hsrfb.remove_from_bag_time
	union all
	select
		null_temp.shopper_id,
		null_temp.web_style_num,
		null_temp.click_spv_country,
		null_temp.click_spv_channel,
		null_temp.click_spv_platform,
		null_temp.click_spv_year,
		null_temp.click_spv_month,
		null_temp.click_spv_day,
		null_temp.click_spv_date,
		null_temp.click_spv_count,
		null_temp.click_spv_time_start,
		null_temp.click_spv_time_end,
		null as add_or_remove_bag_country,
		null as add_or_remove_bag_channel,
		null as add_or_remove_bag_platform,
		null as add_or_remove_bag_year,
		null as add_or_remove_bag_month,
		null as add_or_remove_bag_day,
		null_temp.add_or_remove,
		null_temp.add_or_remove_time
	from
		(
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
			min_click_spv_time,
			max_click_spv_time,
			click_spv_time_start,
			click_spv_time_end,
			add_or_remove,
			add_or_remove_time
		from spv_add_or_remove_from_bag_temp3 hsarfbt3
		where add_or_remove is null) as null_temp
);


-- Customers Checkout History
create or replace temp view spv_order_submitted as (
    select
		shopper_id,
		caos.web_style_num,
		ordernumber,
		checkout_country,
		checkout_channel,
		checkout_platform,
		checkout_year,
		checkout_month,
		checkout_day,
		checkout_time,
		item_price,
		orderlineid,
		order_amount
	from
		(
		 select
			credentials.shopperid as shopper_id,
			item.productstyle.id as web_style_num,
			invoice.ordernumber as ordernumber,
			source.channelcountry as checkout_country,
			source.channel as checkout_channel,
			source.platform as checkout_platform,
			year as checkout_year,
			month as checkout_month,
			day as checkout_day,
			from_utc_timestamp(from_unixtime(cast(decode(headers['SystemTime'], 'utf-8') as bigint) * 0.001), 'US/Pacific') as checkout_time,
			cast(item.price.current.units + item.price.current.nanos * power(10,-9) as decimal(8, 2)) as item_price,
            item.orderlineid,
			cast(invoice.subtotal.units + invoice.subtotal.nanos * power(10, -9) as decimal(8, 2)) as order_amount
		from acp_event_view.customer_activity_order_submitted
		lateral view explode(items) as item
		lateral view explode(item.promotions) as promotion
		where credentials.shopperid is not null
		and trim(credentials.shopperid) <> ''
        and item.productstyle.idtype  = 'WEB'
        and promotion.employee is null
		and to_date(concat(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')),'yyyy-MM-dd') between '2024-08-13' and '2024-08-14'
        and arrays_overlap(map_keys(headers), array('Nord-Load','nord-load','nord-test','Nord-Test', 'Sretest', 'sretest')) = False
        and
            (
            array_contains(map_keys(headers), 'identified-bot') = False
            or
            (array_contains(map_keys(headers), 'identified-bot') = True  and decode(headers['identified-bot'], 'utf-8') = False)
            )
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
        ) as caos
	right join product_with_spv hpws
	on caos.web_style_num = hpws.web_style_num
	and caos.checkout_year = hpws.view_page_year
	and caos.checkout_month = hpws.view_page_month
	and caos.checkout_day = hpws.view_page_day
	where shopper_id is not null
);


-- Joining upstream journey and checkout to fetch existing orders (not null) and the latest click event time
create or replace temp view customer_journey_temp1 as (
    select
        shopper_id,
        web_style_num,
        click_spv_time_start,
        ordernumber,
        orderlineid
    from (
        select
            shopper_id,
            web_style_num,
            click_spv_time_start,
            ordernumber,
            orderlineid,
            rank() over (partition by shopper_id, web_style_num, ordernumber order by click_spv_time_start desc) as ranking
        from
            (
            select
				hsarfb.shopper_id,
				hsarfb.web_style_num,
				click_spv_time_start,
				hsos.ordernumber,
				hsos.orderlineid
            from spv_add_or_remove_from_bag hsarfb
            left join spv_order_submitted hsos
            on hsarfb.shopper_id = hsos.shopper_id
            and hsarfb.web_style_num = hsos.web_style_num
            and hsos.checkout_time between coalesce(hsarfb.add_or_remove_time, hsarfb.click_spv_time_start) and date_add(click_spv_time_end, 5)
            ) as a
        where ordernumber is not null
        ) as b
    where ranking = 1
    group by 1, 2, 3, 4, 5
);


-- Join intermediate key info with all other details
create or replace temp view customer_journey_orderline as (
     select
		hsarfb.shopper_id,
		hsarfb.web_style_num,
		click_spv_country,
		click_spv_channel,
		click_spv_platform,
		click_spv_year,
		click_spv_month,
		click_spv_day,
		click_spv_count,
		hsarfb.click_spv_time_start,
		click_spv_time_end,
		add_or_remove_bag_country,
		add_or_remove_bag_channel,
		add_or_remove_bag_platform,
		add_or_remove_bag_year,
		add_or_remove_bag_month,
		add_or_remove_bag_day,
		add_or_remove,
		add_or_remove_time,
		hsos.ordernumber,
		hsos.checkout_country,
		hsos.checkout_channel,
		hsos.checkout_platform,
		hsos.checkout_year,
		hsos.checkout_month,
		hsos.checkout_day,
		hsos.checkout_time,
		hsos.item_price,
		hsos.orderlineid,
		hsos.order_amount,
		click_spv_date
    from spv_add_or_remove_from_bag hsarfb
    left join customer_journey_temp1 hscjt1
    on hsarfb.shopper_id  = hscjt1.shopper_id
    and hsarfb.web_style_num = hscjt1.web_style_num
    and hsarfb.click_spv_time_start = hscjt1.click_spv_time_start
    left join spv_order_submitted hsos
    on hsos.ordernumber = hscjt1.ordernumber
    and hsos.orderlineid = hscjt1.orderlineid
);


-- Writing output for to CSV
insert overwrite table customer_journey_orderline_landing_table
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
	cast(click_spv_time_start as timestamp) as click_spv_time_start,
	cast(click_spv_time_end as timestamp) as click_spv_time_end,
	add_or_remove_bag_country,
	add_or_remove_bag_channel,
	add_or_remove_bag_platform,
	add_or_remove_bag_year,
	add_or_remove_bag_month,
	add_or_remove_bag_day,
	add_or_remove,
	cast(add_or_remove_time as timestamp) as add_or_remove_time,
	ordernumber,
	checkout_country,
	checkout_channel,
	checkout_platform,
	checkout_year,
	checkout_month,
	checkout_day,
	cast(checkout_time as timestamp) as checkout_time,
	item_price,
	orderlineid,
	order_amount
from customer_journey_orderline
where click_spv_date between '2024-08-13' and '2024-08-14'
;
