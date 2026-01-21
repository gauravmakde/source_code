--FILE CREATION DATE - 04/12/2023
--CREATED BY - Soren Stime
--DataSource - digital_sales_transactions, digital_returns_transactions, session_evt_expanded_attributes, acp_event_intermediate.funnel_events_order_cancelled
--Job Frequency - Daily insert (Append) 
--Last Modified by - Soren Stime
--Last Modified Date - 04/14/2023

create table if not exists {hive_schema}.session_sales_returns_transactions (
	channel string, 
	experience string,
	session_id string,
	pageinstance_id string,
	event_id string,
	ordernumber string,
	orderlinenumber  string,
    orderlineid  string,
	order_style_id  string,
	order_rmssku_id  string,
	order_line_regular_units decimal (10,2),
	order_line_current_units decimal (10,2),
	order_line_discount_units decimal (10,2),
	order_quantity integer,
	sale_order_line_regular_units decimal (10,2),
	sale_order_line_current_units decimal (10,2),
	sale_order_line_discount_units decimal (10,2),
	sale_quantity integer, 
	return_order_line_regular_units decimal (10,2), 
	return_order_line_current_units decimal (10,2), 
	return_order_line_discount_units decimal (10,2),
	return_quantity integer,
	cancel_order_line_regular_units decimal (10,2),
	cancel_order_line_current_units decimal (10,2),
	cancel_order_line_discount_units decimal (10,2),
	cancel_quantity integer,
    item_status string,
	ship_date date, 
	return_date date,
	days_to_return integer,
	days_from_ship_to_return integer,
	activity_date_partition date
)
using ORC
location "s3://{s3_bucket_root_var}/session_sales_returns_transactions"
partitioned by (activity_date_partition);

MSCK REPAIR table {hive_schema}.session_sales_returns_transactions;

create or replace temporary view sales_returns_temp as
select distinct
	ord.channel, 
	ord.experience,
	ord.session_id,
	ord.pageinstance_id,
	ord.event_id,
	ord.ordernumber,
	ord.orderlinenumber,
	ord.orderlineid,
	ord.order_style_id,
	ord.order_rmssku_id,
	ord.order_line_regular_units,
	ord.order_line_current_units,
	ord.order_line_discount_units,
	1 as order_quantity,
	case 
		when sale.sale_amount is not null then ord.order_line_regular_units 
		else null 
	end as sale_order_line_regular_units,
	case 
		when sale.sale_amount is not null then ord.order_line_current_units 
		else null 
	end as sale_order_line_current_units,
	case 
		when sale.sale_amount is not null then ord.order_line_discount_units 
		else null 
	end as sale_order_line_discount_units,
	case 
		when sale.sale_amount is not null then 1 
		else null 
	end as sale_quantity,
	case 
		when retn.return_amount is not null then ord.order_line_regular_units 
		else null 
	end as return_order_line_regular_units,
	case 
		when retn.return_amount is not null then ord.order_line_current_units 
		else null 
	end as return_order_line_current_units,
	case 
		when retn.return_amount is not null then ord.order_line_discount_units 
		else null 
	end as return_order_line_discount_units,
	case 
		when retn.return_amount is not null then 1 
		else null 
	end as return_quantity,
	case 
		when canc.cancel_reason is not null and sale.sale_amount is null then ord.order_line_regular_units 
		else null 
	end as cancel_order_line_regular_units,
	case 
		when canc.cancel_reason is not null and sale.sale_amount is null then ord.order_line_current_units 
		else null 
	end as cancel_order_line_current_units,
	case 
		when canc.cancel_reason is not null and sale.sale_amount is null then ord.order_line_discount_units 
		else null 
	end as cancel_order_line_discount_units,
	case 
		when canc.cancel_reason is not null and sale.sale_amount is null then 1 
		else null 
	end as cancel_quantity,
	case 
		when retn.return_amount is not null then 'RETURNED'
		when sale.sale_amount is not null then 'COMPLETED'
		when canc.cancel_reason is not null then canc.cancel_reason 
		else 'PREPARING_TO_SHIP'
	end as item_status,
	sale.ship_date,
	retn.return_date,
	datediff(return_date, activity_date_partition) as days_to_return,
	retn.days_from_ship_to_return,
	ord.activity_date_partition
from (	
	select distinct 
		channel,
		experience,
		activity_date_partition,
		session_id,
		event_id,
		pageinstance_id,
		ordernumber,
		order_style_id,
		order_rmssku_id,
		orderlineid,
		orderlinenumber,
		order_line_regular_units,
		order_line_current_units,
		order_line_discount_units 
	from acp_event_intermediate.session_evt_expanded_attributes_parquet
	where activity_date_partition between {start_date} - 89 and {end_date}
	and ordernumber is not null
	) ord
	left join 
	(
	select distinct 
	orderlineid,
	cancel_reason 
	from acp_event_intermediate.funnel_events_order_cancelled
	where event_date_pacific between {start_date} - 91 and {end_date} + 30
	) as canc 
	on ord.orderlineid = canc.orderlineid
	left join 
	(
	select distinct 
		ndirordernumber,
		transactionid, 
		transactionlineitemsequencenumber,
		transactionTypeCode, 
		sale_amount,
		sale_itemcount, 
		rmssku_id,
		ship_date
	from ace_etl.digital_sales_transactions 
	where ship_date between {start_date} - 91 and {end_date} + 30
	) sale
	on ord.ordernumber = sale.ndirordernumber
	and ord.order_rmssku_id = sale.rmssku_id
	left join 
	(
	select 
		ndirordernumber,
		originaltransactionidentifier,
		originaltransactionlineitemsequencenumber,
		transactiontypecode, 
		return_amount, 
		return_itemcount, 
		rmssku_id,
		return_date,
		original_ship_date,
		days_from_ship_to_return
	from ace_etl.digital_returns_transactions
	where original_ship_date between {start_date} - 91 and {end_date} +90
	and days_from_ship_to_return <= 90
	) retn
	on sale.transactionid = retn.originaltransactionidentifier
	and sale.transactionlineitemsequencenumber = retn.originaltransactionlineitemsequencenumber 
	;

create or replace temporary view clean_it as
select 
	channel,
	experience,	
	session_id,
	pageinstance_id,
	event_id,
	ordernumber,
	orderlinenumber,
	orderlineid,
	order_style_id,
	order_rmssku_id,
	max(order_line_regular_units) as order_line_regular_units,
	max(order_line_current_units) as order_line_current_units,
	max(order_line_discount_units) as order_line_discount_units,
	max(order_quantity) as order_quantity,
	max(sale_order_line_regular_units) as sale_order_line_regular_units,
	max(sale_order_line_current_units) as sale_order_line_current_units,
	max(sale_order_line_discount_units) as sale_order_line_discount_units,
	max(sale_quantity) as sale_quantity,
	max(return_order_line_regular_units) as return_order_line_regular_units,
	max(return_order_line_current_units) as return_order_line_current_units,
	max(return_order_line_discount_units) as return_order_line_discount_units,
	max(return_quantity) as return_quantity,
	max(cancel_order_line_regular_units) as cancel_order_line_regular_units,
	max(cancel_order_line_current_units) as cancel_order_line_current_units,
	max(cancel_order_line_discount_units) as cancel_order_line_discount_units,
	max(cancel_quantity) as cancel_quantity,
	max(item_status) as item_status,
	min(ship_date) as ship_date, 
	max(return_date) as return_date,
	datediff(max(return_date), activity_date_partition) as days_to_return,
	datediff(max(return_date), min(ship_date)) as days_from_ship_to_return,
	activity_date_partition
from sales_returns_temp
group by 1,2,3,4,5,6,7,8,9,10,32  
;

create or replace temporary view final_form as
select distinct
	channel, 
	experience,
	session_id,
	pageinstance_id,
	event_id,
	ordernumber,
	orderlinenumber,
	orderlineid,
	order_style_id,
	order_rmssku_id,
	order_line_regular_units,
	order_line_current_units,
	order_line_discount_units,
	order_quantity,
	case 
	    when datediff(ship_date, activity_date_partition) > 30 then null
	    else sale_order_line_regular_units
    end as sale_order_line_regular_units,
    case
        when datediff(ship_date, activity_date_partition) > 30 then null 
	    else sale_order_line_current_units
    end as sale_order_line_current_units, 
	case 
	    when datediff(ship_date, activity_date_partition) > 30 then null 
	    else sale_order_line_discount_units
    end as sale_order_line_discount_units,
	case 
	    when datediff(ship_date, activity_date_partition) > 30 then null 
	    else sale_quantity
	end as sale_quantity, 
	case 
	    when datediff(return_date, ship_date) > 90 then null
		when datediff(ship_date, activity_date_partition) > 30 then null 
	    else return_order_line_regular_units
    end as return_order_line_regular_units,
	case 
	    when datediff(return_date, ship_date) > 90 then null
		when datediff(ship_date, activity_date_partition) > 30 then null   
	    else return_order_line_current_units
	end as return_order_line_current_units,
	case
	    when datediff(return_date, ship_date) > 90 then null
		when datediff(ship_date, activity_date_partition) > 30 then null  
	    else return_order_line_discount_units
    end as return_order_line_discount_units,
	case 
	    when datediff(return_date, ship_date) > 90 then null
		when datediff(ship_date, activity_date_partition) > 30 then null   
	    else return_quantity
    end as return_quantity,
	case 
		when (item_status = 'PREPARING_TO_SHIP' and datediff({end_date}, activity_date_partition) > 30)
		or (item_status = 'COMPLETED' and datediff(ship_date, activity_date_partition) > 30) 
		then order_line_regular_units 
		else cancel_order_line_regular_units 
	end as cancel_order_line_regular_units,
	case 
		when (item_status = 'PREPARING_TO_SHIP' and datediff({end_date}, activity_date_partition) > 30)
		or (item_status = 'COMPLETED' and datediff(ship_date, activity_date_partition) > 30) 
		then order_line_current_units 
		else cancel_order_line_current_units 
	end as cancel_order_line_current_units,
	case 
		when (item_status = 'PREPARING_TO_SHIP' and datediff({end_date}, activity_date_partition) > 30)
		or (item_status = 'COMPLETED' and datediff(ship_date, activity_date_partition) > 30) 
		then order_line_discount_units 
		else cancel_order_line_discount_units 
	end as cancel_order_line_discount_units,
	case 
		when (item_status = 'PREPARING_TO_SHIP' and datediff({end_date}, activity_date_partition) > 30)
		or (item_status = 'COMPLETED' and datediff(ship_date, activity_date_partition) > 30) 
		then 1 
		else cancel_quantity
	end as cancel_quantity,
	case 
	    when item_status = 'PREPARING_TO_SHIP' and datediff({end_date}, activity_date_partition) > 30 then 'CANCEL_WITHOUT_REASON' 
	    when item_status = 'RETURNED' and datediff(return_date, ship_date) > 90 and datediff(ship_date, activity_date_partition) > 30 then 'CANCEL_WITHOUT_REASON' 
	    when item_status = 'RETURNED' and datediff(return_date, ship_date) > 90 then 'COMPLETED'
		when item_status = 'RETURNED' and datediff(ship_date, activity_date_partition) > 30 then 'CANCEL_WITHOUT_REASON'
	    when item_status = 'COMPLETED' and datediff(ship_date, activity_date_partition) > 30 then 'CANCEL_WITHOUT_REASON'
	    else item_status
    end as item_status,
	case 
		when datediff(ship_date, activity_date_partition) > 30 then null 
		else ship_date
	end as ship_date,  
	case 
		when datediff(return_date, ship_date) > 90 then null 
		when datediff(ship_date, activity_date_partition) > 30 then null 
		else return_date
	end as return_date,    
	case 
		when datediff(return_date, ship_date) > 90 then null 
		when datediff(ship_date, activity_date_partition) > 30 then null 
		else days_to_return
	end as days_to_return,
	case 
		when datediff(return_date, ship_date) > 90 then null 
		when datediff(ship_date, activity_date_partition) > 30 then null 
		else days_from_ship_to_return
	end as days_from_ship_to_return,
	activity_date_partition
from clean_it
;

insert
    OVERWRITE TABLE {hive_schema}.session_sales_returns_transactions  PARTITION (activity_date_partition)
select
    *
from final_form;
