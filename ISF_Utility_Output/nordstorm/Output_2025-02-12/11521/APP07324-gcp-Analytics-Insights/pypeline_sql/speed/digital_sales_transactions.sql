create table if not exists {hive_schema}.digital_sales_transactions
(
ndirOrderNumber string,
transactionId string,
TRANSACTIONLINEITEMSEQUENCENUMBER string,
transactionTypeCode string,
sale_amount decimal (10,2),
sale_itemcount integer,
rmssku_id string,
ship_date date
)
using ORC
location "s3://{s3_bucket_root_var}/digital_sales_transactions"
partitioned by (ship_date);

MSCK REPAIR table {hive_schema}.digital_sales_transactions;

CREATE OR REPLACE TEMPORARY VIEW sales_data as
select distinct 
	miscItem.ndirOrderNumber,
	transactionIdentifier.transactionId as transactionId, 
	MERCH.TRANSACTIONLINEITEMSEQUENCENUMBER, 
	salescollectheader.transactionTypeCode, 
	abs(CAST((MERCH.AMOUNT.AMOUNT) AS DECIMAL(10,2))) as sale_amount,
	MERCH.QUANTITY as sale_itemcount, 
	merch.itemid as rmssku_id,
	businessdate as ship_date
from object_model.enterprise_retail_transaction
LATERAL VIEW OUTER explode (merchandiseItems) as merch
WHERE SALESCOLLECTHEADER.TRANSACTIONTYPECODE = 'SALE'
and businessdate between {start_date} and {end_date}
and miscItem.ndirOrderNumber in 
	(
	select distinct value.invoice.ordernumber 
	from acp_event.customer_activity_order_submitted_parquet
	where value.source.channelcountry = 'US'
    and
        to_date(
            concat(
                year,
                '-',
                LPAD(month, 2, '0'),
                '-',
                LPAD(day, 2, '0')
            ),
            'yyyy-MM-dd'
        ) between {start_date} - 30
            and {end_date} + 1
	and value.source.platform in ('IOS', 'MOW', 'WEB', 'ANDROID')
        and cast(headers['Nord-Load'] as string) is null
        and cast(headers['nord-load'] as string) is null
        and cast(headers['Nord-Test'] as string) is null
        and cast(headers['nord-test'] as string) is null
        and cast(headers['Sretest'] as string) is null
        and coalesce(cast(headers['identified-bot'] as string),'XXX') <> 'True'
	)
;

insert
    OVERWRITE TABLE {hive_schema}.digital_sales_transactions  PARTITION (ship_date)
select
    *
from sales_data;
