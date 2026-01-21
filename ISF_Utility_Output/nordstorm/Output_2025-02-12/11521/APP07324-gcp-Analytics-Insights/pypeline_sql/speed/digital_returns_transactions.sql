create table if not exists {hive_schema}.digital_returns_transactions
(
ndirOrderNumber string,
originalTransactionIdentifier string,
ORIGINALTRANSACTIONLINEITEMSEQUENCENUMBER string,
transactionTypeCode string,
return_amount decimal (10,2),
return_itemcount integer,
rmssku_id string,
original_ship_date date,
days_from_ship_to_return integer,
return_date date
)
using ORC
location "s3://{s3_bucket_root_var}/digital_returns_transactions"
partitioned by (return_date);

MSCK REPAIR table {hive_schema}.digital_returns_transactions;

CREATE OR REPLACE TEMPORARY VIEW returns_data as
select distinct 
	miscItem.ndirOrderNumber,
	merch.originalTransactionIdentifier,
	MERCH.ORIGINALTRANSACTIONLINEITEMSEQUENCENUMBER,
	salescollectheader.transactionTypeCode,
	abs(CAST((MERCH.AMOUNT.AMOUNT) AS DECIMAL(10,2))) as return_amount, 
	MERCH.QUANTITY as return_itemcount, 
	merch.itemid as rmssku_id,
	date(MERCH.ORIGINALBUSINESSDATE) as original_ship_date,
	datediff(businessdate, date(MERCH.ORIGINALBUSINESSDATE)) as days_from_ship_to_return,
	businessdate as return_date
from object_model.enterprise_retail_transaction
LATERAL VIEW OUTER explode (merchandiseItems) as merch
WHERE salescollectheader.TRANSACTIONTYPECODE = 'RETN'
and businessdate between {start_date} and {end_date}
and merch.originalTransactionIdentifier in 
	(
	select distinct transactionId
	from {hive_schema}.digital_sales_transactions
	where ship_date between {start_date} - 89 and {end_date} +1
	)
;

insert
    OVERWRITE TABLE {hive_schema}.digital_returns_transactions  PARTITION (return_date)
select
    *
from returns_data;
