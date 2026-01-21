
 SET QUERY_BAND = 'App_ID=APP08118; 
     DAG_ID=sales_and_returns_fact_base_11521_ACE_ENG;
     Task_Name=sales_and_returns_fact_base;'
     FOR SESSION VOLATILE; 
 
/* 
T2/Table Name: SALES_AND_RETURNS_FACT_BASE
Team/Owner: Lance Christenson 
Date Modified: 11/30/2022  

Note:
-- What is the the purpose of the table: see below copied from NAPBI 
-- What is the update cadence/lookback window: daily refreshment, run at 7am PST 
*/

  /*
original version Jan 2022
Lance Christenson

refresh sales and returns table
with returns matched to sales

also include services data such as fee_codes
and non-merch divisions (rpos, boutiques)

Feb 2022 - modify for NRHL orders having sku
but no upc

May 2022 modify to include trans only thru yesterday
Jun 2022 modify to include original_business_date for returns
Oct 2022 modify to include new HR worker tables
Nov 2022 modify to filter bad order data
Nov 2022 fix hr v1 logic to check inactive ind
Dec 2022 modify to run in two modes based on day of
week (incremental or full load)
Feb 2023 modify to calc returns days from delivery or pickup
Jun 2023 modify to improve sale-retn matching logic
Feb 2024 modify to use delta updating for weekend run (i.e., update over 
full 5 year period, but only the changed records, like a return coming in
for a sale happening years ago
Apr 2024  
--business_unit  filter adjusted to allow 5800 'Marketplace'
--inclde a 'flag' for marketplace_transaction (based on ownership_model)
--matching for marketplace sale-retn based on financial record keys
--RTDF marketplace transactions for DTC will have intent_store 5405 NOT the requesting FLS store, 
so pull the requesting store from OLDF (SOURCE_STORE_NUM)
-matching enhancement to utilize a new RTDF field original_tran_line_item_seq_num 
(non-Marketplace related enhancement)
*/ 

/*set date range as full or incremental
based on day of week (which is variable to allow
manual full loads any day of the week)*/ 
create multiset volatile table sarf_start_date as 
(select 
case
when td_day_of_week(CURRENT_DATE) = {backfill_day_of_week}
then date'2018-08-05'
else current_date() -{incremental_look_back}
end as start_date,
case
when td_day_of_week(CURRENT_DATE) = {backfill_day_of_week}
then 'backfill'
else 'incremental'
end as delete_range
) with data primary index(start_date) 
on commit preserve rows;  


   ---------------------------------------------------------------------------
 
create multiset volatile table  tran_sale_data_base,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
transaction_identifier varchar(50) character set unicode not casespecific,
upc_num varchar(32) character set unicode not casespecific,
sku_num varchar(32) character set unicode not casespecific,
nonmerch_fee_code varchar(8) character set unicode not casespecific,
upc_or_fee_code varchar(32) character set unicode not casespecific,
order_num varchar(32) character set unicode not casespecific  compress,
tran_line_id smallint  compress(1,2,3,4,5,6,7),
merch_unique_item_id varchar(32) character set unicode not casespecific  compress,
item_source varchar(32) character set unicode not casespecific compress 'SB_SALESEMPINIT',
commission_slsprsn_num varchar(16) character set unicode not casespecific compress ('2079333','2079334','2079335','0000'),
tran_time timestamp(6) with time zone,
tran_date DATE FORMAT 'YYYY-MM-DD',
line_item_net_amt_currency_code char(8),
price_adj_code varchar(10) character set unicode not casespecific  compress,
acp_id VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress,
business_unit_desc VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific
Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA','MARKETPLACE' ),
merch_dept_num varchar(8) character set unicode not casespecific compress,
line_level_matching_id smallint  compress(1,2,3,4,5,6,7),
financial_retail_tran_line_id varchar(50) character set unicode not casespecific compress 
)  primary index (transaction_identifier, upc_or_fee_code)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_sale_data_base
select
hdr.business_day_date,
hdr.global_tran_id,
sdtl.line_item_seq_num,
hdr.transaction_identifier,
ltrim(sdtl.upc_num,'0'),
sdtl.sku_num,
sdtl.nonmerch_fee_code,
coalesce(ltrim(sdtl.upc_num,'0'), sdtl.nonmerch_fee_code) as upc_or_fee_code,
hdr.order_num,
sdtl.tran_line_id,
sdtl.merch_unique_item_id,
sdtl.item_source,
trim(leading '0' from sdtl.commission_slsprsn_num),
sdtl.tran_time, 
sdtl.tran_date,
sdtl.line_item_net_amt_currency_code,
sdtl.price_adj_code,
hdr.acp_id,
stor.business_unit_desc,
sdtl.merch_dept_num,
case 
when hdr.data_source_code = 'pos' then sdtl.tran_line_id 
when hdr.data_source_code = 'com' then sdtl.line_item_seq_num 
else null end as line_level_matching_id,
sdtl.financial_retail_tran_line_id
from
prd_nap_usr_vws.retail_tran_hdr_fact  hdr
join
prd_nap_usr_vws.retail_tran_detail_fact_vw  sdtl
on sdtl.business_day_date = hdr.business_day_date
and sdtl.global_tran_id = hdr.global_tran_id
join
prd_nap_usr_vws.store_dim  stor
on sdtl.intent_store_num = stor.store_num
and stor.business_unit_num in (1000,2000,3500,5000,5800,6000,6500,9000,9500)
where hdr.business_day_date between (select start_date from sarf_start_date) and current_date() -1
and hdr.error_flag = 'N'
and hdr.tran_latest_version_ind = 'Y'
and hdr.tran_type_code IN ('EXCH', 'SALE', 'RETN') 
and ((sdtl.line_item_activity_type_code = 's' and sdtl.line_net_amt <> 0)
or (sdtl.line_item_activity_type_code = 's' and sdtl.line_net_amt = 0 and sdtl.tran_type_code <> 'retn')
or (sdtl.line_item_activity_type_code = 'n' and sdtl.tran_type_code in ( 'sale','exch')));

collect statistics
column ( partition),
column (transaction_identifier, upc_or_fee_code),
column (transaction_identifier, line_level_matching_id)
on
tran_sale_data_base;

 ----------------------------------------------------------------
/*  
Since the mon-sat incremental is only looking back 60 days
for matching returns to sales, there may be a return from
the past 60 days that the Sun run found matched to a sale
earlier than 60 days ago. Since that sale won't be pulled from 
the incremental range, we need to bypass that return 
(it is already accounted for).
So we delete any unmatched returns in the past 60 days that
that exist in sales prior to 60 days back.
*/

create multiset volatile table  prior_matched_returns,
no fallback, no before journal, no after  journal,
checksum =default
(
return_date  date format 'yyyy-mm-dd' ,
return_global_tran_id   bigint ,
return_line_item_seq_num  smallint
) primary index (return_date, return_global_tran_id, return_line_item_seq_num)
partition by range_n(return_date between date '2017-01-01' and date '2027-12-31' each interval '1' day,
unknown)
on commit preserve rows;
  
insert into prior_matched_returns
select sarf.return_date, sarf.return_global_tran_id, sarf.return_line_item_seq_num
from
{sales_returns_t2_schema}.sales_and_returns_fact sarf
where sarf.business_day_date < (select start_date from sarf_start_date) 
and sarf.return_date >= (select start_date from sarf_start_date ); 
 
collect statistics
column (partition),
column (return_date, return_global_tran_id, return_line_item_seq_num)
on
prior_matched_returns;

 --------------------------------------------------------------------
 /* step 2
extract from retail_tran_detail_fact_vw
just the returns data fields (for efficiency) needed for joining sales to returns,
and for joining to other tables to pull in added fields.
*/
 
create multiset volatile table  tran_retn_data_base,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
original_transaction_identifier varchar(50) character set unicode not casespecific,
upc_num varchar(32) character set unicode not casespecific,
sku_num varchar(32) character set unicode not casespecific,
nonmerch_fee_code varchar(8) character set unicode not casespecific compress ('150','6666','140'),
upc_or_fee_code varchar(32) character set unicode not casespecific,
order_num varchar(32) character set unicode not casespecific  compress,
tran_line_id smallint  compress(1,2,3,4,5,6,7),
merch_unique_item_id varchar(32) character set unicode not casespecific  compress,
item_source varchar(32) character set unicode not casespecific compress 'SB_SALESEMPINIT',
commission_slsprsn_num varchar(16) character set unicode not casespecific compress ('2079333','2079334','2079335','0000'),
tran_time timestamp(6) with time zone,
tran_date DATE FORMAT 'YYYY-MM-DD',
line_item_net_amt_currency_code char(8),
acp_id VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress,
business_unit_desc VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific
Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA', 'MARKETPLACE' ),
merch_dept_num varchar(8) character set unicode not casespecific compress,
line_level_matching_id smallint  compress(1,2,3,4,5,6,7),
financial_retail_tran_line_id varchar(50) character set unicode not casespecific compress
)  primary index (original_transaction_identifier, upc_or_fee_code)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_retn_data_base
select
hdr.business_day_date,
hdr.global_tran_id,
sdtl.line_item_seq_num,
sdtl.original_transaction_identifier,
ltrim(sdtl.upc_num,'0'),
sdtl.sku_num,
sdtl.nonmerch_fee_code,
coalesce(ltrim(sdtl.upc_num,'0'), sdtl.nonmerch_fee_code) as upc_or_fee_code,
hdr.order_num,
sdtl.tran_line_id,
sdtl.merch_unique_item_id,
sdtl.item_source,
trim(leading '0' from sdtl.commission_slsprsn_num),
sdtl.tran_time,
sdtl.tran_date,
sdtl.line_item_net_amt_currency_code,
hdr.acp_id,
stor.business_unit_desc,
sdtl.merch_dept_num,
CASE WHEN sdtl.original_tran_line_item_seq_num  = '' -- all spaces
       THEN NULL
     WHEN LTRIM(sdtl.original_tran_line_item_seq_num, '0123456789') = '' -- only digits
       THEN CAST(sdtl.original_tran_line_item_seq_num AS INTEGER)
     ELSE NULL
END as line_level_matching_id,
sdtl.financial_retail_tran_line_id
from
prd_nap_usr_vws.retail_tran_hdr_fact  hdr
join
prd_nap_usr_vws.retail_tran_detail_fact   sdtl
on sdtl.business_day_date = hdr.business_day_date
and sdtl.global_tran_id = hdr.global_tran_id
join
prd_nap_usr_vws.store_dim  stor
on sdtl.intent_store_num = stor.store_num
and stor.business_unit_num in (1000,2000,3500,5000,5800,6000,6500,9000,9500)
left join
prior_matched_returns  pmr
on 1=1
and pmr.return_date              = sdtl.business_day_date
and pmr.return_global_tran_id    = sdtl.global_tran_id
and pmr.return_line_item_seq_num = sdtl.line_item_seq_num 
where hdr.business_day_date between (select start_date from sarf_start_date) and current_date() -1
and hdr.error_flag = 'N'
and hdr.tran_latest_version_ind = 'Y'
and hdr.tran_type_code IN ('EXCH', 'SALE', 'RETN') 
and ((sdtl.line_item_activity_type_code = 'r')
or (sdtl.line_item_activity_type_code = 'n' and sdtl.tran_type_code = 'retn'))
and pmr.return_date is null;

collect statistics
column ( partition),
column (original_transaction_identifier, upc_or_fee_code),
column  (original_transaction_identifier, line_level_matching_id)
on
tran_retn_data_base;

  -------------------------------------------------------
 
/* step 3
use upc to find the department_id, and then use the department_id
to find the type of department (merch e.g. women's, or nonmerch
e.g. rpos)
*/
create multiset volatile table  merch_upc_dept,
no before journal, no after  journal,
checksum =default
(upc_num varchar(32) character set unicode not casespecific,
channel_country varchar(10) character set unicode not casespecific,
prmy_upc_ind  char(1) character set unicode not casespecific,
rms_sku_num varchar(32) character set unicode not casespecific,
dept_num varchar(8) character set unicode not casespecific,
division_num varchar(8) character set unicode not casespecific,
transaction_type varchar(8) character set unicode not casespecific compress('retail','services')
) primary index (upc_num), INDEX(RMS_SKU_NUM)
on commit preserve rows;

insert into  merch_upc_dept
select
ltrim(upc.upc_num,'0'),
upc.channel_country,
upc.prmy_upc_ind,
upc.rms_sku_num,
cast(dpt.dept_num as varchar(8)) dept_num,
cast(dpt.division_num as varchar(8)) division_num,
case when dpt.merch_dept_ind = 'y' then 'retail'
else 'services' end as transaction_type
from
(select upc_num, channel_country, prmy_upc_ind, rms_sku_num
from
prd_nap_usr_vws.product_upc_dim
qualify row_number() over
(partition by trim(leading '0' from  upc_num), channel_country  order by prmy_upc_ind desc)  = 1) upc
join
(select  rms_sku_num, dept_num
from
prd_nap_usr_vws.product_sku_dim_vw
qualify row_number() over
(partition by rms_sku_num  order by channel_country desc)  = 1) sku
on upc.rms_sku_num = sku.rms_sku_num
join
prd_nap_usr_vws.department_dim  dpt
on sku.dept_num = dpt.dept_num;

collect statistics
column (partition),
column ( upc_num),
column ( rms_sku_num)
on
merch_upc_dept;

 -------------------------------------------------------

/* step 3a
use table from step 3 to create a sku xref
with sale/retn keys to fill in missing skus
*/
create multiset volatile table   tran_sku_xref,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
rms_sku_num varchar(32) character set unicode not casespecific
) primary index (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_sku_xref
with cte_sku as
 (select
 upc_num,
 case when line_item_net_amt_currency_code = 'usd' then 'us' else 'ca' end as channel_country,
 business_day_date,
 global_tran_id,
 line_item_seq_num
 from tran_sale_data_base
 where upc_num is not null
 and sku_num is null
union all
 select
 upc_num,
 case when line_item_net_amt_currency_code = 'usd' then 'us' else 'ca' end as channel_country,
 business_day_date,
 global_tran_id,
 line_item_seq_num
 from tran_retn_data_base
 where upc_num is not null
 and sku_num is null)
 select
cte_sku.business_day_date,
cte_sku.global_tran_id,
cte_sku.line_item_seq_num,
dept.rms_sku_num
from
cte_sku
join
merch_upc_dept  dept
on cte_sku.upc_num = dept.upc_num
and cte_sku.channel_country = dept.channel_country; 

collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num) 
on
tran_sku_xref;


  -------------------------------------------------------------------------
 /* step 4
sync the dept info from step 3 above to transactions
to optimize later transaction joins on business_day_date,
global_tran_id, and line_item_seq_num
*/
create multiset volatile table  tran_dept,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
dept_num varchar(8) character set unicode not casespecific,
division_num varchar(8) character set unicode not casespecific,
transaction_type varchar(8) character set unicode not casespecific compress('retail','services')
)  primary index (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_dept
with cte_sar_upc as
 (select
 upc_num,
 case when line_item_net_amt_currency_code = 'usd' then 'us' else 'ca' end as channel_country,
 business_day_date,
 global_tran_id,
 line_item_seq_num
 from tran_sale_data_base
 where upc_num is not null
union all
 select
 upc_num,
 case when line_item_net_amt_currency_code = 'usd' then 'us' else 'ca' end as channel_country,
 business_day_date,
 global_tran_id,
 line_item_seq_num
 from tran_retn_data_base
 where upc_num is not null)
select
cte_sar_upc.business_day_date,
cte_sar_upc.global_tran_id,
cte_sar_upc.line_item_seq_num,
dept.dept_num,
dept.division_num,
dept.transaction_type
from
cte_sar_upc
join
merch_upc_dept  dept
on cte_sar_upc.upc_num = dept.upc_num
and cte_sar_upc.channel_country = dept.channel_country;

/*
add in the services with no upc */

insert into tran_dept
with cte_sar_no_upc as
 (select
 business_day_date,
 global_tran_id,
 line_item_seq_num,
 merch_dept_num
 from tran_sale_data_base
 where upc_num is null
 and merch_dept_num is not null
union all
 select
 business_day_date,
 global_tran_id,
 line_item_seq_num,
 merch_dept_num
 from tran_retn_data_base
 where upc_num is null
 and merch_dept_num is not null)
select
cte_sar_no_upc.business_day_date,
cte_sar_no_upc.global_tran_id,
cte_sar_no_upc.line_item_seq_num,
cast(dept.dept_num as varchar(8)) dept_num,
cast(dept.division_num as varchar(8)) division_num,
case when dept.merch_dept_ind = 'y' then 'retail'
else 'services' end as transaction_type
from
cte_sar_no_upc
join
prd_nap_usr_Vws.department_dim  dept
on cte_sar_no_upc.merch_dept_num = dept.dept_num;

collect statistics
column ( partition),
column ( business_day_date, global_tran_id, line_item_seq_num)
on
tran_dept;

----------------------------------------------------

 /* step 5
pull order keys from order_line_detail_fact for purpose
of creating join keys to sale and return transactions
*/

create multiset volatile table  order_upc_line_id,
no before journal, no after  journal,
checksum =default
(
order_num varchar(32) character set unicode not casespecific,
upc_code varchar(32) character set unicode not casespecific,
order_line_num char(5),
order_line_id varchar(100) character set unicode not casespecific
)  primary index (order_num, order_line_id )
on commit preserve rows;

insert into order_upc_line_id
with cte_sar_ord  as
 (select
 order_num
 from tran_sale_data_base
 where  order_num is not null
union all
 select
 order_num
 from tran_retn_data_base
 where order_num is not null)
select
ord.order_num,
case
when ord.upc_code is not null and ord.upc_code not like '%000000000000%' then ltrim(ord.upc_code,'0')
when skux.rms_sku_num is not null then skux.upc_num 
else '000000000000' end as upc_code,
cast(order_line_num as char(5)),
ord.order_line_id
from
prd_nap_usr_vws.order_line_detail_fact  ord
left join
merch_upc_dept  skux
on ord.rms_sku_num = skux.rms_sku_num
and coalesce(ord.source_channel_country_code,'us') = skux.channel_country
and skux.prmy_upc_ind = 'y'
where ord.order_date_pacific >= (select start_date from sarf_start_date) -180
and ord.canceled_date_pacific is null
and CHARACTERS(TRIM(
REGEXP_REPLACE(ord.order_num||ord.order_line_id,'[A-Z,a-z,0-9,-]*','',1,0,'i'))) = 0
and exists
(select 1
from
cte_sar_ord  cte
where cte.order_num = ord.order_num);


collect statistics
column (partition),
column (order_num, upc_code, order_line_num)
on
order_upc_line_id;


  -------------------------------------------------
 /* step 6
use order key data from step 5 above to create UPC
sequence number; order by order_line_num to attempt
order table / sales returns match where same upc
in order multiple times
*/
create multiset volatile table order_upc_seq,
no fallback, no before journal, no after  journal,
checksum =default
(order_num varchar(32) character set unicode not casespecific,
upc_code varchar(32) character set unicode not casespecific,
order_upc_seq_num smallint,
order_line_id varchar(100) character set unicode not casespecific
)  primary index (order_num, upc_code, order_upc_seq_num)
on commit preserve rows;

insert into order_upc_seq
select
order_num,
upc_code,
row_number() over (partition by order_num, upc_code
order by  order_num, upc_code, order_line_num)
as  order_upc_seq_num,
ord.order_line_id
from
order_upc_line_id  ord;

collect statistics
column ( order_num, upc_code, order_upc_seq_num)
on
order_upc_seq;

-----------------------------------------------------
 /*
because of no-product-returns,
it is possible for a re-shipment to cause multiple sales transactions
for the same order line, so create logic to avoid processing these
as two different order lines
*/
 create multiset volatile table  ord_upc_seq_max,
no fallback, no before journal, no after  journal,
checksum =default
(order_num varchar(32) character set unicode not casespecific,
 upc_code varchar(32) character set unicode not casespecific,
 max_order_upc_seq_num  smallint
)  primary index (order_num, upc_code)
on commit preserve rows;

insert into ord_upc_seq_max
select order_num,  upc_code,
max(order_upc_seq_num) as max_order_upc_seq_num
from
order_upc_seq
group by order_num, upc_code;

collect statistics
column ( order_num, upc_code)
on
ord_upc_seq_max;

 -------------------------------------------------------
 /* step 7 
build a key on the sales side for upc_seq_num;
as in step 6, order by tran_line_id in attempt to match as
close as possible the line sequence in order fact;
first build an intermediate stage table to provide index
by order number for optimization;  because of no-product-returns,
it is possible for a re-shipment to cause multiple sales transactions
for the same order line, so create logic to avoid processing these
as two different order lines
*/
 create multiset volatile table sale_order_upc_seq_stg,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
order_num varchar(32) character set unicode not casespecific,
upc_num varchar(32) character set unicode not casespecific,
tran_line_id smallint,
price_adj_code varchar(10)
)  primary index (order_num, upc_num, tran_line_id)
on commit preserve rows;

insert into sale_order_upc_seq_stg
select
saleb.business_day_date,
saleb.global_tran_id,
saleb.line_item_seq_num,
saleb.order_num,
saleb.upc_num,
saleb.tran_line_id,
saleb.price_adj_code  
from
tran_sale_data_base  saleb
where saleb.order_num is not null
and saleb.upc_num is not null;

collect statistics
column ( order_num, upc_num)
on
sale_order_upc_seq_stg;

  -------------------------------------

create multiset volatile table sale_order_upc_seq,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
order_num varchar(32) character set unicode not casespecific,
upc_num varchar(32) character set unicode not casespecific,
order_upc_seq_num smallint
)  primary index (order_num, upc_num, order_upc_seq_num )
on commit preserve rows;

insert into sale_order_upc_seq
select
 saleseq.business_day_date,
 saleseq.global_tran_id,
 saleseq.line_item_seq_num,
 saleseq.order_num,
 saleseq.upc_num,
case
when saleseq.sale_order_upc_seq_num <= maxseq.max_order_upc_seq_num
then saleseq.sale_order_upc_seq_num
else case
when saleseq.sale_order_upc_seq_num - maxseq.max_order_upc_seq_num > 0
then saleseq.sale_order_upc_seq_num - maxseq.max_order_upc_seq_num  
else maxseq.max_order_upc_seq_num
end end as  order_upc_seq_num
from
(select
stg.business_day_date,
stg.price_adj_code,
stg.global_tran_id,
stg.line_item_seq_num,
stg.order_num,
stg.upc_num,
row_number() over (partition by   stg.order_num,  stg.upc_num
order by   stg.order_num, stg.upc_num, stg.business_day_date, stg.price_adj_code desc, stg.global_tran_id, stg.tran_line_id   )
as sale_order_upc_seq_num
from
sale_order_upc_seq_stg   stg)  saleseq
left join
ord_upc_seq_max  maxseq
on saleseq.order_num = maxseq.order_num
and  saleseq.upc_num =  maxseq.upc_code;

  ---------------------------------------------------------------
/* step 8
use keys built in steps 6 and 7 to create a cross reference betwee
order data and sales data
*/
create multiset volatile table tran_order_xref,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
order_num varchar(32) character set unicode not casespecific,
order_line_id varchar(100) character set unicode not casespecific
)  primary index (order_num, order_line_id)
on commit preserve rows;

insert into tran_order_xref
select
saleseq.business_day_date,
saleseq.global_tran_id,
saleseq.line_item_seq_num,
ordseq.order_num,
ordseq.order_line_id
from
sale_order_upc_seq  saleseq
join
order_upc_seq   ordseq
on saleseq.order_num = ordseq.order_num
and saleseq.upc_num = ordseq.upc_code
and saleseq.order_upc_seq_num = ordseq.order_upc_seq_num;

collect statistics
column ( order_num, order_line_id)
on
tran_order_xref;

  --------------------------------------------------------------------
/* step 9
use xref from step 8 to create transaction key (business_day_date,
global_tran_id, line_item_seq_num) and pull remaining order fields
needed for final table
*/
create multiset volatile table tran_order,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
order_num varchar(32) character set unicode not casespecific,
upc_num varchar(32) character set unicode not casespecific,
order_line_id varchar(100) character set unicode not casespecific,
shopper_id varchar(50) character set unicode not casespecific,
source_platform_code varchar(40) character set unicode not casespecific
compress ('UNKNOWN','WEB','MOW','IOS','ANDROID','POS','IN_STORE_DIGITAL','CSR_STORE','CSR_APP','CSR_PHONE','BACKEND_SERVICE','RPOS'),
promise_type_code varchar(40) character set unicode not casespecific
compress ('STANDARD_SHIPPING','NEXT_DAY_DELIVERY','SAME_DAY_PICKUP','TWO_DAY_SHIPPING','ONE_DAY_SHIPPING','NEXT_DAY_PICKUP',
'SHIP_TO_STORE','StandardShipping','TwoBusinessDay','EXPEDITED','EXPRESS','NextBusinessDay','SAME_DAY_COURIER',
'ship_to_store','standardshipping','twobusinessday','expedited','express','nextbusinessday','same_day_courier'),
requested_level_of_service_code varchar(10) character set unicode not casespecific,
delivery_method_code varchar(40) character set unicode not casespecific compress ('unknown','ship','pick'),
destination_node_num integer,
curbside_pickup_ind char(1) character set unicode not casespecific,
bill_zip_code varchar(10) character set unicode not casespecific,
destination_zip_code varchar(100) character set unicode not casespecific,
nopr_ind char(1) character set unicode not casespecific,
not_picked_up_ind char(1) character set unicode not casespecific,
carrier_delivered_date date format 'yyyy-mm-dd',
picked_up_by_customer_date  date format 'yyyy-mm-dd',
source_store_num integer
)  primary index (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_order
select
xref.business_day_date,
xref.global_tran_id,
xref.line_item_seq_num,
ord.order_num,
ord.upc_code,
ord.order_line_id,
ord.shopper_id,
ord.source_platform_code,
ord.promise_type_code,
ord.requested_level_of_service_code,
ord.delivery_method_code,
ord.destination_node_num,
ord.curbside_pickup_ind,
ord.bill_zip_code,
ord.destination_zip_code,
case when ord.return_type = 'NO_PRODUCT_RETURN' then upper('y') else upper('n') end
as nopr_ind,
case when ord.return_reason_code = 'NOT_PICKED_UP' then upper('y') else upper('n') end
as not_picked_up_ind,
cast(ord.carrier_delivered_tmstp_pacific as date) as carrier_delivered_date ,
ord.picked_up_by_customer_date_pacific as picked_up_by_customer_date,
ord.source_store_num
from
prd_nap_usr_vws.order_line_detail_fact  ord
join
tran_order_xref  xref
on xref.order_num = ord.order_num
and xref.order_line_id = ord.order_line_id
where ord.order_date_pacific >= (select start_date from sarf_start_date) -180
and CHARACTERS(TRIM(
REGEXP_REPLACE(ord.order_num||ord.order_line_id,'[A-Z,a-z,0-9,-]*','',1,0,'i'))) = 0;

collect statistics
column ( partition),
column ( business_day_date, global_tran_id, line_item_seq_num)
on
tran_order;

   -----------------------------------------------------------------------------
/* step 10
find the payroll departments for the commission employees in
the sales and returns transaction, based on when the transaction took place.
Aug 22, consolidate new V1 HR views into temp table;
use other_line_assignment to identify emerging stylists
*/

create multiset volatile table temp_hr_worker_dim  as
(
with      
effdates as 
(select worker_number, cast(trunc(eff_begin_date) as date) as eff_date    
from prd_nap_hr_usr_vws.hr_org_details_dim_eff_date_vw     
union   
select worker_number, trunc(eff_end_date)  
from prd_nap_hr_usr_vws.hr_org_details_dim_eff_date_vw  
union
select worker_number, trunc(eff_begin_date)  
from prd_nap_hr_usr_vws.hr_line_position_details_dim_eff_date_vw    
union   
select worker_number, trunc(eff_end_date)  
from prd_nap_hr_usr_vws.hr_line_position_details_dim_eff_date_vw)         
,      
effdateranges  as      
(      
select worker_number, eff_date as eff_begin_date, 
lead(eff_date,1) over (partition by worker_number order by eff_date) eff_end_date     
from effdates
qualify eff_end_date is not null
)
select      
      v1dim.worker_number,
      v1dim.first_name,
      v1dim.last_name,
      orgeffdts.payroll_department, 
      orgdept.organization_description as payroll_department_description,
      orgeffdts.payroll_store, 
      orgstor.organization_description as payroll_store_description,
      lineffdts.other_line_assignment,  
      effdtrngs.eff_begin_date, effdtrngs.eff_end_date     
from  
(select 
worker_number, 
first_name,
last_name
from
prd_nap_hr_usr_vws.hr_worker_v1_dim   
qualify row_number() over (partition by worker_number order by last_updated desc) = 1) v1dim
 --
inner join effdateranges effdtrngs       
on v1dim.worker_number = effdtrngs.worker_number 
 --
left join
 prd_nap_hr_usr_vws.hr_org_details_dim_eff_date_vw    orgeffdts           
on orgeffdts.worker_number = effdtrngs.worker_number 
and orgeffdts.eff_end_date > effdtrngs.eff_begin_date 
and orgeffdts.eff_begin_date < effdtrngs.eff_end_date 
 --
left join
prd_nap_hr_usr_vws.hr_worker_org_dim  orgdept
on orgeffdts.payroll_department = orgdept.organization_code
and orgdept.organization_type = 'department' 
and orgdept.is_inactive = 0
--
left join
prd_nap_hr_usr_vws.hr_worker_org_dim  orgstor
on orgeffdts.payroll_store = orgstor.organization_code
and orgstor.organization_type = 'store' 
and orgstor.is_inactive = 0
 --
left join  prd_nap_hr_usr_vws.hr_line_position_details_dim_eff_date_vw  lineffdts   
on lineffdts.worker_number = effdtrngs.worker_number 
and lineffdts.eff_end_date > effdtrngs.eff_begin_date 
and lineffdts.eff_begin_date < effdtrngs.eff_end_date 
) with data primary index(worker_number, eff_begin_date, eff_end_date)
on commit preserve rows;
 --
 
 
create multiset volatile table tran_hr_stg,
no before journal, no after  journal,
checksum =default
(
worker_number varchar(20) character set unicode not casespecific ,
payroll_department  char(5) compress,
hr_es_ind char(1) compress,
eff_begin_date DATE FORMAT 'YYYY-MM-DD',
eff_end_date DATE FORMAT 'YYYY-MM-DD' 
)  primary index (worker_number, eff_begin_date, eff_end_date )
partition by worker_number mod 65535 + 1
on commit preserve rows;


insert into tran_hr_stg
select distinct
    worker_number,
    payroll_department,
    case when lower(substring(other_line_assignment,1,2))='es' 
    then upper('Y') ELSE upper('N') END AS HR_ES_IND,
    eff_begin_date,
     eff_end_date - 1 as eff_end_date
    from temp_hr_worker_dim;
   
collect statistics
column ( partition),
column ( worker_number, eff_begin_date, eff_end_date)
on
tran_hr_stg;

create multiset volatile table tran_hr,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
payroll_department  char(5) compress,
hr_es_ind char(1) compress
)  primary index (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_hr
with cte_sar_hr as
 (select
 business_day_date,
 global_tran_id,
 line_item_seq_num,
 commission_slsprsn_num,
 tran_date
 from tran_sale_data_base
 where commission_slsprsn_num is not null
union all
 select
 business_day_date,
 global_tran_id,
 line_item_seq_num,
 commission_slsprsn_num,
 tran_date
 from tran_retn_data_base
 where commission_slsprsn_num is not null)
select
cte_sar_hr.business_day_date,
cte_sar_hr.global_tran_id,
cte_sar_hr.line_item_seq_num,
hr.payroll_department,
hr.hr_es_ind
from
cte_sar_hr
join
tran_hr_stg  hr
on  cte_sar_hr.commission_slsprsn_num = hr.worker_number
and cte_sar_hr.tran_date between hr.eff_begin_date and   hr.eff_end_date;

collect statistics
column ( partition),
column (business_day_date, global_tran_id, line_item_seq_num)
on
tran_hr;
  ---------------------------------------------------------------------
 /* step 11
look up whether the sale qualifies as remote sell by joining
to the remote_sell_transactions table. 
the remote selling table has some dups, so make the sales
and returns the driver, and if a remote selling emp nbr
is present in the remote selling table, use it just once
for each distinct global_tran_id and upc.

create key of
business_day_date, global_tran_id, line_item_seq_num for
joining to sales and returns transactions later
*/
create multiset volatile table  rsell_extract,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
upc_num varchar(32) character set unicode not casespecific,
remote_sell_employee_id  varchar(20) character set unicode not casespecific
)  primary index (global_tran_id, upc_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

locking t2dl_das_remote_selling.remote_sell_transactions for access
insert into rsell_extract
select
mvp.business_day_date,
mvp.global_tran_id,
mvp.upc_num,
mvp.remote_sell_employee_id
from t2dl_das_remote_selling.remote_sell_transactions  mvp
where mvp.remote_sell_employee_id is not null
qualify row_number() over (partition by mvp.business_day_date, mvp.global_tran_id, mvp.upc_num
order by mvp.remote_sell_employee_id desc) = 1;

 
create multiset volatile table tran_rsell,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
remote_sell_employee_id  varchar(20) character set unicode not casespecific
)  primary index (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_rsell
with cte_sar  as
 (select
 business_day_date,
 global_tran_id,
 line_item_seq_num,
 upc_num
 from tran_sale_data_base
union all
 select
 business_day_date,
 global_tran_id,
 line_item_seq_num,
 upc_num
 from tran_retn_data_base)
select
sar.business_day_date,
sar.global_tran_id,
sar.line_item_seq_num,
mvp.remote_sell_employee_id
from 
cte_sar  as sar
join
rsell_extract  mvp
on sar.business_day_date = mvp.business_day_date  
and sar.global_tran_id = cast(mvp.global_tran_id as bigint)  
and ltrim(sar.upc_num,'0') = ltrim(mvp.upc_num,'0');  
 

collect statistics
column ( partition),
column ( business_day_date, global_tran_id, line_item_seq_num)
on
tran_rsell;

 
  -----------------------------------------------------------------


/* step 12
create a template of all sales transactions
then later steps will fill in the return match

*/
create multiset volatile table  sale_retn_xref,
no fallback, no before journal, no after  journal,
checksum =default
(sale_business_day_date  date format 'yyyy-mm-dd',
sale_global_tran_id   bigint,
sale_line_item_seq_num   smallint ,
retn_business_day_date  date format 'yyyy-mm-dd' compress,
retn_global_tran_id   bigint compress,
retn_line_item_seq_num  smallint compress(1,2,3,4,5,6,7),
match_source varchar(50) character set unicode not casespecific
) primary index (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
partition by range_n(sale_business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day,
unknown)
on commit preserve rows;
 
insert into  sale_retn_xref
select
sbase.business_day_date,
sbase.global_tran_id,
sbase.line_item_seq_num,
date'2027-12-31' as return_date,
0 as return_global_tran_id,
0 as return_line_item_seq_num,
'INIT' as match_source
FROM
tran_sale_data_base  sbase;

  -------------------------------------------------------------------------

/* 12a conduct a test for match using SDM financial fields 
In case other matching techniques are later added higher in the code, test
to be sure a matching isn't already in sale_retn_xref for the sale

first Pull returns using financial_retail_tran_line_id
*/
create multiset volatile table tran_retn_sdm,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
original_transaction_identifier varchar(50) character set unicode not casespecific,
financial_retail_tran_line_id varchar(50) character set unicode not casespecific,
upc_or_fee_code varchar(32) character set unicode not casespecific 
)  primary index (original_transaction_identifier, financial_retail_tran_line_id )
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

 
 insert into tran_retn_sdm
select
rbase.business_day_date,
rbase.global_tran_id,
rbase.line_item_seq_num,
rbase.original_transaction_identifier,
rbase.financial_retail_tran_line_id,
rbase.upc_or_fee_code
from
tran_retn_data_base  rbase
where rbase.original_transaction_identifier is not null
and rbase.financial_retail_tran_line_id is not null
and not  exists
(select 1
from
sale_retn_xref  xref
where 1=1
and rbase.business_day_date = xref.retn_business_day_date
and rbase.global_tran_id = xref.retn_global_tran_id
and rbase.line_item_seq_num = xref.retn_line_item_seq_num)
qualify row_number() over (partition by rbase.original_transaction_identifier, rbase.financial_retail_tran_line_id
order by  rbase.business_day_date,  rbase.upc_or_fee_code) = 1; 


collect statistics
column (partition),
column (original_transaction_identifier, financial_retail_tran_line_id) 
on
tran_retn_sdm;

-----------------------------------------------------------
 /* step 12b
same as 12a, but for sales
For efficiency, only do this if there are returns
*/
 
 create multiset volatile table tran_sale_sdm,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
transaction_identifier varchar(50) character set unicode not casespecific,
financial_retail_tran_line_id varchar(50) character set unicode not casespecific,
upc_or_fee_code varchar(32) character set unicode not casespecific 
)  primary index (transaction_identifier, financial_retail_tran_line_id )
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_sale_sdm
select
sbase.business_day_date,
sbase.global_tran_id,
sbase.line_item_seq_num,
sbase.transaction_identifier,
sbase.financial_retail_tran_line_id,
sbase.upc_or_fee_code
from
tran_sale_data_base  sbase
where exists
(select 1
from
tran_retn_sdm  rbase
where 1=1
and sbase.transaction_identifier = rbase.original_transaction_identifier
and sbase.financial_retail_tran_line_id = rbase.financial_retail_tran_line_id)
and exists
(select 1
from
sale_retn_xref  xref
where 1=1
and sbase.business_day_date = xref.sale_business_day_date
and sbase.global_tran_id = xref.sale_global_tran_id
and sbase.line_item_seq_num = xref.sale_line_item_seq_num
and xref.retn_business_day_date = '2027-12-31')   --that is, no return yet matched from earlier
qualify row_number() over (partition by sbase.transaction_identifier, sbase.financial_retail_tran_line_id
order by  sbase.business_day_date, sbase.transaction_identifier, sbase.upc_or_fee_code) = 1;

collect statistics
column (partition),
column (transaction_identifier, financial_retail_tran_line_id)
on
tran_sale_sdm;

 ------------------------------------------------------------
 /* step 12c
create xref of sales / retn based on tran/sdm financial line id
*/
 create multiset volatile table  sale_retn_xref_sdm,
no fallback, no before journal, no after  journal,
checksum =default
(sale_business_day_date  date format 'yyyy-mm-dd',
sale_global_tran_id   bigint,
sale_line_item_seq_num   smallint ,
retn_business_day_date  date format 'yyyy-mm-dd' compress,
retn_global_tran_id   bigint compress,
retn_line_item_seq_num  smallint compress(1,2,3,4,5,6,7)
) primary index (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
partition by range_n(sale_business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day,
unknown)
on commit preserve rows;

insert into  sale_retn_xref_sdm
select
sale.business_day_date,
sale.global_tran_id,
sale.line_item_seq_num,
retn.business_day_date,
retn.global_tran_id,
retn.line_item_seq_num
from
tran_sale_sdm sale
join
tran_retn_sdm  retn
on sale.transaction_identifier = retn.original_transaction_identifier
and sale.financial_retail_tran_line_id = retn.financial_retail_tran_line_id
and sale.upc_or_fee_code = retn.upc_or_fee_code;     --to exclude one-offs where same unique id assigned to multiple lines


MERGE INTO sale_retn_xref  tgt
USING sale_retn_xref_sdm  src
     ON (src.sale_business_day_date = tgt.sale_business_day_date
     and src.sale_global_tran_id = tgt.sale_global_tran_id
     and src.sale_line_item_seq_num = tgt.sale_line_item_seq_num)
WHEN MATCHED THEN
UPDATE
SET
   retn_business_day_date = src.retn_business_day_date,
   retn_global_tran_id = src. retn_global_tran_id,
   retn_line_item_seq_num = src.retn_line_item_seq_num,
   match_source = upper('sdm');
   
collect statistics
column (partition),
column (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
on
sale_retn_xref;
 
 -------------------------------------------------------------------------------------------

 
/* 13a conduct a test for match using line_item_id (won't match here if already matched
 in steps 12_ above).
Pull returns using line_item_id
*/
create multiset volatile table tran_retn_li,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
original_transaction_identifier varchar(50) character set unicode not casespecific,
line_level_matching_id varchar(10) character set unicode not casespecific,
upc_or_fee_code varchar(32) character set unicode not casespecific 
)  primary index (original_transaction_identifier, line_level_matching_id )
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

 
 insert into tran_retn_li
select
rbase.business_day_date,
rbase.global_tran_id,
rbase.line_item_seq_num,
rbase.original_transaction_identifier,
rbase.line_level_matching_id,
rbase.upc_or_fee_code
from
tran_retn_data_base  rbase
where rbase.original_transaction_identifier is not null
and rbase.line_level_matching_id is not null
and not  exists
(select 1
from
sale_retn_xref  xref
where 1=1
and rbase.business_day_date = xref.retn_business_day_date
and rbase.global_tran_id = xref.retn_global_tran_id
and rbase.line_item_seq_num = xref.retn_line_item_seq_num)
qualify row_number() over (partition by rbase.original_transaction_identifier, rbase.line_level_matching_id
order by  rbase.business_day_date,  rbase.upc_or_fee_code) = 1; 


collect statistics
column (partition),
column (original_transaction_identifier, line_level_matching_id) 
on
tran_retn_li;

-----------------------------------------------------------
 /* step 13b
same as 13a, but for sales
For efficiency, only do this if there are returns
*/
 
 create multiset volatile table tran_sale_li,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
transaction_identifier varchar(50) character set unicode not casespecific,
line_level_matching_id varchar(10) character set unicode not casespecific,
upc_or_fee_code varchar(32) character set unicode not casespecific 
)  primary index (transaction_identifier, line_level_matching_id )
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_sale_li
select
sbase.business_day_date,
sbase.global_tran_id,
sbase.line_item_seq_num,
sbase.transaction_identifier,
sbase.line_level_matching_id,
sbase.upc_or_fee_code
from
tran_sale_data_base  sbase
where exists
(select 1
from
tran_retn_li  rbase
where 1=1
and sbase.transaction_identifier = rbase.original_transaction_identifier
and sbase.line_level_matching_id = rbase.line_level_matching_id)
and exists
(select 1
from
sale_retn_xref  xref
where 1=1
and sbase.business_day_date = xref.sale_business_day_date
and sbase.global_tran_id = xref.sale_global_tran_id
and sbase.line_item_seq_num = xref.sale_line_item_seq_num
and xref.retn_business_day_date = '2027-12-31')   --that is, no return yet matched from earlier
qualify row_number() over (partition by sbase.transaction_identifier, sbase.line_level_matching_id
order by  sbase.business_day_date, sbase.transaction_identifier, sbase.upc_or_fee_code) = 1;

collect statistics
column (partition),
column (transaction_identifier, line_level_matching_id)
on
tran_sale_li;

 

 ------------------------------------------------------------
 /* step 13c
create xref of sales / retn based on tran/LI
*/
 create multiset volatile table  sale_retn_xref_li,
no fallback, no before journal, no after  journal,
checksum =default
(sale_business_day_date  date format 'yyyy-mm-dd',
sale_global_tran_id   bigint,
sale_line_item_seq_num   smallint ,
retn_business_day_date  date format 'yyyy-mm-dd' compress,
retn_global_tran_id   bigint compress,
retn_line_item_seq_num  smallint compress(1,2,3,4,5,6,7)
) primary index (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
partition by range_n(sale_business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day,
unknown)
on commit preserve rows;

insert into  sale_retn_xref_li
select
sale.business_day_date,
sale.global_tran_id,
sale.line_item_seq_num,
retn.business_day_date,
retn.global_tran_id,
retn.line_item_seq_num
from
tran_sale_li sale
join
tran_retn_li  retn
on sale.transaction_identifier = retn.original_transaction_identifier
and sale.line_level_matching_id = retn.line_level_matching_id
and sale.upc_or_fee_code = retn.upc_or_fee_code;     --to exclude one-offs where same unique id assigned to multiple lines


MERGE INTO sale_retn_xref  tgt
USING sale_retn_xref_li  src
     ON (src.sale_business_day_date = tgt.sale_business_day_date
     and src.sale_global_tran_id = tgt.sale_global_tran_id
     and src.sale_line_item_seq_num = tgt.sale_line_item_seq_num)
WHEN MATCHED THEN
UPDATE
SET
   retn_business_day_date = src.retn_business_day_date,
   retn_global_tran_id = src. retn_global_tran_id,
   retn_line_item_seq_num = src.retn_line_item_seq_num,
   match_source = upper('li');
   
collect statistics
column (partition),
column (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
on
sale_retn_xref;
 
 -------------------------------------------------------------------------------------------

 -----------------------------------------------------------
 /* 14a conduct a test for match using unique_item_id (won't match here if already matched
 in step 13c above).
Pull returns using unique_item_id
*/
create multiset volatile table tran_retn_ui,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
original_transaction_identifier varchar(50) character set unicode not casespecific,
merch_unique_item_id varchar(32) character set unicode not casespecific,
upc_num varchar(32) character set unicode not casespecific 
)  primary index (original_transaction_identifier, merch_unique_item_id )
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

 
 insert into tran_retn_ui
select
rbase.business_day_date,
rbase.global_tran_id,
rbase.line_item_seq_num,
rbase.original_transaction_identifier,
rbase.merch_unique_item_id,
rbase.upc_num
from
tran_retn_data_base  rbase
where rbase.original_transaction_identifier is not null
and rbase.merch_unique_item_id is not null
and not  exists
(select 1
from
sale_retn_xref  xref
where 1=1
and rbase.business_day_date = xref.retn_business_day_date
and rbase.global_tran_id = xref.retn_global_tran_id
and rbase.line_item_seq_num = xref.retn_line_item_seq_num)
qualify row_number() over (partition by rbase.original_transaction_identifier, rbase.merch_unique_item_id
order by  rbase.business_day_date,  rbase.upc_num) = 1; 


collect statistics
column (partition),
column (original_transaction_identifier,merch_unique_item_id) 
on
tran_retn_ui;

-----------------------------------------------------------
 /* step 14b
same as 14a, but for sales
For efficiency, only do this if there are returns
*/
 
 create multiset volatile table tran_sale_ui,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
transaction_identifier varchar(50) character set unicode not casespecific,
merch_unique_item_id varchar(32) character set unicode not casespecific,
upc_num varchar(32) character set unicode not casespecific 
)  primary index (transaction_identifier, merch_unique_item_id )
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_sale_ui
select
sbase.business_day_date,
sbase.global_tran_id,
sbase.line_item_seq_num,
sbase.transaction_identifier,
sbase.merch_unique_item_id,
sbase.upc_num
from
tran_sale_data_base  sbase
where exists
(select 1
from
tran_retn_ui  rbase
where 1=1
and sbase.transaction_identifier = rbase.original_transaction_identifier
and sbase.merch_unique_item_id = rbase.merch_unique_item_id)
and exists
(select 1
from
sale_retn_xref  xref
where 1=1
and sbase.business_day_date = xref.sale_business_day_date
and sbase.global_tran_id = xref.sale_global_tran_id
and sbase.line_item_seq_num = xref.sale_line_item_seq_num
and xref.retn_business_day_date = '2027-12-31')   --that is, no return yet matched from earlier
qualify row_number() over (partition by sbase.transaction_identifier, sbase.merch_unique_item_id
order by  sbase.business_day_date, sbase.transaction_identifier, sbase.upc_num) = 1;

collect statistics
column (partition),
column (transaction_identifier, merch_unique_item_id)
on
tran_sale_ui;

 

 ------------------------------------------------------------
 /* step 14c
create xref of sales / retn based on tran/UI
*/
 create multiset volatile table  sale_retn_xref_ui,
no fallback, no before journal, no after  journal,
checksum =default
(sale_business_day_date  date format 'yyyy-mm-dd',
sale_global_tran_id   bigint,
sale_line_item_seq_num   smallint ,
retn_business_day_date  date format 'yyyy-mm-dd' compress,
retn_global_tran_id   bigint compress,
retn_line_item_seq_num  smallint compress(1,2,3,4,5,6,7)
) primary index (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
partition by range_n(sale_business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day,
unknown)
on commit preserve rows;

insert into  sale_retn_xref_ui
select
sale.business_day_date,
sale.global_tran_id,
sale.line_item_seq_num,
retn.business_day_date,
retn.global_tran_id,
retn.line_item_seq_num
from
tran_sale_ui sale
join
tran_retn_ui  retn
on sale.transaction_identifier = retn.original_transaction_identifier
and sale.merch_unique_item_id = retn.merch_unique_item_id
and sale.upc_num = retn.upc_num;     --to exclude one-offs where same unique id assigned to multiple lines


MERGE INTO sale_retn_xref  tgt
USING sale_retn_xref_ui  src
     ON (src.sale_business_day_date = tgt.sale_business_day_date
     and src.sale_global_tran_id = tgt.sale_global_tran_id
     and src.sale_line_item_seq_num = tgt.sale_line_item_seq_num)
WHEN MATCHED THEN
UPDATE
SET
   retn_business_day_date = src.retn_business_day_date,
   retn_global_tran_id = src. retn_global_tran_id,
   retn_line_item_seq_num = src.retn_line_item_seq_num,
   match_source = upper('ui');
   
collect statistics
column (partition),
column (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
on
sale_retn_xref;
 
 -------------------------------------------------------------------------------------------

 /* step 15a
pull returns using upc
*/
create multiset volatile table tran_retn_seq,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
original_transaction_identifier varchar(50) character set unicode not casespecific,
upc_or_fee_code varchar(32) character set unicode not casespecific,
upc_or_fee_code_seq_num smallint
)  primary index (original_transaction_identifier, upc_or_fee_code, upc_or_fee_code_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;
 
 insert into tran_retn_seq
select
rbase.business_day_date,
rbase.global_tran_id,
rbase.line_item_seq_num,
rbase.original_transaction_identifier,
rbase.upc_or_fee_code,
row_number() over (partition by rbase.original_transaction_identifier, rbase.upc_or_fee_code
order by  rbase.business_day_date,  rbase.global_tran_id, rbase.line_item_seq_num)
as upc_or_fee_code_seq_num
from
tran_retn_data_base  rbase
where rbase.original_transaction_identifier is not null
and not  exists
(select 1
from
sale_retn_xref  xref
where 1=1
and rbase.business_day_date = xref.retn_business_day_date
and rbase.global_tran_id = xref.retn_global_tran_id
and rbase.line_item_seq_num = xref.retn_line_item_seq_num);


collect statistics
column (partition),
column (original_transaction_identifier,upc_or_fee_code, upc_or_fee_code_seq_num),
column (original_transaction_identifier, upc_or_fee_code)
on
tran_retn_seq;

-----------------------------------------------------------
 /* step 15b
If no unique id return match processed in 13, try upc
(check template from 12 for no return matched via 13)
But for efficiency, only do this if there are returns
*/
 
 create multiset volatile table tran_sale_seq,
no fallback, no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
transaction_identifier varchar(50) character set unicode not casespecific,
upc_or_fee_code varchar(32) character set unicode not casespecific ,
upc_or_fee_code_seq_num smallint
)  primary index (transaction_identifier, upc_or_fee_code, upc_or_fee_code_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into tran_sale_seq
select
sbase.business_day_date,
sbase.global_tran_id,
sbase.line_item_seq_num,
sbase.transaction_identifier,
sbase.upc_or_fee_code,
row_number() over (partition by sbase.transaction_identifier, sbase.upc_or_fee_code
order by  sbase.business_day_date,  sbase.global_tran_id, sbase.line_item_seq_num)
as upc_or_fee_code_seq_num
from
tran_sale_data_base  sbase
where exists
(select 1
from
tran_retn_seq  rbase
where 1=1
and sbase.transaction_identifier = rbase.original_transaction_identifier
and sbase.upc_or_fee_code = rbase.upc_or_fee_code)
and exists
(select 1
from
sale_retn_xref  xref
where 1=1
and sbase.business_day_date = xref.sale_business_day_date
and sbase.global_tran_id = xref.sale_global_tran_id
and sbase.line_item_seq_num = xref.sale_line_item_seq_num
and xref.retn_business_day_date = '2027-12-31');

collect statistics
column (partition),
column (transaction_identifier, upc_or_fee_code, upc_or_fee_code_seq_num)
on
tran_sale_seq;

 
 ------------------------------------------------------------
 /* step 15c
create xref of sales / retn based on tran/upc/seq
inner join will eliminate matches already found by
unique_item_id, since tran_sale_seq only has unmatched
sales (12a logic)
*/
 create multiset volatile table  sale_retn_xref_seq,
no fallback, no before journal, no after  journal,
checksum =default
(sale_business_day_date  date format 'yyyy-mm-dd',
sale_global_tran_id   bigint,
sale_line_item_seq_num   smallint ,
retn_business_day_date  date format 'yyyy-mm-dd' compress,
retn_global_tran_id   bigint compress,
retn_line_item_seq_num  smallint compress(1,2,3,4,5,6,7)
) primary index (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
partition by range_n(sale_business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day,
unknown)
on commit preserve rows;

insert into  sale_retn_xref_seq
select
sale.business_day_date,
sale.global_tran_id,
sale.line_item_seq_num,
retn.business_day_date,
retn.global_tran_id,
retn.line_item_seq_num
from
tran_sale_seq sale
join
tran_retn_seq  retn
on sale.transaction_identifier = retn.original_transaction_identifier
and sale.upc_or_fee_code = retn.upc_or_fee_code
and sale.upc_or_fee_code_seq_num = retn.upc_or_fee_code_seq_num; 


MERGE INTO sale_retn_xref  tgt
USING sale_retn_xref_seq  src
     ON (src.sale_business_day_date = tgt.sale_business_day_date
     and src.sale_global_tran_id = tgt.sale_global_tran_id
     and src.sale_line_item_seq_num = tgt.sale_line_item_seq_num)
WHEN MATCHED THEN
UPDATE
SET
   retn_business_day_date = src.retn_business_day_date,
   retn_global_tran_id = src. retn_global_tran_id,
   retn_line_item_seq_num = src.retn_line_item_seq_num,
   match_source = upper('upc');
   
collect statistics
column (partition),
column (sale_business_day_date, sale_global_tran_id, sale_line_item_seq_num)
on
sale_retn_xref;
  ---------------
  /* step 16
create a file of all returns where a match to sale not found
this allow an index on return date/global_tran/line_item
for processing below for creating 'orphan' returns in SARF
*/
create multiset volatile table   retn_no_match,
no fallback, no before journal, no after  journal,
checksum =default
(retn_business_day_date  date format 'yyyy-mm-dd'  ,
retn_global_tran_id   bigint ,
retn_line_item_seq_num  smallint 
) primary index (retn_business_day_date, retn_global_tran_id, retn_line_item_seq_num)
partition by range_n(retn_business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day,
unknown)
on commit preserve rows;

insert into   retn_no_match
select
rbase.business_day_date,
rbase.global_tran_id,
rbase.line_item_seq_num 
FROM
tran_retn_data_base  rbase
where not exists
  (select 1
  from
  sale_retn_xref  xref 
  where 1=1
  and rbase.business_day_date = xref.retn_business_day_date
  and rbase.global_tran_id = xref.retn_global_tran_id
  and rbase.line_item_seq_num = xref.retn_line_item_seq_num);
;

collect statistics
column (partition),
column (retn_business_day_date, retn_global_tran_id, retn_line_item_seq_num)
on
retn_no_match;
---------------
/* Following section is for delta processing enhancement
to detect differences production table versus today's
data in fields prone to alteration (returns back posted to sales,
product hierarchy, and acp-id) for the weekend run.
The daily run still posts that past 60 days, because there can be
Sales Audit changes to any of the fields, not just product and acp_id.
The basic idea in delta for the weekend is that previously the entire
table was just rebuilt, thus getting all the latest data.  The Delta
enhancement checks which records have data changes, and then just
delete and reinsert those records.
*/
  ----------------------------------------------------------------

  /* STEP 16a
  Build an extract of SARF keys and dimensions
  from current production file (compare later to
  find delta
 */
  
   create multiset volatile table prod_delta_file,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
return_date DATE FORMAT 'yyyy-mm-dd' ,
return_global_tran_id BIGINT ,
return_line_item_seq_num SMALLINT ,
business_unit_desc varchar(50) character set unicode not casespecific , 
acp_id varchar(50) character set unicode not casespecific,
merch_dept_num varchar(8) character set unicode not casespecific,
division_num VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific 
)primary index(business_day_date, global_tran_id, line_item_seq_num,
return_date, return_global_tran_id, return_line_item_seq_num,
business_unit_desc,            
acp_id,                        
merch_dept_num,                
division_num  ),
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;

insert into prod_delta_file
select 
coalesce(business_day_date, date'2027-12-31' ) business_day_date,
coalesce(global_tran_id, 0 ), 
coalesce(line_item_seq_num, 0 ),
coalesce(return_date, date'2027-12-31') return_date,
coalesce(return_global_tran_id, 0 ), 
coalesce(return_line_item_seq_num, 0 ),
coalesce(business_unit_desc,'0'),            
coalesce(acp_id ,'0'),                        
coalesce(merch_dept_num, '0' ),                
coalesce(division_num, '0' )                  
from
{sales_returns_t2_schema}.sales_and_returns_fact
where exists
(select 1 from sarf_start_date where delete_range = 'backfill'); 

 
collect statistics
column ( business_day_date, global_tran_id, line_item_seq_num,
return_date, return_global_tran_id, return_line_item_seq_num,
business_unit_desc,            
acp_id,                        
merch_dept_num,                
division_num)      
on
prod_delta_file;
  -----------------------------------------
  /* STEP 16b
  Build an extract of SARF keys and dimensions
  from latest SARF run up-to-this-point
  to compare to prod for detecting deltas.
  For example if prod extract only had sale key but not
  return key, and now return has posted, so the current run
  will have both sale and return keys, so a delta will result
  which will drive a delete and insert (delete the prod record with 
  just the sale key, insert the record with both sale and return 
  keys).  UPDATE wasn't used because it would require a much more
  extensive re-write of existing process.
 */
 
 create multiset volatile table update_delta_file,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
return_date DATE FORMAT 'yyyy-mm-dd' ,
return_global_tran_id BIGINT ,
return_line_item_seq_num SMALLINT ,
business_unit_desc varchar(50) character set unicode not casespecific , 
acp_id varchar(50) character set unicode not casespecific,
merch_dept_num varchar(8) character set unicode not casespecific,
division_num VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific 
)primary index(business_day_date, global_tran_id, line_item_seq_num,
return_date, return_global_tran_id, return_line_item_seq_num,
business_unit_desc,            
acp_id,                        
merch_dept_num,                
division_num  ),
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day, UNKNOWN)
on commit preserve rows;
 
 
insert into update_delta_file
select
business_day_date, 
global_tran_id, 
line_item_seq_num,
return_date, 
return_global_tran_id, 
return_line_item_seq_num,
business_unit_desc,     
acp_id,                      
merch_dept_num,
division_num
from
(select 
sbase.business_day_date, 
sbase.global_tran_id, 
sbase.line_item_seq_num,
rbase.business_day_date as return_date, 
rbase.global_tran_id as return_global_tran_id, 
rbase.line_item_seq_num as return_line_item_seq_num,
coalesce(sbase.business_unit_desc,'0') as business_unit_desc,     
coalesce(sbase.acp_id, '0') as acp_id,                      
case when dept.dept_num is not null then dept.dept_num else sbase.merch_dept_num end as merch_dept_num,
coalesce(dept.division_num, '0' ) as division_num                                  
from
sale_retn_xref  xref
left join
tran_sale_data_base  sbase
on 1=1
and sbase.business_day_date = xref.sale_business_day_date
and sbase.global_tran_id = xref.sale_global_tran_id
and sbase.line_item_seq_num = xref.sale_line_item_seq_num
left join
tran_retn_data_base  rbase
on 1=1
and rbase.business_day_date = xref.retn_business_day_date
and rbase.global_tran_id = xref.retn_global_tran_id
and rbase.line_item_seq_num = xref.retn_line_item_seq_num
left join
tran_dept  dept
on sbase.business_day_date = dept.business_day_date
and sbase.global_tran_id   = dept.global_tran_id
and sbase.line_item_seq_num = dept.line_item_seq_num
where xref.sale_business_day_date is not null
and xref.retn_business_day_date <> '2027-12-31'
 --
union all
 --
 select 
sbase.business_day_date, 
sbase.global_tran_id, 
sbase.line_item_seq_num,
date'2027-12-31' return_date, 
0 as return_global_tran_id, 
0 as return_line_item_seq_num,
coalesce(sbase.business_unit_desc,'0') as business_unit_desc,     
coalesce(sbase.acp_id, '0') as acp_id,                    
case when dept.dept_num is not null then dept.dept_num else sbase.merch_dept_num end as merch_dept_num,
coalesce(dept.division_num, '0' ) as division_num 
from
sale_retn_xref  xref
left join
tran_sale_data_base  sbase
on 1=1
and sbase.business_day_date = xref.sale_business_day_date
and sbase.global_tran_id = xref.sale_global_tran_id
and sbase.line_item_seq_num = xref.sale_line_item_seq_num
left join
tran_dept  dept
on sbase.business_day_date = dept.business_day_date
and sbase.global_tran_id   = dept.global_tran_id
and sbase.line_item_seq_num = dept.line_item_seq_num
where xref.sale_business_day_date is not null
and  xref.retn_business_day_date = '2027-12-31'
 --
union all
 --
 select 
date'2027-12-31' as business_day_date,
0 as global_tran_id, 
0 as line_item_seq_num,
rbase.business_day_date as return_date,
rbase.global_tran_id as return_global_tran_id,
rbase.line_item_seq_num as return_line_item_seq_num,
coalesce(rbase.business_unit_desc,'0') as business_unit_desc,     
coalesce(rbase.acp_id, '0') as acp_id,                      
case when dept.dept_num is not null then dept.dept_num else rbase.merch_dept_num end as merch_dept_num,
coalesce(dept.division_num, '0' ) as division_num                                  
from
RETN_NO_MATCH   xref   
left join
tran_retn_data_base  rbase
on 1=1
and rbase.business_day_date = xref.retn_business_day_date
and rbase.global_tran_id = xref.retn_global_tran_id
and rbase.line_item_seq_num = xref.retn_line_item_seq_num
left join
tran_dept  dept
on rbase.business_day_date = dept.business_day_date
and rbase.global_tran_id   = dept.global_tran_id
and rbase.line_item_seq_num = dept.line_item_seq_num) tbl 
where exists
(select 1 from sarf_start_date where delete_range = 'backfill'); 
 
 
collect statistics
column ( business_day_date, global_tran_id, line_item_seq_num,
return_date, return_global_tran_id, return_line_item_seq_num,
business_unit_desc,            
acp_id,                        
merch_dept_num,                
division_num)      
on
update_delta_file;

  ----------------------------------------------------------------------
/* step 16d   DELETES
find the delta of production records not in the current refresh.
For example, prod just has sale key/null retn key, but the refresh found match
(sale/key and retn/key) so the prod record will be a delete.
*/

create multiset volatile table  delta_deletes,
no before journal, no after  journal,
checksum =default
as
(select business_day_date, global_tran_id, line_item_seq_num,
return_date, return_global_tran_id, return_line_item_seq_num 
from prod_delta_file  pdf
where 1=1
and not exists
(select 1
from
update_delta_file udf
where 1=1
and udf.business_day_date = pdf.business_day_date 
and udf.global_tran_id =  pdf.global_tran_id 
and udf.line_item_seq_num = pdf.line_item_seq_num
and udf.return_date = pdf.return_date
and udf.return_global_tran_id = pdf.return_global_tran_id 
and udf.return_line_item_seq_num = pdf.return_line_item_seq_num
and udf.business_unit_desc = pdf.business_unit_desc           
and udf.acp_id =  pdf.acp_id                     
and udf.merch_dept_num = pdf.merch_dept_num               
and udf.division_num = pdf.division_num)
) with data primary index  (business_day_date ,global_tran_id ,line_item_seq_num ,
return_date ,return_global_tran_id ,return_line_item_seq_num)
PARTITION BY Range_N(business_day_date  BETWEEN DATE '2017-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' DAY , UNKNOWN)
 ON COMMIT PRESERVE ROWS;



collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num, 
return_date, return_global_tran_id, return_line_item_seq_num) 
on
delta_deletes;

   ------------------------------------------------------------
/* step 16e  INSERTS
Find records in current refresh that are not in the production table.
For example, a newly matched sale/retn will need to be inserted
(the current production table will just have the sale key and so
will be flagged for delete via step 16c.
*/
 
create multiset volatile table  delta_inserts,
no before journal, no after  journal,
checksum =default
as
(select business_day_date, global_tran_id, line_item_seq_num,
return_date, return_global_tran_id, return_line_item_seq_num
from update_delta_file  pdf
where not exists
(select 1
from
prod_delta_file udf
where 1=1
and udf.business_day_date = pdf.business_day_date 
and udf.global_tran_id =  pdf.global_tran_id 
and udf.line_item_seq_num = pdf.line_item_seq_num
and udf.return_date = pdf.return_date
and udf.return_global_tran_id = pdf.return_global_tran_id 
and udf.return_line_item_seq_num = pdf.return_line_item_seq_num
and udf.business_unit_desc = pdf.business_unit_desc           
and udf.acp_id =  pdf.acp_id                     
and udf.merch_dept_num = pdf.merch_dept_num               
and udf.division_num = pdf.division_num)
) with data primary index  (business_day_date ,global_tran_id ,line_item_seq_num ,
return_date ,return_global_tran_id ,return_line_item_seq_num)
PARTITION BY Range_N(business_day_date  BETWEEN DATE '2017-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' DAY , UNKNOWN)
 ON COMMIT PRESERVE ROWS;
 
collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num, 
return_date, return_global_tran_id, return_line_item_seq_num) 
on
delta_inserts;

 
 ----------------------------------------------------------------------
/* step 17a
If this is weekend backfill, the sale_retn_xref at this point will have 
the full backfull.  But we only will need the insert records.  So
subtract the other keys from the xref, so that it will drive a reduced
set of updates in the steps below
*/
 
delete from sale_retn_xref  xref
where exists
(select 1 from sarf_start_date where delete_range = 'backfill') 
and not exists
(select 1
from
delta_inserts  delta
where 1=1
and xref.sale_business_day_date = delta.business_day_date
and xref.sale_global_tran_id = delta.global_tran_id
and xref.sale_line_item_seq_num = delta.line_item_seq_num
and xref.retn_business_day_date = delta.return_date
and xref.retn_global_tran_id = delta.return_global_tran_id
and xref.retn_line_item_seq_num = delta.return_line_item_seq_num);

----------------------------------------------------------------------
/* step 17b
If this is weekend backfill, the retn_no_match at this point will have 
the full backfull for return orphans.  But we only will need the new ones.  So
subtract the other keys from the retn_no_Match, so that it will drive a reduced
set of updates in the steps below
*/
 
delete from retn_no_match  xref
where exists
(select 1 from sarf_start_date where delete_range = 'backfill') 
and not exists
(select 1
from
delta_inserts  delta
where 1=1
and delta.business_day_date = '2027-12-31'
and delta.global_tran_id = 0
and delta.line_item_seq_num = 0
and xref.retn_business_day_date = delta.return_date
and xref.retn_global_tran_id = delta.return_global_tran_id
and xref.retn_line_item_seq_num = delta.return_line_item_seq_num);

  ----------------------------------------------------------------------
/* step 18a
extract all of the sales fields needed for the final
sales and returns table 
*/
create multiset volatile table sar_sales_extract,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
transaction_identifier varchar(50) character set unicode not casespecific,
upc_num varchar(32) character set unicode not casespecific,
sku_num  varchar(32) character set unicode not casespecific,
order_date date format 'yyyy-mm-dd' compress,
tran_date  date format 'yyyy-mm-dd' compress,
acp_id varchar(50) character set unicode not casespecific,
order_num varchar(32) character set unicode not casespecific compress,
line_item_quantity decimal(8,0) compress 1. ,
line_net_amt decimal(12,2),
line_net_usd_amt decimal(12,2),
line_item_regular_price decimal(12,2),
employee_discount_amt decimal(12,2) compress 0.00 ,
employee_discount_usd_amt decimal(12,2) compress 0.00 ,
line_item_promo_amt decimal(12,2) compress 0.00 ,
merch_price_adjust_reason varchar(8) character set unicode not casespecific compress,
line_item_order_type varchar(32) character set unicode not casespecific compress
('StoreInitStoreTake','CustInitWebOrder','RPOS','StoreInitDTCAuto','StoreInitSameStrSend','CustInitPhoneOrder','StoreInitDTCManual','DESKTOP_WEB'),
line_item_fulfillment_type varchar(32) character set unicode not casespecific
compress ('StoreTake','FulfillmentCenter','StoreShipSend','RPOS','VendorDropShip','StorePickUp'),
intent_store_num integer compress (808, 828,867),
business_unit_desc varchar(50) character set unicode not casespecific
compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA', 'MARKETPLACE' ) ,
ringing_store_num integer compress (808, 828,867),
price_adj_code varchar(10) character set unicode not casespecific compress ('N','B','A'),
commission_slsprsn_num varchar(16) character set unicode not casespecific compress ('2079333','2079334','2079335','0000'),
nonmerch_fee_code varchar(8) character set unicode not casespecific compress,
item_source varchar(32) character set unicode not casespecific compress 'SB_SALESEMPINIT',
merch_dept_num varchar(8) character set unicode not casespecific compress,
sa_source_ind  CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
ownership_model varchar(50) character set unicode not casespecific compress 'ECONCESSION'
)primary index (business_day_date, global_tran_id, line_item_seq_num),
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day)
on commit preserve rows;
 
   ----------------------------------------------------------------------
/* step 18b
insert all sales data  
*/
insert into sar_sales_extract
select
sdtl.business_day_date,
sdtl.global_tran_id,
sdtl.line_item_seq_num,
sdtl.transaction_identifier,
sdtl.upc_num,
sdtl.sku_num, 
sdtl.order_date,
sdtl.tran_date,
sdtl.acp_id,
sdtl.order_num,
sdtl.line_item_quantity,
sdtl.line_net_amt,
sdtl.line_net_usd_amt,
sdtl.line_item_regular_price,
sdtl.employee_discount_amt,
sdtl.employee_discount_usd_amt,
sdtl.line_item_promo_amt,
sdtl.merch_price_adjust_reason,
sdtl.line_item_order_type,
sdtl.line_item_fulfillment_type,
sdtl.intent_store_num,
stor.business_unit_desc,
sdtl.ringing_store_num,
sdtl.price_adj_code,
sdtl.commission_slsprsn_num,
sdtl.nonmerch_fee_code,
sdtl.item_source,
sdtl.merch_dept_num,  
case when sdtl.data_source_code = 'sa' then upper('y') else null end as sa_source_ind,
sdtl.ownership_model
from
prd_nap_usr_vws.retail_tran_detail_fact_vw  sdtl
join
sale_retn_xref   xref
on  sdtl.business_day_date = xref.sale_business_day_date
and sdtl.global_tran_id = xref.sale_global_tran_id
and sdtl.line_item_seq_num = xref.sale_line_item_seq_num
join
prd_nap_usr_vws.store_dim  stor
on sdtl.intent_store_num = stor.store_num
and stor.business_unit_num in (1000,2000,3500,5000,5800,6000,6500,9000,9500); 
 
   ----------------------------------------------------------------------

collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num)
on
sar_sales_extract;

   ------------------------------------------------------------------
/* step 19a
extract all of the returns fields needed for the final
sales and returns table
*/
 create multiset volatile table sar_returns_extract,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint ,
transaction_identifier varchar(50) character set unicode not casespecific,
original_transaction_identifier varchar(50) character set unicode not casespecific,
original_business_date date format 'yyyy-mm-dd',
upc_num varchar(32) character set unicode not casespecific,
sku_num  varchar(32) character set unicode not casespecific,
order_date date format 'yyyy-mm-dd' compress,
tran_date  date format 'yyyy-mm-dd' compress,
acp_id varchar(50) character set unicode not casespecific,
order_num varchar(32) character set unicode not casespecific compress,
line_item_quantity decimal(8,0) compress 1. ,
line_net_amt decimal(12,2),
line_net_usd_amt decimal(12,2),
line_item_regular_price decimal(12,2),
employee_discount_amt decimal(12,2) compress 0.00 ,
employee_discount_usd_amt decimal(12,2) compress 0.00 ,
line_item_promo_amt decimal(12,2) compress 0.00 ,
merch_price_adjust_reason varchar(8) character set unicode not casespecific compress,
line_item_order_type varchar(32) character set unicode not casespecific compress
('StoreInitStoreTake','CustInitWebOrder','RPOS','StoreInitDTCAuto','StoreInitSameStrSend','CustInitPhoneOrder','StoreInitDTCManual','DESKTOP_WEB'),
line_item_fulfillment_type varchar(32) character set unicode not casespecific
compress ('StoreTake','FulfillmentCenter','StoreShipSend','RPOS','VendorDropShip','StorePickUp'),
intent_store_num integer compress (808, 828,867),
business_unit_desc varchar(50) character set unicode not casespecific
compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA', 'MARKETPLACE' ) ,
ringing_store_num integer compress (808, 828,867),
xborder_retn_ind  varchar(10) character set unicode not casespecific compress ('N', 'Y'),
commission_slsprsn_num varchar(16) character set unicode not casespecific compress ('2079333','2079334','2079335','0000'),
nonmerch_fee_code varchar(8) character set unicode not casespecific,
item_source varchar(32) character set unicode not casespecific compress 'SB_SALESEMPINIT',
merch_dept_num varchar(8) character set unicode not casespecific compress,
sa_source_ind  CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
ownership_model varchar(50) character set unicode not casespecific compress 'ECONCESSION'
)primary index (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2027-12-31' each interval '1' day, unknown)
on commit preserve rows;

/* step 19b
extract all of the returns fields needed  
*/
insert into sar_returns_extract
with cte_retn as
 (select
 retn_business_day_date,
 retn_global_tran_id,
 retn_line_item_seq_num
 from sale_retn_xref
 where retn_business_day_date <> '2027-12-31'
union all
 select
 retn_business_day_date,
 retn_global_tran_id,
 retn_line_item_seq_num
 from retn_no_match)
select
rdtl.business_day_date,
rdtl.global_tran_id,
rdtl.line_item_seq_num,
rdtl.transaction_identifier,
rdtl.original_transaction_identifier,
rdtl.original_business_date,
rdtl.upc_num,
rdtl.sku_num,
rdtl.order_date,
rdtl.tran_date,
rdtl.acp_id,
rdtl.order_num,
rdtl.line_item_quantity,
rdtl.line_net_amt,
rdtl.line_net_usd_amt,
rdtl.line_item_regular_price,
rdtl.employee_discount_amt,
rdtl.employee_discount_usd_amt,
rdtl.line_item_promo_amt,
rdtl.merch_price_adjust_reason,
rdtl.line_item_order_type,
rdtl.line_item_fulfillment_type,
rdtl.intent_store_num,
stor.business_unit_desc,
rdtl.ringing_store_num,
case when rdtl.line_item_net_amt_currency_code <> rdtl.original_line_item_amt_currency_code
and rdtl.original_line_item_amt_currency_code is not null then upper('y') else upper('n') end as xborder_retn_ind,
rdtl.commission_slsprsn_num,
rdtl.nonmerch_fee_code,
rdtl.item_source,
rdtl.merch_dept_num,
case when rdtl.data_source_code = 'sa' then 'y' else null end as sa_source_ind,
rdtl.ownership_model
from
prd_nap_usr_vws.retail_tran_detail_fact_vw  rdtl
join
cte_retn  xref
on  rdtl.business_day_date = xref.retn_business_day_date
and rdtl.global_tran_id = xref.retn_global_tran_id
and rdtl.line_item_seq_num = xref.retn_line_item_seq_num
join
prd_nap_usr_vws.store_dim  stor
on rdtl.intent_store_num = stor.store_num
and stor.business_unit_num in (1000,2000,3500,5000,5800, 6000,6500,9000,9500);  


collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num)
on
sar_returns_extract;

  -----------------------------------------------------
  /* for efficiency, process data derivations into a  
  staging table, so the final load into prod table can be
  a simple copy*/

CREATE MULTISET VOLATILE TABLE sales_and_returns_stg,
NO FALLBACK, NO BEFORE JOURNAL, NO AFTER  JOURNAL,
CHECKSUM =DEFAULT
(business_day_date  DATE FORMAT 'yyyy-mm-dd',
order_date DATE FORMAT 'yyyy-mm-dd' Compress,
tran_date  DATE FORMAT 'yyyy-mm-dd' Compress,
business_unit_desc VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific
Compress ('FULL LINE','RACK','OFFPRICE ONLINE', 'N.COM','N.CA','FULL LINE CANADA', 'RACK CANADA', 'MARKETPLACE' ) ,
acp_id VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress,
global_tran_id   BIGINT,
line_item_seq_num   SMALLINT ,
order_num VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
order_line_id VARCHAR(100) CHARACTER SET Unicode NOT CaseSpecific Compress,
transaction_type VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress ('retail','services'),
upc_num VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
sku_num  VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress,
shipped_qty INTEGER Compress (1),
shipped_sales DECIMAL(12,2),
shipped_usd_sales DECIMAL(12,2),
employee_discount_amt DECIMAL(12,2) Compress 0.00 ,
has_employee_discount INTEGER Compress (0,1),
employee_discount_usd_amt DECIMAL(12,2) Compress 0.00 ,
line_item_order_type VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress
('StoreInitStoreTake','CustInitWebOrder','RPOS','StoreInitDTCAuto','StoreInitSameStrSend','CustInitPhoneOrder','StoreInitDTCManual','DESKTOP_WEB'),
line_item_fulfillment_type VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific
Compress ('StoreTake','FulfillmentCenter','StoreShipSend','RPOS','VendorDropShip','StorePickUp'),
intent_store_num INTEGER Compress (808,828,867),
ringing_store_num INTEGER Compress (808,828,867),
price_type CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress ('R','C','P'),
regular_price_amt DECIMAL(12,2),
transaction_price_amt DECIMAL(12,2),
sale_price_adj CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress ('N','Y'),
merch_dept_num VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific,
division_num VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific,
nonmerch_fee_code VARCHAR(8) CHARACTER SET Unicode NOT CaseSpecific Compress ('150','6666','140'),
item_source VARCHAR(32) CHARACTER SET Unicode NOT CaseSpecific Compress 'SB_SALESEMPINIT',
commission_slsprsn_num VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress ('2079333','2079334','2079335','0000'),
ps_employee_id VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress,
remote_sell_employee_id VARCHAR(16) CHARACTER SET Unicode NOT CaseSpecific Compress ('2079333','2079334','2079335','0000'),
payroll_dept   CHAR(5) CHARACTER SET Unicode NOT CaseSpecific Compress,
hr_es_ind  CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
shopper_id     VARCHAR(50) CHARACTER SET Unicode NOT CaseSpecific Compress,
source_platform_code   VARCHAR(40) CHARACTER SET Unicode NOT CaseSpecific Compress,
promise_type_code  VARCHAR(40) CHARACTER SET Unicode NOT CaseSpecific Compress,
requested_level_of_service_code VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
delivery_method_code  VARCHAR(40) Compress,
destination_node_num  INTEGER Compress,
curbside_pickup_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
bill_zip_code  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
destination_zip_code  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress,
return_date DATE FORMAT 'yyyy-mm-dd' ,
original_business_date DATE FORMAT 'yyyy-mm-dd' Compress,
return_ringing_store_num INTEGER Compress (808,828,867),
return_global_tran_id BIGINT ,
return_line_item_seq_num SMALLINT ,
return_qty INTEGER Compress (1) ,
return_amt DECIMAL(12,2) Compress,
return_usd_amt DECIMAL(12,2) Compress,
return_employee_disc_amt DECIMAL(12,2) Compress 0.00,
return_employee_disc_usd_amt DECIMAL(12,2) Compress 0.00,
xborder_retn_ind  VARCHAR(10) CHARACTER SET Unicode NOT CaseSpecific Compress ('N', 'Y'),
return_mapped_to_origin INTEGER Compress (0,1,2),
days_to_return  INTEGER Compress(1,2,3,4,5,6,7,8,9,10,11,12,13,14),
dw_sys_load_tmstp  TIMESTAMP(6),
days_to_return_act INTEGER Compress(1,2,3,4,5,6,7,8,9,10,11,12,13,14),
nopr_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
not_picked_up_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
sa_source_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress, 
promo_tran_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress,
source_store_num INTEGER Compress,
marketplace_ind CHAR(1) CHARACTER SET Unicode NOT CaseSpecific Compress) 
PRIMARY INDEX(business_day_date ,global_tran_id ,line_item_seq_num ,
return_date ,return_global_tran_id ,return_line_item_seq_num)
PARTITION BY Range_N(business_day_date  BETWEEN DATE '2017-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' DAY , UNKNOWN)
 ON COMMIT PRESERVE ROWS;

locking {sales_returns_t2_schema}.retail_tran_price_type_fact for access
insert into sales_and_returns_stg
select
sdtl.business_day_date,
sdtl.order_date,
sdtl.tran_date,
sdtl.business_unit_desc,
sdtl.acp_id ,
sdtl.global_tran_id,
sdtl.line_item_seq_num,
sdtl.order_num,
ord.order_line_id,
case when dept.transaction_type is not null
then dept.transaction_type else 'services' end as transaction_type,
sdtl.upc_num,
coalesce(sdtl.sku_num, tsku.rms_sku_num) as sku_num,
sdtl.line_item_quantity as shipped_qty,
sdtl.line_net_amt as shipped_sales,
sdtl.line_net_usd_amt as shipped_usd_sales,
sdtl.employee_discount_amt,
case
when sdtl.employee_discount_amt <> 0
then 1 else 0 end as has_employee_discount,
sdtl.employee_discount_usd_amt,
sdtl.line_item_order_type,
sdtl.line_item_fulfillment_type,
sdtl.intent_store_num,
sdtl.ringing_store_num,
pt.price_type,
pt.regular_price_amt,
pt.compare_price_amt as transaction_price_amt,
case when sdtl.price_adj_code in ('a', 'b') then upper('y') else upper('n') end as sale_price_adj,
case when dept.dept_num is not null then dept.dept_num else sdtl.merch_dept_num end as merch_dept_num,
dept.division_num,
sdtl.nonmerch_fee_code,
sdtl.item_source,
sdtl.commission_slsprsn_num,
case when sdtl.item_source = 'sb_salesempinit'
then sdtl.commission_slsprsn_num else null end
as ps_employee_id,
rsell.remote_sell_employee_id,
hr.payroll_department,
hr.hr_es_ind,
ord.shopper_id,
ord.source_platform_code,
ord.promise_type_code,
ord.requested_level_of_service_code,
ord.delivery_method_code,
ord.destination_node_num,
ord.curbside_pickup_ind,
ord.bill_zip_code,
ord.destination_zip_code,
rdtl.business_day_date as return_date,
rdtl.original_business_date,
rdtl.ringing_store_num as return_ringing_store_num,
rdtl.global_tran_id as return_global_tran_id,
rdtl.line_item_seq_num as return_line_item_seq_num,
rdtl.line_item_quantity as return_quantity,
case when rdtl.line_net_amt is not null then rdtl.line_net_amt * -1 end as return_amt,
case when rdtl.line_net_usd_amt is not null then rdtl.line_net_usd_amt * -1 end as return_usd_amt,
rdtl.employee_discount_amt as return_employee_disc_amt,
rdtl.employee_discount_usd_amt as return_employee_disc_usd_amt,
coalesce(rdtl.xborder_retn_ind,UPPER('N')),
1 as return_mapped_to_origin,
(rdtl.business_day_date - sdtl.business_day_date) as days_to_return,
CAST(CAST(CURRENT_TIMESTAMP AS  VARCHAR(19)) AS TIMESTAMP(6))as dw_sys_load_tmstp,
(rdtl.business_day_date - 
coalesce(ord.carrier_delivered_date, ord.picked_up_by_customer_date, sdtl.tran_date)) as days_to_return_act, 
coalesce(ord.nopr_ind, upper('N')),
coalesce(ord.not_picked_up_ind,upper('N')),
coalesce(sdtl.sa_source_ind, rdtl.sa_source_ind, upper('N')) as sa_source_ind,
coalesce(pt.promo_tran_ind,upper('N')),
ord.source_store_num,
case when sdtl.ownership_model = 'ECONCESSION' then upper('y') else NULL end as marketplace_ind 
from
sale_retn_xref   xref
join
sar_sales_extract  sdtl
on xref.sale_business_day_date = sdtl.business_day_date
and xref.sale_global_tran_id = sdtl.global_tran_id
and xref.sale_line_item_seq_num = sdtl.line_item_seq_num
join
sar_returns_extract  rdtl
on xref.retn_business_day_date = rdtl.business_day_date
and xref.retn_global_tran_id = rdtl.global_tran_id
and xref.retn_line_item_seq_num = rdtl.line_item_seq_num
left join
tran_dept  dept
on sdtl.business_day_date = dept.business_day_date
and sdtl.global_tran_id   = dept.global_tran_id
and sdtl.line_item_seq_num = dept.line_item_seq_num
left join
tran_order  ord
on sdtl.business_day_date = ord.business_day_date
and sdtl.global_tran_id   = ord.global_tran_id
and sdtl.line_item_seq_num = ord.line_item_seq_num
left join
tran_hr hr
on sdtl.business_day_date = hr.business_day_date
and sdtl.global_tran_id   = hr.global_tran_id
and sdtl.line_item_seq_num = hr.line_item_seq_num
left join
{sales_returns_t2_schema}.retail_tran_price_type_fact  pt
on sdtl.business_day_date = pt.business_day_date
and sdtl.global_tran_id   = pt.global_tran_id
and sdtl.line_item_seq_num = pt.line_item_seq_num
left join
tran_rsell  rsell
on sdtl.business_day_date = rsell.business_day_date
and sdtl.global_tran_id   = rsell.global_tran_id
and sdtl.line_item_seq_num = rsell.line_item_seq_num
left join
tran_sku_xref  tsku
on sdtl.business_day_date = tsku.business_day_date
and sdtl.global_tran_id   = tsku.global_tran_id
and sdtl.line_item_seq_num = tsku.line_item_seq_num 
where xref.sale_business_day_date <> '2027-12-31'
and xref.retn_business_day_date <> '2027-12-31'

 -----
union all
   -----
 select
sdtl.business_day_date,
sdtl.order_date,
sdtl.tran_date,
sdtl.business_unit_desc,
sdtl.acp_id ,
sdtl.global_tran_id,
sdtl.line_item_seq_num,
sdtl.order_num,
ord.order_line_id,
case when dept.transaction_type is not null
then dept.transaction_type else 'services' end as transaction_type,
sdtl.upc_num,
coalesce(sdtl.sku_num, tsku.rms_sku_num) as sku_num,
sdtl.line_item_quantity as shipped_qty,
sdtl.line_net_amt as shipped_sales,
sdtl.line_net_usd_amt as shipped_usd_sales,
sdtl.employee_discount_amt,
case
when sdtl.employee_discount_amt <> 0
then 1 else 0 end as has_employee_discount,
sdtl.employee_discount_usd_amt,
sdtl.line_item_order_type,
sdtl.line_item_fulfillment_type,
sdtl.intent_store_num,
sdtl.ringing_store_num,
pt.price_type,
pt.regular_price_amt,
pt.compare_price_amt as transaction_price_amt,
case when sdtl.price_adj_code in ('a', 'b') then upper('y') else upper('n') end as sale_price_adj,
case when dept.dept_num is not null then dept.dept_num else sdtl.merch_dept_num end as merch_dept_num,
dept.division_num,
sdtl.nonmerch_fee_code,
sdtl.item_source,
sdtl.commission_slsprsn_num,
case when sdtl.item_source = 'sb_salesempinit'
then sdtl.commission_slsprsn_num else null end
as ps_employee_id,
rsell.remote_sell_employee_id,
hr.payroll_department,
hr.hr_es_ind,
ord.shopper_id,
ord.source_platform_code,
ord.promise_type_code,
ord.requested_level_of_service_code,
ord.delivery_method_code,
ord.destination_node_num,
ord.curbside_pickup_ind,
ord.bill_zip_code,
ord.destination_zip_code,
cast(null as date) as return_date,
cast(null as date) as original_business_date,
null as  return_ringing_store_num,
null as return_global_tran_id,
null as  return_line_item_seq_num,
null as  return_quantity,
null as  return_amt,
 null  as return_usd_amt,
null as return_employee_disc_amt,
null as return_employee_disc_usd_amt,
upper('N') as xborder_retn_ind,
0 as return_mapped_to_origin,
cast(null as integer)  as days_to_return,
CAST(CAST(CURRENT_TIMESTAMP AS  VARCHAR(19)) AS TIMESTAMP(6))as dw_sys_load_tmstp,
null as days_to_return_act, 
upper('N')as nopr_ind, 
upper('N') as not_picked_up_ind, 
coalesce ( sdtl.sa_source_ind,upper('N')), 
coalesce(pt.promo_tran_ind,upper('N')),
ord.source_store_num,
case when sdtl.ownership_model = 'ECONCESSION' then upper('y') else NULL end as marketplace_ind 
from
sale_retn_xref   xref
join
sar_sales_extract  sdtl
on xref.sale_business_day_date = sdtl.business_day_date
and xref.sale_global_tran_id = sdtl.global_tran_id
and xref.sale_line_item_seq_num = sdtl.line_item_seq_num
left join
tran_dept  dept
on sdtl.business_day_date = dept.business_day_date
and sdtl.global_tran_id   = dept.global_tran_id
and sdtl.line_item_seq_num = dept.line_item_seq_num
left join
tran_order  ord
on sdtl.business_day_date = ord.business_day_date
and sdtl.global_tran_id   = ord.global_tran_id
and sdtl.line_item_seq_num = ord.line_item_seq_num
left join
tran_hr hr
on sdtl.business_day_date = hr.business_day_date
and sdtl.global_tran_id   = hr.global_tran_id
and sdtl.line_item_seq_num = hr.line_item_seq_num
left join
{sales_returns_t2_schema}.retail_tran_price_type_fact  pt
on sdtl.business_day_date = pt.business_day_date
and sdtl.global_tran_id   = pt.global_tran_id
and sdtl.line_item_seq_num = pt.line_item_seq_num
left join
tran_rsell  rsell
on sdtl.business_day_date = rsell.business_day_date
and sdtl.global_tran_id   = rsell.global_tran_id
and sdtl.line_item_seq_num = rsell.line_item_seq_num
left join
tran_sku_xref  tsku
on sdtl.business_day_date = tsku.business_day_date
and sdtl.global_tran_id   = tsku.global_tran_id
and sdtl.line_item_seq_num = tsku.line_item_seq_num 
where xref.sale_business_day_date <>  '2027-12-31'
and  xref.retn_business_day_date = '2027-12-31'
  ----
 union all
 -------
 select
cast(null as date) as business_day_date,
rdtl.order_date,
rdtl.tran_date,
rdtl.business_unit_desc,
rdtl.acp_id ,
null as global_tran_id,
null as line_item_seq_num,
rdtl.order_num,
ord.order_line_id,
case when dept.transaction_type is not null
then dept.transaction_type else 'services' end as transaction_type,
rdtl.upc_num,
coalesce(rdtl.sku_num, tsku.rms_sku_num) as sku_num,
null as   shipped_qty,
null as  shipped_sales,
null as shipped_usd_sales,
null as employee_discount_amt,
case
when rdtl.employee_discount_amt <> 0
then 1 else 0 end as has_employee_discount,
null as employee_discount_usd_amt,
rdtl.line_item_order_type,
rdtl.line_item_fulfillment_type,
rdtl.intent_store_num,
null ringing_store_num,
pt.price_type,
pt.regular_price_amt,
pt.compare_price_amt as transaction_price_amt,
cast(null as varchar(2)) as sale_price_adj,
case when dept.dept_num is not null then dept.dept_num else rdtl.merch_dept_num end as merch_dept_num,
dept.division_num,
rdtl.nonmerch_fee_code,
rdtl.item_source,
rdtl.commission_slsprsn_num,
case when rdtl.item_source = 'sb_salesempinit'
then rdtl.commission_slsprsn_num else null end
as ps_employee_id,
rsell.remote_sell_employee_id,
hr.payroll_department,
hr.hr_es_ind,
ord.shopper_id,
ord.source_platform_code,
ord.promise_type_code,
ord.requested_level_of_service_code,
ord.delivery_method_code,
ord.destination_node_num,
ord.curbside_pickup_ind,
ord.bill_zip_code,
ord.destination_zip_code,
rdtl.business_day_date as return_date,
rdtl.original_business_date,
rdtl.ringing_store_num as return_ringing_store_num,
rdtl.global_tran_id as return_global_tran_id,
rdtl.line_item_seq_num as return_line_item_seq_num,
rdtl.line_item_quantity as return_quantity,
case when rdtl.line_net_amt is not null then rdtl.line_net_amt * -1 end as return_amt,
case when rdtl.line_net_usd_amt is not null then rdtl.line_net_usd_amt * -1 end as return_usd_amt,
rdtl.employee_discount_amt as return_employee_disc_amt,
rdtl.employee_discount_usd_amt as return_employee_disc_usd_amt,
coalesce(rdtl.xborder_retn_ind,upper('N')),
0 as return_mapped_to_origin,
cast(null as integer) as days_to_return,
CAST(CAST(CURRENT_TIMESTAMP AS  VARCHAR(19)) AS TIMESTAMP(6))as dw_sys_load_tmstp,
null as days_to_return_act, 
coalesce(ord.nopr_ind,upper('N')),
coalesce(ord.not_picked_up_ind,upper('N')),
coalesce(rdtl.sa_source_ind,upper('N')),
coalesce(pt.promo_tran_ind,upper('N')),
ord.source_store_num,
case when rdtl.ownership_model = 'ECONCESSION' then upper('y') else NULL end as marketplace_ind 
from
RETN_NO_MATCH   xref    
join
sar_returns_extract  rdtl
on xref.retn_business_day_date = rdtl.business_day_date
and xref.retn_global_tran_id = rdtl.global_tran_id
and xref.retn_line_item_seq_num = rdtl.line_item_seq_num
left join
tran_dept  dept
on rdtl.business_day_date = dept.business_day_date
and rdtl.global_tran_id   = dept.global_tran_id
and rdtl.line_item_seq_num = dept.line_item_seq_num
left join
tran_order  ord
on rdtl.business_day_date = ord.business_day_date
and rdtl.global_tran_id   = ord.global_tran_id
and rdtl.line_item_seq_num = ord.line_item_seq_num
left join
tran_hr hr
on rdtl.business_day_date = hr.business_day_date
and rdtl.global_tran_id   = hr.global_tran_id
and rdtl.line_item_seq_num = hr.line_item_seq_num
left join
{sales_returns_t2_schema}.retail_tran_price_type_fact  pt
on rdtl.business_day_date = pt.business_day_date
and rdtl.global_tran_id   = pt.global_tran_id
and rdtl.line_item_seq_num = pt.line_item_seq_num
left join
tran_rsell  rsell
on rdtl.business_day_date = rsell.business_day_date
and rdtl.global_tran_id   = rsell.global_tran_id
and rdtl.line_item_seq_num = rsell.line_item_seq_num
left join
tran_sku_xref  tsku
on rdtl.business_day_date = tsku.business_day_date
and rdtl.global_tran_id   = tsku.global_tran_id
and rdtl.line_item_seq_num = tsku.line_item_seq_num;

  -------------------------------------------------------------
  
collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num, 
return_date, return_global_tran_id, return_line_item_seq_num) 
on
sales_and_returns_stg;

 -------------------------------------------------------------
delete from {sales_returns_t2_schema}.SALES_AND_RETURNS_FACT_BASE 
where 1=1 
and (business_day_date >= (select start_date from sarf_start_date)
or ( return_date >= (select start_date from sarf_start_date) AND  business_day_date is null))
and exists
(select 1 from sarf_start_date where delete_range = 'incremental'); 

delete from {sales_returns_t2_schema}.SALES_AND_RETURNS_FACT_BASE sarf
where exists
(select 1 from sarf_start_date where delete_range = 'backfill')
and exists
(select 1
from
delta_deletes delta
where 1=1 
and coalesce(sarf.business_day_date, date'2027-12-31') = delta.business_day_date
and coalesce(sarf.global_tran_id, 0) = delta.global_tran_id
and coalesce(sarf.line_item_seq_num, 0) = delta.line_item_seq_num
and coalesce(sarf.return_date, date'2027-12-31') = delta.return_date
and coalesce(sarf.return_global_tran_id, 0) = delta.return_global_tran_id
and coalesce(sarf.return_line_item_seq_num, 0) = delta.return_line_item_seq_num);

COLLECT STATISTICS
column (partition),
COLUMN (business_day_date),
COLUMN ( return_date) 
ON  
{sales_returns_t2_schema}.SALES_AND_RETURNS_FACT_BASE;


insert into {sales_returns_t2_schema}.SALES_AND_RETURNS_FACT_BASE
select *
from
sales_and_returns_stg
WHERE BUSINESS_DAY_DATE < CURRENT_DATE
OR BUSINESS_DAY_DATE IS NULL; 


COLLECT STATISTICS
column (partition),
COLUMN ( return_date)   
ON
{sales_returns_t2_schema}.SALES_AND_RETURNS_FACT_BASE;


SET QUERY_BAND = NONE FOR SESSION;


 
