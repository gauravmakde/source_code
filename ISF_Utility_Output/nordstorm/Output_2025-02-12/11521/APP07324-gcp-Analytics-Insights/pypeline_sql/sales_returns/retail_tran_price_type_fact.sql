 

SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=sales_and_returns_fact_base_11521_ACE_ENG;
     Task_Name=retail_tran_price_type_fact;'
     FOR SESSION VOLATILE;

/* 
Created by: Lance Christenson
Created on: 01/2022  

Daily refresh of T2DL_DAS_SALES_RETURNS.RETAIL_TRAN_PRICE_TYPE_FACT
which provide identification of price type (R,C,P)
for the merchandising transactions in NAP
retail_tran_detail_fact_vw

updated 04/2022 to add regular_price_amt
update 01/2023 to include incremental updating during week, full refresh week end
update 09/2023 to synchronize with clarity price-type rules
update 04/2024 to include Marketplace

*/

/*set date range as full or incremental
based on day of week (which is variable to allow
manual full loads any day of the week)*/ 

create multiset volatile table pt_start_date as 
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

/*
---------------------------------------------------
Step 1b: Build table for finding the as-is department
assignment for a upc/sku, and identify merchandising
departments (for filtering out non-merch such as
leased-boutiques and restaurant)
---------------------------------------------------
*/
create multiset volatile table  merch_upc_sku_dept
(upc_num varchar(32) character set unicode not casespecific,
rms_sku_num  varchar(32) character set unicode not casespecific,
merch_dept_ind char(1) character set unicode not casespecific not null compress ('n','y')
) primary index (upc_num)
partition by upc_num mod 65535 + 1
on commit preserve rows;

insert into  merch_upc_sku_dept
select
upc.upc_num,
sku.rms_sku_num,
dpt.merch_dept_ind
from
(select upc_num, rms_sku_num
from
prd_nap_usr_vws.product_upc_dim
qualify row_number() over
(partition by trim(leading '0' from  upc_num)  order by prmy_upc_ind desc, channel_country desc)  = 1) upc
join
(select  rms_sku_num, dept_num
from
prd_nap_usr_vws.product_sku_dim_hist
qualify row_number() over
(partition by rms_sku_num  order by channel_country desc)  = 1) sku
on upc.rms_sku_num = sku.rms_sku_num
join
prd_nap_usr_vws.department_dim  dpt
on sku.dept_num = dpt.dept_num;


collect statistics
column (partition),
column ( upc_num)
on
merch_upc_sku_dept;

/*
---------------------------------------------------
Step 2: Extract SALES from retail_tran_detail_fact_vw
the fields needed to derive the price type for the
transaction
---------------------------------------------------
*/
 
create multiset volatile table  pt_tran_extract_stg,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
line_item_activity_type_code CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL COMPRESS ('N','R','S'),
intent_store_num varchar(16) character set unicode not casespecific compress,
tran_date date format 'yyyy-mm-dd',
tran_time TIMESTAMP(6) WITH TIME ZONE,
original_business_date  date format 'yyyy-mm-dd',
line_net_amt decimal(12,2),
line_item_regular_price decimal(12,2),
employee_discount_amt decimal(12,2) compress 0.00 ,
line_item_promo_id varchar(16) character set unicode not casespecific compress,
line_item_promo_amt decimal(12,2) compress 0.00,
upc_num varchar(32) character set unicode not casespecific 
)  primary index (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day)
on commit preserve rows;

insert into pt_tran_extract_stg 
select
business_day_date,
global_tran_id,
line_item_seq_num,
line_item_activity_type_code,
intent_store_num,
tran_date,
tran_time,
original_business_date,
line_net_amt,
line_item_regular_price,
employee_discount_amt,
line_item_promo_id,
line_item_promo_amt,
upc_num 
from
prd_nap_usr_vws.retail_tran_detail_fact_vw
where COALESCE(line_item_activity_type_code,'X') in ('s','r')
and upc_num is not null
and COALESCE(line_item_merch_nonmerch_ind,'X') = 'merch'
and COALESCE(price_adj_code,'N') not in ('a','b')
and business_day_date >= (select start_date from pt_start_date);

collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num)
on
pt_tran_extract_stg;

 
create multiset volatile table  pt_tran_extract,
no before journal, no after  journal,
checksum =default
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint,
tran_date date format 'yyyy-mm-dd',
compare_date date format 'yyyy-mm-dd',
compare_time TIMESTAMP(6) WITH TIME ZONE, 
price_store_num smallint,
rms_sku_num varchar(10) character set unicode not casespecific,
line_net_amt decimal(12,2),
line_item_regular_price decimal(12,2),
employee_discount_amt decimal(12,2) compress 0.00 ,
compare_price_amt decimal(12,2),
line_item_promo_id varchar(16) character set unicode not casespecific compress,
line_item_promo_amt decimal(12,2) compress 0.00  
)  primary index (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day)
on commit preserve rows;


insert into pt_tran_extract 
select
rtdf.business_day_date,
rtdf.global_tran_id,
rtdf.line_item_seq_num,
rtdf.tran_date,
case
when line_item_activity_type_code = 's'
then coalesce(rtdf.tran_date, rtdf.business_day_date, date'2021-01-01')
else coalesce(rtdf.original_business_date, rtdf.tran_date, date'2021-01-01')
end as compare_date,
case
when line_item_activity_type_code = 's'
then coalesce(rtdf.tran_time, CAST(rtdf.business_day_date AS TIMESTAMP(6) WITH TIME ZONE))   
else coalesce(CAST(rtdf.original_business_date AS TIMESTAMP(6)WITH TIME ZONE) ,
cast(rtdf.business_day_date AS TIMESTAMP(6)WITH TIME ZONE) )  
end as compare_time,
psd.price_store_num,
upc.rms_sku_num,
abs(rtdf.line_net_amt) line_net_amt,
abs(rtdf.line_item_regular_price) line_item_regular_price,
abs(rtdf.employee_discount_amt) employee_discount_amt,
case
when
abs((abs(rtdf.line_net_amt) + abs(employee_discount_amt) + abs(line_item_promo_amt)) - abs(rtdf.line_item_regular_price)) = .01
then
case
when ( abs(rtdf.line_net_amt) + abs(employee_discount_amt) - .01) = -.01 then .00
else ( abs(rtdf.line_net_amt) + abs(employee_discount_amt) - .01) end
else (abs(rtdf.line_net_amt) + abs(employee_discount_amt) ) end
as compare_price_amt,
rtdf.line_item_promo_id,
abs(rtdf.line_item_promo_amt) as line_item_promo_amt 
 from
pt_tran_extract_stg  rtdf
join
prd_nap_usr_vws.price_store_dim_vw  psd
on rtdf.intent_store_num = psd.store_num
and psd.business_unit_num in (1000,2000,5000,5800,6000,6500,9000,9500)
join
merch_upc_sku_dept  upc
on rtdf.upc_num = upc.upc_num
and upc.merch_dept_ind = 'y';

collect statistics
column (partition),
column ( price_store_num, rms_sku_num)
on
pt_tran_extract;

/*
---------------------------------------------------
Step 3: Build look-up sku/loc/price/date-range
lookup tables.  Include lag to get previous
row (most recent previous row) to catch off-price
transactions honored after end of price range
for pre-2021 include a MADM price_extract
---------------------------------------------------
*/
create multiset volatile table  product_price_stg,
no before journal, no after  journal,
checksum =default
(store_num integer,
rms_sku_num varchar(32) character set unicode not casespecific,
ownership_retail_price_type_code varchar(20) character set unicode not casespecific compress ('REGULAR','CLEARANCE','PROMOTION'),
selling_retail_price_type_code varchar(20) character set unicode not casespecific compress ('REGULAR','CLEARANCE','PROMOTION'),
selling_retail_price_amt decimal(15,2), 
regular_price_amt decimal(15,2),
eff_begin_tmstp timestamp(6) with time zone,
eff_end_tmstp timestamp(6) with time zone 
)  primary index (store_num, rms_sku_num, eff_begin_tmstp, eff_end_tmstp)
 partition by range_n(eff_begin_tmstp BETWEEN
 TIMESTAMP '0001-01-01 00:00:00.000000+00:00' AND TIMESTAMP '9999-12-31 23:23:59.999999+00:00' EACH INTERVAL '1' DAY)
on commit preserve rows;
 
 insert into product_price_stg
with normdates as
 (select normalize on meets or overlaps  
    store_num,
    rms_sku_num,
    ownership_retail_price_type_code,
    selling_retail_price_type_code,
    selling_retail_price_amt,
    regular_price_amt,
    period(eff_begin_tmstp, eff_end_tmstp) as norm_period 
from prd_nap_usr_vws.product_price_timeline_dim)
select   
normdates.store_num, normdates.rms_sku_num, normdates.ownership_retail_price_type_code,
normdates.selling_retail_price_type_code, normdates.selling_retail_price_amt,  regular_price_amt, 
    begin(normdates.norm_period) as eff_begin_tmstp,
    end(normdates.norm_period) as eff_end_tmstp 
    from normdates;
  
collect statistics
column (partition),
column ( store_num,rms_sku_num,eff_begin_tmstp,eff_end_tmstp),
column (selling_retail_price_amt)   
on
product_price_stg;

create multiset volatile table  product_price_extract,
no before journal, no after  journal,
checksum =default
(store_num integer,
rms_sku_num varchar(32) character set unicode not casespecific,
ownership_retail_price_type_code varchar(20) character set unicode not casespecific compress ('REGULAR','CLEARANCE','PROMOTION'),
selling_retail_price_type_code varchar(20) character set unicode not casespecific compress ('REGULAR','CLEARANCE','PROMOTION'),
selling_retail_price_amt decimal(15,2), 
regular_price_amt decimal(15,2), 
eff_begin_tmstp timestamp(6) with time zone,
eff_end_tmstp timestamp(6) with time zone,
prev_selling_retail_price_amt decimal(15,2), 
prev_ownership_retail_price_type_code varchar(20) 
character set unicode not casespecific compress ('REGULAR','CLEARANCE','PROMOTION'),
prev_selling_retail_price_type_code varchar(20) 
character set unicode not casespecific compress ('REGULAR','CLEARANCE','PROMOTION')
)  primary index (store_num, rms_sku_num, eff_begin_tmstp, eff_end_tmstp)
 partition by range_n(eff_begin_tmstp BETWEEN
 TIMESTAMP '0001-01-01 00:00:00.000000+00:00' AND TIMESTAMP '9999-12-31 23:23:59.999999+00:00' EACH INTERVAL '1' DAY)
on commit preserve rows;

insert into product_price_extract
select 
store_num,
    rms_sku_num,
    ownership_retail_price_type_code,
    selling_retail_price_type_code,
    selling_retail_price_amt,
    regular_price_amt,
    eff_begin_tmstp,
    eff_end_tmstp,
    lag(selling_retail_price_amt,1) over (partition by  rms_sku_num, store_num order by eff_begin_tmstp) 
    as prev_selling_retail_price_amt,
	lag(ownership_retail_price_type_code,1) over (partition by  rms_sku_num, store_num order by eff_begin_tmstp) 
    as prev_ownership_retail_price_type_code,
    lag(selling_retail_price_type_code,1) over (partition by  rms_sku_num, store_num order by eff_begin_tmstp) 
    as prev_selling_retail_price_type_code
from
product_price_stg;

collect statistics
column (partition),
column ( store_num,rms_sku_num,eff_begin_tmstp,eff_end_tmstp),
column (selling_retail_price_amt)   
on
product_price_extract;

create multiset volatile table  madm_clr_promo,
no before journal, no after  journal,
checksum =default
(loc_idnt varchar(10) character set unicode not casespecific,
sku_idnt varchar(32) character set unicode not casespecific,
unit_rtl_amt decimal(15,2),
selling_retail_price_type_code  varchar(32) character set unicode not casespecific,
start_dt date format 'yyyy-mm-dd',
end_dt date format 'yyyy-mm-dd'   
)  primary index (loc_idnt, sku_idnt)
on commit preserve rows;
 
insert into madm_clr_promo 
select
loc_idnt, sku_idnt, 
trunc( unit_rtl_amt,2),  
case 
when clearance_soh_ind = 'Y' then 'CLEARANCE' 
when price_typ_cd = 'CL' then 'CLEARANCE'
else 'PROMOTION' end as selling_retail_price_type_code,
start_dt,  
end_dt  
from
t2dl_das_sales_returns.madm_ss_price_typ_sku_loc_ld
qualify (row_number()over(partition by sku_idnt, loc_idnt, trunc( unit_rtl_amt,2)
order by start_dt desc) = 1 
and end_dt = '4444-12-12')
or end_dt <> '4444-12-12';

collect statistics
column ( loc_idnt, sku_idnt),
column (unit_rtl_amt)   
on
madm_clr_promo;

--RMS (MADM source) may skip updaing some locs; since price consistent by brand, create lookup for that
create multiset volatile table  madm_clr_promo_chnl_price,
no before journal, no after  journal,
checksum =default
(price_channel varchar(10) character set unicode not casespecific,  
sku_idnt varchar(32) character set unicode not casespecific,
unit_rtl_amt decimal(15,2),
selling_retail_price_type_code  varchar(32) character set unicode not casespecific compress ('REGULAR','CLEARANCE','PROMOTION') 
)  primary index (price_channel, sku_idnt, unit_rtl_amt)
on commit preserve rows;
 
insert into madm_clr_promo_chnl_price
select
case 
when loc_idnt in ('1','808') then 'USFP'
when loc_idnt in ('338','828') then 'USOP'
when loc_idnt in ('835','867') then 'CAFP'
when loc_idnt in ('844') then 'CAOP'
ELSE 'ERROR' END AS PRICE_CHANNEL,
sku_idnt,  trunc( unit_rtl_amt,2),  
case 
when clearance_soh_ind = 'Y' then 'CLEARANCE' 
when price_typ_cd = 'CL' then 'CLEARANCE'
else 'PROMOTION' end as selling_retail_price_type_code  
from
t2dl_das_sales_returns.madm_ss_price_typ_sku_loc_ld 
qualify (row_number()over(partition by PRICE_CHANNEL, SKU_idnt, trunc( unit_rtl_amt,2)
order by start_dt desc) = 1); 
 
collect statistics
column ( PRICE_CHANNEL, SKU_idnt, unit_rtl_amt) 
ON
madm_clr_promo_chnl_price;

/*
---------------------------------------------------
Step 4: Use the sku/price/date-range tables from
step 3 to find the price type for the SALES
transactions extracted in step 2
---------------------------------------------------
*/

 locking {sales_returns_t2_schema}.product_reg_price_dim for access 
 create multiset volatile table  price_type_extract 
    as
(select trans.business_day_date, trans.global_tran_id, trans.line_item_seq_num, trans.rms_sku_num,
trans.price_store_num, trans.compare_price_amt, regd.regular_price_amt, trans.compare_time,
case
when trans.line_item_promo_id is not null
and trans.line_item_promo_amt <> 0 
then upper('Y') 
else null end as promo_tran_ind,
case  
when price1.ownership_retail_price_type_code is not null then 
    case
    when price1.ownership_retail_price_type_code = 'CLEARANCE' then '1-CLEARANCE'
    when price1.selling_retail_price_type_code = 'REGULAR' then '2-REGULAR'
    when price1.selling_retail_price_type_code = 'PROMOTION' then '2-PROMOTION'
    else '2'||price1.selling_retail_price_type_code end
 when trans.line_item_promo_id is not null and trans.line_item_promo_amt > 0 then '3-PROMOTION'
 when price1r.regular_price_amt is not null then '4-REGULAR' 
 when price2.prev_ownership_retail_price_type_code is not null  then
   case
   when price2.prev_ownership_retail_price_type_code = 'CLEARANCE' then '5-CLEARANCE'
   when price2.prev_selling_retail_price_type_code = 'REGULAR' then '5-REGULAR'
   when price2.prev_selling_retail_price_type_code = 'PROMOTION' then '5-PROMOTION'
  else '5 '||price2.prev_selling_retail_price_type_code end 
when madmsc.selling_retail_price_type_code is not null then
   case 
   when madmsc.selling_retail_price_type_code   = 'PROMOTION' then  '6-PROMOTION'
   when madmsc.selling_retail_price_type_code   = 'CLEARANCE' then  '6-CLEARANCE'
   else '6 '||madmsc.selling_retail_price_type_code end  
 when madmcb.selling_retail_price_type_code is not null then
   case 
   when madmcb.selling_retail_price_type_code   = 'PROMOTION' then  '7-PROMOTION'
   when madmcb.selling_retail_price_type_code   = 'CLEARANCE' then  '7-CLEARANCE'
   else '7 '||madmcb.selling_retail_price_type_code end 
else '8-REGULAR' end as price_type_and_source
 --
from pt_tran_extract  trans
left join 
 --check for date and price match
product_price_extract  price1   
ON price1.rms_sku_num = trans.rms_sku_num
AND price1.store_num = trans.price_store_num
and price1.selling_retail_price_amt = trans.compare_price_amt
AND  trans.compare_time   >= price1.eff_begin_tmstp
AND  trans.compare_time   <  price1.eff_end_tmstp 
left join 
 --check for regular price (non-loyalty example)
product_price_extract  price1r   
ON price1.selling_retail_price_type_code is null
and (trans.line_item_promo_id is null or trans.line_item_promo_amt is null)
and price1r .rms_sku_num = trans.rms_sku_num
AND price1r.store_num = trans.price_store_num
and price1r.regular_price_amt = trans.compare_price_amt
AND  trans.compare_time   >= price1r.eff_begin_tmstp
AND  trans.compare_time   <  price1r.eff_end_tmstp 
 left join      
 -- check for latest previous price only match
product_price_extract   price2
ON price1.selling_retail_price_type_code is null
and (trans.line_item_promo_id is null or trans.line_item_promo_amt is null)
and price1r.selling_retail_price_type_code is null
and price2.rms_sku_num = trans.rms_sku_num
AND price2.store_num = trans.price_store_num
AND  trans.compare_time   >= price2.eff_begin_tmstp
AND  trans.compare_time   <  price2.eff_end_tmstp 
AND  trans.compare_price_amt  = price2.prev_selling_retail_price_amt
left join      
 -- check for pre-May 2021 MADM match loc/sku/date-range/price
madm_clr_promo  madmsc
ON price1.selling_retail_price_type_code is null
and (trans.line_item_promo_id is null or trans.line_item_promo_amt is null)
and price1r.selling_retail_price_type_code is null
and price2.selling_retail_price_type_code is null
and madmsc.sku_idnt = trans.rms_sku_num
and madmsc.loc_idnt = trans.price_store_num
and  trans.tran_date   between madmsc.start_dt and madmsc.end_dt
AND  trans.compare_price_amt  = madmsc.unit_rtl_amt
left join 
 -- check for pre-May 2021 MADM match at sku/channel_brand level/price
madm_clr_promo_chnl_price  madmcb
ON price1.selling_retail_price_type_code is null
and (trans.line_item_promo_id is null or trans.line_item_promo_amt is null)
and price1r.selling_retail_price_type_code is null
and price2.selling_retail_price_type_code is null
and madmsc.selling_retail_price_type_code is null
and madmcb.sku_idnt = trans.rms_sku_num
and madmcb.PRICE_CHANNEL = 
case 
when trans.price_store_num IN (1,808) then 'USFP'
when trans.price_store_num IN (338,828) then 'USOP'
when trans.price_store_num in (835,867) then 'CAFP'
when trans.price_store_num in (844) then 'CAOP'
else 'X' end 
AND  trans.compare_price_amt  = madmcb.unit_rtl_amt
left join
{sales_returns_t2_schema}.product_reg_price_dim  regd
on trans.price_store_num = regd.price_store_num
and trans.rms_sku_num = regd.rms_sku_num
and trans.compare_date between regd.pricing_start_date and regd.pricing_end_date 
 ) with data primary index  (business_day_date, global_tran_id, line_item_seq_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day)
on commit preserve rows;

 
collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num)  
on
price_type_extract;
/*
---------------------------------------------------
Step 7: reload the retail_tran_price_type_fact table

---------------------------------------------------
*/
delete from {sales_returns_t2_schema}.RETAIL_TRAN_PRICE_TYPE_FACT 
where 1=1 
and (business_day_date >= (select start_date from pt_start_date)
and exists
(select 1 from pt_start_date where delete_range = 'incremental')); 

delete from {sales_returns_t2_schema}.RETAIL_TRAN_PRICE_TYPE_FACT 
where exists
(select 1 from pt_start_date where delete_range = 'backfill');

insert into {sales_returns_t2_schema}.RETAIL_TRAN_PRICE_TYPE_FACT --t2dl_das_sales_returns.retail_tran_price_type_fact
select
 ptx.business_day_date,
 ptx.global_tran_id,
 ptx.line_item_seq_num,
 ptx.regular_price_amt,
 SUBSTR(ptx.price_type_and_source,3,1) AS PRICE_TYPE,
 CURRENT_TIMESTAMP as dw_sys_load_tmstp,
 ptx.compare_price_amt,
 ptx.promo_tran_ind,
 SUBSTR(ptx.price_type_and_source,1,1) AS PRICE_TYPE_SOURCE 
 from
 price_type_extract ptx
 where ptx.business_day_date  < current_date(); 

collect statistics
column (partition),
column ( business_day_date, global_tran_id, line_item_seq_num)
on
{sales_returns_t2_schema}.RETAIL_TRAN_PRICE_TYPE_FACT;


SET QUERY_BAND = NONE FOR SESSION; 