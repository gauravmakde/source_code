

SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=sales_and_returns_fact_base_11521_ACE_ENG;
     Task_Name=product_reg_price_dim;'
     FOR SESSION VOLATILE;

/* 
T2/Table Name: product_reg_price_dim
Team/Owner: Lance Christenson 
Date Modified: 11/30/2022
*/

/* 
original version Apr 2022
Lance Christenson
updated Sep 2023 to use product_price_timeline_dim
instead of product_price_dim

refresh regular price table
with the sku regular prices and date ranges 
derived from product_price_timeline_dim
*/
-- t2dl_das_sales_returns.product_reg_price_dim

/* step 1
extract regular prices and start dates
(note PPD has the reg prices, but not always on a
separate row with eff_start/end; sometimes just in the regular_price_amt
field on a clearance row.
*/
create multiset volatile table product_price_reg_price_extract   as
(select  
ppd.rms_sku_num,
cast(trim(ppd.store_num) as integer) as price_store_num,
CAST(ppd.ASOF_TMSTP AS DATE) as pricing_start_date,  
ppd.regular_price_amt
from
prd_nap_usr_vws.product_price_timeline_dim  ppd
where regular_price_amt is not null
qualify row_number() over (partition by ppd.rms_sku_num, ppd.store_num,  pricing_start_date
order by ppd.regular_price_amt) = 1
) with data primary index(rms_sku_num, price_store_num, pricing_start_date)
on commit preserve rows;

collect statistics
column (rms_sku_num, price_store_num, pricing_start_date)               
on  
product_price_reg_price_extract;

/* step 2
Create start/end dates for the regular prices
*/
create multiset volatile table product_price_reg_price_dates  as
(select 
rpe.rms_sku_num,
rpe.price_store_num,
rpe.regular_price_amt,
 --
case 
when lag(rpe.rms_sku_num||rpe.price_store_num,1,cast('1999-01-01' as date)) ignore nulls   
over (
order by rpe.rms_sku_num, rpe.price_store_num, rpe.pricing_start_date) 
<> rpe.rms_sku_num||rpe.price_store_num 
then cast('1970-01-01' as date)
when lag(rpe.rms_sku_num||rpe.price_store_num||rpe.regular_price_amt,1,cast('1999-01-01' as date)) ignore nulls   
over (
order by rpe.rms_sku_num, rpe.price_store_num,rpe.pricing_start_date, rpe.regular_price_amt) 
<> rpe.rms_sku_num||rpe.price_store_num||rpe.regular_price_amt
then rpe.pricing_start_date 
else cast('1999-01-01' as date) end as pricing_start_date,
 --
case 
when lead(rpe.rms_sku_num||rpe.price_store_num,1,cast('2049-12-31' as date)) ignore nulls   
over (
order by rpe.rms_sku_num, rpe.price_store_num, rpe.pricing_start_date) 
<> rpe.rms_sku_num||rpe.price_store_num 
then cast('2049-12-31' as date)
when lead(rpe.rms_sku_num||rpe.price_store_num||rpe.regular_price_amt,1,cast('2049-12-31' as date)) ignore nulls   
over (
order by rpe.rms_sku_num, rpe.price_store_num,rpe.pricing_start_date, rpe.regular_price_amt) 
<> rpe.rms_sku_num||rpe.price_store_num||rpe.regular_price_amt
then lead(rpe.pricing_start_date,1,cast('2049-12-31' as date)) ignore nulls 
over (
order by rpe.rms_sku_num, rpe.price_store_num,rpe.pricing_start_date, rpe.regular_price_amt) - interval '1' day
else cast('1999-01-01' as date) end as pricing_end_date 
 --
 from
product_price_reg_price_extract  rpe
) with data primary index(rms_sku_num, price_store_num)
on commit preserve rows;

collect statistics
column (rms_sku_num, price_store_num, pricing_start_date, pricing_end_date)               
on  
product_price_reg_price_dates;

/* step 3
Create a 'normalized' set of date ranges
for regular prices (which may span dates for multiple
price types 'c' and 'p')
*/

delete from {sales_returns_t2_schema}.product_reg_price_dim;


insert into {sales_returns_t2_schema}.product_reg_price_dim  --t2dl_das_sales_returns.product_reg_price_dim
select
prc_beg.rms_sku_num,
prc_beg.price_store_num,
prc_beg.regular_price_amt,
prc_beg.pricing_start_date,
prc_end.pricing_end_date,
CURRENT_TIMESTAMP as dw_sys_load_tmstp
from
(select  
ppc.rms_sku_num,
ppc.price_store_num,
ppc.regular_price_amt,
ppc.pricing_start_date,
row_number() over 
(partition by ppc.rms_sku_num, ppc.price_store_num, ppc.regular_price_amt
order by ppc.rms_sku_num, ppc.price_store_num, ppc.regular_price_amt, ppc.pricing_start_date) beg_rn 
from
product_price_reg_price_dates  ppc
where ppc.pricing_start_date <> '1999-01-01') prc_beg
 --
join
 --
(select  
ppc.rms_sku_num,
ppc.price_store_num,
ppc.regular_price_amt,
ppc.pricing_end_date,
row_number() over 
(partition by ppc.rms_sku_num, ppc.price_store_num, ppc.regular_price_amt
order by ppc.rms_sku_num, ppc.price_store_num, ppc.regular_price_amt, ppc.pricing_end_date) end_rn 
from
product_price_reg_price_dates  ppc
where ppc.pricing_end_date <> '1999-01-01') prc_end
on prc_beg.rms_sku_num = prc_end.rms_sku_num
and prc_beg.price_store_num = prc_end.price_store_num
and prc_beg.regular_price_amt = prc_end.regular_price_amt
and prc_beg.beg_rn = prc_end.end_rn;

collect statistics
column ( partition),
column (rms_sku_num, price_store_num, pricing_start_date, pricing_end_date)               
on  
{sales_returns_t2_schema}.product_reg_price_dim; 


SET QUERY_BAND = NONE FOR SESSION; 