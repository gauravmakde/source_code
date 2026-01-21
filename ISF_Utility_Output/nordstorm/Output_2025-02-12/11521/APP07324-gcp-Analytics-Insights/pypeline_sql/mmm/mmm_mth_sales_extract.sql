SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=mmm_mth_sales_extract_11521_ACE_ENG;
     Task_Name=mmm_mth_sales_extract;'
     FOR SESSION VOLATILE;

/* intitial version mar 2022
lance christenson

delete/insert previous fiscal month 
mmm net sales totals by day,store,div
into t2dl_das_mmm.mmm_mth_sales_extract
Because a cron for JWN fiscal month end
is too complicated, just run this job weekly.
running weekly for summing last month data is OK as previous
month rows are deleted and reinserted each run

Team/Owner: AE
Date Created/Modified: 11/23/2022
modified Sep 2023 to replace MADM tables with
NAP tables*/


/* step 1
extract previous fiscal month beg/end dates
)
*/
 
create multiset volatile table last_month_date_range as
 (select 
month_idnt,
prev_month_idnt,
prev_month_start_day_date,
prev_month_end_day_date
from
prd_nap_usr_vws.DAY_CAL_454_DIM  currcal
join
(select distinct
 month_idnt as curr_month_idnt,
 lag(month_idnt) over (order by month_idnt) prev_month_idnt, 
 lag(month_start_day_date) over (order by month_idnt) prev_month_start_day_date,
 lag(month_end_day_date) over (order by month_idnt) prev_month_end_day_date
from prd_nap_usr_vws.DAY_CAL_454_DIM) prev_tbl
on currcal.month_idnt = prev_tbl.curr_month_idnt
and prev_tbl.prev_month_idnt < currcal.month_idnt
where current_date  = currcal.day_date
 ) with data primary index(prev_month_start_day_date, prev_month_end_day_date ) 
on commit preserve rows;

collect statistics
column ( prev_month_start_day_date, prev_month_end_day_date)                
on  
last_month_date_range;

 ----------------------------------------------------------------------

/* step 2
extract from retail_tran_detail_fact_vw
just the fields (for efficiency) needed by mmm
for the previous fiscal month  
*/

create multiset volatile table  mmm_tran_extract,
no before journal, no after  journal,
checksum =default 
(business_day_date  date format 'yyyy-mm-dd',
global_tran_id   bigint,
line_item_seq_num  smallint compress (1 ,2 ,3 ,4 ,5 ,6 ,7 ),
intent_store_num integer compress (808, 867, 828),
upc_num varchar(32) character set unicode not casespecific,
line_net_amt decimal(12,2),
line_item_quantity decimal(8,0) compress 1.,
month_idnt  integer
)  primary index (business_day_date, upc_num)
partition by range_n(business_day_date between date '2017-01-01' and date '2025-12-31' each interval '1' day)
on commit preserve rows;


insert into mmm_tran_extract
select
tdtl.business_day_date, 
tdtl.global_tran_id,
tdtl.line_item_seq_num,
tdtl.intent_store_num,   
tdtl.upc_num,
tdtl.line_net_amt,
tdtl.line_item_quantity,
cal.prev_month_idnt  
from
prd_nap_usr_vws.retail_tran_detail_fact_vw  tdtl
join
last_month_date_range  cal
on tdtl.business_day_date between cal.prev_month_start_day_date and cal.prev_month_end_day_date
where tdtl.line_item_merch_nonmerch_ind = 'merch';

collect statistics
column ( partition),
column (business_day_date, upc_num)               
on  
mmm_tran_extract;

/* step 3
generate upc/department  table 
for use in filtering transactions later
to exclude non-merch, and find mmm division
*/
create multiset volatile table  merch_upc_dept,
no before journal, no after  journal,
checksum =default  
(upc_num varchar(32) character set unicode not casespecific,
dept_num varchar(8) character set unicode not casespecific
) primary index (upc_num)
partition by upc_num mod 65535 + 1 
on commit preserve rows;
 
insert into  merch_upc_dept 
select
ltrim(upc_num,'0'),
cast(dpt.dept_num as varchar(8)) dept_num 
from
(select upc_num, rms_sku_num
from
prd_nap_usr_vws.product_upc_dim   
qualify row_number() over
(partition by trim(leading '0' from  upc_num)  order by prmy_upc_ind desc, channel_country desc)  = 1) upc    
join
(select  rms_sku_num, dept_num 
from
prd_nap_usr_vws.product_sku_dim_vw
qualify row_number() over
(partition by rms_sku_num  order by channel_country desc)  = 1) sku 
on upc.rms_sku_num = sku.rms_sku_num
join
prd_nap_usr_vws.department_dim  dpt
on sku.dept_num = dpt.dept_num
and dpt.merch_dept_ind = 'y' ;

collect statistics 
column (partition),
column ( upc_num)
on  
merch_upc_dept;
 
/* insert net sales totals for prev fisc mth*/ 
 
delete from {mmm_t2_schema}.mmm_mth_sales_extract
where month_idnt = (select prev_month_idnt from last_month_date_range);
 
 
insert into {mmm_t2_schema}.mmm_mth_sales_extract
select   
tranx.month_idnt,
tranx.business_day_date as day_date,
coalesce(fpdiv.mmm_division, opdiv.mmm_division) as division,
tranx.intent_store_num as store,    
str.business_unit_desc as channel,
sum(case when pt.price_type = 'r' then tranx.line_net_amt else 0 end) as reg_revenue,
sum(case when pt.price_type = 'r' then tranx.line_item_quantity else 0 end) as reg_qty,
sum(case when pt.price_type = 'p' then tranx.line_net_amt else 0 end) as promo_revenue,
sum(case when pt.price_type = 'p' then tranx.line_item_quantity else 0 end) as promo_qty,
sum(case when pt.price_type = 'c' then tranx.line_net_amt else 0 end) as clr_revenue,
sum(case when pt.price_type = 'c' then tranx.line_item_quantity else 0 end) as clr_qty,
CAST(CAST(CURRENT_TIMESTAMP AS  VARCHAR(19)) AS TIMESTAMP(6))as dw_sys_load_tmstp
from
mmm_tran_extract  tranx
join
prd_nap_usr_vws.store_dim  str
on tranx.intent_store_num = str.store_num
and str.business_unit_num in (1000, 2000, 3500, 9000, 9500)
join
merch_upc_dept   dpt
on trim(leading '0' from  tranx.upc_num) = trim(leading '0' from  dpt.upc_num)
left join
t2dl_das_mmm.jwn_dept_mmm_div_xref_fp  fpdiv
on  dpt.dept_num = fpdiv.dept_num
and str.business_unit_num in (1000, 3500, 9000)
left join
t2dl_das_mmm.jwn_dept_mmm_div_xref_op  opdiv
on  dpt.dept_num = opdiv.dept_num
and str.business_unit_num in (2000, 9500)
join
t2dl_das_sales_returns.retail_tran_price_type_fact  pt
on tranx.business_day_date = pt.business_day_date
and tranx.global_tran_id = pt.global_tran_id
and tranx.line_item_seq_num = pt.line_item_seq_num
 --
group by
month_idnt,
day_date,
division,
store,    
channel; 
 
collect statistics
column ( day_dt, division, store),
column (month_idnt)                  
on  
{mmm_t2_schema}.mmm_mth_sales_extract;

SET QUERY_BAND = NONE FOR SESSION;
