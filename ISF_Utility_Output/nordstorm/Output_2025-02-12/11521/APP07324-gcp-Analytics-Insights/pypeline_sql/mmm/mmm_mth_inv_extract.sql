SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=mmm_mth_inv_extract_11521_ACE_ENG;
     Task_Name=mmm_mth_inv_extract;'
     FOR SESSION VOLATILE;

/* intitial version apr 2022
lance christenson

delete/insert previous fiscal month 
mmm inv totals by day,store,div
into t2dl_das_mmm.mmm_mth_inv_extract
running multiple times OK as previous
month deleted and resummed each run

Team/Owner: AE
Date Created/Modified: 11/23/2022
*/

/* step 1
extract previous fiscal month wk_beg/end dates
)
*/ 
create multiset volatile table last_month_week_dates  as
(select distinct
currcal.day_date,
prevcal.month_idnt,
prevcal.week_start_day_date,
prevcal.week_end_day_date
from
prd_nap_usr_vws.day_cal_454_dim  currcal
join
prd_nap_usr_vws.day_cal_454_dim   prevcal
on  prevcal.month_idnt =
(select prev_month_idnt from
(select 
month_idnt,
prev_month_idnt
from
prd_nap_usr_vws.DAY_CAL_454_DIM  currcal
join
(select distinct
month_idnt as curr_month_idnt,
lag(month_idnt) over (order by month_idnt) prev_month_idnt
from prd_nap_usr_vws.DAY_CAL_454_DIM) prev_tbl
on currcal.month_idnt = prev_tbl.curr_month_idnt
and prev_tbl.prev_month_idnt < currcal.month_idnt
where current_date  = currcal.day_date) tbl)
where current_date  = currcal.day_date
) with data primary index(day_date) 
on commit preserve rows;


/* step 2
create efficient dept_num lookup
to provide xref for the inv skus
to the customer neustar-divisions
*/

create multiset volatile table  sku_dept,
no before journal, no after  journal,
checksum =default  
(rms_sku_num varchar(32) character set unicode not casespecific,
dept_num integer
) primary index (rms_sku_num)
on commit preserve rows;

insert into  sku_dept 
select
rms_sku_num,
dept_num  
from
prd_nap_usr_vws.product_sku_dim  skud
where skud.gwp_ind <> 'y'
and skud.smart_sample_ind <> 'y'
qualify row_number() over
(partition by rms_sku_num  order by channel_country desc)  = 1 ;

/* step 3
extract from INVENTORY_STOCK_QUANTITY_BY_DAY_LOGICAL_FACT
just the fields  needed by mmm
for the previous fiscal month. For efficiency,
to avoid joins, pull past 2 months data, then
filter it for the last month week dates.
*/
create multiset volatile table soh_scan_extract  as
(select 
snapshot_date,
location_id,
rms_sku_id,
stock_on_hand_qty
from
prd_nap_usr_vws.inventory_stock_quantity_by_day_logical_fact  sohf
where snapshot_date >= ADD_MONTHS (current_date , -2) 
and stock_on_hand_qty is not null
) with data primary index( snapshot_date, location_id, rms_sku_id)
partition by range_n(snapshot_date between date '2017-01-01' and date '2025-12-31' each interval '1' day)
on commit preserve rows;

collect statistics
column (partition),
column (snapshot_date, location_id, rms_sku_id)
on
soh_scan_extract;

create multiset volatile table soh_scan_week  as
(select 
lmwd.month_idnt,
lmwd.week_start_day_date,
lmwd.week_end_day_date,
location_id,
rms_sku_id,
stock_on_hand_qty
from
soh_scan_extract  soh
join 
last_month_week_dates lmwd
on soh.snapshot_date = lmwd.week_end_day_date 
) with data primary index( week_start_day_date, week_end_day_date, location_id, rms_sku_id)
on commit preserve rows;

collect statistics
column (week_start_day_date, week_end_day_date, location_id, rms_sku_id)
on
soh_scan_week;

/* step 4
Build tables that will be used to find the price type of
the inventory skus
*/
create multiset volatile table product_price_dim_date     as
(with normdates as
 (select normalize on meets or overlaps  
    store_num,
    rms_sku_num,
    selling_retail_price_amt as current_price_amt, 
    substr(selling_retail_price_type_code,1,1) as current_price_type,
    period(eff_begin_tmstp, eff_end_tmstp) as norm_period 
from prd_nap_usr_vws.product_price_timeline_dim)
select   
normdates.store_num, normdates.rms_sku_num, normdates.current_price_amt, normdates.current_price_type,  
   cast( begin(normdates.norm_period) as date) eff_begin_date,
   cast( end(normdates.norm_period ) as date) - interval '1' day as eff_end_date 
    from normdates
    ) with data primary index(store_num, rms_sku_num, current_price_amt)        
on commit preserve rows;

collect statistics
column ( store_num, rms_sku_num, current_price_amt, eff_begin_date, eff_end_date)
on
product_price_dim_date;


/* step 5
refresh T2DL_DAS_MMM.MMM_MTH_INV_EXTRACT with last months weekly data
*/

delete from {mmm_t2_schema}.mmm_mth_inv_extract
where month_idnt = (select distinct month_idnt from last_month_week_dates);

insert into {mmm_t2_schema}.mmm_mth_inv_extract
select 
soh.month_idnt,
soh.week_start_day_date as wk_start_dt,
soh.week_end_day_date as wk_end_dt,
coalesce(fpdiv.mmm_division, opdiv.mmm_division) as division,
case 
when str.business_unit_num = 8500 then upper('n.com')
when str.business_unit_num = 5500 then upper('full line canada')
else str.business_unit_desc end  as chain_desc,
soh.location_id as loc_idnt,
 --
sum (case
when coalesce(ppdd.current_price_type,'x') in ('p','c')
then 0 
when  ppdd.current_price_type = 'r'
then soh.stock_on_hand_qty 
when regp.regular_price_amt is not null
then soh.stock_on_hand_qty
else 0 end)  as reg_qty,
 --
sum (case
when coalesce(ppdd.current_price_type,'x') in ('p','c')
then 0 
when  ppdd.current_price_type = 'r'
then soh.stock_on_hand_qty * ppdd.current_price_amt  
when regp.regular_price_amt is not null
then soh.stock_on_hand_qty * regp.regular_price_amt
else 0 end)  as reg_amt,
  --
 count(distinct case 
when coalesce(ppdd.current_price_type,'x') in ('p','c')
then null 
when  ppdd.current_price_type = 'r'
then soh.rms_sku_id
when regp.regular_price_amt is not null
then soh.rms_sku_id end) as reg_sku_cnt, 
--
sum(case 
when  ppdd.current_price_type = 'p'
then soh.stock_on_hand_qty 
else 0 end) as promo_qty,
  --
sum(case
when  ppdd.current_price_type = 'p'
then soh.stock_on_hand_qty * ppdd.current_price_amt 
else 0 end) as promo_amt,
 --
count(distinct case 
when  ppdd.current_price_type = 'p'
then soh.rms_sku_id end) as promo_sku_cnt,
 --
sum(case 
when  ppdd.current_price_type = 'c'
then soh.stock_on_hand_qty 
else 0 end) as clr_qty,
  --
sum( case
when  ppdd.current_price_type = 'c'
then soh.stock_on_hand_qty * ppdd.current_price_amt 
else 0 end) as clr_amt,
 --
count(distinct case 
when  ppdd.current_price_type = 'c'
then soh.rms_sku_id end) as clr_sku_cnt,
 --
  CURRENT_TIMESTAMP as dw_sys_load_tmstp
 --
from
soh_scan_week  soh
join 
prd_nap_usr_vws.store_dim  str
on soh.location_id = str.store_num
and str.business_unit_num in (1000,2000,5000, 5500, 6000,6500,8500,9000,9500)
join
prd_nap_usr_vws.price_store_dim_vw  pstr
on soh.location_id = pstr.store_num
join
sku_dept  dpt
on soh.rms_sku_id  = dpt.rms_sku_num
left join
t2dl_das_mmm.jwn_dept_mmm_div_xref_fp  fpdiv
on  dpt.dept_num = fpdiv.dept_num
and str.business_unit_num in (1000, 5500, 6000, 6500, 8500, 9000)
left join
t2dl_das_mmm.jwn_dept_mmm_div_xref_op  opdiv
on  dpt.dept_num = opdiv.dept_num
and str.business_unit_num in (2000,5000, 9500)
left join
product_price_dim_date   ppdd
on pstr.price_store_num = ppdd.store_num
and soh.rms_sku_id = ppdd.rms_sku_num
and ppdd.eff_begin_date is not null
and ppdd.eff_end_date is not null
and soh.week_end_day_date between ppdd.eff_begin_date and ppdd.eff_end_date 
 --
left join
t2dl_das_sales_returns.product_reg_price_dim  regp
on pstr.price_store_num = regp.price_store_num
and soh.rms_sku_id = regp.rms_sku_num
and soh.week_end_day_date between regp.pricing_start_date and regp.pricing_end_date
 --
where division is not null
 --
group by
month_idnt,
wk_start_dt,
wk_end_dt,
division,
chain_desc,
soh.location_id; 

collect statistics
column ( wk_start_dt, wk_end_dt ,division , chain_desc, loc_idnt),
column (month_idnt)                  
on  
{mmm_t2_schema}.mmm_mth_inv_extract;

SET QUERY_BAND = NONE FOR SESSION;