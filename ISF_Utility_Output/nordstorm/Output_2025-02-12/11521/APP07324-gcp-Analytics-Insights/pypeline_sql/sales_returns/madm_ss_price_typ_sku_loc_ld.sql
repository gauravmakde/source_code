

SET QUERY_BAND = 'App_ID=APP08118;
     DAG_ID=madm_ss_price_typ_sku_loc_ld_11521_ACE_ENG;
     Task_Name=madm_ss_price_typ_sku_loc_ld;'
     FOR SESSION VOLATILE;

/*  intitial version May 2023
lance christenson

Team/Owner: AE
Date Created/Modified: May 2023

Extract snapshot of price types from MADM to provide pre-2021 history missing from NAP
*/ 

delete from {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld;

insert into {sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld
select  
    prc.loc_idnt,
    prc.sku_idnt,
    prc.start_dt,
    prc.end_dt,
    prc.price_typ_cd,
    case when coalesce(inv.f_i_soh_clrc_qty,0) > 0 then 'Y' else NULL end as clearance_soh_ind,
    prc.unit_rtl_amt,
    CAST(CAST(CURRENT_TIMESTAMP AS  VARCHAR(19)) AS TIMESTAMP(6))as dw_sys_load_tmstp
from 
prd_usr_vws.price_typ_sku_loc_ld  prc
 -----
join
prd_nap_usr_vws.day_cal  cal
on prc.start_dt = cal.day_date
left join
prd_usr_vws.inv_sku_ld_fct  inv
on prc.sku_idnt = inv.sku_idnt
and prc.loc_idnt = inv.loc_idnt
and cal.week_num = inv.wk_idnt
and cal.day_num = inv.day_idnt
 ------
where prc.loc_idnt in (1,338,808,828,835,844,867)
and prc.unit_rtl_amt is not null
and prc.start_dt <= prc.end_dt
and start_dt >= date'2014-01-01';

collect statistics
column ( partition),
column (loc_idnt, sku_idnt, start_dt, end_dt) 
on
{sales_returns_t2_schema}.madm_ss_price_typ_sku_loc_ld;
 
SET QUERY_BAND = NONE FOR SESSION;

