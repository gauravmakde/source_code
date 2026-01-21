
SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=shoe_size_performance_monthly_base_11521_ACE_ENG;
     Task_Name=shoe_size_performance_monthly_base;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_ccs_categories.shoe_size_performance_monthly_base
Team/Owner: Merchandising Insights
Date Created/Modified: 2/9/2024

Note:
-- Purpose: Normalized shoe sizes with monthly base performance metrics
-- Cadence/lookback window: Runs weekly on Sunday at 19:30 and looks back one month to update merch metrics
*/

-- VARIABLE DATES
CREATE VOLATILE TABLE variable_start_end_week AS
(
  SELECT
    MAX(CASE WHEN day_date < TO_DATE('2023-01-30', 'yyyy-mm-dd') THEN week_idnt  END) AS start_week_num,
    MAX(CASE WHEN day_date < TO_DATE('2024-03-03', 'yyyy-mm-dd') THEN week_idnt  END) AS end_week_num
  FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM
) WITH DATA PRIMARY INDEX (start_week_num, end_week_num) ON COMMIT PRESERVE ROWS;



create volatile multiset table date_lkup as (
  select distinct
     dcd.week_idnt as week_idnt_true
      ,dcd.week_end_day_date             
      ,month_end_day_date            
   from prd_nap_usr_vws.day_cal_454_dim dcd

   where
 week_idnt_true BETWEEN (Select start_week_num from variable_start_end_week) 
 
   AND (Select end_week_num from variable_start_end_week)
   



) WITH DATA PRIMARY INDEX(week_idnt_true) ON COMMIT PRESERVE ROWS;



--drop table store_lkup;
create volatile multiset table store_lkup as (
   select distinct
      -- store_num is text in NAP fact table
      store_num 
      ,Trim(store_num || ', ' ||store_name) AS store_label
      ,channel_num
      ,trim(both from channel_num) ||', '|| channel_desc as channel_label
      -- county for map 
      ,store_address_county
      --,selling_channel as channel_type
      ,Trim(region_num  || ', ' ||region_desc) AS region_label
       ,channel_country AS country   
       ,CASE WHEN channel_brand = 'NORDSTROM' THEN 'N'
        WHEN channel_brand = 'NORDSTROM_RACK' THEN 'NR'
        ELSE NULL END AS banner
   from prd_nap_usr_vws.price_store_dim_vw
   where
      store_country_code = 'US'
      -- exclude Trunk Club, Last Chance, DCs, NQC, NPG, Faconnable
     and channel_num IN (110,111,120,121,130,140,210,211,220,221,250,260,261,310,311) --doesn't include DCs, LC, NQC
    AND store_abbrev_name <> 'CLOSED'

      and channel_num is not null
)
with data unique primary index(store_num) on commit preserve rows
;


collect statistics 
   unique primary index (store_num)
    ,COLUMN (store_num, channel_num)
    ,COLUMN (channel_label, channel_num, country, banner)
    ,COLUMN (store_num, store_label,store_address_county,REGION_LABEL,channel_label,channel_num,BANNER)
on store_lkup
;


-- use BADO table due to size not updated
-- drop table sku_lkup
CREATE VOLATILE MULTISET TABLE sku_lkup
AS (
    SELECT distinct 
        p.sku_idnt as rms_sku_num
 
    FROM prd_ma_bado.prod_sku_sty_cur_lkup_vw p
    )

WITH DATA AND STATS PRIMARY INDEX (rms_sku_num) ON COMMIT PRESERVE ROWS;
COLLECT STATS PRIMARY INDEX (rms_sku_num)

ON sku_lkup;


--drop table sales_reg;
--drop table sales;
create volatile multiset table sales
as (
   --DIAGNOSTIC HELPSTATS ON FOR SESSION;
--EXPLAIN
select
      s.rms_sku_num      
      ,s.store_num
      ,s.week_num
      ,s.rp_ind
      ,s.dropship_ind                           as ds_ind
      ,cast('R' as char(3))                     as price_type
      
      ,sum(net_sales_tot_regular_retl)   as sales_dollars 
      ,sum(net_sales_tot_regular_cost)   as sales_cost
      ,sum(net_sales_tot_regular_units)  as sales_units
      ,sum(RETURNS_TOT_REGULAR_RETL)     as return_dollars 
      ,sum(RETURNS_TOT_REGULAR_COST)   as return_cost
      ,sum(RETURNS_TOT_REGULAR_UNITS )  as return_unit
      
      ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
      
      

      
   
      -- for calculating product margin we need to use only sales where item cost is known
      ,sum(
         case 
            when wac_avlbl_ind = 'Y' then net_sales_tot_regular_retl 
         end
     )   as net_sales_tot_retl_with_cost
      
     from prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw as s
   inner join date_lkup as dt
      on s.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on s.store_num = store.store_num
   inner join sku_lkup as sku
      on s.rms_sku_num = sku.rms_sku_num

   where  s.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
 
    group by 1,2,3,4,5,6
    --having     sum(net_sales_tot_units) <> 0

union all
-- sales clearance
   select
       s.rms_sku_num      
      ,s.store_num
      ,s.week_num
      ,s.rp_ind
      ,s.dropship_ind                           as ds_ind
      ,cast('C' as char(2))                        as price_type
      ,sum(net_sales_tot_clearance_retl)    as sales_dollars 
      ,sum(net_sales_tot_clearance_cost)    as sales_cost
      ,sum(net_sales_tot_clearance_units)   as sales_units
      ,sum(RETURNS_TOT_CLEARANCE_RETL)     as return_dollars
      ,sum(RETURNS_TOT_CLEARANCE_COST)   as return_cost
      ,sum(RETURNS_TOT_CLEARANCE_UNITS)  as return_unit
      
        ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
      
      
      
      -- for calculating product margin we need to use only sales where item cost is known
      ,sum(
         case 
            when wac_avlbl_ind = 'Y' then net_sales_tot_clearance_retl 
         end
      )   as net_sales_tot_retl_with_cost

     from prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw as s
   inner join date_lkup as dt
      on s.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on s.store_num = store.store_num
   inner join sku_lkup as sku
      on s.rms_sku_num = sku.rms_sku_num

   where s.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
 
    group by 1,2,3,4,5,6
    --having     sum(net_sales_tot_units) <> 0


 
 union all
 --sales_promo

   select
      s.rms_sku_num      
      ,s.store_num
      ,s.week_num
      ,s.rp_ind
      ,s.dropship_ind                           as ds_ind
      ,cast('P' as char(3))                     as price_type
      ,sum(net_sales_tot_promo_retl)     as sales_dollars
      ,sum(net_sales_tot_promo_cost)     as sales_cost
      ,sum(net_sales_tot_promo_units)    as sales_units
      ,sum(RETURNS_TOT_PROMO_RETL)     as return_dollars
      ,sum(RETURNS_TOT_PROMO_COST)   as return_cost
      ,sum(RETURNS_TOT_PROMO_UNITS)  as return_unit
      
       ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost
      
       
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
      
      -- for calculating product margin we need to use only sales where item cost is known
      ,sum(
         case 
            when wac_avlbl_ind = 'Y' then net_sales_tot_promo_retl 
         end
      )   as net_sales_tot_retl_with_cost
      from prd_nap_usr_vws.merch_sale_return_sku_store_week_fact_vw as s
   inner join date_lkup as dt
      on s.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on s.store_num = store.store_num
   inner join sku_lkup as sku
      on s.rms_sku_num = sku.rms_sku_num

   where  s.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
 
    group by 1,2,3,4,5,6
    --having     sum(net_sales_tot_units) <> 0
)
with data primary index (rms_sku_num, store_num, week_num) on commit preserve rows
;

collect stats 
    primary index(rms_sku_num, store_num, week_num)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_num)
on sales;

/*
 * DEMAND REG PRICE
 * 
 * Reg price only in this query
 * NAP fact table grain is SKU + loc + Week; price types are baked into measures 
 * NAP table has RP indicator so bring it in here rather than joining to separate RP table 
 * rp_ind not needed to identify a unique row (only one value of rp_ind for each SKU + Loc + Week)*/


--drop table demand;
create volatile multiset table demand
as (
-- demand regular
   -- dropship = Y
   select
      d.rms_sku_num      
      ,d.store_num
      ,d.week_num
      ,d.rp_ind
      ,cast('Y' as char(1)) as ds_ind
      ,cast('R' as char(3)) as price_type
      ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      ,coalesce(d.jwn_demand_dropship_fulfilled_regular_retail_amt_ty,0)  as demand_dollars 
      ,coalesce(d.jwn_demand_dropship_fulfilled_regular_units_ty,0)     as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost
      
     
   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where d.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
     and demand_units <> 0
     
   
   union all
   
   -- dropship = N
   select
      d.rms_sku_num      
      ,d.store_num
      ,d.week_num
      ,d.rp_ind
      ,cast('N' as char(1)) as ds_ind
      ,cast('R' as char(3)) as price_type
       ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      -- Calculate demand non-DS = total demand - demand DS
      ,coalesce(d.jwn_demand_regular_retail_amt_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_regular_retail_amt_ty,0)  as demand_dollars
      ,coalesce(d.jwn_demand_regular_units_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_regular_units_ty,0)           as demand_units  
       ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost

       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost
      

    from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where  d.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
     and demand_units <> 0
     
union all

--demand_clr;

   --dropship = Y
   select
      d.rms_sku_num      
      ,d.store_num
      ,d.week_num
      ,d.rp_ind
      ,cast('Y' as char(1)) as ds_ind
      ,cast('C' as char(3)) as price_type
      
      ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      ,coalesce(d.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty,0) as demand_dollars
      ,coalesce(d.jwn_demand_dropship_fulfilled_clearance_units_ty,0)      as demand_units     
      
       ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost

       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost


   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where  d.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
     and demand_units <> 0
     
   union all
   
   -- dropship = N
   select
      d.rms_sku_num      
      ,d.store_num
      ,d.week_num
      ,d.rp_ind
      ,cast('N' as char(1)) as ds_ind
      ,cast('C' as char(3)) as price_type
      
      
      ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      -- Calculate demand non-DS = total demand - demand DS
      ,coalesce(d.jwn_demand_clearance_retail_amt_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_clearance_retail_amt_ty,0) as demand_dollars
      ,coalesce(d.jwn_demand_clearance_units_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_clearance_units_ty,0)           as demand_units 
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost
       

       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost


   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where d.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
     and demand_units <> 0
     
union all

-- demand_promo

   select
   -- dropship = Y
     
      d.rms_sku_num      
      ,d.store_num
      ,d.week_num
      ,d.rp_ind
      ,cast('Y' as char(1)) as ds_ind
      ,cast('P' as char(3)) as price_type
      
      ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      ,coalesce(d.jwn_demand_dropship_fulfilled_promo_retail_amt_ty,0)  as demand_dollars
      ,coalesce(d.jwn_demand_dropship_fulfilled_promo_units_ty,0)       as demand_units           

     ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      

       , Cast(0 AS DECIMAL(18,2)) AS receipt_dropship_dollars
       , Cast(0 AS DECIMAL(18,2)) AS receipt_dropship_units
       , Cast(0 AS DECIMAL(18,0)) AS receipt_dropship_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost


   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where d.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
     and demand_units <> 0
     

   union all
   
   -- dropship = N
   select
      
      d.rms_sku_num      
      ,d.store_num
      ,d.week_num
      ,d.rp_ind
      ,cast('N' as char(1)) as ds_ind
      ,cast('P' as char(3)) as price_type
      
       ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      -- Calculate demand non-DS = total demand - demand DS
      ,coalesce(jwn_demand_promo_retail_amt_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_promo_retail_amt_ty,0)  as demand_dollars
      ,coalesce(d.jwn_demand_promo_units_ty,0) - coalesce(d.jwn_demand_dropship_fulfilled_promo_units_ty,0)          as demand_units     

   
     ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost

       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost


   from prd_nap_usr_vws.MERCH_JWN_DEMAND_SKU_STORE_WEEK_AGG_FACT  d
   inner join date_lkup as dt
      on d.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on d.store_num = store.store_num
   inner join sku_lkup as sku
      on d.rms_sku_num = sku.rms_sku_num
   where d.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
     and demand_units <> 0
     
)
with data primary index (rms_sku_num, store_num, week_num) on commit preserve rows
;


collect stats 
    primary index(rms_sku_num, store_num, week_num)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_num) on  demand
;




--DROP TABLE inventory;
CREATE VOLATILE MULTISET TABLE inventory
AS (

---regular
   SELECT
   
      i.rms_sku_num      
      ,S.store_num
      ,i.week_num
      ,i.rp_ind
      ,Cast('N' AS CHAR(1)) AS ds_ind
      ,Cast('R' AS CHAR(3)) AS price_type
      
      ,Cast(0 AS DECIMAL(18,2))   AS sales_dollars 
      ,Cast(0 AS DECIMAL(18,2))   AS sales_cost
      ,Cast(0 AS DECIMAL(18,0))   AS sales_units
      
      ,Cast(0 AS DECIMAL(18,2))   AS return_dollars 
      ,Cast(0 AS DECIMAL(18,2))   AS return_cost
      ,Cast(0 AS DECIMAL(18,0))   AS return_units
      
      ,Cast(0 AS DECIMAL(18,2))  AS demand_dollars 
      ,Cast(0 AS DECIMAL(18,0))  AS demand_units
      
     /* ,Coalesce(CASE WHEN dt.week_end_day_date = dt.month_end_day_date THEN i.eoh_regular_retail end ,0)  AS eoh_dollars
      ,Coalesce(CASE WHEN dt.week_end_day_date = dt.month_end_day_date THEN i.eoh_regular_cost end,0)     AS eoh_cost
      ,Coalesce(CASE WHEN dt.week_end_day_date = dt.month_end_day_date THEN i.eoh_regular_units end,0)    AS eoh_units*/
      
      ,Sum(i.eoh_regular_retail)  AS eoh_dollars
      ,Sum(i.eoh_regular_cost)    AS eoh_cost
      ,Sum(i.eoh_regular_units)   AS eoh_units
      
          
       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2)) AS receipt_cost
       , Cast(0 AS DECIMAL(18,0)) AS receipt_units
       
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_cost
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_units
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_cost
       , Cast(0 AS DECIMAL(18,0)) AS oo_units
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_cost
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_units
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_cost
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_units
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost
    
     FROM prd_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS i
   INNER JOIN date_lkup AS dt
      ON i.week_num = dt.week_idnt_true
   INNER JOIN store_lkup AS s
      ON i.store_num = s.store_num
   INNER JOIN sku_lkup AS sku
      ON i.rms_sku_num = sku.rms_sku_num
   WHERE 
      -- only keep records that have inventory
     --eoh_units <> 0 
       i.week_num BETWEEN (SELECT Min(week_idnt_true) FROM date_lkup) AND (SELECT Max(week_idnt_true) FROM date_lkup)
    GROUP BY 1,2,3,4,5,6
    HAVING eoh_units <> 0 

UNION ALL
--clearance
   SELECT
       i.rms_sku_num      
      ,s.store_num
      , i.week_num
      ,i.rp_ind
      ,Cast('N' AS CHAR(1)) AS ds_ind
      ,Cast('C' AS CHAR(3)) AS price_type
      
      ,Cast(0 AS DECIMAL(18,2))   AS sales_dollars 
      ,Cast(0 AS DECIMAL(18,2))   AS sales_cost
      ,Cast(0 AS DECIMAL(18,0))   AS sales_units
      
      ,Cast(0 AS DECIMAL(18,2))   AS return_dollars 
      ,Cast(0 AS DECIMAL(18,2))   AS return_cost
      ,Cast(0 AS DECIMAL(18,0))   AS return_units
      
      ,Cast(0 AS DECIMAL(18,2))  AS demand_dollars 
      ,Cast(0 AS DECIMAL(18,0))  AS demand_units
      
     /* ,Coalesce(CASE WHEN dt.week_end_day_date = dt.month_end_day_date THEN i.eoh_clearance_retail end,0)   AS eoh_dollars
      ,Coalesce(CASE WHEN dt.week_end_day_date = dt.month_end_day_date THEN i.eoh_clearance_cost end,0)     AS eoh_cost
      ,Coalesce(CASE WHEN dt.week_end_day_date = dt.month_end_day_date THEN i.eoh_clearance_units end,0)    AS eoh_units*/
      
      ,Sum(i.eoh_clearance_retail)  AS eoh_dollars
      ,Sum(i.eoh_clearance_cost)    AS eoh_cost
      ,Sum(i.eoh_clearance_units)   AS eoh_units
      
        
      , Cast(0 AS DECIMAL(18,2))   AS receipt_dollars  
      , Cast(0 AS DECIMAL(18,2))  AS receipt_cost
      , Cast(0 AS DECIMAL(18,0))  AS receipt_units 
       
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_cost
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_units
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_cost
       , Cast(0 AS DECIMAL(18,0)) AS oo_units
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_cost
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_units
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_cost
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_units
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost
       
   FROM prd_nap_usr_vws.merch_inventory_sku_store_week_fact_vw AS i
   INNER JOIN date_lkup AS dt
      ON i.week_num = dt.week_idnt_true
   INNER JOIN store_lkup AS s
      ON i.store_num = s.store_num
   INNER JOIN sku_lkup AS sku
      ON i.rms_sku_num = sku.rms_sku_num
   WHERE 
      -- only keep records that have inventory
      --eoh_units <> 0 
      -- adding condition on partitioned field for better performance (logically redundant due to inner join)
       i.week_num BETWEEN (SELECT Min(week_idnt_true) FROM date_lkup) AND (SELECT Max(week_idnt_true) FROM date_lkup)
      GROUP BY 1,2,3,4,5,6
    HAVING eoh_units <> 0 
    )
WITH DATA PRIMARY INDEX (rms_sku_num, store_num, week_num) ON COMMIT PRESERVE ROWS
;

-- explain plan recommendations varied during testing, so capturing all stats that were noted as high confidence at some point

COLLECT STATS 
    PRIMARY INDEX(rms_sku_num, store_num, week_num)
   ,COLUMN(rms_sku_num)
   ,COLUMN(store_num)
   ,COLUMN(week_num) 
ON inventory;


--Receipt staging tables


CREATE MULTISET VOLATILE TABLE receipts_stg AS (
    SELECT
    b.rms_sku_num
    ,b.store_num 
    ,b.week_num 
    --,b.npg_ind
    ,b.dropship_ind
    ,b.rp_ind

        ,b.receipts_regular_units
        ,b.receipts_crossdock_regular_units
        ,b.receipts_regular_retail
        ,b.receipts_crossdock_regular_retail
        ,b.receipts_regular_cost
        ,b.receipts_crossdock_regular_cost
        ,b.receipts_clearance_units
        ,b.receipts_crossdock_clearance_units
        ,b.receipts_clearance_retail
        ,b.receipts_crossdock_clearance_retail
        ,b.receipts_clearance_cost
        ,b.receipts_crossdock_clearance_cost
    FROM prd_nap_usr_vws.merch_poreceipt_sku_store_week_fact_vw b 
    --locations
    INNER JOIN store_lkup as store
        ON b.store_num = store.store_num
        
    WHERE 
        b.week_num IN (SELECT DISTINCT  week_idnt_true FROM date_lkup)
        AND (b.receipts_regular_units > 0 
        OR b.receipts_clearance_units > 0
        OR b.receipts_crossdock_regular_units > 0
        OR b.receipts_crossdock_clearance_units > 0)
        
        
) WITH DATA PRIMARY INDEX(rms_sku_num)
PARTITION BY Range_N(week_num BETWEEN 201900 AND 202512)
ON COMMIT PRESERVE ROWS;

 COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,STORE_NUM ,WEEK_NUM,DROPSHIP_IND ,RP_IND) ON receipts_stg; 
 
 
 



/*select sum(receipts_regular_retail + receipts_crossdock_regular_retail) from receipts_stg where dropship_ind ='Y'*/

-- RS staging table
CREATE MULTISET VOLATILE TABLE transfer_stg AS (
    SELECT
    b.rms_sku_num
    ,b.store_num 
    ,b.week_num 
    --,b.npg_ind
    ,b.rp_ind
    ,b.reservestock_transfer_in_regular_units
    ,b.reservestock_transfer_in_clearance_units
    ,b.reservestock_transfer_in_regular_retail
    ,b.reservestock_transfer_in_clearance_retail
    ,b.reservestock_transfer_in_regular_cost
    ,b.reservestock_transfer_in_clearance_cost
    
    FROM prd_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw b
    --locations
    INNER JOIN store_lkup as store
        ON b.store_num = store.store_num
        
    WHERE 
        b.week_num IN (SELECT DISTINCT  week_idnt_true FROM date_lkup)
        AND (b.reservestock_transfer_in_regular_units > 0 
            OR b.reservestock_transfer_in_clearance_units > 0)
        
) WITH DATA PRIMARY INDEX(rms_sku_num)
PARTITION BY Range_N(week_num BETWEEN 201900 AND 202512)
ON COMMIT PRESERVE ROWS;

 COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,STORE_NUM ,WEEK_NUM, RP_IND) ON transfer_stg;


-- both RP_idnt and DS_ind are in the tables
-- Drop table receipt
CREATE VOLATILE MULTISET TABLE receipt AS (

--regular
  --  DIAGNOSTIC HELPSTATS ON FOR SESSION;
--EXPLAIN
SELECT 
     base.rms_sku_num      
      ,base.store_num
      ,base.week_num
      ,base.rp_ind
      ,base.dropship_ind AS ds_ind
      ,cast('R' as char(3)) as price_type
     


      ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      
        , Sum(receipts_regular_retail + receipts_crossdock_regular_retail) AS receipt_dollars  
        , Sum(receipts_regular_units + receipts_crossdock_regular_units) AS receipt_units
        , Sum(receipts_regular_cost + receipts_crossdock_regular_cost) AS receipt_cost
        
        , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
      
      , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost


    FROM receipts_stg base
   inner join sku_lkup as sku
      on base.rms_sku_num = sku.rms_sku_num

GROUP BY 1,2,3,4,5,6--,7






-- receipt for clearance
UNION ALL


     


  SELECT 
        base.rms_sku_num      
        ,base.store_num
        ,base.week_num
        ,base.rp_ind
        ,base.dropship_ind  AS ds_ind
        ,cast('C' as char(3)) as price_type
        
        
      ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      
        , Sum(receipts_clearance_retail + receipts_crossdock_clearance_retail) AS receipt_dollars  
        , Sum(receipts_clearance_units + receipts_crossdock_clearance_units) AS receipt_units
        , Sum(receipts_clearance_cost + receipts_crossdock_clearance_cost) AS receipt_cost
        
        , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost
      

    FROM receipts_stg base
   inner join sku_lkup as sku
      on base.rms_sku_num = sku.rms_sku_num

GROUP BY 1,2,3,4,5,6--,7

    
-- RS TRANSFER
UNION ALL
-- transfer for clearance
 SELECT
 
        base.rms_sku_num      
        ,base.store_num
        ,base.week_num
        ,base.rp_ind
        ,cast('N' as char(1)) as ds_ind
        ,cast('C' as char(3)) as price_type
          
      ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      
      
      
        , Sum(reservestock_transfer_in_clearance_retail) AS receipt_dollars 
        , Sum(reservestock_transfer_in_clearance_units) AS receipt_units
        , Sum(reservestock_transfer_in_clearance_cost) AS receipt_cost

        
        
        , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost

    FROM transfer_stg base
   inner join sku_lkup as sku
      on base.rms_sku_num = sku.rms_sku_num

GROUP BY 1,2,3,4,5,6--,7
    
-- transfer reg price

UNION ALL

SELECT
        base.rms_sku_num      
        ,base.store_num
        ,base.week_num
        ,base.rp_ind
        ,cast('N' as char(1)) as ds_ind
        ,cast('R' as char(3)) as price_type
          
        ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      
        , Sum(reservestock_transfer_in_regular_retail) AS receipt_dollars
        , Sum(reservestock_transfer_in_regular_units) AS receipt_units
        , Sum(reservestock_transfer_in_regular_cost) AS receipt_cost

            
        , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost

    FROM transfer_stg base

/*   inner join store_lkup as s
      on base.store_num = s.store_num*/
   inner join sku_lkup as sku
      on base.rms_sku_num = sku.rms_sku_num

GROUP BY 1,2,3,4,5,6--,7
) 
with data primary index (rms_sku_num, store_num, week_num) on commit preserve rows
;

-- explain plan recommendations varied during testing, so capturing all stats that were noted as high confidence at some point
collect stats 
    primary index(rms_sku_num, store_num, week_num)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_num) on    receipt;

--drop table transfer_PH
CREATE MULTISET VOLATILE TABLE transfer_PH AS 
(
select

s.rms_sku_num      
        ,s.store_num
        ,s.week_num
        ,s.rp_ind
        ,cast('N' as char(1)) as ds_ind
        ,cast('N/A' as char(3)) as price_type
          
        ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      
      , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost
       
       
       , sum(s.PACKANDHOLD_TRANSFER_IN_RETAIL) as transfer_pah_dollars
        , Sum(s.PACKANDHOLD_TRANSFER_IN_UNITS) as transfer_pah_units
        , Sum(s.PACKANDHOLD_TRANSFER_IN_COST) as transfer_pah_cost
       
       
        , Cast(0 AS DECIMAL(18,2)) AS oo_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_units
       , Cast(0 AS DECIMAL(20,4)) AS oo_cost
       
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_4weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_4weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_dollars
       , Cast(0 AS DECIMAL(18,2)) AS oo_12weeks_units
       , Cast(0 AS DECIMAL(18,0)) AS oo_12weeks_cost
       
       , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost
      
      
      FROM prd_nap_usr_vws.merch_transfer_sku_loc_week_agg_fact_vw  s
      
       inner join date_lkup as dt
      on s.week_num = dt.week_idnt_true
   inner join store_lkup as store
      on s.store_num = store.store_num
   inner join sku_lkup as sku
      on s.rms_sku_num = sku.rms_sku_num

   where s.week_num between (select min(week_idnt_true) from date_lkup) and (select max(week_idnt_true) from date_lkup)
      
      
    GROUP BY 1,2,3,4,5,6--,7
) 
with data primary index (rms_sku_num, store_num, week_num) on commit preserve rows
;

-- explain plan recommendations varied during testing, so capturing all stats that were noted as high confidence at some point
collect stats 
    primary index(rms_sku_num, store_num, week_num)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_num) on transfer_PH; 



-- DROP TABLE oo_stage;

CREATE MULTISET VOLATILE TABLE oo_stage AS (
    SELECT
        base.otb_eow_date
        ,base.store_num
        ,base.rms_sku_num
        ,base.anticipated_price_type AS price_type
        ,CASE WHEN base.order_type = 'AUTOMATIC_REORDER' THEN 'Y' ELSE 'N' END AS rp_ind 
        ,base.quantity_open AS on_order_units
        
        ,CAST((base.quantity_open * base.anticipated_retail_amt) AS DECIMAL(12,2)) AS on_order_retail_dollars
        ,CAST((base.quantity_open * base.unit_cost_amt) AS DECIMAL(12,2)) AS on_order_cost_dollars
        ,CASE WHEN base.otb_eow_date <= (SELECT MAX(week_end_day_date) + 28 FROM date_lkup)
            THEN base.quantity_open ELSE 0 END AS on_order_4wk_units
        ,CASE WHEN base.otb_eow_date <= (SELECT MAX(week_end_day_date) + 28 FROM date_lkup)
            THEN CAST((base.quantity_open * base.anticipated_retail_amt) AS DECIMAL(12,2)) ELSE 0 END AS on_order_4wk_retail_dollars
        ,CASE WHEN base.otb_eow_date <= (SELECT MAX(week_end_day_date) + 28 FROM date_lkup )
            THEN CAST((base.quantity_open * base.unit_cost_amt) AS DECIMAL(12,2)) ELSE 0 END AS on_order_4wk_cost_dollars
            
          ,CASE WHEN base.otb_eow_date <= (SELECT MAX(week_end_day_date) + 84 FROM date_lkup )
            THEN base.quantity_open ELSE 0 END AS on_order_12wk_units
        ,CASE WHEN base.otb_eow_date <= (SELECT MAX(week_end_day_date) + 84 FROM date_lkup )
            THEN CAST((base.quantity_open * base.anticipated_retail_amt) AS DECIMAL(12,2)) ELSE 0 END AS on_order_12wk_retail_dollars
        ,CASE WHEN base.otb_eow_date <= (SELECT MAX(week_end_day_date) + 84 FROM date_lkup )
            THEN CAST((base.quantity_open * base.unit_cost_amt) AS DECIMAL(12,2)) ELSE 0 END AS on_order_12wk_cost_dollars
            
            
    FROM prd_nap_usr_vws.merch_on_order_fact_vw base
    --locations
    INNER JOIN store_lkup as store
        ON base.store_num = store.store_num
    WHERE base.quantity_open > 0
        AND base.first_approval_date IS NOT NULL
        AND (
            (base.status = 'CLOSED' AND base.end_ship_date >= (SELECT MAX(week_end_day_date) FROM date_lkup) - 45)
                OR base.status IN ('APPROVED','WORKSHEET')
            )
) WITH DATA
    PRIMARY INDEX(rms_sku_num)
    PARTITION BY RANGE_N(otb_eow_date BETWEEN CAST('2022-01-01' AS DATE) AND CAST('2050-12-31' AS DATE) EACH INTERVAL '1' DAY )
    ON COMMIT PRESERVE ROWS
;

 COLLECT STATISTICS COLUMN (RMS_SKU_NUM ,STORE_NUM , price_type,RP_IND) ON  oo_stage
;


/*select RP_IND, sum(on_order_retail_dollars) from oo_stage group by 1*/




--DROP TABLE onorder 

CREATE MULTISET VOLATILE TABLE onorder AS 
(
    SELECT 
        
        base.rms_sku_num      
      ,base.store_num
      ,(select MAX(week_idnt_true) FROM date_lkup) AS week_num
      ,base.rp_ind
      ,cast('N' as char(1))AS ds_ind
      ,base.price_type AS price_type
      
     
     ,cast(0 as decimal(18,2))   as sales_dollars 
      ,cast(0 as decimal(18,2))   as sales_cost
      ,cast(0 as decimal(18,0))   as sales_units
      ,cast(0 as decimal(18,2))      as return_dollars 
      ,cast(0 as decimal(18,2))    as return_cost
      ,cast(0 as decimal(18,0))   as return_unit
      
      ,cast(0 as decimal(18,2))  as demand_dollars 
      ,cast(0 as decimal(18,0))  as demand_units
      
      ,cast(0 as decimal(18,2))  as eoh_dollars
      ,cast(0 as decimal(18,2))  as eoh_cost
      ,cast(0 as decimal(18,0))  as eoh_units
      


       , Cast(0 AS DECIMAL(18,2)) AS receipt_dollars  
       , Cast(0 AS DECIMAL(18,2))  AS receipt_units
       , Cast(0 AS DECIMAL(18,0))  AS receipt_cost
      
       
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_dollars
       , Cast(0 AS DECIMAL(18,2)) AS transfer_pah_units
       , Cast(0 AS DECIMAL(18,0)) AS transfer_pah_cost
       
    ,SUM(base.on_order_retail_dollars)             AS oo_dollars
    ,SUM(base.on_order_units)                 AS  oo_units
    ,SUM(base.on_order_cost_dollars)              AS  oo_cost
    
    ,Cast(Sum(base.on_order_4wk_retail_dollars)  AS DECIMAL (38,2)) oo_4weeks_dollars
    ,Cast(Sum(base.on_order_4wk_units)       AS DECIMAL (10,0)) oo_4weeks_units
    ,Cast(Sum(base.on_order_4wk_cost_dollars)    AS DECIMAL (38,2)) oo_4weeks_cost
    
    ,Cast(Sum(base.on_order_12wk_retail_dollars) AS DECIMAL (38,2)) oo_12weeks_dollars
    ,Cast(Sum(base.on_order_12wk_units)      AS DECIMAL (10,0)) oo_12weeks_units
    ,Cast(Sum(base.on_order_12wk_cost_dollars)   AS DECIMAL (38,2)) oo_12weeks_cost
    
    , Cast(0 AS DECIMAL(18,2)) AS net_sales_tot_retl_with_cost

    FROM oo_stage base 

   inner join sku_lkup as sku
      on base.rms_sku_num = sku.rms_sku_num


    GROUP BY 1,2,3,4,5,6
    
) 
with data primary index (rms_sku_num, store_num, week_num) on commit preserve rows
;

-- explain plan recommendations varied during testing, so capturing all stats that were noted as high confidence at some point
collect stats 
    primary index(rms_sku_num, store_num, week_num)
   ,column(rms_sku_num)
   ,column(store_num)
   ,column(week_num)   on onorder;

 --drop table full_data_stg_base
create volatile multiset table full_data_stg_base
as (

      select * from sales
      union all
    select * from demand
      union all
   select * from inventory
      union all
      select * from receipt
      union all
      select * from transfer_PH
      union all
      select * from onorder
    
  
)
with data no primary index on commit preserve rows
;

 
DELETE FROM T2DL_DAS_CCS_CATEGORIES.shoe_size_performance_monthly_base WHERE week_num BETWEEN (Select start_week_num from variable_start_end_week) AND (Select end_week_num from variable_start_end_week);


INSERT INTO T2DL_DAS_CCS_CATEGORIES.shoe_size_performance_monthly_base
SELECT a.*, 
CURRENT_TIMESTAMP AS dw_sys_load_tmstp 
FROM full_data_stg_base a;
;


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (week_num) -- column names used for primary index
on T2DL_DAS_CCS_CATEGORIES.shoe_size_performance_monthly_base;


SET QUERY_BAND = NONE FOR SESSION;