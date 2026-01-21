/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=node_week_sku_inventory_twist_fact_11521_ACE_ENG;
     Task_Name=node_week_sku_inventory_twist_fact;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: t2dl_das_usl.node_week_sku_inventory_twist_fact
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: June 27th 2023

Note:
-- Purpose of the table: Table to directly get Inventory and Twist info at Sku / Store / Week level
-- Update Cadence: Daily

*/

/* 1. Mapping UPC's to MPG_category & merch-roles */
--drop table merch_mpg_sku_dim;
create multiset volatile table merch_mpg_sku_dim as
(select distinct psd.rms_sku_num
                ,psd.rms_style_num
                ,psd.color_num
                ,mpg.mpg_category
                ,th.nord_role_desc
                ,th.rack_role_desc
  from (select sbclass_num, class_num, dept_num, rms_sku_num, rms_style_num, color_num  
         from prd_nap_usr_vws.product_sku_dim    
         qualify row_number() over (partition by rms_sku_num order by channel_country desc, dw_batch_date desc) = 1) as psd
  left join t2dl_das_ccs_categories.ccs_merch_themes as th  --t2dl_das_po_visibility.ccs_merch_themes
    on psd.dept_num = th.dept_idnt
   and psd.class_num = th.class_idnt
   and psd.sbclass_num = th.sbclass_idnt
  left join t2dl_das_cal.customer_merch_mpg_dept_categories as mpg
    on psd.dept_num = mpg.dept_num 
  where (mpg.mpg_category is not null
       or th.nord_role_desc is not null
       or th.rack_role_desc is not null)
)with data primary index(rms_sku_num) on commit preserve rows;

COLLECT STATISTICS COLUMN (rms_style_num, color_num, mpg_category, nord_role_desc) ON merch_mpg_sku_dim;

-- If the inventory table is at sku / weekly / location level
/* 2. Twist Table */
CREATE MULTISET VOLATILE TABLE twist AS (
select rms_sku_num
       ,store_num
       ,tw.day_date
       ,week_num
       ,sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end) as instock_traffic
       ,sum(allocated_traffic) as allocated_traffic
       --,sum(case when mc_instock_ind = 1 then allocated_traffic else 0 end)/sum(allocated_traffic + 0.001) as instock_rate
from t2dl_das_twist.twist_daily as tw 
join prd_nap_usr_vws.day_cal cal on tw.day_date=cal.day_date
where cal.day_date between {start_date} and {end_date}-- the filter we need to date selections
--and business_unit_desc in ('FULL LINE','N.CA','N.COM','RACK', 'RACK CANADA','OFFPRICE ONLINE') 
--and country = 'US' 
group by 1,2,3,4
)with data primary index(rms_sku_num,store_num,day_date) on commit preserve rows;

COLLECT STATISTICS COLUMN (rms_sku_num, store_num, day_date) ON twist;

CREATE MULTISET VOLATILE TABLE twist_weekly AS (
select rms_sku_num
       ,store_num
       ,week_num
       ,sum(instock_traffic) as instock_traffic
       ,sum(allocated_traffic) as allocated_traffic
       --,avg(instock_rate) as twist
from twist
group by 1,2,3
)with data primary index(rms_sku_num,store_num,week_num) on commit preserve rows;

COLLECT STATISTICS COLUMN (rms_sku_num, store_num, week_num) ON twist_weekly;

/* 3. Get BOH Dates */
create multiset volatile table boh_dates as
(select year_num
       ,month_num
       ,week_num
       ,day_date
       ,min(day_date)-1 as prior_wk_end_dt
  from prd_nap_usr_vws.day_cal
  where day_date between {start_date} and {end_date}
  group by 1,2,3,4
)with data primary index(month_num) on commit preserve rows; --primary_index?

COLLECT STATISTICS COLUMN (prior_wk_end_dt, week_num) ON boh_dates;

create multiset volatile table date_range as
(select min(prior_wk_end_dt) as start_date
       ,max(prior_wk_end_dt) as end_date
  from boh_dates
)with data primary index(start_date) on commit preserve rows;

-- Inventory Table
create multiset volatile table inventory_twist as (
select case when length(trim(base.location_id)) >= 4 then cast(1000 as int) 
            when st.business_unit_desc in ('N.COM','TRUNK CLUB') then cast(808 as int) 
            when st.business_unit_desc in ('N.CA') then cast(867 as int) 
            when st.business_unit_desc in ('OFFPRICE ONLINE') then cast(828 as int) 
            else cast(base.location_id as int)
        end as store_num
      ,case when st.business_unit_desc in ('FULL LINE','FULL LINE CANADA') then 'nstore'
            when st.business_unit_desc in ('N.CA','N.COM','TRUNK CLUB') then 'ncom'
            when st.business_unit_desc in ('RACK', 'RACK CANADA') then 'rstore'
            when st.business_unit_desc in ('OFFPRICE ONLINE') then 'rcom'
            else null
        end as channel
      ,cal.week_num 
      ,cal.month_num
      ,cal.year_num
      ,sku.rms_sku_num
      ,sku.rms_style_num
      ,sku.color_num
      ,sku.rms_style_num||'_'|| sku.color_num as selection_cc
      --,cc.customer_choice
      ,coalesce(sku.mpg_category,'undefined') as mpg_category
      ,case when st.business_unit_desc in ('RACK', 'RACK CANADA','OFFPRICE ONLINE') then coalesce(sku.rack_role_desc,'undefined') 
            else coalesce(sku.nord_role_desc,'undefined') end as merch_role
      ,sum(coalesce(base.stock_on_hand_qty,0)) as stock_on_hand_qty
      ,sum(coalesce(base.in_transit_qty,0)) as in_transit_qty
      ,sum(twist.instock_traffic) as instock_traffic
      ,sum(twist.allocated_traffic) as allocated_traffic
      ,sum(instock_traffic)/sum(allocated_traffic+0.001) as twist
      --,avg(twist.twist) as twist
      --,count(distinct sku.rms_style_num||'_'|| sku.color_num) as selection_cc
 from prd_nap_usr_vws.inventory_stock_quantity_by_day_fact as base
 join boh_dates as cal
   on base.snapshot_date = cal.prior_wk_end_dt
 join prd_nap_usr_vws.store_dim as st
   on base.location_id = st.store_num
 left join merch_mpg_sku_dim as sku
   on base.rms_sku_id = sku.rms_sku_num
--  left join t2dl_das_selection.selection_productivity_base cc
--    on base.rms_sku_id = cc.sku_idnt

 join twist_weekly twist 
  on sku.rms_sku_num = twist.rms_sku_num
  and (case when length(trim(base.location_id)) >= 4 then cast(1000 as int) 
            when st.business_unit_desc in ('N.COM','TRUNK CLUB') then cast(808 as int) 
            when st.business_unit_desc in ('N.CA') then cast(867 as int) 
            when st.business_unit_desc in ('OFFPRICE ONLINE') then cast(828 as int) 
            else cast(base.location_id as int)
        end) = twist.store_num
 and cal.week_num = twist.week_num

 where base.snapshot_date between (select start_date from date_range) and (select end_date from date_range)
   and st.business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB','RACK', 'RACK CANADA','OFFPRICE ONLINE')
 group by 1,2,3,4,5,6,7,8,9,10,11) with data primary index(store_num,channel,week_num,rms_sku_num) on commit preserve rows;

/*
--------------------------------------------
DELETE any overlapping records from destination 
table prior to INSERT of new data
--------------------------------------------

*/

DELETE FROM {usl_t2_schema}.node_week_sku_inventory_twist_fact
       WHERE week_num BETWEEN (SELECT week_num from prd_nap_usr_vws.day_cal where day_date = {start_date} )
                      and (SELECT week_num from prd_nap_usr_vws.day_cal where day_date = {end_date} );

INSERT INTO {usl_t2_schema}.node_week_sku_inventory_twist_fact
select store_num as store_number
      -- ,channel
      ,week_num 
      ,month_num
      ,year_num
      ,rms_sku_num
      -- ,rms_style_num
      ,color_num
      ,selection_cc
      -- ,mpg_category
      -- ,merch_role
      ,stock_on_hand_qty
      ,in_transit_qty
      ,instock_traffic
      ,allocated_traffic
      ,twist
      ,CURRENT_TIMESTAMP as dw_sys_load_tmstp
from inventory_twist
;

COLLECT STATISTICS  COLUMN (store_number),
                    COLUMN (week_num),
                    COLUMN (month_num),
                    COLUMN (year_num),
                    COLUMN (rms_sku_num),
                    -- COLUMN (rms_style_num),
                    COLUMN (color_num)
on {usl_t2_schema}.node_week_sku_inventory_twist_fact;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
