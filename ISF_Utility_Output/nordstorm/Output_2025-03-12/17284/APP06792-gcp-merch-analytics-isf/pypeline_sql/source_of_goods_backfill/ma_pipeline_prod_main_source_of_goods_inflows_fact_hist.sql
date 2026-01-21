/*
Name:                source_of_goods_inflows_fact
APPID-Name:          APP09479
Purpose:             Capture historical unit inflows for use in identifying source of goods
Variable(s):         {{dsa_ai_secure_schema}} PROTO_DSA_AI_BASE_VWS for dev or PRD_NAP_DSA_AI_BASE_VWS for prod
                     {{start_date}} as beginning of time period
                     {{end_date}} as end of time period
DAG: 
Author(s):           Jevon Barlas
Date Created:        9/27/2024
Date Last Updated:   9/27/2024
*/

/*
 * OVERALL NOTES
 * 
 * This code must be run for all dates in the source of goods date range and the lookback range
 * 
 * Ex: if we want to determine SOG as of each date in FY24 Jun YTD, 
 *    and we are using a 12 month lookback period,
 *    then we would need to run this entire code set for FY23 Jun through FY24 Jun YTD
 *    because the earliest date in the lookback period for FY24 Jun YTD is in Jun FY23 
 *    (364 days prior to start of Jun FY24)
 * 
 * This means if the lookback period is 364 days then we need 364 days of history to calc source of goods for one day.
 * 
 */

/*
 * LOCATIONS INCLUDED
 *
 * Clarity Inv table has loc and channel, but this reduces hard-coding in later queries
 * Only selling locations are included because non-selling locations can be sources of goods (reserve stock and pack & hold)
 *
 * Unique record: Location
 */
--drop table loc_lkup;
create volatile multiset table loc_lkup as (

   select distinct
      store_num      as location_num
      ,channel_num
      
   from prd_nap_base_vws.price_store_dim_vw
   
   where 1=1
      and store_country_code = 'US'
      -- include Nord & Rack selling locations
      and channel_num in (110,120,210,250)      
      
)
with data 
unique primary index(location_num) 
on commit preserve rows
;
 
collect statistics
   -- high confidence
   column(location_num)
on loc_lkup
;


/*
 * SKUs INCLUDED
 *
 * We can reduce CPU load by limiting to just the items for which we will determine a source of goods.
 * That will be done in a later step, though, because we hit CPU timeout if we try to run the whole time range at once.
 * 
 * Here, we'll just reduce the SKU universe a little bit
 * 
 * Unique record: SKU
 */
 
--drop table sku_lkup;
create volatile multiset table sku_lkup as (

   select
      sku.rms_sku_num
      
   from prd_nap_base_vws.product_sku_dim_vw as sku
         
   where 1=1
      and sku.channel_country = 'US' 
      -- exclude samples and gift with purchase
      and sku.smart_sample_ind <> 'Y'
      and sku.gwp_ind <> 'Y'
      -- exclude NQC items
      and (
         instr(upper(sku.brand_name),'NQC') = 0 
         or instr(upper(sku.dept_desc),'NQC') = 0
      )

)
with data 
unique primary index(rms_sku_num) 
on commit preserve rows
;

collect statistics
   column(rms_sku_num)
on sku_lkup
;


/*
 * FACT STAGING TABLE FOR INFLOWS
 *
 * Must include the lookback date range.
 * Source of Goods is determined based on inflows, so only need to keep records that have flows. 
 * 
 * Racking transfer out rows are included here so we can run later queries from this staging table instead of NAP.
 *  
 * Clarity definitions:
 * received_units: Count of items physically and logically received at a Nordstrom location (FC, DC or Direct to Store). This includes alternative partnerships where we have visibility such as Wholesession.
 * po_transfer_in_units: Count of all inventory items tied to a Purchase Order that have been received at an initial (ship to) location and are moved to the final (distribute to) location. Transfer-In means that this is the distribute-to location.
 * 
 * Expllanation: received_units and po_transfer_in_units are included here because they both track 
 * inventory transferred to a selling location, which is the relevant inflow for identifying 
 * Purchase Order as a source of goods.
 * The most common situations are:
 * received = from supplier direct to a selling location
 * po_transfer = from supplier to DC then later transferred to a selling location 
 *
 * Unique record: SKU + Channel + Day
 *
 */
--drop table inflow_fact_stg;
create volatile multiset table inflow_fact_stg as (

   select
      i.rms_sku_num
      ,i.planning_channel_num                               as channel_num
      ,i.tran_date                                          as day_date
      ,i.inv_price_type                                     as price_type
      ,coalesce(sum(i.received_units),0)                    as received_u
      ,coalesce(sum(i.po_transfer_in_units),0)              as po_transfer_in_u
      ,coalesce(sum(i.reserve_stock_transfer_in_units),0)   as reserve_stock_transfer_in_u
      ,coalesce(sum(i.pack_and_hold_transfer_in_units),0)   as pack_and_hold_transfer_in_u
      ,coalesce(sum(i.racking_transfer_in_units),0)         as racking_transfer_in_u
      ,coalesce(sum(i.racking_transfer_out_units),0)        as racking_transfer_out_u
      
   from prd_nap_jwn_metrics_base_vws.jwn_inventory_sku_loc_tran_post_day_fact_vw as i
      inner join loc_lkup as s
         on s.location_num = i.location_num
      inner join sku_lkup as sku
         on sku.rms_sku_num = i.rms_sku_num

   where 1=1
      -- redundant but helps performance
      and i.channel_country = 'US'
      and i.smart_sample_ind <> 'Y'
      and i.gift_with_purchase_ind <> 'Y'
      -- table is partitioned on tran_date
      and i.tran_date between 
	  (SELECT MIN(DAY_DATE) FROM PRD_nap_base_vws.day_cal_454_dim WHERE month_idnt = (SELECT month_num FROM {dsa_ai_secure_schema}.sog_backfill_lkup))
	  and 
	  (SELECT MAX(DAY_DATE) FROM PRD_nap_base_vws.day_cal_454_dim WHERE month_idnt = (SELECT month_num FROM {dsa_ai_secure_schema}.sog_backfill_lkup))
      
   group by 1,2,3,4
   
   having
      -- only keep records that have flows since that's all we need to determine source of goods
      -- negative flows should eventually true up to zero on a later date, but don't exclude them
      --    if the true-up happens it will be captured during SOG determination summing inflows over the lookback period
      received_u <> 0
      or po_transfer_in_u <> 0
      or reserve_stock_transfer_in_u <> 0
      or pack_and_hold_transfer_in_u <> 0
      or racking_transfer_in_u <> 0
      or racking_transfer_out_u <> 0

)
with data 
primary index(rms_sku_num, channel_num, day_date, price_type) 
on commit preserve rows
;

collect statistics
   -- high confidence
   column(rms_sku_num, day_date, price_type)
   ,column(price_type)
   ,column(channel_num)
   ,column(rms_sku_num, day_date)
   -- medium confidence
   ,column(day_date)
   -- not specifically recommended
   ,column(rms_sku_num, channel_num, day_date, price_type)
on inflow_fact_stg
;


/*
 * FACT TABLE FOR NORD BANNER PRICE TYPE
 *
 * No split by channel because we set prices at banner level,
 *    and if we have cross-channel Racking transfers then joining to banner-level price type ensures we don't miss any outflows.
 * 
 * We only want price type CLEARANCE because that's what we want to use to identify Racking transfers
 * 
 * Research showed some SKU + Channel + Day combos have multiple price types, but this was rare.
 * We will assume that any appearance of price type CLEARANCE will make that whole day CLEARANCE.
 *
 * Unique record: SKU + Day
 *
 */
--drop table nord_clearance;
create volatile multiset table nord_clearance as (

   select
      i.rms_sku_num
      ,i.day_date
      
   from inflow_fact_stg as i
   
   where 1=1
      -- Nordstrom selling channels only
      and i.channel_num in (110,120)
      and i.price_type = 'CLEARANCE'
      
   group by 1,2
   
   having coalesce(sum(i.racking_transfer_out_u),0) <> 0
   
)
with data 
primary index(rms_sku_num, day_date) 
on commit preserve rows
;
 
collect statistics
   -- high confidence
   column(rms_sku_num, day_date)  
on nord_clearance
;


/*
 * FINAL FACT TABLE FOR INFLOWS
 *
 * Only count Racking Inflows if Nord price type is clearance
 *
 * Unique record: SKU + Channel + Day
 *
 */
--drop table inflow_fact;
create volatile multiset table inflow_fact as (

   select
      rms_sku_num
      ,channel_num
      ,day_date
      ,received_u
      ,po_transfer_in_u
      ,reserve_stock_transfer_in_u
      ,pack_and_hold_transfer_in_u
      ,racking_transfer_in_u        
   
   from (
   
         select
            i.rms_sku_num
            ,i.channel_num
            ,i.day_date
            ,sum(i.received_u)                  as received_u
            ,sum(i.po_transfer_in_u)            as po_transfer_in_u
            ,sum(i.reserve_stock_transfer_in_u) as reserve_stock_transfer_in_u
            ,sum(i.pack_and_hold_transfer_in_u) as pack_and_hold_transfer_in_u
            ,sum(
               case
                  -- Only count Racking Inflows if Nord price type is clearance
                  when i.channel_num in (210,250) and n.rms_sku_num is not null then i.racking_transfer_in_u
                  else 0
               end
            )                                   as racking_transfer_in_u
            
         from inflow_fact_stg as i
            left join nord_clearance as n
               on n.rms_sku_num = i.rms_sku_num
               and n.day_date = i.day_date
               
         group by 1,2,3
         
      ) as a
      
   where 
   -- don't need records that had only racking_transfer_out_u because those were used only for custom calc of racking_transfer_in_u
      received_u <> 0
      or po_transfer_in_u <> 0
      or reserve_stock_transfer_in_u <> 0
      or pack_and_hold_transfer_in_u <> 0
      or racking_transfer_in_u <> 0

)
with data 
primary index(rms_sku_num, channel_num, day_date) 
on commit preserve rows
;
 
collect statistics
   -- high confidence
   column(rms_sku_num, channel_num, day_date)  
   ,column(rms_sku_num, channel_num)
   ,column(day_date)
on inflow_fact
;


/*
 * POPULATE STATIC TABLES
 * 1) Delete old records that fall within the refresh range
 * 2) Add new records that fall within the refresh range
 *
 */

delete 

   from {dsa_ai_secure_schema}.source_of_goods_inflows_fact

   where day_date between 
   
   (SELECT MIN(DAY_DATE) FROM PRD_nap_base_vws.day_cal_454_dim WHERE month_idnt = (SELECT month_num FROM {dsa_ai_secure_schema}.sog_backfill_lkup))
	and 
	(SELECT MAX(DAY_DATE) FROM PRD_nap_base_vws.day_cal_454_dim WHERE month_idnt = (SELECT month_num FROM {dsa_ai_secure_schema}.sog_backfill_lkup))

;

--insert into t3dl_ace_pra.source_of_goods_inflows; -- this line exists only for testing purposes
insert into {dsa_ai_secure_schema}.source_of_goods_inflows_fact (
   rms_sku_num
   ,channel_num
   ,day_date
   ,received_units
   ,po_transfer_in_units
   ,reserve_stock_transfer_in_units
   ,pack_and_hold_transfer_in_units
   ,racking_transfer_in_units
   ,dw_sys_load_tmstp
)

   select 
      rms_sku_num
      ,channel_num
      ,day_date
      ,received_u                   as received_units
      ,po_transfer_in_u             as po_transfer_in_units
      ,reserve_stock_transfer_in_u  as reserve_stock_transfer_in_units
      ,pack_and_hold_transfer_in_u  as pack_and_hold_transfer_in_units
      ,racking_transfer_in_u        as racking_transfer_in_units
      ,current_timestamp            as dw_sys_load_tmstp

   from inflow_fact as f

;

-- stats to collect are defined in DDL
