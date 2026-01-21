/*
Name:                source_of_goods_range_fact
APPID-Name:          APP09479
Purpose:             Identify source of goods for limited time ranges
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
 * This code must be run for all dates in the source of goods date range.
 * We do not need to run it for dates in the lookback range.
 * Ex: if we want to determine SOG as of each date in FY24 Jun YTD, 
 *    and we are using a 12 month lookback period,
 *    then we would need to run this entire code set for FY24 Feb through FY24 Jun
 *    because the earliest date in the determination period is the first day of FY24 Feb. 
 * 
 */


/*
 * LIMITED DATE RANGE
 * 
 * Include only the dates as of which we want to determine source of goods on this run-through of the code.
 * The lookback range may extend beyond these dates, which is ok.
 * Recommended: max one quarter per run
 *
 * Unique record: Day
 */
--drop table date_range_limited;
create volatile multiset table date_range_limited as (

   select
      cal.day_date
      
   from prd_nap_base_vws.day_cal_454_dim as cal
   
   where 1=1
      and cal.day_date between 
	  (SELECT MIN(DAY_DATE) FROM PRD_nap_base_vws.day_cal_454_dim WHERE week_idnt = (SELECT week_num FROM {dsa_ai_secure_schema}.sog_backfill_lkup))
	  and 
	  (SELECT MAX(DAY_DATE) FROM PRD_nap_base_vws.day_cal_454_dim WHERE week_idnt = (SELECT week_num FROM {dsa_ai_secure_schema}.sog_backfill_lkup))
      
)
with data 
unique primary index(day_date) 
on commit preserve rows
;
 
collect statistics
   column(day_date)
on date_range_limited
;


/*
 * INPUTS
 *
 * Hard-coded here rather than variables because a change to these values would be a change in methodology.
 * These values would not change for other reasons, such as backfilling. 
 * 
 * Lookback Days
 * Number of prior days we want to include when determining source of goods as of date X
 *
 * Percent To Be Primary
 * The percentage to be primary determines how large a single Source of Goods must be
 * for us to declare it as the Primary source.
 * --95% was an arbitrary figure, chosen to be a very high bar, but not high enough to catch low-volume activity
 *   such as occasional units that end up in a particular location for unknown reasons 
 * --The value must be more than 50%
 *   Later code that identifies primary source of goods relies on one source being larger than any other
 *   and that can only happen if one source is more than 50% of total inflows.
 *
 */
--drop table inputs;
create volatile multiset table inputs as (

   select distinct
      364                              as lookback_days -- number of days in one normal fiscal year
      ,cast(0.9500 as decimal(18,4))   as percent_to_be_primary
       
   from prd_nap_base_vws.day_cal_454_dim cal -- can be any table
   
   where 1=1
      -- actual conditions don't matter, just selecting a small number of rows
      and week_idnt = 202401
      
)
with data 
on commit preserve rows
;
 
 
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
 * BASE TABLE FOR ALL POSSIBLE COMBINATIONS
 *
 * Creating this data set and joining inflow data to it later will ensure that 
 * - we do not accidentally miss any combinations
 * - we will not have any gaps in our date range when we collapse from SKU + Chan + Day to SKU + Chan + date range
 * 
 * One row for every possible SKU + Channel + Date. 
 * 
 * SKU + Channel
 * We use all possible combos regardless of date to ensure we don't miss any that occur only outside the backfill range.
 * We only need SKU + Channel combos that have inflows, so get the combos from the inflows data compiled previously. 
 * 
 * Date: each date for which we want to determine source of goods
 * - If we are determining source of goods for 30 days, we will have 30 values of Date
 * - Must include all dates, even those with no inflows, to ensure no gaps in our date range
 * 
 * We calculate the start of the lookback range (lookback_begin_date) 
 * - using the range length specified earlier in Inputs
 * - then limiting it to the earliest date we have inflows data (no need to create a record for a date before our inflows data begins)
 * 
 * Unique record: SKU + Channel + Lookback End Date
 *
 */

-- create volatile table separately to enforce not null
create volatile multiset table dimension_base (
   rms_sku_num             varchar(16) character set unicode not casespecific not null 
   ,channel_num            integer not null
   ,lookback_begin_date    date format 'yyyy-mm-dd' not null
   ,lookback_end_date      date format 'yyyy-mm-dd' not null
)
unique primary index(rms_sku_num, channel_num, lookback_end_date)
on commit preserve rows
;

--drop table dimension_base;
insert into dimension_base

   with early_bound as (
   
      select min(day_date) as min_date_with_inflows
      
      from {dsa_ai_secure_schema}.source_of_goods_inflows_fact
      
   )
   ,sku_and_chan as (
   
      select distinct
         rms_sku_num
         ,channel_num
         
      from {dsa_ai_secure_schema}.source_of_goods_inflows_fact
      
   )
   select
      sc.rms_sku_num
      ,sc.channel_num
      -- lookback_begin_date is later of: earliest date in the range we specified or earliest date with inflows
      ,greatest(
         -- +1 ensures range will be the length specified in Inputs since between Date A and Date B is inclusive
         drl.day_date - (select lookback_days from inputs) +1
         ,(select min_date_with_inflows from early_bound)
       )                as lookback_begin_date
       -- lookback_end_date is the date as of which we want to determine Source of Goods
      ,drl.day_date     as lookback_end_date
      
   from sku_and_chan as sc
      cross join date_range_limited as drl
      
;

collect statistics
   -- high confidence  
   column(rms_sku_num, channel_num, lookback_begin_date, lookback_end_date)
   ,column(lookback_begin_date)
   ,column(lookback_end_date)
   ,column(rms_sku_num,channel_num)
   -- medium confidence
   ,column(rms_sku_num, channel_num, lookback_end_date)
on dimension_base
;

/*
 * INFLOWS DURING LOOKBACK PERIOD FOR EACH DATE
 *
 * For each SKU + Channel + Lookback End Date, sum up inflows during the lookback range.
 *
 * Ex: Lookback end date = 4/2/2024 and range = 364 days means lookback start date = 4/5/2023.
 *     This query will sum inflows for the period 4/5/2023 - 4/2/2024, inclusive,
 *     and will place those inflow sums on the row for lookback end date 4/2/2024.
 *
 * Unique record: SKU + Channel + Lookback End Date
 *  
 */
--drop table inflows_during_lookback;
create volatile multiset table inflows_during_lookback as (

   select
      d.rms_sku_num
      ,d.channel_num
      ,d.lookback_end_date
      ,coalesce(sum(i.received_units),0) + coalesce(sum(i.po_transfer_in_units),0)  as po_inflow_u
      ,coalesce(sum(reserve_stock_transfer_in_units),0)                             as rs_inflow_u
      ,coalesce(sum(pack_and_hold_transfer_in_units),0)                             as ph_inflow_u
      ,coalesce(sum(racking_transfer_in_units),0)                                   as nx_inflow_u
      
   from dimension_base as d
      left join {dsa_ai_secure_schema}.source_of_goods_inflows_fact as i
         on i.rms_sku_num = d.rms_sku_num
         and i.channel_num = d.channel_num
         and i.day_date between d.lookback_begin_date and d.lookback_end_date
         
   group by 1,2,3
   
)
with data 
primary index(rms_sku_num, channel_num, lookback_end_date) 
on commit preserve rows
;

-- no stats recommended for collection


/*
 * SOURCE OF GOODS CALCULATIONS
 *
 * Rows that have total inflows < 0 will be classified as ZERO INFLOWS because we expect them to eventually get trued up to zero
 * - these will be rows where the SKU + Channel + Date had total inflows during the entire lookback period < 0 
 * 
 * Rare but possible situation: two types of inflows offset each other so that total inflows = 0
 * Example: 
 *    po_inflow_u = -1 and nx_inflow_u = 1 --> total_inflow_u = 0
 *    po_inflow_u_ind = 0, nx_inflow_u_ind = 1
 *    largest_inflow_u = nx_inflow_u
 *    po_inflow_u_proportion = 0, nx_inflow_u_proportion = 0 --> this happens because total_inflow_u = 0
 *    So at the end of all that: 
 *    - the row will have only one indicator (nx_inflow_u_ind)
 *    - the row will show source_of_goods_primary_code = NONE because none of the proportions were above the threshold 
 *    - so it looks like an error, but the result is as expected because PO and NX inflows exactly offset and Total Inflows = 0
 * 
 * Unique record: SKU + Channel + Lookback End Date
 * 
 */
--drop table source_of_goods_day_fact;
create volatile multiset table source_of_goods_day_fact as (

   select
      rms_sku_num
      ,channel_num
      ,lookback_end_date
      ,po_inflow_u
      ,rs_inflow_u
      ,ph_inflow_u
      ,nx_inflow_u
      ,po_inflow_u + rs_inflow_u + ph_inflow_u + nx_inflow_u  as total_inflow_u
       
      -- Indicator for each type of inflow that occurred during the lookback period
      ,case when po_inflow_u <> 0 then 1 else 0 end  as po_inflow_u_ind
      ,case when rs_inflow_u <> 0 then 1 else 0 end  as rs_inflow_u_ind
      ,case when ph_inflow_u <> 0 then 1 else 0 end  as ph_inflow_u_ind
      ,case when nx_inflow_u <> 0 then 1 else 0 end  as nx_inflow_u_ind
       
      -- idenfity inflow units from largest source
      -- ok to have largest inflow be negative; negatives are handled when determining primary source
      ,greatest(po_inflow_u, rs_inflow_u, ph_inflow_u, nx_inflow_u) as largest_inflow_u
       
      -- use the order of whens to determine priority if there is a tie in inflow units from largest source
      ,case
         when largest_inflow_u = 0 then 'ZERO INFLOWS'
         when largest_inflow_u = nx_inflow_u then 'NORDSTROM EXHAUST'
         when largest_inflow_u = rs_inflow_u then 'RESERVE STOCK'
         when largest_inflow_u = ph_inflow_u then 'PACK AND HOLD'
         when largest_inflow_u = po_inflow_u then 'PURCHASE ORDER'
         else 'NO PRIMARY SOURCE'
      end as source_of_goods_largest
      
      -- idenfity primary source
      ,case
         when total_inflow_u <= 0 then 0
         else po_inflow_u*1.0000 / total_inflow_u*1.0000
      end as po_inflow_u_proportion
      ,case
         when total_inflow_u <= 0 then 0     
         else rs_inflow_u*1.0000 / total_inflow_u*1.0000
      end as rs_inflow_u_proportion
      ,case
         when total_inflow_u <= 0 then 0     
         else ph_inflow_u*1.0000 / total_inflow_u*1.0000
      end as ph_inflow_u_proportion
      ,case
         when total_inflow_u <= 0 then 0     
         else nx_inflow_u*1.0000 / total_inflow_u*1.0000
      end as nx_inflow_u_proportion
      ,case
         when largest_inflow_u = 0 then 'ZERO INFLOWS'
         when po_inflow_u_proportion >= (select percent_to_be_primary from inputs) then 'PURCHASE ORDER'
         when rs_inflow_u_proportion >= (select percent_to_be_primary from inputs) then 'RESERVE STOCK'
         when ph_inflow_u_proportion >= (select percent_to_be_primary from inputs) then 'PACK AND HOLD'
         when nx_inflow_u_proportion >= (select percent_to_be_primary from inputs) then 'NORDSTROM EXHAUST'
         else 'NO PRIMARY SOURCE'
      end as source_of_goods_primary
       
      -- determine relationship between Primary and Largest source
      ,case
         when largest_inflow_u = 0 then 'ZERO INFLOWS'
         when largest_inflow_u = total_inflow_u then 'ONE ONLY'
         when source_of_goods_largest = source_of_goods_primary then 'ONE PRIMARY'
         when source_of_goods_largest <> source_of_goods_primary then 'ZERO PRIMARY'
         else 'UNDEFINED'
      end as source_of_goods_status
       
   from inflows_during_lookback
   
)
with data 
primary index(rms_sku_num, channel_num, lookback_end_date) 
on commit preserve rows
;

collect statistics
   -- high confidence  
   column(
      rms_sku_num, channel_num, source_of_goods_status,
      source_of_goods_primary, source_of_goods_largest 
   )
   ,column(rms_sku_num, channel_num)
   ,column(lookback_end_date)
   -- medium confidence  
   ,column(rms_sku_num, channel_num, lookback_end_date)
on source_of_goods_day_fact
;


/*
 * DROP VOLATILE TABLES THAT WE NO LONGER NEED
 */
drop table dimension_base;
drop table inflows_during_lookback;


/*
 * COLLAPSE SOURCE OF GOODS BASE DATA INTO TIME RANGES
 *
 * This is a "gaps and islands" problem
 * Solve via method described here: https://mattboegner.com/improve-your-sql-skills-master-the-gaps-islands-problem/
 *
 * Unique record: SKU + Channel + Eff Begin Date
 *
 */
-- drop table source_of_goods_range_calc
create volatile multiset table source_of_goods_range_calc as (

   with rankings as (
   
      select
         rms_sku_num
         ,channel_num
         ,source_of_goods_largest
         ,source_of_goods_primary
         ,source_of_goods_status
         ,lookback_end_date
         ,dense_rank() over (
            partition by
               rms_sku_num
               ,channel_num
            order by lookback_end_date
         )                                                  as rank_dimensions
         ,dense_rank() over (
            partition by
            rms_sku_num
            ,channel_num
            ,source_of_goods_largest
            ,source_of_goods_primary
            ,source_of_goods_status
            order by lookback_end_date
         )                                                  as rank_changes_within_dimensions
         ,rank_dimensions - rank_changes_within_dimensions  as sequence_grouping
         
      from source_of_goods_day_fact
      
   )
   select
      rms_sku_num
      ,channel_num
      ,source_of_goods_largest
      ,source_of_goods_primary
      ,source_of_goods_status
      ,min(lookback_end_date) as eff_begin_date
      ,max(lookback_end_date) as eff_end_date
      
   from rankings
   
   -- grouping by sequence_grouping collapses from dates to date ranges
   group by 1,2,3,4,5,sequence_grouping
   
)
with data
primary index(rms_sku_num, channel_num, eff_begin_date)
on commit preserve rows
;

collect statistics
   -- high confidence
   column(
      rms_sku_num, channel_num, eff_begin_date, eff_end_date, 
      source_of_goods_largest, source_of_goods_primary, source_of_goods_status 
   )
   ,column(eff_begin_date)
   ,column(eff_end_date)
   -- medium confidence
   ,column(rms_sku_num, channel_num, eff_begin_date)
on source_of_goods_range_calc
;


/*
 * AUGMENT TIME RANGE DATA WITH INFO ABOUT EACH RANGE
 *
 * Smallest proportion in the date range: lower bound on that proportion
 * Ex: if the smallest proportion of Pack & Hold inflows is 0.0234
 *     we know that across that date range the % of P&H inflows was always 2.34% or greater
 *
 * Largest proportion in the date range: upper bound on that proportion
 * Ex: if the largest proportion of Pack & Hold inflows is 0.9463
 *     we know that across that date range the % of P&H inflows was always 94.63% or less
 * 
 * Unique record: SKU + Channel + Eff Begin Date
 *     
 */
--drop table source_of_goods_limited_time_range;
create volatile multiset table source_of_goods_limited_time_range as (

   select
      r.rms_sku_num
      ,r.channel_num
      ,r.source_of_goods_largest
      ,r.source_of_goods_primary
      ,r.source_of_goods_status
      ,r.eff_begin_date
      ,r.eff_end_date
 
      -- capture smallest and largest proportions in the date range
      ,min(b.po_inflow_u_proportion) as po_inflow_u_proportion_min
      ,max(b.po_inflow_u_proportion) as po_inflow_u_proportion_max
       
      ,min(b.rs_inflow_u_proportion) as rs_inflow_u_proportion_min
      ,max(b.rs_inflow_u_proportion) as rs_inflow_u_proportion_max
       
      ,min(b.ph_inflow_u_proportion) as ph_inflow_u_proportion_min
      ,max(b.ph_inflow_u_proportion) as ph_inflow_u_proportion_max
       
      ,min(b.nx_inflow_u_proportion) as nx_inflow_u_proportion_min
      ,max(b.nx_inflow_u_proportion) as nx_inflow_u_proportion_max
       
      -- max(1,0) will be 1 if that inflow was present in any of the lookback ranges for any of the effective dates
      ,max(b.po_inflow_u_ind)        as po_inflow_u_ind
      ,max(b.rs_inflow_u_ind)        as rs_inflow_u_ind
      ,max(b.ph_inflow_u_ind)        as ph_inflow_u_ind
      ,max(b.nx_inflow_u_ind)        as nx_inflow_u_ind
           
   from source_of_goods_range_calc as r
      left join source_of_goods_day_fact as b
         on b.rms_sku_num = r.rms_sku_num
         and b.channel_num = r.channel_num
         and b.lookback_end_date between r.eff_begin_date and r.eff_end_date
         
   group by 1,2,3,4,5,6,7
   
)
with data
primary index(rms_sku_num, channel_num, eff_begin_date)
on commit preserve rows
;

collect statistics
   -- high confidence
   column(
      rms_sku_num, channel_num, eff_begin_date, eff_end_date, 
      source_of_goods_largest, source_of_goods_primary, source_of_goods_status 
   )
   ,column(eff_begin_date)
   ,column(eff_end_date)
   -- medium confidence
   ,column(rms_sku_num, channel_num, eff_begin_date)
on source_of_goods_limited_time_range
;


/*
 * POPULATE STATIC TABLES
 * 1) Delete old records that fall within the refresh range
 * 2) Add new records that fall within the refresh range
 *
 */

delete 

   from {dsa_ai_secure_schema}.source_of_goods_range_fact

   where eff_begin_date between 
   
   (SELECT MIN(DAY_DATE) FROM PRD_nap_base_vws.day_cal_454_dim WHERE week_idnt = (SELECT week_num FROM {dsa_ai_secure_schema}.sog_backfill_lkup))
	and 
	(SELECT MAX(DAY_DATE) FROM PRD_nap_base_vws.day_cal_454_dim WHERE week_idnt = (SELECT week_num FROM {dsa_ai_secure_schema}.sog_backfill_lkup))

;

insert into {dsa_ai_secure_schema}.source_of_goods_range_fact (
   rms_sku_num
   ,channel_num
   ,source_of_goods_largest
   ,source_of_goods_primary
   ,source_of_goods_status
   ,eff_begin_date
   ,eff_end_date
   ,po_inflow_units_proportion_min
   ,po_inflow_units_proportion_max
   ,rs_inflow_units_proportion_min
   ,rs_inflow_units_proportion_max
   ,ph_inflow_units_proportion_min
   ,ph_inflow_units_proportion_max
   ,nx_inflow_units_proportion_min
   ,nx_inflow_units_proportion_max
   ,po_inflow_units_ind
   ,rs_inflow_units_ind
   ,ph_inflow_units_ind
   ,nx_inflow_units_ind
   ,dw_sys_load_tmstp
)

   select 
      rms_sku_num
      ,channel_num
      ,source_of_goods_largest
      ,source_of_goods_primary
      ,source_of_goods_status
      ,eff_begin_date
      ,eff_end_date
      ,po_inflow_u_proportion_min   as po_inflow_units_proportion_min
      ,po_inflow_u_proportion_max   as po_inflow_units_proportion_max
      ,rs_inflow_u_proportion_min   as rs_inflow_units_proportion_min
      ,rs_inflow_u_proportion_max   as rs_inflow_units_proportion_max
      ,ph_inflow_u_proportion_min   as ph_inflow_units_proportion_min
      ,ph_inflow_u_proportion_max   as ph_inflow_units_proportion_max
      ,nx_inflow_u_proportion_min   as nx_inflow_units_proportion_min
      ,nx_inflow_u_proportion_max   as nx_inflow_units_proportion_max
      ,po_inflow_u_ind              as po_inflow_units_ind
      ,rs_inflow_u_ind              as rs_inflow_units_ind
      ,ph_inflow_u_ind              as ph_inflow_units_ind
      ,nx_inflow_u_ind              as nx_inflow_units_ind
      ,current_timestamp            as dw_sys_load_tmstp

   from source_of_goods_limited_time_range as f

;

-- stats to collect are defined in DDL
