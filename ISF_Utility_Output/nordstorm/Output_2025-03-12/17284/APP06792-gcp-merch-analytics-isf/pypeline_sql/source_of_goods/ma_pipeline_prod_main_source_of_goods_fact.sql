/*
Name:                source_of_goods_fact
APPID-Name:          APP09479
Purpose:             Identify source of goods for each time range
Variable(s):         {{dsa_ai_secure_schema}} PROTO_DSA_AI_BASE_VWS for dev or PRD_NAP_DSA_AI_BASE_VWS for prod
DAG: 
Author(s):           Jevon Barlas
Date Created:        9/27/2024
Date Last Updated:   9/27/2024
*/


/*
 * INPUTS
 *
 * End Of All Time
 * The end date for time range records that are effective as of the most recent calculation date
 * 
 * Latest SOG Date
 * Latest date as of which to determine source of goods (pipeline will run weekly)
 *
 */
--drop table inputs;
create volatile multiset table inputs as (

   select
      cast('9999-12-31' as date)                                                          as end_of_all_time
      ,(select max(eff_end_date) from {dsa_ai_secure_schema}.source_of_goods_range_fact)  as latest_sog_date
       
   from prd_nap_base_vws.day_cal_454_dim as cal
   
   -- actual date doesn't matter; just selecting a single row from the table
   where cal.day_date = current_date()
      
)
with data 
on commit preserve rows
;
 

/*
 * COLLAPSE COMPILED TIME RANGE DATA INTO FINAL TIME RANGES
 * 
 * PART 1 of 2
 *
 * This is a "gaps and islands" problem
 * Solve via method described here: https://mattboegner.com/improve-your-sql-skills-master-the-gaps-islands-problem/
 *
 * This is the same approach we used when compiling the original time range data.
 * - That original data had one row per limited time period (e.g. one row per quarter).
 * - This code runs on the that entire compiled data set and collapses rows into the final time ranges.
 * - This code splits the query into two separate volatile tables for ease of understanding. 
 * 
 * Unique record: SKU + Channel + Eff Begin Date
 *
 */

--drop table rankings;
create volatile multiset table rankings as (

   select
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
      
      -- fields to identify records that can be consolidated
      ,row_number() over(
         partition by
            rms_sku_num
            ,channel_num
         order by eff_begin_date      
      )                                                     as row_num_within_sku_chan
      ,dense_rank() over (
         partition by
            rms_sku_num
            ,channel_num
            ,source_of_goods_largest
            ,source_of_goods_primary
            ,source_of_goods_status
         order by eff_begin_date
      )                                                     as change_in_largest_source
      ,row_num_within_sku_chan - change_in_largest_source   as sequence_grouping
      
   from {dsa_ai_secure_schema}.source_of_goods_range_fact
 
)
with data
primary index(rms_sku_num, channel_num, eff_begin_date)
on commit preserve rows
;

collect statistics
   -- high confidence
   column(
      rms_sku_num, channel_num, sequence_grouping
      ,source_of_goods_primary, source_of_goods_largest, source_of_goods_status 
   )
   -- medium confidence
   ,column(channel_num)
   ,column(sequence_grouping)
on rankings
;


/*
 * COLLAPSE COMPILED TIME RANGE DATA INTO FINAL TIME RANGES
 *
 * PART 2 of 2
 *
 * Use sequence grouping from Part 1 to identify distinct time ranges
 * 
 * Using min & max for proportions and indicators gives same result as if we had filled entire date range at once 
 * 
 * Unique record: SKU + Channel + Eff Begin Date
 *
 */
--drop table source_of_goods_range;
create volatile multiset table source_of_goods_range as (
    
   select
      r.rms_sku_num
      ,r.channel_num
      ,r.source_of_goods_largest
      ,r.source_of_goods_primary
      ,r.source_of_goods_status
      ,min(r.eff_begin_date)                 as eff_begin_date
      -- if date is latest possible, replace it with end of all time to indicate that record is still in effect
      ,case 
         when max(r.eff_end_date) = (select latest_sog_date from inputs) then (select end_of_all_time from inputs)
         else max(r.eff_end_date)
       end                                   as eff_end_date
      
      -- capture smallest and largest proportions across entire date range
      ,min(r.po_inflow_units_proportion_min) as po_inflow_units_proportion_lower_bound 
      ,max(r.po_inflow_units_proportion_max) as po_inflow_units_proportion_upper_bound 
      ,min(r.rs_inflow_units_proportion_min) as rs_inflow_units_proportion_lower_bound
      ,max(r.rs_inflow_units_proportion_max) as rs_inflow_units_proportion_upper_bound
      ,min(r.ph_inflow_units_proportion_min) as ph_inflow_units_proportion_lower_bound
      ,max(r.ph_inflow_units_proportion_max) as ph_inflow_units_proportion_upper_bound
      ,min(r.nx_inflow_units_proportion_min) as nx_inflow_units_proportion_lower_bound
      ,max(r.nx_inflow_units_proportion_max) as nx_inflow_units_proportion_upper_bound
      
      -- max(1,0) will be 1 if that inflow was present in any of the ranges
      ,max(r.po_inflow_units_ind)            as po_inflow_units_ind 
      ,max(r.rs_inflow_units_ind)            as rs_inflow_units_ind
      ,max(r.ph_inflow_units_ind)            as ph_inflow_units_ind
      ,max(r.nx_inflow_units_ind)            as nx_inflow_units_ind
      
      ,max(r.po_inflow_units_ind) 
       + max(r.rs_inflow_units_ind)
       + max(r.ph_inflow_units_ind) 
       + max(r.nx_inflow_units_ind)          as source_of_goods_count
      
   from rankings as r
   
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
      source_of_goods_primary, source_of_goods_largest, source_of_goods_status
   )
   ,column(eff_begin_date)
   ,column(eff_end_date)
   -- medium confidence
   ,column(rms_sku_num, channel_num, eff_begin_date)
on source_of_goods_range
;


/*
 * POPULATE STATIC TABLES
 *
 * Delete all contents of table and re-populate
 */

delete 

   from {dsa_ai_secure_schema}.source_of_goods_fact

;

insert into {dsa_ai_secure_schema}.source_of_goods_fact (
   rms_sku_num
   ,channel_num
   ,source_of_goods_largest
   ,source_of_goods_primary
   ,source_of_goods_status
   ,eff_begin_date
   ,eff_end_date
   ,po_inflow_units_proportion_lower_bound
   ,po_inflow_units_proportion_upper_bound
   ,rs_inflow_units_proportion_lower_bound
   ,rs_inflow_units_proportion_upper_bound
   ,ph_inflow_units_proportion_lower_bound
   ,ph_inflow_units_proportion_upper_bound
   ,nx_inflow_units_proportion_lower_bound
   ,nx_inflow_units_proportion_upper_bound
   ,po_inflow_units_ind
   ,rs_inflow_units_ind
   ,ph_inflow_units_ind
   ,nx_inflow_units_ind
   ,source_of_goods_count
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
      ,po_inflow_units_proportion_lower_bound
      ,po_inflow_units_proportion_upper_bound
      ,rs_inflow_units_proportion_lower_bound
      ,rs_inflow_units_proportion_upper_bound
      ,ph_inflow_units_proportion_lower_bound
      ,ph_inflow_units_proportion_upper_bound
      ,nx_inflow_units_proportion_lower_bound
      ,nx_inflow_units_proportion_upper_bound
      ,case when f.po_inflow_units_ind = 1 then 'Y' else 'N' end as po_inflow_units_ind
      ,case when f.rs_inflow_units_ind = 1 then 'Y' else 'N' end as rs_inflow_units_ind
      ,case when f.ph_inflow_units_ind = 1 then 'Y' else 'N' end as ph_inflow_units_ind
      ,case when f.nx_inflow_units_ind = 1 then 'Y' else 'N' end as nx_inflow_units_ind
      ,source_of_goods_count
      ,current_timestamp as dw_sys_load_tmstp

   from source_of_goods_range as f

;

-- stats to collect are defined in DDL
