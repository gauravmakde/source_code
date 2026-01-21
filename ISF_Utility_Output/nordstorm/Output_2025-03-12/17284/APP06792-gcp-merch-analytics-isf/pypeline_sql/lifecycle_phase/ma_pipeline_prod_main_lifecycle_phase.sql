/*
Name: Lifecycle Phase
APPID-Name: APP09478
Purpose: Populate tables that capture each lifecycle phase (reg price, first mark, remark, etc.) for an item, and the range of dates the item is in that phase.
Variable(s):    {{environment_schema}} t2dl_das_in_season_management_reporting 
                {{env_suffix}} dev or prod as appropriate
DAG: 
Author(s): Jevon Barlas
Date Created: 7/31/2024
Date Last Updated: 7/31/2024
*/

/*
 * BANNER TO CHANNEL MAPPING
 * Used near the end of the query sequence to translate from banner to channel
 * Adding channel in order to make joins to Source of Goods easier
 * 
 * Selling and warehouse channels only  
 * 
 */

create multiset volatile table channel_lkup as (

   select distinct 
      channel_num
      ,channel_brand as banner
      
   from prd_nap_usr_vws.price_store_dim_vw
   
   where channel_num in (110,120,210,220,250,260,310)
   
)
with data
unique primary index (channel_num)
on commit preserve rows
;

collect statistics
   column(channel_num)
   ,column(channel_num, banner)
on channel_lkup
;


/*
 * BASE TABLE OF PRICE INFORMATION
 * Prepare all record-level data needed to eventually collapse into time-range rows
 * 
 * price_change fields are used in the next query to identify rows that can/cannot be grouped together
 * 
 * grain: cc, banner, one of (pst_eff_begin_date or pst_eff_end_date)
 * 
 */
--drop table price_sequencing;
create multiset volatile table price_sequencing as (

   select
      cc
      ,channel_brand                as banner
      
      -- core price history fields
      ,case ownership_retail_price_type_code
         when 'REGULAR' then 'R'
         when 'CLEARANCE' then 'C'
       end                          as ownership_price_type_code
      ,last_md_version              as last_markdown_version
      ,regular_price_amt            as regular_price
      ,ownership_retail_price_amt   as ownership_price
      
      -- identify price changes to be used 
      ,lag(regular_price, 1) over(
         partition by cc, banner
         order by pst_eff_begin_date
       )                               as previous_regular_price
      ,case 
         when regular_price = previous_regular_price then 0 
         else 1 
       end                             as regular_price_change
      ,lag(ownership_price, 1) over(
         partition by cc, banner
         order by pst_eff_begin_date
       )                               as previous_ownership_price
      ,case 
         when ownership_price = previous_ownership_price then 0 
         else 1 
       end                             as ownership_price_change
      
      -- row-level effective time & dates
      ,eff_begin_tmstp
      ,eff_end_tmstp
      ,pst_eff_begin_date
      ,pst_eff_end_date
   
      -- lifecycle phase dimensions
      ,case channel_brand 
         when 'NORDSTROM' then
            case lc_flag
               when 1 then 8
               when 0 then
                  case ownership_retail_price_type_code
                     when 'REGULAR' then 1
                     when 'CLEARANCE' then
                        case
                           when racked_flag = 1 then 4
                           when last_md_version = 1 then 2
                           when last_md_version > 1 then 3
                        end
                  end
            end
         when 'NORDSTROM_RACK' then
            case lc_flag
               when 1 then 8
               when 0 then
                  case ownership_retail_price_type_code
                     when 'REGULAR' then 5
                     when 'CLEARANCE' then
                        case
                           when last_md_version = 1 then 6
                           when last_md_version > 1 then 7
                        end
                  end
            end
      end as lifecycle_phase_number
      ,trim(both from lifecycle_phase_number) || '. ' ||
         case lifecycle_phase_number
            when 1 then 'NORD REGULAR PRICE'
            when 2 then 'NORD FIRST MARK'
            when 3 then 'NORD REMARK'
            when 4 then 'RACKING'
            when 5 then 'RACK REGULAR PRICE'
            when 6 then 'RACK FIRST MARK'
            when 7 then 'RACK REMARK'
            when 8 then 'LAST CHANCE'
         end
         as lifecycle_phase_name
         
      -- lifecycle phase effective dates
      -- need to include cycle_pst_eff_begin_dt because one CC can have multiple lifecycles
      ,first_value(pst_eff_begin_date ignore nulls) over (
         partition by cc, banner, lifecycle_phase_number, cycle_pst_eff_begin_dt
         order by pst_eff_begin_date
         rows between unbounded preceding and current row
       ) as lifecycle_phase_pst_eff_begin_date
      ,last_value(pst_eff_end_date ignore nulls) over (
      -- need to include cycle_pst_eff_begin_dt because one CC can have multiple lifecycles
         partition by cc, banner, lifecycle_phase_number, cycle_pst_eff_begin_dt
         order by pst_eff_end_date
         rows between current row and unbounded following
       ) as lifecycle_phase_pst_eff_end_date
      
      -- banner lifecycle dimensions
      ,dense_rank() over (
         partition by cc, banner
         order by cc, banner, cycle_pst_eff_begin_dt
      )                          as banner_lifecycle_number
      
      -- banner lifecycle effective dates
      ,cycle_pst_eff_begin_dt    as banner_lifecycle_pst_eff_begin_date
      ,cycle_pst_eff_end_dt      as banner_lifecycle_pst_eff_end_date
   
   from t2dl_das_markdown.cc_price_hist as prc

   where 1=1
      and channel_country = 'US'
      
)
with data
primary index (cc, banner, pst_eff_begin_date)
on commit preserve rows
;

collect statistics
   -- high confidence
   column(cc, banner)
   -- medium confidence
   ,column(cc, banner, last_markdown_version, regular_price, ownership_price, pst_eff_begin_date)
on price_sequencing
;


/*
 * IDENTIFY GROUPS FOR SEQUENCING THE DATE FIELDS
 * 
 * A price can change from A to B then back to A, and we need to distinguish those two A periods as different periods.
 * We do this by identifying any price changes in the prior query, then doing a running total in this query.
 * 
 * grain: cc, banner, one of (pst_eff_begin_date or pst_eff_end_date)
 * 
 */
--drop table sequence_grouping;
create multiset volatile table sequence_grouping as (

   select
      price_sequencing.*
      ,sum(regular_price_change) over (
         partition by cc, banner
         order by pst_eff_begin_date
         rows between unbounded preceding and current row
       ) as regular_price_counter
      ,sum(ownership_price_change) over (
         partition by cc, banner
         order by pst_eff_begin_date
         rows between unbounded preceding and current row
       ) as ownership_price_counter
      ,regular_price_counter + ownership_price_counter as sequence_group
      
   from price_sequencing
   
)
with data
primary index (cc, banner, pst_eff_begin_date)
on commit preserve rows
;

collect statistics
   -- high confidence
   column(
      cc, banner, ownership_price_type_code, last_markdown_version, regular_price, ownership_price
      ,lifecycle_phase_number, lifecycle_phase_name, banner_lifecycle_number
      ,sequence_group
    )
   -- medium confidence
   ,column(banner_lifecycle_number)
   ,column(lifecycle_phase_number)
   ,column(last_markdown_version)
   ,column(regular_price)
   ,column(ownership_price)
   ,column(sequence_group)
on sequence_grouping
;


/*
 * COLLAPSE ROWS TO FINAL DATE RANGES 
 * 
 * Group by all dimensions to appear in final table, and also by sequence_group to ensure contiguous date ranges
 * 
 * grain: cc, banner, one of (pst_eff_begin_date or pst_eff_end_date)
 * 
 */
--drop table final_ranges;
create multiset volatile table final_ranges as (

   select
      sg.cc
      ,sg.banner
      ,sg.ownership_price_type_code
      ,sg.last_markdown_version
      ,sg.regular_price
      ,sg.ownership_price
      ,sg.lifecycle_phase_number
      ,sg.lifecycle_phase_name
      ,sg.banner_lifecycle_number
      ,min(sg.pst_eff_begin_date)                    as pst_eff_begin_date 
      ,max(sg.pst_eff_end_date)                      as pst_eff_end_date
      ,min(sg.lifecycle_phase_pst_eff_begin_date)    as lifecycle_phase_pst_eff_begin_date
      ,max(sg.lifecycle_phase_pst_eff_end_date)      as lifecycle_phase_pst_eff_end_date
      ,min(sg.banner_lifecycle_pst_eff_begin_date)   as banner_lifecycle_pst_eff_begin_date
      ,max(sg.banner_lifecycle_pst_eff_end_date)     as banner_lifecycle_pst_eff_end_date
      
   from sequence_grouping as sg
   
   group by sequence_group,1,2,3,4,5,6,7,8,9
   
)
with data
primary index (cc, banner, pst_eff_begin_date)
on commit preserve rows
;

collect statistics
   -- high confidence
   column(cc)
   ,column(cc, banner, banner_lifecycle_number)
on final_ranges
;


/*
 * ADD FLAGS TO IDENTIFY WHETHER EACH PHASE OCCURRED DURING A BANNER LIFECYCLE 
 * 
 * grain: cc, banner, one of (pst_eff_begin_date or pst_eff_end_date)
 */
-- drop table final_data_cc_banner
create multiset volatile table final_data_cc_banner as (

   with cc_list as (
   
      select distinct 
         concat(trim(both from sku.rms_style_num), '-', trim(both from cast(coalesce(sku.color_num,0) as smallint))) as cc
         ,sku.rms_style_num
         ,case length(sku.color_num)
            when 1 then '00' || sku.color_num
            when 2 then '0' || sku.color_num
            when 3 then sku.color_num
          end as color_num
         
      from prd_nap_usr_vws.product_sku_dim_vw as sku
      
      where sku.channel_country = 'US'
      
   )
   select 
      f.cc
      ,c.rms_style_num
      ,c.color_num
      ,f.banner
      ,f.ownership_price_type_code
      ,f.last_markdown_version
      ,f.regular_price
      ,f.ownership_price
      ,f.pst_eff_begin_date 
      ,f.pst_eff_end_date
      ,f.lifecycle_phase_number
      ,f.lifecycle_phase_name
      ,f.lifecycle_phase_pst_eff_begin_date
      ,f.lifecycle_phase_pst_eff_end_date
      ,f.banner_lifecycle_number
      ,f.banner_lifecycle_pst_eff_begin_date
      ,f.banner_lifecycle_pst_eff_end_date
      ,max(
         case 
            when f.lifecycle_phase_number = 1 or f.lifecycle_phase_number = 5 then 1 
            else 0
         end
      ) over(partition by f.cc, f.banner, f.banner_lifecycle_number) as ind_reg_price_this_banner_cycle
      ,max(
         case 
            when f.lifecycle_phase_number = 2 or f.lifecycle_phase_number = 6 then 1 
            else 0
         end
      ) over(partition by f.cc, f.banner, f.banner_lifecycle_number) as ind_first_mark_this_banner_cycle
      ,max(
            case 
               when f.lifecycle_phase_number = 3 or f.lifecycle_phase_number = 7 then 1 
               else 0
            end
      ) over(partition by f.cc, f.banner, f.banner_lifecycle_number) as ind_remark_this_banner_cycle
      ,max(
         case 
            when f.lifecycle_phase_number = 4 then 1 
            else 0
         end
      ) over(partition by f.cc, f.banner, f.banner_lifecycle_number) as ind_racking_this_banner_cycle
      ,max(
         case 
            when f.lifecycle_phase_number = 8 then 1 
            else 0
         end
      ) over(partition by f.cc, f.banner, f.banner_lifecycle_number) as ind_last_chance_this_banner_cycle
      
   from final_ranges as f
      inner join cc_list as c
         on c.cc = f.cc

)
with data
primary index (cc, banner, pst_eff_begin_date)
on commit preserve rows
;

-- no stats recommended


/*
 * EXPAND TO SKU + Channel
 * All SKUs within a CC will have same data
 * All selling channels within a banner will have same data
 * 
 * Also reorder columns
 * 
 * grain: sku, channel, one of (pst_eff_begin_date or pst_eff_end_date)
 */
--drop table final_data;
create multiset volatile table final_data_sku_channel as (

   select
      sku.rms_sku_num
      ,f.cc
      ,f.rms_style_num
      ,f.color_num
      ,f.banner
      ,ch.channel_num
      ,f.ownership_price_type_code
      ,f.last_markdown_version
      ,f.regular_price
      ,f.ownership_price
      ,f.pst_eff_begin_date 
      ,f.pst_eff_end_date
      ,f.lifecycle_phase_number
      ,f.lifecycle_phase_name
      ,f.lifecycle_phase_pst_eff_begin_date
      ,f.lifecycle_phase_pst_eff_end_date
      ,f.banner_lifecycle_number
      ,f.banner_lifecycle_pst_eff_begin_date
      ,f.banner_lifecycle_pst_eff_end_date
      ,f.ind_reg_price_this_banner_cycle
      ,f.ind_first_mark_this_banner_cycle
      ,f.ind_remark_this_banner_cycle
      ,f.ind_racking_this_banner_cycle
      ,f.ind_last_chance_this_banner_cycle
      
   from final_data_cc_banner as f
      inner join prd_nap_usr_vws.product_sku_dim_vw as sku
         on concat(trim(both from sku.rms_style_num), '-', trim(both from cast(coalesce(sku.color_num,0) as smallint))) = f.cc
         and sku.channel_country = 'US'
      inner join channel_lkup as ch
         on ch.banner = f.banner

)
with data
primary index (rms_sku_num, banner, pst_eff_begin_date)
on commit preserve rows
;

-- no stats recommended


/*
 * POPULATE STATIC TABLEs 
 */

--delete from t3dl_ace_pra.lifecycle_phase_cc_banner; -- this line exists only for testing purposes
delete from {environment_schema}.lifecycle_phase_cc_banner{env_suffix}
;

--insert into t3dl_ace_pra.lifecycle_phase_cc_banner; -- this line exists only for testing purposes
insert into {environment_schema}.lifecycle_phase_cc_banner{env_suffix}

   select 
      f.*
      ,current_timestamp

   from final_data_cc_banner as f

;

collect statistics
   column(cc, banner, pst_eff_begin_date)
   ,column(cc, banner)
   ,column(cc)
on {environment_schema}.lifecycle_phase_cc_banner{env_suffix}
;

--delete from t3dl_ace_pra.lifecycle_phase_sku_channel; -- this line exists only for testing purposes
delete from {environment_schema}.lifecycle_phase_sku_channel{env_suffix}
;

--insert into t3dl_ace_pra.lifecycle_phase_sku_channel; -- this line exists only for testing purposes
insert into {environment_schema}.lifecycle_phase_sku_channel{env_suffix}

   select 
      f.*
      ,current_timestamp

   from final_data_sku_channel as f

;

collect statistics
   column(rms_sku_num, channel_num, pst_eff_begin_date)
   ,column(rms_sku_num, channel_num)
   ,column(rms_sku_num, pst_eff_begin_date)
   ,column(rms_sku_num, banner, pst_eff_begin_date)
   ,column(rms_sku_num, banner)
   ,column(cc, banner, pst_eff_begin_date)
   ,column(cc, banner)
   ,column(cc, pst_eff_begin_date)
on {environment_schema}.lifecycle_phase_sku_channel{env_suffix}
;
