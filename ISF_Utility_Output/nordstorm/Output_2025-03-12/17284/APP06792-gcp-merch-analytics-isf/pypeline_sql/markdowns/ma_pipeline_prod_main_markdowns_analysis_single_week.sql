/*
Name:                markdowns_analysis_single_week
APPID-Name:          APP09527
Purpose:             Markdowns and Clearance analysis and reporting. 
                     Insert data into table t2dl_das_in_season_management_reporting.markdowns_analysis_single_week
Variable(s):         {{environment_schema}} t2dl_das_in_season_management_reporting 
                     {{env_suffix}} dev or prod as appropriate

DAG: 
Author(s):           Jevon Barlas
Date Created:        9/6/2024
Date Last Updated:   9/27/2024
*/

/*
* DETERMINE SNAPSHOT DATE
* 
* Last date of most recently completed fiscal week.
* Pipeline will run weekly.
* 
*/

create multiset volatile table snap_date as (

   select 
      max(day_date) as snapshot_date
      
   from prd_nap_usr_vws.day_cal_454_dim
   
   where week_end_day_date < current_date

)
with data on commit preserve rows
;


/*
* COPY NAP TABLE CONTENTS
* 
* - Explicitly select columns to protect against potential future changes
* - Select only the latest week in the table; in the future the table will have history, but TBD if that will become part of this pipeline or a different one.
* 
*/

create multiset volatile table cds as (

   select
      snapshot_date
      ,rms_style_num
      ,color_num
      ,channel_country
      ,channel_brand
      ,selling_channel
      ,first_receipt_date
      ,last_receipt_date
      ,available_to_sell
      ,first_sales_date
      ,weeks_sales
      ,total_sales_amt_1wk
      ,total_sales_units_1wk
      ,sell_thru_1wk
      ,total_sales_amt_2wks
      ,total_sales_units_2wks
      ,sell_thru_2wks
      ,total_sales_amt_4wks
      ,total_sales_units_4wks
      ,sell_thru_4wks
      ,total_sales_amt_6mons
      ,total_sales_units_6mons
      ,sell_thru_6mons
      ,sell_thru_since_last_markdown
      ,div_num
      ,div_desc
      ,grp_num
      ,grp_desc
      ,dept_num
      ,dept_desc
      ,class_num
      ,class_desc
      ,sbclass_num
      ,sbclass_desc
      ,style_group_num
      ,supp_part_num
      ,style_desc
      ,prmy_supp_num
      ,vendor_name
      ,vendor_label_name
      ,npg_ind
      ,msrp_amt
      ,anniversary_item_ind
      --,dual_exposure_ind
      ,is_online_purchasable
      ,online_purchasable_eff_begin_tmstp
      ,online_purchasable_eff_end_tmstp
      ,weighted_average_cost
      ,promotion_ind
      ,total_inv_qty
      ,total_inv_dollars
      ,reserve_stock_inventory
      ,reserve_stock_inventory_dollars
      --,warehouse_inventory
      --,warehouse_inventory_dollars
      ,pack_and_hold_inventory
      ,pack_and_hold_inventory_dollars
      ,on_order_units_qty
      ,reserve_stock_on_order_units_qty
      --,warehouse_on_order_units_qty
      ,pack_and_hold_on_order_units_qty
      ,first_clearance_markdown_date
      ,last_clearance_markdown_date
      ,clearance_markdown_version
      ,clearance_markdown_state
      --,clearance_markdown_percent_off
      ,future_clearance_markdown_date
      ,future_clearance_markdown_price
      --,future_clearance_markdown_percent_off
      ,regular_price_amt
      ,ownership_price_amt
      ,base_retail_drop_ship_amt
      --,regular_price_percent_off
      --,floor(f.compare_at_percent_off)*1.00/100 as compare_at_percent_off   -- convert from 51.99 to 0.51 
      ,current_price_amt
      ,current_rack_retail
      ,first_rack_date
      ,rack_ind
      ,supp_color
      ,drop_ship_eligible_ind
      ,return_disposition_code
      ,fp_replenishment_eligible_ind
      ,op_replenishment_eligible_ind
      ,selling_status_code
      ,price_variance_ind

   from prd_nap_usr_vws.clearance_markdown_insights_by_week_fact as f
   
   where 1=1
      and channel_country = 'US'
      and snapshot_date = ( select snapshot_date from snap_date ) 
      
)
with data on commit preserve rows
;

-- no stats recommended for collection


/*
* ADD FIELDS RELATED TO RECORD ID, KEY DIMENSIONS, MERCH HIERARCHY, PRODUCT ATTRIBUTES, PRODUCT STATUS, COST, PRICE, MARKDOWN INFO
* - reorder columns into topical sections for easier understanding of code
* - add more columns for reporting and analysis
* 
*/

create multiset volatile table add_product_info as (

   select
      /*
      * Record ID and Key Dimensions
      */
      snapshot_date
      ,channel_country
      ,channel_brand
      ,case channel_brand
         when 'NORDSTROM' then 'N'
         when 'NORDSTROM_RACK' then 'R'
       end                                                  as banner_code
      ,selling_channel
      ,rms_style_num || '-' || color_num                    as cc
      ,banner_code || '-' || selling_channel || '-' || cc   as banner_channel_cc
      ,channel_country || '-' || banner_channel_cc          as country_banner_channel_cc
      
      -- Data Science & Analytics removes leading zeroes from color for their CC field
      -- Creating this fields so we can more easily join to their data in the future
      ,trim(both from rms_style_num) || '-' || trim(both from cast(coalesce(color_num,0) as smallint)) as cc_dsci
      
      /*
      * Merch Hierarchy
      */
      ,div_num
      ,trim(both from div_num) || ', ' || div_desc          as div_label
      ,grp_num                                              as subdiv_num
      ,trim(both from grp_num) || ', ' || grp_desc          as subdiv_label
      ,dept_num
      ,trim(both from dept_num) || ', ' || dept_desc        as dept_label
      ,class_num
      ,trim(both from class_num) || ', ' || class_desc      as class_label
      ,sbclass_num                                          as subclass_num
      ,trim(both from sbclass_num) || ', ' || sbclass_desc  as subclass_label
      
      /*
      * Product Attributes
      */
      ,style_group_num
      ,rms_style_num
      ,color_num
      ,supp_part_num       as vpn
      ,supp_color
      ,style_desc
      ,prmy_supp_num
      ,vendor_name
      ,vendor_label_name
      ,npg_ind
      
      /*
      * Product Status
      */
      ,promotion_ind
      ,anniversary_item_ind
      ,case banner_code
         when 'N' then fp_replenishment_eligible_ind
         when 'R' then op_replenishment_eligible_ind
       end                                                  as replenishment_eligible_ind
      ,drop_ship_eligible_ind
      ,return_disposition_code
      ,selling_status_code                                  as selling_status_code_legacy
      ,first_rack_date
      ,rack_ind
      ,is_online_purchasable                                as online_purchasable_ind
      ,cast(online_purchasable_eff_begin_tmstp as date)     as online_purchasable_eff_begin_date
      ,cast(online_purchasable_eff_end_tmstp as date)       as online_purchasable_eff_end_date
      
      /*
      * Cost
      * Weighted Average Cost is defined as the cost at the SKU + Loc level. Any higher grain will be Average Unit Cost. 
      * Value in CDS table is up-leveled from SKU + Loc so should be called Average Unit Cost instead.
      * Value in CDS table retains four decimal places (same as WAC) but users of this data think of cost with two decimal places.
      * 
      * This cost is used later to calculate inventory value at cost, so rounding from four decimal places to two will introduce some variation.
      * Therefore, we will round cost at a later step after calculating inventory value.
      */
      ,weighted_average_cost                                            as average_unit_cost
      
      /*
      * Price Amounts, Price Bands
      */
      ,regular_price_amt                                                as price_base
      ,ownership_price_amt                                              as price_ownership
      ,coalesce(future_clearance_markdown_price, ownership_price_amt)   as price_ownership_future
      ,current_price_amt                                                as price_selling
      ,base_retail_drop_ship_amt                                        as price_base_drop_ship
      ,msrp_amt                                                         as price_compare_at
      ,current_rack_retail                                              as price_current_rack
      ,price_variance_ind                                               as price_variance_within_cc_ind
      
      -- next highest dollar fields will be used to join to price band table in later query
      ,ceiling(price_base)                                              as price_base_next_highest_dollar
      ,ceiling(price_ownership)                                         as price_ownership_next_highest_dollar
      ,ceiling(price_selling)                                           as price_selling_next_highest_dollar
      ,ceiling(price_ownership_future)                                  as price_ownership_future_next_highest_dollar

       /*
       * Markdown Info
       */
      ,first_clearance_markdown_date
      ,last_clearance_markdown_date
      ,future_clearance_markdown_date
      ,clearance_markdown_version
      ,clearance_markdown_state
      ,case 
         when future_clearance_markdown_price is not null then 'Y' 
         else 'N' 
       end                                                              as future_markdown_ind
      ,floor( (snapshot_date - last_clearance_markdown_date) / 7 )      as weeks_since_last_markdown
      
      /*
       * Remaining CDS fields to be handled in later queries
       */
      ,first_receipt_date
      ,last_receipt_date
      ,available_to_sell
      ,first_sales_date
      ,weeks_sales
      ,total_sales_amt_1wk
      ,total_sales_units_1wk
      ,sell_thru_1wk
      ,total_sales_amt_2wks
      ,total_sales_units_2wks
      ,sell_thru_2wks
      ,total_sales_amt_4wks
      ,total_sales_units_4wks
      ,sell_thru_4wks
      ,total_sales_amt_6mons
      ,total_sales_units_6mons
      ,sell_thru_6mons
      ,sell_thru_since_last_markdown
      ,total_inv_qty
      ,total_inv_dollars
      ,reserve_stock_inventory
      ,reserve_stock_inventory_dollars
      ,pack_and_hold_inventory
      ,pack_and_hold_inventory_dollars
      ,on_order_units_qty
      ,reserve_stock_on_order_units_qty
      ,pack_and_hold_on_order_units_qty

   from cds
      
)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

-- no stats recommended for collection


/*
* ADD FIELDS RELATED TO PERCENT OFF 
*/

create multiset volatile table add_percent_off as (

   select
      -- fields from prior query
      snapshot_date
      ,channel_country
      ,channel_brand
      ,banner_code
      ,selling_channel
      ,cc
      ,banner_channel_cc
      ,country_banner_channel_cc
      ,cc_dsci
      ,div_num
      ,div_label
      ,subdiv_num
      ,subdiv_label
      ,dept_num
      ,dept_label
      ,class_num
      ,class_label
      ,subclass_num
      ,subclass_label
      ,style_group_num
      ,rms_style_num
      ,color_num
      ,vpn
      ,supp_color
      ,style_desc
      ,prmy_supp_num
      ,vendor_name
      ,vendor_label_name
      ,npg_ind
      ,promotion_ind
      ,anniversary_item_ind
      ,replenishment_eligible_ind
      ,drop_ship_eligible_ind
      ,return_disposition_code
      ,selling_status_code_legacy
      ,first_rack_date
      ,rack_ind
      ,online_purchasable_ind
      ,online_purchasable_eff_begin_date
      ,online_purchasable_eff_end_date
      ,average_unit_cost
      ,price_base
      ,price_ownership
      ,price_ownership_future
      ,price_selling
      ,price_base_drop_ship
      ,price_compare_at
      ,price_current_rack
      ,price_variance_within_cc_ind
      ,price_base_next_highest_dollar
      ,price_ownership_next_highest_dollar
      ,price_selling_next_highest_dollar
      ,price_ownership_future_next_highest_dollar
      ,first_clearance_markdown_date
      ,last_clearance_markdown_date
      ,future_clearance_markdown_date
      ,clearance_markdown_version
      ,clearance_markdown_state
      ,future_markdown_ind
      ,weeks_since_last_markdown

      /*
      * Percent Off
      */
      ,case
         when price_base = 0 then 0
         else floor( (price_base - price_ownership) / price_base*100 )*1.00 / 100
       end                                      as pct_off_ownership_vs_base 
      ,case
         when price_base = 0 then 0
         else floor( (price_base - price_selling) / price_base*100 )*1.00 / 100
       end                                      as pct_off_selling_vs_base
      ,case 
         when price_base = 0 then 0
         else floor( (price_base - price_ownership_future) / price_base *100 )*1.00 / 100
       end                                      as pct_off_ownership_future_vs_base
      ,case 
         when price_compare_at = 0 then 0
         else floor( (price_compare_at - price_ownership) / price_compare_at * 100)*1.00 / 100
       end                                      as pct_off_ownership_vs_compare_at
      ,case 
         when price_compare_at = 0 then 0
         else floor( (price_compare_at - price_ownership_future) / price_compare_at * 100)*1.00 / 100
       end                                      as pct_off_ownership_future_vs_compare_at
       
      /*
       * Percent Off Bands
       */
      ,case
         when pct_off_ownership_vs_base is null then null
         when pct_off_ownership_vs_base < 0 then '< 0%'
         when pct_off_ownership_vs_base = 0 then '0%'
         when pct_off_ownership_vs_base <= .10 then '01-10%'
         when pct_off_ownership_vs_base <= .20 then '11-20%'
         when pct_off_ownership_vs_base <= .30 then '21-30%'
         when pct_off_ownership_vs_base <= .40 then '31-40%'
         when pct_off_ownership_vs_base <= .50 then '41-50%'
         when pct_off_ownership_vs_base <= .60 then '51-60%'
         when pct_off_ownership_vs_base <= .70 then '61-70%'
         when pct_off_ownership_vs_base <= .80 then '71-80%'
         when pct_off_ownership_vs_base <= .90 then '81-90%'
         when pct_off_ownership_vs_base <= 1 then '91-100%'
         else 'UNDEFINED'
       end                                      as pct_off_band_ownership_vs_base
      ,case
         when pct_off_selling_vs_base is null then null
         when pct_off_selling_vs_base < 0 then '< 0%'
         when pct_off_selling_vs_base = 0 then '0%'
         when pct_off_selling_vs_base <= .10 then '01-10%'
         when pct_off_selling_vs_base <= .20 then '11-20%'
         when pct_off_selling_vs_base <= .30 then '21-30%'
         when pct_off_selling_vs_base <= .40 then '31-40%'
         when pct_off_selling_vs_base <= .50 then '41-50%'
         when pct_off_selling_vs_base <= .60 then '51-60%'
         when pct_off_selling_vs_base <= .70 then '61-70%'
         when pct_off_selling_vs_base <= .80 then '71-80%'
         when pct_off_selling_vs_base <= .90 then '81-90%'
         when pct_off_selling_vs_base <= 1 then '91-100%'
         else 'UNDEFINED'
       end                                      as pct_off_band_selling_vs_base
      ,case
         when pct_off_ownership_future_vs_base is null then null
         when pct_off_ownership_future_vs_base < 0 then '< 0%'
         when pct_off_ownership_future_vs_base = 0 then '0%'
         when pct_off_ownership_future_vs_base <= .10 then '01-10%'
         when pct_off_ownership_future_vs_base <= .20 then '11-20%'
         when pct_off_ownership_future_vs_base <= .30 then '21-30%'
         when pct_off_ownership_future_vs_base <= .40 then '31-40%'
         when pct_off_ownership_future_vs_base <= .50 then '41-50%'
         when pct_off_ownership_future_vs_base <= .60 then '51-60%'
         when pct_off_ownership_future_vs_base <= .70 then '61-70%'
         when pct_off_ownership_future_vs_base <= .80 then '71-80%'
         when pct_off_ownership_future_vs_base <= .90 then '81-90%'
         when pct_off_ownership_future_vs_base <= 1 then '91-100%'
         else 'UNDEFINED'
       end                                      as pct_off_band_ownership_future_vs_base
      ,case
         when pct_off_ownership_vs_compare_at is null then null
         when pct_off_ownership_vs_compare_at < 0 then '< 0%'
         when pct_off_ownership_vs_compare_at = 0 then '0%'
         when pct_off_ownership_vs_compare_at <= .10 then '01-10%'
         when pct_off_ownership_vs_compare_at <= .20 then '11-20%'
         when pct_off_ownership_vs_compare_at <= .30 then '21-30%'
         when pct_off_ownership_vs_compare_at <= .40 then '31-40%'
         when pct_off_ownership_vs_compare_at <= .50 then '41-50%'
         when pct_off_ownership_vs_compare_at <= .60 then '51-60%'
         when pct_off_ownership_vs_compare_at <= .70 then '61-70%'
         when pct_off_ownership_vs_compare_at <= .80 then '71-80%'
         when pct_off_ownership_vs_compare_at <= .90 then '81-90%'
         when pct_off_ownership_vs_compare_at <= 1 then '91-100%'
         else 'UNDEFINED'
       end                                      as pct_off_band_ownership_vs_compare_at
      ,case
         when pct_off_ownership_future_vs_compare_at is null then null
         when pct_off_ownership_future_vs_compare_at < 0 then '< 0%'
         when pct_off_ownership_future_vs_compare_at = 0 then '0%'
         when pct_off_ownership_future_vs_compare_at <= .10 then '01-10%'
         when pct_off_ownership_future_vs_compare_at <= .20 then '11-20%'
         when pct_off_ownership_future_vs_compare_at <= .30 then '21-30%'
         when pct_off_ownership_future_vs_compare_at <= .40 then '31-40%'
         when pct_off_ownership_future_vs_compare_at <= .50 then '41-50%'
         when pct_off_ownership_future_vs_compare_at <= .60 then '51-60%'
         when pct_off_ownership_future_vs_compare_at <= .70 then '61-70%'
         when pct_off_ownership_future_vs_compare_at <= .80 then '71-80%'
         when pct_off_ownership_future_vs_compare_at <= .90 then '81-90%'
         when pct_off_ownership_future_vs_compare_at <= 1 then '91-100%'
         else 'UNDEFINED'
       end                                      as pct_off_band_ownership_future_vs_compare_at

      /*
       * Remaining CDS fields to be handled in later queries
       */
      ,first_receipt_date
      ,last_receipt_date
      ,available_to_sell
      ,first_sales_date
      ,weeks_sales
      ,total_sales_amt_1wk
      ,total_sales_units_1wk
      ,sell_thru_1wk
      ,total_sales_amt_2wks
      ,total_sales_units_2wks
      ,sell_thru_2wks
      ,total_sales_amt_4wks
      ,total_sales_units_4wks
      ,sell_thru_4wks
      ,total_sales_amt_6mons
      ,total_sales_units_6mons
      ,sell_thru_6mons
      ,sell_thru_since_last_markdown
      ,total_inv_qty
      ,total_inv_dollars
      ,reserve_stock_inventory
      ,reserve_stock_inventory_dollars
      ,pack_and_hold_inventory
      ,pack_and_hold_inventory_dollars
      ,on_order_units_qty
      ,reserve_stock_on_order_units_qty
      ,pack_and_hold_on_order_units_qty
      
   from add_product_info
   
)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

-- no stats recommended for collection


/*
* ADD FIELDS FOR PRICE TYPE AND PRICE SIGNALS (.97, .01) 
*/

create multiset volatile table add_price_type as (

   select
      -- fields from prior query
      snapshot_date
      ,channel_country
      ,channel_brand
      ,banner_code
      ,selling_channel
      ,cc
      ,banner_channel_cc
      ,country_banner_channel_cc
      ,cc_dsci
      ,div_num
      ,div_label
      ,subdiv_num
      ,subdiv_label
      ,dept_num
      ,dept_label
      ,class_num
      ,class_label
      ,subclass_num
      ,subclass_label
      ,style_group_num
      ,rms_style_num
      ,color_num
      ,vpn
      ,supp_color
      ,style_desc
      ,prmy_supp_num
      ,vendor_name
      ,vendor_label_name
      ,npg_ind
      ,promotion_ind
      ,anniversary_item_ind
      ,replenishment_eligible_ind
      ,drop_ship_eligible_ind
      ,return_disposition_code
      ,selling_status_code_legacy
      ,first_rack_date
      ,rack_ind
      ,online_purchasable_ind
      ,online_purchasable_eff_begin_date
      ,online_purchasable_eff_end_date
      ,average_unit_cost
      ,price_base
      ,price_ownership
      ,price_ownership_future
      ,price_selling
      ,price_base_drop_ship
      ,price_compare_at
      ,price_current_rack
      ,price_variance_within_cc_ind
      ,price_base_next_highest_dollar
      ,price_ownership_next_highest_dollar
      ,price_selling_next_highest_dollar
      ,price_ownership_future_next_highest_dollar
      ,first_clearance_markdown_date
      ,last_clearance_markdown_date
      ,future_clearance_markdown_date
      ,clearance_markdown_version
      ,clearance_markdown_state
      ,future_markdown_ind
      ,weeks_since_last_markdown
      ,pct_off_ownership_vs_base
      ,pct_off_selling_vs_base
      ,pct_off_ownership_future_vs_base
      ,pct_off_ownership_vs_compare_at
      ,pct_off_ownership_future_vs_compare_at
      ,pct_off_band_ownership_vs_base
      ,pct_off_band_selling_vs_base
      ,pct_off_band_ownership_future_vs_base
      ,pct_off_band_ownership_vs_compare_at
      ,pct_off_band_ownership_future_vs_compare_at

      /*
       * Price Type, Price Signals
       * 
       * Signal fields not created for selling price because signals are based on ownership price, not selling price
       */
       -- Price Type
      ,case
         when price_ownership >= price_base then 'R' -- ownership > reg should not happen, but assume it's Reg 
         when price_ownership < price_base then 'C'
         else 'U'
       end                                                                    as price_type_code_ownership
      ,case
         when price_ownership_future >= price_base then 'R' -- ownership > reg could happen if there is a future-dated change to reg price
         when price_ownership_future < price_base then 'C'
         else 'U'
       end                                                                    as price_type_code_ownership_future
      ,case
         when price_selling < price_ownership then 'P' 
         else price_type_code_ownership -- selling > ownership should not happen, but assume it's non-promo
       end                                                                    as price_type_code_selling
      ,case price_type_code_ownership
         when 'R' then 'REGULAR'
         when 'C' then 'CLEARANCE'
         when 'U' then 'UNKNOWN'
       end                                                                    as price_type_ownership
      ,case price_type_code_ownership_future
         when 'R' then 'REGULAR'
         when 'C' then 'CLEARANCE'
         when 'U' then 'UNKNOWN'
       end                                                                    as price_type_ownership_future
      ,case price_type_code_selling
         when 'R' then 'REGULAR'
         when 'C' then 'CLEARANCE'
         when 'U' then 'UNKNOWN'
         when 'P' then 'PROMOTION'
       end                                                                    as price_type_selling

      /*
       * Price Signals
       * 
       * .97 indicator based on price ending
       * .01 indicator based on entire price (not price ending)
       * NULL otherwise 
       */
      ,case 
         when mod(zeroifnull(price_ownership),1) = .97 then '97' 
         when price_ownership = .01 then '01'
       end                                                                    as price_signal
      
      ,case 
         when mod(zeroifnull(price_ownership_future),1) = .97 then '97' 
         when price_ownership_future = .01 then '01'
       end                                                                    as price_signal_future
       
      /*
       * Remaining CDS fields to be handled in later queries
       */       
      ,first_receipt_date
      ,last_receipt_date
      ,available_to_sell
      ,first_sales_date
      ,weeks_sales
      ,total_sales_amt_1wk
      ,total_sales_units_1wk
      ,sell_thru_1wk
      ,total_sales_amt_2wks
      ,total_sales_units_2wks
      ,sell_thru_2wks
      ,total_sales_amt_4wks
      ,total_sales_units_4wks
      ,sell_thru_4wks
      ,total_sales_amt_6mons
      ,total_sales_units_6mons
      ,sell_thru_6mons
      ,sell_thru_since_last_markdown
      ,total_inv_qty
      ,total_inv_dollars
      ,reserve_stock_inventory
      ,reserve_stock_inventory_dollars
      ,pack_and_hold_inventory
      ,pack_and_hold_inventory_dollars
      ,on_order_units_qty
      ,reserve_stock_on_order_units_qty
      ,pack_and_hold_on_order_units_qty

   from add_percent_off
   
)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

-- no stats recommended for collection


/*
* ADD FIELDS RELATED TO LIFECYCLE PHASE 
*/

create multiset volatile table add_lifecycle_phase as (

   select

      -- fields from prior query
      snapshot_date
      ,channel_country
      ,channel_brand
      ,banner_code
      ,selling_channel
      ,cc
      ,banner_channel_cc
      ,country_banner_channel_cc
      ,cc_dsci
      ,div_num
      ,div_label
      ,subdiv_num
      ,subdiv_label
      ,dept_num
      ,dept_label
      ,class_num
      ,class_label
      ,subclass_num
      ,subclass_label
      ,style_group_num
      ,rms_style_num
      ,color_num
      ,vpn
      ,supp_color
      ,style_desc
      ,prmy_supp_num
      ,vendor_name
      ,vendor_label_name
      ,npg_ind
      ,promotion_ind
      ,anniversary_item_ind
      ,replenishment_eligible_ind
      ,drop_ship_eligible_ind
      ,return_disposition_code
      ,selling_status_code_legacy
      ,first_rack_date
      ,rack_ind
      ,online_purchasable_ind
      ,online_purchasable_eff_begin_date
      ,online_purchasable_eff_end_date
      ,average_unit_cost
      ,price_base
      ,price_ownership
      ,price_ownership_future
      ,price_selling
      ,price_base_drop_ship
      ,price_compare_at
      ,price_current_rack
      ,price_variance_within_cc_ind
      ,price_base_next_highest_dollar
      ,price_ownership_next_highest_dollar
      ,price_selling_next_highest_dollar
      ,price_ownership_future_next_highest_dollar
      ,first_clearance_markdown_date
      ,last_clearance_markdown_date
      ,future_clearance_markdown_date
      ,clearance_markdown_version
      ,clearance_markdown_state
      ,future_markdown_ind
      ,weeks_since_last_markdown
      ,pct_off_ownership_vs_base
      ,pct_off_selling_vs_base
      ,pct_off_ownership_future_vs_base
      ,pct_off_ownership_vs_compare_at
      ,pct_off_ownership_future_vs_compare_at
      ,pct_off_band_ownership_vs_base
      ,pct_off_band_selling_vs_base
      ,pct_off_band_ownership_future_vs_base
      ,pct_off_band_ownership_vs_compare_at
      ,pct_off_band_ownership_future_vs_compare_at
      ,price_type_code_ownership
      ,price_type_code_ownership_future
      ,price_type_code_selling
      ,price_type_ownership
      ,price_type_ownership_future
      ,price_type_selling
      ,price_signal
      ,price_signal_future
      
       /*
       * Markdown Type, Lifecycle Phase 
       */
      -- markdown type
      ,case
         when price_type_code_ownership = 'C' then
            case
               when price_signal = '01' then 'LAST CHANCE'
               when banner_code = 'N' and price_signal = '97' then 'RACKING'
               when clearance_markdown_version > 1 then 'REMARK'
               when clearance_markdown_version = 1 then 'FIRST MARK'
               else 'UNKNOWN'
            end
         when price_type_code_ownership = 'R' then 'REG PRICE'
         else 'UNKNOWN'
      end   as markdown_type_ownership
      ,case
         when price_type_code_ownership_future = 'C' then
            case
               when price_signal_future = '01' then 'LAST CHANCE'
               when banner_code = 'N' and price_signal_future = '97' then 'RACKING'
               -- future markdown exists and item is currently clearance
               when future_markdown_ind = 'Y' and clearance_markdown_version > 0 then 'REMARK'
               -- future markdown exists and item is currently reg price
               when future_markdown_ind = 'Y' and clearance_markdown_version = 0 then 'FIRST MARK'
               else markdown_type_ownership
            end
         when price_type_code_ownership_future = 'R' then 'REG PRICE'
         else 'UNKNOWN'
      end   as markdown_type_ownership_future
      
      -- create lifcycle phase names using same values as lifecycle phase data
      -- if markdown type is unknown then lifecycle phase will also be unknown
      ,coalesce(
         case banner_code
            when 'N' then
               case markdown_type_ownership
                  when 'REG PRICE' then '1. NORD '
                  when 'FIRST MARK' then '2. NORD '
                  when 'REMARK' then '3. NORD '
                  when 'RACKING' then '4. NORD '
                  when 'LAST CHANCE' then '8. '
               end
            when 'R' then
               case markdown_type_ownership
                  when 'REG PRICE' then '5. RACK '
                  when 'FIRST MARK' then '6. RACK '
                  when 'REMARK' then '7. RACK '
                  when 'LAST CHANCE' then '8. '
               end
          end 
          || markdown_type_ownership                        
       ,'UNKNOWN'
       )    as lifecycle_phase_name_ownership
      ,coalesce(
         case banner_code
            when 'N' then
               case markdown_type_ownership_future
                  when 'REG PRICE' then '1. NORD '
                  when 'FIRST MARK' then '2. NORD '
                  when 'REMARK' then '3. NORD '
                  when 'RACKING' then '4. NORD '
                  when 'LAST CHANCE' then '8. '
               end
            when 'R' then
               case markdown_type_ownership_future
                  when 'REG PRICE' then '5. RACK '
                  when 'FIRST MARK' then '6. RACK '
                  when 'REMARK' then '7. RACK '
                  when 'LAST CHANCE' then '8. '
               end
          end 
          || markdown_type_ownership_future
       ,'UNKNOWN'
       )    as lifecycle_phase_name_ownership_future
       
      /*
       * Remaining CDS fields to be handled in later queries
       */       
      ,first_receipt_date
      ,last_receipt_date
      ,available_to_sell
      ,first_sales_date
      ,weeks_sales
      ,total_sales_amt_1wk
      ,total_sales_units_1wk
      ,sell_thru_1wk
      ,total_sales_amt_2wks
      ,total_sales_units_2wks
      ,sell_thru_2wks
      ,total_sales_amt_4wks
      ,total_sales_units_4wks
      ,sell_thru_4wks
      ,total_sales_amt_6mons
      ,total_sales_units_6mons
      ,sell_thru_6mons
      ,sell_thru_since_last_markdown
      ,total_inv_qty
      ,total_inv_dollars
      ,reserve_stock_inventory
      ,reserve_stock_inventory_dollars
      ,pack_and_hold_inventory
      ,pack_and_hold_inventory_dollars
      ,on_order_units_qty
      ,reserve_stock_on_order_units_qty
      ,pack_and_hold_on_order_units_qty

   from add_price_type
   
)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

-- no stats recommended for collection


/*
* ADD FIELDS RELATED TO SALES, SELL-THRU AND TIME IN ASSORTMENT
*/

create multiset volatile table add_selling_info as (

   select

      -- fields from prior query
      snapshot_date
      ,channel_country
      ,channel_brand
      ,banner_code
      ,selling_channel
      ,cc
      ,banner_channel_cc
      ,country_banner_channel_cc
      ,cc_dsci
      ,div_num
      ,div_label
      ,subdiv_num
      ,subdiv_label
      ,dept_num
      ,dept_label
      ,class_num
      ,class_label
      ,subclass_num
      ,subclass_label
      ,style_group_num
      ,rms_style_num
      ,color_num
      ,vpn
      ,supp_color
      ,style_desc
      ,prmy_supp_num
      ,vendor_name
      ,vendor_label_name
      ,npg_ind
      ,promotion_ind
      ,anniversary_item_ind
      ,replenishment_eligible_ind
      ,drop_ship_eligible_ind
      ,return_disposition_code
      ,selling_status_code_legacy
      ,first_rack_date
      ,rack_ind
      ,online_purchasable_ind
      ,online_purchasable_eff_begin_date
      ,online_purchasable_eff_end_date
      ,average_unit_cost
      ,price_base
      ,price_ownership
      ,price_ownership_future
      ,price_selling
      ,price_base_drop_ship
      ,price_compare_at
      ,price_current_rack
      ,price_variance_within_cc_ind
      ,price_base_next_highest_dollar
      ,price_ownership_next_highest_dollar
      ,price_selling_next_highest_dollar
      ,price_ownership_future_next_highest_dollar
      ,first_clearance_markdown_date
      ,last_clearance_markdown_date
      ,future_clearance_markdown_date
      ,clearance_markdown_version
      ,clearance_markdown_state
      ,future_markdown_ind
      ,weeks_since_last_markdown
      ,pct_off_ownership_vs_base
      ,pct_off_selling_vs_base
      ,pct_off_ownership_future_vs_base
      ,pct_off_ownership_vs_compare_at
      ,pct_off_ownership_future_vs_compare_at
      ,pct_off_band_ownership_vs_base
      ,pct_off_band_selling_vs_base
      ,pct_off_band_ownership_future_vs_base
      ,pct_off_band_ownership_vs_compare_at
      ,pct_off_band_ownership_future_vs_compare_at
      ,price_type_code_ownership
      ,price_type_code_ownership_future
      ,price_type_code_selling
      ,price_type_ownership
      ,price_type_ownership_future
      ,price_type_selling
      ,price_signal
      ,price_signal_future
      ,markdown_type_ownership
      ,markdown_type_ownership_future
      ,lifecycle_phase_name_ownership
      ,lifecycle_phase_name_ownership_future

      /*
       * Sales
       */
      ,total_sales_amt_1wk             as sales_retail_1wk
      ,total_sales_units_1wk           as sales_units_1wk
      ,total_sales_amt_2wks            as sales_retail_2wk
      ,total_sales_units_2wks          as sales_units_2wk
      ,total_sales_amt_4wks            as sales_retail_4wk
      ,total_sales_units_4wks          as sales_units_4wk
      ,total_sales_amt_6mons           as sales_retail_6mo
      ,total_sales_units_6mons         as sales_units_6mo
      
      /*
       * Sell-Thru
       * 
       * Not carrying forward 1wk, 2wks, 6mos because they are not use in reporting
       */
      ,sell_thru_4wks                                                      as sell_thru_4wk
      ,sell_thru_since_last_markdown
      ,sell_thru_4wks*1.0000 / 4                                           as sell_thru_4wk_avg_wkly
      ,case
         when sell_thru_4wk_avg_wkly is null then null
         when sell_thru_4wk_avg_wkly < 0 then '< 0%'
         when sell_thru_4wk_avg_wkly <= .05 then '00-05%'
         when sell_thru_4wk_avg_wkly <= .10 then '06-10%'
         when sell_thru_4wk_avg_wkly <= .15 then '11-15%'
         when sell_thru_4wk_avg_wkly <= .20 then '16-20%'
         when sell_thru_4wk_avg_wkly <= .25 then '21-25%'
         when sell_thru_4wk_avg_wkly <= .30 then '26-30%'
         when sell_thru_4wk_avg_wkly <= .35 then '31-35%'
         when sell_thru_4wk_avg_wkly <= .40 then '36-40%'
         else '41%+'
       end                                                                 as sell_thru_band_4wk_avg_wkly
      ,case 
         when weeks_since_last_markdown = 0 then 0
         else sell_thru_since_last_markdown*1.0000 / weeks_since_last_markdown
       end                                                                 as sell_thru_since_last_markdown_avg_wkly
      ,case
         when sell_thru_since_last_markdown_avg_wkly is null then null
         when sell_thru_since_last_markdown_avg_wkly < 0 then '< 0%'
         when sell_thru_since_last_markdown_avg_wkly <= .05 then '00-05%'
         when sell_thru_since_last_markdown_avg_wkly <= .10 then '06-10%'
         when sell_thru_since_last_markdown_avg_wkly <= .15 then '11-15%'
         when sell_thru_since_last_markdown_avg_wkly <= .20 then '16-20%'
         when sell_thru_since_last_markdown_avg_wkly <= .25 then '21-25%'
         when sell_thru_since_last_markdown_avg_wkly <= .30 then '26-30%'
         when sell_thru_since_last_markdown_avg_wkly <= .35 then '31-35%'
         when sell_thru_since_last_markdown_avg_wkly <= .40 then '36-40%'
         else '41%+'
       end                                                                 as sell_thru_band_since_last_markdown_avg_wkly

      /*
       * Time in Assortment
       */
      ,first_receipt_date
      ,last_receipt_date
      ,floor((snapshot_date - last_receipt_date)/7)   as weeks_since_last_receipt_date
      ,case
         when weeks_since_last_receipt_date is null then 'UNKNOWN'
         when weeks_since_last_receipt_date < 5 then '0 - 4'
         when weeks_since_last_receipt_date < 9 then '5 - 8'
         when weeks_since_last_receipt_date < 13 then '9 - 12'
         when weeks_since_last_receipt_date < 17 then '13 - 16'
         when weeks_since_last_receipt_date < 27 then '17 - 26'
         when weeks_since_last_receipt_date < 52 then '27 - 51'
         else '52+'
      end                                             as weeks_since_last_receipt_date_band
      ,available_to_sell                              as weeks_available_to_sell
      ,case
         when weeks_available_to_sell is null then 'UNKNOWN'
         when weeks_available_to_sell < 5 then '0 - 4'
         when weeks_available_to_sell < 9 then '5 - 8'
         when weeks_available_to_sell < 13 then '9 - 12'
         when weeks_available_to_sell < 17 then '13 - 16'
         when weeks_available_to_sell < 27 then '17 - 26'
         when weeks_available_to_sell < 52 then '27 - 51'
         else '52+'
      end                                             as weeks_available_to_sell_band
      ,first_sales_date
      ,weeks_sales                                    as weeks_since_first_sale
      ,case
         when weeks_since_first_sale is null then 'UNKNOWN'
         when weeks_since_first_sale < 5 then '0 - 4'
         when weeks_since_first_sale < 9 then '5 - 8'
         when weeks_since_first_sale < 13 then '9 - 12'
         when weeks_since_first_sale < 17 then '13 - 16'
         when weeks_since_first_sale < 27 then '17 - 26'
         when weeks_since_first_sale < 52 then '27 - 51'
         else '52+'
      end                                             as weeks_since_first_sale_band

      /*
       * Remaining CDS fields to be handled in later queries
       */       
      ,total_inv_qty
      ,total_inv_dollars
      ,reserve_stock_inventory
      ,reserve_stock_inventory_dollars
      ,pack_and_hold_inventory
      ,pack_and_hold_inventory_dollars
      ,on_order_units_qty
      ,reserve_stock_on_order_units_qty
      ,pack_and_hold_on_order_units_qty

   from add_lifecycle_phase
   
)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

-- no stats recommended for collection


/*
* ADD FIELDS RELATED TO INVENTORY, ON ORDER, MARKDOWN DOLLARS
*/

create multiset volatile table add_inventory as (

   select

      -- fields from prior query
      snapshot_date
      ,channel_country
      ,channel_brand
      ,banner_code
      ,selling_channel
      ,cc
      ,banner_channel_cc
      ,country_banner_channel_cc
      ,cc_dsci
      ,div_num
      ,div_label
      ,subdiv_num
      ,subdiv_label
      ,dept_num
      ,dept_label
      ,class_num
      ,class_label
      ,subclass_num
      ,subclass_label
      ,style_group_num
      ,rms_style_num
      ,color_num
      ,vpn
      ,supp_color
      ,style_desc
      ,prmy_supp_num
      ,vendor_name
      ,vendor_label_name
      ,npg_ind
      ,promotion_ind
      ,anniversary_item_ind
      ,replenishment_eligible_ind
      ,drop_ship_eligible_ind
      ,return_disposition_code
      ,selling_status_code_legacy
      ,first_rack_date
      ,rack_ind
      ,online_purchasable_ind
      ,online_purchasable_eff_begin_date
      ,online_purchasable_eff_end_date
      ,average_unit_cost
      ,price_base
      ,price_ownership
      ,price_ownership_future
      ,price_selling
      ,price_base_drop_ship
      ,price_compare_at
      ,price_current_rack
      ,price_variance_within_cc_ind
      ,price_base_next_highest_dollar
      ,price_ownership_next_highest_dollar
      ,price_selling_next_highest_dollar
      ,price_ownership_future_next_highest_dollar
      ,first_clearance_markdown_date
      ,last_clearance_markdown_date
      ,future_clearance_markdown_date
      ,clearance_markdown_version
      ,clearance_markdown_state
      ,future_markdown_ind
      ,weeks_since_last_markdown
      ,pct_off_ownership_vs_base
      ,pct_off_selling_vs_base
      ,pct_off_ownership_future_vs_base
      ,pct_off_ownership_vs_compare_at
      ,pct_off_ownership_future_vs_compare_at
      ,pct_off_band_ownership_vs_base
      ,pct_off_band_selling_vs_base
      ,pct_off_band_ownership_future_vs_base
      ,pct_off_band_ownership_vs_compare_at
      ,pct_off_band_ownership_future_vs_compare_at
      ,price_type_code_ownership
      ,price_type_code_ownership_future
      ,price_type_code_selling
      ,price_type_ownership
      ,price_type_ownership_future
      ,price_type_selling
      ,price_signal
      ,price_signal_future
      ,markdown_type_ownership
      ,markdown_type_ownership_future
      ,lifecycle_phase_name_ownership
      ,lifecycle_phase_name_ownership_future
      ,sales_retail_1wk
      ,sales_units_1wk
      ,sales_retail_2wk
      ,sales_units_2wk
      ,sales_retail_4wk
      ,sales_units_4wk
      ,sales_retail_6mo
      ,sales_units_6mo
      ,sell_thru_4wk
      ,sell_thru_since_last_markdown
      ,sell_thru_4wk_avg_wkly
      ,sell_thru_band_4wk_avg_wkly
      ,sell_thru_since_last_markdown_avg_wkly
      ,sell_thru_band_since_last_markdown_avg_wkly
      ,first_receipt_date
      ,last_receipt_date
      ,weeks_since_last_receipt_date
      ,weeks_since_last_receipt_date_band
      ,weeks_available_to_sell
      ,weeks_available_to_sell_band
      ,first_sales_date
      ,weeks_since_first_sale
      ,weeks_since_first_sale_band

      /*
       * Inventory
       * 
       * Null handling
       * - units and retail come from CDS directly so NULL has same meaning as zero (no inventory present)
       * - cost and retail_future and all_locations fields can be NULL because the cost or price component can be NULL 
       *   (i.e. if there are inventory units but no cost, that inventory has a cost and we just don't know it)
       * 
       * Casting rounds to nearest two decimals 
       *
       */
      ,coalesce(total_inv_qty,0)                                                             as eop_units_selling_locations
      ,coalesce(total_inv_dollars,0)                                                         as eop_retail_selling_locations
      ,cast(eop_units_selling_locations * average_unit_cost as decimal(18,2))                as eop_cost_selling_locations
      ,cast(eop_units_selling_locations * price_ownership_future  as decimal(18,2))          as eop_retail_future_selling_locations
      
      ,coalesce(reserve_stock_inventory,0)                                                   as eop_units_reserve_stock
      ,coalesce(reserve_stock_inventory_dollars,0)                                           as eop_retail_reserve_stock
      ,cast(eop_units_reserve_stock * average_unit_cost as decimal(18,2))                    as eop_cost_reserve_stock
      ,cast(eop_units_reserve_stock * price_ownership_future as decimal(18,2))               as eop_retail_future_reserve_stock
 
      ,coalesce(pack_and_hold_inventory,0)                                                   as eop_units_pack_and_hold
      ,coalesce(pack_and_hold_inventory_dollars,0)                                           as eop_retail_pack_and_hold
      ,cast(eop_units_pack_and_hold * average_unit_cost as decimal(18,2))                    as eop_cost_pack_and_hold
      ,cast(eop_units_pack_and_hold * price_ownership_future as decimal(18,2))               as eop_retail_future_pack_and_hold

      ,eop_units_selling_locations + eop_units_reserve_stock + eop_units_pack_and_hold       as eop_units_all_locations
      ,eop_retail_selling_locations + eop_retail_reserve_stock + eop_retail_pack_and_hold    as eop_retail_all_locations
      ,eop_cost_selling_locations + eop_cost_reserve_stock + eop_cost_pack_and_hold          as eop_cost_all_locations
      ,eop_retail_future_selling_locations + eop_retail_future_reserve_stock 
       + eop_retail_future_pack_and_hold                                                     as eop_retail_future_all_locations
       
      /*
       * On Order
       * Null handling: units come from CDS directly so NULL has same meaning as zero (no on order present)
       */
      ,coalesce(on_order_units_qty,0)                                                        as on_order_units_selling_locations
      ,coalesce(reserve_stock_on_order_units_qty,0)                                          as on_order_units_reserve_stock
      ,coalesce(pack_and_hold_on_order_units_qty,0)                                          as on_order_units_pack_and_hold
      ,on_order_units_selling_locations + on_order_units_reserve_stock 
       + on_order_units_pack_and_hold                                                        as on_order_units_all_locations

      /*
       * Markdown Dollars
       * 
       * Null handling: same as Inventory section above
       * Casting rounds to nearest two decimals 
       */
      ,cast((price_base - price_ownership) * eop_units_selling_locations as decimal(18,2))         as markdown_dollars_selling_locations
      ,cast((price_base - price_ownership) * eop_units_pack_and_hold as decimal(18,2))             as markdown_dollars_pack_and_hold
      ,cast((price_base - price_ownership) * eop_units_reserve_stock as decimal(18,2))             as markdown_dollars_reserve_stock
      ,cast((price_base - price_ownership) * eop_units_all_locations as decimal(18,2))             as markdown_dollars_all_locations
      ,cast((price_base - price_ownership_future) * eop_units_selling_locations as decimal(18,2))  as markdown_dollars_future_selling_locations
      ,cast((price_base - price_ownership_future) * eop_units_pack_and_hold as decimal(18,2))      as markdown_dollars_future_pack_and_hold
      ,cast((price_base - price_ownership_future) * eop_units_reserve_stock as decimal(18,2))      as markdown_dollars_future_reserve_stock
      ,cast((price_base - price_ownership_future) * eop_units_all_locations as decimal(18,2))      as markdown_dollars_future_all_locations
       
   from add_selling_info
 
)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

collect statistics
   column( snapshot_date, banner_channel_cc )
on add_inventory
;


/*
 * ADD FIELDS FROM OPPOSITE BANNER
 * 
 * First select specific fields from current data
 * Then join them back to create new columns that provide opposite-banner info on same row
 */

create multiset volatile table other_banner as (

   select 
      snapshot_date
      -- opposite-banner code will enable join back to main data set
      ,case banner_code
         when 'N' then 'R'
         when 'R' then 'N'
       end
       || '-' || selling_channel || '-' || cc         as other_banner_channel_cc
      ,drop_ship_eligible_ind                         as drop_ship_eligible_ind_other_banner
      ,replenishment_eligible_ind                     as replenishment_eligible_ind_other_banner
      ,price_type_ownership                           as price_type_ownership_other_banner
      ,price_type_ownership_future                    as price_type_ownership_future_other_banner
      ,price_type_selling                             as price_type_selling_other_banner
      ,price_base                                     as price_base_other_banner
      ,price_ownership                                as price_ownership_other_banner
      ,price_ownership_future                         as price_ownership_future_other_banner
      ,price_selling                                  as price_selling_other_banner
      ,future_clearance_markdown_date                 as future_clearance_markdown_date_other_banner
      ,future_markdown_ind                            as future_markdown_ind_other_banner
      ,sell_thru_4wk                                  as sell_thru_4wk_other_banner
      ,sell_thru_since_last_markdown                  as sell_thru_since_last_markdown_other_banner
      ,sell_thru_4wk_avg_wkly                         as sell_thru_4wk_avg_wkly_other_banner
      ,sell_thru_band_4wk_avg_wkly                    as sell_thru_band_4wk_avg_wkly_other_banner
      ,sell_thru_since_last_markdown_avg_wkly         as sell_thru_since_last_markdown_avg_wkly_other_banner
      ,sell_thru_band_since_last_markdown_avg_wkly    as sell_thru_band_since_last_markdown_avg_wkly_other_banner
      ,eop_units_selling_locations                    as eop_units_selling_locations_other_banner
      ,eop_retail_selling_locations                   as eop_retail_selling_locations_other_banner
      ,eop_cost_selling_locations                     as eop_cost_selling_locations_other_banner
      ,eop_units_all_locations                        as eop_units_all_locations_other_banner
      ,eop_retail_all_locations                       as eop_retail_all_locations_other_banner
      ,eop_cost_all_locations                         as eop_cost_all_locations_other_banner
   
   from add_inventory

)
with data
primary index ( snapshot_date, other_banner_channel_cc )
on commit preserve rows
;

collect statistics
   column( snapshot_date, other_banner_channel_cc )
on other_banner
;

create multiset volatile table add_other_banner as (

   select
      a.*
      ,ob.drop_ship_eligible_ind_other_banner
      ,ob.replenishment_eligible_ind_other_banner
      ,ob.price_type_ownership_other_banner
      ,ob.price_type_ownership_future_other_banner
      ,ob.price_type_selling_other_banner
      ,ob.price_base_other_banner
      ,ob.price_ownership_other_banner
      ,ob.price_ownership_future_other_banner
      ,ob.price_selling_other_banner
      ,ob.future_clearance_markdown_date_other_banner
      ,ob.future_markdown_ind_other_banner
      ,ob.sell_thru_4wk_other_banner
      ,ob.sell_thru_since_last_markdown_other_banner
      ,ob.sell_thru_4wk_avg_wkly_other_banner
      ,ob.sell_thru_band_4wk_avg_wkly_other_banner
      ,ob.sell_thru_since_last_markdown_avg_wkly_other_banner
      ,ob.sell_thru_band_since_last_markdown_avg_wkly_other_banner
      ,ob.eop_units_selling_locations_other_banner
      ,ob.eop_retail_selling_locations_other_banner
      ,ob.eop_cost_selling_locations_other_banner
      ,ob.eop_units_all_locations_other_banner
      ,ob.eop_retail_all_locations_other_banner
      ,ob.eop_cost_all_locations_other_banner
      
   from add_inventory as a
      left join other_banner as ob
         on ob.snapshot_date = a.snapshot_date
         and ob.other_banner_channel_cc = a.banner_channel_cc

)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

collect statistics
   column (dept_num ,class_num ,subclass_num)
   ,column (banner_channel_cc)
   ,column (snapshot_date ,banner_channel_cc)
   ,column (banner_code ,dept_num)
   ,column (prmy_supp_num)
   ,column (banner_code ,dept_num, prmy_supp_num)
on add_other_banner
;


/*
 * CATEGORY AND SEASON
 */

create volatile multiset table category_lkup as (

   select distinct
      h.dept_num      
      ,h.class_num    
      ,h.sbclass_num  
      ,coalesce(cat1.category, cat2.category, 'UNKNOWN')                      as quantrix_category
      ,coalesce(cat1.category_group, cat2.category_group, 'UNKNOWN')          as quantrix_category_group
      ,coalesce(cat1.seasonal_designation, cat2.seasonal_designation,'NONE')  as quantrix_season
      
   from (
   
      select distinct
         dept_num      
         ,class_num    
         ,sbclass_num  
         
      from prd_nap_usr_vws.product_sku_dim_vw
      
      where channel_country = 'US'
      
   ) as h
      -- Table contains records at Dept + Class + Subclass and Dept + Class
      -- If category has been assigned at Dept + Class then the table will have subclass = -1 
      -- We use the Dept + Class + Subclass record if it exists, and if it does not then we use the Dept + Class record
      left join prd_nap_usr_vws.catg_subclass_map_dim as cat1
          on h.dept_num = cat1.dept_num
          and h.class_num = cat1.class_num
          and h.sbclass_num = cat1.sbclass_num
      left join prd_nap_usr_vws.catg_subclass_map_dim as cat2
          on h.dept_num = cat2.dept_num
          and h.class_num = cat2.class_num
          and cat2.sbclass_num = -1
       
)
with data 
primary index(dept_num, class_num, sbclass_num) 
on commit preserve rows
;

collect statistics
  column (dept_num, class_num, sbclass_num) 
on category_lkup
;


/*
 * ANCHOR BRANDS
 */
create volatile multiset table anchor_brands_lkup as (

   select distinct
      case banner
         when 'NORDSTROM' then 'N'
         when 'NORDSTROM_RACK' then 'R'
      end as banner_code
      ,dept_num
      ,supplier_idnt 
      ,anchor_brand_name
      
   from t2dl_das_in_season_management_reporting.anchor_brands
)
with data 
primary index(banner_code, dept_num, supplier_idnt) 
on commit preserve rows
;

collect statistics
  column (banner_code, dept_num, supplier_idnt)
  ,column (supplier_idnt)
on anchor_brands_lkup
;


/*
 * SELLING RIGHTS
 * 
 * First query: fetch data from NAP table for STORE and ONLINE channels
 * Second query: up-level STORE and ONLINE channels to same grain as CDS
 * Third query: determine datat for OMNI channel
 * Fourth query: put all channels in one table
 */
create volatile multiset table selling_rights_lkup as (
   select
      banner_code || '-' || sell.selling_channel || '-' || sku.rms_style_num || '-' || sku.color_num as banner_channel_cc
      ,sku.rms_sku_num
      ,sku.rms_style_num
      ,sku.color_num
      ,case sell.channel_brand
         when 'NORDSTROM' then 'N'
         when 'NORDSTROM_RACK' then 'R'
       end as banner_code
      ,sell.selling_channel
      ,sell.is_sellable_ind
      ,sell.selling_status_code
      ,sell.eff_begin_tmstp
      ,sell.eff_end_tmstp
      
   from prd_nap_usr_vws.product_item_selling_rights_dim as sell
      inner join prd_nap_usr_vws.product_sku_dim_vw as sku
         on sku.rms_sku_num = sell.rms_sku_num
         and sku.channel_country = sell.channel_country
   where 1=1
      and sell.channel_country = 'US'
)
with data  
primary index ( rms_sku_num, banner_channel_cc, eff_begin_tmstp )
on commit preserve rows
;

collect statistics
   column( rms_sku_num, banner_channel_cc, eff_begin_tmstp )
on selling_rights_lkup
;

create volatile multiset table selling_rights_stg_part1 as (

   with cds_dims as (
   
      select
         snapshot_date
         ,banner_channel_cc
         ,banner_code
         ,selling_channel
         ,cc
         
      from add_other_banner
      
      where selling_channel <> 'OMNI'
      
   )
   
   select
      cds.snapshot_date
      ,cds.banner_channel_cc
      ,cds.banner_code
      ,cds.selling_channel
      ,cds.cc
      -- max of (Y,N) will be Y if any SKU in the CC has selling rights
      ,max(s.is_sellable_ind)       as selling_rights_ind
      -- max of (UNBLOCKED,BLOCKED) will be UNBLOCKED if any SKU in the CC is unblocked
      ,max(s.selling_status_code)   as selling_status_code
      
   from cds_dims as cds
      left join selling_rights_lkup as s
         on s.banner_channel_cc = cds.banner_channel_cc
         and cds.snapshot_date between s.eff_begin_tmstp and s.eff_end_tmstp
   
   group by 1,2,3,4,5
   
)
with data 
primary index ( snapshot_date, banner_channel_cc ) 
on commit preserve rows
;

create volatile multiset table selling_rights_stg_part2 as (

   select
      sr1.snapshot_date
      ,sr1.banner_code || '-' || 'OMNI' || '-' || sr1.cc    as banner_channel_cc
      ,sr1.banner_code
      ,'OMNI' as selling_channel
      ,sr1.cc
      -- max of (Y,N) will be Y if any channel has selling rights
      ,max(sr1.selling_rights_ind)                         as selling_rights_ind
      -- max of (UNBLOCKED,BLOCKED) will be UNBLOCKED if any channel is unblocked
      ,max(sr1.selling_status_code)                        as selling_status_code
      
   from selling_rights_stg_part1 as sr1
   
   group by 1,2,3,4,5
   
)
with data 
primary index ( snapshot_date, banner_channel_cc ) 
on commit preserve rows
;

create volatile multiset table selling_rights_stg as (

   select * from selling_rights_stg_part1
   
   union all
   
   select * from selling_rights_stg_part2

)
with data 
primary index ( snapshot_date, banner_channel_cc ) 
on commit preserve rows
;

collect statistics
   column( snapshot_date, banner_channel_cc )
on selling_rights_stg
;


/*
 * ITEM INTENT
 * 
 * NAP view is already leveled up to a single plan for each Banner + Style + Color.
 * SMD Data Science is treating plan data from this view as Banner + Style + Color without splitting by channel.
 * 
 * For reporting, we will treat this view as the OMNI intent and join it only to that row.
 * We will not add intent to any of the channel-specific rows because the data would be incomplete and we do not want to give the impression that it is complete.
 * 
 */
create volatile multiset table item_intent_lkup as (
   
   select
      banner_code || '-' || 'OMNI' || '-' || rms_style_num || '-' || color_num as banner_channel_cc
      ,case channel_brand
         when 'NORDSTROM' then 'N'
         when 'NORDSTROM_RACK' then 'R'
       end as banner_code
      ,channel_brand   
      ,'OMNI'                    as selling_channel
      ,rms_style_num
      ,sku_nrf_color_num         as color_num
      ,intended_season           as intent_season
      ,intended_lifecycle_type   as intent_lifecycle_type
      ,scaled_event              as intent_scaled_event
      ,holiday_or_celebration    as intent_holiday_or_celebration
      -- month_idnt formatted in standard way: 202406 for July FY24
      ,cast(
         substring(intended_exit_month_year from 4 for 4) 
         || 
         substring(intended_exit_month_year from 1 for 2) 
         as integer
       )                         as intent_exit_month_idnt
       -- week_idnt formatted in standard way: 202406 for 6th week of FY24
      ,cast(
         concat(
            trim( both from plan_year), 
            trim( both from
               case 
                  when length(substring(plan_week from position('_' in plan_week) + 1)) = 1 then 0 || substring(plan_week from position('_' in plan_week) + 1)
                  else substring(plan_week from position('_' in plan_week) + 1) 
               end
            )
         )
         as integer
       )                         as intent_plan_week_idnt
       
   from prd_nap_usr_vws.item_intent_plan_fact_enhanced_smart_markdown_vw
   
   where channel_country = 'US'
   
)  
with data
primary index ( banner_channel_cc )
on commit preserve rows
;

collect statistics
   column( banner_channel_cc )
on item_intent_lkup
;

   
/*
 * ADD CATEGORY, ANCHOR BRAND, SELLING RIGHTS, ITEM INTENT
 */
create volatile multiset table add_nap_dims as (

   select
      b.*
      ,q.quantrix_category
      ,q.quantrix_category_group
      ,q.quantrix_season
      ,case when a.anchor_brand_name is not null then 'Y' else 'N' end as anchor_brand_ind
      ,s.selling_rights_ind
      ,s.selling_status_code
      ,ii.intent_season
      ,ii.intent_lifecycle_type
      ,ii.intent_scaled_event
      ,ii.intent_holiday_or_celebration
      ,ii.intent_exit_month_idnt
      ,ii.intent_plan_week_idnt
      
   from add_other_banner as b
      left join category_lkup as q
         on q.dept_num = b.dept_num 
         and q.class_num = b.class_num 
         and q.sbclass_num = b.subclass_num
      left join anchor_brands_lkup as a
         on a.banner_code = b.banner_code
         and a.dept_num = b.dept_num
         and a.supplier_idnt = b.prmy_supp_num
      left join selling_rights_stg as s
         on s.snapshot_date = b.snapshot_date
         and s.banner_channel_cc = b.banner_channel_cc
      left join item_intent_lkup as ii
         on ii.banner_channel_cc = b.banner_channel_cc
        
)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

collect statistics
   column ( snapshot_date, banner_channel_cc )
   ,column (price_ownership_future_next_highest_dollar)
   ,column (price_ownership_next_highest_dollar)
   ,column (price_base_next_highest_dollar)
   ,column (price_selling_next_highest_dollar)
on add_nap_dims
;


/*
 * ADD PRICE BANDS
 * 
 * Bands will display prices as follows:
 * - $0.00 if price = 0
 * - $0.nn - 10.00 if price > 0 and < 1
 * - $60.01 - 80.00 if price > 1 and less than max possible price band
 * - $5000.01+ if price is above max possible price band
 * 
 * All bands use the same code pattern.
 * See the Ownership Price section for explanation of the code pattern
 * 
 * Lower bound and upper bound fields are not carried forward into other queries.
 * They are calculated here to make code logic clearer.
 */

create volatile multiset table price_band_lkup as (

   select * 
   
   from t2dl_das_in_season_management_reporting.price_bands_by_dollar
   
)
with data 
primary index (next_highest_dollar) 
on commit preserve rows
;

collect statistics
   column(next_highest_dollar)
on price_band_lkup
;

create volatile multiset table add_price_bands as (

   select
      b.*
      
      -- Base Price
      ,case
         when coalesce(b.price_base,0) = 0 then 0 
         else coalesce(pbb.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) 
       end as pbb_lower_bound      
      ,case
         when coalesce(b.price_base,0) = 0 then 0 
         else coalesce(pbb.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) 
       end as pbb_upper_bound
      ,'$' 
       || case when pbb_lower_bound < 1 then '0' else '' end  -- add zero before the decimal place
       || trim(both from pbb_lower_bound)
       || case 
            when pbb_lower_bound = 0 then ''
            else   
               case 
                  when pbb_upper_bound = (select max(price_band_two_upper_bound) from price_band_lkup) then '+'
                  else ' - ' || trim(both from pbb_upper_bound)
               end 
          end as price_band_base

      -- Ownership Price
      -- if price = 0 then lower bound = 0; otherwise, fetch lower bound from the price band table
      ,case
         when coalesce(b.price_ownership,0) = 0 then 0 
         else coalesce(pbo.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) 
       end as pbo_lower_bound
       -- if price = 0 then upper bound = 0; otherwise, fetch upper bound from the price band table
      ,case
         when coalesce(b.price_ownership,0) = 0 then 0 
         else coalesce(pbo.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) 
       end as pbo_upper_bound
      -- assemble the price band
      ,'$' -- first character will be a dollar sign
       || case when pbo_lower_bound < 1 then '0' else '' end  -- add a zero before the decimal place if price < $1.00
       || trim(both from pbo_lower_bound)                      -- lower bound of price range
       || case 
            when pbo_lower_bound = 0 then ''                   -- if price is zero there is no upper bound to the price range
            else   
               case 
                  when pbo_upper_bound = (select max(price_band_two_upper_bound) from price_band_lkup) then '+'   -- if upper bound is the highest possible upper bound, show a + instead of the actual value 
                  else ' - ' || trim(both from pbo_upper_bound)   -- if upper bound is not the highest possible upper bound, show the upper bound
               end 
          end as price_band_ownership
          
      -- Ownership Price Future
      ,case
         when coalesce(b.price_ownership,0) = 0 then 0 
         else coalesce(pbof.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) 
       end as pbof_lower_bound      
      ,case
         when coalesce(b.price_ownership,0) = 0 then 0 
         else coalesce(pbof.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) 
       end as pbof_upper_bound
      ,'$' 
       || case when pbof_lower_bound < 1 then '0' else '' end  -- add zero before the decimal place
       || trim(both from pbof_lower_bound)
       || case 
            when pbof_lower_bound = 0 then ''
            else   
               case 
                  when pbof_upper_bound = (select max(price_band_two_upper_bound) from price_band_lkup) then '+'
                  else ' - ' || trim(both from pbof_upper_bound)
               end 
          end as price_band_ownership_future

      -- Selling Price
      ,case
         when coalesce(b.price_selling,0) = 0 then 0 
         else coalesce(pbs.price_band_two_lower_bound,(select max(price_band_two_lower_bound) from price_band_lkup)) 
       end as pbs_lower_bound      
      ,case
         when coalesce(b.price_selling,0) = 0 then 0 
         else coalesce(pbs.price_band_two_upper_bound,(select max(price_band_two_upper_bound) from price_band_lkup)) 
       end as pbs_upper_bound
      ,'$' 
       || case when pbs_lower_bound < 1 then '0' else '' end  -- add zero before the decimal place
       || trim(both from pbs_lower_bound)
       || case 
            when pbs_lower_bound = 0 then ''
            else   
               case 
                  when pbs_upper_bound = (select max(price_band_two_upper_bound) from price_band_lkup) then '+'
                  else ' - ' || trim(both from pbs_upper_bound)
               end 
          end as price_band_selling
   
   from add_nap_dims as b
      left join price_band_lkup pbb
         on pbb.next_highest_dollar = b.price_base_next_highest_dollar
      left join price_band_lkup pbo
         on pbo.next_highest_dollar = b.price_ownership_next_highest_dollar
      left join price_band_lkup pbof 
         on pbof.next_highest_dollar = b.price_ownership_future_next_highest_dollar
      left join price_band_lkup pbs 
         on pbs.next_highest_dollar = b.price_selling_next_highest_dollar

)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;

collect statistics
   column( snapshot_date, banner_channel_cc )
on add_price_bands
;


/*
 * GET SPECIFIC FIELDS FROM PRIOR WEEK
 * 
 * Reporting: These are used only in the Last Chance tab in the Markdown Recap Report.
 * Operations: Other calculations are needed to identify Net New Marks. Those are not included here.
 */

create volatile multiset table prior_week_lkup as (

   select
      snapshot_date + interval '7' day                      as snapshot_date_for_reporting_week
      ,snapshot_date                                        as snapshot_date_for_prior_week
      ,case channel_brand
         when 'NORDSTROM' then 'N'
         when 'NORDSTROM_RACK' then 'R'
       end                                                  as banner_code
      ,rms_style_num || '-' || color_num                    as cc
      ,banner_code || '-' || selling_channel || '-' || cc   as banner_channel_cc
      ,future_clearance_markdown_date                       as future_clearance_markdown_date_prior_week
      ,future_clearance_markdown_price                      as price_ownership_future_prior_week
      ,ownership_price_amt                                  as price_ownership_prior_week
      ,total_inv_qty                                        as eop_units_selling_locations_prior_week

   from prd_nap_usr_vws.clearance_markdown_insights_by_week_fact
      
   where 1=1
      -- choose the week prior to the most recent week
      and snapshot_date = (( select snapshot_date from snap_date ) - interval '7' day)
                           
)
with data
primary index ( snapshot_date_for_reporting_week, banner_channel_cc )
on commit preserve rows
;

collect statistics
   column( snapshot_date_for_reporting_week, banner_channel_cc )
on prior_week_lkup
;

create volatile multiset table add_prior_week as (

   select
      pb.*
      ,pw.future_clearance_markdown_date_prior_week
      ,pw.price_ownership_future_prior_week
      ,pw.price_ownership_prior_week
      ,pw.eop_units_selling_locations_prior_week

   from add_price_bands as pb
   left join prior_week_lkup as pw
      on pw.snapshot_date_for_reporting_week = pb.snapshot_date
      and pw.banner_channel_cc = pb.banner_channel_cc

)
with data
primary index ( snapshot_date, banner_channel_cc )
index ( snapshot_date, channel_country, channel_brand, selling_channel, rms_style_num, color_num )
on commit preserve rows
;



/*
 * FULL DATA SET - FINAL
 * 
 * Reorder fields into related groups
 */

delete from {environment_schema}.markdowns_analysis_single_week{env_suffix}
;

insert into {environment_schema}.markdowns_analysis_single_week{env_suffix}
   select
      -- record ID and key dimensions
      snapshot_date
      ,channel_country
      ,channel_brand
      ,banner_code
      ,selling_channel
      ,cc
      ,banner_channel_cc
      ,country_banner_channel_cc
      ,cc_dsci
      
      -- Merch Hierarchy
      ,div_num
      ,div_label
      ,subdiv_num
      ,subdiv_label
      ,dept_num
      ,dept_label
      ,class_num
      ,class_label
      ,subclass_num
      ,subclass_label
      
      -- Product Attributes
      ,style_group_num
      ,rms_style_num
      ,color_num
      ,vpn
      ,supp_color
      ,style_desc
      ,prmy_supp_num
      ,vendor_name
      ,vendor_label_name
      ,npg_ind
      ,quantrix_category
      ,quantrix_category_group
      ,quantrix_season
      ,anchor_brand_ind
      
      -- Product Statuses
      ,promotion_ind
      ,anniversary_item_ind
      ,replenishment_eligible_ind
      ,drop_ship_eligible_ind
      ,return_disposition_code
      ,first_rack_date
      ,rack_ind
      ,online_purchasable_ind
      ,online_purchasable_eff_begin_date
      ,online_purchasable_eff_end_date
      ,selling_status_code_legacy
      ,selling_status_code
      ,selling_rights_ind
      
      -- Cost
      ,round(apw.average_unit_cost,2) as average_unit_cost 
      
      -- Price Amounts, Price Bands
      ,price_base
      ,price_ownership
      ,price_ownership_future
      ,price_selling
      ,price_band_base
      ,price_band_ownership
      ,price_band_ownership_future
      ,price_band_selling
      ,price_base_drop_ship
      ,price_compare_at
      ,price_current_rack
      ,price_variance_within_cc_ind
      
      -- Percent Off
      ,pct_off_ownership_vs_base
      ,pct_off_selling_vs_base
      ,pct_off_ownership_future_vs_base
      ,pct_off_ownership_vs_compare_at
      ,pct_off_ownership_future_vs_compare_at
      ,pct_off_band_ownership_vs_base
      ,pct_off_band_selling_vs_base
      ,pct_off_band_ownership_future_vs_base
      ,pct_off_band_ownership_vs_compare_at
      ,pct_off_band_ownership_future_vs_compare_at
      
      -- Price Type, Price Signals
      ,price_type_code_ownership
      ,price_type_code_ownership_future
      ,price_type_code_selling
      ,price_type_ownership
      ,price_type_ownership_future
      ,price_type_selling
      ,price_signal
      ,price_signal_future
      
      -- Mark Type, Lifecycle Phase
      ,markdown_type_ownership
      ,markdown_type_ownership_future
      ,lifecycle_phase_name_ownership
      ,lifecycle_phase_name_ownership_future
      
      -- Markdown Info
      ,first_clearance_markdown_date
      ,last_clearance_markdown_date
      ,future_clearance_markdown_date
      ,clearance_markdown_version
      ,future_markdown_ind
      ,weeks_since_last_markdown

      -- Time in Assortment
      ,first_receipt_date
      ,last_receipt_date
      ,weeks_since_last_receipt_date
      ,weeks_since_last_receipt_date_band
      ,weeks_available_to_sell
      ,weeks_available_to_sell_band
      ,first_sales_date
      ,weeks_since_first_sale
      ,weeks_since_first_sale_band

      -- Item Intent
      ,intent_plan_week_idnt
      ,intent_exit_month_idnt
      ,intent_season
      ,intent_lifecycle_type
      ,intent_scaled_event
      ,intent_holiday_or_celebration
      
      -- Sales
      ,sales_retail_1wk
      ,sales_units_1wk
      ,sales_retail_2wk
      ,sales_units_2wk
      ,sales_retail_4wk
      ,sales_units_4wk
      ,sales_retail_6mo
      ,sales_units_6mo
      
      -- Sell-Thru
      ,sell_thru_4wk
      ,sell_thru_since_last_markdown
      ,sell_thru_4wk_avg_wkly
      ,sell_thru_band_4wk_avg_wkly
      ,sell_thru_since_last_markdown_avg_wkly
      ,sell_thru_band_since_last_markdown_avg_wkly
      
      -- Inventory
      ,eop_units_selling_locations
      ,eop_retail_selling_locations
      ,eop_cost_selling_locations
      ,eop_retail_future_selling_locations
      ,eop_units_reserve_stock
      ,eop_retail_reserve_stock
      ,eop_cost_reserve_stock
      ,eop_retail_future_reserve_stock
      ,eop_units_pack_and_hold
      ,eop_retail_pack_and_hold
      ,eop_cost_pack_and_hold
      ,eop_retail_future_pack_and_hold
      ,eop_units_all_locations
      ,eop_retail_all_locations
      ,eop_cost_all_locations
      ,eop_retail_future_all_locations
      
      -- On Order
      ,on_order_units_selling_locations
      ,on_order_units_reserve_stock
      ,on_order_units_pack_and_hold
      ,on_order_units_all_locations
      
      -- Markdown Dollars
      ,markdown_dollars_selling_locations
      ,markdown_dollars_reserve_stock
      ,markdown_dollars_pack_and_hold
      ,markdown_dollars_all_locations
      ,markdown_dollars_future_selling_locations
      ,markdown_dollars_future_reserve_stock
      ,markdown_dollars_future_pack_and_hold
      ,markdown_dollars_future_all_locations
      
      -- Opposite Banner Info
      ,drop_ship_eligible_ind_other_banner
      ,replenishment_eligible_ind_other_banner
      ,price_type_ownership_other_banner
      ,price_type_ownership_future_other_banner
      ,price_type_selling_other_banner
      ,price_base_other_banner
      ,price_ownership_other_banner
      ,price_ownership_future_other_banner
      ,price_selling_other_banner
      ,future_clearance_markdown_date_other_banner
      ,future_markdown_ind_other_banner
      ,sell_thru_4wk_other_banner
      ,sell_thru_since_last_markdown_other_banner
      ,sell_thru_4wk_avg_wkly_other_banner
      ,sell_thru_band_4wk_avg_wkly_other_banner
      ,sell_thru_since_last_markdown_avg_wkly_other_banner
      ,sell_thru_band_since_last_markdown_avg_wkly_other_banner
      ,eop_units_selling_locations_other_banner
      ,eop_retail_selling_locations_other_banner
      ,eop_cost_selling_locations_other_banner
      ,eop_units_all_locations_other_banner
      ,eop_retail_all_locations_other_banner
      ,eop_cost_all_locations_other_banner

      -- Prior Week Info
      ,future_clearance_markdown_date_prior_week
      ,price_ownership_future_prior_week
      ,price_ownership_prior_week
      ,eop_units_selling_locations_prior_week
      
      -- Load Time
      ,current_timestamp as dw_sys_load_tmstp
      
   from add_prior_week as apw
      
;

-- stats to collect are defined in DDL
collect statistics on {environment_schema}.markdowns_analysis_single_week{env_suffix}
;